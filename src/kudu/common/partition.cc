// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/common/partition.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/types.h"
#include "kudu/gutil/endian.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/decimal_util.h"
#include "kudu/util/hash_util.h"
#include "kudu/util/int128.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/url-coding.h"

using google::protobuf::RepeatedPtrField;
using kudu::pb_util::SecureDebugString;
using std::pair;
using std::set;
using std::string;
using std::vector;
using strings::Substitute;
using strings::SubstituteAndAppend;

namespace kudu {

class faststring;

// The encoded size of a hash bucket in a partition key.
static const size_t kEncodedBucketSize = sizeof(uint32_t);

Slice Partition::range_key_start() const {
  return range_key(partition_key_start());
}

Slice Partition::range_key_end() const {
  return range_key(partition_key_end());
}

Slice Partition::range_key(const string& partition_key) const {
  size_t hash_size = kEncodedBucketSize * hash_buckets().size();
  if (partition_key.size() > hash_size) {
    Slice s = Slice(partition_key);
    s.remove_prefix(hash_size);
    return s;
  } else {
    return Slice();
  }
}

bool Partition::Equals(const Partition& other) const {
  if (this == &other) return true;
  if (partition_key_start() != other.partition_key_start()) return false;
  if (partition_key_end() != other.partition_key_end()) return false;
  if (hash_buckets_ != other.hash_buckets_) return false;
  return true;
}

void Partition::ToPB(PartitionPB* pb) const {
  pb->Clear();
  pb->mutable_hash_buckets()->Reserve(hash_buckets_.size());
  for (int32_t bucket : hash_buckets()) {
    pb->add_hash_buckets(bucket);
  }
  pb->set_partition_key_start(partition_key_start());
  pb->set_partition_key_end(partition_key_end());
}

void Partition::FromPB(const PartitionPB& pb, Partition* partition) {
  partition->hash_buckets_.clear();
  partition->hash_buckets_.reserve(pb.hash_buckets_size());
  for (int32_t hash_bucket : pb.hash_buckets()) {
    partition->hash_buckets_.push_back(hash_bucket);
  }

  partition->partition_key_start_ = pb.partition_key_start();
  partition->partition_key_end_ = pb.partition_key_end();
}

namespace {
// Extracts the column IDs from a protobuf repeated field of column identifiers.
Status ExtractColumnIds(const RepeatedPtrField<PartitionSchemaPB_ColumnIdentifierPB>& identifiers,
                        const Schema& schema,
                        vector<ColumnId>* column_ids) {
    column_ids->reserve(identifiers.size());
    for (const auto& identifier : identifiers) {
      switch (identifier.identifier_case()) {
        case PartitionSchemaPB_ColumnIdentifierPB::kId: {
          ColumnId column_id(identifier.id());
          if (schema.find_column_by_id(column_id) == Schema::kColumnNotFound) {
            return Status::InvalidArgument("unknown column id", SecureDebugString(identifier));
          }
          column_ids->push_back(column_id);
          continue;
        }
        case PartitionSchemaPB_ColumnIdentifierPB::kName: {
          int32_t column_idx = schema.find_column(identifier.name());
          if (column_idx == Schema::kColumnNotFound) {
            return Status::InvalidArgument("unknown column", SecureDebugString(identifier));
          }
          column_ids->push_back(schema.column_id(column_idx));
          continue;
        }
        default: return Status::InvalidArgument("unknown column", SecureDebugString(identifier));
      }
    }
    return Status::OK();
}
// Sets a repeated field of column identifiers to the provided column IDs.
void SetColumnIdentifiers(const vector<ColumnId>& column_ids,
                          RepeatedPtrField<PartitionSchemaPB_ColumnIdentifierPB>* identifiers) {
    identifiers->Reserve(column_ids.size());
    for (const ColumnId& column_id : column_ids) {
      identifiers->Add()->set_id(column_id);
    }
}
} // namespace

Status PartitionSchema::FromPB(const PartitionSchemaPB& pb,
                               const Schema& schema,
                               PartitionSchema* partition_schema) {
  partition_schema->Clear();

  for (const PartitionSchemaPB_HashBucketSchemaPB& hash_bucket_pb : pb.hash_bucket_schemas()) {
    HashBucketSchema hash_bucket;
    RETURN_NOT_OK(ExtractColumnIds(hash_bucket_pb.columns(), schema, &hash_bucket.column_ids));

    // Hashing is column-order dependent, so sort the column_ids to ensure that
    // hash components with the same columns hash consistently. This is
    // important when deserializing a user-supplied partition schema during
    // table creation; after that the columns should remain in sorted order.
    std::sort(hash_bucket.column_ids.begin(), hash_bucket.column_ids.end());

    hash_bucket.seed = hash_bucket_pb.seed();
    hash_bucket.num_buckets = hash_bucket_pb.num_buckets();
    partition_schema->hash_bucket_schemas_.push_back(hash_bucket);
  }

  if (pb.has_range_schema()) {
    const PartitionSchemaPB_RangeSchemaPB& range_pb = pb.range_schema();
    RETURN_NOT_OK(ExtractColumnIds(range_pb.columns(), schema,
                                   &partition_schema->range_schema_.column_ids));
  } else {
    // Fill in the default range partition (PK columns).
    // like the sorting above, this should only happen during table creation
    // while deserializing the user-provided partition schema.
    for (int32_t column_idx = 0; column_idx < schema.num_key_columns(); column_idx++) {
      partition_schema->range_schema_.column_ids.push_back(schema.column_id(column_idx));
    }
  }

  return partition_schema->Validate(schema);
}

void PartitionSchema::ToPB(PartitionSchemaPB* pb) const {
  pb->Clear();
  pb->mutable_hash_bucket_schemas()->Reserve(hash_bucket_schemas_.size());
  for (const HashBucketSchema& hash_bucket : hash_bucket_schemas_) {
    PartitionSchemaPB_HashBucketSchemaPB* hash_bucket_pb = pb->add_hash_bucket_schemas();
    SetColumnIdentifiers(hash_bucket.column_ids, hash_bucket_pb->mutable_columns());
    hash_bucket_pb->set_num_buckets(hash_bucket.num_buckets);
    hash_bucket_pb->set_seed(hash_bucket.seed);
  }

  SetColumnIdentifiers(range_schema_.column_ids, pb->mutable_range_schema()->mutable_columns());
}

template<typename Row>
Status PartitionSchema::EncodeKeyImpl(const Row& row, string* buf) const {
  const KeyEncoder<string>& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));

  for (const HashBucketSchema& hash_bucket_schema : hash_bucket_schemas_) {
    int32_t bucket;
    RETURN_NOT_OK(BucketForRow(row, hash_bucket_schema, &bucket));
    hash_encoder.Encode(&bucket, buf);
  }

  return EncodeColumns(row, range_schema_.column_ids, buf);
}

Status PartitionSchema::EncodeKey(const KuduPartialRow& row, string* buf) const {
  return EncodeKeyImpl(row, buf);
}

Status PartitionSchema::EncodeKey(const ConstContiguousRow& row, string* buf) const {
  return EncodeKeyImpl(row, buf);
}

Status PartitionSchema::EncodeRangeKey(const KuduPartialRow& row,
                                       const Schema& schema,
                                       string* key) const {
  DCHECK(key->empty());
  bool contains_no_columns = true;
  for (int column_idx = 0; column_idx < schema.num_columns(); column_idx++) {
    const ColumnSchema& column = schema.column(column_idx);
    if (row.IsColumnSet(column_idx)) {
      if (std::find(range_schema_.column_ids.begin(),
                    range_schema_.column_ids.end(),
                    schema.column_id(column_idx)) != range_schema_.column_ids.end()) {
        contains_no_columns = false;
      } else {
        return Status::InvalidArgument(
            "split rows may only contain values for range partitioned columns", column.name());
      }
    }
  }

  if (contains_no_columns) {
    return Status::OK();
  }
  return EncodeColumns(row, range_schema_.column_ids, key);
}

Status PartitionSchema::EncodeRangeSplits(const vector<KuduPartialRow>& split_rows,
                                          const Schema& schema,
                                          vector<string>* splits) const {
  DCHECK(splits->empty());
  for (const KuduPartialRow& row : split_rows) {
    string split;
    RETURN_NOT_OK(EncodeRangeKey(row, schema, &split));
    if (split.empty()) {
        return Status::InvalidArgument(
            "split rows must contain a value for at least one range partition column");
    }
    splits->emplace_back(std::move(split));
  }

  std::sort(splits->begin(), splits->end());
  auto unique_end = std::unique(splits->begin(), splits->end());
  if (unique_end != splits->end()) {
    return Status::InvalidArgument("duplicate split row", RangeKeyDebugString(*unique_end, schema));
  }
  return Status::OK();
}

Status PartitionSchema::EncodeRangeBounds(const vector<pair<KuduPartialRow,
                                                            KuduPartialRow>>& range_bounds,
                                          const Schema& schema,
                                          vector<pair<string, string>>* range_partitions) const {
  DCHECK(range_partitions->empty());
  if (range_bounds.empty()) {
    range_partitions->emplace_back("", "");
    return Status::OK();
  }

  for (const auto& bound : range_bounds) {
    string lower;
    string upper;
    RETURN_NOT_OK(EncodeRangeKey(bound.first, schema, &lower));
    RETURN_NOT_OK(EncodeRangeKey(bound.second, schema, &upper));

    if (!lower.empty() && !upper.empty() && lower >= upper) {
      return Status::InvalidArgument(
          "range partition lower bound must be less than the upper bound",
          RangePartitionDebugString(bound.first, bound.second));
    }
    range_partitions->emplace_back(std::move(lower), std::move(upper));
  }

  // Check that the range bounds are non-overlapping
  std::sort(range_partitions->begin(), range_partitions->end());
  for (int i = 0; i < range_partitions->size() - 1; i++) {
    const string& first_upper = range_partitions->at(i).second;
    const string& second_lower = range_partitions->at(i + 1).first;

    if (first_upper.empty() || second_lower.empty() || first_upper > second_lower) {
      return Status::InvalidArgument(
          "overlapping range partitions",
          strings::Substitute("first range partition: $0, second range partition: $1",
                              RangePartitionDebugString(range_partitions->at(i).first,
                                                        range_partitions->at(i).second,
                                                        schema),
                              RangePartitionDebugString(range_partitions->at(i + 1).first,
                                                        range_partitions->at(i + 1).second,
                                                        schema)));
    }
  }

  return Status::OK();
}

Status PartitionSchema::SplitRangeBounds(const Schema& schema,
                                         vector<string> splits,
                                         vector<pair<string, string>>* bounds) const {
  int expected_bounds = std::max(1UL, bounds->size()) + splits.size();

  vector<pair<string, string>> new_bounds;
  new_bounds.reserve(expected_bounds);

  // Iterate through the sorted bounds and sorted splits, splitting the bounds
  // as appropriate and adding them to the result list ('new_bounds').

  auto split = splits.begin();
  for (auto& bound : *bounds) {
    string& lower = bound.first;
    const string& upper = bound.second;

    for (; split != splits.end() && (upper.empty() || *split <= upper); split++) {
      if (!lower.empty() && *split < lower) {
        return Status::InvalidArgument("split out of bounds", RangeKeyDebugString(*split, schema));
      }
      if (lower == *split || upper == *split) {
        return Status::InvalidArgument("split matches lower or upper bound",
                                       RangeKeyDebugString(*split, schema));
      }
      // Split the current bound. Add the lower section to the result list,
      // and continue iterating on the upper section.
      new_bounds.emplace_back(std::move(lower), *split);
      lower = std::move(*split);
    }

    new_bounds.emplace_back(std::move(lower), upper);
  }

  if (split != splits.end()) {
    return Status::InvalidArgument("split out of bounds", RangeKeyDebugString(*split, schema));
  }

  bounds->swap(new_bounds);
  CHECK_EQ(expected_bounds, bounds->size());
  return Status::OK();
}

Status PartitionSchema::CreatePartitions(const vector<KuduPartialRow>& split_rows,
                                         const vector<pair<KuduPartialRow,
                                                           KuduPartialRow>>& range_bounds,
                                         const Schema& schema,
                                         vector<Partition>* partitions) const {
  const KeyEncoder<string>& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));

  // Create a partition per hash bucket combination.
  *partitions = vector<Partition>(1);
  for (const HashBucketSchema& bucket_schema : hash_bucket_schemas_) {
    vector<Partition> new_partitions;
    // For each of the partitions created so far, replicate it
    // by the number of buckets in the next hash bucketing component
    for (const Partition& base_partition : *partitions) {
      for (int32_t bucket = 0; bucket < bucket_schema.num_buckets; bucket++) {
        Partition partition = base_partition;
        partition.hash_buckets_.push_back(bucket);
        hash_encoder.Encode(&bucket, &partition.partition_key_start_);
        hash_encoder.Encode(&bucket, &partition.partition_key_end_);
        new_partitions.push_back(partition);
      }
    }
    partitions->swap(new_partitions);
  }

  std::unordered_set<int> range_column_idxs;
  for (const ColumnId& column_id : range_schema_.column_ids) {
    int column_idx = schema.find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument(Substitute("range partition column ID $0 "
                                                "not found in table schema.", column_id));
    }
    if (!InsertIfNotPresent(&range_column_idxs, column_idx)) {
      return Status::InvalidArgument("duplicate column in range partition",
                                     schema.column(column_idx).name());
    }
  }

  vector<pair<string, string>> bounds;
  vector<string> splits;
  RETURN_NOT_OK(EncodeRangeBounds(range_bounds, schema, &bounds));
  RETURN_NOT_OK(EncodeRangeSplits(split_rows, schema, &splits));
  RETURN_NOT_OK(SplitRangeBounds(schema, std::move(splits), &bounds));

  // Create a partition per range bound and hash bucket combination.
  vector<Partition> new_partitions;
  for (const Partition& base_partition : *partitions) {
    for (const auto& bound : bounds) {
      Partition partition = base_partition;
      partition.partition_key_start_.append(bound.first);
      partition.partition_key_end_.append(bound.second);
      new_partitions.push_back(partition);
    }
  }
  partitions->swap(new_partitions);

  // Note: the following discussion and logic only takes effect when the table's
  // partition schema includes at least one hash bucket component, and the
  // absolute upper and/or absolute lower range bound is unbounded.
  //
  // At this point, we have the full set of partitions built up, but each
  // partition only covers a finite slice of the partition key-space. Some
  // operations involving partitions are easier (pruning, client meta cache) if
  // it can be assumed that the partition keyspace does not have holes.
  //
  // In order to 'fill in' the partition key space, the absolute first and last
  // partitions are extended to cover the rest of the lower and upper partition
  // range by clearing the start and end partition key, respectively.
  //
  // When the table has two or more hash components, there will be gaps in
  // between partitions at the boundaries of the component ranges. Similar to
  // the absolute start and end case, these holes are filled by clearing the
  // partition key beginning at the hash component. For a concrete example,
  // see PartitionTest::TestCreatePartitions.
  for (Partition& partition : *partitions) {
    if (partition.range_key_start().empty()) {
      for (int i = static_cast<int>(partition.hash_buckets().size()) - 1; i >= 0; i--) {
        if (partition.hash_buckets()[i] != 0) {
          break;
        }
        partition.partition_key_start_.erase(kEncodedBucketSize * i);
      }
    }
    if (partition.range_key_end().empty()) {
      for (int i = static_cast<int>(partition.hash_buckets().size()) - 1; i >= 0; i--) {
        partition.partition_key_end_.erase(kEncodedBucketSize * i);
        int32_t hash_bucket = partition.hash_buckets()[i] + 1;
        if (hash_bucket != hash_bucket_schemas_[i].num_buckets) {
          hash_encoder.Encode(&hash_bucket, &partition.partition_key_end_);
          break;
        }
      }
    }
  }

  return Status::OK();
}

template<typename Row>
Status PartitionSchema::PartitionContainsRowImpl(const Partition& partition,
                                                 const Row& row,
                                                 bool* contains) const {
  CHECK_EQ(partition.hash_buckets().size(), hash_bucket_schemas_.size());
  for (int i = 0; i < hash_bucket_schemas_.size(); i++) {
    const HashBucketSchema& hash_bucket_schema = hash_bucket_schemas_[i];
    int32_t bucket;
    RETURN_NOT_OK(BucketForRow(row, hash_bucket_schema, &bucket));

    if (bucket != partition.hash_buckets()[i]) {
      *contains = false;
      return Status::OK();
    }
  }

  string range_partition_key;
  RETURN_NOT_OK(EncodeColumns(row, range_schema_.column_ids, &range_partition_key));

  // If all of the hash buckets match, then the row is contained in the
  // partition if the row is gte the lower bound; and if there is no upper
  // bound, or the row is lt the upper bound.
  *contains = (Slice(range_partition_key).compare(partition.range_key_start()) >= 0)
           && (partition.range_key_end().empty()
                || Slice(range_partition_key).compare(partition.range_key_end()) < 0);

  return Status::OK();
}

Status PartitionSchema::PartitionContainsRow(const Partition& partition,
                                             const KuduPartialRow& row,
                                             bool* contains) const {
  return PartitionContainsRowImpl(partition, row, contains);
}

Status PartitionSchema::PartitionContainsRow(const Partition& partition,
                                             const ConstContiguousRow& row,
                                             bool* contains) const {
  return PartitionContainsRowImpl(partition, row, contains);
}


Status PartitionSchema::DecodeRangeKey(Slice* encoded_key,
                                       KuduPartialRow* row,
                                       Arena* arena) const {
  ContiguousRow cont_row(row->schema(), row->row_data_);
  for (int i = 0; i < range_schema_.column_ids.size(); i++) {
    if (encoded_key->empty()) {
      // This can happen when decoding partition start and end keys, since they
      // are truncated to simulate absolute upper and lower bounds.
      break;
    }

    int32_t column_idx = row->schema()->find_column_by_id(range_schema_.column_ids[i]);
    const ColumnSchema& column = row->schema()->column(column_idx);
    const KeyEncoder<faststring>& key_encoder = GetKeyEncoder<faststring>(column.type_info());
    bool is_last = i == (range_schema_.column_ids.size() - 1);

    // Decode the column.
    RETURN_NOT_OK_PREPEND(key_encoder.Decode(encoded_key,
                                             is_last,
                                             arena,
                                             cont_row.mutable_cell_ptr(column_idx)),
                          Substitute("Error decoding partition key range component '$0'",
                                     column.name()));
    // Mark the column as set.
    BitmapSet(row->isset_bitmap_, column_idx);
  }
  if (!encoded_key->empty()) {
    return Status::InvalidArgument("unable to fully decode range key",
                                   KUDU_REDACT(encoded_key->ToDebugString()));
  }
  return Status::OK();
}

// Decodes a slice of a partition key into the buckets. The slice is modified to
// remove the hash components.
Status PartitionSchema::DecodeHashBuckets(Slice* encoded_key,
                                          vector<int32_t>* buckets) const {
  size_t hash_components_size = kEncodedBucketSize * hash_bucket_schemas_.size();
  if (encoded_key->size() < hash_components_size) {
    return Status::InvalidArgument(
        Substitute("expected encoded hash key to be at least $0 bytes (only found $1)",
                   hash_components_size, encoded_key->size()));
  }
  for (const auto& schema : hash_bucket_schemas_) {
    (void) schema; // quiet unused variable warning
    uint32_t big_endian;
    memcpy(&big_endian, encoded_key->data(), sizeof(uint32_t));
    buckets->push_back(BigEndian::ToHost32(big_endian));
    encoded_key->remove_prefix(sizeof(uint32_t));
  }

  return Status::OK();
}

bool PartitionSchema::IsRangePartitionKeyEmpty(const KuduPartialRow& row) const {
  ConstContiguousRow const_row(row.schema(), row.row_data_);
  for (const ColumnId& column_id : range_schema_.column_ids) {
    if (row.IsColumnSet(row.schema()->find_column_by_id(column_id))) return false;
  }
  return true;
}

void PartitionSchema::AppendRangeDebugStringComponentsOrMin(const KuduPartialRow& row,
                                                            vector<string>* components) const {
  ConstContiguousRow const_row(row.schema(), row.row_data_);

  for (const ColumnId& column_id : range_schema_.column_ids) {
    int32_t column_idx = row.schema()->find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      components->emplace_back("<unknown-column>");
      continue;
    }
    const ColumnSchema& column_schema = row.schema()->column(column_idx);

    if (!row.IsColumnSet(column_idx)) {
      uint8_t min_value[kLargestTypeSize];
      column_schema.type_info()->CopyMinValue(&min_value);
      SimpleConstCell cell(&column_schema, &min_value);
      components->emplace_back(column_schema.Stringify(cell.ptr()));
    } else {
      components->emplace_back(column_schema.Stringify(const_row.cell_ptr(column_idx)));
    }
  }
}

namespace {
// Converts a list of column IDs to a string with the column names seperated by
// a comma character.
string ColumnIdsToColumnNames(const Schema& schema,
                              const vector<ColumnId>& column_ids) {
  vector<string> names;
  for (const ColumnId& column_id : column_ids) {
    names.push_back(schema.column(schema.find_column_by_id(column_id)).name());
  }

  return JoinStrings(names, ", ");
}
} // namespace

string PartitionSchema::PartitionDebugString(const Partition& partition,
                                             const Schema& schema) const {
  // Partitions are considered metadata, so don't redact them.
  ScopedDisableRedaction no_redaction;

  vector<string> components;
  if (partition.hash_buckets_.size() != hash_bucket_schemas_.size()) {
    return "<hash-partition-error>";
  }

  for (int i = 0; i < hash_bucket_schemas_.size(); i++) {
    string s = Substitute("HASH ($0) PARTITION $1",
                          ColumnIdsToColumnNames(schema, hash_bucket_schemas_[i].column_ids),
                          partition.hash_buckets_[i]);
    components.emplace_back(std::move(s));
  }

  if (!range_schema_.column_ids.empty()) {
    string s = Substitute("RANGE ($0) PARTITION $1",
                          ColumnIdsToColumnNames(schema, range_schema_.column_ids),
                          RangePartitionDebugString(partition.range_key_start(),
                                                    partition.range_key_end(),
                                                    schema));
    components.emplace_back(std::move(s));
  }

  return JoinStrings(components, ", ");
}

template<typename Row>
string PartitionSchema::PartitionKeyDebugStringImpl(const Row& row) const {
  vector<string> components;

  for (const HashBucketSchema& hash_bucket_schema : hash_bucket_schemas_) {
    int32_t bucket;
    Status s = BucketForRow(row, hash_bucket_schema, &bucket);
    if (s.ok()) {
      components.emplace_back(
          Substitute("HASH ($0): $1",
                     ColumnIdsToColumnNames(*row.schema(), hash_bucket_schema.column_ids),
                     bucket));
    } else {
      components.emplace_back(Substitute("<hash-error: $0>", s.ToString()));
    }
  }

  if (!range_schema_.column_ids.empty()) {
      components.emplace_back(
          Substitute("RANGE ($0): $1",
                     ColumnIdsToColumnNames(*row.schema(), range_schema_.column_ids),
                     RangeKeyDebugString(row)));
  }

  return JoinStrings(components, ", ");
}
template
string PartitionSchema::PartitionKeyDebugStringImpl(const KuduPartialRow& row) const;
template
string PartitionSchema::PartitionKeyDebugStringImpl(const ConstContiguousRow& row) const;

string PartitionSchema::PartitionKeyDebugString(const ConstContiguousRow& row) const {
  return PartitionKeyDebugStringImpl(row);
}

string PartitionSchema::PartitionKeyDebugString(const KuduPartialRow& row) const {
  return PartitionKeyDebugStringImpl(row);
}

string PartitionSchema::PartitionKeyDebugString(Slice key, const Schema& schema) const {
  vector<string> components;

  size_t hash_components_size = kEncodedBucketSize * hash_bucket_schemas_.size();
  if (key.size() < hash_components_size) {
    return "<hash-decode-error>";
  }

  for (const auto& hash_schema : hash_bucket_schemas_) {
    uint32_t big_endian;
    memcpy(&big_endian, key.data(), sizeof(uint32_t));
    key.remove_prefix(sizeof(uint32_t));
    components.emplace_back(
        Substitute("HASH ($0): $1",
                    ColumnIdsToColumnNames(schema, hash_schema.column_ids),
                    BigEndian::ToHost32(big_endian)));
  }

  if (!range_schema_.column_ids.empty()) {
      components.emplace_back(
          Substitute("RANGE ($0): $1",
                     ColumnIdsToColumnNames(schema, range_schema_.column_ids),
                     RangeKeyDebugString(key, schema)));
  }

  return JoinStrings(components, ", ");
}

string PartitionSchema::RangePartitionDebugString(const KuduPartialRow& lower_bound,
                                                  const KuduPartialRow& upper_bound) const {
  // Partitions are considered metadata, so don't redact them.
  ScopedDisableRedaction no_redaction;

  bool lower_unbounded = IsRangePartitionKeyEmpty(lower_bound);
  bool upper_unbounded = IsRangePartitionKeyEmpty(upper_bound);
  if (lower_unbounded && upper_unbounded) {
    return "UNBOUNDED";
  }
  if (lower_unbounded) {
    return Substitute("VALUES < $0", RangeKeyDebugString(upper_bound));
  }
  if (upper_unbounded) {
    return Substitute("VALUES >= $0", RangeKeyDebugString(lower_bound));
  }
  // TODO(dan): recognize when a simplified 'VALUE =' form can be used (see
  // org.apache.kudu.client.Partition#formatRangePartition).
  return Substitute("$0 <= VALUES < $1",
                    RangeKeyDebugString(lower_bound),
                    RangeKeyDebugString(upper_bound));
}

string PartitionSchema::RangePartitionDebugString(Slice lower_bound,
                                                  Slice upper_bound,
                                                  const Schema& schema) const {
  // Partitions are considered metadata, so don't redact them.
  ScopedDisableRedaction no_redaction;

  Arena arena(256);
  KuduPartialRow lower(&schema);
  KuduPartialRow upper(&schema);

  Status s = DecodeRangeKey(&lower_bound, &lower, &arena);
  if (!s.ok()) {
    return Substitute("<range-key-decode-error: $0>", s.ToString());
  }
  s = DecodeRangeKey(&upper_bound, &upper, &arena);
  if (!s.ok()) {
    return Substitute("<range-key-decode-error: $0>", s.ToString());
  }

  return RangePartitionDebugString(lower, upper);
}

string PartitionSchema::RangeKeyDebugString(Slice range_key, const Schema& schema) const {
  Arena arena(256);
  KuduPartialRow row(&schema);

  Status s = DecodeRangeKey(&range_key, &row, &arena);
  if (!s.ok()) {
    return Substitute("<range-key-decode-error: $0>", s.ToString());
  }
  return RangeKeyDebugString(row);
}

string PartitionSchema::RangeKeyDebugString(const KuduPartialRow& key) const {
  vector<string> components;
  AppendRangeDebugStringComponentsOrMin(key, &components);
  if (components.size() == 1) {
    // Omit the parentheses if the range partition has a single column.
    return components.back();
  }
  return Substitute("($0)", JoinStrings(components, ", "));
}

string PartitionSchema::RangeKeyDebugString(const ConstContiguousRow& key) const {
  vector<string> components;

  for (const ColumnId& column_id : range_schema_.column_ids) {
    string column;
    int32_t column_idx = key.schema()->find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      components.emplace_back("<unknown-column>");
      break;
    }
    key.schema()->column(column_idx).DebugCellAppend(key.cell(column_idx), &column);
    components.push_back(column);
  }

  if (components.size() == 1) {
    // Omit the parentheses if the range partition has a single column.
    return components.back();
  }
  return Substitute("($0)", JoinStrings(components, ", "));
}

vector<string> PartitionSchema::DebugStringComponents(const Schema& schema) const {
  vector<string> components;

  for (const auto& hash_bucket_schema : hash_bucket_schemas_) {
    string s;
    SubstituteAndAppend(&s, "HASH ($0) PARTITIONS $1",
                        ColumnIdsToColumnNames(schema, hash_bucket_schema.column_ids),
                        hash_bucket_schema.num_buckets);
    if (hash_bucket_schema.seed != 0) {
      SubstituteAndAppend(&s, " SEED $0", hash_bucket_schema.seed);
    }
    components.emplace_back(std::move(s));
  }

  if (!range_schema_.column_ids.empty()) {
    string s = Substitute("RANGE ($0)", ColumnIdsToColumnNames(schema, range_schema_.column_ids));
    components.emplace_back(std::move(s));
  }

  return components;
}

string PartitionSchema::DebugString(const Schema& schema) const {
  return JoinStrings(DebugStringComponents(schema), ", ");
}

string PartitionSchema::DisplayString(const Schema& schema,
                                      const vector<string>& range_partitions) const {
  string display_string = JoinStrings(DebugStringComponents(schema), ",\n");

  if (!range_schema_.column_ids.empty()) {
    display_string.append(" (");
    if (range_partitions.empty()) {
      display_string.append(")");
    } else {
      bool is_first = true;
      for (const string& range_partition : range_partitions) {
        if (is_first) {
          is_first = false;
        } else {
          display_string.push_back(',');
        }
        display_string.append("\n    PARTITION ");
        display_string.append(range_partition);
      }
      display_string.append("\n)");
    }
  }
  return display_string;
}

string PartitionSchema::PartitionTableHeader(const Schema& schema) const {
  string header;
  for (const auto& hash_bucket_schema : hash_bucket_schemas_) {
    SubstituteAndAppend(&header, "<th>HASH ($0) PARTITION</th>",
                        EscapeForHtmlToString(
                          ColumnIdsToColumnNames(schema, hash_bucket_schema.column_ids)));
  }
  if (!range_schema_.column_ids.empty()) {
    SubstituteAndAppend(&header, "<th>RANGE ($0) PARTITION</th>",
                        EscapeForHtmlToString(
                          ColumnIdsToColumnNames(schema, range_schema_.column_ids)));
  }
  return header;
}

string PartitionSchema::PartitionTableEntry(const Schema& schema,
                                            const Partition& partition) const {
  string entry;
  for (int32_t bucket : partition.hash_buckets_) {
    SubstituteAndAppend(&entry, "<td>$0</td>", bucket);
  }

  if (!range_schema_.column_ids.empty()) {
    SubstituteAndAppend(&entry, "<td>$0</td>",
                        EscapeForHtmlToString(
                          RangePartitionDebugString(partition.range_key_start(),
                                                    partition.range_key_end(),
                                                    schema)));
  }
  return entry;
}

bool PartitionSchema::Equals(const PartitionSchema& other) const {
  if (this == &other) return true;

  // Compare range component.
  if (range_schema_.column_ids != other.range_schema_.column_ids) return false;

  // Compare hash bucket components.
  if (hash_bucket_schemas_.size() != other.hash_bucket_schemas_.size()) return false;
  for (int i = 0; i < hash_bucket_schemas_.size(); i++) {
    if (hash_bucket_schemas_[i].seed != other.hash_bucket_schemas_[i].seed) return false;
    if (hash_bucket_schemas_[i].num_buckets
        != other.hash_bucket_schemas_[i].num_buckets) return false;
    if (hash_bucket_schemas_[i].column_ids
        != other.hash_bucket_schemas_[i].column_ids) return false;
  }

  return true;
}

// Encodes the specified primary key columns of the supplied row into the buffer.
Status PartitionSchema::EncodeColumns(const ConstContiguousRow& row,
                                      const vector<ColumnId>& column_ids,
                                      string* buf) {
  for (int i = 0; i < column_ids.size(); i++) {
    ColumnId column_id = column_ids[i];
    int32_t column_idx = row.schema()->find_column_by_id(column_id);
    const TypeInfo* type = row.schema()->column(column_idx).type_info();
    GetKeyEncoder<string>(type).Encode(row.cell_ptr(column_idx), i + 1 == column_ids.size(), buf);
  }
  return Status::OK();
}

// Encodes the specified primary key columns of the supplied row into the buffer.
Status PartitionSchema::EncodeColumns(const KuduPartialRow& row,
                                      const vector<ColumnId>& column_ids,
                                      string* buf) {
  for (int i = 0; i < column_ids.size(); i++) {
    int32_t column_idx = row.schema()->find_column_by_id(column_ids[i]);
    CHECK(column_idx != Schema::kColumnNotFound);
    const TypeInfo* type_info = row.schema()->column(column_idx).type_info();
    const KeyEncoder<string>& encoder = GetKeyEncoder<string>(type_info);

    if (PREDICT_FALSE(!row.IsColumnSet(column_idx))) {
      uint8_t min_value[kLargestTypeSize];
      type_info->CopyMinValue(min_value);
      encoder.Encode(min_value, i + 1 == column_ids.size(), buf);
    } else {
      ContiguousRow cont_row(row.schema(), row.row_data_);
      encoder.Encode(cont_row.cell_ptr(column_idx), i + 1 == column_ids.size(), buf);
    }
  }
  return Status::OK();
}

int32_t PartitionSchema::BucketForEncodedColumns(const string& encoded_key,
                                                 const HashBucketSchema& hash_bucket_schema) {
  uint64_t hash = HashUtil::MurmurHash2_64(encoded_key.data(),
                                           encoded_key.length(),
                                           hash_bucket_schema.seed);
  return hash % static_cast<uint64_t>(hash_bucket_schema.num_buckets);
}

template<typename Row>
Status PartitionSchema::BucketForRow(const Row& row,
                                     const HashBucketSchema& hash_bucket_schema,
                                     int32_t* bucket) {
  string buf;
  RETURN_NOT_OK(EncodeColumns(row, hash_bucket_schema.column_ids, &buf));
  uint64_t hash = HashUtil::MurmurHash2_64(buf.data(), buf.length(), hash_bucket_schema.seed);
  *bucket = hash % static_cast<uint64_t>(hash_bucket_schema.num_buckets);
  return Status::OK();
}

//------------------------------------------------------------
// Template instantiations: We instantiate all possible templates to avoid linker issues.
// see: https://isocpp.org/wiki/faq/templates#separate-template-fn-defn-from-decl
//------------------------------------------------------------

template
Status PartitionSchema::BucketForRow(const KuduPartialRow& row,
                                     const HashBucketSchema& hash_bucket_schema,
                                     int32_t* bucket);

template
Status PartitionSchema::BucketForRow(const ConstContiguousRow& row,
                                     const HashBucketSchema& hash_bucket_schema,
                                     int32_t* bucket);

void PartitionSchema::Clear() {
  hash_bucket_schemas_.clear();
  range_schema_.column_ids.clear();
}

Status PartitionSchema::Validate(const Schema& schema) const {
  set<ColumnId> hash_columns;
  for (const PartitionSchema::HashBucketSchema& hash_schema : hash_bucket_schemas_) {
    if (hash_schema.num_buckets < 2) {
      return Status::InvalidArgument("must have at least two hash buckets");
    }

    if (hash_schema.column_ids.size() < 1) {
      return Status::InvalidArgument("must have at least one hash column");
    }

    for (const ColumnId& hash_column : hash_schema.column_ids) {
      if (!hash_columns.insert(hash_column).second) {
        return Status::InvalidArgument("hash bucket schema components must not "
                                       "contain columns in common");
      }
      int32_t column_idx = schema.find_column_by_id(hash_column);
      if (column_idx == Schema::kColumnNotFound) {
        return Status::InvalidArgument("must specify existing columns for hash "
                                       "bucket partition components");
      } else if (column_idx >= schema.num_key_columns()) {
        return Status::InvalidArgument("must specify only primary key columns for "
                                       "hash bucket partition components");
      }
    }
  }

  for (const ColumnId& column_id : range_schema_.column_ids) {
    int32_t column_idx = schema.find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument("must specify existing columns for range "
                                     "partition component");
    } else if (column_idx >= schema.num_key_columns()) {
      return Status::InvalidArgument("must specify only primary key columns for "
                                     "range partition component");
    }
  }

  return Status::OK();
}

namespace {

  // Increments an unset column in the row.
  Status IncrementUnsetColumn(KuduPartialRow* row, int32_t idx) {
    DCHECK(!row->IsColumnSet(idx));
    const ColumnSchema& col = row->schema()->column(idx);
    switch (col.type_info()->type()) {
      case INT8:
        RETURN_NOT_OK(row->SetInt8(idx, INT8_MIN + 1));
        break;
      case INT16:
        RETURN_NOT_OK(row->SetInt16(idx, INT16_MIN + 1));
        break;
      case INT32:
        RETURN_NOT_OK(row->SetInt32(idx, INT32_MIN + 1));
        break;
      case INT64:
      case UNIXTIME_MICROS:
        RETURN_NOT_OK(row->SetInt64(idx, INT64_MIN + 1));
        break;
      case VARCHAR:
        RETURN_NOT_OK(row->SetVarchar(idx, Slice("\0", 1)));
        break;
      case STRING:
        RETURN_NOT_OK(row->SetStringCopy(idx, Slice("\0", 1)));
        break;
      case BINARY:
        RETURN_NOT_OK(row->SetBinaryCopy(idx, Slice("\0", 1)));
        break;
      case DECIMAL32:
      case DECIMAL64:
      case DECIMAL128:
        RETURN_NOT_OK(row->SetUnscaledDecimal(idx,
            MinUnscaledDecimal(col.type_attributes().precision) + 1));
        break;
      default:
        return Status::InvalidArgument("Invalid column type in range partition",
                                       row->schema()->column(idx).ToString());
    }
    return Status::OK();
  }

  // Increments a column in the row, setting 'success' to true if the increment
  // succeeds, or false if the column is already the maximum value.
  Status IncrementColumn(KuduPartialRow* row, int32_t idx, bool* success) {
    DCHECK(row->IsColumnSet(idx));
    const ColumnSchema& col = row->schema()->column(idx);
    *success = true;
    switch (col.type_info()->type()) {
      case INT8: {
        int8_t value;
        RETURN_NOT_OK(row->GetInt8(idx, &value));
        if (value < INT8_MAX) {
          RETURN_NOT_OK(row->SetInt8(idx, value + 1));
        } else {
          *success = false;
        }
        break;
      }
      case INT16: {
        int16_t value;
        RETURN_NOT_OK(row->GetInt16(idx, &value));
        if (value < INT16_MAX) {
          RETURN_NOT_OK(row->SetInt16(idx, value + 1));
        } else {
          *success = false;
        }
        break;
      }
      case INT32: {
        int32_t value;
        RETURN_NOT_OK(row->GetInt32(idx, &value));
        if (value < INT32_MAX) {
          RETURN_NOT_OK(row->SetInt32(idx, value + 1));
        } else {
          *success = false;
        }
        break;
      }
      case INT64: {
         int64_t value;
         RETURN_NOT_OK(row->GetInt64(idx, &value));
         if (value < INT64_MAX) {
           RETURN_NOT_OK(row->SetInt64(idx, value + 1));
         } else {
           *success = false;
         }
         break;
       }
      case UNIXTIME_MICROS: {
        int64_t value;
        RETURN_NOT_OK(row->GetUnixTimeMicros(idx, &value));
        if (value < INT64_MAX) {
          RETURN_NOT_OK(row->SetUnixTimeMicros(idx, value + 1));
        } else {
          *success = false;
        }
        break;
      }
      case DECIMAL32:
      case DECIMAL64:
      case DECIMAL128: {
        int128_t value;
        RETURN_NOT_OK(row->GetUnscaledDecimal(idx, &value));
        if (value < MaxUnscaledDecimal(col.type_attributes().precision)) {
          RETURN_NOT_OK(row->SetUnscaledDecimal(idx, value + 1));
        } else {
          *success = false;
        }
        break;
      }
      case BINARY: {
        Slice value;
        RETURN_NOT_OK(row->GetBinary(idx, &value));
        string incremented = value.ToString();
        incremented.push_back('\0');
        RETURN_NOT_OK(row->SetBinaryCopy(idx, incremented));
        break;
      }
      case VARCHAR: {
        Slice value;
        RETURN_NOT_OK(row->GetVarchar(idx, &value));
        string incremented = value.ToString();
        incremented.push_back('\0');
        RETURN_NOT_OK(row->SetVarchar(idx, incremented));
        break;
      }
      case STRING: {
        Slice value;
        RETURN_NOT_OK(row->GetString(idx, &value));
        string incremented = value.ToString();
        incremented.push_back('\0');
        RETURN_NOT_OK(row->SetStringCopy(idx, incremented));
        break;
      }
      default:
        return Status::InvalidArgument("Invalid column type in range partition",
                                       row->schema()->column(idx).ToString());
    }
    return Status::OK();
  }
} // anonymous namespace

Status PartitionSchema::IncrementRangePartitionKey(KuduPartialRow* row, bool* increment) const {
  vector<int32_t> unset_idxs;
  *increment = false;
  for (auto itr = range_schema_.column_ids.rbegin();
       itr != range_schema_.column_ids.rend(); ++itr) {
    int32_t idx = row->schema()->find_column_by_id(*itr);
    if (idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument(Substitute("range partition column ID $0 "
                                                "not found in range partition key schema.",
                                                *itr));
    }

    if (row->IsColumnSet(idx)) {
      RETURN_NOT_OK(IncrementColumn(row, idx, increment));
      if (*increment) break;
    } else {
      RETURN_NOT_OK(IncrementUnsetColumn(row, idx));
      *increment = true;
      break;
    }
    unset_idxs.push_back(idx);
  }

  if (*increment) {
    for (int32_t idx : unset_idxs) {
      RETURN_NOT_OK(row->Unset(idx));
    }
  }

  return Status::OK();
}

Status PartitionSchema::MakeLowerBoundRangePartitionKeyInclusive(KuduPartialRow* row) const {
  // To transform a lower bound range partition key from exclusive to inclusive,
  // the key must be incremented. To increment the key, start with the least
  // significant column in the key (furthest right), and increment it.  If the
  // increment fails because the value is already the maximum, move on to the
  // next least significant column and attempt to increment it (and so on). When
  // incrementing, an unset cell is equivalent to a cell set to the minimum
  // value for its column (e.g. an unset Int8 column is incremented to -127
  // (-2^7 + 1)). Finally, all columns less significant than the incremented
  // column are unset (which means they are treated as the minimum value for
  // that column). If all columns in the key are the maximum and can not be
  // incremented, then the operation fails.
  //
  // A few examples, given a range partition of three Int8 columns. Underscore
  // signifies unset:
  //
  // (1, 2, 3)       -> (1, 2, 4)
  // (1, 2, 127)     -> (1, 3, _)
  // (1, 127, 3)     -> (1, 127, 4)
  // (1, _, 3)       -> (1, _, 4)
  // (_, _, _)       -> (_, _, 1)
  // (1, 127, 127)   -> (2, _, _)
  // (127, 127, 127) -> fail!

  bool increment;
  RETURN_NOT_OK(IncrementRangePartitionKey(row, &increment));

  if (!increment) {
    vector<string> components;
    AppendRangeDebugStringComponentsOrMin(*row, &components);
    return Status::InvalidArgument("Exclusive lower bound range partition key must not "
                                   "have maximum values for all components",
                                   RangeKeyDebugString(*row));
  }

  return Status::OK();
}

Status PartitionSchema::MakeUpperBoundRangePartitionKeyExclusive(KuduPartialRow* row) const {
  // To transform an upper bound range partition key from inclusive to exclusive,
  // the key must be incremented. Incrementing the key follows the same steps as
  // turning an exclusive lower bound key into exclusive. Upper bound keys have
  // two additional special cases:
  //
  // * For upper bound range partition keys with all columns unset, no
  //   transformation is needed (all unset columns signifies unbounded,
  //   so there is no difference between inclusive and exclusive).
  //
  // * For an upper bound key that can't be incremented because all components
  //   are the maximum value, all columns are unset in order to transform it to
  //   an unbounded upper bound (this is a special case increment).

  bool all_unset = true;
  for (const ColumnId& column_id : range_schema_.column_ids) {
    int32_t idx = row->schema()->find_column_by_id(column_id);
    if (idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument(Substitute("range partition column ID $0 "
                                                "not found in range partition key schema.",
                                                column_id));
    }
    all_unset = !row->IsColumnSet(idx);
    if (!all_unset) break;
  }

  if (all_unset) return Status::OK();

  bool increment;
  RETURN_NOT_OK(IncrementRangePartitionKey(row, &increment));
  if (!increment) {
    for (const ColumnId& column_id : range_schema_.column_ids) {
      int32_t idx = row->schema()->find_column_by_id(column_id);
      RETURN_NOT_OK(row->Unset(idx));
    }
  }

  return Status::OK();
}

Status PartitionSchema::GetRangeSchemaColumnIndexes(const Schema& schema,
                                                    vector<int32_t>* range_column_idxs) const {
  for (const ColumnId& column_id : range_schema_.column_ids) {
    int32_t idx = schema.find_column_by_id(column_id);
    if (idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument(Substitute("range partition column ID $0 "
                                                "not found in range partition key schema.",
                                                column_id));
    }
    range_column_idxs->push_back(idx);
  }
  return Status::OK();
}

} // namespace kudu
