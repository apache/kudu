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
#include <iomanip>
#include <iostream>
#include <iterator>
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
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
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

bool Partition::operator==(const Partition& rhs) const {
  if (this == &rhs) {
    return true;
  }
  if (hash_buckets_ != rhs.hash_buckets_) {
    return false;
  }

  DCHECK(hash_buckets_ == rhs.hash_buckets_);
  const bool this_begin_is_legacy = PartitionKey::IsLegacy(begin_, hash_buckets_);
  const bool other_begin_is_legacy = PartitionKey::IsLegacy(rhs.begin_, rhs.hash_buckets_);
  // If both are legacy or both not, we can compare them directly.
  if (this_begin_is_legacy == other_begin_is_legacy) {
    if (begin_ != rhs.begin_) {
      return false;
    }
  }

  const bool this_end_is_legacy =  PartitionKey::IsLegacy(end_, hash_buckets_);
  const bool other_end_is_legacy = PartitionKey::IsLegacy(rhs.end_, rhs.hash_buckets_);;
  // If both are legacy or both not, we can compare them directly.
  if (this_end_is_legacy == other_end_is_legacy) {
    if (end_ != rhs.end_) {
      return false;
    }
  }

  if (this_begin_is_legacy != other_begin_is_legacy) {
    // If one is legacy and the other is not, we need to convert the legacy one.
    if (this_begin_is_legacy) {
      DCHECK(!other_begin_is_legacy);
      PartitionKey this_begin = PartitionKey::LowerBoundFromLegacy(begin_, hash_buckets_);
      if (this_begin != rhs.begin_) {
        return false;
      }
    } else {
      DCHECK(other_begin_is_legacy);
      PartitionKey other_begin = PartitionKey::LowerBoundFromLegacy(rhs.begin_, rhs.hash_buckets_);
      if (begin_ != other_begin) {
        return false;
      }
    }
  }

  if (this_end_is_legacy != other_end_is_legacy) {
    // If one is legacy and the other is not, we need to convert the legacy one.
    if (this_end_is_legacy) {
      DCHECK(!other_end_is_legacy);
      PartitionKey this_end = PartitionKey::UpperBoundFromLegacy(end_, hash_buckets_);
      if (this_end != rhs.end_) {
        return false;
      }
    } else {
      DCHECK(other_end_is_legacy);
      PartitionKey other_end = PartitionKey::UpperBoundFromLegacy(rhs.end_, rhs.hash_buckets_);
      if (end_ != other_end) {
        return false;
      }
    }
  }

  return true;
}

void Partition::ToPB(PartitionPB* pb) const {
  pb->Clear();
  pb->mutable_hash_buckets()->Reserve(hash_buckets_.size());
  for (int32_t bucket : hash_buckets_) {
    pb->add_hash_buckets(bucket);
  }
  pb->set_partition_key_start(begin().ToString());
  pb->set_partition_key_end(end().ToString());
}

void Partition::FromPB(const PartitionPB& pb, Partition* partition) {
  partition->hash_buckets_.clear();
  partition->hash_buckets_.reserve(pb.hash_buckets_size());
  for (int32_t hash_bucket : pb.hash_buckets()) {
    partition->hash_buckets_.push_back(hash_bucket);
  }

  const size_t hash_buckets_num = partition->hash_buckets_.size();
  partition->begin_ = StringToPartitionKey(
      pb.partition_key_start(), hash_buckets_num);
  partition->end_ = StringToPartitionKey(
      pb.partition_key_end(), hash_buckets_num);
  // Check the invariant: valid partition cannot span over multiple sets
  // of hash buckets.
  //DCHECK(partition->begin_.empty() || partition->end_.empty() ||
  //       partition->begin_.hash_key() == partition->end_.hash_key());
}

PartitionKey Partition::StringToPartitionKey(const std::string& key_str,
                                             size_t hash_dimensions_num) {
  if (hash_dimensions_num == 0) {
    return PartitionKey("", key_str);
  }
  const size_t hash_part_size = kEncodedBucketSize * hash_dimensions_num;
  if (key_str.size() <= hash_part_size) {
    return PartitionKey(key_str, "");
  }
  return PartitionKey(key_str.substr(0, hash_part_size),
                      key_str.substr(hash_part_size));
}

bool PartitionKey::IsLegacy(const PartitionKey& pk,
                            const vector<int32_t>& hash_buckets) {
  return pk.range_key().empty() && !hash_buckets.empty() &&
      pk.hash_key().size() != kEncodedBucketSize * hash_buckets.size();

}

PartitionKey PartitionKey::LowerBoundFromLegacy(const PartitionKey& pk,
                                                const vector<int32_t>& hash_buckets) {
  DCHECK(PartitionKey::IsLegacy(pk, hash_buckets));
  const auto& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));
  PartitionKey res;
  for (const auto bucket : hash_buckets) {
    hash_encoder.Encode(&bucket, res.mutable_hash_key());
  }
  return res;
}

PartitionKey PartitionKey::UpperBoundFromLegacy(const PartitionKey& pk,
                                                const vector<int32_t>& hash_buckets) {
  DCHECK(PartitionKey::IsLegacy(pk, hash_buckets));
  const auto& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));
  PartitionKey res;
  for (auto idx = 0; idx < hash_buckets.size(); ++idx) {
    int32_t b = hash_buckets[idx];
    if (idx + 1 == hash_buckets.size()) {
      ++b;
    }
    hash_encoder.Encode(&b, res.mutable_hash_key());
  }
  return res;
}

string PartitionKey::DebugString() const {
  std::ostringstream ss;
  ss << "h:";
  for (size_t i = 0; i < hash_key_.size(); ++i) {
    ss << std::hex << std::setw(2) << std::setfill('0')
       << static_cast<uint16_t>(hash_key_[i]);
  }
  ss << " r:";
  for (size_t i = 0; i < range_key_.size(); ++i) {
    ss << std::hex << std::setw(2) << std::setfill('0')
       << static_cast<uint16_t>(range_key_[i]);
  }
  return ss.str();
}

std::ostream& operator<<(std::ostream& out, const PartitionKey& key) {
  out << key.DebugString();
  return out;
}

namespace {
// Extracts the column IDs from a protobuf repeated field of column identifiers.
Status ExtractColumnIds(const RepeatedPtrField<PartitionSchemaPB_ColumnIdentifierPB>& identifiers,
                        const Schema& schema,
                        vector<ColumnId>* column_ids) {
  vector<ColumnId> new_column_ids;
  new_column_ids.reserve(identifiers.size());
  for (const auto& identifier : identifiers) {
    switch (identifier.identifier_case()) {
      case PartitionSchemaPB_ColumnIdentifierPB::kId: {
        ColumnId column_id(identifier.id());
        if (schema.find_column_by_id(column_id) == Schema::kColumnNotFound) {
          return Status::InvalidArgument("unknown column id", SecureDebugString(identifier));
        }
        new_column_ids.emplace_back(std::move(column_id));
        continue;
      }
      case PartitionSchemaPB_ColumnIdentifierPB::kName: {
        int32_t column_idx = schema.find_column(identifier.name());
        if (column_idx == Schema::kColumnNotFound) {
          return Status::InvalidArgument("unknown column", SecureDebugString(identifier));
        }
        new_column_ids.emplace_back(schema.column_id(column_idx));
        continue;
      }
      default: return Status::InvalidArgument("unknown column", SecureDebugString(identifier));
    }
  }
  *column_ids = std::move(new_column_ids);
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
} // anonymous namespace


Status PartitionSchema::ExtractHashSchemaFromPB(
    const Schema& schema,
    const RepeatedPtrField<PartitionSchemaPB_HashBucketSchemaPB>& hash_schema_pb,
    HashSchema* hash_schema) {
  for (const auto& hash_dimension_pb : hash_schema_pb) {
    HashDimension hash_dimension;
    RETURN_NOT_OK(ExtractColumnIds(
        hash_dimension_pb.columns(), schema, &hash_dimension.column_ids));

    // Hashing is column-order dependent, so sort the column_ids to ensure that
    // hash components with the same columns hash consistently. This is
    // important when deserializing a user-supplied partition schema during
    // table creation; after that the columns should remain in sorted order.
    std::sort(hash_dimension.column_ids.begin(), hash_dimension.column_ids.end());

    hash_dimension.seed = hash_dimension_pb.seed();
    hash_dimension.num_buckets = hash_dimension_pb.num_buckets();
    hash_schema->push_back(std::move(hash_dimension));
  }
  return Status::OK();
}

Status PartitionSchema::FromPB(
    const PartitionSchemaPB& pb,
    const Schema& schema,
    PartitionSchema* partition_schema,
    RangesWithHashSchemas* ranges_with_hash_schemas) {
  partition_schema->Clear();
  RETURN_NOT_OK(ExtractHashSchemaFromPB(
      schema, pb.hash_schema(), &partition_schema->hash_schema_));

  const auto custom_ranges_num = pb.custom_hash_schema_ranges_size();
  vector<HashSchema> range_hash_schemas(custom_ranges_num);
  vector<pair<KuduPartialRow, KuduPartialRow>> range_bounds;
  for (auto i = 0; i < custom_ranges_num; ++i) {
    const auto& range = pb.custom_hash_schema_ranges(i);
    RETURN_NOT_OK(ExtractHashSchemaFromPB(
        schema, range.hash_schema(), &range_hash_schemas[i]));

    RowOperationsPBDecoder decoder(&range.range_bounds(), &schema, &schema, nullptr);
    vector<DecodedRowOperation> ops;
    RETURN_NOT_OK(decoder.DecodeOperations<DecoderMode::SPLIT_ROWS>(&ops));
    if (ops.size() != 2) {
      return Status::InvalidArgument(Substitute(
          "$0 ops were provided; only two ops are expected "
          "for this pair of range bounds", ops.size()));
    }
    const DecodedRowOperation& op1 = ops[0];
    const DecodedRowOperation& op2 = ops[1];
    switch (op1.type) {
      case RowOperationsPB::RANGE_LOWER_BOUND:
      case RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND: {
        if (op2.type != RowOperationsPB::RANGE_UPPER_BOUND &&
            op2.type != RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND) {
          return Status::InvalidArgument("missing upper range bound in request");
        }

        // Lower bound range partition keys are inclusive and upper bound range partition keys
        // are exclusive by design. If the provided keys are not of this format, these keys
        // will be transformed to their proper format.
        if (op1.type == RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND) {
          RETURN_NOT_OK(partition_schema->MakeLowerBoundRangePartitionKeyInclusive(
              op1.split_row.get()));
        }
        if (op2.type == RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND) {
          RETURN_NOT_OK(partition_schema->MakeUpperBoundRangePartitionKeyExclusive(
              op2.split_row.get()));
        }
        range_bounds.emplace_back(*op1.split_row, *op2.split_row);
        break;
      }
      default:
        return Status::InvalidArgument(
            Substitute("Illegal row operation type in request: $0", op1.type));
    }
  }

  if (pb.has_range_schema()) {
    const PartitionSchemaPB_RangeSchemaPB& range_pb = pb.range_schema();
    RETURN_NOT_OK(ExtractColumnIds(range_pb.columns(), schema,
                                   &partition_schema->range_schema_.column_ids));
  } else {
    // Fill in the default range partition (PK columns excluding the auto-incrementing column).
    // like the sorting above, this should only happen during table creation
    // while deserializing the user-provided partition schema.
    for (size_t column_idx = 0; column_idx < schema.num_key_columns(); ++column_idx) {
      if (schema.auto_incrementing_col_idx() != column_idx) {
        partition_schema->range_schema_.column_ids.push_back(schema.column_id(column_idx));
      }
    }
  }

  RangesWithHashSchemas result_ranges_with_hash_schemas;
  if (!range_bounds.empty()) {
    RETURN_NOT_OK(partition_schema->EncodeRangeBounds(
        range_bounds, range_hash_schemas, schema,
        &result_ranges_with_hash_schemas));
  }

  if (range_bounds.size() != result_ranges_with_hash_schemas.size()) {
    return Status::InvalidArgument(Substitute("the number of range bounds "
        "($0) differs from the number ranges with hash schemas ($1)",
        range_bounds.size(), result_ranges_with_hash_schemas.size()));
  }

  auto& ranges_with_custom_hash_schemas =
      partition_schema->ranges_with_custom_hash_schemas_;
  const auto& table_wide_hash_schema = partition_schema->hash_schema_;
  ranges_with_custom_hash_schemas.reserve(result_ranges_with_hash_schemas.size());
  for (const auto& elem : result_ranges_with_hash_schemas) {
    if (elem.hash_schema != table_wide_hash_schema) {
      ranges_with_custom_hash_schemas.emplace_back(elem);
    }
  }

  // Sort the ranges.
  constexpr struct {
    bool operator()(const PartitionSchema::RangeWithHashSchema& lhs,
                    const PartitionSchema::RangeWithHashSchema& rhs) const {
      return lhs.lower < rhs.lower;
    }
  } rangeLess;
  sort(ranges_with_custom_hash_schemas.begin(),
       ranges_with_custom_hash_schemas.end(),
       rangeLess);

  auto& dict = partition_schema->hash_schema_idx_by_encoded_range_start_;
  for (auto it = ranges_with_custom_hash_schemas.cbegin();
       it != ranges_with_custom_hash_schemas.cend(); ++it) {
    InsertOrDie(&dict, it->lower, std::distance(
        ranges_with_custom_hash_schemas.cbegin(), it));
  }
  DCHECK_EQ(ranges_with_custom_hash_schemas.size(), dict.size());

  RETURN_NOT_OK(partition_schema->Validate(schema));
  if (ranges_with_hash_schemas) {
    *ranges_with_hash_schemas = std::move(result_ranges_with_hash_schemas);
  }

  return Status::OK();
}

Status PartitionSchema::ToPB(const Schema& schema, PartitionSchemaPB* pb) const {
  pb->Clear();
  pb->mutable_hash_schema()->Reserve(hash_schema_.size());
  for (const auto& hash_dimension : hash_schema_) {
    auto* hash_dimension_pb = pb->add_hash_schema();
    SetColumnIdentifiers(hash_dimension.column_ids,
                         hash_dimension_pb->mutable_columns());
    hash_dimension_pb->set_num_buckets(hash_dimension.num_buckets);
    hash_dimension_pb->set_seed(hash_dimension.seed);
  }

  if (!ranges_with_custom_hash_schemas_.empty()) {
    // Consumers of this method does not expect the information
    // on ranges with the table-wide hash schema persisted into the
    // 'custom_hash_schema_ranges' field.
    pb->mutable_custom_hash_schema_ranges()->Reserve(
        ranges_with_custom_hash_schemas_.size());
    Arena arena(256);
    for (const auto& range_hash_schema : ranges_with_custom_hash_schemas_) {
      if (range_hash_schema.hash_schema == hash_schema_) {
        continue;
      }
      auto* range_pb = pb->add_custom_hash_schema_ranges();

      arena.Reset();
      RowOperationsPBEncoder encoder(range_pb->mutable_range_bounds());
      KuduPartialRow lower(&schema);
      KuduPartialRow upper(&schema);
      Slice s_lower = Slice(range_hash_schema.lower);
      Slice s_upper = Slice(range_hash_schema.upper);
      RETURN_NOT_OK(DecodeRangeKey(&s_lower, &lower, &arena));
      RETURN_NOT_OK(DecodeRangeKey(&s_upper, &upper, &arena));
      encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
      encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);

      range_pb->mutable_hash_schema()->Reserve(
          range_hash_schema.hash_schema.size());
      for (const auto& hash_dimension : range_hash_schema.hash_schema) {
        auto* hash_dimension_pb = range_pb->add_hash_schema();
        SetColumnIdentifiers(hash_dimension.column_ids,
                             hash_dimension_pb->mutable_columns());
        hash_dimension_pb->set_num_buckets(hash_dimension.num_buckets);
        hash_dimension_pb->set_seed(hash_dimension.seed);
      }
    }
  }

  SetColumnIdentifiers(range_schema_.column_ids,
                       pb->mutable_range_schema()->mutable_columns());
  return Status::OK();
}

template<typename Row>
void PartitionSchema::EncodeKeyImpl(const Row& row,
                                    string* range_buf,
                                    string* hash_buf) const {
  DCHECK(range_buf);
  DCHECK(hash_buf);
  string range_key;
  EncodeColumns(row, range_schema_.column_ids, &range_key);

  const auto& hash_schema = GetHashSchemaForRange(range_key);
  const auto& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));
  string hash_key;
  for (const auto& hash_dimension : hash_schema) {
    const auto bucket = HashValueForRow(row, hash_dimension);
    hash_encoder.Encode(&bucket, &hash_key);
  }
  *range_buf = std::move(range_key);
  *hash_buf = std::move(hash_key);
}

PartitionKey PartitionSchema::EncodeKey(const KuduPartialRow& row) const {
  string encoded_range;
  string encoded_hash;
  EncodeKeyImpl(row, &encoded_range, &encoded_hash);
  return PartitionKey(std::move(encoded_hash), std::move(encoded_range));
}

PartitionKey PartitionSchema::EncodeKey(const ConstContiguousRow& row) const {
  string encoded_range;
  string encoded_hash;
  EncodeKeyImpl(row, &encoded_range, &encoded_hash);
  return PartitionKey(std::move(encoded_hash), std::move(encoded_range));
}

Status PartitionSchema::EncodeRangeKey(const KuduPartialRow& row,
                                       const Schema& schema,
                                       string* key) const {
  DCHECK(key->empty());
  bool contains_no_columns = true;
  for (size_t column_idx = 0; column_idx < schema.num_columns(); ++column_idx) {
    const ColumnSchema& column = schema.column(column_idx);
    if (row.IsColumnSet(column_idx)) {
      if (std::find(range_schema_.column_ids.begin(),
                    range_schema_.column_ids.end(),
                    schema.column_id(column_idx)) != range_schema_.column_ids.end()) {
        contains_no_columns = false;
      } else {
        return Status::InvalidArgument(
            "split rows may only contain values for range partition columns", column.name());
      }
    }
  }

  if (contains_no_columns) {
    return Status::OK();
  }
  EncodeColumns(row, range_schema_.column_ids, key);
  return Status::OK();
}

Status PartitionSchema::EncodeRangeSplits(const vector<KuduPartialRow>& split_rows,
                                          const Schema& schema,
                                          vector<string>* splits) const {
  DCHECK(splits->empty());
  if (split_rows.empty()) {
    return Status::OK();
  }
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

Status PartitionSchema::EncodeRangeBounds(
    const vector<pair<KuduPartialRow, KuduPartialRow>>& range_bounds,
    const vector<HashSchema>& range_hash_schemas,
    const Schema& schema,
    RangesWithHashSchemas* bounds_with_hash_schemas,
    bool allow_create_partition) const {
  DCHECK(bounds_with_hash_schemas);
  auto& bounds_whs = *bounds_with_hash_schemas;
  DCHECK(bounds_whs.empty());
  if (range_bounds.empty()) {
    if (!allow_create_partition) {
      bounds_whs.emplace_back(RangeWithHashSchema{"", "", hash_schema_});
    }
    return Status::OK();
  }

  if (!range_hash_schemas.empty() &&
      range_hash_schemas.size() != range_bounds.size()) {
    return Status::InvalidArgument(Substitute(
        "$0 vs $1: per range hash schemas and range bounds "
        "must have the same size",
        range_hash_schemas.size(), range_bounds.size()));
  }

  size_t j = 0;
  for (const auto& bound : range_bounds) {
    string lower;
    RETURN_NOT_OK(EncodeRangeKey(bound.first, schema, &lower));
    string upper;
    RETURN_NOT_OK(EncodeRangeKey(bound.second, schema, &upper));

    if (!lower.empty() && !upper.empty() && lower >= upper) {
      return Status::InvalidArgument(
          "range partition lower bound must be less than the upper bound",
          RangePartitionDebugString(bound.first, bound.second));
    }
    RangeWithHashSchema temp{std::move(lower), std::move(upper), hash_schema_};
    if (!range_hash_schemas.empty()) {
      temp.hash_schema = range_hash_schemas[j++];
    }
    bounds_whs.emplace_back(std::move(temp));
  }

  std::sort(bounds_whs.begin(), bounds_whs.end(),
            [](const RangeWithHashSchema& s1, const RangeWithHashSchema& s2) {
    return s1.lower < s2.lower;
  });

  // Check that the range bounds are non-overlapping.
  for (size_t i = 0; i + 1 < bounds_whs.size(); ++i) {
    const string& first_upper = bounds_whs[i].upper;
    const string& second_lower = bounds_whs[i + 1].lower;

    if (first_upper.empty() || second_lower.empty() ||
        first_upper > second_lower) {
      return Status::InvalidArgument(
          "overlapping range partitions",
          Substitute(
              "first range partition: $0, second range partition: $1",
              RangePartitionDebugString(bounds_whs[i].lower,
                                        bounds_whs[i].upper,
                                        schema),
              RangePartitionDebugString(bounds_whs[i + 1].lower,
                                        bounds_whs[i + 1].upper,
                                        schema)));
    }
  }

  return Status::OK();
}

Status PartitionSchema::SplitRangeBounds(
    const Schema& schema,
    const vector<string>& splits,
    RangesWithHashSchemas* bounds_with_hash_schemas) const {
  DCHECK(bounds_with_hash_schemas);
  if (splits.empty()) {
    return Status::OK();
  }

  const auto expected_bounds =
      std::max(1UL, bounds_with_hash_schemas->size()) + splits.size();
  RangesWithHashSchemas new_bounds_with_hash_schemas;
  new_bounds_with_hash_schemas.reserve(expected_bounds);

  // Iterate through the sorted bounds and sorted splits, splitting the bounds
  // as appropriate and adding them to the result list ('new_bounds').

  auto split = splits.begin();
  for (auto& bound : *bounds_with_hash_schemas) {
    string& lower = bound.lower;
    const string& upper = bound.upper;

    for (; split != splits.end() && (upper.empty() || *split <= upper); ++split) {
      if (!lower.empty() && *split < lower) {
        return Status::InvalidArgument("split out of bounds", RangeKeyDebugString(*split, schema));
      }
      if (lower == *split || upper == *split) {
        return Status::InvalidArgument("split matches lower or upper bound",
                                       RangeKeyDebugString(*split, schema));
      }
      // Split the current bound. Add the lower section to the result list,
      // and continue iterating on the upper section.
      new_bounds_with_hash_schemas.emplace_back(
          RangeWithHashSchema{std::move(lower), *split, hash_schema_});
      lower = *split;
    }

    new_bounds_with_hash_schemas.emplace_back(
        RangeWithHashSchema{std::move(lower), upper, hash_schema_});
  }

  if (split != splits.end()) {
    return Status::InvalidArgument("split out of bounds", RangeKeyDebugString(*split, schema));
  }

  bounds_with_hash_schemas->swap(new_bounds_with_hash_schemas);
  CHECK_EQ(expected_bounds, bounds_with_hash_schemas->size());
  return Status::OK();
}

Status PartitionSchema::CreatePartitions(
    const vector<KuduPartialRow>& split_rows,
    const vector<pair<KuduPartialRow, KuduPartialRow>>& range_bounds,
    const Schema& schema,
    vector<Partition>* partitions,
    bool allow_empty_partition) const {
  DCHECK(partitions);

  RETURN_NOT_OK(CheckRangeSchema(schema));

  RangesWithHashSchemas bounds_with_hash_schemas;
  RETURN_NOT_OK(EncodeRangeBounds(range_bounds, {}, schema,
                                  &bounds_with_hash_schemas,
                                  allow_empty_partition));
  vector<string> splits;
  RETURN_NOT_OK(EncodeRangeSplits(split_rows, schema, &splits));
  RETURN_NOT_OK(SplitRangeBounds(schema, splits, &bounds_with_hash_schemas));

  const auto& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));
  const auto base_hash_partitions =
      GenerateHashPartitions(hash_schema_, hash_encoder);

  // Even if no hash partitioning for a table is specified, there must be at
  // least one element in 'base_hash_partitions': it's used to build the result
  // set of range partitions.
  DCHECK_GE(base_hash_partitions.size(), 1);
  // The case of a single element in base_hash_partitions is a special case.
  DCHECK(base_hash_partitions.size() > 1 ||
         base_hash_partitions.front().hash_buckets().empty());

  // Create a partition per range bound and hash bucket combination.
  vector<Partition> new_partitions;
  for (const Partition& base_partition : base_hash_partitions) {
    for (const auto& bound : bounds_with_hash_schemas) {
      Partition p = base_partition;
      p.begin_.mutable_range_key()->append(bound.lower);
      p.end_.mutable_range_key()->append(bound.upper);
      new_partitions.emplace_back(std::move(p));
    }
  }

  UpdatePartitionBoundaries(hash_encoder, &new_partitions);
  *partitions = std::move(new_partitions);

  return Status::OK();
}

Status PartitionSchema::CreatePartitions(
      const RangesWithHashSchemas& ranges_with_hash_schemas,
      const Schema& schema,
      vector<Partition>* partitions) const {
  DCHECK(partitions);

  RETURN_NOT_OK(CheckRangeSchema(schema));

  const auto& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));
  vector<Partition> result_partitions;
  // Iterate through each bound and its hash schemas to generate hash partitions.
  for (size_t i = 0; i < ranges_with_hash_schemas.size(); ++i) {
    const auto& range = ranges_with_hash_schemas[i];
    vector<Partition> current_bound_hash_partitions = GenerateHashPartitions(
        range.hash_schema, hash_encoder);
    // Add range information to the partition key.
    for (Partition& p : current_bound_hash_partitions) {
      DCHECK(p.begin_.range_key().empty()) << p.begin_.DebugString();
      p.begin_.mutable_range_key()->assign(range.lower);
      DCHECK(p.end().range_key().empty());
      p.end_.mutable_range_key()->assign(range.upper);
    }
    result_partitions.insert(result_partitions.end(),
                             std::make_move_iterator(current_bound_hash_partitions.begin()),
                             std::make_move_iterator(current_bound_hash_partitions.end()));
  }

  UpdatePartitionBoundaries(hash_encoder, &result_partitions);
  *partitions = std::move(result_partitions);

  return Status::OK();
}

Status PartitionSchema::CreatePartitionsForRange(
    const pair<KuduPartialRow, KuduPartialRow>& range_bound,
    const HashSchema& range_hash_schema,
    const Schema& schema,
    std::vector<Partition>* partitions) const {
  RETURN_NOT_OK(CheckRangeSchema(schema));

  RangesWithHashSchemas ranges_with_hash_schemas;
  RETURN_NOT_OK(EncodeRangeBounds(
      {range_bound}, {range_hash_schema}, schema, &ranges_with_hash_schemas));
  DCHECK_EQ(1, ranges_with_hash_schemas.size());

  const auto& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));
  const auto& range = ranges_with_hash_schemas.front();
  vector<Partition> result_partitions = GenerateHashPartitions(
      range.hash_schema, hash_encoder);
  // Add range information to the partition key.
  for (Partition& p : result_partitions) {
    DCHECK(p.begin_.range_key().empty()) << p.begin_.DebugString();
    p.begin_.mutable_range_key()->assign(range.lower);
    DCHECK(p.end().range_key().empty());
    p.end_.mutable_range_key()->assign(range.upper);
    UpdatePartitionBoundaries(hash_encoder, range_hash_schema, &p);
  }
  *partitions = std::move(result_partitions);

  return Status::OK();
}

template<typename Row>
bool PartitionSchema::PartitionContainsRowImpl(const Partition& partition,
                                               const Row& row) const {
  string range_key;
  EncodeColumns(row, range_schema_.column_ids, &range_key);
  const auto& hash_schema = GetHashSchemaForRange(range_key);
  for (size_t i = 0; i < hash_schema.size(); ++i) {
    if (!HashPartitionContainsRowImpl(partition, row, hash_schema, i)) {
      return false;
    }
  }
  return RangePartitionContainsEncodedKey(partition, range_key);
}

template<typename Row>
bool PartitionSchema::HashPartitionContainsRowImpl(
    const Partition& partition,
    const Row& row,
    const HashSchema& hash_schema,
    int hash_value) const {
  DCHECK_GE(hash_value, 0);
  DCHECK_LT(hash_value, hash_schema.size());
  DCHECK_EQ(partition.hash_buckets().size(), hash_schema.size());
  const HashDimension& hash_dimension = hash_schema[hash_value];
  const auto bucket = HashValueForRow(row, hash_dimension);
  return partition.hash_buckets()[hash_value] == bucket;
}

template<typename Row>
bool PartitionSchema::RangePartitionContainsRowImpl(
    const Partition& partition, const Row& row) const {
  // If range partition is not used, column_ids would be empty and
  // EncodedColumn() would return immediately.
  string key;
  EncodeColumns(row, range_schema_.column_ids, &key);

  return RangePartitionContainsEncodedKey(partition, key);
}

bool PartitionSchema::PartitionContainsRow(const Partition& partition,
                                           const KuduPartialRow& row) const {
  return PartitionContainsRowImpl(partition, row);
}

bool PartitionSchema::PartitionContainsRow(const Partition& partition,
                                           const ConstContiguousRow& row) const {
  return PartitionContainsRowImpl(partition, row);
}

bool PartitionSchema::PartitionMayContainRow(const Partition& partition,
                                             const KuduPartialRow& row) const {
  // It's the fast and 100% sure path when the row has the primary key set.
  if (row.IsKeySet()) {
    return PartitionContainsRow(partition, row);
  }

  const Schema* schema = row.schema();
  vector<ColumnId> set_column_ids;
  set_column_ids.reserve(schema->num_key_columns());
  for (size_t idx = 0; idx < schema->num_key_columns(); ++idx) {
    DCHECK(schema->is_key_column(idx));
    if (row.IsColumnSet(idx) && !row.IsNull(idx)) {
      set_column_ids.emplace_back(schema->column_id(idx));
      if (set_column_ids.size() > 1) {
        // No futher optimizations in this case for a while.
        // NOTE: This might be a false positive.
        return true;
      }
    }
  }
  if (set_column_ids.empty()) {
    return false;
  }

  DCHECK_EQ(1, set_column_ids.size());
  const auto& single_column_id = set_column_ids[0];

  if (range_schema_.column_ids.size() == 1 &&
      range_schema_.column_ids[0] == single_column_id &&
      !RangePartitionContainsRow(partition, row)) {
    return false;
  }

  const auto& hash_schema = GetHashSchemaForRange(partition.begin_.range_key());
  for (size_t i = 0; i < hash_schema.size(); ++i) {
    const auto& hash_dimension = hash_schema[i];
    if (hash_dimension.column_ids.size() == 1 &&
        hash_dimension.column_ids[0] == single_column_id &&
        !HashPartitionContainsRowImpl(partition, row, hash_schema, i)) {
      return false;
    }
  }

  // NOTE: This might be a false positive.
  return true;
}

bool PartitionSchema::RangePartitionContainsRow(
    const Partition& partition, const KuduPartialRow& row) const {
  return RangePartitionContainsRowImpl(partition, row);
}

Status PartitionSchema::DecodeRangeKey(Slice* encoded_key,
                                       KuduPartialRow* partial_row,
                                       Arena* arena) const {
  ContiguousRow cont_row(partial_row->schema(), partial_row->row_data_);
  for (int i = 0; i < range_schema_.column_ids.size(); i++) {
    if (encoded_key->empty()) {
      // This can happen when decoding partition start and end keys, since they
      // are truncated to simulate absolute upper and lower bounds.
      break;
    }

    int32_t column_idx = partial_row->schema()->find_column_by_id(range_schema_.column_ids[i]);
    const ColumnSchema& column = partial_row->schema()->column(column_idx);
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
    BitmapSet(partial_row->isset_bitmap_, column_idx);

    if (column.type_info()->physical_type() == BINARY) {
      // Copy cell value into the 'partial_row', because in the decoding process above, we just make
      // row data a pointer to the memory allocated by arena.
      partial_row->Set(column_idx, cont_row.cell_ptr(column_idx));
    }
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
  size_t hash_components_size = kEncodedBucketSize * hash_schema_.size();
  if (encoded_key->size() < hash_components_size) {
    return Status::InvalidArgument(
        Substitute("expected encoded hash key to be at least $0 bytes (only found $1)",
                   hash_components_size, encoded_key->size()));
  }
  for (const auto& _ : hash_schema_) {
    (void) _; // quiet unused variable warning
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
  names.reserve(column_ids.size());
  for (const ColumnId& column_id : column_ids) {
    names.push_back(schema.column(schema.find_column_by_id(column_id)).name());
  }

  return JoinStrings(names, ", ");
}
} // namespace

string PartitionSchema::PartitionDebugString(const Partition& partition,
                                             const Schema& schema,
                                             HashPartitionInfo hp) const {
  // Partitions are considered metadata, so don't redact them.
  ScopedDisableRedaction no_redaction;

  const auto& hash_schema = GetHashSchemaForRange(partition.begin().range_key());
  if (partition.hash_buckets_.size() != hash_schema.size()) {
    return Substitute("<hash-partition-error: $0 vs $1 expected buckets>",
                      partition.hash_buckets_.size(), hash_schema.size());
  }

  vector<string> components;
  if (hp == HashPartitionInfo::SHOW) {
    for (size_t i = 0; i < hash_schema.size(); ++i) {
      string s = Substitute("HASH ($0) PARTITION $1",
                            ColumnIdsToColumnNames(schema, hash_schema[i].column_ids),
                            partition.hash_buckets_[i]);
      components.emplace_back(std::move(s));
    }
  }

  if (!range_schema_.column_ids.empty()) {
    string s = Substitute("RANGE ($0) PARTITION $1",
                          ColumnIdsToColumnNames(schema, range_schema_.column_ids),
                          RangePartitionDebugString(partition.begin().range_key(),
                                                    partition.end().range_key(),
                                                    schema));
    components.emplace_back(std::move(s));
  }

  return JoinStrings(components, ", ");
}

template<typename Row>
string PartitionSchema::PartitionKeyDebugStringImpl(const Row& row) const {
  string range_key;
  EncodeColumns(row, range_schema_.column_ids, &range_key);
  const auto& hash_schema = GetHashSchemaForRange(range_key);
  vector<string> components;
  components.reserve(hash_schema.size() + range_schema_.column_ids.size());
  for (const auto& hash_dimension : hash_schema) {
    components.emplace_back(Substitute(
        "HASH ($0): $1",
        ColumnIdsToColumnNames(*row.schema(), hash_dimension.column_ids),
        HashValueForRow(row, hash_dimension)));
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

string PartitionSchema::PartitionKeyDebugString(const PartitionKey& key,
                                                const Schema& schema) const {
  const auto& hash_schema = GetHashSchemaForRange(key.range_key());
  Slice hash_key(key.hash_key());
  if (hash_key.size() != kEncodedBucketSize * hash_schema.size()) {
     return "<hash-decode-error>";
  }

  vector<string> components;
  components.reserve(hash_schema.size() + 1);
  for (const auto& hash_dimension : hash_schema) {
    uint32_t big_endian;
    memcpy(&big_endian, hash_key.data(), kEncodedBucketSize);
    hash_key.remove_prefix(kEncodedBucketSize);
    components.emplace_back(
        Substitute("HASH ($0): $1",
                    ColumnIdsToColumnNames(schema, hash_dimension.column_ids),
                    BigEndian::ToHost32(big_endian)));
  }
  DCHECK(hash_key.empty());

  const bool has_ranges = !range_schema_.column_ids.empty();
  if (has_ranges) {
    Slice range_key(key.range_key());
    components.emplace_back(
        Substitute("RANGE ($0): $1",
                   ColumnIdsToColumnNames(schema, range_schema_.column_ids),
                   RangeKeyDebugString(range_key, schema)));
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
    return Substitute("$0 <= VALUES", RangeKeyDebugString(lower_bound));
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

string PartitionSchema::RangeWithCustomHashPartitionDebugString(Slice lower_bound,
                                                                Slice upper_bound,
                                                                const Schema& schema) const {
  // Partitions are considered metadata, so don't redact them.
  ScopedDisableRedaction no_redaction;
  HashSchema hash_schema = GetHashSchemaForRange(lower_bound.ToString());
  KuduPartialRow lower(&schema);
  Arena arena(256);

  // Decode the lower and upper bounds
  Status s = DecodeRangeKey(&lower_bound, &lower, &arena);
  if (!s.ok()) {
    return Substitute("<range-key-decode-error: $0>", s.ToString());
  }

  KuduPartialRow upper(&schema);
  s = DecodeRangeKey(&upper_bound, &upper, &arena);
  if (!s.ok()) {
    return Substitute("<range-key-decode-error: $0>", s.ToString());
  }

  // Get the range bounds information for the partition
  bool lower_unbounded = IsRangePartitionKeyEmpty(lower);
  bool upper_unbounded = IsRangePartitionKeyEmpty(upper);
  string partition;
  if (lower_unbounded && upper_unbounded) {
    partition = "UNBOUNDED";
  } else if (lower_unbounded) {
    partition = Substitute("VALUES < $0", RangeKeyDebugString(upper));
  } else if (upper_unbounded) {
    partition = Substitute("$0 <= VALUES", RangeKeyDebugString(lower));
  } else {
    partition = Substitute("$0 <= VALUES < $1",
                           RangeKeyDebugString(lower),
                           RangeKeyDebugString(upper));
  }

  // Get the hash schema information for the partition
  if (hash_schema != hash_schema_) {
    for (const auto& hash_dimension: hash_schema) {
      partition.append(Substitute(" HASH($0) PARTITIONS $1",
                                  ColumnIdsToColumnNames(schema, hash_dimension.column_ids),
                                  hash_dimension.num_buckets));
    }
  }
  return partition;
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
    components.emplace_back(std::move(column));
  }

  if (components.size() == 1) {
    // Omit the parentheses if the range partition has a single column.
    return components.back();
  }
  return Substitute("($0)", JoinStrings(components, ", "));
}

vector<string> PartitionSchema::DebugStringComponents(const Schema& schema) const {
  // TODO(aserbin): adapt this to custom hash schemas per range
  vector<string> components;
  for (const auto& hash_dimension : hash_schema_) {
    string s;
    SubstituteAndAppend(&s, "HASH ($0) PARTITIONS $1",
                        ColumnIdsToColumnNames(schema, hash_dimension.column_ids),
                        hash_dimension.num_buckets);
    if (hash_dimension.seed != 0) {
      SubstituteAndAppend(&s, " SEED $0", hash_dimension.seed);
    }
    components.emplace_back(std::move(s));
  }

  if (!range_schema_.column_ids.empty()) {
    string s = Substitute("RANGE ($0)", ColumnIdsToColumnNames(
        schema, range_schema_.column_ids));
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
  if (!hash_schema_.empty()) {
    header.append("<th>Hash Schema Type</th>");
    header.append("<th>Hash Schema</th>");
    header.append("<th>Hash Partition</th>");
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
  const auto* idx_ptr = FindOrNull(
      hash_schema_idx_by_encoded_range_start_, partition.begin_.range_key());

  if (idx_ptr) {
    const auto& range = ranges_with_custom_hash_schemas_[*idx_ptr];
    entry.append("<td>Range Specific</td>");
    entry.append("<td>");
    for (size_t i = 0; i < range.hash_schema.size(); i++) {
      SubstituteAndAppend(&entry, "HASH ($0) PARTITIONS $1<br>",
                          EscapeForHtmlToString(ColumnIdsToColumnNames(
                              schema, range.hash_schema[i].column_ids)),
                          range.hash_schema[i].num_buckets);
    }
    entry.append("</td>");
    entry.append("<td>");
    for (size_t i = 0; i < range.hash_schema.size(); i++) {
      SubstituteAndAppend(&entry, "HASH ($0) PARTITION: $1<br>",
                          EscapeForHtmlToString(ColumnIdsToColumnNames(
                              schema, range.hash_schema[i].column_ids)),
                          partition.hash_buckets_[i]);
    }
    entry.append("</td>");
  } else {
    entry.append("<td>Table Wide</td>");
    entry.append("<td>");
    for (size_t i = 0; i < hash_schema_.size(); i++) {
        SubstituteAndAppend(&entry, "HASH ($0) PARTITIONS $1<br>",
                            EscapeForHtmlToString(ColumnIdsToColumnNames(
                                schema, hash_schema_[i].column_ids)),
                                hash_schema_[i].num_buckets);
    }
    entry.append("</td>");
    entry.append("<td>");
    for (size_t i = 0; i < hash_schema_.size(); i++) {
      SubstituteAndAppend(&entry, "HASH ($0) PARTITION: $1<br>",
                          EscapeForHtmlToString(ColumnIdsToColumnNames(
                              schema, hash_schema_[i].column_ids)),
                          partition.hash_buckets_[i]);
    }
    entry.append("</td>");
  }

  if (!range_schema_.column_ids.empty()) {
    SubstituteAndAppend(&entry, "<td>$0</td>",
                        EscapeForHtmlToString(
                          RangePartitionDebugString(partition.begin().range_key(),
                                                    partition.end().range_key(),
                                                    schema)));
  }
  return entry;
}

bool PartitionSchema::operator==(const PartitionSchema& rhs) const {
  // TODO(aserbin): what about ranges_with_custom_hash_schemas_?
  if (this == &rhs) {
    return true;
  }

  // Compare range component.
  if (range_schema_.column_ids != rhs.range_schema_.column_ids) {
    return false;
  }

  const auto& lhs_ranges = ranges_with_custom_hash_schemas_;
  const auto& rhs_ranges = rhs.ranges_with_custom_hash_schemas_;
  if (hash_schema_.size() != rhs.hash_schema_.size() ||
      lhs_ranges.size() != rhs_ranges.size()) {
    return false;
  }

  // Compare table wide hash bucket schemas.
  for (size_t i = 0; i < hash_schema_.size(); ++i) {
    if (hash_schema_[i] != rhs.hash_schema_[i]) {
      return false;
    }
  }

  // Compare range bounds and per range hash bucket schemas.
  for (size_t i = 0; i < lhs_ranges.size(); ++i) {
    if (lhs_ranges[i].lower != rhs_ranges[i].lower ||
        lhs_ranges[i].upper != rhs_ranges[i].upper) {
      return false;
    }
    const auto& lhs_hash_schema = lhs_ranges[i].hash_schema;
    const auto& rhs_hash_schema = rhs_ranges[i].hash_schema;
    if (lhs_hash_schema.size() != rhs_hash_schema.size()) {
      return false;
    }
    for (size_t j = 0; j < lhs_hash_schema.size(); ++j) {
      if (lhs_hash_schema[j] != rhs_hash_schema[j]) {
        return false;
      }
    }
  }

  return true;
}

// Encodes the specified primary key columns of the supplied row into the buffer.
void PartitionSchema::EncodeColumns(const ConstContiguousRow& row,
                                    const vector<ColumnId>& column_ids,
                                    string* buf) {
  for (size_t i = 0; i < column_ids.size(); ++i) {
    ColumnId column_id = column_ids[i];
    auto column_idx = row.schema()->find_column_by_id(column_id);
    CHECK(column_idx != Schema::kColumnNotFound);
    const TypeInfo* type = row.schema()->column(column_idx).type_info();
    GetKeyEncoder<string>(type).Encode(
        row.cell_ptr(column_idx), i + 1 == column_ids.size(), buf);
  }
}

// Encodes the specified primary key columns of the supplied row into the buffer.
void PartitionSchema::EncodeColumns(const KuduPartialRow& row,
                                    const vector<ColumnId>& column_ids,
                                    string* buf) {
  for (size_t i = 0; i < column_ids.size(); ++i) {
    auto column_idx = row.schema()->find_column_by_id(column_ids[i]);
    CHECK(column_idx != Schema::kColumnNotFound);
    const TypeInfo* type_info = row.schema()->column(column_idx).type_info();
    const KeyEncoder<string>& encoder = GetKeyEncoder<string>(type_info);

    if (PREDICT_FALSE(!row.IsColumnSet(column_idx))) {
      uint8_t min_value[kLargestTypeSize];
      type_info->CopyMinValue(min_value);
      encoder.Encode(min_value, i + 1 == column_ids.size(), buf);
    } else {
      ContiguousRow cont_row(row.schema(), row.row_data_);
      encoder.Encode(
          cont_row.cell_ptr(column_idx), i + 1 == column_ids.size(), buf);
    }
  }
}

uint32_t PartitionSchema::HashValueForEncodedColumns(
    const string& encoded_hash_columns,
    const HashDimension& hash_dimension) {
  uint64_t hash = HashUtil::MurmurHash2_64(encoded_hash_columns.data(),
                                           encoded_hash_columns.length(),
                                           hash_dimension.seed);
  return hash % hash_dimension.num_buckets;
}

vector<Partition> PartitionSchema::GenerateHashPartitions(
    const HashSchema& hash_schema,
    const KeyEncoder<string>& hash_encoder) {
  vector<Partition> hash_partitions(1);
  // Create a partition for each hash bucket combination.
  for (const auto& hash_dimension : hash_schema) {
    vector<Partition> new_partitions;
    new_partitions.reserve(hash_partitions.size() * hash_dimension.num_buckets);
    // For each of the partitions created so far, replicate it
    // by the number of buckets in the next hash bucketing component.
    for (const Partition& base_partition : hash_partitions) {
      for (uint32_t bucket = 0; bucket < hash_dimension.num_buckets; ++bucket) {
        Partition partition = base_partition;
        partition.hash_buckets_.push_back(bucket);

        hash_encoder.Encode(&bucket, partition.begin_.mutable_hash_key());
        hash_encoder.Encode(&bucket, partition.end_.mutable_hash_key());

        new_partitions.emplace_back(std::move(partition));
      }
    }
    hash_partitions = std::move(new_partitions);
  }
  return hash_partitions;
}

bool PartitionSchema::RangePartitionContainsEncodedKey(
    const Partition& partition, const string& key) {
  // When all hash buckets match, then the row is contained in the partition
  // if the row's key is greater or equal to the lower bound, and if there is
  // either no upper bound or the row's key is less than the upper bound.
  return
      (partition.begin().range_key() <= key) &&
      (partition.end().range_key().empty() || key < partition.end().range_key());
}

Status PartitionSchema::ValidateHashSchema(const Schema& schema,
                                           const HashSchema& hash_schema) {
  set<ColumnId> hash_columns;
  for (const PartitionSchema::HashDimension& hash_dimension : hash_schema) {
    if (hash_dimension.num_buckets < 2) {
      return Status::InvalidArgument("must have at least two hash buckets");
    }

    if (hash_dimension.column_ids.empty()) {
      return Status::InvalidArgument("must have at least one hash column");
    }

    for (const ColumnId& hash_column : hash_dimension.column_ids) {
      if (!hash_columns.insert(hash_column).second) {
        return Status::InvalidArgument("hash bucket schema components must not "
                                       "contain columns in common");
      }
      int32_t column_idx = schema.find_column_by_id(hash_column);
      if (column_idx == Schema::kColumnNotFound) {
        return Status::InvalidArgument("must specify existing columns for hash "
                                       "bucket partition components");
      }
      if (column_idx >= schema.num_key_columns()) {
        return Status::InvalidArgument("must specify only primary key columns for "
                                       "hash bucket partition components");
      }
    }
  }
  return Status::OK();
}

template<typename Row>
uint32_t PartitionSchema::HashValueForRow(const Row& row,
                                          const HashDimension& hash_dimension) {
  string buf;
  EncodeColumns(row, hash_dimension.column_ids, &buf);
  uint64_t hash = HashUtil::MurmurHash2_64(
      buf.data(), buf.length(), hash_dimension.seed);
  return hash % hash_dimension.num_buckets;
}

//------------------------------------------------------------
// Template instantiations: We instantiate all possible templates to avoid linker issues.
// see: https://isocpp.org/wiki/faq/templates#separate-template-fn-defn-from-decl
//------------------------------------------------------------

template
uint32_t PartitionSchema::HashValueForRow(const KuduPartialRow& row,
                                          const HashDimension& hash_dimension);
template
uint32_t PartitionSchema::HashValueForRow(const ConstContiguousRow& row,
                                          const HashDimension& hash_dimension);

void PartitionSchema::Clear() {
  hash_schema_idx_by_encoded_range_start_.clear();
  ranges_with_custom_hash_schemas_.clear();
  hash_schema_.clear();
  range_schema_.column_ids.clear();
}

Status PartitionSchema::Validate(const Schema& schema) const {
  RETURN_NOT_OK(ValidateHashSchema(schema, hash_schema_));

  for (const auto& range_with_hash_schema : ranges_with_custom_hash_schemas_) {
    RETURN_NOT_OK(ValidateHashSchema(schema, range_with_hash_schema.hash_schema));
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

Status PartitionSchema::CheckRangeSchema(const Schema& schema) const {
  std::unordered_set<int> range_column_idxs;
  for (const ColumnId& column_id : range_schema_.column_ids) {
    const auto column_idx = schema.find_column_by_id(column_id);
    if (column_idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument(Substitute(
          "range partition column ID $0 not found in table schema", column_id));
    }
    if (!InsertIfNotPresent(&range_column_idxs, column_idx)) {
      return Status::InvalidArgument("duplicate column in range partition",
                                     schema.column(column_idx).name());
    }
  }
  return Status::OK();
}

void PartitionSchema::UpdatePartitionBoundaries(
    const KeyEncoder<string>& hash_encoder,
    vector<Partition>* partitions) const {
  for (size_t idx = 0; idx < partitions->size(); ++idx) {
    auto& p = (*partitions)[idx];
    UpdatePartitionBoundaries(
        hash_encoder, GetHashSchemaForRange(p.begin().range_key()), &p);
  }
}

void PartitionSchema::UpdatePartitionBoundaries(
    const KeyEncoder<string>& hash_encoder,
    const HashSchema& partition_hash_schema,
    Partition* partition) {
  if (partition_hash_schema.empty()) {
    // A simple short-circuit in case of an empty hash schema.
    return;
  }

  // Note: the following discussion and logic only takes effect when the table's
  // partition schema includes at least one hash dimension component, and the
  // absolute upper and/or absolute lower range bound is unbounded.
  //
  // At this point, we have the full set of partitions built up, but due
  // to the convention on the unbounded range keys and how PartitionKey
  // instances are compared, it's necessary to add an iota to the hash part
  // of the key for each unbounded range at the right end to keep proper
  // ordering of the ranges.
  //
  // It would be great to make the partition key space continuous
  // (i.e. without holes) by carrying over iotas to next hash dimension index,
  // but that would break the proper ordering of the ranges since the
  // introduction of range-specific hash schemas. For example, consider the
  // following partitioning:
  //   [-inf, 0) x 3 buckets x 3 buckets; [0, +inf) x 2 buckets x 2 buckets
  //
  // The original set of ranges looks like the following:
  //
  //  [ (0, 0,  ""), (0, 0, "0") )
  //  [ (0, 1,  ""), (0, 1, "0") )
  //  [ (0, 2,  ""), (0, 2, "0") )
  //  [ (1, 0,  ""), (1, 0, "0") )
  //  [ (1, 1,  ""), (1, 1, "0") )
  //  [ (1, 2,  ""), (1, 2, "0") )
  //  [ (2, 0,  ""), (2, 0, "0") )
  //  [ (2, 1,  ""), (2, 1, "0") )
  //  [ (2, 2,  ""), (2, 2, "0") )
  //
  //  [ (0, 0, "0"), (0, 0,  "") )
  //  [ (0, 1, "0"), (0, 1,  "") )
  //  [ (1, 0, "0"), (1, 0,  "") )
  //  [ (1, 1, "0"), (1, 1,  "") )
  //
  // Below is the list of the same partitions, updated and sorted:
  //  [ (0, 0,  ""), (0, 0, "0") )
  //  [ (0, 0, "0"), (0, 1,  "") )
  //  [ (0, 1,  ""), (0, 1, "0") )
  //  [ (0, 1, "0"), (0, 2,  "") )
  //  [ (0, 2,  ""), (0, 2, "0") )
  //
  //  [ (1, 0,  ""), (1, 0, "0") )
  //  [ (1, 0, "0"), (1, 1,  "") )
  //  [ (1, 1,  ""), (1, 1, "0") )
  //  [ (1, 1, "0"), (1, 2,  "") )
  //  [ (1, 2,  ""), (1, 2, "0") )
  //
  //  [ (2, 0,  ""), (2, 0, "0") )
  //  [ (2, 1,  ""), (2, 1, "0") )
  //  [ (2, 2,  ""), (2, 2, "0") )
  //
  // An alternate way of updating the hash components that tries to keep the
  // key space continuous would transform [ (0, 1, "0"), (0, 1,  "") ) into
  // [ (0, 1, "0"), (1, 0,  "") ), but that would put range
  // [ (0, 2,  ""), (0, 2, "0") ) inside the transformed one. That's not
  // consistent since it messes up the interval logic of the partition pruning
  // and the client's metacache: both are built under the assumption that
  // partition key ranges backed by different tablets do not intersect.
  DCHECK(partition);
  auto& p = *partition; // just a handy shortcut for the code below
  CHECK_EQ(partition_hash_schema.size(), p.hash_buckets().size());
  const auto hash_dimensions_num = static_cast<int>(p.hash_buckets().size());

  // Increment the hash bucket component of the last hash dimension for
  // each range that has the unbounded end range key.
  //
  // For example, the following is the result of the range end partition keys'
  // transformation, where the key is (hash_comp1, hash_comp2, range_comp) and
  // the number of hush buckets per each dimensions is 2:
  //   [ (0, 0, "") -> (0, 1, "") ]
  //   [ (0, 1, "") -> (0, 2, "") ]
  //   [ (1, 0, "") -> (1, 1, "") ]
  //   [ (1, 1, "") -> (1, 2, "") ]
  if (p.end().range_key().empty()) {
    const int dimension_idx = hash_dimensions_num - 1;
    DCHECK_GE(dimension_idx, 0);
    p.end_.mutable_hash_key()->erase(kEncodedBucketSize * dimension_idx);
    const int32_t bucket = p.hash_buckets()[dimension_idx] + 1;
    hash_encoder.Encode(&bucket, p.end_.mutable_hash_key());
  }
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
      case DATE:
        RETURN_NOT_OK(row->SetDate(idx, DataTypeTraits<DATE>::kMinValue + 1));
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
      case DATE: {
        int32_t value;
        RETURN_NOT_OK(row->GetDate(idx, &value));
        if (value < DataTypeTraits<DATE>::kMaxValue) {
          RETURN_NOT_OK(row->SetDate(idx, value + 1));
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

const PartitionSchema::HashSchema& PartitionSchema::GetHashSchemaForRange(
    const string& range_key) const {
  // Find proper hash bucket schema corresponding to the specified range key.
  const auto* idx = FindFloorOrNull(
      hash_schema_idx_by_encoded_range_start_, range_key);
  bool has_custom_range = (idx != nullptr);
  if (has_custom_range) {
    DCHECK_LT(*idx, ranges_with_custom_hash_schemas_.size());
    const auto& upper = ranges_with_custom_hash_schemas_[*idx].upper;
    if (!upper.empty() && upper <= range_key) {
      has_custom_range = false;
    }
  }
  return has_custom_range ? ranges_with_custom_hash_schemas_[*idx].hash_schema
                          : hash_schema_;
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

Status PartitionSchema::GetRangeSchemaColumnIndexes(
    const Schema& schema,
    vector<int32_t>* range_column_indexes) const {
  DCHECK(range_column_indexes);
  for (const ColumnId& column_id : range_schema_.column_ids) {
    int32_t idx = schema.find_column_by_id(column_id);
    if (idx == Schema::kColumnNotFound) {
      return Status::InvalidArgument(Substitute(
          "range partition column ID $0 not found in table schema", column_id));
    }
    range_column_indexes->push_back(idx);
  }
  return Status::OK();
}

Status PartitionSchema::GetHashSchemaForRange(const KuduPartialRow& lower,
                                              const Schema& schema,
                                              HashSchema* hash_schema) const {
  string lower_enc;
  RETURN_NOT_OK(EncodeRangeKey(lower, schema, &lower_enc));
  *hash_schema = GetHashSchemaForRange(lower_enc);
  return Status::OK();
}

Status PartitionSchema::DropRange(const KuduPartialRow& lower,
                                  const KuduPartialRow& upper,
                                  const Schema& schema) {
  string lower_enc;
  RETURN_NOT_OK(EncodeRangeKey(lower, schema, &lower_enc));
  const auto* idx_ptr = FindOrNull(
      hash_schema_idx_by_encoded_range_start_, lower_enc);
  if (!idx_ptr) {
    return Status::NotFound(Substitute(
        "'$0': range with specified lower bound not found",
        RangeKeyDebugString(lower_enc, schema)));
  }
  const auto dropped_range_idx = *idx_ptr;

  // Check for the upper range match.
  string upper_enc;
  RETURN_NOT_OK(EncodeRangeKey(upper, schema, &upper_enc));
  const auto& dropped_range_upper_enc =
      ranges_with_custom_hash_schemas_[dropped_range_idx].upper;
  if (dropped_range_upper_enc != upper_enc) {
    // Using Status::InvalidArgument() to distinguish between an absent record
    // for the range's lower bound and non-matching upper bound.
    return Status::InvalidArgument(Substitute(
        "range ['$0', '$1'): '$2' vs '$3': upper bound does not match",
        RangeKeyDebugString(lower_enc, schema),
        RangeKeyDebugString(upper_enc, schema),
        RangeKeyDebugString(dropped_range_upper_enc, schema),
        RangeKeyDebugString(upper_enc, schema)));
  }

  // Update the 'ranges_with_custom_hash_schemas_' array and the
  // 'hash_schema_idx_by_encoded_range_start_' helper map.
  const auto size = ranges_with_custom_hash_schemas_.size();
  DCHECK_GE(size, 1);
  decltype(hash_schema_idx_by_encoded_range_start_) updated_mapping;
  decltype(ranges_with_custom_hash_schemas_) updated_array;
  updated_array.reserve(size - 1);
  for (size_t idx = 0; idx < size; ++idx) {
    if (idx == dropped_range_idx) {
      continue;
    }
    updated_array.emplace_back(std::move(ranges_with_custom_hash_schemas_[idx]));
    const auto cur_idx = updated_array.size() - 1;
    InsertOrDie(&updated_mapping, updated_array[cur_idx].lower, cur_idx);
  }
  DCHECK_EQ(updated_array.size() + 1, size);
  DCHECK_EQ(updated_array.size(), updated_mapping.size());

  ranges_with_custom_hash_schemas_.swap(updated_array);
  hash_schema_idx_by_encoded_range_start_.swap(updated_mapping);

  return Status::OK();
}

} // namespace kudu
