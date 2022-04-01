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

#include "kudu/common/partition_pruner.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/key_util.h"
#include "kudu/common/partition.h"
#include "kudu/common/row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/array_view.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"

using std::distance;
using std::find;
using std::iota;
using std::lower_bound;
using std::memcpy;
using std::move;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace {

// Returns true if the partition schema's range columns are a prefix of the
// primary key columns.
bool AreRangeColumnsPrefixOfPrimaryKey(const Schema& schema,
                                       const vector<ColumnId>& range_columns) {
  CHECK(range_columns.size() <= schema.num_key_columns());
  for (int32_t col_idx = 0; col_idx < range_columns.size(); ++col_idx) {
    if (schema.column_id(col_idx) != range_columns[col_idx]) {
      return false;
    }
  }
  return true;
}

// Translates the scan primary key bounds into range keys. This should only be
// used when the range columns are a prefix of the primary key columns.
void EncodeRangeKeysFromPrimaryKeyBounds(const Schema& schema,
                                         const ScanSpec& scan_spec,
                                         size_t num_range_columns,
                                         string* range_key_start,
                                         string* range_key_end) {
  if (scan_spec.lower_bound_key() == nullptr && scan_spec.exclusive_upper_bound_key() == nullptr) {
    // Don't bother if there are no lower and upper PK bounds
    return;
  }

  if (num_range_columns == schema.num_key_columns()) {
    // The range columns are the primary key columns, so the range key is the
    // primary key.
    if (scan_spec.lower_bound_key() != nullptr) {
      *range_key_start = scan_spec.lower_bound_key()->encoded_key().ToString();
    }
    if (scan_spec.exclusive_upper_bound_key() != nullptr) {
      *range_key_end = scan_spec.exclusive_upper_bound_key()->encoded_key().ToString();
    }
  } else {
    // The range-partition key columns are a prefix of the primary key columns.
    // Copy the column values over to a row, and then encode the row as a range
    // key.

    vector<int32_t> col_idxs(num_range_columns);
    iota(col_idxs.begin(), col_idxs.end(), 0);

    unique_ptr<uint8_t[]> buf(new uint8_t[schema.key_byte_size()]);
    ContiguousRow row(&schema, buf.get());

    if (scan_spec.lower_bound_key() != nullptr) {
      for (int32_t idx : col_idxs) {
        memcpy(row.mutable_cell_ptr(idx),
               scan_spec.lower_bound_key()->raw_keys()[idx],
               schema.column(idx).type_info()->size());
      }
      key_util::EncodeKey(col_idxs, row, range_key_start);
    }

    if (scan_spec.exclusive_upper_bound_key() != nullptr) {
      for (int32_t idx : col_idxs) {
        memcpy(row.mutable_cell_ptr(idx),
               scan_spec.exclusive_upper_bound_key()->raw_keys()[idx],
               schema.column(idx).type_info()->size());
      }

      // Determine if the upper bound primary key columns which aren't in the
      // range-partition key are all set to the minimum value. If so, the
      // range-partition key prefix of the primary key is already effectively an
      // exclusive bound. If not, then we increment the range-key prefix in
      // order to transform it from inclusive to exclusive.
      bool min_suffix = true;
      for (int32_t idx = num_range_columns; idx < schema.num_key_columns(); ++idx) {
        min_suffix &= schema.column(idx)
                            .type_info()
                            ->IsMinValue(scan_spec.exclusive_upper_bound_key()->raw_keys()[idx]);
      }
      Arena arena(std::max<size_t>(Arena::kMinimumChunkSize, schema.key_byte_size()));
      if (!min_suffix) {
        if (!key_util::IncrementPrimaryKey(&row, num_range_columns, &arena)) {
          // The range-partition key upper bound can't be incremented, which
          // means it's an inclusive bound on the maximum possible value, so
          // skip it.
          return;
        }
      }

      key_util::EncodeKey(col_idxs, row, range_key_end);
    }
  }
}

// Push the scan predicates into the range keys.
void EncodeRangeKeysFromPredicates(const Schema& schema,
                                   const unordered_map<string, ColumnPredicate>& predicates,
                                   const vector<ColumnId>& range_columns,
                                   string* range_key_start,
                                   string* range_key_end) {
  // Find the column indexes of the range columns.
  vector<int32_t> col_idxs;
  col_idxs.reserve(range_columns.size());
  for (const auto& column : range_columns) {
    int32_t col_idx = schema.find_column_by_id(column);
    CHECK_NE(Schema::kColumnNotFound, col_idx);
    CHECK_LT(col_idx, schema.num_key_columns());
    col_idxs.push_back(col_idx);
  }

  // Arenas must be at least the minimum chunk size, and we require at least
  // enough space for the range key columns.
  Arena arena(std::max<size_t>(Arena::kMinimumChunkSize, schema.key_byte_size()));
  uint8_t* buf = static_cast<uint8_t*>(CHECK_NOTNULL(arena.AllocateBytes(schema.key_byte_size())));
  ContiguousRow row(&schema, buf);

  if (key_util::PushLowerBoundKeyPredicates(col_idxs, predicates, &row, &arena) > 0) {
    key_util::EncodeKey(col_idxs, row, range_key_start);
  }

  if (key_util::PushUpperBoundKeyPredicates(col_idxs, predicates, &row, &arena) > 0) {
    key_util::EncodeKey(col_idxs, row, range_key_end);
  }
}
} // anonymous namespace

vector<bool> PartitionPruner::PruneHashComponent(
    const PartitionSchema::HashDimension& hash_dimension,
    const Schema& schema,
    const ScanSpec& scan_spec) {
  vector<bool> hash_bucket_bitset(hash_dimension.num_buckets, false);
  vector<string> encoded_strings(1, "");
  for (size_t col_offset = 0; col_offset < hash_dimension.column_ids.size(); ++col_offset) {
    vector<string> new_encoded_strings;
    const ColumnSchema& column = schema.column_by_id(hash_dimension.column_ids[col_offset]);
    const ColumnPredicate& predicate = FindOrDie(scan_spec.predicates(), column.name());
    const KeyEncoder<string>& encoder = GetKeyEncoder<string>(column.type_info());

    vector<const void*> predicate_values;
    if (predicate.predicate_type() == PredicateType::Equality) {
      predicate_values.push_back(predicate.raw_lower());
    } else {
      CHECK(predicate.predicate_type() == PredicateType::InList);
      predicate_values.insert(predicate_values.end(),
                              predicate.raw_values().begin(),
                              predicate.raw_values().end());
    }
    // For each of the encoded string, replicate it by the number of values in
    // equality and in-list predicate.
    for (const string& encoded_string : encoded_strings) {
      for (const void* predicate_value : predicate_values) {
        string new_encoded_string = encoded_string;
        encoder.Encode(predicate_value,
                       col_offset + 1 == hash_dimension.column_ids.size(),
                       &new_encoded_string);
        new_encoded_strings.emplace_back(new_encoded_string);
      }
    }
    encoded_strings.swap(new_encoded_strings);
  }
  for (const string& encoded_string : encoded_strings) {
    uint32_t hash_value = PartitionSchema::HashValueForEncodedColumns(
        encoded_string, hash_dimension);
    hash_bucket_bitset[hash_value] = true;
  }
  return hash_bucket_bitset;
}

vector<PartitionPruner::PartitionKeyRange> PartitionPruner::ConstructPartitionKeyRanges(
    const Schema& schema,
    const ScanSpec& scan_spec,
    const PartitionSchema::HashSchema& hash_schema,
    const RangeBounds& range_bounds) {
  // Create the hash bucket portion of the partition key.

  // The list of hash buckets bitset per hash component
  vector<vector<bool>> hash_bucket_bitsets;
  hash_bucket_bitsets.reserve(hash_schema.size());
  for (const auto& hash_dimension : hash_schema) {
    bool can_prune = true;
    for (const auto& column_id : hash_dimension.column_ids) {
      const ColumnSchema& column = schema.column_by_id(column_id);
      const ColumnPredicate* predicate = FindOrNull(scan_spec.predicates(), column.name());
      if (predicate == nullptr ||
          (predicate->predicate_type() != PredicateType::Equality &&
              predicate->predicate_type() != PredicateType::InList)) {
        can_prune = false;
        break;
      }
    }
    if (can_prune) {
      auto hash_bucket_bitset = PruneHashComponent(
          hash_dimension, schema, scan_spec);
      hash_bucket_bitsets.emplace_back(std::move(hash_bucket_bitset));
    } else {
      hash_bucket_bitsets.emplace_back(hash_dimension.num_buckets, true);
    }
  }

  // The index of the final constrained component in the partition key.
  size_t constrained_index;
  if (!range_bounds.lower.empty() || !range_bounds.upper.empty()) {
    // The range component is constrained.
    constrained_index = hash_schema.size();
  } else {
    // Search the hash bucket constraints from right to left, looking for the
    // first constrained component.
    constrained_index = hash_schema.size() -
        distance(hash_bucket_bitsets.rbegin(),
                 find_if(hash_bucket_bitsets.rbegin(),
                         hash_bucket_bitsets.rend(),
                         [] (const vector<bool>& x) {
                           return std::find(x.begin(), x.end(), false) != x.end();
                         }));
  }

  // Build up a set of partition key ranges out of the hash components.
  //
  // Each hash component simply appends its bucket number to the
  // partition key ranges (possibly incrementing the upper bound by one bucket
  // number if this is the final constraint, see note 2 in the example above).
  vector<PartitionKeyRange> partition_key_ranges(1);
  const KeyEncoder<string>& hash_encoder = GetKeyEncoder<string>(GetTypeInfo(UINT32));
  for (size_t hash_idx = 0; hash_idx < constrained_index; ++hash_idx) {
    // This is the final partition key component if this is the final constrained
    // bucket, and the range upper bound is empty. In this case we need to
    // increment the bucket on the upper bound to convert from inclusive to
    // exclusive.
    bool is_last = hash_idx + 1 == constrained_index && range_bounds.upper.empty();

    vector<PartitionKeyRange> new_partition_key_ranges;
    for (const auto& key_range : partition_key_ranges) {
      const vector<bool>& buckets_bitset = hash_bucket_bitsets[hash_idx];
      for (uint32_t bucket = 0; bucket < buckets_bitset.size(); ++bucket) {
        if (!buckets_bitset[bucket]) {
          continue;
        }
        uint32_t bucket_upper = is_last ? bucket + 1 : bucket;
        auto lower = key_range.start.hash_key();
        auto upper = key_range.end.hash_key();
        hash_encoder.Encode(&bucket, &lower);
        hash_encoder.Encode(&bucket_upper, &upper);
        new_partition_key_ranges.emplace_back(PartitionKeyRange{
            { lower, key_range.start.range_key() },
            { upper, key_range.end.range_key() }});
      }
    }
    partition_key_ranges.swap(new_partition_key_ranges);
  }

  // Append the (possibly empty) range bounds to the partition key ranges.
  for (auto& range : partition_key_ranges) {
    range.start.mutable_range_key()->append(range_bounds.lower);
    range.end.mutable_range_key()->append(range_bounds.upper);
  }

  // Remove all partition key ranges past the scan spec's upper bound partition key.
  const auto& upper_bound_partition_key = scan_spec.exclusive_upper_bound_partition_key();
  if (!upper_bound_partition_key.empty()) {
    for (auto range = partition_key_ranges.rbegin();
         range != partition_key_ranges.rend();
         ++range) {
      if (!(*range).end.empty() && upper_bound_partition_key >= (*range).end) {
        break;
      }
      if (upper_bound_partition_key <= (*range).start) {
        partition_key_ranges.pop_back();
      } else {
        (*range).end = upper_bound_partition_key;
      }
    }
  }

  return partition_key_ranges;
}

// NOTE: the lower ranges are inclusive, the upper ranges are exclusive.
void PartitionPruner::PrepareRangeSet(
    const string& scan_lower_bound,
    const string& scan_upper_bound,
    const PartitionSchema::HashSchema& table_wide_hash_schema,
    const PartitionSchema::RangesWithHashSchemas& ranges,
    PartitionSchema::RangesWithHashSchemas* result_ranges) {
  DCHECK(result_ranges);
  CHECK(scan_upper_bound.empty() || scan_lower_bound < scan_upper_bound);

  // If there aren't any ranges with custom hash schemas or there isn't an
  // intersection between the set of ranges with custom hash schemas and the
  // scan range, the result is trivial: the whole scan range is attributed
  // to the table-wide hash schema.
  if (ranges.empty() ||
      (!scan_upper_bound.empty() && scan_upper_bound < ranges.front().lower) ||
      (!scan_lower_bound.empty() && !ranges.back().upper.empty() &&
           ranges.back().upper <= scan_lower_bound)) {
    *result_ranges =
        { { scan_lower_bound, scan_upper_bound, table_wide_hash_schema } };
    return;
  }

  // Find the first range that is at or after the specified bounds.
  const auto range_it = std::lower_bound(ranges.cbegin(), ranges.cend(),
      RangeBounds{scan_lower_bound, scan_upper_bound},
      [] (const PartitionSchema::RangeWithHashSchema& range,
          const RangeBounds& bounds) {
        // return true if range < bounds
        return !range.upper.empty() && range.upper <= bounds.lower;
      });
  CHECK(range_it != ranges.cend());

  // Current position of the iterator.
  string cur_point = scan_lower_bound;
  // Index of the known range with custom hash schema that the iterator is
  // currently pointing at or about to point if the iterator is currently
  // at the scan boundary.
  size_t cur_idx = distance(ranges.begin(), range_it);

  CHECK_LT(cur_idx, ranges.size());

  // Iterate over the scan range from one known boundary to the next one,
  // enumerating the resulting consecutive sub-ranges and attributing each
  // sub-range to a proper hash schema. If that's a known range with custom hash
  // schema, it's attributed to its range-specific hash schema; otherwise,
  // a sub-range is attributed to the table-wide hash schema.
  PartitionSchema::RangesWithHashSchemas result;
  while (cur_idx < ranges.size() &&
         (cur_point < scan_upper_bound || scan_upper_bound.empty())) {
    // Check the disposition of cur_point related to the lower boundary
    // of the range pointed to by 'cur_idx'.
    const auto& cur_range = ranges[cur_idx];
    if (cur_point < cur_range.lower) {
      // The iterator is before the current range:
      //     |---|
      //   ^
      // The next known bound is either the upper bound of the current range
      // or the upper bound of the scan.
      auto upper_bound = scan_upper_bound.empty()
          ? cur_range.lower : std::min(cur_range.lower, scan_upper_bound);
      result.emplace_back(PartitionSchema::RangeWithHashSchema{
          cur_point, upper_bound, table_wide_hash_schema});
      // Not advancing the 'cur_idx' since cur_point is either at the beginning
      // of the range or before it at the upper bound of the scan.
    } else if (cur_point == cur_range.lower) {
      // The iterator is at the lower boundary of the current range:
      //   |---|
      //   ^
      if ((!cur_range.upper.empty() && cur_range.upper <= scan_upper_bound) ||
          scan_upper_bound.empty()) {
        // The current range is within the scan boundaries.
        result.emplace_back(cur_range);
      } else {
        // The current range spans over the upper bound of the scan.
        result.emplace_back(PartitionSchema::RangeWithHashSchema{
            cur_point, scan_upper_bound, cur_range.hash_schema});
      }
      // Done with the current range, advance to the next one, if any.
      ++cur_idx;
    } else {
      // The iterator is ahead of the current range's lower boundary:
      //   |---|
      //     ^
      if ((!scan_upper_bound.empty() && scan_upper_bound <= cur_range.upper) ||
          cur_range.upper.empty()) {
        result.emplace_back(PartitionSchema::RangeWithHashSchema{
            cur_point, scan_upper_bound, cur_range.hash_schema});
      } else {
        result.emplace_back(PartitionSchema::RangeWithHashSchema{
            cur_point, cur_range.upper, cur_range.hash_schema});
      }
      // Done with the current range, advance to the next one, if any.
      ++cur_idx;
    }
    // Advance the iterator.
    cur_point = result.back().upper;
  }

  // If exiting from the cycle above by the 'cur_idx < ranges.size()' condition,
  // check if the upper bound of the scan is beyond the upper bound of the last
  // range with custom hash schema. If so, add an extra range that spans from
  // the upper bound of the last range to the upper bound of the scan.
  if (result.back().upper != scan_upper_bound) {
    DCHECK_EQ(cur_point, result.back().upper);
    result.emplace_back(PartitionSchema::RangeWithHashSchema{
        cur_point, scan_upper_bound, table_wide_hash_schema});
  }

  *result_ranges = std::move(result);
}

void PartitionPruner::Init(const Schema& schema,
                           const PartitionSchema& partition_schema,
                           const ScanSpec& scan_spec) {
  // If we can already short circuit the scan, we don't need to bother with
  // partition pruning. This also allows us to assume some invariants of the
  // scan spec, such as no None predicates and that the lower bound PK < upper
  // bound PK.
  if (scan_spec.CanShortCircuit()) { return; }

  // Build a set of partition key ranges which cover the tablets necessary for
  // the scan.
  //
  // Example predicate sets and resulting partition key ranges, based on the
  // following tablet schema:
  //
  // CREATE TABLE t (a INT32, b INT32, c INT32) PRIMARY KEY (a, b, c)
  // DISTRIBUTE BY RANGE (c)
  //               HASH (a) INTO 2 BUCKETS
  //               HASH (b) INTO 3 BUCKETS;
  //
  // Assume that hash(0) = 0 and hash(2) = 2.
  //
  // | Predicates | Partition Key Ranges                                   |
  // +------------+--------------------------------------------------------+
  // | a = 0      | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
  // | b = 2      |                                                        |
  // | c = 0      |                                                        |
  // +------------+--------------------------------------------------------+
  // | a = 0      | [(bucket=0, bucket=2), (bucket=0, bucket=3))           |
  // | b = 2      |                                                        |
  // +------------+--------------------------------------------------------+
  // | a = 0      | [(bucket=0, bucket=0, c=0), (bucket=0, bucket=0, c=1)) |
  // | c = 0      | [(bucket=0, bucket=1, c=0), (bucket=0, bucket=1, c=1)) |
  // |            | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
  // +------------+--------------------------------------------------------+
  // | b = 2      | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
  // | c = 0      | [(bucket=1, bucket=2, c=0), (bucket=1, bucket=2, c=1)) |
  // +------------+--------------------------------------------------------+
  // | a = 0      | [(bucket=0), (bucket=1))                               |
  // +------------+--------------------------------------------------------+
  // | b = 2      | [(bucket=0, bucket=2), (bucket=0, bucket=3))           |
  // |            | [(bucket=1, bucket=2), (bucket=1, bucket=3))           |
  // +------------+--------------------------------------------------------+
  // | c = 0      | [(bucket=0, bucket=0, c=0), (bucket=0, bucket=0, c=1)) |
  // |            | [(bucket=0, bucket=1, c=0), (bucket=0, bucket=1, c=1)) |
  // |            | [(bucket=0, bucket=2, c=0), (bucket=0, bucket=2, c=1)) |
  // |            | [(bucket=1, bucket=0, c=0), (bucket=1, bucket=0, c=1)) |
  // |            | [(bucket=1, bucket=1, c=0), (bucket=1, bucket=1, c=1)) |
  // |            | [(bucket=1, bucket=2, c=0), (bucket=1, bucket=2, c=1)) |
  // +------------+--------------------------------------------------------+
  // | None       | [(), ())                                               |
  //
  // If the partition key is considered as a sequence of the hash bucket
  // components and a range component, then a few patterns emerge from the
  // examples above:
  //
  // 1) The partition keys are truncated after the final constrained component.
  //    Hash bucket components are constrained when the scan is limited to a
  //    subset of buckets via equality or in-list predicates on that component.
  //    Range components are constrained if they have an upper or lower bound
  //    via range or equality predicates on that component.
  //
  // 2) If the final constrained component is a hash bucket, then the
  //    corresponding bucket in the upper bound is incremented in order to make
  //    it an exclusive key.
  //
  // 3) The number of partition key ranges in the result is equal to the product
  //    of the number of buckets of each unconstrained hash component which come
  //    before a final constrained component. If there are no unconstrained hash
  //    components, then the number of resulting partition key ranges is one. Note
  //    that this can be a lot of ranges, and we may find we need to limit the
  //    algorithm to give up on pruning if the number of ranges exceeds a limit.
  //    Until this becomes a problem in practice, we'll continue always pruning,
  //    since it is precisely these highly-hash-partitioned tables which get the
  //    most benefit from pruning.

  // Build the range portion of the partition key by using
  // the lower and upper bounds specified by the scan.
  string scan_range_lower_bound;
  string scan_range_upper_bound;
  const vector<ColumnId>& range_columns = partition_schema.range_schema_.column_ids;
  if (!range_columns.empty()) {
    if (AreRangeColumnsPrefixOfPrimaryKey(schema, range_columns)) {
      EncodeRangeKeysFromPrimaryKeyBounds(schema,
                                          scan_spec,
                                          range_columns.size(),
                                          &scan_range_lower_bound,
                                          &scan_range_upper_bound);
    } else {
      EncodeRangeKeysFromPredicates(schema,
                                    scan_spec.predicates(),
                                    range_columns,
                                    &scan_range_lower_bound,
                                    &scan_range_upper_bound);
    }
  }

  if (partition_schema.ranges_with_custom_hash_schemas().empty()) {
    auto partition_key_ranges = ConstructPartitionKeyRanges(
        schema, scan_spec, partition_schema.hash_schema_,
        {scan_range_lower_bound, scan_range_upper_bound});
    partition_key_ranges_.resize(partition_key_ranges.size());
    move(partition_key_ranges.rbegin(), partition_key_ranges.rend(),
         partition_key_ranges_.begin());
  } else {
    // Build the preliminary set of ranges: that's to convey information on
    // range-specific hash schemas since some ranges in the table can have
    // custom (i.e. different from the table-wide) hash schemas.
    PartitionSchema::RangesWithHashSchemas preliminary_ranges;
    PartitionPruner::PrepareRangeSet(
        scan_range_lower_bound,
        scan_range_upper_bound,
        partition_schema.hash_schema(),
        partition_schema.ranges_with_custom_hash_schemas(),
        &preliminary_ranges);

    // Construct partition key ranges from the ranges and their respective hash
    // schemas that falls within the scan's bounds.
    for (size_t i = 0; i < preliminary_ranges.size(); ++i) {
      const auto& hash_schema = preliminary_ranges[i].hash_schema;
      RangeBounds range_bounds {preliminary_ranges[i].lower, preliminary_ranges[i].upper};
      auto partition_key_ranges = ConstructPartitionKeyRanges(
          schema, scan_spec, hash_schema, range_bounds);
      partition_key_ranges_.resize(partition_key_ranges_.size() + partition_key_ranges.size());
      move(partition_key_ranges.begin(), partition_key_ranges.end(),
           partition_key_ranges_.rbegin());
    }
    // Reverse the order of the partition key ranges, so that it is efficient
    // to remove the partition key ranges from the vector in ascending order.
    constexpr struct {
      bool operator()(const PartitionKeyRange& lhs, const PartitionKeyRange& rhs) const {
        return lhs.start > rhs.start;
      }
    } PartitionKeyRangeLess;
    sort(partition_key_ranges_.begin(),
         partition_key_ranges_.end(),
         PartitionKeyRangeLess);
  }

  // Remove all partition key ranges before the scan spec's lower bound partition key.
  if (!scan_spec.lower_bound_partition_key().empty()) {
    RemovePartitionKeyRange(scan_spec.lower_bound_partition_key());
  }

}
bool PartitionPruner::HasMorePartitionKeyRanges() const {
  return NumRangesRemaining() != 0;
}

const PartitionKey& PartitionPruner::NextPartitionKey() const {
  CHECK(HasMorePartitionKeyRanges());
  return partition_key_ranges_.back().start;
}

void PartitionPruner::RemovePartitionKeyRange(const PartitionKey& upper_bound) {
  if (upper_bound.empty()) {
    partition_key_ranges_.clear();
    return;
  }

  for (auto range_it = partition_key_ranges_.rbegin();
       range_it != partition_key_ranges_.rend();
       ++range_it) {
    if (upper_bound <= (*range_it).start) { break; }
    // Condition met if upper_bound lies in the middle of current partition key range
    if ((*range_it).end.empty() || upper_bound < (*range_it).end) {
       (*range_it).start = upper_bound;
    } else {
      partition_key_ranges_.pop_back();
    }
  }
}

bool PartitionPruner::ShouldPrune(const Partition& partition) const {
  // range is an iterator that points to the first partition key range which
  // overlaps or is greater than the partition.
  auto range = lower_bound(partition_key_ranges_.rbegin(), partition_key_ranges_.rend(), partition,
                           [] (const PartitionKeyRange& scan_range, const Partition& partition) {
    // return true if scan_range < partition
    const auto& scan_upper = scan_range.end;
    return !scan_upper.empty() && scan_upper <= partition.begin();
  });
  if (range == partition_key_ranges_.rend()) {
    return true;
  }
  return !(partition.end().empty() || partition.end() > (*range).start);
}

string PartitionPruner::ToString(const Schema& schema,
                                 const PartitionSchema& partition_schema) const {
  vector<string> strings;
  for (auto range = partition_key_ranges_.rbegin();
       range != partition_key_ranges_.rend();
       ++range) {
    strings.push_back(strings::Substitute(
        "[($0), ($1))",
        (*range).start.empty() ? "<start>" :
          partition_schema.PartitionKeyDebugString((*range).start, schema),
          (*range).end.empty() ? "<end>" :
          partition_schema.PartitionKeyDebugString((*range).end, schema)));
  }

  return JoinStrings(strings, ", ");
}

} // namespace kudu
