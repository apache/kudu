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
#ifndef KUDU_COMMON_PARTITION_H
#define KUDU_COMMON_PARTITION_H

#include <string>
#include <utility>
#include <vector>

#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

class ColumnRangePredicate;
class ConstContiguousRow;
class KuduPartialRow;
class PartitionSchemaPB;
class TypeInfo;

// A Partition describes the set of rows that a Tablet is responsible for
// serving. Each tablet is assigned a single Partition.
//
// Partitions consist primarily of a start and end partition key. Every row with
// a partition key that falls in a Tablet's Partition will be served by that
// tablet.
//
// In addition to the start and end partition keys, a Partition holds metadata
// to determine if a scan can prune, or skip, a partition based on the scan's
// start and end primary keys, and predicates.
class Partition {
 public:

  const std::vector<int32_t>& hash_buckets() const {
    return hash_buckets_;
  }

  Slice range_key_start() const;

  Slice range_key_end() const;

  const std::string& partition_key_start() const {
    return partition_key_start_;
  }

  const std::string& partition_key_end() const {
    return partition_key_end_;
  }

  // Returns true if the other partition is equivalent to this one.
  bool Equals(const Partition& other) const;

  // Serializes a partition into a protobuf message.
  void ToPB(PartitionPB* pb) const;

  // Deserializes a protobuf message into a partition.
  //
  // The protobuf message is not validated, since partitions are only expected
  // to be created by the master process.
  static void FromPB(const PartitionPB& pb, Partition* partition);

 private:
  friend class PartitionSchema;

  // Helper function for accessing the range key portion of a partition key.
  Slice range_key(const std::string& partition_key) const;

  std::vector<int32_t> hash_buckets_;

  std::string partition_key_start_;
  std::string partition_key_end_;
};

// A partition schema describes how the rows of a table are distributed among
// tablets.
//
// Primarily, a table's partition schema is responsible for translating the
// primary key column values of a row into a partition key that can be used to
// determine the tablet containing the key.
//
// The partition schema is made up of zero or more hash bucket components,
// followed by a single range component.
//
// Each hash bucket component includes one or more columns from the primary key
// column set, with the restriction that an individual primary key column may
// only be included in a single hash component.
//
// To determine the hash bucket of an individual row, the values of the columns
// of the hash component are encoded into bytes (in PK or lexicographic
// preserving encoding), then hashed into a u64, then modded into an i32. When
// constructing a partition key from a row, the buckets of the row are simply
// encoded into the partition key in order (again in PK or lexicographic
// preserving encoding).
//
// The range component contains a (possibly full or empty) subset of the primary
// key columns. When encoding the partition key, the columns of the partition
// component are encoded in order.
//
// The above is true of the relationship between rows and partition keys. It
// gets trickier with partitions (tablet partition key boundaries), because the
// boundaries of tablets do not necessarily align to rows. For instance,
// currently the absolute-start and absolute-end primary keys of a table
// represented as an empty key, but do not have a corresponding row. Partitions
// are similar, but instead of having just one absolute-start and absolute-end,
// each component of a partition schema has an absolute-start and absolute-end.
// When creating the initial set of partitions during table creation, we deal
// with this by "carrying through" absolute-start or absolute-ends into lower
// significance components.
//
// Notes on redaction:
//
// For the purposes of redaction, Kudu considers partitions and partition
// schemas to be metadata - not sensitive data which needs to be redacted from
// log files. However, the partition keys of individual rows _are_ considered
// sensitive, so we redact them from log messages and error messages. Thus,
// methods which format partitions and partition schemas will never redact, but
// the methods which format individual partition keys do redact.
class PartitionSchema {
 public:

  // Deserializes a protobuf message into a partition schema.
  static Status FromPB(const PartitionSchemaPB& pb,
                       const Schema& schema,
                       PartitionSchema* partition_schema) WARN_UNUSED_RESULT;

  // Serializes a partition schema into a protobuf message.
  void ToPB(PartitionSchemaPB* pb) const;

  // Appends the row's encoded partition key into the provided buffer.
  // On failure, the buffer may have data partially appended.
  Status EncodeKey(const KuduPartialRow& row, std::string* buf) const WARN_UNUSED_RESULT;
  Status EncodeKey(const ConstContiguousRow& row, std::string* buf) const WARN_UNUSED_RESULT;

  // Creates the set of table partitions for a partition schema and collection
  // of split rows and split bounds.
  //
  // Split bounds define disjoint ranges for which tablets will be created. If
  // empty, then Kudu assumes a single unbounded range. Each split key must fall
  // into one of the ranges, and results in the range being split.  The number
  // of resulting partitions is the product of the number of hash buckets for
  // each hash bucket component, multiplied by
  // (split_rows.size() + max(1, range_bounds.size())).
  Status CreatePartitions(const std::vector<KuduPartialRow>& split_rows,
                          const std::vector<std::pair<KuduPartialRow,
                                                      KuduPartialRow>>& range_bounds,
                          const Schema& schema,
                          std::vector<Partition>* partitions) const WARN_UNUSED_RESULT;

  // Tests if the partition contains the row.
  Status PartitionContainsRow(const Partition& partition,
                              const KuduPartialRow& row,
                              bool* contains) const WARN_UNUSED_RESULT;
  Status PartitionContainsRow(const Partition& partition,
                              const ConstContiguousRow& row,
                              bool* contains) const WARN_UNUSED_RESULT;

  // Returns a text description of the partition suitable for debug printing.
  //
  // Partitions are considered metadata, so no redaction will happen on the hash
  // and range bound values.
  std::string PartitionDebugString(const Partition& partition, const Schema& schema) const;

  // Returns a text description of a partition key suitable for debug printing.
  std::string PartitionKeyDebugString(Slice key, const Schema& schema) const;
  std::string PartitionKeyDebugString(const KuduPartialRow& row) const;
  std::string PartitionKeyDebugString(const ConstContiguousRow& row) const;

  // Returns a text description of the range partition with the provided
  // inclusive lower bound and exclusive upper bound.
  //
  // Range partitions are considered metadata, so no redaction will happen on
  // the row values.
  std::string RangePartitionDebugString(const KuduPartialRow& lower_bound,
                                        const KuduPartialRow& upper_bound) const;
  std::string RangePartitionDebugString(Slice lower_bound,
                                        Slice upper_bound,
                                        const Schema& schema) const;

  // Returns a text description of this partition schema suitable for debug printing.
  //
  // The partition schema is considered metadata, so partition bound information
  // is not redacted from the returned string.
  std::string DebugString(const Schema& schema) const;

  // Returns a text description of this partition schema suitable for display in the web UI.
  // The format of this string is not guaranteed to be identical cross-version.
  //
  // 'range_partitions' should include the set of range partitions in the table,
  // as formatted by 'RangePartitionDebugString'.
  std::string DisplayString(const Schema& schema,
                            const std::vector<std::string>& range_partitions) const;

  // Returns header and entry HTML cells for the partition schema for the master
  // table web UI. This is an abstraction leak, but it's better than leaking the
  // internals of partitions to the master path handlers.
  //
  // Partitions are considered metadata, so no redaction will be done.
  std::string PartitionTableHeader(const Schema& schema) const;
  std::string PartitionTableEntry(const Schema& schema, const Partition& partition) const;

  // Returns true if the other partition schema is equivalent to this one.
  bool Equals(const PartitionSchema& other) const;

  // Transforms an exclusive lower bound range partition key into an inclusive
  // lower bound range partition key.
  //
  // The provided partial row is considered metadata, so error messages may
  // contain unredacted row data.
  Status MakeLowerBoundRangePartitionKeyInclusive(KuduPartialRow* row) const;

  // Transforms an inclusive upper bound range partition key into an exclusive
  // upper bound range partition key.
  //
  // The provided partial row is considered metadata, so error messages may
  // contain unredacted row data.
  Status MakeUpperBoundRangePartitionKeyExclusive(KuduPartialRow* row) const;

 private:
  friend class PartitionPruner;
  FRIEND_TEST(PartitionTest, TestIncrementRangePartitionBounds);
  FRIEND_TEST(PartitionTest, TestIncrementRangePartitionStringBounds);

  struct RangeSchema {
    std::vector<ColumnId> column_ids;
  };

  struct HashBucketSchema {
    std::vector<ColumnId> column_ids;
    int32_t num_buckets;
    uint32_t seed;
  };

  // Returns a text description of the encoded range key suitable for debug printing.
  std::string RangeKeyDebugString(Slice range_key, const Schema& schema) const;
  std::string RangeKeyDebugString(const KuduPartialRow& key) const;
  std::string RangeKeyDebugString(const ConstContiguousRow& key) const;

  // Encodes the specified columns of a row into lexicographic sort-order
  // preserving format.
  static Status EncodeColumns(const KuduPartialRow& row,
                              const std::vector<ColumnId>& column_ids,
                              std::string* buf);

  // Encodes the specified columns of a row into lexicographic sort-order
  // preserving format.
  static Status EncodeColumns(const ConstContiguousRow& row,
                              const std::vector<ColumnId>& column_ids,
                              std::string* buf);

  // Returns the hash bucket of the encoded hash column. The encoded columns must match the
  // columns of the hash bucket schema.
  static int32_t BucketForEncodedColumns(const std::string& encoded_hash_columns,
                                         const HashBucketSchema& hash_bucket_schema);

  // Assigns the row to a hash bucket according to the hash schema.
  template<typename Row>
  static Status BucketForRow(const Row& row,
                             const HashBucketSchema& hash_bucket_schema,
                             int32_t* bucket);

  // PartitionKeyDebugString implementation for row types.
  template<typename Row>
  std::string PartitionKeyDebugStringImpl(const Row& row) const;

  // Private templated helper for PartitionContainsRow.
  template<typename Row>
  Status PartitionContainsRowImpl(const Partition& partition,
                                  const Row& row,
                                  bool* contains) const;

  // Private templated helper for EncodeKey.
  template<typename Row>
  Status EncodeKeyImpl(const Row& row, string* buf) const;

  // Returns true if all of the columns in the range partition key are unset in
  // the row.
  bool IsRangePartitionKeyEmpty(const KuduPartialRow& row) const;

  // Appends the stringified range partition components of a partial row to a
  // vector.
  //
  // If any columns of the range partition do not exist in the partial row, the
  // logical minimum value for that column will be used instead.
  void AppendRangeDebugStringComponentsOrMin(const KuduPartialRow& row,
                                             std::vector<std::string>* components) const;

  // Returns the stringified hash and range schema componenets of the partition
  // schema.
  //
  // Partition schemas are considered metadata, so no redaction will happen on
  // the hash and range bound values.
  std::vector<std::string> DebugStringComponents(const Schema& schema) const;

  // Encode the provided row into a range key. The row must not include values
  // for any columns not in the range key. Missing range values will be filled
  // with the logical minimum value for the column. A row without any values
  // will encode to an empty string.
  //
  // This method is useful used for encoding splits and bounds.
  Status EncodeRangeKey(const KuduPartialRow& row, const Schema& schema, std::string* key) const;

  // Decodes a range partition key into a partial row, with variable-length
  // fields stored in the arena.
  Status DecodeRangeKey(Slice* encode_key,
                        KuduPartialRow* partial_row,
                        Arena* arena) const;

  // Decodes the hash bucket component of a partition key into its buckets.
  //
  // This should only be called with partition keys created from a row, not with
  // partition keys from a partition.
  Status DecodeHashBuckets(Slice* partition_key, std::vector<int32_t>* buckets) const;

  // Clears the state of this partition schema.
  void Clear();

  // Validates that this partition schema is valid. Returns OK, or an
  // appropriate error code for an invalid partition schema.
  Status Validate(const Schema& schema) const;

  // Validates the split rows, converts them to partition key form, and inserts
  // them into splits in sorted order.
  Status EncodeRangeSplits(const std::vector<KuduPartialRow>& split_rows,
                           const Schema& schema,
                           std::vector<std::string>* splits) const;

  // Validates the range bounds, converts them to partition key form, and
  // inserts them into encoded_range_partitions in sorted order.
  Status EncodeRangeBounds(const std::vector<std::pair<KuduPartialRow,
                                                       KuduPartialRow>>& range_bounds,
                           const Schema& schema,
                           std::vector<std::pair<std::string,
                                                 std::string>>* encoded_range_bounds) const;

  // Splits the encoded range bounds by the split points. The splits and bounds
  // must be sorted. If `bounds` is empty, then a single unbounded range is
  // assumed. If any of the splits falls outside of the bounds then an
  // InvalidArgument status is returned.
  Status SplitRangeBounds(const Schema& schema,
                          std::vector<std::string> splits,
                          std::vector<std::pair<std::string, std::string>>* bounds) const;

  // Increments a range partition key, setting 'increment' to true if the
  // increment succeeds, or false if all range partition columns are already the
  // maximum value. Unset columns will be incremented to increment(min_value).
  Status IncrementRangePartitionKey(KuduPartialRow* row, bool* increment) const;

  std::vector<HashBucketSchema> hash_bucket_schemas_;
  RangeSchema range_schema_;
};

} // namespace kudu

#endif
