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
#pragma once

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/common/schema.h"
#include "kudu/gutil/port.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace google {
namespace protobuf {
template <typename T> class RepeatedPtrField;
}  // namespace protobuf
}  // namespace google

namespace kudu {

namespace master {
class MasterTest_AlterTableAddAndDropRangeWithSpecificHashSchema_Test;
}

class Arena;
class ConstContiguousRow;
class KuduPartialRow;
class PartitionPB;
class PartitionSchemaPB;
class PartitionSchemaPB_HashBucketSchemaPB;
template <typename Buffer> class KeyEncoder;

// This class is a representation of a table's row (or a row limit) according
// to the table's partition schema. A table's partition can be defined by a pair
// of such PartitionKey objects: the beginning and the end of a partition. Rows
// that belong to a given partition all fall within the same partition key
// range, i.e. they are between the beginning and the end of the partition
// defined by two PartitionKey objects. Essentially, there is a way to serialize
// a subset of primary keys columns in a table's row given the table's partition
// schema, and PartitionKey consists of the hash and the range parts of that
// serialized representation.
class PartitionKey {
 public:
  // Build a PartitionKey that represents infinity. For the beginning of a
  // partition represents '-inf'; for the end of a partition represents '+inf'.
  PartitionKey() = default;

  // Build PartitionKey object given hash and range parts.
  PartitionKey(std::string hash_key, std::string range_key)
      : hash_key_(std::move(hash_key)),
        range_key_(std::move(range_key)) {
  }

  PartitionKey& operator=(const PartitionKey& other) {
    hash_key_ = other.hash_key_;
    range_key_ = other.range_key_;
    return *this;
  }

  bool operator==(const PartitionKey& other) const {
    return hash_key_ == other.hash_key_ && range_key_ == other.range_key_;
  }

  bool operator!=(const PartitionKey& other) const {
    return !(*this == other);
  }

  bool operator<(const PartitionKey& other) const {
    if (hash_key_ < other.hash_key_) {
      return true;
    }
    if (hash_key_ == other.hash_key_) {
      return range_key_ < other.range_key_;
    }
    return false;
  }

  bool operator<=(const PartitionKey& other) const {
    return !(*this > other);
  }

  bool operator>(const PartitionKey& other) const {
    return other < *this;
  }

  bool operator>=(const PartitionKey& other) const {
    return !(*this < other);
  }

  bool empty() const {
    return hash_key_.empty() && range_key_.empty();
  }

  const std::string& hash_key() const { return hash_key_; }
  const std::string& range_key() const { return range_key_; }

  std::string* mutable_hash_key() { return &hash_key_; }
  std::string* mutable_range_key() { return &range_key_; }

  // The serialized representation of this partition key. In legacy Kudu
  // versions, this string representation was used as a partition key.
  std::string ToString() const {
    return hash_key_ + range_key_;
  }

  // Return a string with hexadecimal representation of the data comprising
  // the key: useful for representing the key in human-readable format.
  std::string DebugString() const;

 private:
  std::string hash_key_;  // the hash part of the key, encoded
  std::string range_key_; // the range part of the key, encoded
};

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

  const PartitionKey& begin() const {
    return begin_;
  }

  const PartitionKey& end() const {
    return end_;
  }

  // Returns true iff the given partition 'rhs' is equivalent to this one.
  bool operator==(const Partition& rhs) const;
  bool operator!=(const Partition& rhs) const {
    return !(*this == rhs);
  }

  // Serializes a partition into a protobuf message.
  void ToPB(PartitionPB* pb) const;

  // Deserializes a protobuf message into a partition.
  //
  // The protobuf message is not validated, since partitions are only expected
  // to be created by the master process.
  static void FromPB(const PartitionPB& pb, Partition* partition);

  // Knowing the number of hash bucketing dimensions, it's possible to separate
  // the encoded hash-related part from the range-related part, so it's possible
  // to build corresponding PartitionKey object from a string where the hash-
  // and the range-related parts are concatenated.
  static PartitionKey StringToPartitionKey(const std::string& key_str,
                                           size_t hash_dimensions_num);
 private:
  friend class PartitionSchema;

  std::vector<int32_t> hash_buckets_;

  PartitionKey begin_;
  PartitionKey end_;
};

std::ostream& operator<<(std::ostream& out, const PartitionKey& key);

// A partition schema describes how the rows of a table are distributed among
// tablets.
//
// Primarily, a table's partition schema is responsible for translating the
// primary key column values of a row into a partition key that can be used to
// determine the tablet containing the key.
//
// The partition schema for a table is made up of a single range component, and
// per-range hash components, where a hash component consists of zero or more
// hash bucket dimensions. When all the ranges have the same hash component,
// the partition schema for a table reduces into a single range component and
// a table-wide hash component. The range component is called 'range schema',
// and the collection of per-range hash components is called 'hash schema'. With
// that, the partition schema for a table consists of the range schema and
// the hash schema.
//
// Each hash bucket dimension includes one or more columns from the set of the
// table's primary key columns, with the restriction that an individual primary
// key column may only be included in a single hash dimension.
//
// In encoded partition keys (they are represented as sequence of bytes),
// first comes the hash-related part, and then comes the range-related part.
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
  // This structure represents the range component of the table's partitioning
  // schema. It consists of at least one column, and every column must be one
  // of the primary key's columns.
  struct RangeSchema {
    std::vector<ColumnId> column_ids;
  };

  // This structure represents one dimension of the hash bucketing. To find the
  // hash value (which directly corresponds to the hash bucket index) for a
  // particular row in the given hash dimension, it's necessary to compute the
  // hash value for a row by calling
  // HashFunction(value_of_column_ids[0], ..., value_of_column_ids[N-1]), where
  // N = column_ids.size().
  //
  // NOTE: this structure corresponds to PartitionSchemaPB::HashBucketSchemaPB
  struct HashDimension {
    std::vector<ColumnId> column_ids;
    int32_t num_buckets;
    uint32_t seed;

    bool operator==(const HashDimension& other) const {
      if (this == &other) {
        return true;
      }
      if (seed != other.seed) {
        return false;
      }
      if (num_buckets != other.num_buckets) {
        return false;
      }
      if (column_ids != other.column_ids) {
        return false;
      }
      return true;
    }

    bool operator!=(const HashDimension& other) const {
      return !(*this == other);
    }
  };

  // A hash schema consists of zero or more hash dimensions. With that,
  // N-dimensional hash bucketing for a row is defined by N hash values computed
  // in each dimension of the hash schema.
  typedef std::vector<HashDimension> HashSchema;

  // A structure representing a range with custom hash schema.
  struct RangeWithHashSchema {
    std::string lower;      // encoded range key: lower boundary
    std::string upper;      // encoded range key: upper boundary
    HashSchema hash_schema; // hash schema for the range
  };
  typedef std::vector<RangeWithHashSchema> RangesWithHashSchemas;

  // Extracts HashSchema from a protobuf repeated field of hash buckets.
  static Status ExtractHashSchemaFromPB(
      const Schema& schema,
      const google::protobuf::RepeatedPtrField<PartitionSchemaPB_HashBucketSchemaPB>&
          hash_schema_pb,
      HashSchema* hash_schema);

  // Deserializes a protobuf message into a partition schema. If not nullptr,
  // the optional output parameter 'ranges_with_hash_schemas' is populated
  // with the information on table's ranges with their hash schemas.
  static Status FromPB(
      const PartitionSchemaPB& pb,
      const Schema& schema,
      PartitionSchema* partition_schema,
      RangesWithHashSchemas* ranges_with_hash_schemas = nullptr) WARN_UNUSED_RESULT;

  // Serializes a partition schema into a protobuf message.
  // Requires a schema to encode the range bounds.
  Status ToPB(const Schema& schema, PartitionSchemaPB* pb) const;

  // Returns partition key for the row.
  PartitionKey EncodeKey(const KuduPartialRow& row) const;
  PartitionKey EncodeKey(const ConstContiguousRow& row) const;

  // Creates the set of table partitions for a partition schema and collection
  // of split rows and split bounds.
  //
  // Split bounds define disjoint ranges for which tablets will be created. If
  // empty, then Kudu assumes a single unbounded range. Each split key must fall
  // into one of the ranges, and results in the range being split.  The number
  // of resulting partitions is the product of the number of hash buckets for
  // each hash bucket component, multiplied by
  // (split_rows.size() + max(1, range_bounds.size())).
  Status CreatePartitions(
      const std::vector<KuduPartialRow>& split_rows,
      const std::vector<std::pair<KuduPartialRow, KuduPartialRow>>& range_bounds,
      const Schema& schema,
      std::vector<Partition>* partitions) const WARN_UNUSED_RESULT;

  // Create the set of partitions given the specified ranges with per-range
  // hash schemas. The 'partitions' output parameter must be non-null.
  Status CreatePartitions(
      const RangesWithHashSchemas& ranges_with_hash_schemas,
      const Schema& schema,
      std::vector<Partition>* partitions) const WARN_UNUSED_RESULT;

  // Create the set of partitions for a single range with specified hash schema.
  Status CreatePartitionsForRange(
      const std::pair<KuduPartialRow, KuduPartialRow>& range_bound,
      const HashSchema& range_hash_schema,
      const Schema& schema,
      std::vector<Partition>* partitions) const WARN_UNUSED_RESULT;

  // Check if the given partition contains the specified row. The row must have
  // all the columns participating in the table's partition schema
  // set to particular values.
  bool PartitionContainsRow(const Partition& partition,
                            const KuduPartialRow& row) const;
  bool PartitionContainsRow(const Partition& partition,
                            const ConstContiguousRow& row) const;

  // Check if the specified row is probably in the given partition.
  // The collection of columns set to particular values in the row can be a
  // subset of all the columns participating in the table's partition schema.
  // This method can be used to optimize the set of values for IN list
  // predicates. As of now, this method is effectively implemented only for
  // single-column hash and single-column range partition schemas, meaning
  // that it can return false positives in case of other than single-row range
  // and hash schemas.
  //
  // NOTE: this method returns false positives in some cases (see above)
  //
  // TODO(aserbin): implement this for multi-row range schemas as well,
  //                substituting non-specified columns in the row with values
  //                from the partition's start key and return logically inverted
  //                result of calling PartitionContainsRow() with the
  //                artificially constructed row
  bool PartitionMayContainRow(const Partition& partition,
                              const KuduPartialRow& row) const;

  // Returns a text description of the partition suitable for debug printing.
  //
  // Partitions are considered metadata, so no redaction will happen on the hash
  // and range bound values.
  std::string PartitionDebugString(const Partition& partition, const Schema& schema) const;

  // Returns a text description of a partition key suitable for debug printing.
  std::string PartitionKeyDebugString(const PartitionKey& key,
                                      const Schema& schema) const;
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

  // Returns 'true' iff the partition schema 'rhs' is equivalent to this one.
  bool operator==(const PartitionSchema& rhs) const;

  // Returns 'true' iff the partition schema 'rhs' is not equivalent to this one.
  bool operator!=(const PartitionSchema& rhs) const {
    return !(*this == rhs);
  }

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

  // Decodes a range partition key into a partial row, with variable-length
  // fields stored in the arena.
  Status DecodeRangeKey(Slice* encoded_key,
                        KuduPartialRow* partial_row,
                        Arena* arena) const;

  const RangeSchema& range_schema() const {
    return range_schema_;
  }

  // TODO(aserbin): this method is becoming obsolete with the introduction of
  //                custom per-range hash schemas -- update this or remove
  //                completely
  const HashSchema& hash_schema() const {
    return hash_schema_;
  }

  const RangesWithHashSchemas& ranges_with_custom_hash_schemas() const {
    return ranges_with_custom_hash_schemas_;
  }

  bool HasCustomHashSchemas() const {
    return !ranges_with_custom_hash_schemas_.empty();
  }

  // Given the specified table schema, populate the 'range_column_indexes'
  // container with column indexes of the range partition keys.
  // If any of the columns is not in the key range columns then an
  // InvalidArgument status is returned.
  Status GetRangeSchemaColumnIndexes(
      const Schema& schema,
      std::vector<int>* range_column_indexes) const;

  Status GetHashSchemaForRange(const KuduPartialRow& lower,
                               const Schema& schema,
                               HashSchema* hash_schema) const;

  // Drop range partition with the specified lower and upper bounds. The
  // method updates member fields of this class, so that PartitionSchema::ToPB()
  // generates PartitionSchemaPB::custom_hash_schema_ranges field accordingly.
  Status DropRange(const KuduPartialRow& lower,
                   const KuduPartialRow& upper,
                   const Schema& schema);

 private:
  friend class PartitionPruner;
  friend class PartitionPrunerTest;
  FRIEND_TEST(master::MasterTest, AlterTableAddAndDropRangeWithSpecificHashSchema);
  FRIEND_TEST(PartitionTest, CustomHashSchemaRangesToPB);
  FRIEND_TEST(PartitionTest, DropRange);
  FRIEND_TEST(PartitionTest, HasCustomHashSchemasWhenAddingAndDroppingRanges);
  FRIEND_TEST(PartitionTest, TestPartitionSchemaPB);
  FRIEND_TEST(PartitionTest, TestIncrementRangePartitionBounds);
  FRIEND_TEST(PartitionTest, TestIncrementRangePartitionStringBounds);
  FRIEND_TEST(PartitionTest, TestVarcharRangePartitions);

  // Tests if the hash partition contains the row with given hash_idx.
  bool HashPartitionContainsRow(const Partition& partition,
                                const KuduPartialRow& row,
                                int hash_idx) const;

  // Tests if the range partition contains the row.
  bool RangePartitionContainsRow(const Partition& partition,
                                 const KuduPartialRow& row) const;

  // Returns a text description of the encoded range key suitable for debug printing.
  std::string RangeKeyDebugString(Slice range_key, const Schema& schema) const;
  std::string RangeKeyDebugString(const KuduPartialRow& key) const;
  std::string RangeKeyDebugString(const ConstContiguousRow& key) const;

  // Encodes the specified columns of a row into lexicographic sort-order
  // preserving format.
  static void EncodeColumns(const KuduPartialRow& row,
                            const std::vector<ColumnId>& column_ids,
                            std::string* buf);

  // Encodes the specified columns of a row into lexicographic sort-order
  // preserving format.
  static void EncodeColumns(const ConstContiguousRow& row,
                            const std::vector<ColumnId>& column_ids,
                            std::string* buf);

  // Returns the hash value of the encoded hash columns. The encoded columns
  // must match the columns of the hash dimension.
  static uint32_t HashValueForEncodedColumns(
      const std::string& encoded_hash_columns,
      const HashDimension& hash_dimension);

  // Assigns the row to a bucket according to the hash rules.
  template<typename Row>
  static uint32_t HashValueForRow(const Row& row,
                                  const HashDimension& hash_dimension);

  // Helper function that validates the hash schemas.
  static Status ValidateHashSchema(const Schema& schema,
                                   const HashSchema& hash_schema);

  // Generates hash partitions for each combination of hash buckets in hash_schemas.
  static std::vector<Partition> GenerateHashPartitions(
      const HashSchema& hash_schema,
      const KeyEncoder<std::string>& hash_encoder);

  // PartitionKeyDebugString implementation for row types.
  template<typename Row>
  std::string PartitionKeyDebugStringImpl(const Row& row) const;

  // Private templated helper for PartitionContainsRow.
  template<typename Row>
  bool PartitionContainsRowImpl(const Partition& partition,
                                const Row& row) const;

  // Private templated helper for HashPartitionContainsRow.
  template<typename Row>
  bool HashPartitionContainsRowImpl(const Partition& partition,
                                    const Row& row,
                                    const HashSchema& hash_schema,
                                    int hash_value) const;

  // Private templated helper for RangePartitionContainsRow.
  template<typename Row>
  bool RangePartitionContainsRowImpl(const Partition& partition,
                                     const Row& row) const;

  // Private templated helper for EncodeKey.
  template<typename Row>
  void EncodeKeyImpl(const Row& row,
                     std::string* range_buf,
                     std::string* hash_buf) const;

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

  // Returns the stringified hash and range schema components of the partition
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

  // Decodes the hash bucket component of a partition key into its buckets.
  //
  // This should only be called with partition keys created from a row, not with
  // partition keys from a partition.
  Status DecodeHashBuckets(Slice* encoded_key, std::vector<int32_t>* buckets) const;

  // Clears the state of this partition schema.
  void Clear();

  // Validates that this partition schema is valid. Returns OK, or an
  // appropriate error code for an invalid partition schema.
  Status Validate(const Schema& schema) const;

  // Check the range partition schema for consistency: make sure the columns
  // used in the range partitioning are present in the table's schema and there
  // aren't any duplicate columns.
  Status CheckRangeSchema(const Schema& schema) const;

  // Update partitions' boundaries provided with 'partitions' in-out parameter,
  // assuming the 'partitions' are populated by calling the CreatePartitions()
  // method. When a table has two or more hash components, there will be gaps
  // in between partitions at the boundaries of the component ranges. This
  // method fills in the address space to have the proper ordering of the
  // serialized partition keys -- that's important for partition pruning and
  // overall ordering of the serialized partition keys.
  void UpdatePartitionBoundaries(std::vector<Partition>* partitions) const;

  // Validates the split rows, converts them to partition key form, and inserts
  // them into splits in sorted order.
  Status EncodeRangeSplits(const std::vector<KuduPartialRow>& split_rows,
                           const Schema& schema,
                           std::vector<std::string>* splits) const;

  // Validates the range bounds, converts them to partition key form, and
  // inserts them into 'bounds_with_hash_schemas' in sorted order. The hash schemas
  // per range are stored within 'range_hash_schemas'. If 'range_hash_schemas' is empty,
  // it indicates that the table wide hash schema will be used per range.
  Status EncodeRangeBounds(
      const std::vector<std::pair<KuduPartialRow, KuduPartialRow>>& range_bounds,
      const std::vector<HashSchema>& range_hash_schemas,
      const Schema& schema,
      RangesWithHashSchemas* bounds_with_hash_schemas) const;

  // Splits the encoded range bounds by the split points. The splits and bounds within
  // 'bounds_with_hash_schemas' must be sorted. If `bounds_with_hash_schemas` is empty,
  // then a single unbounded range is assumed. If any of the splits falls outside
  // of the bounds, then an InvalidArgument status is returned.
  Status SplitRangeBounds(const Schema& schema,
                          const std::vector<std::string>& splits,
                          RangesWithHashSchemas* bounds_with_hash_schemas) const;

  // Increments a range partition key, setting 'increment' to true if the
  // increment succeeds, or false if all range partition columns are already the
  // maximum value. Unset columns will be incremented to increment(min_value).
  Status IncrementRangePartitionKey(KuduPartialRow* row, bool* increment) const;

  // Find hash schema for the given encoded range key. Depending on the
  // partition schema and the key, it might be either table-wide or a custom
  // hash schema for a particular range.
  const HashSchema& GetHashSchemaForRange(const std::string& range_key) const;

  RangeSchema range_schema_;
  HashSchema hash_schema_;

  // This contains only ranges with range-specific (i.e. different from
  // the table-wide) hash schemas.
  RangesWithHashSchemas ranges_with_custom_hash_schemas_;

  // Encoded start of the range --> index of the hash bucket schemas for the
  // range in the 'ranges_with_custom_hash_schemas_' array container.
  // NOTE: the contents of this map and 'ranges_with_custom_hash_schemas_'
  //       are tightly coupled -- it's necessary to clear/set this map
  //       along with 'ranges_with_custom_hash_schemas_'.
  typedef std::map<std::string, size_t> HashSchemasByEncodedLowerRange;
  HashSchemasByEncodedLowerRange hash_schema_idx_by_encoded_range_start_;
};

} // namespace kudu
