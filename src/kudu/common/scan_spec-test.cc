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

#include "kudu/common/scan_spec.h"

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::pair;
using std::string;
using std::vector;

namespace kudu {

namespace {
// Generate partition schema of a table with given hash_partitions and range partition keys.
// E.g. GeneratePartitionSchema(schema, {make_pair({a, b}, 3), make_pair({c}, 5) })
// Returns 'partition by hash(a, b) partitions 3, hash(c) partitions 5'.
void GeneratePartitionSchema(const Schema& schema,
                             const vector<pair<vector<string>, int>>& hash_partitions,
                             const vector<string>& range_partition_columns,
                             PartitionSchema* partition_schema) {
  PartitionSchemaPB partition_schema_pb;
  for (const auto& col_names_and_num_buckets : hash_partitions) {
    auto* hash_dimension_pb = partition_schema_pb.add_hash_schema();
    hash_dimension_pb->set_num_buckets(col_names_and_num_buckets.second);
    hash_dimension_pb->set_seed(0);
    for (const auto& col_name : col_names_and_num_buckets.first) {
      auto* column_pb = hash_dimension_pb->add_columns();
      int col_idx = schema.find_column(col_name);
      column_pb->set_id(col_idx);
      column_pb->set_name(col_name);
    }
  }
  if (!range_partition_columns.empty()) {
    auto* range_schema = partition_schema_pb.mutable_range_schema();
    for (const auto& range_column : range_partition_columns) {
      range_schema->add_columns()->set_name(range_column);
    }
  }
  CHECK_OK(PartitionSchema::FromPB(partition_schema_pb, schema, partition_schema));
}

// Copy a spec and return the pruned spec string.
string PruneInlistValuesAndGetSchemaString(const ScanSpec& spec,
                                           const Schema& schema,
                                           const Partition& partition,
                                           const PartitionSchema& partition_schema,
                                           Arena* arena) {
  ScanSpec copy_spec = spec;
  copy_spec.PruneInlistValuesIfPossible(schema, partition, partition_schema);
  copy_spec.OptimizeScan(schema, arena, true);

  return copy_spec.ToString(schema);
}

} // anonymous namespace

static string ToString(const vector<ColumnSchema>& columns) {
  string str;
  for (const auto& column : columns) {
    str += column.ToString();
    str += "\n";
  }
  return str;
}

class TestScanSpec : public KuduTest {
 public:
  explicit TestScanSpec(Schema s)
    : arena_(1024),
      schema_(std::move(s)) {
  }

  enum ComparisonOp {
    GE,
    EQ,
    LE,
    LT
  };

  template<class T>
  void AddPredicate(ScanSpec* spec, StringPiece col, ComparisonOp op, T val) {
    int idx = schema_.find_column(col);
    CHECK(idx != Schema::kColumnNotFound);

    void* val_void = arena_.AllocateBytes(sizeof(val));
    memcpy(val_void, &val, sizeof(val));

    switch (op) {
      case GE:
        spec->AddPredicate(ColumnPredicate::Range(schema_.column(idx), val_void, nullptr));
        break;
      case EQ:
        spec->AddPredicate(ColumnPredicate::Equality(schema_.column(idx), val_void));
        break;
      case LE: {
        auto p = ColumnPredicate::InclusiveRange(schema_.column(idx), nullptr, val_void, &arena_);
        if (p) spec->AddPredicate(*p);
        break;
      }
      case LT:
        spec->AddPredicate(ColumnPredicate::Range(schema_.column(idx), nullptr, val_void));
        break;
    }
  }

  template<class T>
  void AddInPredicate(ScanSpec* spec, StringPiece col, const vector<T>& values) {
    int idx = schema_.find_column(col);
    CHECK(idx != Schema::kColumnNotFound);

    vector<const void*> copied_values;
    for (const auto& val : values) {
      void* val_void = arena_.AllocateBytes(sizeof(val));
      memcpy(val_void, &val, sizeof(val));
      copied_values.push_back(val_void);
    }

    spec->AddPredicate(ColumnPredicate::InList(schema_.column(idx), &copied_values));
  }

  // Set the lower bound of the spec to the provided row. The row must outlive
  // the spec.
  void SetLowerBound(ScanSpec* spec, const KuduPartialRow& row) {
    CHECK(row.IsKeySet());
    ConstContiguousRow cont_row(row.schema(), row.row_data_);
    EncodedKey* enc_key = EncodedKey::FromContiguousRow(cont_row, &arena_);
    spec->SetLowerBoundKey(enc_key);
  }

  // Set the exclusive upper bound of the spec to the provided row. The row must
  // outlive the spec.
  void SetExclusiveUpperBound(ScanSpec* spec, const KuduPartialRow& row) {
    CHECK(row.IsKeySet());
    ConstContiguousRow cont_row(row.schema(), row.row_data_);
    EncodedKey* enc_key = EncodedKey::FromContiguousRow(cont_row, &arena_);
    spec->SetExclusiveUpperBoundKey(enc_key);
  }

 protected:
  Arena arena_;
  Schema schema_;
  ScanSpec spec_;
};

class CompositeIntKeysTest : public TestScanSpec {
 public:
  CompositeIntKeysTest() :
    TestScanSpec(
        Schema({ ColumnSchema("a", INT8),
                 ColumnSchema("b", INT8),
                 ColumnSchema("c", INT8),
                 ColumnSchema("d", INT8, true),
                 ColumnSchema("e", INT8) },
               3)) {
  }
};

// Test that multiple predicates on a column are collapsed.
TEST_F(CompositeIntKeysTest, TestSimplify) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 127);
  AddPredicate<int8_t>(&spec, "b", GE, 3);
  AddPredicate<int8_t>(&spec, "b", LE, 127);
  AddPredicate<int8_t>(&spec, "b", LE, 100);
  AddPredicate<int8_t>(&spec, "c", LE, 64);
  SCOPED_TRACE(spec.ToString(schema_));

  ASSERT_EQ(3, spec.predicates().size());
  ASSERT_EQ("a = 127", FindOrDie(spec.predicates(), "a").ToString());
  ASSERT_EQ("b >= 3 AND b < 101", FindOrDie(spec.predicates(), "b").ToString());
  ASSERT_EQ("c < 65", FindOrDie(spec.predicates(), "c").ToString());
}

// Predicate: a == 64
TEST_F(CompositeIntKeysTest, TestPrefixEquality) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 64);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);

  // Expect: key >= (64, -128, -128) AND key < (65, -128, -128)
  EXPECT_EQ("PK >= (int8 a=64, int8 b=-128, int8 c=-128) AND "
            "PK < (int8 a=65, int8 b=-128, int8 c=-128)",
            spec.ToString(schema_));
}

// Predicate: a <= 126
TEST_F(CompositeIntKeysTest, TestPrefixUpperBound) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", LE, 126);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK < (int8 a=127, int8 b=-128, int8 c=-128)", spec.ToString(schema_));
}

// Predicate: a >= 126
TEST_F(CompositeIntKeysTest, TestPrefixLowerBound) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", GE, 126);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=126, int8 b=-128, int8 c=-128)", spec.ToString(schema_));
}

// Predicates: a >= 3 AND b >= 4 AND c >= 5
TEST_F(CompositeIntKeysTest, TestConsecutiveLowerRangePredicates) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", GE, 3);
  AddPredicate<int8_t>(&spec, "b", GE, 4);
  AddPredicate<int8_t>(&spec, "c", GE, 5);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=3, int8 b=4, int8 c=5) AND b >= 4 AND c >= 5",
            spec.ToString(schema_));
}

// Predicates: a <= 3 AND b <= 4 AND c <= 5
TEST_F(CompositeIntKeysTest, TestConsecutiveUpperRangePredicates) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", LE, 3);
  AddPredicate<int8_t>(&spec, "b", LE, 4);
  AddPredicate<int8_t>(&spec, "c", LE, 5);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK < (int8 a=3, int8 b=4, int8 c=6) AND b < 5 AND c < 6",
            spec.ToString(schema_));
}

// Predicates: a = 3 AND b >= 4 AND c >= 5
TEST_F(CompositeIntKeysTest, TestEqualityAndConsecutiveLowerRangePredicates) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 3);
  AddPredicate<int8_t>(&spec, "b", GE, 4);
  AddPredicate<int8_t>(&spec, "c", GE, 5);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=3, int8 b=4, int8 c=5) AND "
            "PK < (int8 a=4, int8 b=-128, int8 c=-128) AND "
            "c >= 5", spec.ToString(schema_));
}

// Predicates: a = 3 AND 4 <= b <= 14 AND 15 <= c <= 15
TEST_F(CompositeIntKeysTest, TestEqualityAndConsecutiveRangePredicates) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 3);
  AddPredicate<int8_t>(&spec, "b", GE, 4);
  AddPredicate<int8_t>(&spec, "b", LE, 14);
  AddPredicate<int8_t>(&spec, "c", GE, 5);
  AddPredicate<int8_t>(&spec, "c", LE, 15);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=3, int8 b=4, int8 c=5) AND "
            "PK < (int8 a=3, int8 b=14, int8 c=16) AND "
            "c >= 5 AND c < 16", spec.ToString(schema_));
}

// Test a predicate on a non-prefix part of the key. Can't be pushed.
//
// Predicate: b == 64
TEST_F(CompositeIntKeysTest, TestNonPrefix) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "b", EQ, 64);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  // Expect: nothing pushed (predicate is still on b, not PK)
  EXPECT_EQ("b = 64", spec.ToString(schema_));
}

// Test what happens when an upper bound on a cell is equal to the maximum
// value for the cell. In this case, the preceding cell is also at the maximum
// value as well, so we eliminate the upper bound entirely.
//
// Predicate: a == 127 AND b >= 3 AND b <= 127
TEST_F(CompositeIntKeysTest, TestRedundantUpperBound) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 127);
  AddPredicate<int8_t>(&spec, "b", GE, 3);
  AddPredicate<int8_t>(&spec, "b", LE, 127);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=127, int8 b=3, int8 c=-128)", spec.ToString(schema_));
}

// A similar test, but in this case we still have an equality prefix
// that needs to be accounted for, so we can't eliminate the upper bound
// entirely.
//
// Predicate: a == 1 AND b >= 3 AND b < 127
TEST_F(CompositeIntKeysTest, TestRedundantUpperBound2) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 1);
  AddPredicate<int8_t>(&spec, "b", GE, 3);
  AddPredicate<int8_t>(&spec, "b", LE, 127);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=1, int8 b=3, int8 c=-128) AND "
            "PK < (int8 a=2, int8 b=-128, int8 c=-128)",
            spec.ToString(schema_));
}

// Test what happens with equality bounds on max value.
//
// Predicate: a == 127 AND b = 127
TEST_F(CompositeIntKeysTest, TestRedundantUpperBound3) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 127);
  AddPredicate<int8_t>(&spec, "b", EQ, 127);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=127, int8 b=127, int8 c=-128)",
            spec.ToString(schema_));
}

// Test that, if so desired, pushed predicates are not erased.
//
// Predicate: a == 126
TEST_F(CompositeIntKeysTest, TestNoErasePredicates) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 126);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, false);
  EXPECT_EQ("PK >= (int8 a=126, int8 b=-128, int8 c=-128) AND "
            "PK < (int8 a=127, int8 b=-128, int8 c=-128) AND "
            "a = 126", spec.ToString(schema_));
}

// Test that, if pushed predicates are erased, that we don't
// erase non-pushed predicates.
// Because we have no predicate on column 'b', we can't push a
// a range predicate that includes 'c'.
//
// Predicate: a == 126 AND c == 126
TEST_F(CompositeIntKeysTest, TestNoErasePredicates2) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 126);
  AddPredicate<int8_t>(&spec, "c", EQ, 126);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  // The predicate on column A should be pushed while "c" remains.
  EXPECT_EQ("PK >= (int8 a=126, int8 b=-128, int8 c=-128) AND "
            "PK < (int8 a=127, int8 b=-128, int8 c=-128) AND "
            "c = 126", spec.ToString(schema_));
}

// Test that predicates added out of key order are OK.
//
// Predicate: b == 126 AND a == 126
TEST_F(CompositeIntKeysTest, TestPredicateOrderDoesntMatter) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "b", EQ, 126);
  AddPredicate<int8_t>(&spec, "a", EQ, 126);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=126, int8 b=126, int8 c=-128) AND "
            "PK < (int8 a=126, int8 b=127, int8 c=-128)",
            spec.ToString(schema_));
}

// Test that IS NOT NULL predicates do *not* get filtered from non-nullable
// columns. This is a regression test for KUDU-1652, where previously attempting
// to push an IS NOT NULL predicate would cause a CHECK failure.
TEST_F(CompositeIntKeysTest, TestIsNotNullPushdown) {
  ScanSpec spec;
  spec.AddPredicate(ColumnPredicate::IsNotNull(schema_.column(0)));
  spec.AddPredicate(ColumnPredicate::IsNotNull(schema_.column(3)));
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("d IS NOT NULL", spec.ToString(schema_));
}

// Test that IN list predicates get pushed into the primary key bounds.
TEST_F(CompositeIntKeysTest, TestInListPushdown) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "a", { 0, 10 });
  AddInPredicate<int8_t>(&spec, "b", { 50, 100 });
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=10, int8 b=101, int8 c=-128) AND "
            "a IN (0, 10) AND b IN (50, 100)",
            spec.ToString(schema_));
}

// Test that hash(a) IN list predicates prune with right values.
TEST_F(CompositeIntKeysTest, OneHashKeyInListHashPruning) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "a", { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
  AddInPredicate<int8_t>(&spec, "b", { 50, 100 });

  const auto schema = schema_.CopyWithColumnIds();
  PartitionSchema partition_schema;
  GeneratePartitionSchema(schema, { { { "a" }, 3 } }, {}, &partition_schema);

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({}, {}, schema, &partitions));
  ASSERT_EQ(3, partitions.size());

  // Verify the splitted values can merge into original set without overlapping.
  ASSERT_EQ("PK >= (int8 a=4, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=101, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[0], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=101, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[1], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=101, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[2], partition_schema, &arena_));
}

// Test that hash(a), range(a) IN list predicates prune would happen on
// both hash and range aspects.
TEST_F(CompositeIntKeysTest, OneHashKeyOneRangeKeyInListHashPruning) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "a", { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
  AddInPredicate<int8_t>(&spec, "b", { 50, 100 });

  const auto schema = schema_.CopyWithColumnIds();
  PartitionSchema partition_schema;
  GeneratePartitionSchema(
      schema, { { { "a" }, 3 } }, { "a" }, &partition_schema);

  KuduPartialRow split1(&schema);
  KuduPartialRow split2(&schema);
  ASSERT_OK(split1.SetInt8("a", 3));
  ASSERT_OK(split2.SetInt8("a", 6));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(
      { split1, split2 }, {}, schema, &partitions));
  ASSERT_EQ(9, partitions.size());

  ASSERT_EQ("a IN () AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[0], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=4, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=4, int8 b=101, int8 c=-128) AND "
            "a IN (4) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[1], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=7, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=101, int8 c=-128) AND "
            "a IN (7, 8) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[2], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=2, int8 b=101, int8 c=-128) AND "
            "a IN (0, 2) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[3], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=5, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=5, int8 b=101, int8 c=-128) AND "
            "a IN (5) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[4], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=9, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=101, int8 c=-128) AND "
            "a IN (9) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[5], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=1, int8 b=101, int8 c=-128) AND "
            "a IN (1) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[6], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=3, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=3, int8 b=101, int8 c=-128) AND "
            "a IN (3) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[7], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=6, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=101, int8 c=-128) AND "
            "a IN (6) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[8], partition_schema, &arena_));
}

// Test that hash(a), range(a, b) IN list predicates prune would happen
// on hash-key but not on range key.
TEST_F(CompositeIntKeysTest, OneHashKeyMultiRangeKeyInListHashPruning) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "a", { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
  AddInPredicate<int8_t>(&spec, "b", { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

  const auto schema = schema_.CopyWithColumnIds();
  PartitionSchema partition_schema;
  GeneratePartitionSchema(
      schema, { { { "a" }, 3 } }, { "a", "b" }, &partition_schema);

  KuduPartialRow split1(&schema);
  KuduPartialRow split2(&schema);
  ASSERT_OK(split1.SetInt8("a", 2));
  ASSERT_OK(split1.SetInt8("b", 3));

  ASSERT_OK(split2.SetInt8("a", 6));
  ASSERT_OK(split2.SetInt8("b", 6));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(
      { split1, split2 }, {}, schema, &partitions));
  ASSERT_EQ(9, partitions.size());

  ASSERT_EQ("PK >= (int8 a=4, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=10, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[0], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=4, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=10, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[1], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=4, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=10, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[2], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=10, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[3], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=10, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[4], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=10, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[5], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=10, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[6], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=10, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[7], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=10, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[8], partition_schema, &arena_));
}

// Test that hash(a), range(b) IN list predicates prune would happen
// on both hash and range aspects.
TEST_F(CompositeIntKeysTest, DifferentHashRangeKeyInListHashPruning) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "a", { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
  AddInPredicate<int8_t>(&spec, "b", { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

  const auto schema = schema_.CopyWithColumnIds();
  PartitionSchema partition_schema;
  GeneratePartitionSchema(
      schema, { { { "a" }, 3 } }, { "b" }, &partition_schema);

  KuduPartialRow split1(&schema);
  KuduPartialRow split2(&schema);

  ASSERT_OK(split1.SetInt8("b", 3));
  ASSERT_OK(split2.SetInt8("b", 6));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(
      { split1, split2 }, {}, schema, &partitions));
  ASSERT_EQ(9, partitions.size());

  ASSERT_EQ("PK >= (int8 a=4, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=3, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (0, 1, 2)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[0], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=4, int8 b=3, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=6, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (3, 4, 5)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[1], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=4, int8 b=6, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=10, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[2], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=3, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (0, 1, 2)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[3], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=3, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=6, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (3, 4, 5)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[4], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=6, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=10, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[5], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=0, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=3, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (0, 1, 2)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[6], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=3, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=6, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (3, 4, 5)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[7], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=6, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=10, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (6, 7, 8, 9)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[8], partition_schema, &arena_));
}

// Test that in case hash(a) prune all predicate values, the rest predicate values for
// pruned in-list predicate should be detect correctly by CanShortCircuit().
// BTW, empty IN list predicates wouldn't result in the crash of OptimizeScan().
TEST_F(CompositeIntKeysTest, HashKeyInListHashPruningEmptyDetect) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "a", { 0, 2, 4, 5, 7, 8, 9 });
  AddInPredicate<int8_t>(&spec, "b", { 50, 100 });

  const auto schema = schema_.CopyWithColumnIds();
  PartitionSchema partition_schema;
  GeneratePartitionSchema(
      schema, { { { "a" }, 3 } }, {}, &partition_schema);

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({}, {}, schema, &partitions));
  ASSERT_EQ(3, partitions.size());

  ASSERT_EQ("PK >= (int8 a=4, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=101, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[0], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=101, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[1], partition_schema, &arena_));

  ASSERT_EQ("a IN () AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[2], partition_schema, &arena_));
}

// Test that hash(a), hash(b) IN list predicates should be pruned.
TEST_F(CompositeIntKeysTest, MultiHashKeyOneColumnInListHashPruning) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "a", { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
  AddInPredicate<int8_t>(&spec, "b", { 10, 20, 30, 40, 50, 60, 70, 80 });

  const auto schema = schema_.CopyWithColumnIds();
  PartitionSchema partition_schema;
  GeneratePartitionSchema(
      schema, { { { "a" }, 3 }, { { "b" }, 3 }, }, {}, &partition_schema);

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({}, {}, schema, &partitions));
  ASSERT_EQ(9, partitions.size());

  // p1, p2, p3 should have the same predicate values to be pushed on hash(a).
  // p1, p4, p7 should have the same predicate values to be pushed on hash(b).
  // pi refer to partitions[i-1], e.g. p1 = partitions[0]
  ASSERT_EQ("PK >= (int8 a=4, int8 b=40, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=71, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (40, 60, 70)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[0], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=4, int8 b=20, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=51, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (20, 30, 50)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[1], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=4, int8 b=10, int8 c=-128) AND "
            "PK < (int8 a=8, int8 b=81, int8 c=-128) AND "
            "a IN (4, 7, 8) AND b IN (10, 80)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[2], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=40, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=71, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (40, 60, 70)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[3], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=20, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=51, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (20, 30, 50)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[4], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=10, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=81, int8 c=-128) AND "
            "a IN (0, 2, 5, 9) AND b IN (10, 80)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[5], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=40, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=71, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (40, 60, 70)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[6], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=20, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=51, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (20, 30, 50)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[7], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=1, int8 b=10, int8 c=-128) AND "
            "PK < (int8 a=6, int8 b=81, int8 c=-128) AND "
            "a IN (1, 3, 6) AND b IN (10, 80)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[8], partition_schema, &arena_));
}

// Test that hash(a, b) IN list predicates should not be pruned.
TEST_F(CompositeIntKeysTest, MultiHashColumnsInListHashPruning) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "a", { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
  AddInPredicate<int8_t>(&spec, "b", { 50, 100 });

  const auto schema = schema_.CopyWithColumnIds();
  PartitionSchema partition_schema;
  GeneratePartitionSchema(
      schema, { { { "a", "b" }, 3 } }, {}, &partition_schema);

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({}, {}, schema, &partitions));
  ASSERT_EQ(3, partitions.size());

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=101, int8 c=-128) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[0], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=101, int8 c=-128) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[1], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=9, int8 b=101, int8 c=-128) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND b IN (50, 100)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[2], partition_schema, &arena_));
}

// Test that hash(a, b), hash(c) InList predicates.
// Neither a or b IN list can be pruned.
// c IN list should be pruned.
TEST_F(CompositeIntKeysTest, MultiHashKeyMultiHashInListHashPruning) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "a", { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
  AddInPredicate<int8_t>(&spec, "b", { 50, 100 });
  AddInPredicate<int8_t>(&spec, "c", { 20, 30, 40, 50, 60, 70, 80, 90 });

  const auto schema = schema_.CopyWithColumnIds();
  PartitionSchema partition_schema;
  GeneratePartitionSchema(
      schema, { { { "a", "b" }, 3 }, { { "c" }, 3 } }, {}, &partition_schema);

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({}, {}, schema, &partitions));
  ASSERT_EQ(9, partitions.size());

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=40) AND "
            "PK < (int8 a=9, int8 b=100, int8 c=71) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND "
            "b IN (50, 100) AND c IN (40, 60, 70)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[0], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=20) AND "
            "PK < (int8 a=9, int8 b=100, int8 c=51) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND "
            "b IN (50, 100) AND c IN (20, 30, 50)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[1], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=80) AND "
            "PK < (int8 a=9, int8 b=100, int8 c=91) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND "
            "b IN (50, 100) AND c IN (80, 90)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[2], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=40) AND "
            "PK < (int8 a=9, int8 b=100, int8 c=71) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND "
            "b IN (50, 100) AND c IN (40, 60, 70)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[3], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=20) AND "
            "PK < (int8 a=9, int8 b=100, int8 c=51) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND "
            "b IN (50, 100) AND c IN (20, 30, 50)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[4], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=80) AND "
            "PK < (int8 a=9, int8 b=100, int8 c=91) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND "
            "b IN (50, 100) AND c IN (80, 90)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[5], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=40) AND "
            "PK < (int8 a=9, int8 b=100, int8 c=71) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND "
            "b IN (50, 100) AND c IN (40, 60, 70)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[6], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=20) AND "
            "PK < (int8 a=9, int8 b=100, int8 c=51) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND "
            "b IN (50, 100) AND c IN (20, 30, 50)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[7], partition_schema, &arena_));

  ASSERT_EQ("PK >= (int8 a=0, int8 b=50, int8 c=80) AND "
            "PK < (int8 a=9, int8 b=100, int8 c=91) AND "
            "a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) AND "
            "b IN (50, 100) AND c IN (80, 90)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[8], partition_schema, &arena_));
}

// Of course, no pruning of IN list predicate's values for non-key columns.
TEST_F(CompositeIntKeysTest, NonKeyValuesInListHashPruning) {
  ScanSpec spec;
  AddInPredicate<int8_t>(&spec, "d", { 1, 2, 3, 4, 5 });

  const auto schema = schema_.CopyWithColumnIds();
  PartitionSchema partition_schema;
  GeneratePartitionSchema(
      schema, { { { "a" }, 3 } }, {}, &partition_schema);

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({}, {}, schema, &partitions));
  ASSERT_EQ(3, partitions.size());

  ASSERT_EQ("d IN (1, 2, 3, 4, 5)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[0], partition_schema, &arena_));

  ASSERT_EQ("d IN (1, 2, 3, 4, 5)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[1], partition_schema, &arena_));

  ASSERT_EQ("d IN (1, 2, 3, 4, 5)",
            PruneInlistValuesAndGetSchemaString(
                spec, schema, partitions[2], partition_schema, &arena_));
}

// Test that IN list mixed with range predicates get pushed into the primary key
// bounds.
TEST_F(CompositeIntKeysTest, TestInListPushdownWithRange) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", GE, 10);
  AddPredicate<int8_t>(&spec, "a", LE, 100);
  AddInPredicate<int8_t>(&spec, "b", { 50, 100 });
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=10, int8 b=50, int8 c=-128) AND "
            "PK < (int8 a=100, int8 b=101, int8 c=-128) AND "
            "b IN (50, 100)",
            spec.ToString(schema_));

  // Test redaction.
  ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));
  EXPECT_EQ("PK >= (int8 a=<redacted>, int8 b=<redacted>, int8 c=<redacted>) AND "
            "PK < (int8 a=<redacted>, int8 b=<redacted>, int8 c=<redacted>) AND "
            "b IN (<redacted>)",
            spec.ToString(schema_));
}

// Tests that a scan spec without primary key bounds will not have predicates
// after optimization.
TEST_F(CompositeIntKeysTest, TestLiftPrimaryKeyBounds_NoBounds) {
  ScanSpec spec;
  spec.OptimizeScan(schema_, &arena_, false);
  ASSERT_TRUE(spec.predicates().empty());
}

// Test that implicit constraints specified in the lower primary key bound are
// lifted into the predicates.
TEST_F(CompositeIntKeysTest, TestLiftPrimaryKeyBounds_LowerBound) {
  { // key >= (10, 11, 12)
    ScanSpec spec;

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", 11));
    CHECK_OK(lower_bound.SetInt8("c", 12));

    SetLowerBound(&spec, lower_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(1, spec.predicates().size());
    ASSERT_EQ("a >= 10", FindOrDie(spec.predicates(), "a").ToString());
  }
  { // key >= (10, 11, min)
    ScanSpec spec;

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", 11));
    CHECK_OK(lower_bound.SetInt8("c", INT8_MIN));

    SetLowerBound(&spec, lower_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(1, spec.predicates().size());
    ASSERT_EQ("a >= 10", FindOrDie(spec.predicates(), "a").ToString());
  }
  { // key >= (10, min, min)
    ScanSpec spec;

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", INT8_MIN));
    CHECK_OK(lower_bound.SetInt8("c", INT8_MIN));

    SetLowerBound(&spec, lower_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(1, spec.predicates().size());
    ASSERT_EQ("a >= 10", FindOrDie(spec.predicates(), "a").ToString());
  }
}

// Test that implicit constraints specified in the lower primary key bound are
// lifted into the predicates.
TEST_F(CompositeIntKeysTest, TestLiftPrimaryKeyBounds_UpperBound) {
  {
    // key < (10, 11, 12)
    ScanSpec spec;

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 10));
    CHECK_OK(upper_bound.SetInt8("b", 11));
    CHECK_OK(upper_bound.SetInt8("c", 12));

    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(1, spec.predicates().size());
    ASSERT_EQ("a < 11", FindOrDie(spec.predicates(), "a").ToString());
  }
  {
    // key < (10, 11, min)
    ScanSpec spec;

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 10));
    CHECK_OK(upper_bound.SetInt8("b", 11));
    CHECK_OK(upper_bound.SetInt8("c", INT8_MIN));

    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(1, spec.predicates().size());
    ASSERT_EQ("a < 11", FindOrDie(spec.predicates(), "a").ToString());
  }
  {
    // key < (10, min, min)
    ScanSpec spec;

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 10));
    CHECK_OK(upper_bound.SetInt8("b", INT8_MIN));
    CHECK_OK(upper_bound.SetInt8("c", INT8_MIN));

    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(1, spec.predicates().size());
    ASSERT_EQ("a < 10", FindOrDie(spec.predicates(), "a").ToString());
  }
}

// Test that implicit constraints specified in the primary key bounds are lifted
// into the predicates.
TEST_F(CompositeIntKeysTest, TestLiftPrimaryKeyBounds_BothBounds) {
  {
    // key >= (10, 11, 12)
    //      < (10, 11, 13)
    ScanSpec spec;

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", 11));
    CHECK_OK(lower_bound.SetInt8("c", 12));

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 10));
    CHECK_OK(upper_bound.SetInt8("b", 11));
    CHECK_OK(upper_bound.SetInt8("c", 13));

    SetLowerBound(&spec, lower_bound);
    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(3, spec.predicates().size());
    ASSERT_EQ("a = 10", FindOrDie(spec.predicates(), "a").ToString());
    ASSERT_EQ("b = 11", FindOrDie(spec.predicates(), "b").ToString());
    ASSERT_EQ("c = 12", FindOrDie(spec.predicates(), "c").ToString());
  }
  {
    // key >= (10, 11, 12)
    //      < (10, 11, 14)
    ScanSpec spec;

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", 11));
    CHECK_OK(lower_bound.SetInt8("c", 12));

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 10));
    CHECK_OK(upper_bound.SetInt8("b", 11));
    CHECK_OK(upper_bound.SetInt8("c", 14));

    SetLowerBound(&spec, lower_bound);
    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(3, spec.predicates().size());
    ASSERT_EQ("a = 10", FindOrDie(spec.predicates(), "a").ToString());
    ASSERT_EQ("b = 11", FindOrDie(spec.predicates(), "b").ToString());
    ASSERT_EQ("c >= 12 AND c < 14", FindOrDie(spec.predicates(), "c").ToString());
  }
  {
    // key >= (10, 11, 12)
    //      < (10, 12, min)
    ScanSpec spec;

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", 11));
    CHECK_OK(lower_bound.SetInt8("c", 12));

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 10));
    CHECK_OK(upper_bound.SetInt8("b", 12));
    CHECK_OK(upper_bound.SetInt8("c", INT8_MIN));

    SetLowerBound(&spec, lower_bound);
    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(3, spec.predicates().size());
    ASSERT_EQ("a = 10", FindOrDie(spec.predicates(), "a").ToString());
    ASSERT_EQ("b = 11", FindOrDie(spec.predicates(), "b").ToString());
    ASSERT_EQ("c >= 12", FindOrDie(spec.predicates(), "c").ToString());
  }
  {
    // key >= (10, 11, 12)
    //      < (10, 12, 13)
    ScanSpec spec;

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", 11));
    CHECK_OK(lower_bound.SetInt8("c", 12));

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 10));
    CHECK_OK(upper_bound.SetInt8("b", 12));
    CHECK_OK(upper_bound.SetInt8("c", 13));

    SetLowerBound(&spec, lower_bound);
    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(2, spec.predicates().size());
    ASSERT_EQ("a = 10", FindOrDie(spec.predicates(), "a").ToString());
    ASSERT_EQ("b >= 11 AND b < 13", FindOrDie(spec.predicates(), "b").ToString());
  }
  {
    // key >= (10, 11, 12)
    //      < (11, min, min)
    ScanSpec spec;

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", 11));
    CHECK_OK(lower_bound.SetInt8("c", 12));

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 11));
    CHECK_OK(upper_bound.SetInt8("b", INT8_MIN));
    CHECK_OK(upper_bound.SetInt8("c", INT8_MIN));

    SetLowerBound(&spec, lower_bound);
    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(2, spec.predicates().size());
    ASSERT_EQ("a = 10", FindOrDie(spec.predicates(), "a").ToString());
    ASSERT_EQ("b >= 11", FindOrDie(spec.predicates(), "b").ToString());
  }
  {
    // key >= (10, min, min)
    //      < (12, min, min)
    ScanSpec spec;

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", INT8_MIN));
    CHECK_OK(lower_bound.SetInt8("c", INT8_MIN));

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 12));
    CHECK_OK(upper_bound.SetInt8("b", INT8_MIN));
    CHECK_OK(upper_bound.SetInt8("c", INT8_MIN));

    SetLowerBound(&spec, lower_bound);
    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(1, spec.predicates().size());
    ASSERT_EQ("a >= 10 AND a < 12", FindOrDie(spec.predicates(), "a").ToString());
  }
}

// Test that implicit constraints specified in the primary key upper/lower
// bounds are merged into the set of predicates.
TEST_F(CompositeIntKeysTest, TestLiftPrimaryKeyBounds_WithPredicates) {
  {
    // b >= 15
    // c >= 3
    // c <= 100
    // key >= (10, min, min)
    //      < (10,  90, min)
    ScanSpec spec;
    AddPredicate<int8_t>(&spec, "b", GE, 15);
    AddPredicate<int8_t>(&spec, "c", GE, 3);
    AddPredicate<int8_t>(&spec, "c", LE, 100);

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", INT8_MIN));
    CHECK_OK(lower_bound.SetInt8("c", INT8_MIN));

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 10));
    CHECK_OK(upper_bound.SetInt8("b", 90));
    CHECK_OK(upper_bound.SetInt8("c", INT8_MIN));

    SetLowerBound(&spec, lower_bound);
    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(3, spec.predicates().size());
    ASSERT_EQ("a = 10", FindOrDie(spec.predicates(), "a").ToString());
    ASSERT_EQ("b >= 15 AND b < 90", FindOrDie(spec.predicates(), "b").ToString());
    ASSERT_EQ("c >= 3 AND c < 101", FindOrDie(spec.predicates(), "c").ToString());
  }
  {
    // b >= 15
    // c >= 3
    // c <= 100
    // key >= (10,  5, min)
    //      < (10, 10, min)
    ScanSpec spec;
    AddPredicate<int8_t>(&spec, "b", GE, 15);
    AddPredicate<int8_t>(&spec, "c", GE, 3);
    AddPredicate<int8_t>(&spec, "c", LE, 100);

    KuduPartialRow lower_bound(&schema_);
    CHECK_OK(lower_bound.SetInt8("a", 10));
    CHECK_OK(lower_bound.SetInt8("b", 5));
    CHECK_OK(lower_bound.SetInt8("c", INT8_MIN));

    KuduPartialRow upper_bound(&schema_);
    CHECK_OK(upper_bound.SetInt8("a", 10));
    CHECK_OK(upper_bound.SetInt8("b", 10));
    CHECK_OK(upper_bound.SetInt8("c", INT8_MIN));

    SetLowerBound(&spec, lower_bound);
    SetExclusiveUpperBound(&spec, upper_bound);

    spec.OptimizeScan(schema_, &arena_, false);
    ASSERT_EQ(3, spec.predicates().size());
    ASSERT_EQ("a = 10", FindOrDie(spec.predicates(), "a").ToString());
    ASSERT_EQ("b NONE", FindOrDie(spec.predicates(), "b").ToString());
    ASSERT_EQ("c >= 3 AND c < 101", FindOrDie(spec.predicates(), "c").ToString());
    ASSERT_EQ(true, spec.CanShortCircuit());
  }
}

// Predicates: a = 3 AND 4 <= b AND c IsNotNull And d IsNotNull.
TEST_F(CompositeIntKeysTest, TestGetMissingColumns) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", GE, 3);
  AddPredicate<int8_t>(&spec, "b", GE, 4);
  spec.AddPredicate(ColumnPredicate::IsNotNull(schema_.column(2)));
  spec.AddPredicate(ColumnPredicate::IsNotNull(schema_.column(3)));
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=3, int8 b=4, int8 c=-128) AND "
            "b >= 4 AND d IS NOT NULL", spec.ToString(schema_));
  EXPECT_EQ(2, spec.predicates().size());
  {
    // Projection: e.
    Schema projection({ ColumnSchema("e", INT8) }, 0);
    vector<ColumnSchema> missing_cols = spec.GetMissingColumns(projection);
    EXPECT_EQ(2, missing_cols.size());
    string missing_cols_str = ToString(missing_cols);
    EXPECT_STR_CONTAINS(missing_cols_str, "b INT8");
    EXPECT_STR_CONTAINS(missing_cols_str, "d INT8");
  }
  {
    // Projection: d e.
    Schema projection({ ColumnSchema("d", INT8, true),
                        ColumnSchema("e", INT8) }, 0);
    vector<ColumnSchema> missing_cols = spec.GetMissingColumns(projection);
    EXPECT_EQ(1, missing_cols.size());
    string missing_cols_str = ToString(missing_cols);
    EXPECT_STR_CONTAINS(missing_cols_str, "b INT8");
  }

  {
    // Projection: b d e.
    Schema projection({ ColumnSchema("b", INT8),
                        ColumnSchema("d", INT8, true),
                        ColumnSchema("e", INT8) }, 0);
    vector<ColumnSchema> missing_cols = spec.GetMissingColumns(projection);
    EXPECT_EQ(0, missing_cols.size());
  }
}

// Tests for String parts in composite keys
//------------------------------------------------------------
class CompositeIntStringKeysTest : public TestScanSpec {
 public:
  CompositeIntStringKeysTest() :
    TestScanSpec(
        Schema({ ColumnSchema("a", INT8),
                 ColumnSchema("b", STRING),
                 ColumnSchema("c", STRING) },
               3)) {
  }
};

// Predicate: a == 64
TEST_F(CompositeIntStringKeysTest, TestPrefixEquality) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 64);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  // Expect: key >= (64, "", "") AND key < (65, "", "")
  EXPECT_EQ(R"(PK >= (int8 a=64, string b="", string c="") AND )"
            R"(PK < (int8 a=65, string b="", string c=""))",
            spec.ToString(schema_));
}

// Predicate: a == 64 AND b = "abc"
TEST_F(CompositeIntStringKeysTest, TestPrefixEqualityWithString) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 64);
  AddPredicate<Slice>(&spec, "b", EQ, Slice("abc"));
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ(R"(PK >= (int8 a=64, string b="abc", string c="") AND )"
            R"(PK < (int8 a=64, string b="abc\000", string c=""))",
            spec.ToString(schema_));
}

TEST_F(CompositeIntStringKeysTest, TestDecreaseUpperBoundKey) {
  {
    ScanSpec spec;
    AddPredicate<int8_t>(&spec, "a", LT, 64);
    AddPredicate<Slice>(&spec, "b", LT, Slice("abc"));
    AddPredicate<Slice>(&spec, "c", LT, Slice("def\0", 4));
    SCOPED_TRACE(spec.ToString(schema_));
    spec.OptimizeScan(schema_, &arena_, true);
    EXPECT_EQ(R"(PK < (int8 a=63, string b="abc", string c="") AND )"
              R"(b < "abc" AND c < "def\000")",
              spec.ToString(schema_));
  }
  {
    ScanSpec spec;
    AddPredicate<int8_t>(&spec, "a", LT, 64);
    AddPredicate<Slice>(&spec, "b", LT, Slice("abc\0", 4));
    AddPredicate<Slice>(&spec, "c", LT, Slice("def"));
    SCOPED_TRACE(spec.ToString(schema_));
    spec.OptimizeScan(schema_, &arena_, true);
    EXPECT_EQ(R"(PK < (int8 a=63, string b="abc", string c="def") AND )"
              R"(b < "abc\000" AND c < "def")",
              spec.ToString(schema_));
  }
  {
    ScanSpec spec;
    AddPredicate<int8_t>(&spec, "a", LT, 64);
    AddPredicate<Slice>(&spec, "b", LT, Slice("abc\0", 4));
    AddPredicate<Slice>(&spec, "c", LT, Slice("def\0", 4));
    SCOPED_TRACE(spec.ToString(schema_));
    spec.OptimizeScan(schema_, &arena_, true);
    EXPECT_EQ(R"(PK < (int8 a=63, string b="abc", string c="def\000") AND )"
              R"(b < "abc\000" AND c < "def\000")",
              spec.ToString(schema_));
  }
  {
    ScanSpec spec;
    AddPredicate<int8_t>(&spec, "a", LT, 64);
    AddPredicate<Slice>(&spec, "b", LT, Slice("abc"));
    AddPredicate<Slice>(&spec, "c", LT, Slice("def"));
    SCOPED_TRACE(spec.ToString(schema_));
    spec.OptimizeScan(schema_, &arena_, true);
    EXPECT_EQ(R"(PK < (int8 a=63, string b="abc", string c="") AND )"
              R"(b < "abc" AND c < "def")",
              spec.ToString(schema_));
  }
}

// Tests for non-composite int key
//------------------------------------------------------------
class SingleIntKeyTest : public TestScanSpec {
 public:
  SingleIntKeyTest() :
    TestScanSpec(
        Schema({ ColumnSchema("a", INT8) }, 1)) {
    }
};

TEST_F(SingleIntKeyTest, TestEquality) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 64);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=64) AND PK < (int8 a=65)", spec.ToString(schema_));
}

TEST_F(SingleIntKeyTest, TestRedundantUpperBound) {
  ScanSpec spec;
  AddPredicate<int8_t>(&spec, "a", EQ, 127);
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("PK >= (int8 a=127)",
            spec.ToString(schema_));
}

TEST_F(SingleIntKeyTest, TestNoPredicates) {
  ScanSpec spec;
  SCOPED_TRACE(spec.ToString(schema_));
  spec.OptimizeScan(schema_, &arena_, true);
  EXPECT_EQ("", spec.ToString(schema_));
}

} // namespace kudu
