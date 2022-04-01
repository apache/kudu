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
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::count_if;
using std::get;
using std::make_tuple;
using std::nullopt;
using std::optional;
using std::ostream;
using std::pair;
using std::string;
using std::tuple;
using std::vector;
using strings::Substitute;

namespace kudu {

class PartitionPrunerTest : public KuduTest {
 public:
  typedef tuple<vector<string>, int32_t, uint32_t> ColumnNamesNumBucketsAndSeed;
  typedef pair<string, string> ColumnNameAndStringValue;
  typedef pair<string, int8_t> ColumnNameAndIntValue;

  static void CreatePartitionSchemaPB(
      const vector<string>& range_columns,
      const vector<ColumnNamesNumBucketsAndSeed>& table_hash_schema,
      PartitionSchemaPB* partition_schema_pb);

  static void AddRangePartitionWithSchema(
      const Schema& schema,
      const vector<ColumnNameAndStringValue>& lower_string_cols,
      const vector<ColumnNameAndStringValue>& upper_string_cols,
      const vector<ColumnNameAndIntValue>& lower_int_cols,
      const vector<ColumnNameAndIntValue>& upper_int_cols,
      const vector<ColumnNamesNumBucketsAndSeed>& hash_schemas,
      PartitionSchemaPB* pb);

  static void CheckPrunedPartitions(const Schema& schema,
                                    const PartitionSchema& partition_schema,
                                    const vector<Partition>& partitions,
                                    const ScanSpec& spec,
                                    size_t remaining_tablets,
                                    size_t pruner_ranges);
};

void PartitionPrunerTest::CreatePartitionSchemaPB(
    const vector<string>& range_columns,
    const vector<ColumnNamesNumBucketsAndSeed>& table_hash_schema,
    PartitionSchemaPB* partition_schema_pb) {
  auto* range_schema = partition_schema_pb->mutable_range_schema();
  for (const auto& range_column : range_columns) {
    range_schema->add_columns()->set_name(range_column);
  }
  for (const auto& hash_dimension : table_hash_schema) {
    auto* hash_dimension_pb = partition_schema_pb->add_hash_schema();
    for (const auto& hash_schema_column : get<0>(hash_dimension)) {
      hash_dimension_pb->add_columns()->set_name(hash_schema_column);
    }
    hash_dimension_pb->set_num_buckets(get<1>(hash_dimension));
    hash_dimension_pb->set_seed(get<2>(hash_dimension));
  }
}

void PartitionPrunerTest::AddRangePartitionWithSchema(
    const Schema& schema,
    const vector<ColumnNameAndStringValue>& lower_string_cols,
    const vector<ColumnNameAndStringValue>& upper_string_cols,
    const vector<ColumnNameAndIntValue>& lower_int_cols,
    const vector<ColumnNameAndIntValue>& upper_int_cols,
    const vector<ColumnNamesNumBucketsAndSeed>& hash_buckets_info,
    PartitionSchemaPB* pb) {
  auto* range = pb->add_custom_hash_schema_ranges();
  RowOperationsPBEncoder encoder(range->mutable_range_bounds());
  KuduPartialRow lower(&schema);
  KuduPartialRow upper(&schema);
  for (const auto& bound : lower_string_cols) {
    ASSERT_OK(lower.SetStringCopy(bound.first, bound.second));
  }
  for (const auto& bound : upper_string_cols) {
    ASSERT_OK(upper.SetStringCopy(bound.first, bound.second));
  }
  for (const auto& bound : lower_int_cols) {
    ASSERT_OK(lower.SetInt8(bound.first, bound.second));
  }
  for (const auto& bound : upper_int_cols) {
    ASSERT_OK(upper.SetInt8(bound.first, bound.second));
  }
  encoder.Add(RowOperationsPB::RANGE_LOWER_BOUND, lower);
  encoder.Add(RowOperationsPB::RANGE_UPPER_BOUND, upper);
  PartitionSchema::HashSchema hash_schema;
  for (const auto& hash_bucket_info : hash_buckets_info) {
    auto* hash_dimension_pb = range->add_hash_schema();
    PartitionSchema::HashDimension hash_dimension;
    for (const auto& hash_schema_columns : get<0>(hash_bucket_info)) {
      hash_dimension_pb->add_columns()->set_name(hash_schema_columns);
      hash_dimension.column_ids.emplace_back(schema.find_column(hash_schema_columns));
    }
    hash_dimension_pb->set_num_buckets(get<1>(hash_bucket_info));
    hash_dimension.num_buckets = get<1>(hash_bucket_info);
    hash_dimension_pb->set_seed(get<2>(hash_bucket_info));
    hash_dimension.seed = get<2>(hash_bucket_info);
    hash_schema.emplace_back(hash_dimension);
  }
}

void PartitionPrunerTest::CheckPrunedPartitions(
    const Schema& schema,
    const PartitionSchema& partition_schema,
    const vector<Partition>& partitions,
    const ScanSpec& spec,
    size_t remaining_tablets,
    size_t pruner_ranges) {

  ScanSpec opt_spec(spec);
  Arena arena(256);
  opt_spec.OptimizeScan(schema, &arena, false);

  PartitionPruner pruner;
  pruner.Init(schema, partition_schema, opt_spec);

  SCOPED_TRACE(strings::Substitute("schema: $0", schema.ToString()));
  SCOPED_TRACE(strings::Substitute("partition schema: $0", partition_schema.DebugString(schema)));
  SCOPED_TRACE(strings::Substitute("partition pruner: $0",
                                   pruner.ToString(schema, partition_schema)));
  SCOPED_TRACE(strings::Substitute("optimized scan spec: $0", opt_spec.ToString(schema)));
  SCOPED_TRACE(strings::Substitute("original  scan spec: $0", spec.ToString(schema)));

  int pruned_partitions = count_if(partitions.begin(), partitions.end(),
                                   [&] (const Partition& partition) {
                                     return pruner.ShouldPrune(partition);
                                   });

  ASSERT_EQ(remaining_tablets, partitions.size() - pruned_partitions);
  ASSERT_EQ(pruner_ranges, pruner.NumRangesRemaining());
}

TEST_F(PartitionPrunerTest, TestPrimaryKeyRangePruning) {
  // CREATE TABLE t
  // (a INT8, b INT8, c INT8)
  // PRIMARY KEY (a, b, c)) SPLIT ROWS [(0, 0, 0), (10, 10, 10)]
  // DISTRIBUTE BY RANGE(a, b, c);
  Schema schema({ ColumnSchema("a", INT8),
                  ColumnSchema("b", INT8),
                  ColumnSchema("c", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);
  Arena arena(1024);

  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(PartitionSchemaPB(), schema, &partition_schema));

  KuduPartialRow split1(&schema);
  ASSERT_OK(split1.SetInt8("a", 0));
  ASSERT_OK(split1.SetInt8("b", 0));
  ASSERT_OK(split1.SetInt8("c", 0));

  KuduPartialRow split2(&schema);
  ASSERT_OK(split2.SetInt8("a", 10));
  ASSERT_OK(split2.SetInt8("b", 10));
  ASSERT_OK(split2.SetInt8("c", 10));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({ split1, split2 }, {}, schema, &partitions));

  // Creates a scan with optional lower and upper bounds, and checks that the
  // expected number of tablets are pruned.
  const auto check = [&] (optional<tuple<int8_t, int8_t, int8_t>> lower,
                    optional<tuple<int8_t, int8_t, int8_t>> upper,
                    size_t remaining_tablets) {
    ScanSpec spec;
    KuduPartialRow lower_bound(&schema);
    KuduPartialRow upper_bound(&schema);
    EncodedKey* enc_lower_bound = nullptr;
    EncodedKey* enc_upper_bound = nullptr;

    if (lower) {
      ASSERT_OK(lower_bound.SetInt8("a", get<0>(*lower)));
      ASSERT_OK(lower_bound.SetInt8("b", get<1>(*lower)));
      ASSERT_OK(lower_bound.SetInt8("c", get<2>(*lower)));
      ConstContiguousRow row(lower_bound.schema(), lower_bound.row_data_);
      enc_lower_bound = EncodedKey::FromContiguousRow(row, &arena);
      spec.SetLowerBoundKey(enc_lower_bound);
    }
    if (upper) {
      ASSERT_OK(upper_bound.SetInt8("a", get<0>(*upper)));
      ASSERT_OK(upper_bound.SetInt8("b", get<1>(*upper)));
      ASSERT_OK(upper_bound.SetInt8("c", get<2>(*upper)));
      ConstContiguousRow row(upper_bound.schema(), upper_bound.row_data_);
      enc_upper_bound = EncodedKey::FromContiguousRow(row, &arena);
      spec.SetExclusiveUpperBoundKey(enc_upper_bound);
    }
    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // No bounds
  NO_FATALS(check(nullopt, nullopt, 3));

  // PK < (-1, min, min)
  NO_FATALS(check(nullopt,
                  make_tuple<int8_t, int8_t, int8_t>(-1, INT8_MIN, INT8_MIN),
                  1));

  // PK < (10, 10, 10)
  NO_FATALS(check(nullopt,
                  make_tuple<int8_t, int8_t, int8_t>(10, 10, 10),
                  2));

  // PK < (100, min, min)
  NO_FATALS(check(nullopt,
                  make_tuple<int8_t, int8_t, int8_t>(100, INT8_MIN, INT8_MIN),
                  3));

  // PK >= (-10, -10, -10)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(-10, -10, -10),
                  nullopt,
                  3));

  // PK >= (0, 0, 0)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(0, 0, 0),
                  nullopt,
                  2));

  // PK >= (100, 0, 0)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(100, 0, 0),
                  nullopt,
                  1));

  // PK >= (-10, 0, 0)
  // PK  < (100, 0, 0)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(-10, 0, 0),
                  make_tuple<int8_t, int8_t, int8_t>(100, 0, 0),
                  3));

  // PK >= (0, 0, 0)
  // PK  < (10, 10, 10)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(0, 0, 0),
                  make_tuple<int8_t, int8_t, int8_t>(10, 10, 10),
                  1));

  // PK >= (0, 0, 0)
  // PK  < (10, 10, 11)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(0, 0, 0),
                  make_tuple<int8_t, int8_t, int8_t>(10, 10, 11),
                  2));

  // PK  < (0, 0, 0)
  // PK >= (10, 10, 11)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(10, 10, 11),
                  make_tuple<int8_t, int8_t, int8_t>(0, 0, 0),
                  0));
}

TEST_F(PartitionPrunerTest, TestPartialPrimaryKeyRangePruning) {
  // CREATE TABLE t
  // (a INT8, b STRING, c STRING, PRIMARY KEY (a, b, c))
  // DISTRIBUTE BY RANGE(a, b)
  // SPLIT ROWS [(0, "m"), (10, "r"];
  Schema schema({ ColumnSchema("a", INT8),
      ColumnSchema("b", STRING),
      ColumnSchema("c", STRING) },
      { ColumnId(0), ColumnId(1), ColumnId(2) },
      3);
  Arena arena(1024);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({"a", "b"}, {}, &pb);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  KuduPartialRow split1(&schema);
  ASSERT_OK(split1.SetInt8("a", 0));
  ASSERT_OK(split1.SetStringCopy("b", "m"));

  KuduPartialRow split2(&schema);
  ASSERT_OK(split2.SetInt8("a", 10));
  ASSERT_OK(split2.SetStringCopy("b", "r"));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({ split1, split2 }, {}, schema, &partitions));

  // Applies the specified lower and upper bound primary keys against the
  // schema, and checks that the expected number of partitions are pruned.
  const auto check = [&] (optional<tuple<int8_t, string, string>> lower,
                          optional<tuple<int8_t, string, string>> upper,
                          size_t remaining_tablets ) {
    ScanSpec spec;
    KuduPartialRow lower_bound(&schema);
    KuduPartialRow upper_bound(&schema);
    EncodedKey* enc_lower_bound = nullptr;
    EncodedKey* enc_upper_bound = nullptr;

    if (lower) {
      ASSERT_OK(lower_bound.SetInt8("a", get<0>(*lower)));
      ASSERT_OK(lower_bound.SetStringCopy("b", get<1>(*lower)));
      ASSERT_OK(lower_bound.SetStringCopy("c", get<2>(*lower)));
      ConstContiguousRow row(lower_bound.schema(), lower_bound.row_data_);
      enc_lower_bound = EncodedKey::FromContiguousRow(row, &arena);
      spec.SetLowerBoundKey(enc_lower_bound);
    }
    if (upper) {
      ASSERT_OK(upper_bound.SetInt8("a", get<0>(*upper)));
      ASSERT_OK(upper_bound.SetStringCopy("b", get<1>(*upper)));
      ASSERT_OK(upper_bound.SetStringCopy("c", get<2>(*upper)));
      ConstContiguousRow row(upper_bound.schema(), upper_bound.row_data_);
      enc_upper_bound = EncodedKey::FromContiguousRow(row, &arena);
      spec.SetExclusiveUpperBoundKey(enc_upper_bound);
    }
    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // No bounds
  NO_FATALS(check(nullopt, nullopt, 3));

  // PK < (-1, "", "")
  NO_FATALS(check(nullopt, make_tuple<int8_t, string, string>(-1, "", ""), 1));

  // PK < (10, "r", "")
  NO_FATALS(check(nullopt, make_tuple<int8_t, string, string>(10, "r", ""), 2));

  // PK < (10, "r", "z")
  NO_FATALS(check(nullopt, make_tuple<int8_t, string, string>(10, "r", "z"), 3));

  // PK < (100, "", "")
  NO_FATALS(check(nullopt, make_tuple<int8_t, string, string>(100, "", ""), 3));

  // PK >= (-10, "m", "")
  NO_FATALS(check(make_tuple<int8_t, string, string>(-10, "m", ""), nullopt, 3));

  // PK >= (0, "", "")
  NO_FATALS(check(make_tuple<int8_t, string, string>(0, "", ""), nullopt, 3));

  // PK >= (0, "m", "")
  NO_FATALS(check(make_tuple<int8_t, string, string>(0, "m", ""), nullopt, 2));

  // PK >= (100, "", "")
  NO_FATALS(check(make_tuple<int8_t, string, string>(100, "", ""), nullopt, 1));

  // PK >= (-10, "", "")
  // PK  < (100, "", "")
  NO_FATALS(check(make_tuple<int8_t, string, string>(-10, "", ""),
                  make_tuple<int8_t, string, string>(100, "", ""), 3));

  // PK >= (0, "m", "")
  // PK  < (10, "r", "")
  NO_FATALS(check(make_tuple<int8_t, string, string>(0, "m", ""),
                  make_tuple<int8_t, string, string>(10, "r", ""), 1));

  // PK >= (0, "m", "")
  // PK  < (10, "r", "z")
  NO_FATALS(check(make_tuple<int8_t, string, string>(0, "m", ""),
                  make_tuple<int8_t, string, string>(10, "r", "z"), 2));

  // PK >= (0, "", "")
  // PK  < (10, "m", "z")
  NO_FATALS(check(make_tuple<int8_t, string, string>(0, "", ""),
                  make_tuple<int8_t, string, string>(10, "m", "z"), 2));

  // PK >= (10, "m", "")
  // PK  < (10, "m", "z")
  NO_FATALS(check(make_tuple<int8_t, string, string>(10, "m", ""),
                  make_tuple<int8_t, string, string>(10, "m", "z"), 1));
}

TEST_F(PartitionPrunerTest, TestIntPartialPrimaryKeyRangePruning) {
  // CREATE TABLE t
  // (a INT8, b INT8, c INT8, PRIMARY KEY (a, b, c))
  // DISTRIBUTE BY RANGE(a, b)
  // SPLIT ROWS [(0, 0)];
  Schema schema({ ColumnSchema("a", INT8),
                  ColumnSchema("b", INT8),
                  ColumnSchema("c", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);
  Arena arena(1024);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({"a", "b"}, {}, &pb);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  KuduPartialRow split(&schema);
  ASSERT_OK(split.SetInt8("a", 0));
  ASSERT_OK(split.SetInt8("b", 0));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({ split }, {}, schema, &partitions));

  // Applies the specified lower and upper bound primary keys against the
  // schema, and checks that the expected number of partitions are pruned.
  const auto check = [&] (optional<tuple<int8_t, int8_t, int8_t>> lower,
                          optional<tuple<int8_t, int8_t, int8_t>> upper,
                          size_t remaining_tablets ) {
    ScanSpec spec;
    KuduPartialRow lower_bound(&schema);
    KuduPartialRow upper_bound(&schema);
    EncodedKey* enc_lower_bound = nullptr;
    EncodedKey* enc_upper_bound = nullptr;

    if (lower) {
      ASSERT_OK(lower_bound.SetInt8("a", get<0>(*lower)));
      ASSERT_OK(lower_bound.SetInt8("b", get<1>(*lower)));
      ASSERT_OK(lower_bound.SetInt8("c", get<2>(*lower)));
      ConstContiguousRow row(lower_bound.schema(), lower_bound.row_data_);
      enc_lower_bound = EncodedKey::FromContiguousRow(row, &arena);
      spec.SetLowerBoundKey(enc_lower_bound);
    }
    if (upper) {
      ASSERT_OK(upper_bound.SetInt8("a", get<0>(*upper)));
      ASSERT_OK(upper_bound.SetInt8("b", get<1>(*upper)));
      ASSERT_OK(upper_bound.SetInt8("c", get<2>(*upper)));
      ConstContiguousRow row(upper_bound.schema(), upper_bound.row_data_);
      enc_upper_bound = EncodedKey::FromContiguousRow(row, &arena);
      spec.SetExclusiveUpperBoundKey(enc_upper_bound);
    }
    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // No bounds
  NO_FATALS(check(nullopt, nullopt, 2));

  // PK < (0, 0, min)
  NO_FATALS(check(nullopt, make_tuple<int8_t, int8_t, int8_t>(0, 0, INT8_MIN), 1));

  // PK < (0, 0, 0);
  NO_FATALS(check(nullopt, make_tuple<int8_t, int8_t, int8_t>(0, 0, 0), 2));

  // PK < (0, max, 0);
  NO_FATALS(check(nullopt, make_tuple<int8_t, int8_t, int8_t>(0, INT8_MAX, 0), 2));

  // PK < (max, max, min);
  NO_FATALS(check(nullopt,
                  make_tuple<int8_t, int8_t, int8_t>(INT8_MAX, INT8_MAX, INT8_MIN), 2));

  // PK < (max, max, 0);
  NO_FATALS(check(nullopt, make_tuple<int8_t, int8_t, int8_t>(INT8_MAX, INT8_MAX, 0), 2));

  // PK >= (0, 0, 0);
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(0, 0, 0), nullopt, 1));

  // PK >= (0, 0, -1);
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(0, 0, -1), nullopt, 1));

  // PK >= (0, 0, min);
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(0, 0, INT8_MIN), nullopt, 1));
}

TEST_F(PartitionPrunerTest, TestRangePruning) {
  // CREATE TABLE t
  // (a INT8, b STRING, c INT8)
  // PRIMARY KEY (a, b, c))
  // DISTRIBUTE BY RANGE(c, b);
  // SPLIT ROWS [(0, "m"), (10, "r")];
  Schema schema({ ColumnSchema("a", INT8),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({"c", "b"}, {}, &pb);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  KuduPartialRow split1(&schema);
  ASSERT_OK(split1.SetInt8("c", 0));
  ASSERT_OK(split1.SetStringCopy("b", "m"));

  KuduPartialRow split2(&schema);
  ASSERT_OK(split2.SetInt8("c", 10));
  ASSERT_OK(split2.SetStringCopy("b", "r"));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({ split1, split2 }, {}, schema, &partitions));

  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  const auto check = [&] (const vector<ColumnPredicate>& predicates, size_t remaining_tablets) {
    ScanSpec spec;
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  constexpr int8_t neg_ten = -10;
  constexpr int8_t zero = 0;
  constexpr int8_t five = 5;
  constexpr int8_t ten = 10;
  constexpr int8_t hundred = 100;
  constexpr int8_t min = INT8_MIN;
  constexpr int8_t max = INT8_MAX;

  Slice empty = "";
  Slice a = "a";
  Slice m = "m";
  Slice m0 = Slice("m\0", 2);
  Slice r = "r";
  Slice z = "z";

  // No Bounds
  NO_FATALS(check({}, 3));

  // c < -10
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), nullptr, &neg_ten) }, 1));

  // c = -10
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(2), &neg_ten) }, 1));

  // c < 10
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), nullptr, &ten) }, 2));

  // c < 100
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), nullptr, &hundred) }, 3));

  // c < MIN
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), nullptr, &min) }, 0));

  // c < MAX
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), nullptr, &max) }, 3));

  // c >= -10
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &neg_ten, nullptr) }, 3));

  // c >= 0
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &zero, nullptr) }, 3));

  // c >= 5
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &five, nullptr) }, 2));

  // c >= 10
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &ten, nullptr) }, 2));

  // c >= 100
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &hundred, nullptr) }, 1));

  // c >= MIN
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &min, nullptr) }, 3));

  // c >= MAX
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &max, nullptr) }, 1));

  // c = MIN
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(2), &min) }, 1));

  // c = MAX
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(2), &max) }, 1));

  // c >= -10
  // c < 0
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &neg_ten, &zero) }, 1));

  // c >= 5
  // c < 100
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &five, &hundred) }, 2));

  // b = ""
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &empty) }, 3));

  // b >= "z"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(1), &z, nullptr) }, 3));

  // b < "a"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(1), nullptr, &a) }, 3));

  // b >= "m"
  // b < "z"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(1), &m, &z) }, 3));

  // c >= 10
  // b >= "r"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &ten, nullptr),
                    ColumnPredicate::Range(schema.column(1), &r, nullptr) },
                  1));

  // c >= 10
  // b < "r"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &ten, nullptr),
                    ColumnPredicate::Range(schema.column(1), nullptr, &r) },
                  2));

  // c = 10
  // b < "r"
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(2), &ten),
                    ColumnPredicate::Range(schema.column(1), nullptr, &r) },
                  1));

  // c < 0
  // b < "m"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), nullptr, &zero),
                    ColumnPredicate::Range(schema.column(1), nullptr, &m) },
                  1));

  // c < 0
  // b < "z"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), nullptr, &zero),
                    ColumnPredicate::Range(schema.column(1), nullptr, &z) },
                  1));

  // c = 0
  // b = "m\0"
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(2), &zero),
                    ColumnPredicate::Equality(schema.column(1), &m0) },
                  1));

  // c = 0
  // b < "m"
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(2), &zero),
                    ColumnPredicate::Range(schema.column(1), nullptr, &m) },
                  1));

  // c = 0
  // b < "m\0"
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(2), &zero),
                    ColumnPredicate::Range(schema.column(1), nullptr, &m0) },
                  2));

  // c IS NOT NULL
  NO_FATALS(check({ ColumnPredicate::IsNotNull(schema.column(2)) }, 3));
}

TEST_F(PartitionPrunerTest, TestHashPruning) {
  // CREATE TABLE t
  // (a INT8, b INT8, c INT8)
  // PRIMARY KEY (a, b, c)
  // DISTRIBUTE BY HASH(a) INTO 2 BUCKETS,
  //               HASH(b, c) INTO 2 BUCKETS;
  Schema schema({ ColumnSchema("a", INT8),
                  ColumnSchema("b", INT8),
                  ColumnSchema("c", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);

    PartitionSchemaPB pb;
    CreatePartitionSchemaPB({}, { {{"a"}, 2, 0}, {{"b", "c"}, 2, 0} }, &pb);
    pb.mutable_range_schema()->Clear();
    PartitionSchema partition_schema;
    ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

    vector<Partition> partitions;
    ASSERT_OK(partition_schema.CreatePartitions(vector<KuduPartialRow>(), {},
                                                       schema, &partitions));


  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  const auto check = [&] (const vector<ColumnPredicate>& predicates,
                    size_t remaining_tablets,
                    size_t pruner_ranges) {
    ScanSpec spec;
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  constexpr int8_t zero = 0;
  constexpr int8_t one = 1;
  constexpr int8_t two = 2;

  // No Bounds
  NO_FATALS(check({}, 4, 1));

  // a = 0;
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &zero) }, 2, 1));

  // a >= 0;
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &zero, nullptr) }, 4, 1));

  // a >= 0;
  // a < 1;
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &zero, &one) }, 2, 1));

  // a >= 0;
  // a < 2;
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &zero, &two) }, 4, 1));

  // b = 1;
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &one) }, 4, 1));

  // b = 1;
  // c = 2;
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &one),
                    ColumnPredicate::Equality(schema.column(2), &two) },
                  2, 2));

  // a = 0;
  // b = 1;
  // c = 2;
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &zero),
                    ColumnPredicate::Equality(schema.column(1), &one),
                    ColumnPredicate::Equality(schema.column(2), &two) },
                  1, 1));
}

TEST_F(PartitionPrunerTest, TestInListHashPruning) {
  // CREATE TABLE t
  // (a INT8, b INT8, c INT8)
  // PRIMARY KEY (a, b, c)
  // DISTRIBUTE BY HASH(a) INTO 3 BUCKETS,
  //               HASH(b) INTO 3 BUCKETS;
  //               HASH(c) INTO 3 BUCKETS;
  Schema schema({ ColumnSchema("a", INT8),
                  ColumnSchema("b", INT8),
                  ColumnSchema("c", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({}, { {{"a"}, 3, 0}, {{"b"}, 3, 0}, {{"c"}, 3, 0} }, &pb);
  pb.mutable_range_schema()->clear_columns();
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(vector<KuduPartialRow>(), {},
                                                     schema, &partitions));


  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  const auto check = [&] (const vector<ColumnPredicate>& predicates,
                    size_t remaining_tablets,
                    size_t pruner_ranges) {
    ScanSpec spec;
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // zero, one, eight are in different buckets when bucket number is 3 and seed is 0.
  constexpr int8_t zero = 0;
  constexpr int8_t one = 1;
  constexpr int8_t eight = 8;

  vector<const void*> a_values;
  vector<const void*> b_values;
  vector<const void*> c_values;

  // a in [0, 1];
  a_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(0), &a_values) }, 18, 2));

  // a in [0, 1, 8];
  a_values = { &zero, &one, &eight };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(0), &a_values) }, 27, 1));

  // b in [0, 1]
  b_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(1), &b_values) }, 18, 6));

  // c in [0, 1]
  c_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(2), &c_values) }, 18, 18));

  // b in [0, 1], c in [0, 1]
  b_values = { &zero, &one };
  c_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(1), &b_values),
                    ColumnPredicate::InList(schema.column(2), &c_values) },
                  12, 12));

  // a in [0, 1], b in [0, 1], c in [0, 1]
  a_values = { &zero, &one };
  b_values = { &zero, &one };
  c_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(0), &a_values),
                    ColumnPredicate::InList(schema.column(1), &b_values),
                    ColumnPredicate::InList(schema.column(2), &c_values) },
                  8, 8));
}

TEST_F(PartitionPrunerTest, TestMultiColumnInListHashPruning) {
  // CREATE TABLE t
  // (a INT8, b INT8, c INT8)
  // PRIMARY KEY (a, b, c)
  // DISTRIBUTE BY HASH(a) INTO 3 BUCKETS,
  //               HASH(b, c) INTO 3 BUCKETS;
  Schema schema({ ColumnSchema("a", INT8),
                  ColumnSchema("b", INT8),
                  ColumnSchema("c", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({}, { {{"a"}, 3, 0}, {{"b", "c"}, 3, 0} }, &pb);
  pb.mutable_range_schema()->clear_columns();
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(vector<KuduPartialRow>(), {},
                                                     schema, &partitions));

  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  const auto check = [&] (const vector<ColumnPredicate>& predicates,
                    size_t remaining_tablets,
                    size_t pruner_ranges) {
    ScanSpec spec;
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // zero, one, eight are in different buckets when bucket number is 3 and seed is 0.
  constexpr int8_t zero = 0;
  constexpr int8_t one = 1;
  constexpr int8_t eight = 8;

  vector<const void*> a_values;
  vector<const void*> b_values;
  vector<const void*> c_values;

  // a in [0, 1];
  a_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(0), &a_values) }, 6, 2));

  // a in [0, 1, 8];
  a_values = { &zero, &one, &eight };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(0), &a_values) }, 9, 1));

  // b in [0, 1]
  b_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(1), &b_values) }, 9, 1));

  // c in [0, 1]
  c_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(2), &c_values) }, 9, 1));

  // b in [0, 1], c in [0, 1]
  // (0, 0) in bucket 2
  // (0, 1) in bucket 2
  // (1, 0) in bucket 1
  // (1, 1) in bucket 0
  b_values = { &zero, &one };
  c_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(1), &b_values),
                    ColumnPredicate::InList(schema.column(2), &c_values) },
                  9, 1));

  // b = 0, c in [0, 1]
  c_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &zero),
                    ColumnPredicate::InList(schema.column(2), &c_values) },
                  3, 3));

  // b = 1, c in [0, 1]
  c_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &one),
                    ColumnPredicate::InList(schema.column(2), &c_values) },
                  6, 6));

  //a in [0, 1], b in [0, 1], c in [0, 1]
  a_values = { &zero, &one };
  b_values = { &zero, &one };
  c_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(0), &a_values),
                    ColumnPredicate::InList(schema.column(1), &b_values),
                    ColumnPredicate::InList(schema.column(2), &c_values) },
                  6, 2));
}

TEST_F(PartitionPrunerTest, TestPruning) {
  // CREATE TABLE timeseries
  // (host STRING, metric STRING, time UNIXTIME_MICROS, value DOUBLE)
  // PRIMARY KEY (host, metric, time)
  // PARTITION BY RANGE (time) (PARTITION VALUES < 10,
  //                            PARTITION VALUES >= 10)
  //              HASH (host, metric) 2 PARTITIONS;
  Schema schema({ ColumnSchema("host", STRING),
                  ColumnSchema("metric", STRING),
                  ColumnSchema("time", UNIXTIME_MICROS),
                  ColumnSchema("value", DOUBLE) },
                { ColumnId(0), ColumnId(1), ColumnId(2), ColumnId(3) },
                3);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({"time"}, { {{"host", "metric"}, 2, 0} }, &pb);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  KuduPartialRow split(&schema);
  ASSERT_OK(split.SetUnixTimeMicros("time", 10));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(
      vector<KuduPartialRow>{ split }, {},  schema, &partitions));
  ASSERT_EQ(4, partitions.size());

  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  const auto check = [&] (const vector<ColumnPredicate>& predicates,
                    const PartitionKey& lower_bound_partition_key,
                    const PartitionKey& upper_bound_partition_key,
                    size_t remaining_tablets,
                    size_t pruner_ranges) {
    ScanSpec spec;
    spec.SetLowerBoundPartitionKey(lower_bound_partition_key);
    spec.SetExclusiveUpperBoundPartitionKey(upper_bound_partition_key);
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  Slice a = "a";

  constexpr int64_t nine = 9;
  constexpr int64_t ten = 10;
  constexpr int64_t twenty = 20;

  // host = "a"
  // metric = "a"
  // timestamp >= 9;
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &a),
                    ColumnPredicate::Equality(schema.column(1), &a),
                    ColumnPredicate::Range(schema.column(2), &nine, nullptr) },
                  {}, {},
                  2, 1));

  // host = "a"
  // metric = "a"
  // timestamp >= 10;
  // timestamp < 20;
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &a),
                    ColumnPredicate::Equality(schema.column(1), &a),
                    ColumnPredicate::Range(schema.column(2), &ten, &twenty) },
                  {}, {},
                  1, 1));

  // host = "a"
  // metric = "a"
  // timestamp < 10;
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &a),
                    ColumnPredicate::Equality(schema.column(1), &a),
                    ColumnPredicate::Range(schema.column(2), nullptr, &ten) },
                  {}, {},
                  1, 1));

  // host = "a"
  // metric = "a"
  // timestamp >= 10;
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &a),
                    ColumnPredicate::Equality(schema.column(1), &a),
                    ColumnPredicate::Range(schema.column(2), &ten, nullptr) },
                  {}, {},
                  1, 1));

  // host = "a"
  // metric = "a"
  // timestamp = 10;
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &a),
                    ColumnPredicate::Equality(schema.column(1), &a),
                    ColumnPredicate::Equality(schema.column(2), &ten) },
                  {}, {},
                  1, 1));

  // partition key < (hash=1)
  NO_FATALS(check({}, {}, { string("\0\0\0\1", 4), "" }, 2, 1));

  // partition key >= (hash=1)
  NO_FATALS(check({}, { string("\0\0\0\1", 4), "" }, {}, 2, 1));

  // timestamp = 10
  // partition key < (hash=1)
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(2), &ten) },
      {}, { string("\0\0\0\1", 4), "" }, 1, 1));

  // timestamp = 10
  // partition key >= (hash=1)
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(2), &ten) },
      { string("\0\0\0\1", 4), "" }, {}, 1, 1));
}

TEST_F(PartitionPrunerTest, TestKudu2173) {
  // CREATE TABLE t
  // (a INT8, b INT8, PRIMARY KEY (a, b))
  // DISTRIBUTE BY RANGE(a)
  // SPLIT ROWS [(10)]
  Schema schema({ ColumnSchema("a", INT8),
          ColumnSchema("b", INT8)},
    { ColumnId(0), ColumnId(1) },
    2);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({"a"}, {}, &pb);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  KuduPartialRow split1(&schema);
  ASSERT_OK(split1.SetInt8("a", 10));
  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({ split1 }, {}, schema, &partitions));

  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  const auto check = [&] (const vector<ColumnPredicate>& predicates, size_t remaining_tablets) {
    ScanSpec spec;
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  constexpr int8_t eleven = 11;
  constexpr int8_t max = INT8_MAX;

  // a < 11
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), nullptr, &eleven) }, 2));

  // a < 11 AND b < 11
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), nullptr, &eleven),
                    ColumnPredicate::Range(schema.column(1), nullptr, &eleven) },
                  2));

  // a < max
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), nullptr, &max) }, 2));

  // a < max AND b < 11
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), nullptr, &max),
                    ColumnPredicate::Range(schema.column(1), nullptr, &eleven) },
                  2));
}

// TODO(aserbin): re-enable this scenario once varying hash dimensions per range
//                are supported
TEST_F(PartitionPrunerTest, DISABLED_TestHashSchemasPerRangePruning) {
  // CREATE TABLE t
  // (A INT8, B INT8, C STRING)
  // PRIMARY KEY (A, B, C)
  // PARTITION BY RANGE (C)
  // DISTRIBUTE BY HASH(A) INTO 2 BUCKETS
  //               HASH(B) INTO 2 BUCKETS;
  Schema schema({ ColumnSchema("A", INT8),
                  ColumnSchema("B", INT8),
                  ColumnSchema("C", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({"C"}, { {{"A"}, 2, 0}, {{"B"}, 2, 0} }, &pb);

  // Need to add per range hash schema components to the field
  // 'ranges_with_hash_schemas_' of PartitionSchema because PartitionPruner will
  // use them to construct partition key ranges. Currently,
  // PartitionSchema::CreatePartitions() does not leverage this field, so these
  // components will have to be passed separately to the function as well.

  // None of the ranges below uses the table-wide hash schema.

  // [(_, _, a), (_, _, c))
  AddRangePartitionWithSchema(schema, {{"C", "a"}}, {{"C", "c"}}, {}, {},
                              { {{"A"}, 3, 0} }, &pb);

  // [(_, _, d), (_, _, f))
  AddRangePartitionWithSchema(schema, {{"C", "d"}}, {{"C", "f"}}, {}, {},
                              { {{"A"}, 2, 0}, {{"B"}, 3, 0} }, &pb);

  // [(_, _, h), (_, _, j))
  AddRangePartitionWithSchema(schema, {{"C", "h"}}, {{"C", "j"}}, {}, {}, {}, &pb);

  // [(_, _, k), (_, _, m))
  AddRangePartitionWithSchema(schema, {{"C", "k"}}, {{"C", "m"}}, {}, {},
                              { {{"B"}, 2, 0} }, &pb);

  PartitionSchema partition_schema;
  PartitionSchema::RangesWithHashSchemas ranges;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema, &ranges));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(ranges, schema, &partitions));
  ASSERT_EQ(12, partitions.size());

  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  const auto check = [&] (const vector<ColumnPredicate>& predicates,
                          const PartitionKey& lower_bound_partition_key,
                          const PartitionKey& upper_bound_partition_key,
                          size_t remaining_tablets,
                          size_t pruner_ranges) {
    ScanSpec spec;
    spec.SetLowerBoundPartitionKey(lower_bound_partition_key);
    spec.SetExclusiveUpperBoundPartitionKey(upper_bound_partition_key);
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  constexpr int8_t zero = 0;
  constexpr int8_t one = 1;

  const Slice a = "a";
  const Slice b = "b";
  const Slice e = "e";
  const Slice f = "f";
  const Slice i = "i";
  const Slice l = "l";
  const Slice m = "m";

  // No Bounds
  NO_FATALS(check({}, {}, {}, 12, 12));
  // A = 1
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &one) },
            {}, {}, 7, 7));
  // B = 1
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &one) },
            {}, {}, 7, 7));
  // A = 0
  // B = 1
  // C >= "e"
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &zero),
                    ColumnPredicate::Equality(schema.column(1), &one),
                    ColumnPredicate::Range(schema.column(2), &e, nullptr) },
                  {}, {}, 3, 3));
  // A = 0
  // B = 1
  // C = "e"
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(0), &zero),
                    ColumnPredicate::Equality(schema.column(1), &one),
                    ColumnPredicate::Equality(schema.column(2), &e) },
                  {}, {}, 1, 1));
  // B = 1
  // C >= "b"
  // C < "j"
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &one),
                    ColumnPredicate::Range(schema.column(2), &b, nullptr),
                    ColumnPredicate::Range(schema.column(2), nullptr, &i) },
                   {}, {}, 6, 6));
  // B = 0
  // C >= "e"
  // C < "l"
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &zero),
                    ColumnPredicate::Range(schema.column(2), &e, nullptr),
                    ColumnPredicate::Range(schema.column(2), nullptr, &l) },
                  {}, {}, 4, 4));
  // C >= "a"
  // C < "b"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &a, &b) },
                  {}, {}, 3, 3));
  // C >= "a"
  // C < "e"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &a, &e) },
                  {}, {}, 9, 9));
  // C >= "e"
  // C < "i"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &e, &i) },
                  {}, {}, 7, 7));
  // C >= "a"
  // C < "l"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &a, &l) },
                  {}, {}, 12, 12));
  // C >= "i"
  // C < "l"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &i, &l) },
                  {}, {}, 3, 3));
  // C >= "e"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &e, nullptr) },
                  {}, {}, 9, 9));
  // C < "f"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), nullptr, &f) },
                  {}, {}, 9, 9));
  // C >= "f"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &f, nullptr) },
                  {}, {}, 3, 3));
  // C < "a"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), nullptr, &a) },
                  {}, {}, 0, 0));
  // C >= "m"
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(2), &m, nullptr) },
                  {}, {}, 0, 0));

  // Uses None predicate to short circuit scan
  NO_FATALS(check({ ColumnPredicate::None(schema.column(2))}, {}, {}, 0, 0));

  // partition key >= (hash=1, hash=0)
  NO_FATALS(check({}, {string("\0\0\0\1\0\0\0\0", 8), ""}, {}, 7, 7));

  // partition key < (hash=1, hash=0)
  NO_FATALS(check({}, {}, {string("\0\0\0\1\0\0\0\0", 8), ""}, 5, 5));

  // C >= "e"
  // C < "m"
  // partition key >= (hash=1)
  //
  // The only non-pruned tablets are:
  //   * range d-f:
  //       hash keys:
  //         ** "\0\0\0\1\0\0\0\0"
  //         ** "\0\0\0\1\0\0\0\1"
  //         ** "\0\0\0\1\0\0\0\2"
  //   * range k-m:
  //       hash keys:
  //         ** "\0\0\0\1"
  NO_FATALS(check({ColumnPredicate::Range(schema.column(2), &e, &m)},
            { string("\0\0\0\1", 4), "" }, {}, 5, 5));

  // C >= "e"
  // C < "m"
  // partition key < (hash=1)
  //
  // The only non-pruned tablets are:
  //   * range d-f:
  //       hash keys:
  //         ** "\0\0\0\0\0\0\0\0"
  //         ** "\0\0\0\0\0\0\0\1"
  //         ** "\0\0\0\0\0\0\0\2"
  //   * range k-m:
  //       hash keys:
  //         ** "\0\0\0\0"
  NO_FATALS(check({ColumnPredicate::Range(schema.column(2), &e, &m)},
            {}, { string("\0\0\0\1", 4), "" }, 4, 4));
}

TEST_F(PartitionPrunerTest, TestHashSchemasPerRangeWithPartialPrimaryKeyRangePruning) {
  // CREATE TABLE t
  // (a INT8, b INT8, c INT8)
  // PRIMARY KEY (a, b, c)
  // PARTITION BY RANGE(a, b)
  Schema schema({ ColumnSchema("a", INT8),
                  ColumnSchema("b", INT8),
                  ColumnSchema("c", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({"a", "b"}, { {{"c"}, 2, 10} }, &pb);

  // [(0, 0, _), (2, 2, _))
  AddRangePartitionWithSchema(schema, {}, {}, {{"a", 0}, {"b", 0}}, {{"a", 2}, {"b", 2}},
                              { {{"c"}, 2, 0} }, &pb);

  // [(2, 2, _), (4, 4, _))
  AddRangePartitionWithSchema(schema, {}, {}, {{"a", 2}, {"b", 2}}, {{"a", 4}, {"b", 4}},
                              { {{"c"}, 3, 0} }, &pb);

  // [(4, 4, _), (6, 6, _))
  AddRangePartitionWithSchema(schema, {}, {}, {{"a", 4}, {"b", 4}}, {{"a", 6}, {"b", 6}},
                              { {{"c"}, 4, 0} }, &pb);

  PartitionSchema partition_schema;
  PartitionSchema::RangesWithHashSchemas ranges;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema, &ranges));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(ranges, schema, &partitions));
  ASSERT_EQ(9, partitions.size());

  Arena arena(1024);
  // Applies the specified lower and upper bound primary keys against the
  // schema, and checks that the expected number of partitions are pruned.
  const auto check = [&] (optional<tuple<int8_t, int8_t, int8_t>> lower,
                          optional<tuple<int8_t, int8_t, int8_t>> upper,
                          size_t remaining_tablets,
                          size_t pruner_ranges) {
    ScanSpec spec;
    KuduPartialRow lower_bound(&schema);
    KuduPartialRow upper_bound(&schema);
    EncodedKey* enc_lower_bound = nullptr;
    EncodedKey* enc_upper_bound = nullptr;

    if (lower) {
      ASSERT_OK(lower_bound.SetInt8("a", get<0>(*lower)));
      ASSERT_OK(lower_bound.SetInt8("b", get<1>(*lower)));
      ASSERT_OK(lower_bound.SetInt8("c", get<2>(*lower)));
      ConstContiguousRow row(lower_bound.schema(), lower_bound.row_data_);
      enc_lower_bound = EncodedKey::FromContiguousRow(row, &arena);
      spec.SetLowerBoundKey(enc_lower_bound);
    }
    if (upper) {
      ASSERT_OK(upper_bound.SetInt8("a", get<0>(*upper)));
      ASSERT_OK(upper_bound.SetInt8("b", get<1>(*upper)));
      ASSERT_OK(upper_bound.SetInt8("c", get<2>(*upper)));
      ConstContiguousRow row(upper_bound.schema(), upper_bound.row_data_);
      enc_upper_bound = EncodedKey::FromContiguousRow(row, &arena);
      spec.SetExclusiveUpperBoundKey(enc_upper_bound);
    }
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // No bounds
  NO_FATALS(check(nullopt, nullopt, 9, 13));

  // PK < (2, 2, min)
  NO_FATALS(check(nullopt, make_tuple<int8_t, int8_t, int8_t>(2, 2, INT8_MIN), 2, 4));

  // PK < (2, 2, 0)
  NO_FATALS(check(nullopt, make_tuple<int8_t, int8_t, int8_t>(2, 2, 0), 5, 7));

  // PK >= (2, 2, 0)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(2, 2, 0), nullopt, 7, 9));

  // PK >= (2, 2, min)
  // PK < (4, 4, min)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(2, 2, INT8_MIN),
                  make_tuple<int8_t, int8_t, int8_t>(4, 4, INT8_MIN), 3, 3));

  // PK >= (2, 2, min)
  // PK < (4, 4, 0)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(2, 2, INT8_MIN),
                  make_tuple<int8_t, int8_t, int8_t>(4, 4, 0), 7, 7));

  // PK >= (2, 0, min)
  // PK < (4, 2, min)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(2, 0, INT8_MIN),
                  make_tuple<int8_t, int8_t, int8_t>(4, 2, INT8_MIN), 5, 5));

  // PK >= (6, 6, min)
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(6, 6, INT8_MIN), nullopt, 0, 2));

  // PK >= (4, 4, min)
  // PK < (2, 2, min)
  // Lower bound PK > Upper bound PK so scan is short-circuited.
  NO_FATALS(check(make_tuple<int8_t, int8_t, int8_t>(4, 4, INT8_MIN),
                  make_tuple<int8_t, int8_t, int8_t>(2, 2, INT8_MIN), 0, 0));
}

TEST_F(PartitionPrunerTest, TestInListHashPruningPerRange) {
  // CREATE TABLE t
  // (A STRING, B INT8, C INT8)
  // PRIMARY KEY (A, B, C)
  // PARTITION BY RANGE (A)
  // DISTRIBUTE HASH(B, C) INTO 3 BUCKETS;
  Schema schema({ ColumnSchema("A", STRING),
                  ColumnSchema("B", INT8),
                  ColumnSchema("C", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({"A"}, { {{"B", "C"}, 3, 0} }, &pb);

  // None of the ranges below uses the table-wide hash schema.

  // [(a, _, _), (c, _, _))
  AddRangePartitionWithSchema(schema, {{"A", "a"}}, {{"A", "c"}}, {}, {},
                              { {{"B"}, 3, 0} }, &pb);

  // [(c, _, _), (e, _, _))
  AddRangePartitionWithSchema(schema, {{"A", "c"}}, {{"A", "e"}}, {}, {},
                              {}, &pb);

  // [(e, _, _), (g, _, _))
  AddRangePartitionWithSchema(schema, {{"A", "e"}}, {{"A", "g"}}, {}, {},
                              { {{"C"}, 3, 0} }, &pb);

  PartitionSchema partition_schema;
  PartitionSchema::RangesWithHashSchemas ranges;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema, &ranges));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(ranges, schema, &partitions));
  ASSERT_EQ(7, partitions.size());

  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  const auto check = [&] (const vector<ColumnPredicate>& predicates,
                    size_t remaining_tablets,
                    size_t pruner_ranges) {
    ScanSpec spec;
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // zero, one, eight are in different buckets when bucket number is 3 and seed is 0.
  constexpr int8_t zero = 0;
  constexpr int8_t one = 1;
  constexpr int8_t eight = 8;

  vector<const void*> B_values;
  vector<const void*> C_values;

  // B in [0, 1, 8];
  B_values = { &zero, &one, &eight };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(1), &B_values) },
                  7, 13));

  // B in [0, 1];
  B_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(1), &B_values) },
                  6, 12));

  // C in [0, 1];
  C_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(2), &C_values) },
                  6, 12));

  // B in [0, 1], C in [0, 1]
  // (0, 0) in bucket 2
  // (0, 1) in bucket 2
  // (1, 0) in bucket 1
  // (1, 1) in bucket 0
  B_values = { &zero, &one };
  C_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::InList(schema.column(1), &B_values),
                    ColumnPredicate::InList(schema.column(2), &C_values) },
                  5,  11));

  // B = 0, C in [0, 1]
  C_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &zero),
                    ColumnPredicate::InList(schema.column(2), &C_values) },
                  4, 6));

  // B = 1, C in [0, 1]
  C_values = { &zero, &one };
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &one),
                    ColumnPredicate::InList(schema.column(2), &C_values) },
                  4, 8));
}

// TODO(aserbin): re-enable this scenario once varying hash dimensions per range
//                are supported
TEST_F(PartitionPrunerTest, DISABLED_TestSingleRangeElementAndBoundaryCase) {
  // CREATE TABLE t
  // (A INT8, B INT8)
  // PRIMARY KEY (A, B)
  // PARTITION BY RANGE (A);
  Schema schema({ ColumnSchema("A", INT8),
                  ColumnSchema("B", INT8) },
                { ColumnId(0), ColumnId(1) },
                2);

  PartitionSchemaPB pb;
  CreatePartitionSchemaPB({"A"}, {}, &pb);

  // [(_, _), (1, _))
  AddRangePartitionWithSchema(schema, {}, {}, {}, {{"A", 1}},
                              {{{"B"}, 4, 0}}, &pb);

  // [(1, _), (2, _))
  AddRangePartitionWithSchema(schema, {}, {}, {{"A", 1}}, {{"A", 2}},
                              { {{"B"}, 2, 0} }, &pb);

  // [(2, _), (3, _))
  AddRangePartitionWithSchema(schema, {}, {}, {{"A", 2}}, {{"A", 3}},
                              { {{"B"}, 3, 0} }, &pb);

  // [(3, _), (_, _))
  AddRangePartitionWithSchema(schema, {}, {}, {{"A", 3}}, {}, {}, &pb);

  PartitionSchema partition_schema;
  PartitionSchema::RangesWithHashSchemas ranges;
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema, &ranges));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(ranges, schema, &partitions));
  ASSERT_EQ(10, partitions.size());

  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  const auto check = [&] (const vector<ColumnPredicate>& predicates,
      size_t remaining_tablets,
      size_t pruner_ranges) {
    ScanSpec spec;
    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  constexpr int8_t neg_one = -1;
  constexpr int8_t zero = 0;
  constexpr int8_t one = 1;
  constexpr int8_t two = 2;
  constexpr int8_t three = 3;
  constexpr int8_t four = 4;
  constexpr int8_t five = 5;

  // No Bounds
  NO_FATALS(check({}, 10, 10));

  // A >= 0
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &zero, nullptr)}, 10, 10));

  // A >= 1
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &one, nullptr)}, 6, 6));

  // A < 1
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), nullptr, &one)}, 4, 4));

  // A >= 2
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &two, nullptr)}, 4, 4));

  // A < 2
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), nullptr, &two)}, 6, 6));

  // A < 3
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), nullptr, &three)}, 9, 9));

  // A >= 0
  // A < 2
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &zero, &two)}, 6, 6));

  // A >= 1
  // A < 2
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &one, &two)}, 2, 2));

  // A >= 1
  // A < 3
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &one, &three)}, 5, 5));

  // A >= 3
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &three, nullptr)}, 1, 1));

  // A >= 4
  // A < 5
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &four, &five)}, 1, 1));

  // A >= -1
  // A < 0
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &neg_one, &zero)}, 4, 4));

  // A >= 5
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &five, nullptr)}, 1, 1));

  // A < -1
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), nullptr, &neg_one)}, 4, 4));

  // B = 1
  NO_FATALS(check({ ColumnPredicate::Equality(schema.column(1), &one)}, 4, 4));

  // A >= 0
  // A < 2
  // B = 1
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), &zero, &two),
                    ColumnPredicate::Equality(schema.column(1), &one)}, 2, 2));

  // A < 0
  // B = 1
  NO_FATALS(check({ ColumnPredicate::Range(schema.column(0), nullptr, &zero),
                    ColumnPredicate::Equality(schema.column(1), &one)}, 1, 1));
}

// Test for the functionality of PartitionPruner::PrepareRangeSet() method.
class PartitionPrunerRangeSetTest : public PartitionPrunerTest {
 public:
  struct TestStruct {
    const string description;
    const string scan_lower_bound;
    const string scan_upper_bound;
    const PartitionSchema::HashSchema& table_wide_hash_schema;
    const PartitionSchema::RangesWithHashSchemas ranges_with_custom_hash_schemas;
    const PartitionSchema::RangesWithHashSchemas expected_result;
  };

  static void DoCheck(const TestStruct& t) {
    PartitionSchema::RangesWithHashSchemas result_ranges;
    PartitionPruner::PrepareRangeSet(
        t.scan_lower_bound, t.scan_upper_bound, t.table_wide_hash_schema,
        t.ranges_with_custom_hash_schemas,
        &result_ranges);
    SCOPED_TRACE(t.description);
    ASSERT_EQ(t.expected_result.size(), result_ranges.size())
        << result_ranges;
    for (auto i = 0; i < result_ranges.size(); ++i) {
      SCOPED_TRACE(Substitute("range $0", i));
      const auto& lhs = t.expected_result[i];
      const auto& rhs = result_ranges[i];
      ASSERT_EQ(lhs.lower, rhs.lower);
      ASSERT_EQ(lhs.upper, rhs.upper);
      ASSERT_EQ(lhs.hash_schema.size(), rhs.hash_schema.size());
      for (auto j = 0; j < lhs.hash_schema.size(); ++j) {
        SCOPED_TRACE(Substitute("hash dimension $0", j));
        ASSERT_EQ(lhs.hash_schema[j].num_buckets,
                  rhs.hash_schema[j].num_buckets);
      }
    }
  }
};

ostream& operator<<(ostream& os,
                    const PartitionSchema::RangeWithHashSchema& range) {
  os << "(" << (range.lower.empty() ? "*" : range.lower)
     << " " << (range.upper.empty() ? "*" : range.upper) << ")";
  return os;
}

ostream& operator<<(ostream& os,
                    const PartitionSchema::RangesWithHashSchemas& ranges) {
  for (const auto& range : ranges) {
    os << range << " ";
  }
  return os;
}

TEST_F(PartitionPrunerRangeSetTest, PrepareRangeSetX) {
  const PartitionSchema::HashSchema _2 = { { {ColumnId(0)}, 2, 0 } };
  const PartitionSchema::HashSchema _3 = { { {ColumnId(0)}, 3, 0 } };
  const PartitionSchema::HashSchema _4 = { { {ColumnId(0)}, 4, 0 } };
  const PartitionSchema::HashSchema _5 = { { {ColumnId(0)}, 5, 0 } };

  // For each element, there is a representation of the corresponding scenario
  // in the 'description' field: the first line is for the scan boundaries, the
  // second line is for the set of ranges with custom hash schemas. Two lines
  // are properly aligned to express the disposition of the ranges with custom
  // hash schemas vs the scan range. For the range bounds, the asterisk symbol
  // means "unlimited", i.e. no bound. Square [] and regular () parentheses
  // don't have the inclusivity/exclusivity semantics: they are rather to
  // distinguish scan bounds from range bounds.
  const vector<TestStruct> test_inputs_and_results = {
    {
R"*(
"[a b]"
""
)*",
      "a", "b", _2,
      {},
      { {"a", "b", _2} }
    },
    {
R"*(
"[a *]"
"(a *)"
)*",
      "a", "", _2,
      { {"a", "", _3} },
      { {"a", "", _3} }
    },
    {
R"*(
"[* b]"
"(* b)"
)*",
      "", "b", _2,
      { {"", "b", _3} },
      { {"", "b", _3} }
    },
    {
R"*(
"[*   c]"
"  (b c)"
)*",
      "", "c", _2,
      { {"b", "c", _3} },
      { {"", "b", _2}, {"b", "c", _3} }
    },
    {
R"*(
"[*   *]"
"(* b)"
)*",
      "", "", _2,
      { {"", "b", _3} },
      { {"", "b", _3}, {"b", "", _2} }
    },
    {
R"*(
"[*   *]"
"  (b *)"
)*",
      "", "", _2,
      { {"b", "", _3} },
      { { "", "b", _2}, {"b", "", _3} }
    },
    {
R"*(
"    [c d]"
"(a b)"
)*",
      "c", "d", _2,
      { {"a", "b", _3} },
      { {"c", "d", _2} }
    },
    {
R"*(
"    [c d]"
"(* b)"
)*",
      "c", "d", _2,
      { { "", "b", _3} },
      { {"c", "d", _2} }
    },
    {
R"*(
"       [c d]"
"(a b)(b c)"
)*",
      "c", "d", _2,
      { {"a", "b", _3}, {"b", "c", _4} },
      { {"c", "d", _2} }
    },
    {
R"*(
"         [d e]"
"(a b)  (c d)"
)*",
      "d", "e", _2,
      { {"a", "b", _3}, {"c", "d", _4} },
      { {"d", "e", _2} }
    },
    {
R"*(
"[a b]"
"    (c d)"
)*",
      "a", "b", _2,
      { {"c", "d", _3} },
      { {"a", "b", _2} }
    },
    {
R"*(
"[a b]"
"    (c *)"
)*",
      "a", "b", _2,
      { {"c",  "", _3} },
      { {"a", "b", _2} }
    },
    {
R"*(
"[* b]"
"    (c d)"
)*",
      "", "b", _2,
      { {"c", "d", _3} },
      { { "", "b", _2} }
    },
    {
R"*(
"[* b]"
"    (c *)"
)*",
      "", "b", _2,
      { {"c",  "", _3} },
      { { "", "b", _2} }
    },
    {
R"*(
"    [c d]"
"(a b)   (e f)"
)*",
      "c", "d", _2,
      { { "a", "b", _3 }, { "e", "f", _4 } },
      { { "c", "d", _2 } }
    },
    {
R"*(
"    [c d]"
"(* b)   (e f)"
)*",
      "c", "d", _2,
      { { "", "b", _3}, {"e", "f", _4} },
      { {"c", "d", _2} }
    },
    {
R"*(
"    [c d]"
"(a b)   (e *)"
)*",
      "c", "d", _2,
      { {"a", "b", _3}, {"e", "", _4} },
      { {"c", "d", _2} }
    },
    {
R"*(
"    [c d]"
"(* b)   (e *)"
)*",
      "c", "d", _2,
      { { "", "b", _3}, {"e", "", _4} },
      { {"c", "d", _2} }
    },
    {
R"*(
"  [b    c]"
"(a b)  (c d)"
)*",
      "b", "c", _2,
      { {"a", "b", _3}, {"c", "d", _4} },
      { {"b", "c", _2} }
    },
    {
R"*(
"  [b    c]"
"(* b)  (c d)"
)*",
      "b", "c", _2,
      { { "", "b", _3}, {"c", "d", _4} },
      { {"b", "c", _2} }
    },
    {
R"*(
"  [b    c]"
"(a b)  (c *)"
)*",
      "b", "c", _2,
      { {"a", "b", _3}, {"c",  "", _4} },
      { {"b", "c", _2} }
    },
    {
R"*(
"  [b    c]"
"(* b)  (c *)"
)*",
      "b", "c", _2,
      { { "", "b", _3}, {"c",  "", _4} },
      { {"b", "c", _2} }
    },
    {
R"*(
"  [b c]"
"(* b)"
)*",
      "b", "c", _2,
      { { "", "b", _3} },
      { {"b", "c", _2} }
    },
    {
R"*(
"  [b c]"
"(a b)"
)*",
      "b", "c", _2,
      { {"a", "b", _3} },
      { {"b", "c", _2} }
    },
    {
R"*(
"[a b]"
"   (b c)"
)*",
      "a", "b", _2,
      { {"b", "c", _3} },
      { {"a", "b", _2} }
    },
    {
R"*(
"[a b]"
"  (b *)"
)*",
      "a", "b", _2,
      { {"b",  "", _3} },
      { {"a", "b", _2} }
    },
    {
R"*(
"[a   c]"
"(a b)"
)*",
      "a", "c", _2,
      { {"a", "b", _3} },
      { {"a", "b", _3}, {"b", "c", _2} }
    },
    {
R"*(
"[a   c]"
"  (b c)"
)*",
      "a", "c", _2,
      { {"b", "c", _3} },
      { {"a", "b", _2}, {"b", "c", _3} }
    },
    {
R"*(
"[a   *]"
"  (b *)"
)*",
      "a", "", _2,
      { {"b",  "", _3} },
      { {"a", "b", _2}, {"b",  "", _3} }
    },
    {
R"*(
"[*   b]"
"(* a)"
)*",
      "", "b", _2,
      { { "", "a", _3} },
      { { "", "a", _3}, {"a", "b", _2} }
    },
    {
R"*(
"[a     d]"
"  (b c)"
)*",
      "a", "d", _2,
      { {"b", "c", _3} },
      { {"a", "b", _2}, {"b", "c", _3}, {"c", "d", _2} }
    },
    {
R"*(
"[*     d]"
"  (b c)"
)*",
      "", "d", _2,
      { {"b", "c", _3} },
      { { "", "b", _2}, {"b", "c", _3}, {"c", "d", _2} }
    },
    {
R"*(
"[*     *]"
"  (b c)"
)*",
      "", "", _2,
      { {"b", "c", _3} },
      { { "", "b", _2}, {"b", "c", _3}, {"c", "", _2} }
    },
    {
R"*(
"  [b c]"
"(a     d)"
)*",
      "b", "c", _2,
      { {"a", "d", _3} },
      { {"b", "c", _3} }
    },
    {
R"*(
"  [b c]"
"(a     *)"
)*",
      "b", "c", _2,
      { {"a",  "", _3} },
      { {"b", "c", _3} }
    },
    {
R"*(
"  [b c]"
"(*     d)"
)*",
      "b", "c", _2,
      { { "", "d", _3} },
      { {"b", "c", _3} }
    },
    {
R"*(
"  [b c]"
"(*     *)"
)*",
      "b", "c", _2,
      { { "",  "", _3} },
      { {"b", "c", _3} }
    },
    {
R"*(
"[a   c]"
"  (b   *)"
)*",
      "a", "c", _2,
      { {"b",  "", _3} },
      { {"a", "b", _2}, {"b", "c", _3} }
    },
    {
R"*(
"[a      c]"
"(a b)(b c)"
)*",
      "a", "c", _2,
      { {"a", "b", _3}, {"b", "c", _4} },
      { {"a", "b", _3}, {"b", "c", _4} }
    },
    {
R"*(
"[a      c]"
"(a b)(b   *)"
)*",
      "a", "c", _2,
      { {"a", "b", _3}, {"b",  "", _4} },
      { {"a", "b", _3}, {"b", "c", _4} }
    },
    {
R"*(
"[a          e]"
"  (b c)(c d)"
)*",
      "a", "e", _2,
      { {"b", "c", _3}, {"c", "d", _4} },
      { {"a", "b", _2}, {"b", "c", _3}, {"c", "d", _4}, {"d", "e", _2} }
    },
    {
R"*(
"[a          *]"
"  (b c)  (d *)"
)*",
      "a", "", _2,
      { {"b", "c", _3}, {"d",  "", _4} },
      { {"a", "b", _2}, {"b", "c", _3}, {"c", "d", _2}, {"d",  "", _4} }
    },
    {
R"*(
"[a          f]"
"  (b c)  (d   *)"
)*",
      "a", "f", _2,
      { {"b", "c", _3}, {"d",  "", _4} },
      { {"a", "b", _2}, {"b", "c", _3}, {"c", "d", _2}, {"d", "f", _4} }
    },
    {
R"*(
"[a            f]"
"  (b c)  (d e)"
)*",
      "a", "f", _2,
      { {"b", "c", _3}, {"d", "e", _4} },
      {
        {"a", "b", _2},
        {"b", "c", _3},
        {"c", "d", _2},
        {"d", "e", _4},
        {"e", "f", _2},
      }
    },
    {
R"*(
"[a            *]"
"  (b c)  (d e)"
)*",
      "a", "", _2,
      { {"b", "c", _3}, {"d", "e", _4} },
      {
        {"a", "b", _2},
        {"b", "c", _3},
        {"c", "d", _2},
        {"d", "e", _4},
        {"e",  "", _2},
      }
    },
    {
R"*(
"[*            f]"
"  (b c)  (d e)"
)*",
      "", "f", _2,
      { {"b", "c", _3}, {"d", "e", _4} },
      {
        { "", "b", _2},
        {"b", "c", _3},
        {"c", "d", _2},
        {"d", "e", _4},
        {"e", "f", _2},
      }
    },
    {
R"*(
"[a     d]"
"  (b c) (e f)"
)*",
      "a", "d", _2,
      { {"b", "c", _3}, {"e", "f", _4} },
      { {"a", "b", _2}, {"b", "c", _3}, {"c", "d", _2} }
    },
    {
R"*(
"  [b        e]"
"(a   c)  (d   *)"
)*",
      "b", "e", _2,
      { {"a", "c", _3}, {"d", "", _4} },
      { {"b", "c", _3}, {"c", "d", _2}, {"d", "e", _4} }
    },
    {
R"*(
"[*          e]"
"  (b c)  (d   *)"
)*",
      "", "e", _2,
      { {"b", "c", _3}, {"d", "", _4} },
      { { "", "b", _2}, {"b", "c", _3}, {"c", "d", _2}, {"d", "e", _4} }
    },
    {
R"*(
"  [b           e]"
"(a b)  (c d)  (e f)"
)*",
      "b", "e", _2,
      { {"a", "b", _3}, {"c", "d", _4}, {"e", "f", _5}, },
      { {"b", "c", _2}, {"c", "d", _4}, {"d", "e", _2}, }
    },
    {
R"*(
"  [b           e]"
"(* b)  (c d)  (e f)"
)*",
      "b", "e", _2,
      { { "", "b", _3}, {"c", "d", _4}, {"e", "f", _5}, },
      { {"b", "c", _2}, {"c", "d", _4}, {"d", "e", _2}, }
    },
    {
R"*(
"  [b           e]"
"(a b)  (c d)  (e *)"
)*",
      "b", "e", _2,
      { {"a", "b", _3}, {"c", "d", _4}, {"e",  "", _5}, },
      { {"b", "c", _2}, {"c", "d", _4}, {"d", "e", _2}, }
    },
    {
R"*(
"  [b           e]"
"(* b)  (c d)  (e *)"
)*",
      "b", "e", _2,
      { { "", "b", _3}, {"c", "d", _4}, {"e",  "", _5}, },
      { {"b", "c", _2}, {"c", "d", _4}, {"d", "e", _2}, }
    },
    {
R"*(
"[*             e]"
"(* b)  (c d)  (e *)"
)*",
      "", "e", _2,
      { { "", "b", _3}, {"c", "d", _4}, {"e",  "", _5}, },
      { { "", "b", _3}, {"b", "c", _2}, {"c", "d", _4}, {"d", "e", _2}, }
    },
  };

  for (const auto& scenario : test_inputs_and_results) {
    NO_FATALS(DoCheck(scenario));
  }
}

} // namespace kudu
