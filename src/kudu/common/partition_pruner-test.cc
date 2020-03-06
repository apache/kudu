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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using boost::optional;
using std::count_if;
using std::get;
using std::make_tuple;
using std::move;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::vector;

namespace kudu {

class PartitionPrunerTest : public KuduTest {
};

void CheckPrunedPartitions(const Schema& schema,
                           const PartitionSchema& partition_schema,
                           const vector<Partition>& partitions,
                           const ScanSpec& spec,
                           size_t remaining_tablets,
                           size_t pruner_ranges) {

  ScanSpec opt_spec(spec);
  AutoReleasePool p;
  Arena arena(256);
  opt_spec.OptimizeScan(schema, &arena, &p, false);

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
  ASSERT_EQ(pruner_ranges, pruner.NumRangesRemainingForTests());
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
  auto Check = [&] (optional<tuple<int8_t, int8_t, int8_t>> lower,
                    optional<tuple<int8_t, int8_t, int8_t>> upper,
                    size_t remaining_tablets) {
    ScanSpec spec;
    KuduPartialRow lower_bound(&schema);
    KuduPartialRow upper_bound(&schema);
    unique_ptr<EncodedKey> enc_lower_bound;
    unique_ptr<EncodedKey> enc_upper_bound;

    if (lower) {
      CHECK_OK(lower_bound.SetInt8("a", get<0>(*lower)));
      CHECK_OK(lower_bound.SetInt8("b", get<1>(*lower)));
      CHECK_OK(lower_bound.SetInt8("c", get<2>(*lower)));
      ConstContiguousRow row(lower_bound.schema(), lower_bound.row_data_);
      enc_lower_bound = EncodedKey::FromContiguousRow(row);
      spec.SetLowerBoundKey(enc_lower_bound.get());
    }
    if (upper) {
      CHECK_OK(upper_bound.SetInt8("a", get<0>(*upper)));
      CHECK_OK(upper_bound.SetInt8("b", get<1>(*upper)));
      CHECK_OK(upper_bound.SetInt8("c", get<2>(*upper)));
      ConstContiguousRow row(upper_bound.schema(), upper_bound.row_data_);
      enc_upper_bound = EncodedKey::FromContiguousRow(row);
      spec.SetExclusiveUpperBoundKey(enc_upper_bound.get());
    }
    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // No bounds
  Check(boost::none, boost::none, 3);

  // PK < (-1, min, min)
  Check(boost::none,
        make_tuple<int8_t, int8_t, int8_t>(-1, INT8_MIN, INT8_MIN),
        1);

  // PK < (10, 10, 10)
  Check(boost::none,
        make_tuple<int8_t, int8_t, int8_t>(10, 10, 10),
        2);

  // PK < (100, min, min)
  Check(boost::none,
        make_tuple<int8_t, int8_t, int8_t>(100, INT8_MIN, INT8_MIN),
        3);

  // PK >= (-10, -10, -10)
  Check(make_tuple<int8_t, int8_t, int8_t>(-10, -10, -10),
        boost::none,
        3);

  // PK >= (0, 0, 0)
  Check(make_tuple<int8_t, int8_t, int8_t>(0, 0, 0),
        boost::none,
        2);

  // PK >= (100, 0, 0)
  Check(make_tuple<int8_t, int8_t, int8_t>(100, 0, 0),
        boost::none,
        1);

  // PK >= (-10, 0, 0)
  // PK  < (100, 0, 0)
  Check(make_tuple<int8_t, int8_t, int8_t>(-10, 0, 0),
        make_tuple<int8_t, int8_t, int8_t>(100, 0, 0),
        3);

  // PK >= (0, 0, 0)
  // PK  < (10, 10, 10)
  Check(make_tuple<int8_t, int8_t, int8_t>(0, 0, 0),
        make_tuple<int8_t, int8_t, int8_t>(10, 10, 10),
        1);

  // PK >= (0, 0, 0)
  // PK  < (10, 10, 11)
  Check(make_tuple<int8_t, int8_t, int8_t>(0, 0, 0),
        make_tuple<int8_t, int8_t, int8_t>(10, 10, 11),
        2);

  // PK  < (0, 0, 0)
  // PK >= (10, 10, 11)
  Check(make_tuple<int8_t, int8_t, int8_t>(10, 10, 11),
        make_tuple<int8_t, int8_t, int8_t>(0, 0, 0),
        0);
}

TEST_F(PartitionPrunerTest, TestPartialPrimaryKeyRangePruning) {
  // CREATE TABLE t
  // (a INT8, b STRING, c STRING, PRIMARY KEY (a, b, c))
  // DISTRIBUTE BY RANGE(a, b)
  // SPLIT ROWS [(0, "m"), (10, "r"];

  // Setup the Schema
  Schema schema({ ColumnSchema("a", INT8),
      ColumnSchema("b", STRING),
      ColumnSchema("c", STRING) },
      { ColumnId(0), ColumnId(1), ColumnId(2) },
      3);

  PartitionSchema partition_schema;
  auto pb = PartitionSchemaPB();
  auto range_schema = pb.mutable_range_schema();
  range_schema->add_columns()->set_name("a");
  range_schema->add_columns()->set_name("b");
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
  auto Check = [&] (optional<tuple<int8_t, string, string>> lower,
                    optional<tuple<int8_t, string, string>> upper,
                    size_t remaining_tablets ) {
    ScanSpec spec;
    KuduPartialRow lower_bound(&schema);
    KuduPartialRow upper_bound(&schema);
    unique_ptr<EncodedKey> enc_lower_bound;
    unique_ptr<EncodedKey> enc_upper_bound;

    if (lower) {
      CHECK_OK(lower_bound.SetInt8("a", get<0>(*lower)));
      CHECK_OK(lower_bound.SetStringCopy("b", get<1>(*lower)));
      CHECK_OK(lower_bound.SetStringCopy("c", get<2>(*lower)));
      ConstContiguousRow row(lower_bound.schema(), lower_bound.row_data_);
      enc_lower_bound = EncodedKey::FromContiguousRow(row);
      spec.SetLowerBoundKey(enc_lower_bound.get());
    }
    if (upper) {
      CHECK_OK(upper_bound.SetInt8("a", get<0>(*upper)));
      CHECK_OK(upper_bound.SetStringCopy("b", get<1>(*upper)));
      CHECK_OK(upper_bound.SetStringCopy("c", get<2>(*upper)));
      ConstContiguousRow row(upper_bound.schema(), upper_bound.row_data_);
      enc_upper_bound = EncodedKey::FromContiguousRow(row);
      spec.SetExclusiveUpperBoundKey(enc_upper_bound.get());
    }
    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // No bounds
  Check(boost::none, boost::none, 3);

  // PK < (-1, min, "")
  Check(boost::none, make_tuple<int8_t, string, string>(-1, "", ""), 1);

  // PK < (10, "r", "")
  Check(boost::none, make_tuple<int8_t, string, string>(10, "r", ""), 2);

  // PK < (10, "r", "z")
  Check(boost::none, make_tuple<int8_t, string, string>(10, "r", "z"), 3);

  // PK < (100, min, "")
  Check(boost::none, make_tuple<int8_t, string, string>(100, "", ""), 3);

  // PK >= (-10, "m", "")
  Check(make_tuple<int8_t, string, string>(-10, "m", ""), boost::none, 3);

  // PK >= (0, "", "")
  Check(make_tuple<int8_t, string, string>(0, "", ""), boost::none, 3);

  // PK >= (0, "m", "")
  Check(make_tuple<int8_t, string, string>(0, "m", ""), boost::none, 2);

  // PK >= (100, "", "")
  Check(make_tuple<int8_t, string, string>(100, "", ""), boost::none, 1);

  // PK >= (-10, 0, "")
  // PK  < (100, 0, "")
  Check(make_tuple<int8_t, string, string>(-10, "", ""),
        make_tuple<int8_t, string, string>(100, "", ""), 3);

  // PK >= (0, "m", "")
  // PK  < (10, "r", "")
  Check(make_tuple<int8_t, string, string>(0, "m", ""),
        make_tuple<int8_t, string, string>(10, "r", ""), 1);

  // PK >= (0, "m", "")
  // PK  < (10, "r", "z")
  Check(make_tuple<int8_t, string, string>(0, "m", ""),
        make_tuple<int8_t, string, string>(10, "r", "z"), 2);

  // PK >= (0, "", "")
  // PK  < (10, "m", "z")
  Check(make_tuple<int8_t, string, string>(0, "", ""),
        make_tuple<int8_t, string, string>(10, "m", "z"), 2);

  // PK >= (10, "m", "")
  // PK  < (10, "m", "z")
  Check(make_tuple<int8_t, string, string>(10, "m", ""),
        make_tuple<int8_t, string, string>(10, "m", "z"), 1);
}

TEST_F(PartitionPrunerTest, TestIntPartialPrimaryKeyRangePruning) {
  // CREATE TABLE t
  // (a INT8, b INT8, c INT8, PRIMARY KEY (a, b, c))
  // DISTRIBUTE BY RANGE(a, b)
  // SPLIT ROWS [(0, 0)];

  // Setup the Schema
  Schema schema({ ColumnSchema("a", INT8),
                  ColumnSchema("b", INT8),
                  ColumnSchema("c", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) },
                3);

  PartitionSchema partition_schema;
  auto pb = PartitionSchemaPB();
  auto range_schema = pb.mutable_range_schema();
  range_schema->add_columns()->set_name("a");
  range_schema->add_columns()->set_name("b");
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  KuduPartialRow split(&schema);
  ASSERT_OK(split.SetInt8("a", 0));
  ASSERT_OK(split.SetInt8("b", 0));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({ split }, {}, schema, &partitions));

  // Applies the specified lower and upper bound primary keys against the
  // schema, and checks that the expected number of partitions are pruned.
  auto Check = [&] (optional<tuple<int8_t, int8_t, int8_t>> lower,
                    optional<tuple<int8_t, int8_t, int8_t>> upper,
                    size_t remaining_tablets ) {
    ScanSpec spec;
    KuduPartialRow lower_bound(&schema);
    KuduPartialRow upper_bound(&schema);
    unique_ptr<EncodedKey> enc_lower_bound;
    unique_ptr<EncodedKey> enc_upper_bound;

    if (lower) {
      CHECK_OK(lower_bound.SetInt8("a", get<0>(*lower)));
      CHECK_OK(lower_bound.SetInt8("b", get<1>(*lower)));
      CHECK_OK(lower_bound.SetInt8("c", get<2>(*lower)));
      ConstContiguousRow row(lower_bound.schema(), lower_bound.row_data_);
      enc_lower_bound = EncodedKey::FromContiguousRow(row);
      spec.SetLowerBoundKey(enc_lower_bound.get());
    }
    if (upper) {
      CHECK_OK(upper_bound.SetInt8("a", get<0>(*upper)));
      CHECK_OK(upper_bound.SetInt8("b", get<1>(*upper)));
      CHECK_OK(upper_bound.SetInt8("c", get<2>(*upper)));
      ConstContiguousRow row(upper_bound.schema(), upper_bound.row_data_);
      enc_upper_bound = EncodedKey::FromContiguousRow(row);
      spec.SetExclusiveUpperBoundKey(enc_upper_bound.get());
    }
    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  // No bounds
  Check(boost::none, boost::none, 2);

  // PK < (0, 0, min)
  Check(boost::none, make_tuple<int8_t, int8_t, int8_t>(0, INT8_MIN, INT8_MIN), 1);

  // PK < (0, 0, 0);
  Check(boost::none, make_tuple<int8_t, int8_t, int8_t>(0, 0, 0), 2);

  // PK < (0, max, 0);
  Check(boost::none, make_tuple<int8_t, int8_t, int8_t>(INT8_MAX, INT8_MAX, 0), 2);

  // PK < (max, max, min);
  Check(boost::none, make_tuple<int8_t, int8_t, int8_t>(INT8_MAX, INT8_MAX, INT8_MIN), 2);

  // PK < (max, max, 0);
  Check(boost::none, make_tuple<int8_t, int8_t, int8_t>(INT8_MAX, INT8_MAX, 0), 2);

  // PK >= (0, 0, 0);
  Check(make_tuple<int8_t, int8_t, int8_t>(0, 0, 0), boost::none, 1);

  // PK >= (0, 0, -1);
  Check(make_tuple<int8_t, int8_t, int8_t>(0, 0, -1), boost::none, 1);

  // PK >= (0, 0, min);
  Check(make_tuple<int8_t, int8_t, int8_t>(0, 0, INT8_MIN), boost::none, 1);
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

  PartitionSchema partition_schema;
  auto pb = PartitionSchemaPB();
  auto range_schema = pb.mutable_range_schema();
  range_schema->add_columns()->set_name("c");
  range_schema->add_columns()->set_name("b");
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
  auto Check = [&] (const vector<ColumnPredicate>& predicates, size_t remaining_tablets) {
    ScanSpec spec;

    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }

    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  int8_t neg_ten = -10;
  int8_t zero = 0;
  int8_t five = 5;
  int8_t ten = 10;
  int8_t hundred = 100;
  int8_t min = INT8_MIN;
  int8_t max = INT8_MAX;

  Slice empty = "";
  Slice a = "a";
  Slice m = "m";
  Slice m0 = Slice("m\0", 2);
  Slice r = "r";
  Slice z = "z";

  // No Bounds
  Check({}, 3);

  // c < -10
  Check({ ColumnPredicate::Range(schema.column(2), nullptr, &neg_ten) }, 1);

  // c = -10
  Check({ ColumnPredicate::Equality(schema.column(2), &neg_ten) }, 1);

  // c < 10
  Check({ ColumnPredicate::Range(schema.column(2), nullptr, &ten) }, 2);

  // c < 100
  Check({ ColumnPredicate::Range(schema.column(2), nullptr, &hundred) }, 3);

  // c < MIN
  Check({ ColumnPredicate::Range(schema.column(2), nullptr, &min) }, 0);

  // c < MAX
  Check({ ColumnPredicate::Range(schema.column(2), nullptr, &max) }, 3);

  // c >= -10
  Check({ ColumnPredicate::Range(schema.column(0), &neg_ten, nullptr) }, 3);

  // c >= 0
  Check({ ColumnPredicate::Range(schema.column(2), &zero, nullptr) }, 3);

  // c >= 5
  Check({ ColumnPredicate::Range(schema.column(2), &five, nullptr) }, 2);

  // c >= 10
  Check({ ColumnPredicate::Range(schema.column(2), &ten, nullptr) }, 2);

  // c >= 100
  Check({ ColumnPredicate::Range(schema.column(2), &hundred, nullptr) }, 1);

  // c >= MIN
  Check({ ColumnPredicate::Range(schema.column(2), &min, nullptr) }, 3);

  // c >= MAX
  Check({ ColumnPredicate::Range(schema.column(2), &max, nullptr) }, 1);

  // c = MIN
  Check({ ColumnPredicate::Equality(schema.column(2), &min) }, 1);

  // c = MAX
  Check({ ColumnPredicate::Equality(schema.column(2), &max) }, 1);

  // c >= -10
  // c < 0
  Check({ ColumnPredicate::Range(schema.column(2), &neg_ten, &zero) }, 1);

  // c >= 5
  // c < 100
  Check({ ColumnPredicate::Range(schema.column(2), &five, &hundred) }, 2);

  // b = ""
  Check({ ColumnPredicate::Equality(schema.column(1), &empty) }, 3);

  // b >= "z"
  Check({ ColumnPredicate::Range(schema.column(1), &z, nullptr) }, 3);

  // b < "a"
  Check({ ColumnPredicate::Range(schema.column(1), nullptr, &a) }, 3);

  // b >= "m"
  // b < "z"
  Check({ ColumnPredicate::Range(schema.column(1), &m, &z) }, 3);

  // c >= 10
  // b >= "r"
  Check({ ColumnPredicate::Range(schema.column(2), &ten, nullptr),
          ColumnPredicate::Range(schema.column(1), &r, nullptr) },
        1);

  // c >= 10
  // b < "r"
  Check({ ColumnPredicate::Range(schema.column(2), &ten, nullptr),
          ColumnPredicate::Range(schema.column(1), nullptr, &r) },
        2);

  // c = 10
  // b < "r"
  Check({ ColumnPredicate::Equality(schema.column(2), &ten),
          ColumnPredicate::Range(schema.column(1), nullptr, &r) },
        1);

  // c < 0
  // b < "m"
  Check({ ColumnPredicate::Range(schema.column(2), nullptr, &zero),
          ColumnPredicate::Range(schema.column(1), nullptr, &m) },
        1);

  // c < 0
  // b < "z"
  Check({ ColumnPredicate::Range(schema.column(2), nullptr, &zero),
          ColumnPredicate::Range(schema.column(1), nullptr, &z) },
        1);

  // c = 0
  // b = "m\0"
  Check({ ColumnPredicate::Equality(schema.column(2), &zero),
          ColumnPredicate::Equality(schema.column(1), &m0) },
        1);

  // c = 0
  // b < "m"
  Check({ ColumnPredicate::Equality(schema.column(2), &zero),
          ColumnPredicate::Range(schema.column(1), nullptr, &m) },
        1);

  // c = 0
  // b < "m\0"
  Check({ ColumnPredicate::Equality(schema.column(2), &zero),
          ColumnPredicate::Range(schema.column(1), nullptr, &m0) },
        2);

  // c IS NOT NULL
  Check({ ColumnPredicate::IsNotNull(schema.column(2)) }, 3);
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

    PartitionSchema partition_schema;
    auto pb = PartitionSchemaPB();
    pb.mutable_range_schema()->Clear();
    auto hash_component_1 = pb.add_hash_bucket_schemas();
    hash_component_1->add_columns()->set_name("a");
    hash_component_1->set_num_buckets(2);
    auto hash_component_2 = pb.add_hash_bucket_schemas();
    hash_component_2->add_columns()->set_name("b");
    hash_component_2->add_columns()->set_name("c");
    hash_component_2->set_num_buckets(2);

    ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

    vector<Partition> partitions;
    ASSERT_OK(partition_schema.CreatePartitions(vector<KuduPartialRow>(), {}, schema, &partitions));


  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  auto Check = [&] (const vector<ColumnPredicate>& predicates,
                    size_t remaining_tablets,
                    size_t pruner_ranges) {
    ScanSpec spec;

    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }

    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  int8_t zero = 0;
  int8_t one = 1;
  int8_t two = 2;

  // No Bounds
  Check({}, 4, 1);

  // a = 0;
  Check({ ColumnPredicate::Equality(schema.column(0), &zero) }, 2, 1);

  // a >= 0;
  Check({ ColumnPredicate::Range(schema.column(0), &zero, nullptr) }, 4, 1);

  // a >= 0;
  // a < 1;
  Check({ ColumnPredicate::Range(schema.column(0), &zero, &one) }, 2, 1);

  // a >= 0;
  // a < 2;
  Check({ ColumnPredicate::Range(schema.column(0), &zero, &two) }, 4, 1);

  // b = 1;
  Check({ ColumnPredicate::Equality(schema.column(1), &one) }, 4, 1);

  // b = 1;
  // c = 2;
  Check({ ColumnPredicate::Equality(schema.column(1), &one),
          ColumnPredicate::Equality(schema.column(2), &two) },
        2, 2);

  // a = 0;
  // b = 1;
  // c = 2;
  Check({ ColumnPredicate::Equality(schema.column(0), &zero),
          ColumnPredicate::Equality(schema.column(1), &one),
          ColumnPredicate::Equality(schema.column(2), &two) },
        1, 1);
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

  PartitionSchema partition_schema;
  auto pb = PartitionSchemaPB();
  auto hash_component_1 = pb.add_hash_bucket_schemas();
  hash_component_1->add_columns()->set_name("a");
  hash_component_1->set_num_buckets(3);
  hash_component_1->set_seed(0);
  auto hash_component_2 = pb.add_hash_bucket_schemas();
  hash_component_2->add_columns()->set_name("b");
  hash_component_2->set_num_buckets(3);
  hash_component_2->set_seed(0);
  auto hash_component_3 = pb.add_hash_bucket_schemas();
  hash_component_3->add_columns()->set_name("c");
  hash_component_3->set_num_buckets(3);
  hash_component_3->set_seed(0);
  pb.mutable_range_schema()->clear_columns();

  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(vector<KuduPartialRow>(), {}, schema, &partitions));


  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  auto Check = [&] (const vector<ColumnPredicate>& predicates,
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
  int8_t zero = 0;
  int8_t one = 1;
  int8_t eight = 8;

  vector<const void*> a_values;
  vector<const void*> b_values;
  vector<const void*> c_values;

  // a in [0, 1];
  a_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(0), &a_values) }, 18, 2);

  // a in [0, 1, 8];
  a_values = { &zero, &one, &eight };
  Check({ ColumnPredicate::InList(schema.column(0), &a_values) }, 27, 1);

  // b in [0, 1]
  b_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(1), &b_values) }, 18, 6);

  // c in [0, 1]
  c_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(2), &c_values) }, 18, 18);

  // b in [0, 1], c in [0, 1]
  b_values = { &zero, &one };
  c_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(1), &b_values),
          ColumnPredicate::InList(schema.column(2), &c_values) },
        12, 12);

  //a in [0, 1], b in [0, 1], c in [0, 1]
  a_values = { &zero, &one };
  b_values = { &zero, &one };
  c_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(0), &a_values),
          ColumnPredicate::InList(schema.column(1), &b_values),
          ColumnPredicate::InList(schema.column(2), &c_values) },
        8, 8);
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

  PartitionSchema partition_schema;
  auto pb = PartitionSchemaPB();
  auto hash_component_1 = pb.add_hash_bucket_schemas();
  hash_component_1->add_columns()->set_name("a");
  hash_component_1->set_num_buckets(3);
  hash_component_1->set_seed(0);
  auto hash_component_2 = pb.add_hash_bucket_schemas();
  hash_component_2->add_columns()->set_name("b");
  hash_component_2->add_columns()->set_name("c");
  hash_component_2->set_num_buckets(3);
  hash_component_2->set_seed(0);
  pb.mutable_range_schema()->clear_columns();

  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(vector<KuduPartialRow>(), {}, schema, &partitions));


  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  auto Check = [&] (const vector<ColumnPredicate>& predicates,
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
  int8_t zero = 0;
  int8_t one = 1;
  int8_t eight = 8;

  vector<const void*> a_values;
  vector<const void*> b_values;
  vector<const void*> c_values;

  // a in [0, 1];
  a_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(0), &a_values) }, 6, 2);

  // a in [0, 1, 8];
  a_values = { &zero, &one, &eight };
  Check({ ColumnPredicate::InList(schema.column(0), &a_values) }, 9, 1);

  // b in [0, 1]
  b_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(1), &b_values) }, 9, 1);

  // c in [0, 1]
  c_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(2), &c_values) }, 9, 1);

  // b in [0, 1], c in [0, 1]
  // (0, 0) in bucket 2
  // (0, 1) in bucket 2
  // (1, 0) in bucket 1
  // (1, 1) in bucket 0
  b_values = { &zero, &one };
  c_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(1), &b_values),
          ColumnPredicate::InList(schema.column(2), &c_values) },
        9, 1);

  // b = 0, c in [0, 1]
  c_values = { &zero, &one };
  Check({ ColumnPredicate::Equality(schema.column(1), &zero),
          ColumnPredicate::InList(schema.column(2), &c_values) },
        3, 3);

  // b = 1, c in [0, 1]
  c_values = { &zero, &one };
  Check({ ColumnPredicate::Equality(schema.column(1), &one),
          ColumnPredicate::InList(schema.column(2), &c_values) },
        6, 6);

  //a in [0, 1], b in [0, 1], c in [0, 1]
  a_values = { &zero, &one };
  b_values = { &zero, &one };
  c_values = { &zero, &one };
  Check({ ColumnPredicate::InList(schema.column(0), &a_values),
          ColumnPredicate::InList(schema.column(1), &b_values),
          ColumnPredicate::InList(schema.column(2), &c_values) },
        6, 2);
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

  PartitionSchema partition_schema;
  auto pb = PartitionSchemaPB();
  pb.mutable_range_schema()->add_columns()->set_name("time");

  auto hash = pb.add_hash_bucket_schemas();
  hash->add_columns()->set_name("host");
  hash->add_columns()->set_name("metric");
  hash->set_num_buckets(2);

  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  KuduPartialRow split(&schema);
  ASSERT_OK(split.SetUnixTimeMicros("time", 10));

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(vector<KuduPartialRow>{ split },
                                              {}, schema, &partitions));
  ASSERT_EQ(4, partitions.size());

  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  auto Check = [&] (const vector<ColumnPredicate>& predicates,
                    string lower_bound_partition_key,
                    string upper_bound_partition_key,
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

  int64_t nine = 9;
  int64_t ten = 10;
  int64_t twenty = 20;

  // host = "a"
  // metric = "a"
  // timestamp >= 9;
  Check({ ColumnPredicate::Equality(schema.column(0), &a),
          ColumnPredicate::Equality(schema.column(1), &a),
          ColumnPredicate::Range(schema.column(2), &nine, nullptr) },
        "", "",
        2, 1);

  // host = "a"
  // metric = "a"
  // timestamp >= 10;
  // timestamp < 20;
  Check({ ColumnPredicate::Equality(schema.column(0), &a),
          ColumnPredicate::Equality(schema.column(1), &a),
          ColumnPredicate::Range(schema.column(2), &ten, &twenty) },
        "", "",
        1, 1);

  // host = "a"
  // metric = "a"
  // timestamp < 10;
  Check({ ColumnPredicate::Equality(schema.column(0), &a),
          ColumnPredicate::Equality(schema.column(1), &a),
          ColumnPredicate::Range(schema.column(2), nullptr, &ten) },
        "", "",
        1, 1);

  // host = "a"
  // metric = "a"
  // timestamp >= 10;
  Check({ ColumnPredicate::Equality(schema.column(0), &a),
          ColumnPredicate::Equality(schema.column(1), &a),
          ColumnPredicate::Range(schema.column(2), &ten, nullptr) },
        "", "",
        1, 1);

  // host = "a"
  // metric = "a"
  // timestamp = 10;
  Check({ ColumnPredicate::Equality(schema.column(0), &a),
          ColumnPredicate::Equality(schema.column(1), &a),
          ColumnPredicate::Equality(schema.column(2), &ten) },
        "", "",
        1, 1);

  // partition key < (hash=1)
  Check({}, "", string("\0\0\0\1", 4), 2, 1);

  // partition key >= (hash=1)
  Check({}, string("\0\0\0\1", 4), "", 2, 1);

  // timestamp = 10
  // partition key < (hash=1)
  Check({ ColumnPredicate::Equality(schema.column(2), &ten) },
        "", string("\0\0\0\1", 4), 1, 1);

  // timestamp = 10
  // partition key >= (hash=1)
  Check({ ColumnPredicate::Equality(schema.column(2), &ten) },
        string("\0\0\0\1", 4), "", 1, 1);
}

TEST_F(PartitionPrunerTest, TestKudu2173) {
  // CREATE TABLE t
  // (a INT8, b INT8, PRIMARY KEY (a, b))
  // DISTRIBUTE BY RANGE(a)
  // SPLIT ROWS [(10)]

  // Setup the Schema
  Schema schema({ ColumnSchema("a", INT8),
          ColumnSchema("b", INT8)},
    { ColumnId(0), ColumnId(1) },
    2);

  PartitionSchema partition_schema;
  auto pb = PartitionSchemaPB();
  auto range_schema = pb.mutable_range_schema();
  range_schema->add_columns()->set_name("a");
  ASSERT_OK(PartitionSchema::FromPB(pb, schema, &partition_schema));

  KuduPartialRow split1(&schema);
  ASSERT_OK(split1.SetInt8("a", 10));
  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions({ split1 }, {}, schema, &partitions));

  // Applies the specified predicates to a scan and checks that the expected
  // number of partitions are pruned.
  auto Check = [&] (const vector<ColumnPredicate>& predicates, size_t remaining_tablets) {
    ScanSpec spec;

    for (const auto& pred : predicates) {
      spec.AddPredicate(pred);
    }
    size_t pruner_ranges = remaining_tablets == 0 ? 0 : 1;
    CheckPrunedPartitions(schema, partition_schema, partitions, spec,
                          remaining_tablets, pruner_ranges);
  };

  int8_t eleven = 11;
  int8_t max = INT8_MAX;

  // a < 11
  Check({ ColumnPredicate::Range(schema.column(0), nullptr, &eleven) }, 2);

  // a < 11 AND b < 11
  Check({ ColumnPredicate::Range(schema.column(0), nullptr, &eleven),
          ColumnPredicate::Range(schema.column(1), nullptr, &eleven) },
        2);

  // a < max
  Check({ ColumnPredicate::Range(schema.column(0), nullptr, &max) }, 2);

  // a < max AND b < 11
  Check({ ColumnPredicate::Range(schema.column(0), nullptr, &max),
          ColumnPredicate::Range(schema.column(1), nullptr, &eleven) },
        2);
}
} // namespace kudu
