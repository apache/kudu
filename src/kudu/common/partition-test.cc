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

#include <stdint.h>

#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>

#include <boost/optional.hpp>
#include <gflags/gflags_declare.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/hash_util.h"
#include "kudu/util/test_util.h"

using boost::optional;
using std::pair;
using std::string;
using std::vector;

namespace kudu {

namespace {
void AddHashBucketComponent(PartitionSchemaPB* partition_schema_pb,
                            const vector<string>& columns,
                            uint32_t num_buckets, int32_t seed) {
  PartitionSchemaPB::HashBucketSchemaPB* hash_bucket_schema =
      partition_schema_pb->add_hash_bucket_schemas();
  for (const string& column : columns) {
    hash_bucket_schema->add_columns()->set_name(column);
  }
  hash_bucket_schema->set_num_buckets(num_buckets);
  hash_bucket_schema->set_seed(seed);
}

void SetRangePartitionComponent(PartitionSchemaPB* partition_schema_pb,
                                const vector<string>& columns) {
  PartitionSchemaPB::RangeSchemaPB* range_schema = partition_schema_pb->mutable_range_schema();
  range_schema->Clear();
  for (const string& column : columns) {
    range_schema->add_columns()->set_name(column);
  }
}

void CheckCreateRangePartitions(const vector<pair<optional<string>, optional<string>>>& raw_bounds,
                                const vector<string>& raw_splits,
                                const vector<pair<string, string>>& expected_partition_ranges) {
  CHECK_EQ(std::max(raw_bounds.size(), 1UL) + raw_splits.size(), expected_partition_ranges.size());

  // CREATE TABLE t (col STRING PRIMARY KEY),
  // PARITITION BY RANGE (col);
  Schema schema({ ColumnSchema("col", STRING) }, { ColumnId(0) }, 1);

  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(PartitionSchemaPB(), schema, &partition_schema));

  ASSERT_EQ("RANGE (col)", partition_schema.DebugString(schema));

  vector<pair<KuduPartialRow, KuduPartialRow>> bounds;
  for (const auto& bound : raw_bounds) {
    const optional<string>& lower = bound.first;
    const optional<string>& upper = bound.second;
    KuduPartialRow lower_bound(&schema);
    KuduPartialRow upper_bound(&schema);
    if (lower) {
      ASSERT_OK(lower_bound.SetStringCopy("col", *lower));
    }
    if (upper) {
      ASSERT_OK(upper_bound.SetStringCopy("col", *upper));
    }
    bounds.emplace_back(std::move(lower_bound), std::move(upper_bound));
  }

  vector<KuduPartialRow> splits;
  for (const string& split_value : raw_splits) {
    KuduPartialRow split(&schema);
    ASSERT_OK(split.SetStringCopy("col", split_value));
    splits.emplace_back(split);
  }

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(splits, bounds, schema, &partitions));
  ASSERT_EQ(expected_partition_ranges.size(), partitions.size());

  for (int i = 0; i < partitions.size(); i++) {
    const string& lower = expected_partition_ranges[i].first;
    const string& upper = expected_partition_ranges[i].second;

    EXPECT_TRUE(partitions[i].hash_buckets().empty());
    EXPECT_EQ(lower, partitions[i].range_key_start());
    EXPECT_EQ(upper, partitions[i].range_key_end());
    EXPECT_EQ(lower, partitions[i].partition_key_start());
    EXPECT_EQ(upper, partitions[i].partition_key_end());
  }
}

} // namespace

class PartitionTest : public KuduTest {};

// Tests that missing values are correctly filled in with minimums when creating
// range partition keys, and that completely missing keys are encoded as the
// logical minimum and logical maximum for lower and upper bounds, respectively.
TEST_F(PartitionTest, TestCompoundRangeKeyEncoding) {

  // CREATE TABLE t (c1 STRING, c2 STRING, c3 STRING),
  // PRIMARY KEY (c1, c2, c3)
  // PARITITION BY RANGE (c1, c2, c3);
  Schema schema({ ColumnSchema("c1", STRING),
                  ColumnSchema("c2", STRING),
                  ColumnSchema("c3", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3);

  PartitionSchemaPB schema_builder;
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  ASSERT_EQ("RANGE (c1, c2, c3)", partition_schema.DebugString(schema));

  vector<pair<KuduPartialRow, KuduPartialRow>> bounds;
  vector<KuduPartialRow> splits;

  { // [(_, _, _), (_, _, b))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(upper.SetStringCopy("c3", "b"));
    bounds.emplace_back(std::move(lower), std::move(upper));
  }

  { // [(_, b, c), (d, _, f))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("c2", "b"));
    ASSERT_OK(lower.SetStringCopy("c3", "c"));
    ASSERT_OK(upper.SetStringCopy("c1", "d"));
    ASSERT_OK(upper.SetStringCopy("c3", "f"));
    bounds.emplace_back(std::move(lower), std::move(upper));
  }

  { // [(e, _, _), (_, _, _))
    KuduPartialRow lower(&schema);
    KuduPartialRow upper(&schema);
    ASSERT_OK(lower.SetStringCopy("c1", "e"));
    bounds.emplace_back(std::move(lower), std::move(upper));
  }

  { // split: (_, _, a)
    KuduPartialRow split(&schema);
    ASSERT_OK(split.SetStringCopy("c3", "a"));
    splits.emplace_back(std::move(split));
  }

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(splits, bounds, schema, &partitions));
  ASSERT_EQ(4, partitions.size());

  EXPECT_TRUE(partitions[0].hash_buckets().empty());

  EXPECT_EQ(R"(RANGE (c1, c2, c3) PARTITION VALUES < ("", "", "a"))",
            partition_schema.PartitionDebugString(partitions[0], schema));
  EXPECT_EQ(R"(RANGE (c1, c2, c3) PARTITION ("", "", "a") <= VALUES < ("", "", "b"))",
            partition_schema.PartitionDebugString(partitions[1], schema));
  EXPECT_EQ(R"(RANGE (c1, c2, c3) PARTITION ("", "b", "c") <= VALUES < ("d", "", "f"))",
            partition_schema.PartitionDebugString(partitions[2], schema));
  EXPECT_EQ(R"(RANGE (c1, c2, c3) PARTITION VALUES >= ("e", "", ""))",
            partition_schema.PartitionDebugString(partitions[3], schema));
}

TEST_F(PartitionTest, TestPartitionKeyEncoding) {
  // CREATE TABLE t (a INT32, b VARCHAR, c VARCHAR, PRIMARY KEY (a, b, c))
  // PARITITION BY [HASH BUCKET (a, b), HASH BUCKET (c), RANGE (a, b, c)];
  Schema schema({ ColumnSchema("a", INT32),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3);

  PartitionSchemaPB schema_builder;
  AddHashBucketComponent(&schema_builder, { "a", "b" }, 32, 0);
  AddHashBucketComponent(&schema_builder, { "c" }, 32, 42);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  ASSERT_EQ("HASH (a, b) PARTITIONS 32, HASH (c) PARTITIONS 32 SEED 42, RANGE (a, b, c)",
            partition_schema.DebugString(schema));

  {
    string key;
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 0));
    ASSERT_OK(partition_schema.EncodeKey(row, &key));

    EXPECT_EQ(string("\0\0\0\0"   // hash(0, "")
                     "\0\0\0\x14" // hash("")
                     "\x80\0\0\0" // a = 0
                     "\0\0",      // b = ""; c is elided
                     14), key);
    string expected = R"(HASH (a, b): 0, HASH (c): 20, RANGE (a, b, c): (0, "", ""))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));
  }

  {
    string key;
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 1));
    ASSERT_OK(partition_schema.EncodeKey(row, &key));

    EXPECT_EQ(string("\0\0\0\x5"    // hash(1, "")
                     "\0\0\0\x14"   // hash("")
                     "\x80\0\0\x01" // a = 1
                     "\0\0",        // b = ""; c is elided
                     14), key);

    string expected = R"(HASH (a, b): 5, HASH (c): 20, RANGE (a, b, c): (1, "", ""))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));
  }

  {
    string key;
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 0));
    ASSERT_OK(row.SetStringCopy("b", "b"));
    ASSERT_OK(row.SetStringCopy("c", "c"));
    ASSERT_OK(partition_schema.EncodeKey(row, &key));

    EXPECT_EQ(string("\0\0\0\x1A" // hash(0, "b")
                     "\0\0\0\x1D" // hash("c")
                     "\x80\0\0\0" // a = 0
                     "b\0\0"      // b = "b"
                     "c",         // c = "c"
                     16), key);

    string expected = R"(HASH (a, b): 26, HASH (c): 29, RANGE (a, b, c): (0, "b", "c"))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));
  }

  {
    string key;
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 1));
    ASSERT_OK(row.SetStringCopy("b", "b"));
    ASSERT_OK(row.SetStringCopy("c", "c"));
    ASSERT_OK(partition_schema.EncodeKey(row, &key));

    EXPECT_EQ(string("\0\0\0\x0"   // hash(1, "b")
                     "\0\0\0\x1D"  // hash("c")
                     "\x80\0\0\x1" // a = 1
                     "b\0\0"       // b = "b"
                     "c",          // c = "c"
                     16), key);

    string expected = R"(HASH (a, b): 0, HASH (c): 29, RANGE (a, b, c): (1, "b", "c"))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));
  }

  {
    // Check that row values are redacted when the log redact flag is set.
    ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));
    string key;
    KuduPartialRow row(&schema);
    ASSERT_OK(row.SetInt32("a", 1));
    ASSERT_OK(row.SetStringCopy("b", "b"));
    ASSERT_OK(row.SetStringCopy("c", "c"));
    ASSERT_OK(partition_schema.EncodeKey(row, &key));

    string expected =
      R"(HASH (a, b): 0, HASH (c): 29, RANGE (a, b, c): (<redacted>, <redacted>, <redacted>))";
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(row));
    EXPECT_EQ(expected, partition_schema.PartitionKeyDebugString(key, schema));

    // Check that row values are redacted from error messages when
    // --redact is set with 'log'.

    EXPECT_EQ("<hash-decode-error>",
              partition_schema.PartitionKeyDebugString(string("\0\1\0\1", 4), schema));
    EXPECT_EQ("HASH (a, b): 0, HASH (c): 0, RANGE (a, b, c): "
              "<range-key-decode-error: Invalid argument: "
              "Error decoding partition key range component 'a': key too short: <redacted>>",
              partition_schema.PartitionKeyDebugString(string("\0\0\0\0"
                                                              "\0\0\0\0"
                                                              "a", 9), schema));
  }
}

TEST_F(PartitionTest, TestCreateRangePartitions) {
  {
    // Splits:
    // { a: "1" }
    // { a: "2" }
    // { a: "2\0" }
    //
    // Encoded Partition Keys:
    // [ ( ""), ("1") )
    // [ ("1"), ("2") )
    // [ ("2"), ("2\0") )
    // [ ("2\0"), ("") )

    vector<string> splits { "2", "1", string("2\0", 2) };
    vector<pair<string, string>> partitions {
      { "", "1" },
      { "1", "2" },
      { "2", string("2\0", 2) },
      { string("2\0", 2), "" },
    };
    CheckCreateRangePartitions({}, splits, partitions);
  }
  {
    // Bounds
    //
    // ({ a: "a" }, { a: "m" })
    // ({ a: "x" }, { a: "z" })
    //
    // Encoded Partition Keys:
    //
    // [ ("a"), ("m") )
    // [ ("x"), ("z") )

    vector<pair<optional<string>, optional<string>>> bounds {
      { string("a"), string("m") },
      { string("x"), string("z") },
    };
    vector<pair<string, string>> partitions {
      { "a", "m" },
      { "x", "z" },
    };
    CheckCreateRangePartitions(bounds, {}, partitions);
  }
  {
    // Bounds:
    // ({          }, { col: "b" })
    // ({ col: "c" }, { col: "f" })
    // ({ col: "f" }, { col: "z" })
    //
    // Splits:
    // { col: "a"   }
    // { col: "d"   }
    // { col: "e"   }
    // { col: "h"   }
    // { col: "h\0" }
    //
    // Encoded Partition Keys:
    // [ (""),    ("a")   )
    // [ ("a"),   ("b")   )
    // [ ("c"),   ("d")   )
    // [ ("d"),   ("e")   )
    // [ ("e"),   ("f")   )
    // [ ("f"),   ("h")   )
    // [ ("h"),   ("h\0") )
    // [ ("h\0"), ("z")   )

    vector<pair<optional<string>, optional<string>>> bounds {
      { boost::none, string("b") },
      { string("c"), string("f") },
      { string("f"), string("z") },
    };
    vector<string> splits { "d", "a", "h", "e", string("h\0", 2) };
    vector<pair<string, string>> partitions {
      { "", "a" },
      { "a", "b" },
      { "c", "d" },
      { "d", "e" },
      { "e", "f" },
      { "f", "h" },
      { "h", string("h\0", 2) },
      { string("h\0", 2), "z" },
    };
    CheckCreateRangePartitions(bounds, splits, partitions);
  }
  {
    // Bounds:
    // ({ }, { })
    //
    // Splits:
    // { col: "m"   }
    //
    // Encoded Partition Keys:
    // [ (""),    ("m")   )
    // [ ("m"),   ("")   )

    vector<pair<optional<string>, optional<string>>> bounds {
      { boost::none, boost::none },
    };
    vector<string> splits { "m" };
    vector<pair<string, string>> partitions {
      { "", "m" },
      { "m", "" },
    };
    CheckCreateRangePartitions(bounds, splits, partitions);
  }
}

TEST_F(PartitionTest, TestCreateHashBucketPartitions) {
  // CREATE TABLE t (a VARCHAR PRIMARY KEY),
  // PARITITION BY [HASH BUCKET (a)];
  Schema schema({ ColumnSchema("a", STRING) }, { ColumnId(0) }, 1);

  PartitionSchemaPB schema_builder;
  SetRangePartitionComponent(&schema_builder, vector<string>());
  AddHashBucketComponent(&schema_builder, { "a" }, 3, 42);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  ASSERT_EQ("HASH (a) PARTITIONS 3 SEED 42", partition_schema.DebugString(schema));

  // Encoded Partition Keys:
  //
  // [ (_), (1) )
  // [ (1), (2) )
  // [ (3), (_) )

  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(vector<KuduPartialRow>(), {}, schema, &partitions));
  ASSERT_EQ(3, partitions.size());

  EXPECT_EQ(0, partitions[0].hash_buckets()[0]);
  EXPECT_EQ("", partitions[0].range_key_start());
  EXPECT_EQ("", partitions[0].range_key_end());
  EXPECT_EQ("", partitions[0].partition_key_start());
  EXPECT_EQ(string("\0\0\0\1", 4), partitions[0].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 0",
            partition_schema.PartitionDebugString(partitions[0], schema));

  EXPECT_EQ(1, partitions[1].hash_buckets()[0]);
  EXPECT_EQ("", partitions[1].range_key_start());
  EXPECT_EQ("", partitions[1].range_key_end());
  EXPECT_EQ(string("\0\0\0\1", 4), partitions[1].partition_key_start());
  EXPECT_EQ(string("\0\0\0\2", 4), partitions[1].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 1",
            partition_schema.PartitionDebugString(partitions[1], schema));

  EXPECT_EQ(2, partitions[2].hash_buckets()[0]);
  EXPECT_EQ("", partitions[2].range_key_start());
  EXPECT_EQ("", partitions[2].range_key_end());
  EXPECT_EQ(string("\0\0\0\2", 4), partitions[2].partition_key_start());
  EXPECT_EQ("", partitions[2].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 2",
            partition_schema.PartitionDebugString(partitions[2], schema));
}

TEST_F(PartitionTest, TestCreatePartitions) {
  // Explicitly enable redaction. It should have no effect on the subsequent
  // partition pretty printing tests, as partitions are metadata and thus not
  // redacted.
  ASSERT_NE("", gflags::SetCommandLineOption("redact", "log"));

  // CREATE TABLE t (a VARCHAR, b VARCHAR, c VARCHAR, PRIMARY KEY (a, b, c))
  // PARITITION BY [HASH BUCKET (a), HASH BUCKET (b), RANGE (a, b, c)];
  Schema schema({ ColumnSchema("a", STRING),
                  ColumnSchema("b", STRING),
                  ColumnSchema("c", STRING) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3);

  PartitionSchemaPB schema_builder;
  AddHashBucketComponent(&schema_builder, { "a" }, 2, 0);
  AddHashBucketComponent(&schema_builder, { "b" }, 2, 0);
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  ASSERT_EQ("HASH (a) PARTITIONS 2, HASH (b) PARTITIONS 2, RANGE (a, b, c)",
            partition_schema.DebugString(schema));

  // Split Rows:
  //
  // { a: "a1", b: "b1", c: "c1" }
  // { b: "a2", b: "b2" }
  //
  // non-specified column values default to the logical minimum value ("").
  //
  // Encoded Partition Keys:
  //
  // [ (_, _,        _), (0, 0, "a1b1c1") )
  // [ (0, 0, "a1b1c1"), (0, 0,   "a2b2") )
  // [ (0, 0,   "a2b2"), (0, 1,        _) )
  //
  // [ (0, 1,        _), (0, 1, "a1b1c1") )
  // [ (0, 1, "a1b1c1"), (0, 1,   "a2b2") )
  // [ (0, 1,   "a2b2"), (1, _,        _) )
  //
  // [ (1, _,        _), (1, 0, "a1b1c1") )
  // [ (1, 0, "a1b1c1"), (1, 0,   "a2b2") )
  // [ (1, 0,   "a2b2"), (1, 1,        _) )
  //
  // [ (1, 1,        _), (1, 1, "a1b1c1") )
  // [ (1, 1, "a1b1c1"), (1, 1,   "a2b2") )
  // [ (1, 1,   "a2b2"), (_, _,        _) )
  //
  // _ signifies that the value is omitted from the encoded partition key.

  KuduPartialRow split_a(&schema);
  ASSERT_OK(split_a.SetStringCopy("a", "a1"));
  ASSERT_OK(split_a.SetStringCopy("b", "b1"));
  ASSERT_OK(split_a.SetStringCopy("c", "c1"));
  string partition_key_a;
  ASSERT_OK(partition_schema.EncodeKey(split_a, &partition_key_a));

  KuduPartialRow split_b(&schema);
  ASSERT_OK(split_b.SetStringCopy("a", "a2"));
  ASSERT_OK(split_b.SetStringCopy("b", "b2"));
  string partition_key_b;
  ASSERT_OK(partition_schema.EncodeKey(split_b, &partition_key_b));

  // Split keys need not be passed in sorted order.
  vector<KuduPartialRow> split_rows = { split_b, split_a };
  vector<Partition> partitions;
  ASSERT_OK(partition_schema.CreatePartitions(split_rows, {}, schema, &partitions));
  ASSERT_EQ(12, partitions.size());

  EXPECT_EQ(0, partitions[0].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[0].hash_buckets()[1]);
  EXPECT_EQ("", partitions[0].range_key_start());
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[0].range_key_end());
  EXPECT_EQ("", partitions[0].partition_key_start());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a1\0\0b1\0\0c1", 18), partitions[0].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION VALUES < ("a1", "b1", "c1"))",
            partition_schema.PartitionDebugString(partitions[0], schema));

  EXPECT_EQ(0, partitions[1].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[1].hash_buckets()[1]);
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[1].range_key_start());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[1].range_key_end());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a1\0\0b1\0\0c1", 18),
            partitions[1].partition_key_start());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a2\0\0b2\0\0", 16), partitions[1].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION ("a1", "b1", "c1") <= VALUES < ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[1], schema));

  EXPECT_EQ(0, partitions[2].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[2].hash_buckets()[1]);
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[2].range_key_start());
  EXPECT_EQ("", partitions[2].range_key_end());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\0" "a2\0\0b2\0\0", 16), partitions[2].partition_key_start());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1", 8), partitions[2].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION VALUES >= ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[2], schema));

  EXPECT_EQ(0, partitions[3].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[3].hash_buckets()[1]);
  EXPECT_EQ("", partitions[3].range_key_start());
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[3].range_key_end());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1", 8), partitions[3].partition_key_start());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a1\0\0b1\0\0c1", 18), partitions[3].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION VALUES < ("a1", "b1", "c1"))",
            partition_schema.PartitionDebugString(partitions[3], schema));

  EXPECT_EQ(0, partitions[4].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[4].hash_buckets()[1]);
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[4].range_key_start());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[4].range_key_end());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a1\0\0b1\0\0c1", 18),
            partitions[4].partition_key_start());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a2\0\0b2\0\0", 16), partitions[4].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION ("a1", "b1", "c1") <= VALUES < ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[4], schema));

  EXPECT_EQ(0, partitions[5].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[5].hash_buckets()[1]);
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[5].range_key_start());
  EXPECT_EQ("", partitions[5].range_key_end());
  EXPECT_EQ(string("\0\0\0\0" "\0\0\0\1" "a2\0\0b2\0\0", 16), partitions[5].partition_key_start());
  EXPECT_EQ(string("\0\0\0\1", 4), partitions[5].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 0, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION VALUES >= ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[5], schema));

  EXPECT_EQ(1, partitions[6].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[6].hash_buckets()[1]);
  EXPECT_EQ("", partitions[6].range_key_start());
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[6].range_key_end());
  EXPECT_EQ(string("\0\0\0\1", 4), partitions[6].partition_key_start());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a1\0\0b1\0\0c1", 18), partitions[6].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION VALUES < ("a1", "b1", "c1"))",
            partition_schema.PartitionDebugString(partitions[6], schema));

  EXPECT_EQ(1, partitions[7].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[7].hash_buckets()[1]);
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[7].range_key_start());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[7].range_key_end());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a1\0\0b1\0\0c1", 18),
            partitions[7].partition_key_start());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a2\0\0b2\0\0", 16), partitions[7].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION ("a1", "b1", "c1") <= VALUES < ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[7], schema));

  EXPECT_EQ(1, partitions[8].hash_buckets()[0]);
  EXPECT_EQ(0, partitions[8].hash_buckets()[1]);
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[8].range_key_start());
  EXPECT_EQ("", partitions[8].range_key_end());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\0" "a2\0\0b2\0\0", 16), partitions[8].partition_key_start());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1", 8), partitions[8].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 0, "
            R"(RANGE (a, b, c) PARTITION VALUES >= ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[8], schema));

  EXPECT_EQ(1, partitions[9].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[9].hash_buckets()[1]);
  EXPECT_EQ("", partitions[9].range_key_start());
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[9].range_key_end());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1", 8), partitions[9].partition_key_start());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a1\0\0b1\0\0c1", 18), partitions[9].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION VALUES < ("a1", "b1", "c1"))",
            partition_schema.PartitionDebugString(partitions[9], schema));

  EXPECT_EQ(1, partitions[10].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[10].hash_buckets()[1]);
  EXPECT_EQ(string("a1\0\0b1\0\0c1", 10), partitions[10].range_key_start());
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[10].range_key_end());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a1\0\0b1\0\0c1", 18),
            partitions[10].partition_key_start());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a2\0\0b2\0\0", 16), partitions[10].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION ("a1", "b1", "c1") <= VALUES < ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[10], schema));

  EXPECT_EQ(1, partitions[11].hash_buckets()[0]);
  EXPECT_EQ(1, partitions[11].hash_buckets()[1]);
  EXPECT_EQ(string("a2\0\0b2\0\0", 8), partitions[11].range_key_start());
  EXPECT_EQ("", partitions[11].range_key_end());
  EXPECT_EQ(string("\0\0\0\1" "\0\0\0\1" "a2\0\0b2\0\0", 16), partitions[11].partition_key_start());
  EXPECT_EQ("", partitions[11].partition_key_end());
  EXPECT_EQ("HASH (a) PARTITION 1, HASH (b) PARTITION 1, "
            R"(RANGE (a, b, c) PARTITION VALUES >= ("a2", "b2", ""))",
            partition_schema.PartitionDebugString(partitions[11], schema));
}

TEST_F(PartitionTest, TestIncrementRangePartitionBounds) {
  // CREATE TABLE t (a INT8, b INT8, c INT8, PRIMARY KEY (a, b, c))
  // PARITITION BY RANGE (a, b, c);
  Schema schema({ ColumnSchema("c1", INT8),
                  ColumnSchema("c2", INT8),
                  ColumnSchema("c3", INT8) },
                { ColumnId(0), ColumnId(1), ColumnId(2) }, 3);

  PartitionSchemaPB schema_builder;
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  vector<vector<boost::optional<int8_t>>> tests {
    // Big list of test cases. First three columns are the input columns, final
    // three columns are the expected output columns. For example,
    { 1, 2, 3, 1, 2, 4 },
    // corresponds to the test case:
    // (1, 2, 3) -> (1, 2, 4)

    { 1, 2, boost::none, 1, 2, -127 },
    { 1, boost::none, 3, 1, boost::none, 4 },
    { boost::none, 2, 3, boost::none, 2, 4 },
    { 1, boost::none, boost::none, 1, boost::none, -127 },
    { boost::none, boost::none, 3, boost::none, boost::none, 4 },
    { boost::none, 2, boost::none, boost::none, 2, -127 },
    { 1, 2, 127, 1, 3, boost::none },
    { 1, 127, 3, 1, 127, 4},
    { 1, 127, 127, 2, boost::none, boost::none },
  };

  auto check = [&] (const vector<boost::optional<int8_t>>& test, bool lower_bound) {
    CHECK_EQ(6, test.size());
    KuduPartialRow bound(&schema);
    if (test[0]) ASSERT_OK(bound.SetInt8("c1", *test[0]));
    if (test[1]) ASSERT_OK(bound.SetInt8("c2", *test[1]));
    if (test[2]) ASSERT_OK(bound.SetInt8("c3", *test[2]));

    vector<string> components;
    partition_schema.AppendRangeDebugStringComponentsOrMin(bound, &components);
    SCOPED_TRACE(JoinStrings(components, ", "));
    SCOPED_TRACE(lower_bound ? "lower bound" : "upper bound");

    if (lower_bound) {
      ASSERT_OK(partition_schema.MakeLowerBoundRangePartitionKeyInclusive(&bound));
    } else {
      ASSERT_OK(partition_schema.MakeUpperBoundRangePartitionKeyExclusive(&bound));
    }

    int8_t val;
    if (test[3]) {
      ASSERT_OK(bound.GetInt8("c1", &val));
      ASSERT_EQ(*test[3], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c1"));
    }
    if (test[4]) {
      ASSERT_OK(bound.GetInt8("c2", &val));
      ASSERT_EQ(*test[4], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c2"));
    }
    if (test[5]) {
      ASSERT_OK(bound.GetInt8("c3", &val));
      ASSERT_EQ(*test[5], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c3"));
    }
  };

  for (const auto& test : tests) {
    check(test, true);
    check(test, false);
  }

  // Special cases:
  // lower bound: (_, _, _) -> (_, _, -127)
  check({ boost::none, boost::none, boost::none, boost::none, boost::none, -127 }, true);
  // upper bound: (_, _, _) -> (_, _, _)
  check({ boost::none, boost::none, boost::none, boost::none, boost::none, boost::none }, false);
  // upper bound: (127, 127, 127) -> (_, _, _)
  check({ 127, 127, 127, boost::none, boost::none, boost::none }, false);

  // lower bound: (127, 127, 127) -> fail!
    KuduPartialRow lower_bound(&schema);
    ASSERT_OK(lower_bound.SetInt8("c1", 127));
    ASSERT_OK(lower_bound.SetInt8("c2", 127));
    ASSERT_OK(lower_bound.SetInt8("c3", 127));
    Status s = partition_schema.MakeLowerBoundRangePartitionKeyInclusive(&lower_bound);
    ASSERT_EQ("Invalid argument: Exclusive lower bound range partition key must not have "
              "maximum values for all components: (127, 127, 127)",
              s.ToString());
}

TEST_F(PartitionTest, TestIncrementRangePartitionStringBounds) {
  // CREATE TABLE t (a STRING, b STRING, PRIMARY KEY (a, b))
  // PARITITION BY RANGE (a, b, c);
  Schema schema({ ColumnSchema("c1", STRING),
                  ColumnSchema("c2", STRING) },
                { ColumnId(0), ColumnId(1) }, 2);

  PartitionSchemaPB schema_builder;
  PartitionSchema partition_schema;
  ASSERT_OK(PartitionSchema::FromPB(schema_builder, schema, &partition_schema));

  vector<vector<boost::optional<string>>> tests {
    { string("a"), string("b"), string("a"), string("b\0", 2) },
    { string("a"), boost::none, string("a"), string("\0", 1) },
  };

  auto check = [&] (const vector<boost::optional<string>>& test, bool lower_bound) {
    CHECK_EQ(4, test.size());
    KuduPartialRow bound(&schema);
    if (test[0]) ASSERT_OK(bound.SetString("c1", *test[0]));
    if (test[1]) ASSERT_OK(bound.SetString("c2", *test[1]));

    vector<string> components;
    partition_schema.AppendRangeDebugStringComponentsOrMin(bound, &components);
    SCOPED_TRACE(JoinStrings(components, ", "));
    SCOPED_TRACE(lower_bound ? "lower bound" : "upper bound");

    if (lower_bound) {
      ASSERT_OK(partition_schema.MakeLowerBoundRangePartitionKeyInclusive(&bound));
    } else {
      ASSERT_OK(partition_schema.MakeUpperBoundRangePartitionKeyExclusive(&bound));
    }

    Slice val;
    if (test[2]) {
      ASSERT_OK(bound.GetString("c1", &val));
      ASSERT_EQ(*test[2], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c1"));
    }
    if (test[3]) {
      ASSERT_OK(bound.GetString("c2", &val));
      ASSERT_EQ(*test[3], val);
    } else {
      ASSERT_FALSE(bound.IsColumnSet("c2"));
    }
  };

  for (const auto& test : tests) {
    check(test, true);
    check(test, false);
  }
}
} // namespace kudu
