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

#include "kudu/common/schema.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <tuple>  // IWYU pragma: keep
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h> // IWYU pragma: keep
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/row.h"
#include "kudu/common/types.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/faststring.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/int128.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"  // IWYU pragma: keep
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace tablet {

using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

// Copy a row and its referenced data into the given Arena.
static Status CopyRowToArena(const Slice &row,
                             const Schema &schema,
                             Arena *dst_arena,
                             ContiguousRow *copied) {
  Slice row_data;

  // Copy the direct row data to arena
  if (!dst_arena->RelocateSlice(row, &row_data)) {
    return Status::IOError("no space for row data in arena");
  }

  copied->Reset(row_data.mutable_data());
  RETURN_NOT_OK(RelocateIndirectDataToArena(copied, dst_arena));
  return Status::OK();
}

class TestSchema : public KuduTest {};

// Test basic functionality of Schema definition
TEST_F(TestSchema, TestSchema) {
  Schema empty_schema;
  ASSERT_GT(empty_schema.memory_footprint_excluding_this(), 0);

  ColumnSchema col1("key", STRING);
  ColumnSchema col2("uint32val", UINT32, true);
  ColumnSchema col3("int32val", INT32);

  vector<ColumnSchema> cols = { col1, col2, col3 };
  Schema schema(cols, 1);

  ASSERT_EQ(sizeof(Slice) + sizeof(uint32_t) + sizeof(int32_t),
            schema.byte_size());
  ASSERT_EQ(3, schema.num_columns());
  ASSERT_EQ(0, schema.column_offset(0));
  ASSERT_EQ(sizeof(Slice), schema.column_offset(1));
  ASSERT_GT(schema.memory_footprint_excluding_this(),
            empty_schema.memory_footprint_excluding_this());

  EXPECT_EQ("(\n"
            "    key STRING NOT NULL,\n"
            "    uint32val UINT32 NULLABLE,\n"
            "    int32val INT32 NOT NULL,\n"
            "    PRIMARY KEY (key)\n"
            ")",
            schema.ToString());
  EXPECT_EQ("key STRING NOT NULL", schema.column(0).ToString());
  EXPECT_EQ("UINT32 NULLABLE", schema.column(1).TypeToString());
}

TEST_F(TestSchema, TestSchemaToStringMode) {
  SchemaBuilder builder;
  builder.AddKeyColumn("key", DataType::INT32);
  const auto schema = builder.Build();
  EXPECT_EQ(
      Substitute("(\n"
                 "    $0:key INT32 NOT NULL,\n"
                 "    PRIMARY KEY (key)\n"
                 ")",
                 schema.column_id(0)),
      schema.ToString());
  EXPECT_EQ("(\n"
            "    key INT32 NOT NULL,\n"
            "    PRIMARY KEY (key)\n"
            ")",
            schema.ToString(Schema::ToStringMode::BASE_INFO));
}

enum IncludeColumnIds {
  INCLUDE_COL_IDS,
  NO_COL_IDS
};

class ParameterizedSchemaTest : public KuduTest,
                                public ::testing::WithParamInterface<IncludeColumnIds> {};

INSTANTIATE_TEST_CASE_P(SchemaTypes, ParameterizedSchemaTest,
                        ::testing::Values(INCLUDE_COL_IDS, NO_COL_IDS));

TEST_P(ParameterizedSchemaTest, TestCopyAndMove) {
  auto check_schema = [](const Schema& schema) {
    ASSERT_EQ(sizeof(Slice) + sizeof(uint32_t) + sizeof(int32_t),
              schema.byte_size());
    ASSERT_EQ(3, schema.num_columns());
    ASSERT_EQ(0, schema.column_offset(0));
    ASSERT_EQ(sizeof(Slice), schema.column_offset(1));

    EXPECT_EQ(Substitute("(\n"
                         "    $0key STRING NOT NULL,\n"
                         "    $1uint32val UINT32 NULLABLE,\n"
                         "    $2int32val INT32 NOT NULL,\n"
                         "    PRIMARY KEY (key)\n"
                         ")",
                         schema.has_column_ids() ? "0:" : "",
                         schema.has_column_ids() ? "1:" : "",
                         schema.has_column_ids() ? "2:" : ""),
              schema.ToString());
    EXPECT_EQ("key STRING NOT NULL", schema.column(0).ToString());
    EXPECT_EQ("UINT32 NULLABLE", schema.column(1).TypeToString());
  };

  ColumnSchema col1("key", STRING);
  ColumnSchema col2("uint32val", UINT32, true);
  ColumnSchema col3("int32val", INT32);

  vector<ColumnSchema> cols = { col1, col2, col3 };
  vector<ColumnId> ids = { ColumnId(0), ColumnId(1), ColumnId(2) };
  const int kNumKeyCols = 1;

  Schema schema = GetParam() == INCLUDE_COL_IDS ?
                      Schema(cols, ids, kNumKeyCols) :
                      Schema(cols, kNumKeyCols);

  NO_FATALS(check_schema(schema));

  // Check copy- and move-assignment.
  Schema moved_schema;
  {
    Schema copied_schema = schema;
    NO_FATALS(check_schema(copied_schema));
    ASSERT_TRUE(copied_schema.Equals(schema, Schema::COMPARE_ALL));

    // Move-assign to 'moved_to_schema' from 'copied_schema' and then let
    // 'copied_schema' go out of scope to make sure none of the 'moved_schema'
    // resources are incorrectly freed.
    moved_schema = std::move(copied_schema);

    // 'copied_schema' is moved from so it should still be valid to call
    // ToString(), though we can't expect any particular result.
    copied_schema.ToString(); // NOLINT(*)
  }
  NO_FATALS(check_schema(moved_schema));
  ASSERT_TRUE(moved_schema.Equals(schema, Schema::COMPARE_ALL));

  // Check copy- and move-construction.
  {
    Schema copied_schema(schema);
    NO_FATALS(check_schema(copied_schema));
    ASSERT_TRUE(copied_schema.Equals(schema, Schema::COMPARE_ALL));

    Schema moved_schema(std::move(copied_schema));
    copied_schema.ToString(); // NOLINT(*)
    NO_FATALS(check_schema(moved_schema));
    ASSERT_TRUE(moved_schema.Equals(schema, Schema::COMPARE_ALL));
  }
}

// Test basic functionality of Schema definition with decimal columns
TEST_F(TestSchema, TestSchemaWithDecimal) {
  ColumnSchema col1("key", STRING);
  ColumnSchema col2("decimal32val", DECIMAL32, false,
                    NULL, NULL, ColumnStorageAttributes(),
                    ColumnTypeAttributes(9, 4));
  ColumnSchema col3("decimal64val", DECIMAL64, true,
                    NULL, NULL, ColumnStorageAttributes(),
                    ColumnTypeAttributes(18, 10));
  ColumnSchema col4("decimal128val", DECIMAL128, true,
                    NULL, NULL, ColumnStorageAttributes(),
                    ColumnTypeAttributes(38, 2));

  vector<ColumnSchema> cols = { col1, col2, col3, col4 };
  Schema schema(cols, 1);

  ASSERT_EQ(sizeof(Slice) + sizeof(int32_t) +
                sizeof(int64_t) + sizeof(int128_t),
            schema.byte_size());

  EXPECT_EQ("(\n"
            "    key STRING NOT NULL,\n"
            "    decimal32val DECIMAL(9, 4) NOT NULL,\n"
            "    decimal64val DECIMAL(18, 10) NULLABLE,\n"
            "    decimal128val DECIMAL(38, 2) NULLABLE,\n"
            "    PRIMARY KEY (key)\n"
            ")",
            schema.ToString());

  EXPECT_EQ("DECIMAL(9, 4) NOT NULL", schema.column(1).TypeToString());
  EXPECT_EQ("DECIMAL(18, 10) NULLABLE", schema.column(2).TypeToString());
  EXPECT_EQ("DECIMAL(38, 2) NULLABLE", schema.column(3).TypeToString());
}

// Test Schema::Equals respects decimal column attributes
TEST_F(TestSchema, TestSchemaEqualsWithDecimal) {
  ColumnSchema col1("key", STRING);
  ColumnSchema col_18_10("decimal64val", DECIMAL64, true,
                         NULL, NULL, ColumnStorageAttributes(),
                         ColumnTypeAttributes(18, 10));
  ColumnSchema col_18_9("decimal64val", DECIMAL64, true,
                        NULL, NULL, ColumnStorageAttributes(),
                        ColumnTypeAttributes(18, 9));
  ColumnSchema col_17_10("decimal64val", DECIMAL64, true,
                         NULL, NULL, ColumnStorageAttributes(),
                         ColumnTypeAttributes(17, 10));
  ColumnSchema col_17_9("decimal64val", DECIMAL64, true,
                        NULL, NULL, ColumnStorageAttributes(),
                        ColumnTypeAttributes(17, 9));

  Schema schema_18_10({ col1, col_18_10 }, 1);
  Schema schema_18_9({ col1, col_18_9 }, 1);
  Schema schema_17_10({ col1, col_17_10 }, 1);
  Schema schema_17_9({ col1, col_17_9 }, 1);

  EXPECT_TRUE(schema_18_10.Equals(schema_18_10));
  EXPECT_FALSE(schema_18_10.Equals(schema_18_9));
  EXPECT_FALSE(schema_18_10.Equals(schema_17_10));
  EXPECT_FALSE(schema_18_10.Equals(schema_17_9));
}

TEST_F(TestSchema, TestColumnSchemaEquals) {
  Slice default_str("read-write default");
  ColumnSchema col1("key", STRING);
  ColumnSchema col2("key1", STRING);
  ColumnSchema col3("key", STRING, true);
  ColumnSchema col4("key", STRING, true, &default_str, &default_str);

  ASSERT_TRUE(col1.Equals(col1));
  ASSERT_FALSE(col1.Equals(col2, ColumnSchema::COMPARE_NAME));
  ASSERT_TRUE(col1.Equals(col2, ColumnSchema::COMPARE_TYPE));
  ASSERT_TRUE(col1.Equals(col3, ColumnSchema::COMPARE_NAME));
  ASSERT_FALSE(col1.Equals(col3, ColumnSchema::COMPARE_TYPE));
  ASSERT_TRUE(col1.Equals(col3, ColumnSchema::COMPARE_OTHER));
  ASSERT_FALSE(col3.Equals(col4, ColumnSchema::COMPARE_OTHER));
  ASSERT_TRUE(col4.Equals(col4, ColumnSchema::COMPARE_OTHER));
}

TEST_F(TestSchema, TestSchemaEquals) {
  Schema schema1({ ColumnSchema("col1", STRING),
                   ColumnSchema("col2", STRING),
                   ColumnSchema("col3", UINT32) },
                 2);
  Schema schema2({ ColumnSchema("newCol1", STRING),
                   ColumnSchema("newCol2", STRING),
                   ColumnSchema("newCol3", UINT32) },
                 2);
  Schema schema3({ ColumnSchema("col1", STRING),
                   ColumnSchema("col2", UINT32),
                   ColumnSchema("col3", UINT32, true) },
                 2);
  Schema schema4({ ColumnSchema("col1", STRING),
                   ColumnSchema("col2", UINT32),
                   ColumnSchema("col3", UINT32, false) },
                 2);
  ASSERT_FALSE(schema1.Equals(schema2));
  ASSERT_TRUE(schema1.KeyEquals(schema1));
  ASSERT_TRUE(schema1.KeyEquals(schema2, ColumnSchema::COMPARE_TYPE));
  ASSERT_FALSE(schema1.KeyEquals(schema2, ColumnSchema::COMPARE_NAME));
  ASSERT_TRUE(schema1.KeyTypeEquals(schema2));
  ASSERT_FALSE(schema2.KeyTypeEquals(schema3));
  ASSERT_FALSE(schema3.Equals(schema4));
  ASSERT_TRUE(schema4.Equals(schema4));
  ASSERT_TRUE(schema3.KeyEquals(schema4, ColumnSchema::COMPARE_NAME_AND_TYPE));
}

TEST_F(TestSchema, TestReset) {
  Schema schema;
  ASSERT_FALSE(schema.initialized());

  ASSERT_OK(schema.Reset({ ColumnSchema("col3", UINT32),
                           ColumnSchema("col2", STRING) },
                         1));
  ASSERT_TRUE(schema.initialized());

  // Move an uninitialized schema into the initialized schema.
  Schema schema2;
  schema = std::move(schema2);
  ASSERT_FALSE(schema.initialized());
}

// Test for KUDU-943, a bug where we suspected that Variant didn't behave
// correctly with empty strings.
TEST_F(TestSchema, TestEmptyVariant) {
  Slice empty_val("");
  Slice nonempty_val("test");

  Variant v(STRING, &nonempty_val);
  ASSERT_EQ("test", (static_cast<const Slice*>(v.value()))->ToString());
  v.Reset(STRING, &empty_val);
  ASSERT_EQ("", (static_cast<const Slice*>(v.value()))->ToString());
  v.Reset(STRING, &nonempty_val);
  ASSERT_EQ("test", (static_cast<const Slice*>(v.value()))->ToString());
}

TEST_F(TestSchema, TestProjectSubset) {
  Schema schema1({ ColumnSchema("col1", STRING),
                   ColumnSchema("col2", STRING),
                   ColumnSchema("col3", UINT32) },
                 1);

  Schema schema2({ ColumnSchema("col3", UINT32),
                   ColumnSchema("col2", STRING) },
                 0);

  RowProjector row_projector(&schema1, &schema2);
  ASSERT_OK(row_projector.Init());

  // Verify the mapping
  ASSERT_EQ(2, row_projector.base_cols_mapping().size());
  ASSERT_EQ(0, row_projector.projection_defaults().size());

  const vector<RowProjector::ProjectionIdxMapping>& mapping = row_projector.base_cols_mapping();
  ASSERT_EQ(mapping[0].first, 0);  // col3 schema2
  ASSERT_EQ(mapping[0].second, 2); // col3 schema1
  ASSERT_EQ(mapping[1].first, 1);  // col2 schema2
  ASSERT_EQ(mapping[1].second, 1); // col2 schema1
}

// Test projection when the type of the projected column
// doesn't match the original type.
TEST_F(TestSchema, TestProjectTypeMismatch) {
  Schema schema1({ ColumnSchema("key", STRING),
                   ColumnSchema("val", UINT32) },
                 1);
  Schema schema2({ ColumnSchema("val", STRING) }, 0);

  RowProjector row_projector(&schema1, &schema2);
  Status s = row_projector.Init();
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.message().ToString(), "must have type");
}

// Test projection when the some columns in the projection
// are not present in the base schema
TEST_F(TestSchema, TestProjectMissingColumn) {
  Schema schema1({ ColumnSchema("key", STRING), ColumnSchema("val", UINT32) }, 1);
  Schema schema2({ ColumnSchema("val", UINT32), ColumnSchema("non_present", STRING) }, 0);
  Schema schema3({ ColumnSchema("val", UINT32), ColumnSchema("non_present", UINT32, true) }, 0);
  uint32_t default_value = 15;
  Schema schema4({ ColumnSchema("val", UINT32),
                   ColumnSchema("non_present", UINT32, false, &default_value) },
                 0);

  RowProjector row_projector(&schema1, &schema2);
  Status s = row_projector.Init();
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.message().ToString(),
    "does not exist in the projection, and it does not have a default value or a nullable type");

  // Verify Default nullable column with no default value
  ASSERT_OK(row_projector.Reset(&schema1, &schema3));

  ASSERT_EQ(1, row_projector.base_cols_mapping().size());
  ASSERT_EQ(1, row_projector.projection_defaults().size());

  ASSERT_EQ(row_projector.base_cols_mapping()[0].first, 0);  // val schema2
  ASSERT_EQ(row_projector.base_cols_mapping()[0].second, 1); // val schema1
  ASSERT_EQ(row_projector.projection_defaults()[0], 1);      // non_present schema3

  // Verify Default non nullable column with default value
  ASSERT_OK(row_projector.Reset(&schema1, &schema4));

  ASSERT_EQ(1, row_projector.base_cols_mapping().size());
  ASSERT_EQ(1, row_projector.projection_defaults().size());

  ASSERT_EQ(row_projector.base_cols_mapping()[0].first, 0);  // val schema4
  ASSERT_EQ(row_projector.base_cols_mapping()[0].second, 1); // val schema1
  ASSERT_EQ(row_projector.projection_defaults()[0], 1);      // non_present schema4
}

// Test projection mapping using IDs.
// This simulate a column rename ('val' -> 'val_renamed')
// and a new column added ('non_present')
TEST_F(TestSchema, TestProjectRename) {
  SchemaBuilder builder;
  ASSERT_OK(builder.AddKeyColumn("key", STRING));
  ASSERT_OK(builder.AddColumn("val", UINT32));
  Schema schema1 = builder.Build();

  builder.Reset(schema1);
  ASSERT_OK(builder.AddNullableColumn("non_present", UINT32));
  ASSERT_OK(builder.RenameColumn("val", "val_renamed"));
  Schema schema2 = builder.Build();

  RowProjector row_projector(&schema1, &schema2);
  ASSERT_OK(row_projector.Init());

  ASSERT_EQ(2, row_projector.base_cols_mapping().size());
  ASSERT_EQ(1, row_projector.projection_defaults().size());

  ASSERT_EQ(row_projector.base_cols_mapping()[0].first, 0);  // key schema2
  ASSERT_EQ(row_projector.base_cols_mapping()[0].second, 0); // key schema1

  ASSERT_EQ(row_projector.base_cols_mapping()[1].first, 1);  // val_renamed schema2
  ASSERT_EQ(row_projector.base_cols_mapping()[1].second, 1); // val schema1

  ASSERT_EQ(row_projector.projection_defaults()[0], 2);      // non_present schema2
}

// Test that we can map a projection schema (no column ids) onto a tablet
// schema (column ids).
TEST_F(TestSchema, TestGetMappedReadProjection) {
  Schema tablet_schema({ ColumnSchema("key", STRING),
                         ColumnSchema("val", INT32) },
                       { ColumnId(0),
                         ColumnId(1) },
                       1);
  const bool kReadDefault = false;
  Schema projection({ ColumnSchema("key", STRING),
                      ColumnSchema("deleted", IS_DELETED,
                                   /*is_nullable=*/false, /*read_default=*/&kReadDefault) },
                    1);

  Schema mapped;
  ASSERT_OK(tablet_schema.GetMappedReadProjection(projection, &mapped));
  ASSERT_EQ(1, mapped.num_key_columns());
  ASSERT_EQ(2, mapped.num_columns());
  ASSERT_TRUE(mapped.has_column_ids());
  ASSERT_FALSE(mapped.Equals(projection, Schema::COMPARE_ALL));

  // The column id for the 'key' column in the mapped projection should match
  // the one from the tablet schema.
  ASSERT_EQ("key", mapped.column(0).name());
  ASSERT_EQ(0, mapped.column_id(0));

  // Since 'deleted' is a virtual column and thus does not appear in the tablet
  // schema, in the mapped schema it should have been assigned a higher column
  // id than the highest column id in the tablet schema.
  ASSERT_EQ("deleted", mapped.column(1).name());
  ASSERT_GT(mapped.column_id(1), tablet_schema.column_id(1));
  ASSERT_GT(mapped.max_col_id(), tablet_schema.max_col_id());

  // Ensure that virtual columns that are nullable or that do not have read
  // defaults are rejected.
  Schema nullable_projection({ ColumnSchema("key", STRING),
                               ColumnSchema("deleted", IS_DELETED,
                                            /*is_nullable=*/true,
                                            /*read_default=*/&kReadDefault) },
                          1);
  Status s = tablet_schema.GetMappedReadProjection(nullable_projection, &mapped);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "must not be nullable");

  Schema no_default_projection({ ColumnSchema("key", STRING),
                                 ColumnSchema("deleted", IS_DELETED,
                                              /*is_nullable=*/false,
                                              /*read_default=*/nullptr) },
                               1);
  s = tablet_schema.GetMappedReadProjection(no_default_projection, &mapped);
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "must have a default value for read");
}

// Test that the schema can be used to compare and stringify rows.
TEST_F(TestSchema, TestRowOperations) {
  Schema schema({ ColumnSchema("col1", STRING),
                  ColumnSchema("col2", STRING),
                  ColumnSchema("col3", UINT32),
                  ColumnSchema("col4", INT32) },
                1);

  Arena arena(1024);

  RowBuilder rb(schema);
  rb.AddString(string("row_a_1"));
  rb.AddString(string("row_a_2"));
  rb.AddUint32(3);
  rb.AddInt32(-3);
  ContiguousRow row_a(&schema);
  ASSERT_OK(CopyRowToArena(rb.data(), schema, &arena, &row_a));

  rb.Reset();
  rb.AddString(string("row_b_1"));
  rb.AddString(string("row_b_2"));
  rb.AddUint32(3);
  rb.AddInt32(-3);
  ContiguousRow row_b(&schema);
  ASSERT_OK(CopyRowToArena(rb.data(), schema, &arena, &row_b));

  ASSERT_GT(schema.Compare(row_b, row_a), 0);
  ASSERT_LT(schema.Compare(row_a, row_b), 0);

  ASSERT_EQ(R"((string col1="row_a_1", string col2="row_a_2", uint32 col3=3, int32 col4=-3))",
            schema.DebugRow(row_a));
}

TEST(TestKeyEncoder, TestKeyEncoder) {
  faststring fs;
  const KeyEncoder<faststring>& encoder = GetKeyEncoder<faststring>(GetTypeInfo(STRING));

  typedef std::tuple<vector<Slice>, Slice> test_pair;

  vector<test_pair> pairs;

  // Simple key
  pairs.push_back(test_pair({ Slice("foo", 3) }, Slice("foo", 3)));

  // Simple compound key
  pairs.push_back(test_pair({ Slice("foo", 3), Slice("bar", 3) },
                            Slice("foo" "\x00\x00" "bar", 8)));

  // Compound key with a \x00 in it
  pairs.push_back(test_pair({ Slice("xxx\x00yyy", 7), Slice("bar", 3) },
                            Slice("xxx" "\x00\x01" "yyy" "\x00\x00" "bar", 13)));

  int i = 0;
  for (const test_pair &t : pairs) {
    const vector<Slice> &in = std::get<0>(t);
    Slice expected = std::get<1>(t);

    fs.clear();
    for (int col = 0; col < in.size(); col++) {
      encoder.Encode(&in[col], col == in.size() - 1, &fs);
    }

    ASSERT_EQ(0, expected.compare(Slice(fs)))
      << "Failed encoding example " << i << ".\n"
      << "Expected: " << HexDump(expected) << "\n"
      << "Got:      " << HexDump(Slice(fs));
    i++;
  }
}

TEST_F(TestSchema, TestDecodeKeys_CompoundStringKey) {
  Schema schema({ ColumnSchema("col1", STRING),
                  ColumnSchema("col2", STRING),
                  ColumnSchema("col3", STRING) },
                2);

  EXPECT_EQ(R"((string col1="foo", string col2="bar"))",
            schema.DebugEncodedRowKey(Slice("foo\0\0bar", 8), Schema::START_KEY));
  EXPECT_EQ(R"((string col1="fo\000o", string col2="bar"))",
            schema.DebugEncodedRowKey(Slice("fo\x00\x01o\0\0""bar", 10), Schema::START_KEY));
  EXPECT_EQ(R"((string col1="fo\000o", string col2="bar\000xy"))",
            schema.DebugEncodedRowKey(Slice("fo\x00\x01o\0\0""bar\0xy", 13), Schema::START_KEY));

  EXPECT_EQ("<start of table>",
            schema.DebugEncodedRowKey("", Schema::START_KEY));
  EXPECT_EQ("<end of table>",
            schema.DebugEncodedRowKey("", Schema::END_KEY));
}

// Test that appropriate statuses are returned when trying to decode an invalid
// encoded key.
TEST_F(TestSchema, TestDecodeKeys_InvalidKeys) {
  Schema schema({ ColumnSchema("col1", STRING),
                  ColumnSchema("col2", UINT32),
                  ColumnSchema("col3", STRING) },
                2);

  EXPECT_EQ("<invalid key: Invalid argument: Error decoding composite key component"
            " 'col1': Missing separator after composite key string component: foo>",
            schema.DebugEncodedRowKey(Slice("foo"), Schema::START_KEY));
  EXPECT_EQ("<invalid key: Invalid argument: Error decoding composite key component 'col2': "
            "key too short>",
            schema.DebugEncodedRowKey(Slice("foo\x00\x00", 5), Schema::START_KEY));
  EXPECT_EQ("<invalid key: Invalid argument: Error decoding composite key component 'col2': "
            "key too short: \\xff\\xff>",
            schema.DebugEncodedRowKey(Slice("foo\x00\x00\xff\xff", 7), Schema::START_KEY));
}

TEST_F(TestSchema, TestCreateProjection) {
  Schema schema({ ColumnSchema("col1", STRING),
                  ColumnSchema("col2", STRING),
                  ColumnSchema("col3", STRING),
                  ColumnSchema("col4", STRING),
                  ColumnSchema("col5", STRING) },
                2);
  Schema schema_with_ids = SchemaBuilder(schema).Build();
  Schema partial_schema;

  // By names, without IDs
  ASSERT_OK(schema.CreateProjectionByNames({ "col1", "col2", "col4" }, &partial_schema));
  EXPECT_EQ("(\n"
            "    col1 STRING NOT NULL,\n"
            "    col2 STRING NOT NULL,\n"
            "    col4 STRING NOT NULL,\n"
            "    PRIMARY KEY ()\n"
            ")",
            partial_schema.ToString());

  // By names, with IDS
  ASSERT_OK(schema_with_ids.CreateProjectionByNames({ "col1", "col2", "col4" }, &partial_schema));
  EXPECT_EQ(Substitute("(\n"
                       "    $0:col1 STRING NOT NULL,\n"
                       "    $1:col2 STRING NOT NULL,\n"
                       "    $2:col4 STRING NOT NULL,\n"
                       "    PRIMARY KEY ()\n"
                       ")",
                       schema_with_ids.column_id(0),
                       schema_with_ids.column_id(1),
                       schema_with_ids.column_id(3)),
            partial_schema.ToString());

  // By names, with missing names.
  Status s = schema.CreateProjectionByNames({ "foobar" }, &partial_schema);
  EXPECT_EQ("Not found: column not found: foobar", s.ToString());

  // By IDs
  ASSERT_OK(schema_with_ids.CreateProjectionByIdsIgnoreMissing({ schema_with_ids.column_id(0),
                                                                 schema_with_ids.column_id(1),
                                                                 ColumnId(1000), // missing column
                                                                 schema_with_ids.column_id(3) },
                                                               &partial_schema));
  EXPECT_EQ(Substitute("(\n"
                       "    $0:col1 STRING NOT NULL,\n"
                       "    $1:col2 STRING NOT NULL,\n"
                       "    $2:col4 STRING NOT NULL,\n"
                       "    PRIMARY KEY ()\n"
                       ")",
                       schema_with_ids.column_id(0),
                       schema_with_ids.column_id(1),
                       schema_with_ids.column_id(3)),
            partial_schema.ToString());
}

#ifdef NDEBUG
TEST(TestKeyEncoder, BenchmarkSimpleKey) {
  faststring fs;
  Schema schema({ ColumnSchema("col1", STRING) }, 1);

  RowBuilder rb(schema);
  rb.AddString(Slice("hello world"));
  ConstContiguousRow row(&rb.schema(), rb.data());

  LOG_TIMING(INFO, "Encoding") {
    for (int i = 0; i < 10000000; i++) {
      schema.EncodeComparableKey(row, &fs);
    }
  }
}
#endif

} // namespace tablet
} // namespace kudu
