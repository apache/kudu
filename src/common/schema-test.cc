// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>
#include <tr1/unordered_map>

#include "common/row.h"
#include "common/schema.h"
#include "common/key_encoder.h"
#include "util/hexdump.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

using std::vector;
using std::tr1::unordered_map;

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



// Test basic functionality of Schema definition
TEST(TestSchema, TestSchema) {
  ColumnSchema col1("key", STRING);
  ColumnSchema col2("uint32val", UINT32, true);
  ColumnSchema col3("int32val", INT32);

  vector<ColumnSchema> cols = boost::assign::list_of
    (col1)(col2)(col3);
  Schema schema(cols, 1);

  ASSERT_EQ(sizeof(Slice) + sizeof(uint32_t) + sizeof(int32_t),
            schema.byte_size());
  ASSERT_EQ(3, schema.num_columns());
  ASSERT_EQ(0, schema.column_offset(0));
  ASSERT_EQ(sizeof(Slice), schema.column_offset(1));

  EXPECT_EQ("Schema [key[string NOT NULL], "
            "uint32val[uint32 NULLABLE], "
            "int32val[int32 NOT NULL]]", schema.ToString());
  EXPECT_EQ("key[string NOT NULL]", schema.column(0).ToString());
  EXPECT_EQ("uint32 NULLABLE", schema.column(1).TypeToString());
}

TEST(TestSchema, TestSwap) {
  Schema schema1(boost::assign::list_of
                 (ColumnSchema("col1", STRING))
                 (ColumnSchema("col2", STRING))
                 (ColumnSchema("col3", UINT32)),
                 2);
  Schema schema2(boost::assign::list_of
                 (ColumnSchema("col3", UINT32))
                 (ColumnSchema("col2", STRING)),
                 1);
  schema1.swap(schema2);
  ASSERT_EQ(2, schema1.num_columns());
  ASSERT_EQ(1, schema1.num_key_columns());
  ASSERT_EQ(3, schema2.num_columns());
  ASSERT_EQ(2, schema2.num_key_columns());
}

TEST(TestSchema, TestReset) {
  Schema schema;
  ASSERT_FALSE(schema.initialized());

  ASSERT_STATUS_OK(schema.Reset(boost::assign::list_of
                                (ColumnSchema("col3", UINT32))
                                (ColumnSchema("col2", STRING)),
                                1));
  ASSERT_TRUE(schema.initialized());

  // Swap the initialized schema with an uninitialized one.
  Schema schema2;
  schema2.swap(schema);
  ASSERT_FALSE(schema.initialized());
  ASSERT_TRUE(schema2.initialized());
}

TEST(TestSchema, TestProjectSubset) {
  Schema schema1(boost::assign::list_of
                 (ColumnSchema("col1", STRING))
                 (ColumnSchema("col2", STRING))
                 (ColumnSchema("col3", UINT32)),
                 1);

  Schema schema2(boost::assign::list_of
                 (ColumnSchema("col3", UINT32))
                 (ColumnSchema("col2", STRING)),
                 0);

  RowProjector row_projector(&schema1, &schema2);
  ASSERT_STATUS_OK(row_projector.Init());

  // Verify the mapping
  ASSERT_EQ(2, row_projector.base_cols_mapping().size());
  ASSERT_EQ(0, row_projector.adapter_cols_mapping().size());
  ASSERT_EQ(0, row_projector.projection_defaults().size());

  const vector<RowProjector::ProjectionIdxMapping>& mapping = row_projector.base_cols_mapping();
  ASSERT_EQ(mapping[0].first, 0);  // col3 schema2
  ASSERT_EQ(mapping[0].second, 2); // col3 schema1
  ASSERT_EQ(mapping[1].first, 1);  // col2 schema2
  ASSERT_EQ(mapping[1].second, 1); // col2 schema1
}

// Test projection when the type of the projected column
// doesn't match the original type.
TEST(TestSchema, TestProjectTypeMismatch) {
  Schema schema1(boost::assign::list_of
                 (ColumnSchema("key", STRING))
                 (ColumnSchema("val", UINT32)),
                 1);
  Schema schema2(boost::assign::list_of
                 (ColumnSchema("val", STRING)),
                 0);

  RowProjector row_projector(&schema1, &schema2);
  Status s = row_projector.Init();
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.message().ToString(), "must have type");
}

// Test projection when the some columns in the projection
// are not present in the base schema
TEST(TestSchema, TestProjectMissingColumn) {
  Schema schema1(boost::assign::list_of
                 (ColumnSchema("key", STRING))
                 (ColumnSchema("val", UINT32)),
                 1);
  Schema schema2(boost::assign::list_of
                 (ColumnSchema("val", UINT32))
                 (ColumnSchema("non_present", STRING)),
                 0);
  Schema schema3(boost::assign::list_of
                 (ColumnSchema("val", UINT32))
                 (ColumnSchema("non_present", UINT32, true)),
                 0);
  uint32_t default_value = 15;
  Schema schema4(boost::assign::list_of
                 (ColumnSchema("val", UINT32))
                 (ColumnSchema("non_present", UINT32, false, &default_value)),
                 0);

  RowProjector row_projector(&schema1, &schema2);
  Status s = row_projector.Init();
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.message().ToString(),
    "does not exist in the projection, and it does not have a default value or a nullable type");

  // Verify Default nullable column with no default value
  ASSERT_STATUS_OK(row_projector.Reset(&schema1, &schema3));

  ASSERT_EQ(1, row_projector.base_cols_mapping().size());
  ASSERT_EQ(0, row_projector.adapter_cols_mapping().size());
  ASSERT_EQ(1, row_projector.projection_defaults().size());

  ASSERT_EQ(row_projector.base_cols_mapping()[0].first, 0);  // val schema2
  ASSERT_EQ(row_projector.base_cols_mapping()[0].second, 1); // val schema1
  ASSERT_EQ(row_projector.projection_defaults()[0], 1);      // non_present schema3

  // Verify Default non nullable column with default value
  ASSERT_STATUS_OK(row_projector.Reset(&schema1, &schema4));

  ASSERT_EQ(1, row_projector.base_cols_mapping().size());
  ASSERT_EQ(0, row_projector.adapter_cols_mapping().size());
  ASSERT_EQ(1, row_projector.projection_defaults().size());

  ASSERT_EQ(row_projector.base_cols_mapping()[0].first, 0);  // val schema4
  ASSERT_EQ(row_projector.base_cols_mapping()[0].second, 1); // val schema1
  ASSERT_EQ(row_projector.projection_defaults()[0], 1);      // non_present schema4
}

// Test projection mapping using IDs.
// This simulate a column rename ('val' -> 'val_renamed')
// and a new column added ('non_present')
TEST(TestSchema, TestProjectRename) {
  SchemaBuilder builder;
  ASSERT_STATUS_OK(builder.AddKeyColumn("key", STRING));
  ASSERT_STATUS_OK(builder.AddColumn("val", UINT32));
  Schema schema1 = builder.Build();

  builder.Reset(schema1);
  ASSERT_STATUS_OK(builder.AddNullableColumn("non_present", UINT32));
  ASSERT_STATUS_OK(builder.RenameColumn("val", "val_renamed"));
  Schema schema2 = builder.Build();

  RowProjector row_projector(&schema1, &schema2);
  ASSERT_STATUS_OK(row_projector.Init());

  ASSERT_EQ(2, row_projector.base_cols_mapping().size());
  ASSERT_EQ(0, row_projector.adapter_cols_mapping().size());
  ASSERT_EQ(1, row_projector.projection_defaults().size());

  ASSERT_EQ(row_projector.base_cols_mapping()[0].first, 0);  // key schema2
  ASSERT_EQ(row_projector.base_cols_mapping()[0].second, 0); // key schema1

  ASSERT_EQ(row_projector.base_cols_mapping()[1].first, 1);  // val_renamed schema2
  ASSERT_EQ(row_projector.base_cols_mapping()[1].second, 1); // val schema1

  ASSERT_EQ(row_projector.projection_defaults()[0], 2);      // non_present schema2
}


// Test that the schema can be used to compare and stringify rows.
TEST(TestSchema, TestRowOperations) {
  Schema schema(boost::assign::list_of
                 (ColumnSchema("col1", STRING))
                 (ColumnSchema("col2", STRING))
                 (ColumnSchema("col3", UINT32))
                 (ColumnSchema("col4", INT32)),
                 1);

  Arena arena(1024, 256*1024);

  RowBuilder rb(schema);
  rb.AddString(string("row_a_1"));
  rb.AddString(string("row_a_2"));
  rb.AddUint32(3);
  rb.AddInt32(-3);
  ContiguousRow row_a(schema);
  ASSERT_STATUS_OK(CopyRowToArena(rb.data(), schema, &arena, &row_a));

  rb.Reset();
  rb.AddString(string("row_b_1"));
  rb.AddString(string("row_b_2"));
  rb.AddUint32(3);
  rb.AddInt32(-3);
  ContiguousRow row_b(schema);
  ASSERT_STATUS_OK(CopyRowToArena(rb.data(), schema, &arena, &row_b));

  ASSERT_GT(schema.Compare(row_b, row_a), 0);
  ASSERT_LT(schema.Compare(row_a, row_b), 0);

  ASSERT_EQ(string("(string col1=row_a_1, string col2=row_a_2, uint32 col3=3, int32 col4=-3)"),
            schema.DebugRow(row_a));
}

TEST(TestKeyEncoder, TestKeyEncoder) {
  faststring fs;
  const KeyEncoder& encoder = GetKeyEncoder(STRING);

  typedef boost::tuple<vector<Slice>, Slice> test_pair;
  using boost::assign::list_of;

  vector<test_pair> pairs;

  // Simple key
  pairs.push_back(test_pair(list_of(Slice("foo", 3)),
                            Slice("foo", 3)));

  // Simple compound key
  pairs.push_back(test_pair(list_of(Slice("foo", 3))(Slice("bar", 3)),
                            Slice("foo" "\x00\x00" "bar", 8)));

  // Compound key with a \x00 in it
  pairs.push_back(test_pair(list_of(Slice("xxx\x00yyy", 7))(Slice("bar", 3)),
                            Slice("xxx" "\x00\x01" "yyy" "\x00\x00" "bar", 13)));

  int i = 0;
  BOOST_FOREACH(const test_pair &t, pairs) {
    const vector<Slice> &in = boost::get<0>(t);
    Slice expected = boost::get<1>(t);

    fs.clear();
    for (int col = 0; col < in.size(); col++) {
      encoder.Encode(&in[col], col == in.size() - 1, &fs);
    }

    ASSERT_EQ(0, expected.compare(Slice(fs)))
      << "Failed encoding example " << i << ".\n"
      << "Expected: " << HexDump(expected)
      << "Got:      " << HexDump(Slice(fs));
    i++;
  }
}

TEST(TestSchema, TestCreatePartialSchema) {
  Schema schema(boost::assign::list_of
                (ColumnSchema("col1", STRING))
                (ColumnSchema("col2", STRING))
                (ColumnSchema("col3", STRING))
                (ColumnSchema("col4", STRING))
                (ColumnSchema("col5", STRING)),
                2);

  vector<size_t> partial_cols;
  unordered_map<size_t, size_t> old_to_new;
  {
    // All keys are included
    Schema partial_schema;
    partial_cols.push_back(0);
    partial_cols.push_back(1);
    partial_cols.push_back(3);

    ASSERT_STATUS_OK(schema.CreatePartialSchema(partial_cols, &old_to_new, &partial_schema));
    ASSERT_EQ("Schema [col1[string NOT NULL], col2[string NOT NULL], col4[string NOT NULL]]",
              partial_schema.ToString());
    ASSERT_EQ(old_to_new[0], 0);
    ASSERT_EQ(old_to_new[1], 1);
    ASSERT_EQ(old_to_new[3], 2);
  }
  partial_cols.clear();
  old_to_new.clear();
  {
    // No keys are included
    Schema partial_schema;
    partial_cols.push_back(2);
     partial_cols.push_back(3);
     partial_cols.push_back(4);
     ASSERT_STATUS_OK(schema.CreatePartialSchema(partial_cols, &old_to_new, &partial_schema));
     ASSERT_EQ("Schema [col3[string NOT NULL], col4[string NOT NULL], col5[string NOT NULL]]",
               partial_schema.ToString());
     ASSERT_EQ(old_to_new[2], 0);
     ASSERT_EQ(old_to_new[3], 1);
     ASSERT_EQ(old_to_new[4], 2);
  }
  partial_cols.clear();
  {
    // Keys are partially included
    Schema partial_schema;
    partial_cols.push_back(0);
    partial_cols.push_back(4);
    Status s = schema.CreatePartialSchema(partial_cols, NULL, &partial_schema);
    ASSERT_TRUE(s.IsInvalidArgument());
  }
  partial_cols.clear();
  partial_cols.push_back(1);
  partial_cols.push_back(4);
  {
    // Keys are partially included, another variant
    Schema partial_schema;
    Status s = schema.CreatePartialSchema(partial_cols, NULL, &partial_schema);
    ASSERT_TRUE(s.IsInvalidArgument());
  }
}

#ifdef NDEBUG
TEST(TestKeyEncoder, BenchmarkSimpleKey) {
  faststring fs;
  Schema schema(boost::assign::list_of
                (ColumnSchema("col1", STRING)), 1);

  RowBuilder rb(schema);
  rb.AddString(Slice("hello world"));
  ConstContiguousRow row(rb.schema(), rb.data());

  LOG_TIMING(INFO, "Encoding") {
    for (int i = 0; i < 10000000; i++) {
      schema.EncodeComparableKey(row, &fs);
    }
  }
}
#endif

} // namespace tablet
} // namespace kudu
