// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "common/row.h"
#include "common/schema.h"
#include "common/key_encoder.h"
#include "util/hexdump.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

using std::vector;

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
  ColumnSchema col2("uint32val", UINT32);
  ColumnSchema col3("int32val", INT32);

  vector<ColumnSchema> cols = boost::assign::list_of
    (col1)(col2)(col3);
  Schema schema(cols, 1);

  ASSERT_EQ(sizeof(Slice) + sizeof(uint32_t) + sizeof(int32_t),
            schema.byte_size());
  ASSERT_EQ(3, schema.num_columns());
  ASSERT_EQ(0, schema.column_offset(0));
  ASSERT_EQ(sizeof(Slice), schema.column_offset(1));
}

// Test projection from many columns down to a subset.
TEST(TestSchema, TestProjection) {
  Schema schema1(boost::assign::list_of
                 (ColumnSchema("col1", STRING))
                 (ColumnSchema("col2", STRING))
                 (ColumnSchema("col3", UINT32)),
                 1);
  Schema schema2(boost::assign::list_of
                 (ColumnSchema("col3", UINT32))
                 (ColumnSchema("col2", STRING)),
                 1);

  vector<size_t> proj;
  ASSERT_STATUS_OK(schema2.GetProjectionFrom(schema1, &proj));
  ASSERT_EQ(2, proj.size());
  ASSERT_EQ(2, proj[0]);
  ASSERT_EQ(1, proj[1]);


  Schema key_cols = schema1.CreateKeyProjection();
  ASSERT_EQ(1, key_cols.num_columns());
  ASSERT_EQ("col1", key_cols.column(0).name());
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

// Test projection when the type of the projected column
// doesn't match the original type.
TEST(TestSchema, TestProjectTypeMismatch) {
  Schema schema1(boost::assign::list_of
                 (ColumnSchema("key", STRING))
                 (ColumnSchema("val", UINT32)),
                 1);
  Schema schema2(boost::assign::list_of
                 (ColumnSchema("val", STRING)),
                 1);

  vector<size_t> proj;
  Status s = schema2.GetProjectionFrom(schema1, &proj);
  ASSERT_STR_CONTAINS(s.ToString(), "type mismatch");
}

// Test projection when the type of the projected column
// doesn't match the original type.
TEST(TestSchema, TestProjectMissingColumn) {
  Schema schema1(boost::assign::list_of
                 (ColumnSchema("key", STRING))
                 (ColumnSchema("val", UINT32)),
                 1);
  Schema schema2(boost::assign::list_of
                 (ColumnSchema("val", UINT32))
                 (ColumnSchema("non_present", STRING)),
                 1);

  vector<size_t> proj;
  Status s = schema2.GetProjectionFrom(schema1, &proj);
  ASSERT_STR_CONTAINS(s.ToString(), "column 'non_present' not present");
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
