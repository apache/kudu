// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "common/row.h"
#include "common/schema.h"
#include "common/key_encoder.h"
#include "util/hexdump.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

using std::vector;

// Test basic functionality of Schema definition
TEST(TestSchema, TestSchema) {
  ColumnSchema col1("key", STRING);
  ColumnSchema col2("val", UINT32);

  vector<ColumnSchema> cols = boost::assign::list_of
    (col1)(col2);
  Schema schema(cols, 1);

  ASSERT_EQ(sizeof(Slice) + sizeof(uint32_t),
            schema.byte_size());
  ASSERT_EQ(2, schema.num_columns());
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
                 (ColumnSchema("col3", UINT32)),
                 1);

  Arena arena(1024, 256*1024);

  RowBuilder rb(schema);
  rb.AddString(string("row_a_1"));
  rb.AddString(string("row_a_2"));
  rb.AddUint32(3);
  Slice row_a;
  ASSERT_STATUS_OK(rb.CopyRowToArena(&arena, &row_a));

  rb.Reset();
  rb.AddString(string("row_b_1"));
  rb.AddString(string("row_b_2"));
  rb.AddUint32(3);
  Slice row_b;
  ASSERT_STATUS_OK(rb.CopyRowToArena(&arena, &row_b));

  ASSERT_GT(schema.Compare(row_b.data(), row_a.data()), 0);
  ASSERT_LT(schema.Compare(row_a.data(), row_b.data()), 0);

  ASSERT_EQ(string("(string col1=row_a_1, string col2=row_a_2, uint32 col3=3)"),
            schema.DebugRow(row_a.data()));
}

TEST(TestKeyEncoder, TestKeyEncoder) {
  faststring fs;
  KeyEncoder enc(&fs);

  typedef boost::tuple<Slice, Slice> test_pair;

  vector<test_pair> pairs = boost::assign::tuple_list_of
    (Slice("foo", 3), Slice("foo\x00\x00", 5))
    (Slice("xxx\x00yyy", 7), Slice("xxx\x00\x01yyy\x00\x00", 10));

  BOOST_FOREACH(test_pair &t, pairs) {
    Slice in = boost::get<0>(t);
    Slice out = boost::get<1>(t);

    fs.clear();
    enc.EncodeBytes(in);
    ASSERT_EQ(0, out.compare(Slice(fs)))
      << "Failed encoding.\n"
      << "Encoded:  " << HexDump(in)
      << "Expected: " << HexDump(out)
      << "Got:      " << HexDump(Slice(fs));
  }
}

}
}
