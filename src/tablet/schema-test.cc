// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "tablet/schema.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

using std::vector;

TEST(TestSchema, TestSchema) {
  ColumnSchema col1("key", kudu::cfile::STRING);
  ColumnSchema col2("val", kudu::cfile::UINT32);

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
                 (ColumnSchema("col1", kudu::cfile::STRING))
                 (ColumnSchema("col2", kudu::cfile::STRING))
                 (ColumnSchema("col3", kudu::cfile::UINT32)),
                 1);
  Schema schema2(boost::assign::list_of
                 (ColumnSchema("col3", kudu::cfile::UINT32))
                 (ColumnSchema("col2", kudu::cfile::STRING)),
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
                 (ColumnSchema("key", kudu::cfile::STRING))
                 (ColumnSchema("val", kudu::cfile::UINT32)),
                 1);
  Schema schema2(boost::assign::list_of
                 (ColumnSchema("val", kudu::cfile::STRING)),
                 1);

  vector<size_t> proj;
  Status s = schema2.GetProjectionFrom(schema1, &proj);
  ASSERT_STR_CONTAINS(s.ToString(), "type mismatch");
}

// Test projection when the type of the projected column
// doesn't match the original type.
TEST(TestSchema, TestProjectMissingColumn) {
  Schema schema1(boost::assign::list_of
                 (ColumnSchema("key", kudu::cfile::STRING))
                 (ColumnSchema("val", kudu::cfile::UINT32)),
                 1);
  Schema schema2(boost::assign::list_of
                 (ColumnSchema("val", kudu::cfile::UINT32))
                 (ColumnSchema("non_present", kudu::cfile::STRING)),
                 1);

  vector<size_t> proj;
  Status s = schema2.GetProjectionFrom(schema1, &proj);
  ASSERT_STR_CONTAINS(s.ToString(), "column 'non_present' not present");
}

// TODO: write test for comparison (move RowBuilder somewhere, use that)
}
}
