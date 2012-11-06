// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <vector>

#include "tablet/schema.h"

namespace kudu {
namespace tablet {

using std::vector;

TEST(TestSchema, TestSchema) {
  ColumnSchema col1(kudu::cfile::STRING);
  ColumnSchema col2(kudu::cfile::UINT32);

  vector<ColumnSchema> cols = boost::assign::list_of
    (col1)(col2);
  Schema schema(cols, 1);

  ASSERT_EQ(sizeof(Slice) + sizeof(uint32_t),
            schema.byte_size());
  ASSERT_EQ(2, schema.num_columns());
  ASSERT_EQ(0, schema.column_offset(0));
  ASSERT_EQ(sizeof(Slice), schema.column_offset(1));
}

// TODO: write test for comparison (move RowBuilder somewhere, use that)
}
}
