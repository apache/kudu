// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <boost/assign/list_of.hpp>

#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/schema.h"
#include "kudu/util/test_util.h"

namespace kudu {

class PartialRowTest : public KuduTest {
 public:
  PartialRowTest()
    : schema_(boost::assign::list_of
              (ColumnSchema("key", UINT32))
              (ColumnSchema("int_val", UINT32))
              (ColumnSchema("string_val", STRING, true)),
              1) {
    SeedRandom();
  }
 protected:
  Schema schema_;
};

TEST_F(PartialRowTest, UnitTest) {
  KuduPartialRow row(&schema_);

  // Initially all columns are unset.
  EXPECT_FALSE(row.IsColumnSet(0));
  EXPECT_FALSE(row.IsColumnSet(1));
  EXPECT_FALSE(row.IsColumnSet(2));
  EXPECT_FALSE(row.IsKeySet());
  EXPECT_EQ("", row.ToString());

  // Set just the key.
  EXPECT_OK(row.SetUInt32("key", 12345));
  EXPECT_TRUE(row.IsKeySet());
  EXPECT_FALSE(row.IsColumnSet(1));
  EXPECT_FALSE(row.IsColumnSet(2));
  EXPECT_EQ("uint32 key=12345", row.ToString());
  uint32_t x;
  EXPECT_OK(row.GetUInt32("key", &x));
  EXPECT_EQ(12345, x);
  EXPECT_FALSE(row.IsNull("key"));

  // Fill in the other columns.
  EXPECT_OK(row.SetUInt32("int_val", 54321));
  EXPECT_OK(row.SetStringCopy("string_val", "hello world"));
  EXPECT_TRUE(row.IsColumnSet(1));
  EXPECT_TRUE(row.IsColumnSet(2));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321, string string_val=hello world",
            row.ToString());
  Slice slice;
  EXPECT_OK(row.GetString("string_val", &slice));
  EXPECT_EQ("hello world", slice.ToString());
  EXPECT_FALSE(row.IsNull("key"));

  // Set a nullable entry to NULL
  EXPECT_OK(row.SetNull("string_val"));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321, string string_val=NULL",
            row.ToString());
  EXPECT_TRUE(row.IsNull("string_val"));

  // Try to set an entry with the wrong type
  Status s = row.SetStringCopy("int_val", "foo");
  EXPECT_EQ("Invalid argument: invalid type string provided for column 'int_val' (expected uint32)",
            s.ToString());

  // Try to get an entry with the wrong type
  s = row.GetString("int_val", &slice);
  EXPECT_EQ("Invalid argument: invalid type string provided for column 'int_val' (expected uint32)",
            s.ToString());

  // Try to set a non-nullable entry to NULL
  s = row.SetNull("key");
  EXPECT_EQ("Invalid argument: column not nullable: key[uint32 NOT NULL]", s.ToString());

  // Set the NULL string back to non-NULL
  EXPECT_OK(row.SetStringCopy("string_val", "goodbye world"));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321, string string_val=goodbye world",
            row.ToString());

  // Unset some columns.
  EXPECT_OK(row.Unset("string_val"));
  EXPECT_EQ("uint32 key=12345, uint32 int_val=54321", row.ToString());

  EXPECT_OK(row.Unset("key"));
  EXPECT_EQ("uint32 int_val=54321", row.ToString());

  // Set the column by index
  EXPECT_OK(row.SetUInt32(1, 99999));
  EXPECT_EQ("uint32 int_val=99999", row.ToString());
}


} // namespace kudu
