// Copyright (c) 2013, Cloudera,inc.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/schema.h"
#include "common/row_changelist.h"
#include "util/faststring.h"
#include "util/hexdump.h"
#include "util/test_macros.h"

namespace kudu {


TEST(TestRowChangeList, TestEncodeDecode) {
  Schema schema(boost::assign::list_of
                (ColumnSchema("col1", STRING))
                (ColumnSchema("col2", STRING))
                (ColumnSchema("col3", UINT32)),
                1);
  
  faststring buf;
  RowChangeListEncoder rcl(schema, &buf);

  // Construct an update with several columns changed
  Slice update1("update1");
  Slice update2("update2");
  uint32 update3 = 12345;

  rcl.AddColumnUpdate(0, &update1);
  rcl.AddColumnUpdate(1, &update2);
  rcl.AddColumnUpdate(2, &update3);

  LOG(INFO) << "Encoded: " << HexDump(buf);

  // Read it back.
  RowChangeListDecoder decoder(schema, RowChangeList(buf));
  EXPECT_EQ(string("SET col1=update1, col2=update2, col3=12345"),
            RowChangeList(Slice(buf)).ToString(schema));

  size_t idx;
  const void *val;

  ASSERT_TRUE(decoder.HasNext());
  ASSERT_STATUS_OK(decoder.DecodeNext(&idx, &val));
  ASSERT_EQ(0, idx);
  ASSERT_TRUE(update1 == *reinterpret_cast<const Slice *>(val));

  ASSERT_TRUE(decoder.HasNext());
  ASSERT_STATUS_OK(decoder.DecodeNext(&idx, &val));
  ASSERT_EQ(1, idx);
  ASSERT_TRUE(update2 == *reinterpret_cast<const Slice *>(val));

  ASSERT_TRUE(decoder.HasNext());
  ASSERT_STATUS_OK(decoder.DecodeNext(&idx, &val));
  ASSERT_EQ(2, idx);

  ASSERT_FALSE(decoder.HasNext());
}

} // namespace kudu
