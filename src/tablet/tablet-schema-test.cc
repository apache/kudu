// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/assign/list_of.hpp>
#include <boost/lexical_cast.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "common/schema.h"
#include "tablet/tablet.h"
#include "tablet/tablet-test-base.h"
#include "util/test_macros.h"
#include "util/test_util.h"

namespace kudu {
namespace tablet {

class TestTabletSchema : public KuduTabletTest {
 public:
  TestTabletSchema()
    : KuduTabletTest(CreateBaseSchema()) {
  }

  void InsertBaseData(size_t nrows) {
    for (size_t i = 0; i < nrows; ++i) {
      RowBuilder rb(schema_);
      rb.AddInt32(i);
      rb.AddUint32(i);
      rb.AddString(boost::lexical_cast<string>(i));
      rb.AddInt32(i);
      TransactionContext tx_ctx;
      ASSERT_STATUS_OK(tablet_->Insert(&tx_ctx, rb.row()));
      // Half of the rows will be on disk
      // and the other half in the MemRowSet
      if (i == (nrows / 2)) {
        ASSERT_STATUS_OK(tablet_->Flush());
      }
    }
  }

 private:
  Schema CreateBaseSchema() {
    return Schema(boost::assign::list_of
                  (ColumnSchema("key", INT32))
                  (ColumnSchema("v1", UINT32))
                  (ColumnSchema("v2", STRING))
                  (ColumnSchema("v3", INT32)),
                  1);
  }
};

TEST_F(TestTabletSchema, TestRead) {
  const size_t kNumRows = 10;
  const uint64_t v4_default = 25;
  const Slice v5_default("Hello World");
  Schema projection(boost::assign::list_of
                    (ColumnSchema("key", INT32))
                    (ColumnSchema("v1", UINT32))
                    (ColumnSchema("v3", INT32))
                    (ColumnSchema("v4", UINT64, false, &v4_default))
                    (ColumnSchema("v5", STRING, false, &v5_default)),
                    1);

  InsertBaseData(kNumRows);

  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(tablet_->NewRowIterator(projection, &iter));
  ASSERT_STATUS_OK(iter->Init(NULL));

  size_t nrows = 0;
  Arena arena(32*1024, 256*1024);
  RowBlock block(projection, 20, &arena);
  while (iter->HasNext()) {
    ASSERT_STATUS_OK(RowwiseIterator::CopyBlock(iter.get(), &block));

    for (int i = 0; i < block.nrows(); ++i) {
      RowBlockRow row = block.row(i);
      int32_t key = *projection.ExtractColumnFromRow<INT32>(row, 0);
      ASSERT_TRUE(key >= 0 && key <= kNumRows);
      ASSERT_EQ(key, *projection.ExtractColumnFromRow<INT32>(row, 0));
      ASSERT_EQ(key, *projection.ExtractColumnFromRow<UINT32>(row, 1));
      ASSERT_EQ(key, *projection.ExtractColumnFromRow<INT32>(row, 2));
      ASSERT_EQ(v4_default, *projection.ExtractColumnFromRow<UINT64>(row, 3));
      ASSERT_EQ(v5_default, *projection.ExtractColumnFromRow<STRING>(row, 4));
      nrows++;
    }
  }
  ASSERT_EQ(kNumRows, nrows);
}

} // namespace tablet
} // namespace kudu
