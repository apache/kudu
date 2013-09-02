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

  void InsertBaseData() {
    for (int i = 0; i < 10; ++i) {
      RowBuilder rb(schema_);
      rb.AddInt32(i);
      rb.AddUint32(i);
      rb.AddString(boost::lexical_cast<string>(i));
      rb.AddInt32(i);
      TransactionContext tx_ctx;
      tablet_->Insert(&tx_ctx, rb.row());
    }
    tablet_->Flush();
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
  Schema projection(boost::assign::list_of
                    (ColumnSchema("key", INT32))
                    (ColumnSchema("v1", UINT32))
                    (ColumnSchema("v3", INT32))
                    (ColumnSchema("v4", UINT64)),
                    1);

  InsertBaseData();

  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(tablet_->NewRowIterator(projection, &iter));
  Status s = iter->Init(NULL);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "Not Implemented Default Value Iterator");
}

} // namespace tablet
} // namespace kudu
