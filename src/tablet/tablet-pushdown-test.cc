// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/assign/list_of.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/schema.h"
#include "tablet/tablet.h"
#include "tablet/tablet-test-base.h"
#include "util/test_macros.h"
#include "util/test_util.h"

namespace kudu {
namespace tablet {

class TabletPushdownTest : public KuduTest {
 public:
  TabletPushdownTest()
    : schema_(boost::assign::list_of
              (ColumnSchema("key", UINT32))
              (ColumnSchema("int_val", UINT32))
              (ColumnSchema("string_val", STRING)),
              1) {
  }

  virtual void SetUp() {
    KuduTest::SetUp();
    tablet_dir_ = env_->JoinPathSegments(test_dir_, "tablet");

    CreateTestTablet();
  }

  void CreateTestTablet() {
    tablet_.reset(new Tablet(schema_, tablet_dir_));
    ASSERT_STATUS_OK(tablet_->CreateNew());
    ASSERT_STATUS_OK(tablet_->Open());

    RowBuilder rb(schema_);

    uint64_t nrows = 2100;
    if (AllowSlowTests()) {
      nrows = 100000;
    }

    for (uint64_t i = 0; i < nrows; i++) {
      rb.Reset();
      rb.AddUint32(i);
      rb.AddUint32(i * 10);
      rb.AddString(StringPrintf("%08ld", i));

      ASSERT_STATUS_OK_FAST(tablet_->Insert(rb.row()));

      // Flush at 90% so we have some rows on disk and some in memrowset
      if (i == nrows * 9 / 10) {
        ASSERT_STATUS_OK(tablet_->Flush());
      }
    }
  }

 protected:
  const Schema schema_;
  string tablet_dir_;
  gscoped_ptr<Tablet> tablet_;
};


TEST_F(TabletPushdownTest, TestPushdownIntValueRange) {
  // Push down a double-ended range on the integer value column.
  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &iter));

  ScanSpec spec;
  uint32_t lower = 2000;
  uint32_t upper = 2100;
  ColumnRangePredicate pred1(schema_.column(1), &lower, &upper);
  spec.AddPredicate(pred1);

  ASSERT_STATUS_OK(iter->Init(&spec));
  ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

  vector<string> results;
  LOG_TIMING(INFO, "Filtering by int value") {
    ASSERT_STATUS_OK(IterateToStringList(iter.get(), &results));
  }
  BOOST_FOREACH(const string &str, results) {
    LOG(INFO) << str;
  }
  ASSERT_EQ(11, results.size());
  ASSERT_EQ("(uint32 key=200, uint32 int_val=2000, string string_val=00000200)",
            results[0]);
  ASSERT_EQ("(uint32 key=210, uint32 int_val=2100, string string_val=00000210)",
            results[10]);
  // TODO: collect IO statistics per column, verify that most of the string blocks
  // were not read.
}

} // namespace tablet
} // namespace kudu
