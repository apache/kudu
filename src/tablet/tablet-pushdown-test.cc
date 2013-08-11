// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/assign/list_of.hpp>
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

enum Setup {
  ALL_IN_MEMORY,
  SPLIT_MEMORY_DISK,
  ALL_ON_DISK
};

class TabletPushdownTest : public KuduTabletTest,
                           public ::testing::WithParamInterface<Setup> {
 public:
  TabletPushdownTest()
    : KuduTabletTest(Schema(boost::assign::list_of
              (ColumnSchema("key", UINT32))
              (ColumnSchema("int_val", UINT32))
              (ColumnSchema("string_val", STRING)),
              1)) {
  }

  virtual void SetUp() {
    KuduTabletTest::SetUp();

    FillTestTablet();
  }

  void FillTestTablet() {
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

      if (i == 205 && GetParam() == SPLIT_MEMORY_DISK) {
        ASSERT_STATUS_OK(tablet_->Flush());
      }
    }

    if (GetParam() == ALL_ON_DISK) {
      ASSERT_STATUS_OK(tablet_->Flush());
    }
  }

  // The predicates tested in the various test cases all yield
  // the same set of rows. Run the scan and verify that the
  // expected rows are returned.
  void TestScanYieldsExpectedResults(ScanSpec spec) {
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &iter));
    ASSERT_STATUS_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    vector<string> results;
    LOG_TIMING(INFO, "Filtering by int value") {
      ASSERT_STATUS_OK(IterateToStringList(iter.get(), &results));
    }
    std::sort(results.begin(), results.end());
    BOOST_FOREACH(const string &str, results) {
      LOG(INFO) << str;
    }
    ASSERT_EQ(11, results.size());
    ASSERT_EQ("(uint32 key=200, uint32 int_val=2000, string string_val=00000200)",
              results[0]);
    ASSERT_EQ("(uint32 key=210, uint32 int_val=2100, string string_val=00000210)",
              results[10]);
  }
};

TEST_P(TabletPushdownTest, TestPushdownIntKeyRange) {
  ScanSpec spec;
  uint32_t lower = 200;
  uint32_t upper = 210;
  ColumnRangePredicate pred0(schema_.column(0), &lower, &upper);
  spec.AddPredicate(pred0);

  TestScanYieldsExpectedResults(spec);
}

TEST_P(TabletPushdownTest, TestPushdownIntValueRange) {
  // Push down a double-ended range on the integer value column.

  ScanSpec spec;
  uint32_t lower = 2000;
  uint32_t upper = 2100;
  ColumnRangePredicate pred1(schema_.column(1), &lower, &upper);
  spec.AddPredicate(pred1);

  TestScanYieldsExpectedResults(spec);
  // TODO: collect IO statistics per column, verify that most of the string blocks
  // were not read.
}

INSTANTIATE_TEST_CASE_P(AllMemory, TabletPushdownTest, ::testing::Values(ALL_IN_MEMORY));
INSTANTIATE_TEST_CASE_P(SplitMemoryDisk, TabletPushdownTest, ::testing::Values(SPLIT_MEMORY_DISK));
INSTANTIATE_TEST_CASE_P(AllDisk, TabletPushdownTest, ::testing::Values(ALL_ON_DISK));

} // namespace tablet
} // namespace kudu
