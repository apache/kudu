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

  virtual void SetUp() OVERRIDE {
    KuduTabletTest::SetUp();

    FillTestTablet();
  }

  void FillTestTablet() {
    RowBuilder rb(schema_);

    nrows_ = 2100;
    if (AllowSlowTests()) {
      nrows_ = 100000;
    }

    WriteTransactionState tx_state;
    for (uint64_t i = 0; i < nrows_; i++) {
      rb.Reset();
      rb.AddUint32(i);
      rb.AddUint32(i * 10);
      rb.AddString(StringPrintf("%08ld", i));

      ASSERT_STATUS_OK_FAST(tablet_->InsertForTesting(&tx_state, rb.row()));

      if (i == 205 && GetParam() == SPLIT_MEMORY_DISK) {
        ASSERT_STATUS_OK(tablet_->Flush());
      }
      tx_state.Reset();
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

    int expected_blocks_from_disk;
    int expected_rows_from_disk;
    bool check_stats = true;
    switch (GetParam()) {
      case ALL_IN_MEMORY:
        expected_blocks_from_disk = 0;
        expected_rows_from_disk = 0;
        break;
      case SPLIT_MEMORY_DISK:
        expected_blocks_from_disk = 1;
        expected_rows_from_disk = 206;
        break;
      case ALL_ON_DISK:
        // If AllowSlowTests() is true and all data is on disk
        // (vs. first 206 rows -- containing the values we're looking
        // for -- on disk and the rest in-memory), then the number
        // of blocks and rows we will scan through can't be easily
        // determined (as it depends on default cfile block size, the
        // size of cfile header, and how much data each column takes
        // up).
        if (AllowSlowTests()) {
          check_stats = false;
        } else {
          // If AllowSlowTests() is false, then all of the data fits
          // into a single cfile.
          expected_blocks_from_disk = 1;
          expected_rows_from_disk = nrows_;
        }
        break;
    }
    if (check_stats) {
      vector<IteratorStats> stats;
      iter->GetIteratorStats(&stats);
      BOOST_FOREACH(const IteratorStats& col_stats, stats) {
        EXPECT_EQ(expected_blocks_from_disk, col_stats.data_blocks_read_from_disk);
        EXPECT_EQ(expected_rows_from_disk, col_stats.rows_read_from_disk);
      }
    }
  }

  // Test that a scan with an empty projection and the given spec
  // returns the expected number of rows. The rows themselves
  // should be empty.
  void TestCountOnlyScanYieldsExpectedResults(ScanSpec spec) {
    Schema empty_schema(std::vector<ColumnSchema>(), 0);
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(tablet_->NewRowIterator(empty_schema, &iter));
    ASSERT_STATUS_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    vector<string> results;
    ASSERT_STATUS_OK(IterateToStringList(iter.get(), &results));
    ASSERT_EQ(11, results.size());
    BOOST_FOREACH(const string& result, results) {
      ASSERT_EQ("()", result);
    }
  }
 private:
  uint64_t nrows_;
};

TEST_P(TabletPushdownTest, TestPushdownIntKeyRange) {
  ScanSpec spec;
  uint32_t lower = 200;
  uint32_t upper = 210;
  ColumnRangePredicate pred0(schema_.column(0), &lower, &upper);
  spec.AddPredicate(pred0);

  TestScanYieldsExpectedResults(spec);
  TestCountOnlyScanYieldsExpectedResults(spec);
}

TEST_P(TabletPushdownTest, TestPushdownIntValueRange) {
  // Push down a double-ended range on the integer value column.

  ScanSpec spec;
  uint32_t lower = 2000;
  uint32_t upper = 2100;
  ColumnRangePredicate pred1(schema_.column(1), &lower, &upper);
  spec.AddPredicate(pred1);

  TestScanYieldsExpectedResults(spec);

  // TODO: support non-key predicate pushdown on columns which aren't
  // part of the projection. The following line currently would crash.
  // TestCountOnlyScanYieldsExpectedResults(spec);

  // TODO: collect IO statistics per column, verify that most of the string blocks
  // were not read.
}

INSTANTIATE_TEST_CASE_P(AllMemory, TabletPushdownTest, ::testing::Values(ALL_IN_MEMORY));
INSTANTIATE_TEST_CASE_P(SplitMemoryDisk, TabletPushdownTest, ::testing::Values(SPLIT_MEMORY_DISK));
INSTANTIATE_TEST_CASE_P(AllDisk, TabletPushdownTest, ::testing::Values(ALL_ON_DISK));

} // namespace tablet
} // namespace kudu
