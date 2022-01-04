// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::unique_ptr;
using std::vector;

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
    : KuduTabletTest(Schema({ ColumnSchema("key", INT32),
                              ColumnSchema("int_val", INT32),
                              ColumnSchema("string_val", STRING) }, 1)) {
  }

  void SetUp() override {
    KuduTabletTest::SetUp();

    FillTestTablet();
  }

  void FillTestTablet() {
    RowBuilder rb(&client_schema_);

    nrows_ = 2100;
    if (AllowSlowTests()) {
      nrows_ = 100000;
    }

    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);
    for (int64_t i = 0; i < nrows_; i++) {
      CHECK_OK(row.SetInt32(0, i));
      CHECK_OK(row.SetInt32(1, i * 10));
      CHECK_OK(row.SetStringCopy(2, StringPrintf("%08" PRId64, i)));
      ASSERT_OK_FAST(writer.Insert(row));

      if (i == 205 && GetParam() == SPLIT_MEMORY_DISK) {
        ASSERT_OK(tablet()->Flush());
      }
    }

    if (GetParam() == ALL_ON_DISK) {
      ASSERT_OK(tablet()->Flush());
    }
  }

  // The predicates tested in the various test cases all yield
  // the same set of rows. Run the scan and verify that the
  // expected rows are returned.
  void TestScanYieldsExpectedResults(ScanSpec spec) {
    Arena arena(128);
    spec.OptimizeScan(schema_, &arena, true);

    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_ptr_, &iter));
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    vector<string> results;
    LOG_TIMING(INFO, "Filtering by int value") {
      ASSERT_OK(IterateToStringList(iter.get(), &results));
    }
    std::sort(results.begin(), results.end());
    for (const string &str : results) {
      LOG(INFO) << str;
    }
    ASSERT_EQ(11, results.size());
    ASSERT_EQ(R"((int32 key=200, int32 int_val=2000, string string_val="00000200"))",
              results[0]);
    ASSERT_EQ(R"((int32 key=210, int32 int_val=2100, string string_val="00000210"))",
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
      for (const IteratorStats& col_stats : stats) {
        EXPECT_EQ(expected_blocks_from_disk, col_stats.blocks_read);
        EXPECT_EQ(expected_rows_from_disk, col_stats.cells_read);
      }
    }
  }

  // Test that a scan with an empty projection and the given spec
  // returns the expected number of rows. The rows themselves
  // should be empty.
  void TestCountOnlyScanYieldsExpectedResults(ScanSpec spec) {
    Arena arena(128);
    spec.OptimizeScan(schema_, &arena, true);

    SchemaPtr empty_schema_ptr = std::make_shared<Schema>(std::vector<ColumnSchema>(), 0);
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(empty_schema_ptr, &iter));
    ASSERT_OK(iter->Init(&spec));
    ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

    vector<string> results;
    ASSERT_OK(IterateToStringList(iter.get(), &results));
    ASSERT_EQ(11, results.size());
    for (const string& result : results) {
      ASSERT_EQ("()", result);
    }
  }
 private:
  uint64_t nrows_;
};

TEST_P(TabletPushdownTest, TestPushdownIntKeyRange) {
  ScanSpec spec;
  int32_t lower = 200;
  int32_t upper = 211;
  auto pred0 = ColumnPredicate::Range(schema_.column(0), &lower, &upper);
  spec.AddPredicate(pred0);

  TestScanYieldsExpectedResults(spec);
  TestCountOnlyScanYieldsExpectedResults(spec);
}

TEST_P(TabletPushdownTest, TestPushdownIntValueRange) {
  // Push down a double-ended range on the integer value column.

  ScanSpec spec;
  int32_t lower = 2000;
  int32_t upper = 2101;
  auto pred1 = ColumnPredicate::Range(schema_.column(1), &lower, &upper);
  spec.AddPredicate(pred1);

  TestScanYieldsExpectedResults(spec);

  // TODO: support non-key predicate pushdown on columns which aren't
  // part of the projection. The following line currently would crash.
  // TestCountOnlyScanYieldsExpectedResults(spec);

  // TODO: collect IO statistics per column, verify that most of the string blocks
  // were not read.
}

INSTANTIATE_TEST_SUITE_P(AllMemory, TabletPushdownTest, ::testing::Values(ALL_IN_MEMORY));
INSTANTIATE_TEST_SUITE_P(SplitMemoryDisk, TabletPushdownTest, ::testing::Values(SPLIT_MEMORY_DISK));
INSTANTIATE_TEST_SUITE_P(AllDisk, TabletPushdownTest, ::testing::Values(ALL_ON_DISK));

class TabletSparsePushdownTest : public KuduTabletTest {
 public:
  TabletSparsePushdownTest()
      : KuduTabletTest(Schema({ ColumnSchema("key", INT32),
                                ColumnSchema("val", INT32, true) },
                              1)) {
  }

  void SetUp() override {
    KuduTabletTest::SetUp();

    RowBuilder rb(&client_schema_);

    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow row(&client_schema_);

    // FLAGS_scanner_batch_size_rows * 2
    int kWidth = 100 * 2;
    for (int i = 0; i < kWidth * 2; i++) {
      CHECK_OK(row.SetInt32(0, i));
      if (i % 3 == 0) {
        CHECK_OK(row.SetNull(1));
      } else {
        CHECK_OK(row.SetInt32(1, i % kWidth));
      }
      ASSERT_OK_FAST(writer.Insert(row));
    }
    ASSERT_OK(tablet()->Flush());
  }
};

// The is a regression test for KUDU-2231, which fixed a CFileReader bug causing
// blocks to be repeatedly materialized when a scan had predicates on other
// columns which caused sections of a column to be skipped. The setup for this
// test creates a dataset and predicate which only match every-other batch of
// 100 rows of the 'val' column.
TEST_F(TabletSparsePushdownTest, Kudu2231) {
  ScanSpec spec;
  int32_t value = 50;
  spec.AddPredicate(ColumnPredicate::Equality(schema_.column(1), &value));

  unique_ptr<RowwiseIterator> iter;
  ASSERT_OK(tablet()->NewRowIterator(client_schema_ptr_, &iter));
  ASSERT_OK(iter->Init(&spec));
  ASSERT_TRUE(spec.predicates().empty()) << "Should have accepted all predicates";

  vector<string> results;
  ASSERT_OK(IterateToStringList(iter.get(), &results));

  EXPECT_EQ(results, vector<string>({
      "(int32 key=50, int32 val=50)",
      "(int32 key=250, int32 val=50)",
  }));

  vector<IteratorStats> stats;
  iter->GetIteratorStats(&stats);

  ASSERT_EQ(2, stats.size());
  EXPECT_EQ(1, stats[0].blocks_read);
  EXPECT_EQ(1, stats[1].blocks_read);

  EXPECT_EQ(400, stats[0].cells_read);
  EXPECT_EQ(400, stats[1].cells_read);
}

} // namespace tablet
} // namespace kudu
