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

#include "kudu/tablet/compaction_policy.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/mock-rowsets.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_info.h"
#include "kudu/tablet/rowset_tree.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

DECLARE_double(compaction_minimum_improvement);
DECLARE_double(compaction_small_rowset_tradeoff);
DECLARE_int64(budgeted_compaction_target_rowset_size);

namespace kudu {
namespace tablet {

class TestCompactionPolicy : public KuduTest {
 protected:
  void RunTestCase(const RowSetVector& vec,
                   int size_budget_mb,
                   CompactionSelection* picked,
                   double* quality) {
    RowSetTree tree;
    ASSERT_OK(tree.Reset(vec));

    BudgetedCompactionPolicy policy(size_budget_mb);

    ASSERT_OK(policy.PickRowSets(tree, picked, quality, /*log=*/nullptr));
  }
};

// Simple test for budgeted compaction: with three rowsets which
// mostly overlap, and a high budget, they should all be selected.
TEST_F(TestCompactionPolicy, TestBudgetedSelection) {
  // This tests the overlap-based part of compaction policy and not the
  // rowset-size based policy.
  FLAGS_compaction_small_rowset_tradeoff = 0.0;

  /*
   *   [C ------ c]
   *  [B ----- a]
   * [A ------- b]
   */
  const RowSetVector rowsets = {
    std::make_shared<MockDiskRowSet>("C", "c"),
    std::make_shared<MockDiskRowSet>("B", "a"),
    std::make_shared<MockDiskRowSet>("A", "b")
  };

  constexpr auto kBudgetMb = 1000; // Enough to select all rowsets.
  CompactionSelection picked;
  double quality = 0.0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(rowsets.size(), picked.size());
  // Since all three rowsets are picked and each covers almost the entire key
  // range, the sum of widths is about 3 while the union width is 1, so the
  // quality score should be between 1 and 2.
  ASSERT_GE(quality, 1.0);
  ASSERT_LE(quality, 2.0);
}

// Test for the case when we have many rowsets, but none of them
// overlap at all. This is likely to occur in workloads where the
// primary key is always increasing (such as a timestamp).
TEST_F(TestCompactionPolicy, TestNonOverlappingRowSets) {
  // This tests the overlap-based part of compaction policy and not the
  // rowset-size based policy.
  FLAGS_compaction_small_rowset_tradeoff = 0.0;

  /* NB: Zero-padding of string keys omitted to save space.
   *
   * [0 - 1] [2 - 3] ... [198 - 199]
   */
  const auto kNumRowSets = AllowSlowTests() ? 10000 : 100;
  RowSetVector rowsets;
  for (auto i = 0; i < kNumRowSets; i++) {
    rowsets.emplace_back(new MockDiskRowSet(
        StringPrintf("%010d", i * 2),
        StringPrintf("%010d", i * 2 + 1)));
  }
  constexpr auto kBudgetMb = 128;
  CompactionSelection picked;
  double quality = 0.0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(0.0, quality);
  ASSERT_TRUE(picked.empty());
}

// Similar to the above, but with some small overlap between adjacent
// rowsets.
TEST_F(TestCompactionPolicy, TestTinyOverlapRowSets) {
  // This tests the overlap-based part of compaction policy and not the
  // rowset-size based policy.
  FLAGS_compaction_small_rowset_tradeoff = 0.0;

  /* NB: Zero-padding of string keys omitted to save space.
   *
   * [0 - 11000]
   *    [10000 - 21000]
   *           ...
   *                    [990000 - 1001000]
   */
  const auto kNumRowSets = AllowSlowTests() ? 10000 : 100;
  RowSetVector rowsets;
  for (auto i = 0; i < kNumRowSets; i++) {
    rowsets.emplace_back(new MockDiskRowSet(
        StringPrintf("%09d", i * 10000),
        StringPrintf("%09d", i * 10000 + 11000)));
  }
  constexpr auto kBudgetMb = 128;
  CompactionSelection picked;
  double quality = 0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  // With such small overlaps, no compaction will be considered worthwhile.
  ASSERT_EQ(0.0, quality);
  ASSERT_TRUE(picked.empty());
}

// Test case with 100 rowsets, each of which overlaps with its two
// neighbors to the right.
TEST_F(TestCompactionPolicy, TestSignificantOverlap) {
  // This tests the overlap-based part of compaction policy and not the
  // rowset-size based policy.
  FLAGS_compaction_small_rowset_tradeoff = 0.0;

  /* NB: Zero-padding of string keys omitted to save space.
   *
   * [0 ------ 20000]
   *    [10000 ------ 30000]
   *           [20000 ------ 40000]
   *                  ...
   *                                [990000 - 1010000]
   */
  constexpr auto kNumRowSets = 100;
  RowSetVector rowsets;
  for (auto i = 0; i < kNumRowSets;  i++) {
    rowsets.emplace_back(new MockDiskRowSet(
        StringPrintf("%010d", i * 10000),
        StringPrintf("%010d", (i + 2) * 10000)));
  }
  constexpr auto kBudgetMb = 64;
  CompactionSelection picked;
  double quality = 0.0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  // Each rowset is 1MB so the number of rowsets picked should be the number of
  // MB in the budget.
  ASSERT_EQ(kBudgetMb, picked.size());

  // In picking 64 of 100 equally-sized and -spaced rowsets, the union width is
  // about 0.64. Each rowset intersects the next in half its width, which makes
  // the sum of widths about 2 * 0.64 = 1.28, if we ignore the smaller overlap
  // between the ith and (i + 2)th rowsets. So, the quality score should be
  // around 0.64.
  ASSERT_GT(quality, 0.5);
}

// Test the adjustment we make to the quality measure that penalizes wider
// solutions that have almost the same quality score. For example, with no
// adjustment and inputs
//
//  [ -- A -- ]
//  [ -- B -- ]
//            [ -- C -- ]
//
// compacting {A, B, C} results in the same reduction in average tablet height
// as compacting {A, B}, but uses more I/O. We'd like to choose {A, B} if
// A, B, and C are large, but {A, B, C} if A, B, and C are small.
TEST_F(TestCompactionPolicy, TestSupportAdjust) {
  constexpr auto kBudgetMb = 1000; // Enough to select all rowsets.
  CompactionSelection picked;
  double quality = 0.0;

  // For big rowsets, compaction should favor just compacting the two that
  // overlap.
  const auto big_rowset_size = FLAGS_budgeted_compaction_target_rowset_size * 0.9;
  const RowSetVector big_rowsets = {
    std::make_shared<MockDiskRowSet>("A", "B", big_rowset_size),
    std::make_shared<MockDiskRowSet>("A", "B", big_rowset_size),
    std::make_shared<MockDiskRowSet>("B", "C", big_rowset_size),
  };
  NO_FATALS(RunTestCase(big_rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(2, picked.size());
  // The widths of A and B are both 0.67, so with the adjustment the quality
  // score should be a little less than 0.67.
  ASSERT_GE(quality, 0.6);

  // For small rowsets, compaction should favor compacting all three.
  const auto small_rowset_size = FLAGS_budgeted_compaction_target_rowset_size * 0.3;
  const RowSetVector small_rowsets = {
    std::make_shared<MockDiskRowSet>("A", "B", small_rowset_size),
    std::make_shared<MockDiskRowSet>("A", "B", small_rowset_size),
    std::make_shared<MockDiskRowSet>("B", "C", small_rowset_size),
  };
  picked.clear();
  NO_FATALS(RunTestCase(small_rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(3, picked.size());
  ASSERT_GE(quality, 0.6);
}

static RowSetVector LoadFile(const string& name) {
  RowSetVector rowsets;
  const string path = JoinPathSegments(GetTestExecutableDirectory(), name);
  faststring data;
  CHECK_OK_PREPEND(ReadFileToString(Env::Default(), path, &data),
                   strings::Substitute("unable to load test data file $0", path));
  const vector<string> lines = strings::Split(data.ToString(), "\n");
  for (const auto& line : lines) {
    if (line.empty() || line[0] == '#') continue;
    const vector<string> fields = strings::Split(line, "\t");
    CHECK_EQ(3, fields.size()) << "Expected 3 fields on line: " << line;
    const int size_mb = ParseLeadingInt32Value(fields[0], -1);
    CHECK_GE(size_mb, 1) << "Expected size at least 1MB on line: " << line;
    rowsets.emplace_back(new MockDiskRowSet(fields[1] /* min key */,
                                            fields[2] /* max key */,
                                            size_mb * 1024 * 1024));
  }

  return rowsets;
}

// Realistic test using data scraped from a tablet containing 200GB+ of YCSB
// data. This test can be used as a benchmark for optimizing the compaction
// policy, and also serves as a basic regression/stress test using real data.
TEST_F(TestCompactionPolicy, TestYcsbCompaction) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }
  const RowSetVector rowsets = LoadFile("testdata/ycsb-test-rowsets.tsv");
  RowSetTree tree;
  ASSERT_OK(tree.Reset(rowsets));
  vector<double> qualities;
  for (int budget_mb : {128, 256, 512, 1024}) {
    BudgetedCompactionPolicy policy(budget_mb);

    CompactionSelection picked;
    double quality = 0.0;
    LOG_TIMING(INFO, strings::Substitute("computing compaction with $0MB budget",
                                         budget_mb)) {
      ASSERT_OK(policy.PickRowSets(tree, &picked, &quality, /*log=*/nullptr));
    }
    LOG(INFO) << "quality=" << quality;
    int total_size = 0;
    for (const auto* rs : picked) {
      total_size += rs->OnDiskBaseDataSizeWithRedos() / 1024 / 1024;
    }
    ASSERT_LE(total_size, budget_mb);
    qualities.push_back(quality);
  }

  // Since the budget increased in each iteration, the qualities should be
  // non-decreasing.
  ASSERT_TRUE(std::is_sorted(qualities.begin(), qualities.end())) << qualities;
}

// Regression test for KUDU-2251 which ensures that large (> 2GiB) rowsets don't
// cause integer overflow in compaction planning.
TEST_F(TestCompactionPolicy, KUDU2251) {
  // Raise the target size to keep the quality score in the same range as for
  // "normal size" rowsets.
  FLAGS_budgeted_compaction_target_rowset_size = 1L << 34;

  // Same arrangement as in TestBudgetedSelection.
  const RowSetVector rowsets = {
    std::make_shared<MockDiskRowSet>("C", "c", 1L << 31),
    std::make_shared<MockDiskRowSet>("B", "a", 1L << 32),
    std::make_shared<MockDiskRowSet>("A", "b", 1L << 33)
  };

  constexpr auto kBudgetMb = 1L << 30; // Enough to select all rowsets.
  CompactionSelection picked;
  double quality = 0.0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(rowsets.size(), picked.size());
  ASSERT_GT(quality, 1.0);
  ASSERT_LT(quality, 2.0);
}

// Test that we don't compact together non-overlapping rowsets if doing so
// doesn't actually reduce the number of rowsets.
TEST_F(TestCompactionPolicy, TestSmallRowsetCompactionReducesRowsetCount) {
  constexpr auto kBudgetMb = 1000000; // Enough to select all rowsets in all cases.
  const auto num_rowsets = 100;
  // Use rowsets close enough to the target size that 'num_rowsets' count of
  // them compacted together does not reduce the count of rowsets.
  const auto rowset_size =
    FLAGS_budgeted_compaction_target_rowset_size * (1 - 1.0 / (num_rowsets - 1));
  RowSetVector rowsets;
  CompactionSelection picked;
  double quality = 0.0;
  for (int i = 0; i < num_rowsets; i++) {
    picked.clear();

    // [0 - 1][1 - 2] ... [98 - 99][99 - 100] built up over time.
    rowsets.push_back(std::make_shared<MockDiskRowSet>(
        StringPrintf("%010d", i),
        StringPrintf("%010d", i + 1),
        rowset_size));
    NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));

    // There should never be a worthwhile compaction.
    ASSERT_TRUE(picked.empty());
    ASSERT_EQ(0.0, quality);
  }
}

// Test that the score of compacting two rowsets decreases monotonically as the
// size of the two rowsets increases, and that the tradeoff factor is set
// correctly so that a compaction actually reduce the count of rowsets.
TEST_F(TestCompactionPolicy, TestSmallRowsetTradeoffFactor) {
  constexpr auto kBudgetMb = 1000; // Enough to select all rowsets in all cases.
  CompactionSelection picked;
  double quality = 0.0;
  double prev_quality = std::numeric_limits<double>::max();

  // Compact two equal-sized rowsets that are various fractions of the target
  // rowset size.
  const double target_size_bytes =
      FLAGS_budgeted_compaction_target_rowset_size;
  for (const auto divisor : { 32, 16, 8, 4, 2, 1 }) {
    SCOPED_TRACE(strings::Substitute("divisor = $0", divisor));
    picked.clear();
    const auto size_bytes = target_size_bytes / divisor;
    /*
     * [A -- B][B -- C]
     */
    const RowSetVector rowsets = {
      std::make_shared<MockDiskRowSet>("A", "B", size_bytes),
      std::make_shared<MockDiskRowSet>("B", "C", size_bytes),
    };
    NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));

    // The quality score should monotonically decrease.
    ASSERT_LE(quality, prev_quality);
    prev_quality = quality;

    // As a spot check of the prioritization, we should stop wanting to compact
    // when the divisor is two or one.
    if (divisor > 2) {
      ASSERT_EQ(rowsets.size(), picked.size());
    } else {
      ASSERT_LT(quality, FLAGS_compaction_minimum_improvement);
      ASSERT_TRUE(picked.empty());
    }
  }

  // However, compacting a target rowset size's worth of data with it split
  // between more than two rowsets should result in a compaction.
  const auto size_bytes = target_size_bytes / 3;
  /*
   * [A -- B][B -- C][C -- D]
   */
  const RowSetVector rowsets = {
    std::make_shared<MockDiskRowSet>("A", "B", size_bytes),
    std::make_shared<MockDiskRowSet>("B", "C", size_bytes),
    std::make_shared<MockDiskRowSet>("C", "D", size_bytes),
  };
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(rowsets.size(), picked.size());
  ASSERT_GT(quality, FLAGS_compaction_minimum_improvement);
}

// Compaction policy is designed so that height-reducing compactions should
// generally take priority over size-based compactions. So, in particular,
// given a layout like
//
// [ -- 32MiB -- ]    [ 8MiB ][ 8 MiB ][8 MiB ][ 8MiB ]
// [ -- 32MiB -- ]
//
// and a budget of 64MiB, the policy should choose to compact the two
// overlapping rowsets despite the fact that that selection has a size-score
// of 0, while compacting one or more of the 8MiB rowsets has a big size score.
TEST_F(TestCompactionPolicy, TestHeightBasedDominatesSizeBased) {
  constexpr auto kBigRowSetSizeBytes = 32 * 1024 * 1024;
  constexpr auto kSmallRowSetSizeBytes = 8 * 1024 * 1024;
  constexpr auto kBudgetMb = 64;
  const RowSetVector rowsets = {
    std::make_shared<MockDiskRowSet>("A", "B", kBigRowSetSizeBytes),
    std::make_shared<MockDiskRowSet>("A", "B", kBigRowSetSizeBytes),
    std::make_shared<MockDiskRowSet>("C", "D", kSmallRowSetSizeBytes),
    std::make_shared<MockDiskRowSet>("D", "E", kSmallRowSetSizeBytes),
    std::make_shared<MockDiskRowSet>("E", "F", kSmallRowSetSizeBytes),
    std::make_shared<MockDiskRowSet>("F", "G", kSmallRowSetSizeBytes),
  };

  CompactionSelection picked;
  double quality = 0.0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(2, picked.size());
  for (const auto* rowset : picked) {
    ASSERT_EQ(kBigRowSetSizeBytes, rowset->OnDiskSize());
  }
}

namespace {
double ComputeAverageRowsetHeight(
    const vector<std::pair<string, string>>& intervals) {
  RowSetVector rowsets;
  for (const auto& interval : intervals) {
    rowsets.push_back(std::make_shared<MockDiskRowSet>(interval.first,
                                                       interval.second));
  }
  RowSetTree tree;
  CHECK_OK(tree.Reset(rowsets));

  double rowset_total_height, rowset_total_width;
  RowSetInfo::ComputeCdfAndCollectOrdered(tree,
                                          &rowset_total_height,
                                          &rowset_total_width,
                                          nullptr,
                                          nullptr);
  return rowset_total_width > 0 ? rowset_total_height / rowset_total_width : 0.0;
}
} // anonymous namespace

class KeySpaceCdfTest : public KuduTest {
 protected:
  static void AssertWithinEpsilon(double epsilon,
                                  double expected,
                                  double actual) {
    ASSERT_GE(actual, expected - epsilon);
    ASSERT_LE(actual, expected + epsilon);
  }
};

// Test the computation of average rowset heights. This isn't strictly used in
// compaction policy, but this is a convenient place to put it, for now.
TEST_F(KeySpaceCdfTest, TestComputingAverageRowSetHeight) {
  // No rowsets.
  EXPECT_EQ(0.0, ComputeAverageRowsetHeight({ }));

  /* A rowset that's one key wide.
   * |
   */
  EXPECT_EQ(0.0, ComputeAverageRowsetHeight({ { "A", "A" } }));

  /* A single rowset.
   * [ --- ]
   */
  EXPECT_EQ(1.0, ComputeAverageRowsetHeight({ { "A", "B" } }));

  /* Two rowsets with no empty space between.
   * [ --- ][ --- ]
   */
  EXPECT_EQ(1.0, ComputeAverageRowsetHeight({ { "A", "B" }, { "B", "C" } }));


  /* Three rowsets with no empty spaces between.
   * [ --- ][ --- ][ --- ]
   */
  EXPECT_EQ(1.0, ComputeAverageRowsetHeight({ { "A", "B" },
                                              { "B", "C" },
                                              { "C", "D" } }));

  /* Two rowsets with empty space between them.
   * [ --- ]       [ --- ]
   */
  EXPECT_EQ(1.0, ComputeAverageRowsetHeight({ { "A", "B" }, { "C", "D" } }));

  /* Three rowsets with empty space between them.
   * [ --- ]       [ --- ]       [ --- ]
   */
  EXPECT_EQ(1.0, ComputeAverageRowsetHeight({ { "A", "B" },
                                              { "C", "D" },
                                              { "E", "F" } }));

  /* Three rowsets with empty space between them, and one is a single key.
   * [ --- ]       |       [ --- ]
   */
  EXPECT_EQ(1.0, ComputeAverageRowsetHeight({ { "A", "B" },
                                              { "C", "C" },
                                              { "D", "D" } }));

  /* Two rowsets that completely overlap.
   * [ --- ]
   * [ --- ]
   */
  EXPECT_EQ(2.0, ComputeAverageRowsetHeight({ { "A", "B" }, { "A", "B" } }));

  /* Three rowsets that completely overlap, but two are single keys, and the
   * overlaps are on the boundaries.
   * [ --- ]
   * |     |
   */
  EXPECT_EQ(1.0, ComputeAverageRowsetHeight({ { "A", "B" },
                                              { "A", "A" },
                                              { "B", "B" } }));

  /* Three rowsets that completely overlap.
   * [ --- ]
   * [ --- ]
   * [ --- ]
   */
  EXPECT_EQ(3.0, ComputeAverageRowsetHeight({ { "A", "B" },
                                              { "A", "B" },
                                              { "A", "B" } }));

  /* Three rowsets that completely overlap, but one is a single key.
   * [ --- ]
   * [ --- ]
   *    |
   */
  EXPECT_EQ(2.0, ComputeAverageRowsetHeight({ { "A", "C" },
                                              { "A", "C" },
                                              { "B", "B" } }));

  /* Two rowsets that partially overlap.
   * [ --- ]
   *    [ --- ]
   */
  EXPECT_EQ(1.5, ComputeAverageRowsetHeight({ { "A", "C" }, { "B", "D" } }));

  // Now the numbers stop being round so we'll check for being within some small
  // range of an expected number.
  constexpr auto epsilon = 0.001;

  /* Three rowsets that partially overlap.
   * [ --- ]
   *    [ --- ]
   *       [ --- ]
   */
  AssertWithinEpsilon(epsilon,
                      1.66667,
                      ComputeAverageRowsetHeight({ { "A", "C" },
                                                   { "B", "D" },
                                                   { "C", "E" } }));

  /* Three rowsets that partially overlap.
   * [ --- ]
   *    [ --- ]
   * [ --- ]
   */
  AssertWithinEpsilon(epsilon,
                      2.33333,
                      ComputeAverageRowsetHeight({ { "A", "C" },
                                                   { "B", "D" },
                                                   { "A", "C" } }));

  /* Five rowsets that partially overlap.
   * [ --- ][ --- ]
   *    [ --- ]
   * [ --- ][ --- ]
   */
  AssertWithinEpsilon(epsilon,
                      2.6,
                      ComputeAverageRowsetHeight({ { "A", "C" }, { "C", "E" },
                                                   { "B", "D" },
                                                   { "A", "C" }, { "C", "E" } }));

  /* A messy collection of rowsets overlapping in all kinds of ways.
   * [ --- ][ --- ]       [ --- ]  [ --- ]
   *    [ --- ]                 [ --- ]
   * [ --- ]  |           [ --- ]     |
   */
  AssertWithinEpsilon(
      epsilon,
      2.3125,
      ComputeAverageRowsetHeight(
          { { "A", "C" }, { "C", "E" }, { "F", "G" }, { "H", "J" },
            { "B", "D" }, { "F", "I" },
            { "A", "C" }, { "D", "D" }, { "F", "G" }, { "I", "I" } }));
}

// A regression test for KUDU-2704. If rowsets are larger than the target size
// but there is fruitful height-based compaction that is within budget, it
// might not occur because the size-based portion of the rowset's value is
// negative.
TEST_F(TestCompactionPolicy, TestKUDU2704) {
  CompactionSelection picked;
  double quality = 0.0;

  constexpr auto kNumRowSets = 100;
  const auto big_rowset_size = FLAGS_budgeted_compaction_target_rowset_size * 2;
  const auto budget_mb = 2 * big_rowset_size / 1024 / 1024;
  RowSetVector rowsets;
  for (auto i = 0; i < kNumRowSets; i++) {
    rowsets.emplace_back(new MockDiskRowSet(
        StringPrintf("%010d", i * 2),
        StringPrintf("%010d", i * 2 + 1),
        big_rowset_size));
  }
  // Add one rowset overlapping the first rowset, so that it decreases average
  // rowset height to compact this one and the first one, but the average rowset
  // height doesn't decrease by much.
  rowsets.emplace_back(new MockDiskRowSet("0000000000",
                                          "0000000001",
                                          big_rowset_size));
  NO_FATALS(RunTestCase(rowsets, budget_mb, &picked, &quality));

  // We should still pick two rowsets because we do not allow the above-target
  // size of the rowsets to hurt compaction based on average height reduction.
  ASSERT_EQ(2, picked.size());
  ASSERT_GT(quality, 0.0);
}
} // namespace tablet
} // namespace kudu
