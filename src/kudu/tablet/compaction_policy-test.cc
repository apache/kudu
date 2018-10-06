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
#include <memory>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/compaction_policy.h"
#include "kudu/tablet/mock-rowsets.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_tree.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::unordered_set;
using std::string;
using std::vector;

namespace kudu {
namespace tablet {

class TestCompactionPolicy : public KuduTest {
 protected:
  void RunTestCase(const RowSetVector& vec,
                   int size_budget_mb,
                   unordered_set<RowSet*>* picked,
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
  unordered_set<RowSet*> picked;
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
  unordered_set<RowSet*> picked;
  double quality = 0.0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(quality, 0.0);
  ASSERT_TRUE(picked.empty());
}

// Similar to the above, but with some small overlap between adjacent
// rowsets.
TEST_F(TestCompactionPolicy, TestTinyOverlapRowSets) {
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
  unordered_set<RowSet*> picked;
  double quality = 0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  // With such small overlaps, no compaction will be considered worthwhile.
  ASSERT_EQ(quality, 0.0);
  ASSERT_TRUE(picked.empty());
}

// Test case with 100 rowsets, each of which overlaps with its two
// neighbors to the right.
TEST_F(TestCompactionPolicy, TestSignificantOverlap) {
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
  unordered_set<RowSet*> picked;
  double quality = 0.0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  // Each rowset is 1MB so the number of rowsets picked should be the number of
  // MB in the budget.
  ASSERT_EQ(picked.size(), kBudgetMb);

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
// compacting {A, B, C} results in the same quality score as {A, B}, 0.67, but
// uses more budget. By penalizing the wider solution {A, B, C}, we ensure we
// don't waste I/O.
TEST_F(TestCompactionPolicy, TestSupportAdjust) {
  const RowSetVector rowsets = {
    std::make_shared<MockDiskRowSet>("A", "B"),
    std::make_shared<MockDiskRowSet>("A", "B"),
    std::make_shared<MockDiskRowSet>("B", "C"),
  };
  constexpr auto kBudgetMb = 1000; // Enough to select all rowsets.
  unordered_set<RowSet*> picked;
  double quality = 0.0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(2, picked.size());
  // The weights of A and B are both 0.67, so with the adjustment the quality
  // score should be a little less than 0.67.
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
  const RowSetVector rowsets = LoadFile("ycsb-test-rowsets.tsv");
  RowSetTree tree;
  ASSERT_OK(tree.Reset(rowsets));
  vector<double> qualities;
  for (int budget_mb : {128, 256, 512, 1024}) {
    BudgetedCompactionPolicy policy(budget_mb);

    unordered_set<RowSet*> picked;
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
  // Same arrangement as in TestBudgetedSelection.
  const RowSetVector rowsets = {
    std::make_shared<MockDiskRowSet>("C", "c", 1L << 31),
    std::make_shared<MockDiskRowSet>("B", "a", 1L << 32),
    std::make_shared<MockDiskRowSet>("A", "b", 1L << 33)
  };

  constexpr auto kBudgetMb = 1L << 30; // Enough to select all rowsets.
  unordered_set<RowSet*> picked;
  double quality = 0.0;
  NO_FATALS(RunTestCase(rowsets, kBudgetMb, &picked, &quality));
  ASSERT_EQ(3, picked.size());
  ASSERT_GT(quality, 1.0);
  ASSERT_LT(quality, 2.0);
}

} // namespace tablet
} // namespace kudu
