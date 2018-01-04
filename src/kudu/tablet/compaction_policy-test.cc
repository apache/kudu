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

    ASSERT_OK(policy.PickRowSets(tree, picked, quality, nullptr));
  }
};


// Simple test for budgeted compaction: with three rowsets which
// mostly overlap, and an high budget, they should all be selected.
TEST_F(TestCompactionPolicy, TestBudgetedSelection) {
  RowSetVector vec = {
    std::make_shared<MockDiskRowSet>("C", "c"),
    std::make_shared<MockDiskRowSet>("B", "a"),
    std::make_shared<MockDiskRowSet>("A", "b")
  };

  const int kBudgetMb = 1000; // enough to select all
  unordered_set<RowSet*> picked;
  double quality = 0;
  NO_FATALS(RunTestCase(vec, kBudgetMb, &picked, &quality));
  ASSERT_EQ(3, picked.size());
  ASSERT_GE(quality, 1.0);
}

// Test for the case when we have many rowsets, but none of them
// overlap at all. This is likely to occur in workloads where the
// primary key is always increasing (such as a timestamp).
TEST_F(TestCompactionPolicy, TestNonOverlappingRowsets) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipped";
    return;
  }
  RowSetVector vec;
  for (int i = 0; i < 10000;  i++) {
    vec.emplace_back(new MockDiskRowSet(
        StringPrintf("%010d", i * 2),
        StringPrintf("%010d", i * 2 + 1)));
  }
  unordered_set<RowSet*> picked;
  double quality = 0;
  NO_FATALS(RunTestCase(vec, /*size_budget_mb=*/128, &picked, &quality));
  ASSERT_EQ(quality, 0);
  ASSERT_TRUE(picked.empty());
}

// Similar to the above, but with some small overlap between adjacent
// rowsets.
TEST_F(TestCompactionPolicy, TestTinyOverlapRowsets) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipped";
    return;
  }

  RowSetVector vec;
  for (int i = 0; i < 10000;  i++) {
    vec.emplace_back(new MockDiskRowSet(
        StringPrintf("%010d", i * 10000),
        StringPrintf("%010d", i * 10000 + 11000)));
  }
  unordered_set<RowSet*> picked;
  double quality = 0;
  NO_FATALS(RunTestCase(vec, /*size_budget_mb=*/128, &picked, &quality));
  ASSERT_EQ(quality, 0);
  ASSERT_TRUE(picked.empty());
}

// Test case with 100 rowsets, each of which overlaps with its two
// neighbors to the right.
TEST_F(TestCompactionPolicy, TestSignificantOverlap) {
  RowSetVector vec;
  for (int i = 0; i < 100;  i++) {
    vec.emplace_back(new MockDiskRowSet(
        StringPrintf("%010d", i * 10000),
        StringPrintf("%010d", (i + 2) * 10000)));
  }
  unordered_set<RowSet*> picked;
  double quality = 0;
  const int kBudgetMb = 64;
  NO_FATALS(RunTestCase(vec, kBudgetMb, &picked, &quality));
  ASSERT_GT(quality, 0.5);
  // Each rowset is 1MB so we should exactly fill the budget.
  ASSERT_EQ(picked.size(), kBudgetMb);
}

// Return the directory of the currently-running executable.
static string GetExecutableDir() {
  string exec;
  CHECK_OK(Env::Default()->GetExecutablePath(&exec));
  return DirName(exec);
}

static RowSetVector LoadFile(const string& name) {
  RowSetVector ret;
  string path = JoinPathSegments(GetExecutableDir(), name);
  faststring data;
  CHECK_OK_PREPEND(ReadFileToString(Env::Default(), path, &data),
                   strings::Substitute("Unable to load test data file $0", path));
  vector<string> lines = strings::Split(data.ToString(), "\n");
  for (const auto& line : lines) {
    if (line.empty() || line[0] == '#') continue;
    vector<string> fields = strings::Split(line, "\t");
    CHECK_EQ(3, fields.size()) << "Expected 3 fields on line: " << line;
    int size_mb = ParseLeadingInt32Value(fields[0], -1);
    CHECK_GE(size_mb, 1) << "Expected size at least 1MB on line: " << line;
    ret.emplace_back(new MockDiskRowSet(fields[1] /* min key */,
                                        fields[2] /* max key */,
                                        size_mb * 1024 * 1024));
  }

  return ret;
}

// Realistic test using data scraped from a tablet containing 200+GB of YCSB data.
// This test can be used as a benchmark for optimizing the compaction policy,
// and also serves as a basic regression/stress test using real data.
TEST_F(TestCompactionPolicy, TestYcsbCompaction) {
  RowSetVector vec = LoadFile("ycsb-test-rowsets.tsv");
  RowSetTree tree;
  ASSERT_OK(tree.Reset(vec));
  vector<double> qualities;
  for (int budget_mb : {128, 256, 512, 1024}) {
    BudgetedCompactionPolicy policy(budget_mb);

    unordered_set<RowSet*> picked;
    double quality = 0;
    LOG_TIMING(INFO, strings::Substitute("Computing compaction with $0MB budget", budget_mb)) {
      ASSERT_OK(policy.PickRowSets(tree, &picked, &quality, nullptr));
    }
    LOG(INFO) << "quality=" << quality;
    int total_size = 0;
    for (const auto* rs : picked) {
      total_size += rs->OnDiskBaseDataSizeWithRedos() / 1024 / 1024;
    }
    ASSERT_LE(total_size, budget_mb);
    qualities.push_back(quality);
  }

  // Given increasing budgets, our solutions should also be higher quality.
  ASSERT_TRUE(std::is_sorted(qualities.begin(), qualities.end()))
      << qualities;
}

// Regression test for KUDU-2251 which ensures that large (> 2GiB) rowsets don't
// cause integer overflow in compaction planning.
TEST_F(TestCompactionPolicy, KUDU2251) {
  RowSetVector vec = {
    std::make_shared<MockDiskRowSet>("C", "c", 1L << 31),
    std::make_shared<MockDiskRowSet>("B", "a", 1L << 32),
    std::make_shared<MockDiskRowSet>("A", "b", 1L << 33)
  };

  const int kBudgetMb = 1L << 30; // enough to select all
  unordered_set<RowSet*> picked;
  double quality = 0;
  NO_FATALS(RunTestCase(vec, kBudgetMb, &picked, &quality));
  ASSERT_EQ(3, picked.size());
  ASSERT_GE(quality, 1.0);
}

} // namespace tablet
} // namespace kudu
