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

using std::shared_ptr;
using std::unordered_set;
using std::string;
using std::vector;

namespace kudu {
namespace tablet {

// Simple test for budgeted compaction: with three rowsets which
// mostly overlap, and an high budget, they should all be selected.
TEST(TestCompactionPolicy, TestBudgetedSelection) {
  RowSetVector vec;
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet("C", "c")));
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet("B", "a")));
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet("A", "b")));

  RowSetTree tree;
  ASSERT_OK(tree.Reset(vec));

  const int kBudgetMb = 1000; // enough to select all
  BudgetedCompactionPolicy policy(kBudgetMb);

  unordered_set<RowSet*> picked;
  double quality = 0;
  ASSERT_OK(policy.PickRowSets(tree, &picked, &quality, nullptr));
  ASSERT_EQ(3, picked.size());
  ASSERT_GE(quality, 1.0);
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
TEST(TestCompactionPolicy, TestYcsbCompaction) {
  RowSetVector vec = LoadFile("ycsb-test-rowsets.tsv");;
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
      total_size += rs->OnDiskDataSizeNoUndos() / 1024 / 1024;
    }
    ASSERT_LE(total_size, budget_mb);
    qualities.push_back(quality);
  }

  // Given increasing budgets, our solutions should also be higher quality.
  ASSERT_TRUE(std::is_sorted(qualities.begin(), qualities.end()))
      << qualities;
}

} // namespace tablet
} // namespace kudu
