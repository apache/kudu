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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <memory>
#include <stdio.h>
#include <unordered_set>

#include "kudu/gutil/map-util.h"
#include "kudu/tablet/mock-rowsets.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_tree.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::unordered_set;

namespace kudu { namespace tablet {

class TestRowSetTree : public KuduTest {
};

namespace {

// Generates random rowsets with keys between 0 and 10000
static RowSetVector GenerateRandomRowSets(int num_sets) {
  RowSetVector vec;
  for (int i = 0; i < num_sets; i++) {
    int min = rand() % 9000;
    int max = min + 1000;

    vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet(StringPrintf("%04d", min),
                                                        StringPrintf("%04d", max))));
  }
  return vec;
}

} // anonymous namespace

TEST_F(TestRowSetTree, TestTree) {
  RowSetVector vec;
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet("0", "5")));
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet("3", "5")));
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet("5", "9")));
  vec.push_back(shared_ptr<RowSet>(new MockMemRowSet()));

  RowSetTree tree;
  ASSERT_OK(tree.Reset(vec));

  // "2" overlaps 0-5 and the MemRowSet.
  vector<RowSet *> out;
  tree.FindRowSetsWithKeyInRange("2", &out);
  ASSERT_EQ(2, out.size());
  ASSERT_EQ(vec[3].get(), out[0]); // MemRowSet
  ASSERT_EQ(vec[0].get(), out[1]);

  // "4" overlaps 0-5, 3-5, and the MemRowSet
  out.clear();
  tree.FindRowSetsWithKeyInRange("4", &out);
  ASSERT_EQ(3, out.size());
  ASSERT_EQ(vec[3].get(), out[0]); // MemRowSet
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);

  // interval (2,4) overlaps 0-5, 3-5 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval("3", "4", &out);
  ASSERT_EQ(3, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);

  // interval (0,2) overlaps 0-5 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval("0", "2", &out);
  ASSERT_EQ(2, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);

  // interval (5,7) overlaps 0-5, 3-5, 5-9 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval("5", "7", &out);
  ASSERT_EQ(4, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);
  ASSERT_EQ(vec[2].get(), out[3]);
}

class TestRowSetTreePerformance : public TestRowSetTree,
                                  public testing::WithParamInterface<std::tuple<int, int>> {
};
INSTANTIATE_TEST_CASE_P(
    Parameters, TestRowSetTreePerformance,
    testing::Combine(
        // Number of rowsets.
        // Up to 500 rowsets (500*32MB = 16GB tablet)
        testing::Values(10, 100, 250, 500),
        // Number of query points in a batch.
        testing::Values(10, 100, 500, 1000, 5000)));

TEST_P(TestRowSetTreePerformance, TestPerformance) {
  const int kNumRowSets = std::get<0>(GetParam());
  const int kNumQueries = std::get<1>(GetParam());
  const int kNumIterations = AllowSlowTests() ? 1000 : 10;
  SeedRandom();

  Stopwatch one_at_time_timer;
  Stopwatch batch_timer;
  for (int i = 0; i < kNumIterations; i++) {
    // Create a bunch of rowsets, each of which spans about 10% of the "row space".
    // The row space here is 4-digit 0-padded numbers.
    RowSetVector vec = GenerateRandomRowSets(kNumRowSets);

    RowSetTree tree;
    ASSERT_OK(tree.Reset(vec));

    vector<string> queries;
    for (int i = 0; i < kNumQueries; i++) {
      int query = rand() % 10000;
      queries.emplace_back(StringPrintf("%04d", query));
    }

    int individual_matches = 0;
    one_at_time_timer.resume();
    {
      vector<RowSet *> out;
      for (const auto& q : queries) {
        out.clear();
        tree.FindRowSetsWithKeyInRange(Slice(q), &out);
        individual_matches += out.size();
      }
    }
    one_at_time_timer.stop();

    vector<Slice> query_slices;
    for (const auto& q : queries) {
      query_slices.emplace_back(q);
    }

    batch_timer.resume();
    std::sort(query_slices.begin(), query_slices.end(), Slice::Comparator());
    int bulk_matches = 0;
    {
      vector<RowSet *> out;
      tree.ForEachRowSetContainingKeys(
          query_slices, [&](RowSet* rs, int slice_idx) {
            bulk_matches++;
          });
    }
    batch_timer.stop();

    ASSERT_EQ(bulk_matches, individual_matches);
  }

  double batch_total = batch_timer.elapsed().user;
  double oat_total = one_at_time_timer.elapsed().user;
  const string& case_desc = StringPrintf("Q=% 5d R=% 5d", kNumQueries, kNumRowSets);
  LOG(INFO) << StringPrintf("%s %10s %d ms",
                            case_desc.c_str(),
                            "1-by-1",
                            static_cast<int>(oat_total/1e6));
  LOG(INFO) << StringPrintf("%s %10s %d ms (%.2fx)",
                            case_desc.c_str(),
                            "batched",
                            static_cast<int>(batch_total/1e6),
                            oat_total / batch_total);
}

TEST_F(TestRowSetTree, TestEndpointsConsistency) {
  const int kNumRowSets = 1000;
  RowSetVector vec = GenerateRandomRowSets(kNumRowSets);
  // Add pathological one-key rows
  for (int i = 0; i < 10; ++i) {
    vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet(StringPrintf("%04d", 11000),
                                                        StringPrintf("%04d", 11000))));
  }
  vec.push_back(shared_ptr<RowSet>(new MockDiskRowSet(StringPrintf("%04d", 12000),
                                                      StringPrintf("%04d", 12000))));
  // Make tree
  RowSetTree tree;
  ASSERT_OK(tree.Reset(vec));
  // Keep track of "currently open" intervals defined by the endpoints
  unordered_set<RowSet*> open;
  // Keep track of all rowsets that have been visited
  unordered_set<RowSet*> visited;

  Slice prev;
  for (const RowSetTree::RSEndpoint& rse : tree.key_endpoints()) {
    RowSet* rs = rse.rowset_;
    enum RowSetTree::EndpointType ept = rse.endpoint_;
    const Slice& slice = rse.slice_;

    ASSERT_TRUE(rs != nullptr) << "RowSetTree has an endpoint with no rowset";
    ASSERT_TRUE(!slice.empty()) << "RowSetTree has an endpoint with no key";

    if (!prev.empty()) {
      ASSERT_LE(prev.compare(slice), 0);
    }

    string min, max;
    ASSERT_OK(rs->GetBounds(&min, &max));
    if (ept == RowSetTree::START) {
      ASSERT_EQ(min, slice.ToString());
      ASSERT_TRUE(InsertIfNotPresent(&open, rs));
      ASSERT_TRUE(InsertIfNotPresent(&visited, rs));
    } else if (ept == RowSetTree::STOP) {
      ASSERT_EQ(max, slice.ToString());
      ASSERT_TRUE(open.erase(rs) == 1);
    } else {
      FAIL() << "No such endpoint type exists";
    }
  }
}

} // namespace tablet
} // namespace kudu
