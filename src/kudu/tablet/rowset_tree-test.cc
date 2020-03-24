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

#include "kudu/tablet/rowset_tree.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/mock-rowsets.h"
#include "kudu/tablet/rowset.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::shared_ptr;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

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

  // interval [3,4) overlaps 0-5, 3-5 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(Slice("3"), Slice("4"), &out);
  ASSERT_EQ(3, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);

  // interval [0,2) overlaps 0-5 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(Slice("0"), Slice("2"), &out);
  ASSERT_EQ(2, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);

  // interval [5,7) overlaps 0-5, 3-5, 5-9 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(Slice("5"), Slice("7"), &out);
  ASSERT_EQ(4, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);
  ASSERT_EQ(vec[2].get(), out[3]);

  // "3" overlaps 0-5, 3-5, and the MemRowSet.
  out.clear();
  tree.FindRowSetsWithKeyInRange("3", &out);
  ASSERT_EQ(3, out.size());
  ASSERT_EQ(vec[3].get(), out[0]); // MemRowSet
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);

  // "5" overlaps 0-5, 3-5, 5-9, and the MemRowSet
  out.clear();
  tree.FindRowSetsWithKeyInRange("5", &out);
  ASSERT_EQ(4, out.size());
  ASSERT_EQ(vec[3].get(), out[0]); // MemRowSet
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);
  ASSERT_EQ(vec[2].get(), out[3]);

  // interval [0,5) overlaps 0-5, 3-5, and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(Slice("0"), Slice("5"), &out);
  ASSERT_EQ(3, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);

  // interval [3,5) overlaps 0-5, 3-5 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(Slice("3"), Slice("5"), &out);
  ASSERT_EQ(3, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);

  // interval [-OO,3) overlaps 0-5 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(boost::none, Slice("3"), &out);
  ASSERT_EQ(2, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);

  // interval [-OO,5) overlaps 0-5, 3-5 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(boost::none, Slice("5"), &out);
  ASSERT_EQ(3, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);

  // interval [-OO,99) overlaps 0-5, 3-5, 5-9 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(boost::none, Slice("99"), &out);
  ASSERT_EQ(4, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);
  ASSERT_EQ(vec[2].get(), out[3]);

  // interval [6,+OO) overlaps 5-9 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(Slice("6"), boost::none, &out);
  ASSERT_EQ(2, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[2].get(), out[1]);

  // interval [5,+OO) overlaps 0-5, 3-5, 5-9 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(Slice("5"), boost::none, &out);
  ASSERT_EQ(4, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);
  ASSERT_EQ(vec[2].get(), out[3]);

  // interval [4,+OO) overlaps 0-5, 3-5, 5-9 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(Slice("4"), boost::none, &out);
  ASSERT_EQ(4, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);
  ASSERT_EQ(vec[2].get(), out[3]);

  // interval [-OO,+OO) overlaps 0-5, 3-5, 5-9 and the MemRowSet
  out.clear();
  tree.FindRowSetsIntersectingInterval(boost::none, boost::none, &out);
  ASSERT_EQ(4, out.size());
  ASSERT_EQ(vec[3].get(), out[0]);
  ASSERT_EQ(vec[0].get(), out[1]);
  ASSERT_EQ(vec[1].get(), out[2]);
  ASSERT_EQ(vec[2].get(), out[3]);
}

TEST_F(TestRowSetTree, TestTreeRandomized) {
  enum BoundOperator {
    BOUND_LESS_THAN,
    BOUND_LESS_EQUAL,
    BOUND_GREATER_THAN,
    BOUND_GREATER_EQUAL,
    BOUND_EQUAL
  };
  const auto& GetStringPair = [] (const BoundOperator op) -> std::pair<string, string> {
    while (true) {
      string s1 = Substitute("$0", rand() % 100);
      string s2 = Substitute("$0", rand() % 100);
      int r = strcmp(s1.c_str(), s2.c_str());
      switch (op) {
      case BOUND_LESS_THAN:
        if (r == 0) continue; // pass through.
      case BOUND_LESS_EQUAL:
        return std::pair<string, string>(std::min(s1, s2), std::max(s1, s2));
      case BOUND_GREATER_THAN:
        if (r == 0) continue; // pass through.
      case BOUND_GREATER_EQUAL:
        return std::pair<string, string>(std::max(s1, s2), std::min(s1, s2));
      case BOUND_EQUAL:
        return std::pair<string, string>(s1, s1);
      }
    }
  };

  SeedRandom();
  RowSetVector vec;
  for (int i = 0; i < 100; i++) {
    std::pair<string, string> bound = GetStringPair(BOUND_LESS_EQUAL);
    ASSERT_LE(bound.first, bound.second);
    vec.push_back(shared_ptr<RowSet>(
        new MockDiskRowSet(bound.first, bound.second)));
  }
  RowSetTree tree;
  ASSERT_OK(tree.Reset(vec));

  // When lower < upper.
  vector<RowSet*> out;
  for (int i = 0; i < 100; i++) {
    out.clear();
    std::pair<string, string> bound = GetStringPair(BOUND_LESS_THAN);
    ASSERT_LT(bound.first, bound.second);
    tree.FindRowSetsIntersectingInterval(
        Slice(bound.first), Slice(bound.second), &out);
    for (const auto& e : out) {
      string min, max;
      e->GetBounds(&min, &max);
      if (min < bound.first) {
        ASSERT_GE(max, bound.first);
      } else {
        ASSERT_LT(min, bound.second);
      }
      if (max >= bound.second) {
        ASSERT_LT(min, bound.second);
      } else {
        ASSERT_GE(max, bound.first);
      }
    }
  }
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
                            batch_total ? (oat_total / batch_total) : 0);
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
