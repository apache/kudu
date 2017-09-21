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

#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/sorted_disjoint_interval_list.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::vector;

namespace kudu {

class TestSortedDisjointIntervalList : public KuduTest {
};

typedef int PointType;
typedef std::pair<PointType, PointType> ClosedInterval;

TEST_F(TestSortedDisjointIntervalList, TestBasic) {
  // Coalesce an empty interval list.
  vector<ClosedInterval> intervals = {};
  ASSERT_OK(CoalesceIntervals<PointType>(&intervals));
  vector<ClosedInterval> expected = {};
  ASSERT_EQ(expected, intervals);

  // Coalesce an interval list with length 0 interval.
  intervals = {{26, 26}};
  ASSERT_OK(CoalesceIntervals<PointType>(&intervals));
  expected = {{26, 26}};
  ASSERT_EQ(expected, intervals);

  // Coalesce an interval list with a single interval.
  intervals = {{33, 69}};
  ASSERT_OK(CoalesceIntervals<PointType>(&intervals));
  expected = {{33, 69}};
  ASSERT_EQ(expected, intervals);

  // Coalesce an interval list with adjacent ranges.
  intervals = {{4, 7}, {3, 4}, {1, 2}, {-23, 1}};
  ASSERT_OK(CoalesceIntervals<PointType>(&intervals));
  expected = {{-23, 2}, {3, 7}};
  ASSERT_EQ(expected, intervals);
}

TEST_F(TestSortedDisjointIntervalList, TestOverlappedIntervals) {
  vector<ClosedInterval> intervals = {{4, 7}, {3, 9}};
  ASSERT_OK(CoalesceIntervals<PointType>(&intervals));
  vector<ClosedInterval> expected = {{3, 9}};
  ASSERT_EQ(expected, intervals);

  intervals = {{4, 7}, {3, 9}, {-23, 1},
               {4, 350}, {369, 400}};
  ASSERT_OK(CoalesceIntervals<PointType>(&intervals));
  expected = {{-23, 1}, {3, 350}, {369, 400}};
  ASSERT_EQ(expected, intervals);
}

TEST_F(TestSortedDisjointIntervalList, TestDuplicateIntervals) {
  vector<ClosedInterval> intervals = {{1, 2}, {4, 7},
                                      {1, 2}, {1, 2}};
  ASSERT_OK(CoalesceIntervals<PointType>(&intervals));
  const vector<ClosedInterval> expected = {{1, 2}, {4, 7}};
  ASSERT_EQ(expected, intervals);
}

TEST_F(TestSortedDisjointIntervalList, TestInvalidIntervals) {
  vector<ClosedInterval> intervals = {{1, 2}, {10, 2},
                                      {4, 7}, {40, 7}};
  ASSERT_TRUE(CoalesceIntervals<PointType>(&intervals).IsInvalidArgument());
}

TEST_F(TestSortedDisjointIntervalList, TestSingleElementIntervals) {
  vector<ClosedInterval> intervals = {{0, 0}, {0, 1}, {1, 2}};
  ASSERT_OK(CoalesceIntervals<PointType>(&intervals));
  const vector<ClosedInterval> expected = {{0, 2}};
  ASSERT_EQ(expected, intervals);
}

} // namespace kudu
