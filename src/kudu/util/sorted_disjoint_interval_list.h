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

#pragma once

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {

// Constructs a sorted disjoint interval list in-place given a list of intervals.
// The result is written back to the given 'intervals'.
//
// Returns an error if the list contains any invalid intervals.
//
// Sorted disjoint interval list is a data structure holding a group of sorted
// non-overlapping ranges. The operation to construct such one is O(nlg n + n)
// where 'n' is the number of intervals.
//
// For example, given the input interval list:
//
//   [------2-------)         [-----1-----)
//       [--3--)    [---5--)    [----4----)
//
// The output sorted disjoint interval list:
//
//   [----------1----------)  [-----2-----)
//
//
// This method assumes that all intervals are "half-open" intervals -- the
// intervals are inclusive of their start point and exclusive of end point,
// e.g., [3, 6). Note that interval with the same start and end point is
// considered to be valid in this implementation.
// It also assumes 'PointType' has a proper defined comparator.
template<typename PointType>
Status CoalesceIntervals(std::vector<std::pair<PointType, PointType>>* intervals) {
  if (intervals->empty()) return Status::OK();

  // Sort the intervals to prepare for coalescing overlapped ranges.
  for (const auto& interval : *intervals) {
    if (interval.first > interval.second) {
      return Status::InvalidArgument(strings::Substitute("invalid interval: [$0, $1)",
                                                         interval.first,
                                                         interval.second));
    }
  }
  std::sort(intervals->begin(), intervals->end());

  // Traverse the intervals to coalesce overlapped intervals. During the process,
  // uses 'head', 'tail' to track the start and end point of the current disjoint
  // interval.
  auto head = intervals->begin();
  auto tail = head;
  while (++tail != intervals->end()) {
    // If interval 'head' and 'tail' overlap with each other, coalesce them and move
    // to next. Otherwise, the two intervals are disjoint.
    if (head->second >= tail->first) {
      if (tail->second > head->second) head->second = std::move(tail->second);
    } else {
      // The two intervals are disjoint. If the 'head' previously already coalesced
      // some intervals, 'head' and 'tail' will not be adjacent. If so, move 'tail'
      // to the next 'head' to make sure we do not include any of the previously-coalesced
      // intervals.
      ++head;
      if (head != tail) *head = std::move(*tail);
    }
  }

  // Truncate the rest useless elements, if any.
  intervals->erase(++head, tail);
  return Status::OK();
}

} // namespace kudu
