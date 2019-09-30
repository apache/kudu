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

#include "kudu/clock/builtin_ntp-internal.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/net/sockaddr.h"

using std::vector;
using std::pair;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace clock {
namespace internal {

Interval FindIntersection(
    const vector<RecordedResponse>& responses,
    int64_t reftime,
    int clock_skew_ppm) {
  vector<pair<int64_t, int>> interval_endpoints;
  for (const auto& r : responses) {
    int64_t wall = reftime + r.offset_us;
    int64_t error = r.error_us + (reftime - r.monotime) * clock_skew_ppm / 1e6;
    DCHECK_GE(reftime, r.monotime);
    DCHECK_GE(error, 0);
    interval_endpoints.emplace_back(wall - error, -1);
    interval_endpoints.emplace_back(wall + error, 1);
    VLOG(2) << Substitute("correctness interval ($0, $1) from $2",
                          wall - error, wall + error, r.addr.ToString());
  }

  if (responses.size() == 1) {
    // Short-circuiting the search since the algorithm below doesn't handle
    // single interval.
    CHECK_EQ(2, interval_endpoints.size());
    return std::make_pair(interval_endpoints[0].first,
                          interval_endpoints[1].first);
  }

  std::sort(interval_endpoints.begin(), interval_endpoints.end());

  int best = 1; // for an intersection, at least 2 intervals are needed
  int count_overlap = 0;
  Interval best_interval = kIntervalNone;
  for (int i = 1; i < interval_endpoints.size(); i++) {
    const auto& cur = interval_endpoints[i - 1];
    const auto& next = interval_endpoints[i];
    count_overlap -= cur.second;
    // TODO(aserbin): in the layouts like the following, which interval is
    //                better to choose? Right now, the first is chosen,
    //                but maybe it's better to randomize the choice to avoid
    //                bias or simply choose some wider interval which covers
    //                both intersections intervals?
    //
    // source A     :   <---->
    // source B     :           <--->
    // source C     :  <-------------->
    // intersection :   <====>  <===>
    if (count_overlap > best) {
      best = count_overlap;
      best_interval = std::make_pair(cur.first, next.first);
    }
  }
  return best_interval;
}

} // namespace internal
} // namespace clock
} // namespace kudu
