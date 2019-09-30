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

#include <cstdint>
#include <utility>
#include <vector>

#include "kudu/util/net/sockaddr.h"

namespace kudu {
namespace clock {
namespace internal {

// Representation of time interval: the first pair component is the
// left/earlier point, the second is right/later point.
typedef std::pair<int64_t, int64_t> Interval;

// A special interval meaning 'an empty interval'.
static const Interval kIntervalNone = { -1, -1 };

// A time measurement recorded after receiving a response from an NTP server.
struct RecordedResponse {
  // The time at which the response was recorded.
  int64_t monotime;
  // The calculated estimated offset between our monotime and the server's wall-clock time.
  int64_t offset_us;
  // The estimated maximum error between our time and the server's time.
  int64_t error_us;
  // The server which provided the response.
  Sockaddr addr;
  // The server's transmit timestamp.
  uint64_t tx_timestamp;
};

// Build an intersection interval given the set of correctness intervals
// and reference local time.
Interval FindIntersection(
    const std::vector<RecordedResponse>& responses,
    int64_t reftime,
    int clock_skew_ppm);

} // namespace internal
} // namespace clock
} // namespace kudu
