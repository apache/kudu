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

#include <gtest/gtest_prod.h>

#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"

namespace kudu {

// A throttler to throttle both operation/s and IO byte/s.
class Throttler final {
 public:
  // Refill period is 100ms.
  static constexpr const int64_t kRefillPeriodMicros = 100000;
  static constexpr const uint64_t kNoLimit = 0;

  // Construct a throttler with max operation per second, max IO bytes per second
  // and burst factor (burst_rate = rate * burst_factor), burst_rate means
  // the maximum throughput within one refill period.
  // Set op_rate_per_sec to kNoLimit to disable operation rate throttling.
  // Set byte_rate_per_sec to kNoLimit to disable IO rate throttling.
  Throttler(uint64_t op_rate_per_sec,
            uint64_t byte_rate_per_sec,
            double burst_factor);

  // Throttle an "operation group" by taking 'ops' operation tokens and 'bytes'
  // byte tokens.
  // Return true if there are enough tokens, and operation is allowed.
  // Return false if there are not enough tokens, and operation is throttled.
  bool Take(uint64_t ops, uint64_t bytes);

 private:
  FRIEND_TEST(ThrottlerTest, OpThrottle);
  FRIEND_TEST(ThrottlerTest, IOThrottle);
  FRIEND_TEST(ThrottlerTest, Burst);

  Throttler(MonoTime now,
            uint64_t op_rate_per_sec,
            uint64_t byte_rate_per_sec,
            double burst_factor);

  bool Take(MonoTime now, uint64_t ops, uint64_t bytes);

  void Refill(MonoTime now);

  const uint64_t byte_refill_;
  const uint64_t byte_token_max_;
  const uint64_t op_refill_;
  const uint64_t op_token_max_;

  uint64_t byte_token_;
  uint64_t op_token_;
  MonoTime next_refill_;
  simple_spinlock lock_;
};

} // namespace kudu
