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
#ifndef KUDU_UTIL_THROTTLER_H
#define KUDU_UTIL_THROTTLER_H

#include <cstdint>

#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"

namespace kudu {

// A throttler to throttle both operation/s and IO byte/s.
class Throttler {
 public:
  // Refill period is 100ms.
  enum {
    kRefillPeriodMicros = 100000
  };

  // Construct a throttler with max operation per second, max IO bytes per second
  // and burst factor (burst_rate = rate * burst), burst rate means maximum
  // throughput within one refill period.
  // Set op_per_sec to 0 to disable operation throttling.
  // Set byte_per_sec to 0 to disable IO bytes throttling.
  Throttler(MonoTime now, uint64_t op_per_sec, uint64_t byte_per_sec, double burst_factor);

  // Throttle an "operation group" by taking 'op' operation tokens and 'byte' byte tokens.
  // Return true if there are enough tokens, and operation is allowed.
  // Return false if there are not enough tokens, and operation is throttled.
  bool Take(MonoTime now, uint64_t op, uint64_t byte);

 private:
  void Refill(MonoTime now);

  MonoTime next_refill_;
  uint64_t op_refill_;
  uint64_t op_token_;
  uint64_t op_token_max_;
  uint64_t byte_refill_;
  uint64_t byte_token_;
  uint64_t byte_token_max_;
  simple_spinlock lock_;
};

} // namespace kudu

#endif
