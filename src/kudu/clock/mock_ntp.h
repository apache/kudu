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

#include "kudu/clock/time_service.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {
namespace clock {

// Mock implementation of TimeService.
//
// This is used for various tests which rely on manually advancing time.
class MockNtp : public TimeService {
 public:
  MockNtp() = default;
  virtual ~MockNtp() = default;

  virtual Status Init() override {
    return Status::OK();
  }

  virtual Status WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) override;

  virtual int64_t skew_ppm() const override {
    // Just return the same constant as the default configuration for NTP:
    // the local clock frequency may accumulate error at a max rate of
    // 500us per second.
    return 500;
  }

  // Sets the time to be returned by a mock call to the system clock, for tests.
  // Requires that 'now_usec' is higher than the previously set time.
  // NOTE: This refers to the time returned by the system clock, not the time returned
  // by HybridClock, i.e. 'now_usec' is not a HybridTime timestamp and shouldn't have
  // a logical component.
  void SetMockClockWallTimeForTests(uint64_t now_usec);

  // Sets the max. error to be returned by a mock call to the system clock, for tests.
  // This can be used to make HybridClock report the wall clock as unsynchronized, by
  // setting error to be more than the configured tolerance.
  void SetMockMaxClockErrorForTests(uint64_t max_error_usec);

 private:
  simple_spinlock lock_;

  uint64_t mock_clock_time_usec_ = 0;
  uint64_t mock_clock_max_error_usec_ = 0;

  DISALLOW_COPY_AND_ASSIGN(MockNtp);
};

} // namespace clock
} // namespace kudu
