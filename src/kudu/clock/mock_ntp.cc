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

#include "kudu/clock/mock_ntp.h"

#include <mutex>
#include <ostream>

#include <glog/logging.h>

#include "kudu/util/status.h"

namespace kudu {
namespace clock {

Status MockNtp::WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) {
  std::lock_guard<simple_spinlock> lock(lock_);
  VLOG(1) << "Current clock time: " << mock_clock_time_usec_
          << " error: " << mock_clock_max_error_usec_;
  *now_usec = mock_clock_time_usec_;
  *error_usec = mock_clock_max_error_usec_;
  return Status::OK();
}

void MockNtp::SetMockClockWallTimeForTests(uint64_t now_usec) {
  std::lock_guard<simple_spinlock> lock(lock_);
  VLOG(1) << "Updating to time: " << now_usec;
  CHECK_GE(now_usec, mock_clock_time_usec_);
  mock_clock_time_usec_ = now_usec;
}

void MockNtp::SetMockMaxClockErrorForTests(uint64_t max_error_usec) {
  std::lock_guard<simple_spinlock> lock(lock_);
  VLOG(1) << "Updating to error: " << max_error_usec;
  mock_clock_max_error_usec_ = max_error_usec;
}

} // namespace clock
} // namespace kudu
