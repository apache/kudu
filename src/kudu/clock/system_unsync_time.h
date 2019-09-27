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

#include "kudu/clock/time_service.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {
namespace clock {

// TimeService implementation which uses the system clock without requiring
// that NTP is running or synchronized.
//
// This is useful on OSX which doesn't support the required adjtimex() syscall
// to query the NTP synchronization status.
class SystemUnsyncTime : public TimeService {
 public:
  SystemUnsyncTime() = default;

  Status Init() override;

  Status WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) override;

  int64_t skew_ppm() const override {
    return 500; // Reasonable default value.
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(SystemUnsyncTime);
};

} // namespace clock
} // namespace kudu
