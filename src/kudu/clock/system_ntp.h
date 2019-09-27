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

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "kudu/clock/time_service.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {
namespace clock {

// TimeService implementation which uses the 'ntp_adjtime' call (corresponding to the
// 'adjtimex' syscall) to consult the Linux kernel for the current time
// and error bound.
//
// This implementation relies on the ntpd service running on the local host
// to keep the kernel's timekeeping up to date and in sync.
class SystemNtp : public TimeService {
 public:
  SystemNtp();

  Status Init() override {
    return Status::OK();
  }

  Status WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) override;

  int64_t skew_ppm() const override {
    return skew_ppm_;
  }

  void DumpDiagnostics(std::vector<std::string>* log) const override;

 private:
  // The scaling factor used to obtain ppms. From the adjtimex source:
  // "scale factor used by adjtimex freq param.  1 ppm = 65536"
  static const double kAdjtimexScalingFactor;

  static const uint64_t kMicrosPerSec;

  // The skew rate in PPM reported by the kernel.
  std::atomic<int64_t> skew_ppm_;

  DISALLOW_COPY_AND_ASSIGN(SystemNtp);
};

} // namespace clock
} // namespace kudu
