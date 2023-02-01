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
#include <string>
#include <vector>

#include "kudu/clock/time_service.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu {
namespace clock {

// The SystemNtp is a TimeService implementation which uses the ntp_adjtime()
// call [1] (similar to ntp_gettime() call [2] from the NTP kernel API, both
// based on the adjtimex() syscall on Linux) to consult the Linux kernel
// for the current time and error bound.
//
// This implementation relies on the NTP service running on the local host
// (implemented, for example, by chronyd, ntpd, etc.) to keep the system clock
// synchronized with reference NTP time servers.
//
// [1] https://man7.org/linux/man-pages/man3/ntp_adjtime.3.html
// [2] https://man7.org/linux/man-pages/man3/ntp_gettime.3.html
class SystemNtp : public TimeService {
 public:
  explicit SystemNtp(const scoped_refptr<MetricEntity>& metric_entity);

  Status Init() override;

  Status WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) override;

  int64_t skew_ppm() const override {
    return skew_ppm_;
  }

  void DumpDiagnostics(std::vector<std::string>* log) const override;

 private:
  // Returns current timestamp, maxerror, and status as returned by the
  // invocation of ntp_gettime()/ntp_adjtime() NTP kernel API call.
  static std::string ClockNtpStatusForMetrics();

  // The maximum possible clock frequency skew rate reported by the kernel,
  // parts-per-million (PPM).
  int64_t skew_ppm_;

  // Metric entity. Used to fetch information on NTP-related metrics upon
  // calling DumpDiagnostics().
  scoped_refptr<MetricEntity> metric_entity_;

  // Metrics are set to detach to their last value. This means that, during
  // running the destructor, there might be a need to access other class members
  // declared above. Hence, this member must be declared last.
  FunctionGaugeDetacher metric_detacher_;

  DISALLOW_COPY_AND_ASSIGN(SystemNtp);
};

} // namespace clock
} // namespace kudu
