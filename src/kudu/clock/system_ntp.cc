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

#include "kudu/clock/system_ntp.h"

#include <sys/timex.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/errno.h"
#include "kudu/util/logging.h"
#include "kudu/util/status.h"

namespace kudu {
namespace clock {

using strings::Substitute;

const double SystemNtp::kAdjtimexScalingFactor = 65536;
const uint64_t SystemNtp::kNanosPerSec = 1000000;

namespace {

// Returns the clock modes and checks if the clock is synchronized.
Status GetClockModes(timex* timex) {
  // this makes ntp_adjtime a read-only call
  timex->modes = 0;
  int rc = ntp_adjtime(timex);
  if (PREDICT_FALSE(rc == TIME_ERROR)) {
    return Status::ServiceUnavailable(
        Substitute("Error reading clock. Clock considered unsynchronized. Return code: $0", rc));
  }
  // TODO what to do about leap seconds? see KUDU-146
  if (PREDICT_FALSE(rc != TIME_OK)) {
    LOG(ERROR) << Substitute("TODO Server undergoing leap second. Return code: $0", rc);
  }
  return Status::OK();
}

// Returns the current time/max error and checks if the clock is synchronized.
Status GetClockTime(ntptimeval* timeval) {
  int rc = ntp_gettime(timeval);
  switch (rc) {
    case TIME_OK:
      return Status::OK();
    case -1: // generic error
      return Status::ServiceUnavailable("Error reading clock. ntp_gettime() failed",
                                        ErrnoToString(errno));
    case TIME_ERROR:
      return Status::ServiceUnavailable("Error reading clock. Clock considered unsynchronized");
    default:
      // TODO what to do about leap seconds? see KUDU-146
      KLOG_FIRST_N(ERROR, 1) << "Server undergoing leap second. This may cause consistency issues "
        << "(rc=" << rc << ")";
      return Status::OK();
  }
}

} // anonymous namespace

Status SystemNtp::Init() {
  // Read the current time. This will return an error if the clock is not synchronized.
  uint64_t now_usec;
  uint64_t error_usec;
  RETURN_NOT_OK(WalltimeWithError(&now_usec, &error_usec));

  timex timex;
  RETURN_NOT_OK(GetClockModes(&timex));
  // read whether the STA_NANO bit is set to know whether we'll get back nanos
  // or micros in timeval.time.tv_usec. See:
  // http://stackoverflow.com/questions/16063408/does-ntp-gettime-actually-return-nanosecond-precision
  // set the timeval.time.tv_usec divisor so that we always get micros
  if (timex.status & STA_NANO) {
    divisor_ = 1000;
  } else {
    divisor_ = 1;
  }

  // Calculate the sleep skew adjustment according to the max tolerance of the clock.
  // Tolerance comes in parts per million but needs to be applied a scaling factor.
  skew_ppm_ = timex.tolerance / kAdjtimexScalingFactor;

  LOG(INFO) << "NTP initialized. Resolution in nanos?: " << (divisor_ == 1000)
            << " Skew PPM: " << skew_ppm_
            << " Current error: " << error_usec;

  return Status::OK();
}


Status SystemNtp::WalltimeWithError(uint64_t *now_usec,
                                    uint64_t *error_usec) {
  // Read the time. This will return an error if the clock is not synchronized.
  ntptimeval timeval;
  RETURN_NOT_OK(GetClockTime(&timeval));
  *now_usec = timeval.time.tv_sec * kNanosPerSec + timeval.time.tv_usec / divisor_;
  *error_usec = timeval.maxerror;
  return Status::OK();
}

} // namespace clock
} // namespace kudu
