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

#include <string>
#include <vector>

#include "kudu/gutil/macros.h"

namespace kudu {
class Status;

namespace clock {

// Interface encapsulating interaction with a time service (eg NTP).
class TimeService {
 public:
  TimeService() = default;
  virtual ~TimeService() = default;

  // Initialize the clock source. No verification is performed on the
  // synchronisation status of the clock. To verify the clock is synchronized
  // and the time service is ready to use, make sure WalltimeWithError() returns
  // Status::OK() after calling Init(). The Init() method it itself should be
  // called only once.
  virtual Status Init() = 0;

  // Return the current wall time in microseconds since the Unix epoch in '*now_usec'.
  // The current maximum error bound in microseconds is returned in '*error_usec'.
  // Neither of the output parameters can be null.
  //
  // May return a bad Status if the NTP service has become unsynchronized or
  // otherwise unavailable. In case if the method returns ServiceUnavailable()
  // after calling Init(), the caller may call this method again and eventually
  // get Status::OK() if anticipating the clock synchronisation to happen soon.
  virtual Status WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) = 0;

  // Return the estimated max amount of clock skew as configured by this NTP service.
  // This is expressed in PPM (parts-per-million) and indicates the maximum number
  // of microseconds by which the local clock can be expected to drift for each
  // elapsed second of actual time.
  //
  // For example, if the local monotonic clock indicates that 1 second has elapsed,
  // and skew_ppm() returns 500, then the actual time may have actually changed by
  // 1sec +/- 500us.
  virtual int64_t skew_ppm() const = 0;

  // Run diagnostics tool related to this time service and save the output into 'log'.
  // If 'log' is null, logs to LOG(ERROR).
  //
  // NOTE: this may fork out to external processes which may time out waiting on network
  // responses, etc. As such, it may take several seconds to run.
  virtual void DumpDiagnostics(std::vector<std::string>* /*log*/) const {}
};

} // namespace clock
} // namespace kudu
