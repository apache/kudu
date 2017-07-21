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

#include "kudu/gutil/macros.h"

namespace kudu {
class Status;

namespace clock {

// Interface encapsulating interaction with a time service (eg NTP).
class TimeService {
 public:
  TimeService() = default;
  virtual ~TimeService() = default;

  // Initialize the NTP source, validating that it is available and
  // properly synchronized.
  virtual Status Init() = 0;

  // Return the current wall time in microseconds since the Unix epoch in '*now_usec'.
  // The current maximum error bound in microseconds is returned in '*error_usec'.
  //
  // May return a bad Status if the NTP service has become unsynchronized or otherwise
  // unavailable.
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
};

} // namespace clock
} // namespace kudu
