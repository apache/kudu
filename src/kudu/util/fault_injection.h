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
#ifndef KUDU_UTIL_FAULT_INJECTION_H
#define KUDU_UTIL_FAULT_INJECTION_H

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

// Macros for injecting various kinds of faults with varying probability. If
// configured with 0 probability, each of these macros is evaluated inline and
// is fast enough to run even in hot code paths.

// With some probability, crash at the current point in the code
// by issuing LOG(FATAL).
//
// The probability is determined by the 'fraction_flag' argument.
//
// Typical usage:
//
//   DEFINE_double(fault_crash_before_foo, 0.0,
//                 "Fraction of the time when we will crash before doing foo");
//   TAG_FLAG(fault_crash_before_foo, unsafe);
#define MAYBE_FAULT(fraction_flag) \
  kudu::fault_injection::MaybeFault(AS_STRING(fraction_flag), fraction_flag)

// Inject a uniformly random amount of latency between 0 and the configured
// number of milliseconds.
#define MAYBE_INJECT_RANDOM_LATENCY(max_ms_flag) \
  kudu::fault_injection::MaybeInjectRandomLatency(max_ms_flag)

// With some probability, return the failure described by 'status_expr'.
//
// Unlike the other MAYBE_ macros, this one does not chain to an inline
// function so that 'status_expr' isn't evaluated unless 'fraction_flag'
// really is non-zero.
#define MAYBE_RETURN_FAILURE(fraction_flag, status_expr) \
  static const Status status_eval = (status_expr); \
  RETURN_NOT_OK(kudu::fault_injection::MaybeReturnFailure(fraction_flag, status_eval));

// Implementation details below.
// Use the MAYBE_FAULT macro instead.
namespace kudu {
namespace fault_injection {

// The exit status returned from a process exiting due to a fault.
// The choice of value here is arbitrary: just needs to be something
// wouldn't normally be returned by a non-fault-injection code path.
constexpr int kExitStatus = 85;

// Out-of-line implementation.
void DoMaybeFault(const char* fault_str, double fraction);
void DoInjectRandomLatency(double max_latency_ms);
Status DoMaybeReturnFailure(double fraction,
                            const Status& bad_status_to_return);

inline void MaybeFault(const char* fault_str, double fraction) {
  if (PREDICT_TRUE(fraction <= 0)) return;
  DoMaybeFault(fault_str, fraction);
}

inline void MaybeInjectRandomLatency(double max_latency) {
  if (PREDICT_TRUE(max_latency <= 0)) return;
  DoInjectRandomLatency(max_latency);
}

inline Status MaybeReturnFailure(double fraction,
                                 const Status& bad_status_to_return) {
  if (PREDICT_TRUE(fraction <= 0)) return Status::OK();
  return DoMaybeReturnFailure(fraction, bad_status_to_return);
}

} // namespace fault_injection
} // namespace kudu
#endif /* KUDU_UTIL_FAULT_INJECTION_H */
