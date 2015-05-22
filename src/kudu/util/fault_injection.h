// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_FAULT_INJECTION_H
#define KUDU_UTIL_FAULT_INJECTION_H

#include "kudu/gutil/macros.h"

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
//
// This macro should be fast enough to run even in hot code paths.
#define MAYBE_FAULT(fraction_flag) \
  kudu::fault_injection::MaybeFault(AS_STRING(fraction_flag), fraction_flag)

// Implementation details below.
// Use the MAYBE_FAULT macro instead.
namespace kudu {
namespace fault_injection {

// Out-of-line implementation.
void DoMaybeFault(const char* fault_str, double fraction);

inline void MaybeFault(const char* fault_str, double fraction) {
  if (PREDICT_TRUE(fraction <= 0)) return;
  DoMaybeFault(fault_str, fraction);
}

} // namespace fault_injection
} // namespace kudu
#endif /* KUDU_UTIL_FAULT_INJECTION_H */
