// Copyright (c) 2013, Cloudera, inc.
//
// Inline functions for doing overflow-safe operations on integers.
// These should be used when doing bounds checks on user-provided data,
// for example.
// See also: https://www.securecoding.cert.org/confluence/display/cplusplus/INT32-CPP.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow
#ifndef KUDU_UTIL_SAFE_MATH_H
#define KUDU_UTIL_SAFE_MATH_H

#include "gutil/mathlimits.h"

namespace kudu {

// Add 'a' and 'b', and set *overflowed to true if overflow occured.
template<typename Type>
inline Type AddWithOverflowCheck(Type a, Type b, bool *overflowed) {
  if (MathLimits<Type>::kIsSigned) {
    // Implementation from the CERT article referenced in the file header.
    *overflowed = (((a > 0) && (b > 0) && (a > (MathLimits<Type>::kMax - b))) ||
                   ((a < 0) && (b < 0) && (a < (MathLimits<Type>::kMin - b))));
    return a + b;
  }
  // Unsigned ints:
  Type ret = a + b;
  *overflowed = ret < a;
  return ret;
}

} // namespace kudu
#endif
