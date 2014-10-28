// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Inline functions for doing overflow-safe operations on integers.
// These should be used when doing bounds checks on user-provided data,
// for example.
// See also: https://www.securecoding.cert.org/confluence/display/cplusplus/INT32-CPP.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow
#ifndef KUDU_UTIL_SAFE_MATH_H
#define KUDU_UTIL_SAFE_MATH_H

#include "kudu/gutil/mathlimits.h"

namespace kudu {

namespace safe_math_internal {

// Template which is specialized for signed and unsigned types separately.
template<typename Type, bool is_signed>
struct WithOverflowCheck {
};


// Specialization for signed types.
template<typename Type>
struct WithOverflowCheck<Type, true> {
  static inline Type Add(Type a, Type b, bool *overflowed) {
    // Implementation from the CERT article referenced in the file header.
    *overflowed = (((a > 0) && (b > 0) && (a > (MathLimits<Type>::kMax - b))) ||
                   ((a < 0) && (b < 0) && (a < (MathLimits<Type>::kMin - b))));
    return a + b;
  }
};

// Specialization for unsigned types.
template<typename Type>
struct WithOverflowCheck<Type, false> {
  static inline Type Add(Type a, Type b, bool *overflowed) {
    Type ret = a + b;
    *overflowed = ret < a;
    return a + b;
  }
};

} // namespace safe_math_internal

// Add 'a' and 'b', and set *overflowed to true if overflow occured.
template<typename Type>
inline Type AddWithOverflowCheck(Type a, Type b, bool *overflowed) {
  // Pick the right specialization based on whether Type is signed.
  typedef safe_math_internal::WithOverflowCheck<Type, MathLimits<Type>::kIsSigned> my_struct;
  return my_struct::Add(a, b, overflowed);
}

} // namespace kudu
#endif
