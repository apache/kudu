// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <stdint.h>
#include <gtest/gtest.h>
#include "kudu/gutil/mathlimits.h"
#include "kudu/util/safe_math.h"

namespace kudu {
template<typename T>
static void DoTest(T a, T b, bool expected) {
  SCOPED_TRACE(a);
  SCOPED_TRACE(b);
  bool overflow = false;
  T ret = AddWithOverflowCheck(a, b, &overflow);
  EXPECT_EQ(overflow, expected);
  if (!overflow) {
    EXPECT_EQ(ret, a + b);
  }
}

TEST(TestSafeMath, TestSignedInts) {
  // Overflow above max of range.
  DoTest<int32_t>(MathLimits<int32_t>::kMax - 10, 15, true);
  DoTest<int32_t>(MathLimits<int32_t>::kMax - 10, 10, false);

  // Underflow around negative
  DoTest<int32_t>(MathLimits<int32_t>::kMin + 10, -15, true);
  DoTest<int32_t>(MathLimits<int32_t>::kMin + 10, -5, false);

}

TEST(TestSafeMath, TestUnsignedInts) {
  // Overflow above max
  DoTest<uint32_t>(MathLimits<uint32_t>::kMax - 10, 15, true);
  DoTest<uint32_t>(MathLimits<uint32_t>::kMax - 10, 10, false);
}

} // namespace kudu
