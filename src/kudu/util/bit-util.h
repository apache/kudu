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

#include "kudu/gutil/port.h"

namespace kudu {

// Utility class to do standard bit tricks
// TODO(unknown): is this in boost or something else like that?
class BitUtil {
 public:
  // Returns the ceil of value/divisor
  static constexpr inline int Ceil(int value, int divisor) {
    return value / divisor + (value % divisor != 0);
  }

  // Similar to the above, but a bit optimized for the case when the divisor
  // is a power of two: LOG2_DIV is log2(divisor), e.g. 3 for the divisor of 8.
  template <int LOG2_DIV>
  static constexpr inline int Ceil(int value) {
    constexpr int kDivisor = 1 << LOG2_DIV;
    constexpr int kComplement = kDivisor - 1;
    constexpr int kComplementMask = -kDivisor;
    return ((value + kComplement) & kComplementMask) >> LOG2_DIV;
  }

  // Returns the 'num_bits' least-significant bits of 'v'.
  static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
    if (PREDICT_FALSE(num_bits == 0)) {
      return 0;
    }
    if (PREDICT_FALSE(num_bits >= 64)) {
      return v;
    }
    int n = 64 - num_bits;
    return (v << n) >> n;
  }

  static inline uint64_t ShiftLeftZeroOnOverflow(uint64_t v, int num_bits) {
    if (PREDICT_FALSE(num_bits >= 64)) {
      return 0;
    }
    return v << num_bits;
  }

  static inline uint64_t ShiftRightZeroOnOverflow(uint64_t v, int num_bits) {
    if (PREDICT_FALSE(num_bits >= 64)) {
      return 0;
    }
    return v >> num_bits;
  }
};

} // namespace kudu
