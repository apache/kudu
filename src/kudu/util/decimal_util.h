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

#include <stdint.h>
#include <string>

#include "kudu/util/int128.h"

namespace kudu {
  // Maximum precision and absolute value of a Decimal that can be stored
  // in 4 bytes.
  static const int8_t kMaxDecimal32Precision = 9;
  static const int32_t kMaxUnscaledDecimal32 = 999999999; // 9 9's
  static const int32_t kMinUnscaledDecimal32 = -kMaxUnscaledDecimal32; // 9 9's

  // Maximum precision and absolute value of a valid Decimal can be
  // stored in 8 bytes.
  static const int8_t kMaxDecimal64Precision = 18;
  static const int64_t kMaxUnscaledDecimal64 = 999999999999999999; // 18 9's
  static const int64_t kMinUnscaledDecimal64 = -kMaxUnscaledDecimal64; // 18 9's

  // Maximum precision and absolute value of a valid Decimal can be
  // stored in 16 bytes.
  static const int8_t kMaxDecimal128Precision = 38;
  // Hacky calculation because int128 literals are not supported.
  static const int128_t kMaxUnscaledDecimal128 =
      (((static_cast<int128_t>(999999999999999999) * 1000000000000000000) +
          999999999999999999) * 100) + 99; // 38 9's
  static const int128_t kMinUnscaledDecimal128 = -kMaxUnscaledDecimal128;

  // Minimum and maximum precision for any Decimal.
  static const int8_t kMinDecimalPrecision = 1;
  static const int8_t kMaxDecimalPrecision = kMaxDecimal128Precision;
  // Maximum absolute value for any Decimal.
  static const int128_t kMaxUnscaledDecimal = kMaxUnscaledDecimal128;
  static const int128_t kMinUnscaledDecimal = kMinUnscaledDecimal128;

  // Minimum scale for any Decimal.
  static const int8_t kMinDecimalScale = 0;
  static const int8_t kDefaultDecimalScale = 0;
  // The maximum scale is the Decimal's precision.

  // Returns the maximum unscaled decimal value that can be stored
  // based on the precision
  int128_t MaxUnscaledDecimal(int8_t precision);

  std::string DecimalToString(int128_t value, int8_t scale);

} // namespace kudu
