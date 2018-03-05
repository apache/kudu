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

#include "kudu/util/decimal_util.h"

#include <string>

#include <glog/logging.h>

#include "kudu/gutil/port.h"

namespace kudu {

using std::string;

// Workaround for an ASAN build issue documented here:
// https://bugs.llvm.org/show_bug.cgi?id=16404
ATTRIBUTE_NO_SANITIZE_UNDEFINED
int128_t MaxUnscaledDecimal(int8_t precision) {
  DCHECK_GE(precision, kMinDecimalPrecision);
  DCHECK_LE(precision, kMaxDecimalPrecision);
  int128_t result = 1;
  for (; precision > 0; precision--) {
    result = result * 10;
  }
  return result - 1;
}

int128_t MinUnscaledDecimal(int8_t precision) {
  return -MaxUnscaledDecimal(precision);
}

// Workaround for an ASAN build issue documented here:
// https://bugs.llvm.org/show_bug.cgi?id=16404
ATTRIBUTE_NO_SANITIZE_UNDEFINED
string DecimalToString(int128_t d, int8_t scale) {
  // 38 digits, 1 extra leading zero, decimal point,
  // and sign are good for 128-bit or smaller decimals.
  char local[41];
  char *p = local + sizeof(local);
  int128_t n = d < 0? -d : d;
  int position = 0;
  while (n) {
    // Print the decimal in the scale position.
    // No decimal is output when scale is 0.
    if (scale != 0 && position == scale) {
      *--p = '.';
    }
    // Unroll the next digits.
    *--p = '0' + n % 10;
    n /= 10;
    position++;
  }
  // True if the value is between 1 and -1.
  bool fractional = position <= scale;
  // Pad with zeros until the scale
  while (position < scale) {
    *--p = '0';
    position++;
  }
  // Add leading "0.".
  if (fractional) {
    if (d != 0) {
      *--p = '.';
    }
    *--p = '0';
  }
  // Add sign for negative values.
  if (d < 0) {
    *--p = '-';
  }
  return string(p, local + sizeof(local));
}

} // namespace kudu
