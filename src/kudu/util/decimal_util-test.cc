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

#include <string>

#include <gtest/gtest.h>

#include "kudu/util/decimal_util.h"

using std::string;

namespace kudu {

TEST(TestDecimalUtil, TestMaxUnscaledDecimal) {
  ASSERT_EQ(9, MaxUnscaledDecimal(1));
  ASSERT_EQ(99999, MaxUnscaledDecimal(5));
  ASSERT_EQ(kMaxUnscaledDecimal32, MaxUnscaledDecimal(kMaxDecimal32Precision));
  ASSERT_EQ(kMaxUnscaledDecimal64, MaxUnscaledDecimal(kMaxDecimal64Precision));
  ASSERT_EQ(kMaxUnscaledDecimal128, MaxUnscaledDecimal(kMaxDecimal128Precision));
}

TEST(TestDecimalUtil, TestToString) {
  ASSERT_EQ("999999999",
            DecimalToString(kMaxUnscaledDecimal32, kDefaultDecimalScale));
  ASSERT_EQ("0.999999999",
            DecimalToString(kMaxUnscaledDecimal32, kMaxDecimal32Precision));
  ASSERT_EQ("-999999999",
            DecimalToString(kMinUnscaledDecimal32, kDefaultDecimalScale));
  ASSERT_EQ("-0.999999999",
            DecimalToString(kMinUnscaledDecimal32, kMaxDecimal32Precision));

  ASSERT_EQ("999999999999999999",
            DecimalToString(kMaxUnscaledDecimal64, kDefaultDecimalScale));
  ASSERT_EQ("0.999999999999999999",
            DecimalToString(kMaxUnscaledDecimal64, kMaxDecimal64Precision));
  ASSERT_EQ("-999999999999999999",
            DecimalToString(kMinUnscaledDecimal64, kDefaultDecimalScale));
  ASSERT_EQ("-0.999999999999999999",
            DecimalToString(kMinUnscaledDecimal64, kMaxDecimal64Precision));

  ASSERT_EQ("99999999999999999999999999999999999999",
            DecimalToString(kMaxUnscaledDecimal128, kDefaultDecimalScale));
  ASSERT_EQ("0.99999999999999999999999999999999999999",
            DecimalToString(kMaxUnscaledDecimal128, kMaxDecimal128Precision));
  ASSERT_EQ("-99999999999999999999999999999999999999",
            DecimalToString(kMinUnscaledDecimal128, kDefaultDecimalScale));
  ASSERT_EQ("-0.99999999999999999999999999999999999999",
            DecimalToString(kMinUnscaledDecimal128, kMaxDecimal128Precision));

  ASSERT_EQ("0", DecimalToString(0, 0));
  ASSERT_EQ("12345", DecimalToString(12345, 0));
  ASSERT_EQ("-12345", DecimalToString(-12345, 0));
  ASSERT_EQ("123.45", DecimalToString(12345, 2));
  ASSERT_EQ("-123.45", DecimalToString(-12345, 2));
  ASSERT_EQ("0.00012345", DecimalToString(12345, 8));
  ASSERT_EQ("-0.00012345", DecimalToString(-12345, 8));
}

} // namespace kudu
