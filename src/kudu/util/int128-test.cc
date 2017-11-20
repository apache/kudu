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

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>

#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/int128.h"

using std::string;

namespace kudu {

TEST(TestInt128, TestOstreamSigned) {
  int128_t INTEGERS[] = {0, -1, 1, -1234567890,
                         INT64_MIN, UINT64_MAX,
                         INT128_MIN,
                         INT128_MAX};
  std::string STRINGS[] = {"0", "-1", "1", "-1234567890",
                           "-9223372036854775808", "18446744073709551615",
                           "-170141183460469231731687303715884105728",
                           "170141183460469231731687303715884105727"};
  for (size_t i = 0; i < arraysize(INTEGERS); i++) {
    std::ostringstream ss;
    ss << INTEGERS[i];
    ASSERT_EQ(STRINGS[i], ss.str());
  }
}

TEST(TestInt128, TestOstreamUnsigned) {
  uint128_t INTEGERS[] = {0, 1, 1234567890,
                          UINT128_MIN, UINT128_MAX};
  string STRINGS[] = {"0", "1", "1234567890",
                      "0", "340282366920938463463374607431768211455"};
  for (size_t i = 0; i < arraysize(INTEGERS); i++) {
    std::ostringstream ss;
    ss << INTEGERS[i];
    ASSERT_EQ(STRINGS[i], ss.str());
  }
}

TEST(TestInt128, TestCasting) {
  uint128_t mathToMax = (static_cast<uint128_t>(INT128_MAX) * 2) + 1;
  ASSERT_EQ(UINT128_MAX, mathToMax);

  uint128_t castToMax = static_cast<uint128_t>(-1);
  ASSERT_EQ(UINT128_MAX, castToMax);
}

} // namespace kudu
