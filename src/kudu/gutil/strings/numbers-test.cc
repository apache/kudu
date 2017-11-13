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

#include "kudu/gutil/strings/numbers.h"
#include "kudu/util/int128.h"

namespace kudu {

TEST(TestNumbers, FastInt128ToBufferLeft) {
  char buf[64];
  std::string maxStr = std::string(buf, FastInt128ToBufferLeft(INT128_MAX, buf));
  ASSERT_EQ("170141183460469231731687303715884105727", maxStr);

  char buf2[64];
  std::string minStr = std::string(buf2, FastInt128ToBufferLeft(INT128_MIN, buf2));
  ASSERT_EQ("-170141183460469231731687303715884105728", minStr);

  char buf3[64];
  std::string shortStr = std::string(buf3, FastInt128ToBufferLeft(INT128_MIN / 10, buf3));
  ASSERT_EQ("-17014118346046923173168730371588410572", shortStr);

  char buf4[64];
  std::string shorterStr = std::string(buf4, FastInt128ToBufferLeft(INT128_MIN / 100000, buf4));
  ASSERT_EQ("-1701411834604692317316873037158841", shorterStr);
}

} // namespace kudu
