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

#include "kudu/util/char_util.h"

#include <cstdint>
#include <memory>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/init.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

using std::unique_ptr;

namespace kudu {

class CharUtilTest : public KuduTest {
 protected:
  Slice data_utf8_;
  Slice data_ascii_;

  void SetUp() override {
    // UTF8Truncate uses SSE4.1 instructions so we need to make sure the CPU
    // running the test has these opcodes.
    CHECK_OK(CheckCPUFlags());
    ReadFileToString(env_, JoinPathSegments(GetTestExecutableDirectory(),
                                           "testdata/char_truncate_utf8.txt"),
                     &string_utf8_);
    data_utf8_ = Slice(string_utf8_);
    ReadFileToString(env_, JoinPathSegments(GetTestExecutableDirectory(),
                                           "testdata/char_truncate_ascii.txt"),
                     &string_ascii_);
    data_ascii_ = Slice(string_ascii_);
  }

  unique_ptr<const uint8_t[]> Truncate(const Slice& slice, int length, Slice* result) {
    *result = UTF8Truncate(slice, length);
    return std::unique_ptr<const uint8_t[]>(result->data());
  }

  void StressTest(const Slice& slice, int length) {
    for (int i = 0; i < kNumCycles_; ++i) {
      Slice result;
      auto ptr = Truncate(slice, length, &result);
      ASSERT_FALSE(result.empty());
    }
  }

 private:
  const int kNumCycles_ = 1000000;
  faststring string_utf8_;
  faststring string_ascii_;
};

TEST_F(CharUtilTest, CorrectnessTestUtf8) {
  Slice result;
  auto ptr = Truncate(data_utf8_, 9756, &result);
  ASSERT_EQ(10549, result.size());

  ptr = Truncate(data_utf8_, 10549, &result);
  ASSERT_EQ(10550, result.size());
}

TEST_F(CharUtilTest, CorrectnessTestAscii) {
  Slice result;
  auto ptr = Truncate(data_ascii_, 9756, &result);
  ASSERT_EQ(9756, result.size());
  ptr = Truncate(data_ascii_, 10549, &result);
  ASSERT_EQ(10549, result.size());
}

TEST_F(CharUtilTest, CorrectnessTestIncompleteUtf8) {
  Slice result;
  Slice test_data = "aaaa\xf3";

  auto ptr = Truncate(test_data, 5, &result);
  ASSERT_EQ(test_data, result);
}

TEST_F(CharUtilTest, CorrectnessTestUtf8AndAscii) {
  Slice result;
  Slice data = "ááááááááááááááááááááááááááááááááaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

  auto ptr = Truncate(data, 64, &result);
  ASSERT_EQ(data, result);

  data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaáááááááááááááááááááááááááááááááá";
  ptr = Truncate(data, 64, &result);
  ASSERT_EQ(data, result);
}

TEST_F(CharUtilTest, StressTestUtf8) {
  StressTest(data_utf8_, 9000);
}

TEST_F(CharUtilTest, StressTestAscii) {
  StressTest(data_ascii_, 9000);
}

} // namespace kudu
