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
#include "kudu/util/version_util.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

class VersionUtilTest : public KuduTest {};

TEST_F(VersionUtilTest, TestVersion) {
  const vector<Version> good_test_cases = {
    { "0.0.0", 0, 0, 0, "" },
    { "1.0.0", 1, 0, 0, "" },
    { "1.1.0", 1, 1, 0, "" },
    { "1.1.1", 1, 1, 1, "" },
    { "1.10.100-1000", 1, 10, 100, "1000" },
    { "1.2.3-SNAPSHOT", 1, 2, 3, "SNAPSHOT" },
  };

  Version v;
  for (const auto& test_case : good_test_cases) {
    ASSERT_OK(ParseVersion(test_case.raw_version, &v));
    EXPECT_EQ(test_case, v);
  }

  const vector<string> bad_test_cases = {
    "",
    "foo",
    "foo.1.0",
    "1.bar.0",
    "1.0.foo",
    "1.0foo.bar",
    "foo5-1.4.3",
  };

  for (const auto& test_case : bad_test_cases) {
    ASSERT_TRUE(ParseVersion(test_case, &v).IsInvalidArgument());
  }
}

} // namespace kudu
