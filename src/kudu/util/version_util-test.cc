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
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/version_info.h"

using std::string;
using std::pair;
using std::vector;

namespace kudu {

TEST(VersionUtilTest, TestVersion) {
  const vector<pair<Version, string>> good_test_cases = {
    { { "0.0.0", 0, 0, 0, boost::none, "" }, "0.0.0" },
    { { "1.0.0", 1, 0, 0, boost::none, "" }, "1.0.0" },
    { { "1.1.0", 1, 1, 0, boost::none, "" }, "1.1.0" },
    { { "1.1.1", 1, 1, 1, boost::none, "" }, "1.1.1" },
    { { "1.1.1-", 1, 1, 1, boost::none, "" }, "1.1.1" },
    { { "1.1.1-0-1-2", 1, 1, 1, '-', "0-1-2" }, "1.1.1-0-1-2" },
    { { "1.10.100-1000.0", 1, 10, 100, '-', "1000.0" }, "1.10.100-1000.0" },
    { { "1.2.3-SNAPSHOT", 1, 2, 3, '-', "SNAPSHOT" }, "1.2.3-SNAPSHOT" },
    { { "1.8.0-x-SNAPSHOT", 1, 8, 0, '-', "x-SNAPSHOT" }, "1.8.0-x-SNAPSHOT" },
    { { "0.1.2-a-b-c-d", 0, 1, 2, '-', "a-b-c-d" }, "0.1.2-a-b-c-d" },
    // no octals: leading zeros are just chopped off
    { { "00.01.010", 0, 1, 10, boost::none, "" }, "0.1.10" },
    { { "  0.1.2----suffix  ", 0, 1, 2, '-', "---suffix" }, "0.1.2----suffix" },
    { { "0.1.2- - -x- -y- ", 0, 1, 2, '-', " - -x- -y-" }, "0.1.2- - -x- -y-" },
    { { "1.11.0.7.0.0.0-SNAPSHOT", 1, 11, 0, '.', "7.0.0.0-SNAPSHOT" }, "1.11.0.7.0.0.0-SNAPSHOT" },
  };

  for (const auto& test_case : good_test_cases) {
    const auto& version = test_case.first;
    const auto& canonical_str = test_case.second;
    Version v;
    ASSERT_OK(ParseVersion(version.raw_version, &v));
    EXPECT_EQ(version, v);
    EXPECT_EQ(canonical_str, v.ToString());
  }

  const vector<string> bad_test_cases = {
    "",
    " ",
    "-",
    " -",
    "--",
    " - - ",
    "..",
    "0.1",
    "0.1.",
    "0.1.+",
    "+ 0.+ 1.+ 2",
    "0.1.+-woops",
    "0.1.2+",
    "1..1",
    ".0.1",
    " . . ",
    "-0.1.2-013",
    "0.-1.2-013",
    "0.1.-2-013",
    "1000,000.2999,999.999",
    "1e10.0.1",
    "1e+10.0.1",
    "1e-1.0.1",
    "1E+1.2.3",
    "1E-5.0.1",
    "foo",
    "foo.1.0",
    "a.b.c",
    "0.x1.x2",
    "0x0.0x1.0x2",
    "1.bar.0",
    "1.0.foo",
    "1.0foo.bar",
    "foo5-1.4.3",
    "1-2-3",
    "1.2-3",
    "1-2.3",
    "1-2-3-SNAPSHOT",
    "1.2-3-SNAPSHOT",
    "1-2.3-SNAPSHOT",
    " 0 .1 .2 -x",
    " 0 . 1 .  2 -x ",
    "+1.+2.+3-6",
    " +1 .  +2 .   +3 -100 .. ",
  };

  for (const auto& input_str : bad_test_cases) {
    Version v;
    const auto s = ParseVersion(input_str, &v);
    ASSERT_TRUE(s.IsInvalidArgument())
        << s.ToString() << ": " << input_str << " ---> " << v.ToString();
  }
}

// Sanity check: parse current Kudu version string and make sure the 'canonical'
// representation of the parsed version matches the 'raw' input as is.
TEST(VersionUtilTest, ParseCurrentKuduVersionString) {
  const auto ver_string = VersionInfo::GetShortVersionInfo();
  Version v;
  const auto s = ParseVersion(ver_string, &v);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(ver_string, v.ToString());
}

} // namespace kudu
