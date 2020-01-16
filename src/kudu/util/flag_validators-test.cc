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

#include <cstdlib>
#include <functional>
#include <map>
#include <ostream>
#include <string>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/flags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_string(grouped_0, "", "First flag to set.");
DEFINE_string(grouped_1, "", "Second flag to set.");
DEFINE_string(grouped_2, "", "Third flag to set.");
DEFINE_string(grouped_3, "", "Fourth flag to set.");

namespace kudu {

static bool CheckGroupedFlags01() {
  const bool is_set_0 = !FLAGS_grouped_0.empty();
  const bool is_set_1 = !FLAGS_grouped_1.empty();

  if (is_set_0 != is_set_1) {
    LOG(ERROR) << "--grouped_0 and --grouped_1 must be set as a group";
    return false;
  }

  return true;
}
GROUP_FLAG_VALIDATOR(test_group_validator01, CheckGroupedFlags01)

static bool CheckGroupedFlags23() {
  const bool is_set_2 = !FLAGS_grouped_2.empty();
  const bool is_set_3 = !FLAGS_grouped_3.empty();

  if (is_set_2 != is_set_3) {
    LOG(ERROR) << "--grouped_2 and --grouped_3 must be set as a group";
    return false;
  }

  return true;
}
GROUP_FLAG_VALIDATOR(test_group_validator23, CheckGroupedFlags23)

class FlagsValidatorsBasicTest : public KuduTest {
 public:
  void RunTest(const char** argv, int argc) {
    char** casted_argv = const_cast<char**>(argv);
    // ParseCommandLineFlags() calls exit(1) if it finds inconsistency in flags.
    ASSERT_EQ(1, ParseCommandLineFlags(&argc, &casted_argv, true));
  }
};

TEST_F(FlagsValidatorsBasicTest, Grouped) {
  const auto& validators = GetFlagValidators();
  ASSERT_EQ(2, validators.size());
  const auto& it = validators.find("test_group_validator01");
  ASSERT_NE(validators.end(), it);
  const auto& validator = it->second;
  EXPECT_TRUE(validator());
  FLAGS_grouped_0 = "0";
  EXPECT_FALSE(validator());
  FLAGS_grouped_1 = "1";
  EXPECT_TRUE(validator());
  FLAGS_grouped_0 = "";
  EXPECT_FALSE(validator());
  FLAGS_grouped_1 = "";
  EXPECT_TRUE(validator());
}

class FlagsValidatorsDeathTest : public KuduTest {
 public:
  void Run(const char** argv, int argc) {
    debug::ScopedLeakCheckDisabler disabler;
    char** casted_argv = const_cast<char**>(argv);
    // ParseCommandLineFlags() calls exit(1) if one of the custom validators
    // finds inconsistency in flags.
    ParseCommandLineFlags(&argc, &casted_argv, true);
    exit(0);
  }

  void RunSuccess(const char** argv, int argc) {
    EXPECT_EXIT(Run(argv, argc), ::testing::ExitedWithCode(0), ".*");
  }

  void RunFailure(const char** argv, int argc) {
    EXPECT_EXIT(Run(argv, argc), ::testing::ExitedWithCode(1),
        ".* Detected inconsistency in command-line flags; exiting");
  }
};

TEST_F(FlagsValidatorsDeathTest, GroupedSuccessNoFlags) {
  const char* argv[] = { "argv_set_0" };
  NO_FATALS(RunSuccess(argv, KUDU_ARRAYSIZE(argv)));
}

TEST_F(FlagsValidatorsDeathTest, GroupedSuccessSimple) {
  static const size_t kArgvSize = 1 + 2;
  const char* argv_sets[][kArgvSize] = {
    {
      "argv_set_0",
      "--grouped_0=first",
      "--grouped_1=second",
    },
    {
      "argv_set_1",
      "--grouped_0=second",
      "--grouped_1=first",
    },
    {
      "argv_set_2",
      "--grouped_0=",
      "--grouped_1=",
    },
    {
      "argv_set_3",
      "--grouped_1=",
      "--grouped_0=",
    },
    {
      "argv_set_4",
      "--grouped_2=2",
      "--grouped_3=3",
    },
    {
      "argv_set_5",
      "--grouped_3=",
      "--grouped_2=",
    },
  };
  for (auto argv : argv_sets) {
    RunSuccess(argv, kArgvSize);
  }
}

TEST_F(FlagsValidatorsDeathTest, GroupedFailureSimple) {
  static const size_t kArgvSize = 1 + 1;
  const char* argv_sets[][kArgvSize] = {
    {
      "argv_set_0",
      "--grouped_0=a",
    },
    {
      "argv_set_1",
      "--grouped_1=b",
    },
    {
      "argv_set_2",
      "--grouped_2=2",
    },
    {
      "argv_set_3",
      "--grouped_3=3",
    },
  };
  for (auto argv : argv_sets) {
    RunFailure(argv, kArgvSize);
  }
}

// Test for correct behavior when only one of two group validators is failing.
TEST_F(FlagsValidatorsDeathTest, GroupedFailureOneOfTwoValidators) {
  static const size_t kArgvSize = 4 + 1;
  const char* argv_sets[][kArgvSize] = {
    {
      "argv_set_0",
      "--grouped_0=0",
      "--grouped_1=1",
      "--grouped_2=",
      "--grouped_3=3",
    },
    {
      "argv_set_1",
      "--grouped_2=",
      "--grouped_3=3",
      "--grouped_0=0",
      "--grouped_1=1",
    },
    {
      "argv_set_2",
      "--grouped_0=0",
      "--grouped_1=",
      "--grouped_2=2",
      "--grouped_3=3",
    },
    {
      "argv_set_3",
      "--grouped_3=3",
      "--grouped_2=2",
      "--grouped_1=1",
      "--grouped_0=",
    },
  };
  for (auto argv : argv_sets) {
    RunFailure(argv, kArgvSize);
  }
}

TEST_F(FlagsValidatorsDeathTest, GroupedFailureWithEmptyValues) {
  static const size_t kArgvSize = 1 + 2;
  const char* argv_sets[][kArgvSize] = {
    {
      "argv_set_0",
      "--grouped_0=a",
      "--grouped_1=",
    },
    {
      "argv_set_1",
      "--grouped_1=",
      "--grouped_0=a",
    },
    {
      "argv_set_2",
      "--grouped_0=",
      "--grouped_1=b",
    },
    {
      "argv_set_3",
      "--grouped_1=b",
      "--grouped_0=",
    },
  };
  for (auto argv : argv_sets) {
    RunFailure(argv, kArgvSize);
  }
}

} // namespace kudu
