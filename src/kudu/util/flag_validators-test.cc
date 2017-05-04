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

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/util/debug/leakcheck_disabler.h"
#include "kudu/util/flags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/logging.h"
#include "kudu/util/test_util.h"

DEFINE_string(grouped_0, "", "First flag to set.");
DEFINE_string(grouped_1, "", "Second flag to set.");

namespace kudu {

static bool CheckGroupedFlags() {
  const bool is_set_0 = !FLAGS_grouped_0.empty();
  const bool is_set_1 = !FLAGS_grouped_1.empty();

  if (is_set_0 != is_set_1) {
    LOG(ERROR) << "--grouped_0 and --grouped_1 must be set as a group";
    return false;
  }

  return true;
}
GROUP_FLAG_VALIDATOR(test_group_validator, CheckGroupedFlags)

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
  ASSERT_EQ(1, validators.size());
  const auto& validator = validators.begin()->second;
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
  NO_FATALS(RunSuccess(argv, ARRAYSIZE(argv)));
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
