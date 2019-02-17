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

#include "kudu/util/flags.h"

#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

// Test gflags
DEFINE_string(test_nondefault_ff, "default",
             "Check if we track non defaults from flagfile");
DEFINE_string(test_nondefault_explicit, "default",
             "Check if we track explicitly set non defaults");
DEFINE_string(test_default_ff, "default",
             "Check if we track defaults from flagfile");
DEFINE_string(test_default_explicit, "default",
             "Check if we track explicitly set defaults");
DEFINE_bool(test_sensitive_flag, false, "a sensitive flag");
TAG_FLAG(test_sensitive_flag, sensitive);

DECLARE_bool(never_fsync);

namespace kudu {

class FlagsTest : public KuduTest {};

TEST_F(FlagsTest, TestNonDefaultFlags) {
  std::string flagfile_path(GetTestPath("test_nondefault_flags"));
  std::string flagfile_contents = "--test_nondefault_ff=nondefault\n"
                                  "--test_default_ff=default";

  CHECK_OK(WriteStringToFile(Env::Default(),
                             Slice(flagfile_contents.data(),
                                   flagfile_contents.size()),
                             flagfile_path));

  std::string flagfile_flag = strings::Substitute("--flagfile=$0", flagfile_path);
  int argc = 4;
  const char* argv[4] = {
    "some_executable_file",
    "--test_nondefault_explicit=nondefault",
    "--test_default_explicit=default",
    flagfile_flag.c_str()
  };

  char** casted_argv = const_cast<char**>(argv);
  ParseCommandLineFlags(&argc, &casted_argv, true);

  std::vector<const char*> expected_flags = {
    "--test_nondefault_explicit=nondefault",
    "--test_nondefault_ff=nondefault",
    flagfile_flag.c_str()
  };

  std::vector<const char*> unexpected_flags = {
    "--test_default_explicit",
    "--test_default_ff"
  };

  // Setting a sensitive flag with non-default value should return
  // a redacted value.
  FLAGS_test_sensitive_flag = true;
  kudu::g_should_redact = kudu::RedactContext::LOG;
  std::string result = GetNonDefaultFlags();

  for (const auto& expected : expected_flags) {
    ASSERT_STR_CONTAINS(result, expected);
  }

  for (const auto& unexpected : unexpected_flags) {
    ASSERT_STR_NOT_CONTAINS(result, unexpected);
  }

  ASSERT_STR_CONTAINS(result, strings::Substitute("--test_sensitive_flag=$0",
                                                  kRedactionMessage));
}

} // namespace kudu
