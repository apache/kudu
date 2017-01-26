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

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <unordered_set>

#include "kudu/gutil/map-util.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/test_util.h"

DECLARE_bool(log_redact_user_data);
DECLARE_bool(never_fsync);

DEFINE_int32(flag_with_no_tags, 0, "test flag that has no tags");

DEFINE_int32(flag_with_one_tag, 0, "test flag that has 1 tag");
TAG_FLAG(flag_with_one_tag, stable);

DEFINE_int32(flag_with_two_tags, 0, "test flag that has 2 tags");
TAG_FLAG(flag_with_two_tags, evolving);
TAG_FLAG(flag_with_two_tags, unsafe);

DEFINE_bool(test_unsafe_flag, false, "an unsafe flag");
TAG_FLAG(test_unsafe_flag, unsafe);

DEFINE_bool(test_experimental_flag, false, "an experimental flag");
TAG_FLAG(test_experimental_flag, experimental);

using std::string;
using std::unordered_set;

namespace kudu {

class FlagTagsTest : public KuduTest {};

TEST_F(FlagTagsTest, TestTags) {
  unordered_set<string> tags;
  GetFlagTags("flag_with_no_tags", &tags);
  EXPECT_EQ(0, tags.size());

  GetFlagTags("flag_with_one_tag", &tags);
  EXPECT_EQ(1, tags.size());
  EXPECT_TRUE(ContainsKey(tags, "stable"));

  GetFlagTags("flag_with_two_tags", &tags);
  EXPECT_EQ(2, tags.size());
  EXPECT_TRUE(ContainsKey(tags, "evolving"));
  EXPECT_TRUE(ContainsKey(tags, "unsafe"));

  GetFlagTags("missing_flag", &tags);
  EXPECT_EQ(0, tags.size());
}

TEST_F(FlagTagsTest, TestUnlockFlags) {
  // Setting an unsafe flag without unlocking should crash.
  {
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("test_unsafe_flag", "true");
    ASSERT_DEATH({ HandleCommonFlags(); },
                 "Flag --test_unsafe_flag is unsafe and unsupported.*"
                 "Use --unlock_unsafe_flags to proceed");
  }

  // Setting an unsafe flag with unlocking should proceed with a warning.
  {
    StringVectorSink sink;
    ScopedRegisterSink reg(&sink);
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("test_unsafe_flag", "true");
    gflags::SetCommandLineOption("unlock_unsafe_flags", "true");
    HandleCommonFlags();
    ASSERT_EQ(1, sink.logged_msgs().size());
    ASSERT_STR_CONTAINS(sink.logged_msgs()[0], "Enabled unsafe flag: --test_unsafe_flag");
  }

  // Setting an experimental flag without unlocking should crash.
  {
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("test_experimental_flag", "true");
    ASSERT_DEATH({ HandleCommonFlags(); },
                 "Flag --test_experimental_flag is experimental and unsupported.*"
                 "Use --unlock_experimental_flags to proceed");
  }

  // Setting an experimental flag with unlocking should proceed with a warning.
  {
    StringVectorSink sink;
    ScopedRegisterSink reg(&sink);
    gflags::FlagSaver s;
    gflags::SetCommandLineOption("test_experimental_flag", "true");
    gflags::SetCommandLineOption("unlock_experimental_flags", "true");
    HandleCommonFlags();
    ASSERT_EQ(1, sink.logged_msgs().size());
    ASSERT_STR_CONTAINS(sink.logged_msgs()[0],
                        "Enabled experimental flag: --test_experimental_flag");
  }
}

} // namespace kudu
