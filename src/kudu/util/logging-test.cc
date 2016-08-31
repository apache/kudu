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

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <string>
#include <vector>

#include "kudu/util/logging_test_util.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"

using std::string;
using std::vector;

namespace kudu {

// Test the KLOG_EVERY_N_SECS(...) macro.
TEST(LoggingTest, TestThrottledLogging) {
  StringVectorSink sink;
  ScopedRegisterSink srs(&sink);

  for (int i = 0; i < 10000; i++) {
    KLOG_EVERY_N_SECS(INFO, 1) << "test" << THROTTLE_MSG;
    SleepFor(MonoDelta::FromMilliseconds(1));
    if (sink.logged_msgs().size() >= 2) break;
  }
  const vector<string>& msgs = sink.logged_msgs();
  ASSERT_GE(msgs.size(), 2);

  // The first log line shouldn't have a suppression count.
  EXPECT_THAT(msgs[0], testing::ContainsRegex("test$"));
  // The second one should have suppressed at least three digits worth of log messages.
  EXPECT_THAT(msgs[1], testing::ContainsRegex("\\[suppressed [0-9]{3,} similar messages\\]"));
}

TEST(LoggingTest, TestAdvancedThrottling) {
  StringVectorSink sink;
  ScopedRegisterSink srs(&sink);

  logging::LogThrottler throttle_a;

  // First, log only using a single tag and throttler.
  for (int i = 0; i < 100000; i++) {
    KLOG_EVERY_N_SECS_THROTTLER(INFO, 1, throttle_a, "tag_a") << "test" << THROTTLE_MSG;
    SleepFor(MonoDelta::FromMilliseconds(1));
    if (sink.logged_msgs().size() >= 2) break;
  }
  auto& msgs = sink.logged_msgs();
  ASSERT_GE(msgs.size(), 2);

  // The first log line shouldn't have a suppression count.
  EXPECT_THAT(msgs[0], testing::ContainsRegex("test$"));
  // The second one should have suppressed at least three digits worth of log messages.
  EXPECT_THAT(msgs[1], testing::ContainsRegex("\\[suppressed [0-9]{3,} similar messages\\]"));
  msgs.clear();

  // Now, try logging using two different tags in rapid succession. This should not
  // throttle, because the tag is switching.
  KLOG_EVERY_N_SECS_THROTTLER(INFO, 1, throttle_a, "tag_b") << "test b" << THROTTLE_MSG;
  KLOG_EVERY_N_SECS_THROTTLER(INFO, 1, throttle_a, "tag_b") << "test b" << THROTTLE_MSG;
  KLOG_EVERY_N_SECS_THROTTLER(INFO, 1, throttle_a, "tag_c") << "test c" << THROTTLE_MSG;
  KLOG_EVERY_N_SECS_THROTTLER(INFO, 1, throttle_a, "tag_b") << "test b" << THROTTLE_MSG;
  ASSERT_EQ(msgs.size(), 3);
  EXPECT_THAT(msgs[0], testing::ContainsRegex("test b$"));
  EXPECT_THAT(msgs[1], testing::ContainsRegex("test c$"));
  EXPECT_THAT(msgs[2], testing::ContainsRegex("test b$"));
}

} // namespace kudu
