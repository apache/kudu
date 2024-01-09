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

#include "kudu/util/logging.h"

#include <atomic>
#include <cstdint>
#include <ctime>
#include <functional>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/async_logger.h"
#include "kudu/util/barrier.h"
#include "kudu/util/logging_test_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"  // IWYU pragma: keep
#include "kudu/util/test_util.h"

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

TEST(LoggingTest, ThrottledLoggingNoThrottleMsg) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  StringVectorSink sink;
  ScopedRegisterSink srs(&sink);

  for (int i = 0; i < 10000; i++) {
    KLOG_EVERY_N_SECS(INFO, 1) << "test";
    SleepFor(MonoDelta::FromMilliseconds(1));
    if (sink.logged_msgs().size() >= 2) {
      break;
    }
  }
  const vector<string>& msgs = sink.logged_msgs();
  ASSERT_GE(msgs.size(), 2);

  for (const auto& m: msgs) {
    // All the lines should contain the message logged.
    ASSERT_THAT(m, testing::ContainsRegex("test$"));
    // Since the special THROTTLE_MSG isn't used, there isn't any report on
    // suppressed messages.
    ASSERT_STR_NOT_CONTAINS(m, "suppressed");
  }
}

// Test the KLOG_EVERY_N_SECS(...) macro with slow-paced messages, making sure
// no messages are lost or suppressed if they come staggered by more than
// the suppression time interval.
TEST(LoggingTest, ThrottledLoggingLowFrequency) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  StringVectorSink sink;
  ScopedRegisterSink srs(&sink);

  for (int i = 0; i < 5; ++i) {
    KLOG_EVERY_N_SECS(INFO, 1) << "test " << i << THROTTLE_MSG;
    SleepFor(MonoDelta::FromMilliseconds(1050));
  }

  const auto& msgs = sink.logged_msgs();
  // Expecting the exact number: nothing more is logged from anywhere,
  // and all the logged messages should be sent to the sink without any
  // suppression.
  ASSERT_EQ(5, msgs.size());

  for (const auto& m: msgs) {
    EXPECT_THAT(m, testing::ContainsRegex("test [0-4]$"));
    // No messages should be suppressed.
    ASSERT_STR_NOT_CONTAINS(m, "suppressed");
  }
}

// A test scenario for KLOG_EVERY_N_SECS() where a short burst of messages
// is sent through.
//
// This scenario sends many messages separated from each other by an interval
// a few orders of magnitude shorter than the suppression interval,
// and all the messages are sent within a single suppression time interval.
//
// Only the very first message is logged, and nothing is reported on the rest
// that were suppresed. The information on the suppressed messages is output
// only when another log message arrives, and it may never arrive or arrive
// a long time after the original message burst.
TEST(LoggingTest, ThrottledLoggingShortBurst) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  StringVectorSink sink;
  ScopedRegisterSink srs(&sink);
  for (int i = 0; i < 2000; ++i) {
    KLOG_EVERY_N_SECS(INFO, 1) << "test " << i << THROTTLE_MSG;
    SleepFor(MonoDelta::FromMicroseconds(10));
  }
  // Just in case, sleep for two suppression intervals.
  SleepFor(MonoDelta::FromMilliseconds(2000));

  const auto& msgs = sink.logged_msgs();
  // Only the very first message in the burst is accounted for.
  ASSERT_EQ(1, msgs.size());
  ASSERT_THAT(msgs[0], testing::ContainsRegex("test 0$"));

  for (const auto& m: msgs) {
    // No information on thousands of suppressed messages yet.
    ASSERT_STR_NOT_CONTAINS(m, "suppressed");
  }
}

TEST(LoggingTest, LogThrottleDestructorReport) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  StringVectorSink sink;
  ScopedRegisterSink srs(&sink);

  {
    logging::LogThrottler throttler(5, "in test");

    for (int i = 0; i < 100; ++i) {
      KLOG_THROTTLER(INFO, throttler) << "test " << i << THROTTLE_MSG;
    }
    // Sleep for a second to check for the proper timings reported in the
    // summary output by the LogThrottler's destructor.
    SleepFor(MonoDelta::FromMilliseconds(1000));

    const auto& msgs = sink.logged_msgs();
    // Only the very first message in the burst is accounted for.
    ASSERT_EQ(1, msgs.size());
    ASSERT_THAT(msgs.front(), testing::ContainsRegex("test 0$"));

    for (const auto& m: msgs) {
      // No information on suppressed messages yet.
      ASSERT_STR_NOT_CONTAINS(m, "suppressed");
    }
  }

  // The throttler should report on the suppressed but not yet logged messages
  // in the destructor. To avoid flakiness due to scheduling anomalies on busy
  // test nodes, the expected timing is flexible to accommodate for several
  // extra seconds between the first message logged and the time when the
  // LogThrottler's destructor has run.
  const auto& msgs = sink.logged_msgs();
  ASSERT_EQ(2, msgs.size());
  ASSERT_THAT(msgs.back(), testing::ContainsRegex(
      "suppressed but not reported on 99 messages "
      "since previous log \\~[1-9] seconds ago"));
}

// Test Logger implementation that just counts the number of messages
// and flushes.
//
// This is purposefully thread-unsafe because we expect that the
// AsyncLogger is only accessing the underlying logger from a single
// thhread.
class CountingLogger : public google::base::Logger {
 public:
  void Write(bool force_flush,
             time_t /*timestamp*/,
             const char* /*message*/,
             size_t /*message_len*/) override {
    message_count_++;
    if (force_flush) {
      Flush();
    }
  }

  void Flush() override {
    // Simulate a slow disk.
    SleepFor(MonoDelta::FromMilliseconds(5));
    flush_count_++;
  }

  uint32_t LogSize() override {
    return 0;
  }

  std::atomic<int> flush_count_ = {0};
  std::atomic<int> message_count_ = {0};
};

TEST(LoggingTest, TestAsyncLogger) {
  const int kNumThreads = 4;
  const int kNumMessages = 10000;
  const int kBuffer = 10000;
  CountingLogger base;
  AsyncLogger async(&base, kBuffer);
  async.Start();

  vector<std::thread> threads;
  Barrier go_barrier(kNumThreads + 1);
  // Start some threads writing log messages.
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&]() {
        go_barrier.Wait();
        for (int m = 0; m < kNumMessages; m++) {
          async.Write(true, m, "x", 1);
        }
      });
  }

  // And a thread calling Flush().
  threads.emplace_back([&]() {
      go_barrier.Wait();
      for (int i = 0; i < 10; i++) {
        async.Flush();
        SleepFor(MonoDelta::FromMilliseconds(3));
      }
    });

  for (auto& t : threads) {
    t.join();
  }
  async.Stop();
  ASSERT_EQ(base.message_count_, kNumMessages * kNumThreads);
  // The async logger should only flush once per "batch" rather than
  // once per message, even though we wrote every message with
  // 'flush' set to true.
  ASSERT_LT(base.flush_count_, kNumMessages * kNumThreads);
  ASSERT_GT(async.app_threads_blocked_count_for_tests(), 0);
}

TEST(LoggingTest, TestAsyncLoggerAutoFlush) {
  const int kBuffer = 10000;
  CountingLogger base;
  AsyncLogger async(&base, kBuffer);

  FLAGS_logbufsecs = 1;
  async.Start();

  // Write some log messages with non-force_flush types.
  async.Write(false, 0, "test-x", 1);
  async.Write(false, 1, "test-y", 1);

  // The flush wait timeout might take a little bit of time to run.
  ASSERT_EVENTUALLY([&]() {
    ASSERT_EQ(base.message_count_, 2);
    // The AsyncLogger should have flushed at least once by the timer automatically
    // so there should be no more messages in the buffer.
    ASSERT_GT(base.flush_count_, 0);
  });
  async.Stop();
}

// Basic test that the redaction utilities work as expected.
TEST(LoggingTest, TestRedactionBasic) {
  ASSERT_STREQ("<redacted>", KUDU_REDACT("hello"));
  {
    ScopedDisableRedaction no_redaction;
    ASSERT_STREQ("hello", KUDU_REDACT("hello"));
  }
  ASSERT_STREQ("hello", KUDU_DISABLE_REDACTION(KUDU_REDACT("hello")));
}

// Typically, ToString() methods apply to some complex object with a bunch
// of fields, some of which are user data (need redaction) and others of which
// are not. This shows an example of a such a function, which will behave
// differently based on whether the calling scope has explicitly disabled
// redaction.
string SomeComplexStringify(const string& public_data, const string& private_data) {
  return strings::Substitute("public=$0, private=$1",
                             public_data,
                             KUDU_REDACT(private_data));
}

TEST(LoggingTest, TestRedactionIllustrateUsage) {
  // By default, the private data will be redacted.
  ASSERT_EQ("public=abc, private=<redacted>", SomeComplexStringify("abc", "def"));

  // We can wrap the expression in KUDU_DISABLE_REDACTION(...) to evaluate it
  // with redaction temporarily disabled.
  ASSERT_EQ("public=abc, private=def", KUDU_DISABLE_REDACTION(SomeComplexStringify("abc", "def")));

  // Or we can execute an entire scope with redaction disabled.
  KUDU_DISABLE_REDACTION(({
    ASSERT_EQ("public=abc, private=def", SomeComplexStringify("abc", "def"));
  }));
}


TEST(LoggingTest, TestLogTiming) {
  LOG_TIMING(INFO, "foo") {
  }
  {
    SCOPED_LOG_TIMING(INFO, "bar");
  }
  LOG_SLOW_EXECUTION(INFO, 1, "baz") {
  }

  // Previous implementations of the above macro confused clang-tidy's use-after-move
  // check and generated false positives.
  string s1 = "hello";
  string s2;
  LOG_SLOW_EXECUTION(INFO, 1, "baz") {
    LOG(INFO) << s1;
    s2 = std::move(s1);
  }

  ASSERT_EQ("hello", s2);
}

// Test that VLOG(n) does not evaluate its message if the verbose level is < n,
// ensuring that it is perf-safe to write things like
//
//   VLOG(1) << Substitute("your foo is $0", compute_costly_bar_string());
//
// in hot code paths.
TEST(LoggingTest, TestVlogDoesNotEvaluateMessage) {
  if (VLOG_IS_ON(1)) {
    GTEST_SKIP() << "Test skipped: verbose level is at least 1";
  }

  int numVlogs = 0;
  VLOG(1) << "This shouldn't be logged: " << numVlogs++;
  ASSERT_EQ(0, numVlogs);
}
} // namespace kudu
