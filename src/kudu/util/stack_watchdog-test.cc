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

#include "kudu/util/kernel_stack_watchdog.h"

#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/monotime.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::thread;
using std::vector;
using strings::Substitute;

DECLARE_int32(hung_task_check_interval_ms);
DECLARE_int32(inject_latency_on_kernel_stack_lookup_ms);

namespace kudu {

class StackWatchdogTest : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    KernelStackWatchdog::GetInstance()->SaveLogsForTests(true);
    ANNOTATE_BENIGN_RACE(&FLAGS_hung_task_check_interval_ms, "");
    ANNOTATE_BENIGN_RACE(&FLAGS_inject_latency_on_kernel_stack_lookup_ms, "");
    FLAGS_hung_task_check_interval_ms = 10;
  }
};

// The KernelStackWatchdog is only enabled on Linux, since we can't get kernel
// stack traces on other platforms.
#if defined(__linux__)
TEST_F(StackWatchdogTest, TestWatchdog) {
  vector<string> log;
  {
    SCOPED_WATCH_STACK(20);
    for (int i = 0; i < 50; i++) {
      SleepFor(MonoDelta::FromMilliseconds(100));
      log = KernelStackWatchdog::GetInstance()->LoggedMessagesForTests();
      // Wait for several samples, since it's possible that we get unlucky
      // and the watchdog sees us just before or after a sleep.
      if (log.size() > 5) {
        break;
      }
    }
  }
  string s = JoinStrings(log, "\n");
  ASSERT_STR_CONTAINS(s, "TestWatchdog_Test::TestBody()");
  ASSERT_STR_CONTAINS(s, "nanosleep");
}
#endif

// Test that SCOPED_WATCH_STACK scopes can be nested.
TEST_F(StackWatchdogTest, TestNestedScopes) {
  vector<string> log;
  int line1;
  int line2;
  {
    SCOPED_WATCH_STACK(20); line1 = __LINE__;
    {
      SCOPED_WATCH_STACK(20); line2 = __LINE__;
      for (int i = 0; i < 50; i++) {
        SleepFor(MonoDelta::FromMilliseconds(100));
        log = KernelStackWatchdog::GetInstance()->LoggedMessagesForTests();
        if (log.size() > 3) {
          break;
        }
      }
    }
  }

  // Verify that both nested scopes were collected.
  string s = JoinStrings(log, "\n");
  ASSERT_STR_CONTAINS(s, Substitute("stack_watchdog-test.cc:$0", line1));
  ASSERT_STR_CONTAINS(s, Substitute("stack_watchdog-test.cc:$0", line2));
}

TEST_F(StackWatchdogTest, TestPerformance) {
  // Reset the check interval to be reasonable. Otherwise the benchmark
  // wastes a lot of CPU running the watchdog thread too often.
  FLAGS_hung_task_check_interval_ms = 500;
  LOG_TIMING(INFO, "1M SCOPED_WATCH_STACK()s") {
    for (int i = 0; i < 1000000; i++) {
      SCOPED_WATCH_STACK(100);
    }
  }
}

// Stress test to ensure that we properly handle the case where threads are short-lived
// and the watchdog may try to grab a stack of a thread that has already exited.
//
// This also serves as a benchmark -- we make the stack-grabbing especially slow and
// ensure that we can still start and join threads quickly.
TEST_F(StackWatchdogTest, TestShortLivedThreadsStress) {
  // Run the stack watchdog continuously.
  FLAGS_hung_task_check_interval_ms = 0;

  // Make the actual stack trace collection slow. In practice we find that
  // stack trace collection can often take quite some time due to symbolization, etc.
  FLAGS_inject_latency_on_kernel_stack_lookup_ms = 1000;

  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(5);
  vector<thread> threads(100);
  int started = 0;
  while (MonoTime::Now() < deadline) {
    thread* t = &threads[started % threads.size()];
    if (t->joinable()) {
      t->join();
    }
    *t = thread([&]() {
        // Trigger watchdog at 1ms, but then sleep for 2ms, to ensure that
        // the watchdog has plenty of work to do.
        SCOPED_WATCH_STACK(1);
        SleepFor(MonoDelta::FromMilliseconds(2));
      });
    started++;
  }
  for (auto& t : threads) {
    if (t.joinable()) t.join();
  }
  LOG(INFO) << "started and joined " << started << " threads";
}
} // namespace kudu
