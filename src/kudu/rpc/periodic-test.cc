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

#include <atomic>
#include <functional>
#include <memory>

#include <gtest/gtest.h>

#include "kudu/rpc/messenger.h"
#include "kudu/rpc/periodic.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::shared_ptr;

namespace kudu {
namespace rpc {

class PeriodicTimerTest : public KuduTest,
                          public ::testing::WithParamInterface<double> {
 public:
  PeriodicTimerTest()
      :
#ifdef THREAD_SANITIZER
        // TSAN tends to deschedule threads for long periods of time.
        period_ms_(200),
#else
        period_ms_(20),
#endif
        counter_(0) {
  }

  virtual void SetUp() override {
    KuduTest::SetUp();

    MessengerBuilder builder("test");
    ASSERT_OK(builder.Build(&messenger_));

    timer_ = PeriodicTimer::Create(messenger_,
                                   [&] { counter_++; },
                                   MonoDelta::FromMilliseconds(period_ms_),
                                   GetParam());
  }

  virtual void TearDown() override {
    // Ensure that the reactor threads are fully quiesced (and thus no timer
    // callbacks are running) by the time 'counter_' is destroyed.
    messenger_->Shutdown();

    KuduTest::TearDown();
  }

 protected:
  const int64_t period_ms_;
  atomic<int64_t> counter_;
  shared_ptr<Messenger> messenger_;
  shared_ptr<PeriodicTimer> timer_;
};

INSTANTIATE_TEST_CASE_P(AllJitterModes,
                        PeriodicTimerTest,
                        ::testing::Values(0.0, 0.25));

TEST_P(PeriodicTimerTest, TestStartStop) {
  // Before the timer starts, the counter's value should not change.
  SleepFor(MonoDelta::FromMilliseconds(period_ms_ * 2));
  ASSERT_EQ(0, counter_);

  // Once started, it should increase (exactly how much depends on load and the
  // underlying OS scheduler).
  timer_->Start();
  SleepFor(MonoDelta::FromMilliseconds(period_ms_ * 2));
  ASSERT_EVENTUALLY([&]{
    ASSERT_GT(counter_, 0);
  });

  // After stopping the timer, the value should either remain the same or
  // increment once (if Stop() raced with a scheduled task).
  timer_->Stop();
  int64_t v = counter_;
  messenger_->Shutdown();
  ASSERT_TRUE(counter_ == v ||
              counter_ == v + 1);
}

TEST_P(PeriodicTimerTest, TestReset) {
  timer_->Start();
  MonoTime start_time = MonoTime::Now();

  // Loop for a little while, resetting the timer's period over and over. As a
  // result, the timer should never fire.
  while (true) {
    MonoTime now = MonoTime::Now();
    if (now - start_time > MonoDelta::FromMilliseconds(period_ms_ * 5)) {
      break;
    }
    timer_->Snooze();
    ASSERT_EQ(0, counter_);
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
}

TEST_P(PeriodicTimerTest, TestResetWithDelta) {
  timer_->Start();
  timer_->Snooze(MonoDelta::FromMilliseconds(period_ms_ * 5));

  // One period later, the counter still hasn't incremented...
  SleepFor(MonoDelta::FromMilliseconds(period_ms_));
  ASSERT_EQ(0, counter_);

  // ...but it will increment eventually.
  ASSERT_EVENTUALLY([&](){
    ASSERT_GT(counter_, 0);
  });
}

TEST_P(PeriodicTimerTest, TestStartWithDelta) {
  timer_->Start(MonoDelta::FromMilliseconds(period_ms_ * 5));

  // One period later, the counter still hasn't incremented...
  SleepFor(MonoDelta::FromMilliseconds(period_ms_));
  ASSERT_EQ(0, counter_);

  // ...but it will increment eventually.
  ASSERT_EVENTUALLY([&](){
    ASSERT_GT(counter_, 0);
  });
}

} // namespace rpc
} // namespace kudu
