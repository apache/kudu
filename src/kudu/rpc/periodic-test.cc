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
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/rpc/messenger.h"
#include "kudu/rpc/periodic.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::atomic;
using std::shared_ptr;
using std::vector;

namespace kudu {
namespace rpc {

class PeriodicTimerTest : public KuduTest {
 public:
  PeriodicTimerTest()
      : period_ms_(200) {}

 protected:
  const int64_t period_ms_;
};

class JitteredPeriodicTimerTest : public PeriodicTimerTest,
                                  public ::testing::WithParamInterface<double> {
 public:
  // In TSAN builds it takes a long time to de-schedule a thread. Also,
  // the actual time that thread spends sleeping in SleepFor() scenarios
  // might be much longer than requested. Setting the task period to be long
  // enough allows for more stable behavior of the test, so no flakiness
  // is observed even under substantial load. Otherwise it would be necessary
  // to introduce additional logic to verify that the actual timings satisfy
  // the implicit constraints of the test scenarios below.
  JitteredPeriodicTimerTest()
      : counter_(0) {
  }

  virtual void SetUp() override {
    PeriodicTimerTest::SetUp();

    MessengerBuilder builder("test");
    ASSERT_OK(builder.Build(&messenger_));

    timer_ = PeriodicTimer::Create(messenger_,
                                   [&] { counter_++; },
                                   MonoDelta::FromMilliseconds(period_ms_),
                                   GetOptions());
  }

  virtual void TearDown() override {
    // Ensure that the reactor threads are fully quiesced (and thus no timer
    // callbacks are running) by the time 'counter_' is destroyed.
    messenger_->Shutdown();

    KuduTest::TearDown();
  }

 protected:

  virtual PeriodicTimer::Options GetOptions() {
    PeriodicTimer::Options opts;
    opts.jitter_pct = GetParam();
    return opts;
  }

  atomic<int64_t> counter_;
  shared_ptr<Messenger> messenger_;
  shared_ptr<PeriodicTimer> timer_;
};

INSTANTIATE_TEST_CASE_P(AllJitterModes,
                        JitteredPeriodicTimerTest,
                        ::testing::Values(0.0, 0.25));

TEST_P(JitteredPeriodicTimerTest, TestStartStop) {
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

TEST_P(JitteredPeriodicTimerTest, TestReset) {
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

TEST_P(JitteredPeriodicTimerTest, TestResetWithDelta) {
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

TEST_P(JitteredPeriodicTimerTest, TestStartWithDelta) {
  timer_->Start(MonoDelta::FromMilliseconds(period_ms_ * 5));

  // One period later, the counter still hasn't incremented...
  SleepFor(MonoDelta::FromMilliseconds(period_ms_));
  ASSERT_EQ(0, counter_);

  // ...but it will increment eventually.
  ASSERT_EVENTUALLY([&](){
    ASSERT_GT(counter_, 0);
  });
}

TEST_F(PeriodicTimerTest, TestCallbackRestartsTimer) {
  const int64_t kPeriods = 10;

  shared_ptr<Messenger> messenger;
  ASSERT_OK(MessengerBuilder("test").Build(&messenger));

  // Create a timer that restarts itself from within its functor.
  PeriodicTimer::Options opts;
  opts.jitter_pct = 0.0; // don't need jittering
  shared_ptr<PeriodicTimer> timer = PeriodicTimer::Create(
      messenger,
      [&] {
        timer->Stop();
        timer->Start();
      },
      MonoDelta::FromMilliseconds(period_ms_),
      std::move(opts));

  // Run the timer for a fixed amount of time.
  timer->Start();
  SleepFor(MonoDelta::FromMilliseconds(period_ms_ * kPeriods));
  timer->Stop();

  // Although the timer is restarted by its functor, its overall period should
  // remain more or less the same (since the period expired just as the functor
  // ran). As such, we should see no more than three callbacks per period:
  // one to start scheduling the callback loop, one when it fires, and one more
  // after it has been replaced by a new callback loop.
  ASSERT_LE(timer->NumCallbacksForTests(), kPeriods * 3);
}

class JitteredOneShotPeriodicTimerTest : public JitteredPeriodicTimerTest {
 protected:
  virtual PeriodicTimer::Options GetOptions() override {
    PeriodicTimer::Options opts;
    opts.jitter_pct = GetParam();
    opts.one_shot = true;
    return opts;
  }
};

INSTANTIATE_TEST_CASE_P(AllJitterModes,
                        JitteredOneShotPeriodicTimerTest,
                        ::testing::Values(0.0, 0.25));

TEST_P(JitteredOneShotPeriodicTimerTest, TestBasics) {
  // Kick off the one-shot timer a few times.
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(i, counter_);

    // Eventually the task will run.
    timer_->Start();
    ASSERT_EVENTUALLY([&](){
      ASSERT_EQ(i + 1, counter_);
    });

    // Even if we explicitly wait another few periods, the counter value
    // shouldn't change.
    SleepFor(MonoDelta::FromMilliseconds(period_ms_ * 2));
    ASSERT_EQ(i + 1, counter_);
  }
}

TEST_F(PeriodicTimerTest, TestCallbackRestartsOneShotTimer) {
  atomic<int64_t> counter(0);
  shared_ptr<Messenger> messenger;
  ASSERT_OK(MessengerBuilder("test")
            .Build(&messenger));

  // Create a timer that restarts itself from within its functor.
  PeriodicTimer::Options opts;
  opts.jitter_pct = 0.0; // don't need jittering
  opts.one_shot = true;
  shared_ptr<PeriodicTimer> timer = PeriodicTimer::Create(
      messenger,
      [&] {
        counter++;
        timer->Start();
      },
      MonoDelta::FromMilliseconds(period_ms_),
      std::move(opts));

  // Because the timer restarts itself every time the functor runs, we
  // should see the counter value increase with each period.
  timer->Start();
  ASSERT_EVENTUALLY([&](){
    ASSERT_GE(counter, 5);
  });

  // Ensure that the reactor threads are fully quiesced (and thus no timer
  // callbacks are running) by the time 'counter' is destroyed.
  messenger->Shutdown();
}

TEST_F(PeriodicTimerTest, TestPerformance) {
  const int kNumTimers = 1000;
  shared_ptr<Messenger> messenger;
  ASSERT_OK(MessengerBuilder("test")
            .set_num_reactors(1)
            .Build(&messenger));
  SCOPED_CLEANUP({ messenger->Shutdown(); });

  vector<shared_ptr<PeriodicTimer>> timers;
  for (int i = 0; i < kNumTimers; i++) {
    timers.emplace_back(PeriodicTimer::Create(
        messenger,
        [&] {}, // No-op.
        MonoDelta::FromMilliseconds(10)));
    timers.back()->Start();
  }

  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  SleepFor(MonoDelta::FromSeconds(1));
  sw.stop();
  LOG(INFO) << "User CPU for running " << kNumTimers << " timers for 1 second: "
            << sw.elapsed().user_cpu_seconds() << "s";

  for (auto& t : timers) {
    t->Stop();
  }

}

} // namespace rpc
} // namespace kudu
