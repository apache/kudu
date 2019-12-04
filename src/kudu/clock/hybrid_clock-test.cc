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

#include "kudu/clock/hybrid_clock.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/mock_ntp.h"
#include "kudu/clock/time_service.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/util/atomic.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"


DECLARE_bool(inject_unsync_time_errors);
DECLARE_string(time_source);

using std::string;
using std::vector;

namespace kudu {
namespace clock {

class HybridClockTest : public KuduTest {
 public:
  HybridClockTest()
      : clock_(new HybridClock) {
  }

  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(clock_->Init());
  }

 protected:
  scoped_refptr<HybridClock> clock_;
};

clock::MockNtp* mock_ntp(HybridClock* clock) {
  return down_cast<clock::MockNtp*>(clock->time_service());
}

TEST(MockHybridClockTest, TestMockedSystemClock) {
  google::FlagSaver saver;
  FLAGS_time_source = "mock";
  scoped_refptr<HybridClock> clock(new HybridClock);
  ASSERT_OK(clock->Init());
  Timestamp timestamp;
  uint64_t max_error_usec;
  clock->NowWithError(&timestamp, &max_error_usec);
  ASSERT_EQ(timestamp.ToUint64(), 0);
  ASSERT_EQ(max_error_usec, 0);
  // If we read the clock again we should see the logical component be incremented.
  clock->NowWithError(&timestamp, &max_error_usec);
  ASSERT_EQ(timestamp.ToUint64(), 1);
  // Now set an arbitrary time and check that is the time returned by the clock.
  uint64_t time = 1234 * 1000;
  uint64_t error = 100 * 1000;
  mock_ntp(clock.get())->SetMockClockWallTimeForTests(time);
  mock_ntp(clock.get())->SetMockMaxClockErrorForTests(error);
  clock->NowWithError(&timestamp, &max_error_usec);
  ASSERT_EQ(timestamp.ToUint64(),
            HybridClock::TimestampFromMicrosecondsAndLogicalValue(time, 0).ToUint64());
  ASSERT_EQ(max_error_usec, error);
  // Perform another read, we should observe the logical component increment, again.
  clock->NowWithError(&timestamp, &max_error_usec);
  ASSERT_EQ(timestamp.ToUint64(),
            HybridClock::TimestampFromMicrosecondsAndLogicalValue(time, 1).ToUint64());
}

// Test that, if the rate at which the clock is read is greater than the maximum
// resolution of the logical counter (12 bits in our implementation), it properly
// "overflows" into the physical portion of the clock, and maintains all ordering
// guarantees even as the physical clock continues to increase.
//
// This is a regression test for KUDU-1345.
TEST(MockHybridClockTest, TestClockDealsWithWrapping) {
  google::FlagSaver saver;
  FLAGS_time_source = "mock";
  scoped_refptr<HybridClock> clock(new HybridClock);
  ASSERT_OK(clock->Init());
  mock_ntp(clock.get())->SetMockClockWallTimeForTests(1000);

  Timestamp prev = clock->Now();

  // Update the clock from 10us in the future
  ASSERT_OK(clock->Update(HybridClock::TimestampFromMicroseconds(1010)));

  // Now read the clock value enough times so that the logical value wraps
  // over, and should increment the _physical_ portion of the clock.
  for (int i = 0; i < 10000; i++) {
    Timestamp now = clock->Now();
    ASSERT_GT(now.value(), prev.value());
    prev = now;
  }
  ASSERT_EQ(1012, HybridClock::GetPhysicalValueMicros(prev));

  // Advance the time microsecond by microsecond, and ensure the clock never
  // goes backwards.
  for (int time = 1001; time < 1020; time++) {
    mock_ntp(clock.get())->SetMockClockWallTimeForTests(time);
    Timestamp now = clock->Now();

    // Clock should run strictly forwards.
    ASSERT_GT(now.value(), prev.value());

    // Additionally, once the physical time surpasses the logical time, we should
    // be running on the physical clock. Otherwise, we should stick with the physical
    // time we had rolled forward to above.
    if (time > 1012) {
      ASSERT_EQ(time, HybridClock::GetPhysicalValueMicros(now));
    } else {
      ASSERT_EQ(1012, HybridClock::GetPhysicalValueMicros(now));
    }

    prev = now;
  }
}

// Test that two subsequent time reads are monotonically increasing.
TEST_F(HybridClockTest, TestNow_ValuesIncreaseMonotonically) {
  const Timestamp now1 = clock_->Now();
  const Timestamp now2 = clock_->Now();
  ASSERT_LT(now1.value(), now2.value());
}

// Tests the clock updates with the incoming value if it is higher.
TEST_F(HybridClockTest, TestUpdate_LogicalValueIncreasesByAmount) {
  Timestamp now = clock_->Now();
  uint64_t now_micros = HybridClock::GetPhysicalValueMicros(now);

  // increase the logical value
  uint64_t logical = HybridClock::GetLogicalValue(now);
  logical += 10;

  // increase the physical value so that we're sure the clock will take this
  // one, 200 msecs should be more than enough.
  now_micros += 200000;

  Timestamp now_increased = HybridClock::TimestampFromMicrosecondsAndLogicalValue(now_micros,
                                                                                  logical);

  ASSERT_OK(clock_->Update(now_increased));

  Timestamp now2 = clock_->Now();
  ASSERT_EQ(logical + 1, HybridClock::GetLogicalValue(now2));
  ASSERT_EQ(HybridClock::GetPhysicalValueMicros(now) + 200000,
            HybridClock::GetPhysicalValueMicros(now2));
}

// Test that the incoming event is in the past, i.e. less than now - max_error
TEST_F(HybridClockTest, TestWaitUntilAfter_TestCase1) {
  MonoTime no_deadline;
  MonoTime before = MonoTime::Now();

  Timestamp past_ts;
  uint64_t max_error;
  clock_->NowWithError(&past_ts, &max_error);

  // make the event 3 * the max. possible error in the past
  Timestamp past_ts_changed = HybridClock::AddPhysicalTimeToTimestamp(
      past_ts,
      MonoDelta::FromMicroseconds(-3 * static_cast<int64_t>(max_error)));
  ASSERT_OK(clock_->WaitUntilAfter(past_ts_changed, no_deadline));

  MonoTime after = MonoTime::Now();
  MonoDelta delta = after - before;
  // The delta should be close to 0, but it takes some time for the hybrid
  // logical clock to decide that it doesn't need to wait.
  ASSERT_LT(delta.ToMicroseconds(), 25000);
}

// The normal case for transactions. Obtain a timestamp and then wait until
// we're sure that tx_latest < now_earliest.
TEST_F(HybridClockTest, TestWaitUntilAfter_TestCase2) {
  const MonoTime before = MonoTime::Now();

  // we do no time adjustment, this event should fall right within the possible
  // error interval
  Timestamp past_ts;
  uint64_t past_max_error;
  clock_->NowWithError(&past_ts, &past_max_error);
  // Make sure the error is at least a small number of microseconds, to ensure
  // that we always have to wait.
  past_max_error = std::max(past_max_error, static_cast<uint64_t>(2000));
  Timestamp wait_until = HybridClock::AddPhysicalTimeToTimestamp(
      past_ts,
      MonoDelta::FromMicroseconds(past_max_error));

  Timestamp current_ts;
  uint64_t current_max_error;
  clock_->NowWithError(&current_ts, &current_max_error);

  // Check waiting with a deadline which already expired.
  {
    MonoTime deadline = before;
    Status s = clock_->WaitUntilAfter(wait_until, deadline);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  }

  // Wait with a deadline well in the future. This should succeed.
  {
    MonoTime deadline = before + MonoDelta::FromSeconds(60);
    ASSERT_OK(clock_->WaitUntilAfter(wait_until, deadline));
  }

  MonoTime after = MonoTime::Now();
  MonoDelta delta = after - before;

  // In the common case current_max_error >= past_max_error and we should have waited
  // 2 * past_max_error, but if the clock's error is reset between the two reads we might
  // have waited less time, but always more than 'past_max_error'.
  if (current_max_error >= past_max_error) {
    ASSERT_GE(delta.ToMicroseconds(), 2 * past_max_error);
  } else {
    ASSERT_GE(delta.ToMicroseconds(), past_max_error);
  }
}

TEST_F(HybridClockTest, TestIsAfter) {
  Timestamp ts1 = clock_->Now();
  ASSERT_TRUE(clock_->IsAfter(ts1));

  // Update the clock in the future, make sure it still
  // handles "IsAfter" properly even when it's running in
  // "logical" mode.
  Timestamp now_increased = HybridClock::TimestampFromMicroseconds(
    HybridClock::GetPhysicalValueMicros(ts1) + 1 * 1000 * 1000);
  ASSERT_OK(clock_->Update(now_increased));
  Timestamp ts2 = clock_->Now();

  ASSERT_TRUE(clock_->IsAfter(ts1));
  ASSERT_TRUE(clock_->IsAfter(ts2));
}

// Thread which loops polling the clock and updating it slightly
// into the future.
void StresserThread(HybridClock* clock, AtomicBool* stop) {
  Random rng(GetRandomSeed32());
  Timestamp prev(0);
  while (!stop->Load()) {
    Timestamp t = clock->Now();
    CHECK_GT(t.value(), prev.value());
    prev = t;

    // Add a random bit of offset to the clock, and perform an update.
    Timestamp new_ts = HybridClock::AddPhysicalTimeToTimestamp(
        t, MonoDelta::FromMicroseconds(rng.Uniform(10000)));
    CHECK_OK(clock->Update(new_ts));
  }
}

// Regression test for KUDU-953: if threads are updating and polling the
// clock concurrently, the clock should still never run backwards.
TEST_F(HybridClockTest, TestClockDoesntGoBackwardsWithUpdates) {
  vector<scoped_refptr<kudu::Thread>> threads;
  AtomicBool stop(false);

  SCOPED_CLEANUP({
    stop.Store(true);
    for (const auto& t : threads) {
      t->Join();
    }
  });

  for (int i = 0; i < 4; i++) {
    scoped_refptr<Thread> thread;
    ASSERT_OK(Thread::Create("test", "stresser",
                             &StresserThread, clock_.get(), &stop,
                             &thread));
    threads.push_back(thread);
  }

  SleepFor(MonoDelta::FromSeconds(1));
}

TEST_F(HybridClockTest, TestGetPhysicalComponentDifference) {
  Timestamp now1 = HybridClock::TimestampFromMicrosecondsAndLogicalValue(100, 100);
  SleepFor(MonoDelta::FromMilliseconds(1));
  Timestamp now2 = HybridClock::TimestampFromMicrosecondsAndLogicalValue(200, 0);
  MonoDelta delta = clock_->GetPhysicalComponentDifference(now2, now1);
  MonoDelta negative_delta = clock_->GetPhysicalComponentDifference(now1, now2);
  ASSERT_EQ(100, delta.ToMicroseconds());
  ASSERT_EQ(-100, negative_delta.ToMicroseconds());
}

TEST_F(HybridClockTest, TestRideOverNtpInterruption) {
  Timestamp timestamps[3];
  uint64_t max_error_usec[3];

  // Get the clock once, with a working NTP.
  clock_->NowWithError(&timestamps[0], &max_error_usec[0]);

  // Try to read the clock again a second later, but with an error
  // injected. It should extrapolate from the first read.
  SleepFor(MonoDelta::FromSeconds(1));
  FLAGS_inject_unsync_time_errors = true;
  clock_->NowWithError(&timestamps[1], &max_error_usec[1]);

  // The new clock reading should be a second or longer from the
  // first one, since SleepFor guarantees sleeping at least as long
  // as specified.
  MonoDelta phys_diff = clock_->GetPhysicalComponentDifference(
      timestamps[1], timestamps[0]);
  ASSERT_GE(phys_diff.ToSeconds(), 1);

  // The new clock reading should have higher error than the first.
  // The error should have increased based on the clock skew.
  int64_t error_diff = max_error_usec[1] - max_error_usec[0];
  ASSERT_NEAR(error_diff, clock_->time_service()->skew_ppm() * phys_diff.ToSeconds(),
              10);

  // Now restore the ability to read the system clock, and
  // read it again.
  FLAGS_inject_unsync_time_errors = false;
  clock_->NowWithError(&timestamps[2], &max_error_usec[2]);

  ASSERT_LT(timestamps[0].ToUint64(), timestamps[1].ToUint64());
  ASSERT_LT(timestamps[1].ToUint64(), timestamps[2].ToUint64());
}

#ifndef __APPLE__
TEST_F(HybridClockTest, TestNtpDiagnostics) {
  vector<string> log;
  clock_->time_service()->DumpDiagnostics(&log);
  string s = JoinStrings(log, "\n");
  SCOPED_TRACE(s);
  ASSERT_STR_MATCHES(s, "(ntp_gettime\\(\\) returns code |chronyc -n tracking)");
  ASSERT_STR_MATCHES(s, "(ntpq -n |chronyc -n sources)");
}
#endif

}  // namespace clock
}  // namespace kudu
