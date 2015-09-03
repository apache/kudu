// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/server/hybrid_clock.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace server {

class HybridClockTest : public KuduTest {
 public:
  HybridClockTest()
      : clock_(new HybridClock) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    ASSERT_OK(clock_->Init());
  }

 protected:
  scoped_refptr<HybridClock> clock_;
};

// Test that two subsequent time reads are monotonically increasing.
TEST_F(HybridClockTest, TestNow_ValuesIncreaseMonotonically) {
  const Timestamp now1 = clock_->Now();
  const Timestamp now2 = clock_->Now();
  ASSERT_GE(HybridClock::GetLogicalValue(now1), HybridClock::GetLogicalValue(now2));
  ASSERT_GE(HybridClock::GetPhysicalValueMicros(now1),
            HybridClock::GetPhysicalValueMicros(now1));
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
  MonoTime before = MonoTime::Now(MonoTime::FINE);

  Timestamp past_ts;
  uint64_t max_error;
  clock_->NowWithError(&past_ts, &max_error);

  // make the event 3 * the max. possible error in the past
  Timestamp past_ts_changed = HybridClock::AddPhysicalTimeToTimestamp(
      past_ts,
      MonoDelta::FromMicroseconds(-3 * max_error));

  Timestamp current_ts;
  uint64_t current_max_error;
  clock_->NowWithError(&current_ts, &current_max_error);

  Status s = clock_->WaitUntilAfter(past_ts_changed, no_deadline);

  ASSERT_OK(s);

  MonoTime after = MonoTime::Now(MonoTime::FINE);
  MonoDelta delta = after.GetDeltaSince(before);
  // Actually this should be close to 0, but we are sure it can't be bigger than
  // current_ts.physical_ts.max_error_usec
  ASSERT_LT(delta.ToMicroseconds(), current_max_error);
}

// The normal case for transactions. Obtain a timestamp and then wait until
// we're sure that tx_latest < now_earliest.
TEST_F(HybridClockTest, TestWaitUntilAfter_TestCase2) {
  MonoTime before = MonoTime::Now(MonoTime::FINE);

  // we do no time adjustment, this event should fall right within the possible
  // error interval
  Timestamp past_ts;
  uint64_t past_max_error;
  clock_->NowWithError(&past_ts, &past_max_error);
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
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Wait with a deadline well in the future. This should succeed.
  {
    MonoTime deadline = before;
    deadline.AddDelta(MonoDelta::FromSeconds(60));
    ASSERT_OK(clock_->WaitUntilAfter(wait_until, deadline));
  }

  MonoTime after = MonoTime::Now(MonoTime::FINE);
  MonoDelta delta = after.GetDeltaSince(before);

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
  Timestamp prev(0);;
  while (!stop->Load()) {
    Timestamp t = clock->Now();
    CHECK_GT(t.value(), prev.value());
    prev = t;

    // Add a random bit of offset to the clock, and perform an update.
    Timestamp new_ts = HybridClock::AddPhysicalTimeToTimestamp(
        t, MonoDelta::FromMicroseconds(rng.Uniform(10000)));
    clock->Update(new_ts);
  }
}

// Regression test for KUDU-953: if threads are updating and polling the
// clock concurrently, the clock should still never run backwards.
TEST_F(HybridClockTest, TestClockDoesntGoBackwardsWithUpdates) {
  vector<scoped_refptr<kudu::Thread> > threads;

  AtomicBool stop(false);
  for (int i = 0; i < 4; i++) {
    scoped_refptr<Thread> thread;
    ASSERT_OK(Thread::Create("test", "stresser",
                             &StresserThread, clock_.get(), &stop,
                             &thread));
    threads.push_back(thread);
  }

  SleepFor(MonoDelta::FromSeconds(1));
  stop.Store(true);
  BOOST_FOREACH(const scoped_refptr<Thread> t, threads) {
    t->Join();
  }
}

}  // namespace server
}  // namespace kudu
