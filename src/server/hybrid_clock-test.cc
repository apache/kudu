// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "server/hybrid_clock.h"
#include "util/monotime.h"
#include "util/test_util.h"

DECLARE_int32(max_clock_sync_error_usec);

namespace kudu {
namespace server {

class HybridClockTest : public KuduTest {
 public:
  HybridClockTest()
      : clock_(new HybridClock) {
  }

  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    // set the synchronization requirement higher since test servers/local dev machines
    // might be quite unsynchronized.
    FLAGS_max_clock_sync_error_usec = 5000000;

    ASSERT_STATUS_OK(clock_->Init());
  }

 protected:
  scoped_refptr<HybridClock> clock_;
};

// Test that two subsequent time reads are monotonically increasing.
TEST_F(HybridClockTest, TestNow_ValuesIncreaseMonotonically) {
  const Timestamp now1 = clock_->Now();
  const Timestamp now2 = clock_->Now();
  ASSERT_GE(HybridClock::GetLogicalValue(now1), HybridClock::GetLogicalValue(now2));
  ASSERT_GE(HybridClock::GetPhysicalValue(now1), HybridClock::GetPhysicalValue(now1));
}

// Tests the clock updates with the incoming value if it is higher.
TEST_F(HybridClockTest, TestUpdate_LogicalValueIncreasesByAmount) {
  Timestamp now = clock_->Now();
  uint64_t now_micros = HybridClock::GetPhysicalValue(now);

  // increase the logical value
  uint64_t logical = HybridClock::GetLogicalValue(now);
  logical += 10;

  // increase the physical value so that we're sure the clock will take this
  // one, 200 msecs should be more than enough.
  now_micros += 200000;

  Timestamp now_increased = HybridClock::TimestampFromMicrosecondsAndLogicalValue(now_micros,
                                                                                  logical);

  ASSERT_STATUS_OK(clock_->Update(now_increased));

  Timestamp now2 = clock_->Now();
  ASSERT_EQ(logical + 1, HybridClock::GetLogicalValue(now2));
  ASSERT_EQ(HybridClock::GetPhysicalValue(now) + 200000, HybridClock::GetPhysicalValue(now2));
}

// Test that the incoming event is in the past, i.e. less than now - max_error
TEST_F(HybridClockTest, TestWaitUntilAfter_TestCase1) {

  MonoTime before = MonoTime::Now(MonoTime::FINE);

  Timestamp past_ts;
  uint64_t max_error;
  clock_->NowWithError(&past_ts, &max_error);

  // make the event 3 * the max. possible error in the past
  Timestamp past_ts_changed = HybridClock::AddPhysicalTimeToTimestamp(past_ts, -3 * max_error);

  Timestamp current_ts;
  uint64_t current_max_error;
  clock_->NowWithError(&current_ts, &current_max_error);

  Status s = clock_->WaitUntilAfter(past_ts_changed);

  ASSERT_STATUS_OK(s);

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
  Timestamp wait_until = HybridClock::AddPhysicalTimeToTimestamp(past_ts, past_max_error);

  Timestamp current_ts;
  uint64_t current_max_error;
  clock_->NowWithError(&current_ts, &current_max_error);
  ASSERT_STATUS_OK(clock_->WaitUntilAfter(wait_until));

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
    HybridClock::GetPhysicalValue(ts1) + 1 * 1000 * 1000);
  ASSERT_STATUS_OK(clock_->Update(now_increased));
  Timestamp ts2 = clock_->Now();

  ASSERT_TRUE(clock_->IsAfter(ts1));
  ASSERT_TRUE(clock_->IsAfter(ts2));
}

}  // namespace server
}  // namespace kudu
