// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "server/hybrid_clock.h"
#include "util/monotime.h"
#include "util/test_util.h"

DECLARE_int32(max_clock_sync_error_usec);
DECLARE_bool(require_synchronized_clocks);

namespace kudu {
namespace server {

class HybridClockTest : public KuduTest {
 public:
  HybridClockTest()
      : clock_(new HybridClock) {
  }

  virtual void SetUp() {
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

  Timestamp past_ts;
  uint64_t max_error;
  clock_->NowWithError(&past_ts, &max_error);

  // make the event 3 * the max. possible error in the past
  uint64_t past_ts_phys = HybridClock::GetPhysicalValue(past_ts) - 3 * max_error;
  past_ts = HybridClock::TimestampFromMicroseconds(past_ts_phys);

  MonoTime before = MonoTime::Now(MonoTime::FINE);
  Timestamp current_ts;
  uint64_t current_max_error;
  clock_->NowWithError(&current_ts, &current_max_error);

  Status s = clock_->WaitUntilAfter(past_ts);

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

  // we do no time adjustment, this event should fall right within the possible
  // error interval
  Timestamp past_ts_latest = clock_->NowLatest();

  MonoTime before = MonoTime::Now(MonoTime::FINE);
  Timestamp current_ts;
  uint64_t max_error;
  clock_->NowWithError(&current_ts, &max_error);
  ASSERT_STATUS_OK(clock_->WaitUntilAfter(past_ts_latest));

  MonoTime after = MonoTime::Now(MonoTime::FINE);
  MonoDelta delta = after.GetDeltaSince(before);
  // should have waited 2 * max_error
  ASSERT_GE(delta.ToMicroseconds(), 2 * max_error);
}

// Test that we don't wait unbound amounts of time. If an event is too much into
// the future (outside our possible interval) we trim if to the the latest
// possible within our interval.
TEST_F(HybridClockTest, TestWaitUntilAfter_TestIncomingEventIsInTheFuture) {

  Timestamp future_ts;
  uint64_t max_error;
  clock_->NowWithError(&future_ts, &max_error);

  // make the event 3* the max. possible error in the future
  uint64_t future_ts_phys = HybridClock::GetPhysicalValue(future_ts);
  future_ts_phys = future_ts_phys + 3 * max_error;
  future_ts = HybridClock::TimestampFromMicroseconds(future_ts_phys);

  MonoTime before = MonoTime::Now(MonoTime::FINE);
  ASSERT_STATUS_OK(clock_->WaitUntilAfter(future_ts));

  MonoTime after = MonoTime::Now(MonoTime::FINE);
  MonoDelta delta = after.GetDeltaSince(before);

  // Make sure we didn't wait more that 3 times the error.
  ASSERT_LT(delta.ToMicroseconds(), 3 * max_error);
}

}  // namespace server
}  // namespace kudu
