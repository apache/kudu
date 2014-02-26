// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "tablet/logical_clock.h"
#include "util/monotime.h"
#include "util/test_util.h"

namespace kudu {
namespace tablet {

class LogicalClockTest : public KuduTest {
 public:
  LogicalClockTest()
      : clock_(new LogicalClock(Timestamp::kInitialTimestamp.value())) {
  }

 protected:
  scoped_refptr<LogicalClock> clock_;
};

// Test that two subsequent time reads are monotonically increasing.
TEST_F(LogicalClockTest, TestNow_ValuesIncreaseMonotonically) {
  const Timestamp now1 = clock_->Now();
  const Timestamp now2 = clock_->Now();
  ASSERT_EQ(now1.value() + 1, now2.value());
}

// Tests that the clock gets updated if the incoming value is higher.
TEST_F(LogicalClockTest, TestUpdate_LogicalValueIncreasesByAmount) {
  Timestamp initial = clock_->Now();
  Timestamp future(initial.value() + 10);
  clock_->Update(future);
  Timestamp now = clock_->Now();
  // now should be 1 after future
  ASSERT_EQ(initial.value() + 11, now.value());
}

// Tests that the clock doesn't get updated if the incoming value is lower.
TEST_F(LogicalClockTest, TestUpdate_LogicalValueDoesNotIncrease) {
  Timestamp ts(0);
  // update the clock to 0 should do nothing
  clock_->Update(ts);
  Timestamp now = clock_->Now();
  ASSERT_EQ(now.value(), 2);
}

TEST_F(LogicalClockTest, TestWaitUntilAfterIsUnavailable) {
  Status status = clock_->WaitUntilAfter(Timestamp(10));
  ASSERT_TRUE(status.IsServiceUnavailable());
}

}  // namespace tablet
}  // namespace kudu

