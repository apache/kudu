// Copyright (c) 2014, Cloudera, inc.

#include "util/resettable_heartbeater.h"

#include <boost/bind/bind.hpp>
#include <boost/thread/locks.hpp>
#include <gtest/gtest.h>
#include <string>

#include "gutil/gscoped_ptr.h"
#include "util/countdown_latch.h"
#include "util/locks.h"
#include "util/monotime.h"
#include "util/status.h"
#include "util/test_util.h"

namespace kudu {

static const int64_t kSleepPeriodMsecs = 100;
static const int kNumPeriodsToWait = 3;
// Wait triple the required time before we time out, should be enough to avoid test flakiness.
static const uint64_t kMaxWaitMsecs = kSleepPeriodMsecs * kNumPeriodsToWait * 3;

class ResettableHeartbeaterTest : public KuduTest {
 public:
  ResettableHeartbeaterTest()
    : KuduTest(),
      latch_(kNumPeriodsToWait) {
  }

 protected:
  void CreateHeartbeater(const MonoDelta period, const std::string& name) {
    heartbeater_.reset(
        new ResettableHeartbeater(name,
                                  period,
                                  boost::bind(&ResettableHeartbeaterTest::HeartbeatFunction,
                                              this)));
  }

  Status HeartbeatFunction() {
    latch_.CountDown();
    return Status::OK();
  }

  void WaitForCountDown() {
    CHECK(latch_.TimedWait(boost::posix_time::milliseconds(kMaxWaitMsecs)))
        << "Failed to count down " << kNumPeriodsToWait << " times in " << kMaxWaitMsecs
        << " ms: latch count == " << latch_.count();
  }

  CountDownLatch latch_;
  gscoped_ptr<ResettableHeartbeater> heartbeater_;
};

// Tests that if Reset() is not called the heartbeat method is called
// the expected number of times.
TEST_F(ResettableHeartbeaterTest, TestRegularHeartbeats) {
  CreateHeartbeater(MonoDelta::FromMilliseconds(kSleepPeriodMsecs), CURRENT_TEST_NAME());
  ASSERT_STATUS_OK(heartbeater_->Start());
  WaitForCountDown();
  ASSERT_STATUS_OK(heartbeater_->Stop());
}

// Tests that if we Reset() the heartbeater in a period smaller than
// the heartbeat period the heartbeat method never gets called.
// After we stop resetting heartbeats should resume as normal
TEST_F(ResettableHeartbeaterTest, TestResetHeartbeats) {
  CreateHeartbeater(MonoDelta::FromMilliseconds(kSleepPeriodMsecs), CURRENT_TEST_NAME());
  ASSERT_STATUS_OK(heartbeater_->Start());
  for (int i = 0; i < 10; i++) {
    usleep(kSleepPeriodMsecs / 4 * 1000);
    heartbeater_->Reset();
    ASSERT_EQ(kNumPeriodsToWait, latch_.count());
  }
  WaitForCountDown();
  ASSERT_STATUS_OK(heartbeater_->Stop());
}

}  // namespace kudu
