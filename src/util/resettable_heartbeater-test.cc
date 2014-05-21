// Copyright (c) 2014, Cloudera, inc.

#include "util/resettable_heartbeater.h"

#include <boost/bind/bind.hpp>
#include <boost/thread/locks.hpp>
#include <gtest/gtest.h>
#include <string>

#include "gutil/gscoped_ptr.h"
#include "util/locks.h"
#include "util/monotime.h"
#include "util/status.h"
#include "util/test_util.h"

namespace kudu {

static const int64_t kSleepPeriodMsecs = 250;

class ResettableHeartbeaterTest : public KuduTest {
 protected:
  void CreateHeartbeater(const MonoDelta period, const std::string& name) {
    heartbeater_.reset(
        new ResettableHeartbeater(name,
                                  period,
                                  boost::bind(&ResettableHeartbeaterTest::HeartbeatFunction,
                                              this)));
    boost::lock_guard<simple_spinlock> lock(lock_);
    heartbeats_ = 0;
  }

  Status HeartbeatFunction() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    heartbeats_++;
    return Status::OK();
  }

  gscoped_ptr<ResettableHeartbeater> heartbeater_;

  int heartbeats_;
  mutable simple_spinlock lock_;
};

// Tests that if Reset() is not called the heartbeat method is called
// the expected number of times.
TEST_F(ResettableHeartbeaterTest, TestRegularHeartbeats) {
  CreateHeartbeater(MonoDelta::FromMilliseconds(kSleepPeriodMsecs), "test-1");
  ASSERT_STATUS_OK(heartbeater_->Start());
  usleep(kSleepPeriodMsecs * 4 * 1000);
  ASSERT_STATUS_OK(heartbeater_->Stop());
  boost::lock_guard<simple_spinlock> lock(lock_);
  ASSERT_GE(heartbeats_, 3);
  ASSERT_LE(heartbeats_, 4);
}

// Tests that if we Reset() the heartbeater in a period smaller than
// the heartbeat period the heartbeat method never gets called.
// After we stop resetting heartbeats should resume as normal
TEST_F(ResettableHeartbeaterTest, TestResetHeartbeats) {
  CreateHeartbeater(MonoDelta::FromMilliseconds(kSleepPeriodMsecs), "test-1");
  ASSERT_STATUS_OK(heartbeater_->Start());
  for (int i = 0; i < 10; i++) {
    usleep(kSleepPeriodMsecs / 4);
    heartbeater_->Reset();
  }
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    ASSERT_EQ(heartbeats_, 0);
  }
  usleep(kSleepPeriodMsecs * 4 * 1000);
  ASSERT_STATUS_OK(heartbeater_->Stop());
  {
    boost::lock_guard<simple_spinlock> lock(lock_);
    ASSERT_GE(heartbeats_, 3);
    ASSERT_LE(heartbeats_, 4);
  }
}

}  // namespace kudu
