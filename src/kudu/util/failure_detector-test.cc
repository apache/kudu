// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.


#include <gtest/gtest.h>
#include <string>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/failure_detector.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

namespace kudu {

// Number of heartbeats after which the FD will consider the node dead.
static const int kNumHeartbeats = 2;
static const int kHeartbeatPeriodMillis = 100;
// Let's check for failures every 100 msecs in average +- 10
static const int kFailureMonitorMedianNanos = 100 * 1000 * 1000;
static const int kFailureMonitorStddev = 10 * 1000 * 1000;
static const char* kNodeName = "a";
static const char* kTestTabletName = "t";

class FailureDetectorTest : public KuduTest {
 public:
  FailureDetectorTest()
    : KuduTest(),
      latch_(kNumHeartbeats),
      monitor_(new RandomizedFailureMonitor(kFailureMonitorMedianNanos,
                                            kFailureMonitorStddev)) {
  }

  void FailureFunction(const std::string& name, const Status& status) {
    LOG(INFO) << "Detected failure of " << name;
    latch_.CountDown();
  }
 protected:
  void WaitForCountDown() {
    latch_.Wait();
  }

  CountDownLatch latch_;
  gscoped_ptr<RandomizedFailureMonitor> monitor_;
};

// Tests that we can track a node, that while we notify that we're received messages from
// that node everything is ok and that once we stop doing so the failure detection function
// gets called.
TEST_F(FailureDetectorTest, TestDetectsFailure) {
  latch_.Reset(1);
  ASSERT_STATUS_OK(monitor_->Start());

  scoped_refptr<FailureDetector> fd(new TimedFailureDetector(
      MonoDelta::FromMilliseconds(kHeartbeatPeriodMillis * kNumHeartbeats)));

  monitor_->MonitorFailureDetector(kTestTabletName,
                                   fd);

  ASSERT_OK(fd->Track(MonoTime::Now(MonoTime::FINE),
                      kNodeName,
                      Bind(&FailureDetectorTest::FailureFunction, Unretained(this))));
  for (int i = 0; i < 10; i++) {
    fd->MessageFrom(kNodeName, MonoTime::Now(MonoTime::FINE));
    // we sleep for a heartbeat period.
    usleep(100 * 1000);
    // the latch shouldn't have counted down, since the node's been reporting that
    // it's still alive.
    ASSERT_EQ(1, latch_.count());
  }
  // If we stop reporting he node is alive the failure callback is eventually
  // triggered and we exit.
  WaitForCountDown();
  ASSERT_STATUS_OK(monitor_->Stop());
}

}  // namespace kudu
