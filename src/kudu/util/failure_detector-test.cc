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

// How often we expect a node to heartbeat to assert its "aliveness".
static const int kExpectedHeartbeatPeriodMillis = 100;

// Number of heartbeats after which the FD will consider the node dead.
static const int kMaxMissedHeartbeats = 2;

// Let's check for failures every 100ms on average +/- 10ms.
static const int kFailureMonitorMeanMillis = 100;
static const int kFailureMonitorStddevMillis = 10;

static const char* kNodeName = "node-1";
static const char* kTestTabletName = "test-tablet";

class FailureDetectorTest : public KuduTest {
 public:
  FailureDetectorTest()
    : KuduTest(),
      latch_(1),
      monitor_(new RandomizedFailureMonitor(SeedRandom(),
                                            kFailureMonitorMeanMillis,
                                            kFailureMonitorStddevMillis)) {
  }

  void FailureFunction(const std::string& name, const Status& status) {
    LOG(INFO) << "Detected failure of " << name;
    latch_.CountDown();
  }

 protected:
  void WaitForFailure() {
    latch_.Wait();
  }

  CountDownLatch latch_;
  gscoped_ptr<RandomizedFailureMonitor> monitor_;
};

// Tests that we can track a node, that while we notify that we're received messages from
// that node everything is ok and that once we stop doing so the failure detection function
// gets called.
TEST_F(FailureDetectorTest, TestDetectsFailure) {
  ASSERT_OK(monitor_->Start());

  scoped_refptr<FailureDetector> detector(new TimedFailureDetector(
      MonoDelta::FromMilliseconds(kExpectedHeartbeatPeriodMillis * kMaxMissedHeartbeats)));

  monitor_->MonitorFailureDetector(kTestTabletName, detector);
  ASSERT_FALSE(detector->IsTracking(kNodeName));
  ASSERT_OK(detector->Track(kNodeName,
                            MonoTime::Now(MonoTime::FINE),
                            Bind(&FailureDetectorTest::FailureFunction, Unretained(this))));
  ASSERT_TRUE(detector->IsTracking(kNodeName));

  const int kNumPeriodsToWait = 4;  // Num heartbeat periods to wait for a failure.
  const int kUpdatesPerPeriod = 10; // Num updates we give per period to minimize test flakiness.

  for (int i = 0; i < kNumPeriodsToWait * kUpdatesPerPeriod; i++) {
    // Report in (heartbeat) to the detector.
    ASSERT_OK(detector->MessageFrom(kNodeName, MonoTime::Now(MonoTime::FINE)));

    // We sleep for a fraction of heartbeat period, to minimize test flakiness.
    SleepFor(MonoDelta::FromMilliseconds(kExpectedHeartbeatPeriodMillis / kUpdatesPerPeriod));

    // The latch shouldn't have counted down, since the node's been reporting that
    // it's still alive.
    ASSERT_EQ(1, latch_.count());
  }

  // If we stop reporting he node is alive the failure callback is eventually
  // triggered and we exit.
  WaitForFailure();

  ASSERT_OK(detector->UnTrack(kNodeName));
  ASSERT_FALSE(detector->IsTracking(kNodeName));

  ASSERT_OK(monitor_->UnmonitorFailureDetector(kTestTabletName));
  monitor_->Shutdown();
}

}  // namespace kudu
