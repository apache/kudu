// Copyright (c) 2013, Cloudera, inc.

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <string>
#include <tr1/memory>

#include "master/master.h"
#include "master/mini_master.h"
#include "master/ts_descriptor.h"
#include "master/ts_manager.h"
#include "gutil/gscoped_ptr.h"
#include "tserver/mini_tablet_server.h"
#include "util/test_util.h"
#include "util/stopwatch.h"

DECLARE_int32(heartbeat_interval_ms);

namespace kudu {

using std::vector;
using std::tr1::shared_ptr;
using tserver::MiniTabletServer;
using master::MiniMaster;

static const double kRegistrationWaitTimeSeconds = 5.0;

// Tests for the Tablet Server registering with the Master,
// and the master maintaining the tablet descriptor.
class RegistrationTest : public KuduTest {
 public:
  virtual void SetUp() {
    // TODO: much of this will probably eventually be refactored into
    // a MiniCluster class.

    // Make heartbeats faster to speed test runtime.
    FLAGS_heartbeat_interval_ms = 10;

    KuduTest::SetUp();
    master_.reset(new MiniMaster(env_.get(), GetTestPath("master-root")));
    ASSERT_STATUS_OK(master_->Start());

    ts_.reset(new MiniTabletServer(env_.get(), GetTestPath("ts-root")));
    ts_->options()->master_hostport = HostPort(master_->bound_rpc_addr());
    ASSERT_STATUS_OK(ts_->Start());
  }

  virtual void TearDown() {
    ASSERT_STATUS_OK(ts_->Shutdown());
    ASSERT_STATUS_OK(master_->Shutdown());
  }

 protected:
  // Wait until the master has registered at least one tablet server.
  // If no TS registers within kRegistrationWaitTimeSeconds, fails the test.
  void WaitForRegistration() {
    Stopwatch sw;
    sw.start();
    while (sw.elapsed().wall_seconds() < kRegistrationWaitTimeSeconds) {
      vector<shared_ptr<master::TSDescriptor> > descs;
      master_->master()->ts_manager()->GetAllDescriptors(&descs);
      if (!descs.empty()) {
        LOG(INFO) << "TS registered with Master after " << sw.elapsed().wall_seconds() << "s";
        return;
      }
      usleep(1 * 1000); // 1ms
    }
    FAIL() << "TS never registered with master";
  }

  gscoped_ptr<MiniTabletServer> ts_;
  gscoped_ptr<MiniMaster> master_;
};

TEST_F(RegistrationTest, TestTSRegisters) {
  ASSERT_NO_FATAL_FAILURE(WaitForRegistration());

  // Restart the master, so it loses the descriptor, and ensure that the
  // hearbeater thread handles re-registering.
  ASSERT_STATUS_OK(master_->Restart());

  ASSERT_NO_FATAL_FAILURE(WaitForRegistration());

  // TODO: when the instance ID / sequence number stuff is implemented,
  // restart the TS and ensure that it re-registers with the newer sequence
  // number.
}

} // namespace kudu
