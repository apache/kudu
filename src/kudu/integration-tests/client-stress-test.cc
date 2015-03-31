// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>

#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/pstack_watcher.h"
#include "kudu/util/test_util.h"

namespace kudu {

class ClientStressTest : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    if (multi_master()) {
      opts.num_masters = 3;
      opts.master_rpc_ports = boost::assign::list_of(11010)(11011)(11012);
    }
    opts.num_tablet_servers = 3;
    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_OK(cluster_->Start());
  }

  virtual void TearDown() OVERRIDE {
    alarm(0);
    cluster_->Shutdown();
    KuduTest::TearDown();
  }

 protected:
  virtual bool multi_master() const {
    return false;
  }

  gscoped_ptr<ExternalMiniCluster> cluster_;
};

// Stress test a case where most of the operations are expected to time out.
// This is a regression test for various bugs we've seen in timeout handling,
// especially with concurrent requests.
TEST_F(ClientStressTest, TestLookupTimeouts) {
  const int kSleepMillis = AllowSlowTests() ? 5000 : 100;

  TestWorkload work(cluster_.get());
  work.set_num_write_threads(64);
  work.set_write_timeout_millis(10);
  work.set_timeout_allowed(true);
  work.Setup();
  work.Start();
  SleepFor(MonoDelta::FromMilliseconds(kSleepMillis));
  work.StopAndJoin();
}

// Override the base test to run in multi-master mode.
class ClientStressTest_MultiMaster : public ClientStressTest {
 protected:
  virtual bool multi_master() const OVERRIDE {
    return true;
  }
};

// Stress test a case where most of the operations are expected to time out.
// This is a regression test for KUDU-614 - it would cause a deadlock prior
// to fixing that bug.
TEST_F(ClientStressTest_MultiMaster, TestLeaderResolutionTimeout) {
  TestWorkload work(cluster_.get());
  work.set_num_write_threads(64);

  // This timeout gets applied to the master requests. It's lower than the
  // amount of time that we sleep the masters, to ensure they timeout.
  work.set_client_default_rpc_timeout_millis(250);
  // This is the time budget for the whole request. It has to be longer than
  // the above timeout so that the client actually attempts to resolve
  // the leader.
  work.set_write_timeout_millis(280);
  work.set_timeout_allowed(true);
  work.Setup();

  work.Start();

  cluster_->tablet_server(0)->Pause();
  cluster_->tablet_server(1)->Pause();
  cluster_->tablet_server(2)->Pause();
  cluster_->master(0)->Pause();
  cluster_->master(1)->Pause();
  cluster_->master(2)->Pause();
  SleepFor(MonoDelta::FromMilliseconds(300));
  cluster_->tablet_server(0)->Resume();
  cluster_->tablet_server(1)->Resume();
  cluster_->tablet_server(2)->Resume();
  cluster_->master(0)->Resume();
  cluster_->master(1)->Resume();
  cluster_->master(2)->Resume();
  SleepFor(MonoDelta::FromMilliseconds(100));

  // Set an explicit timeout. This test has caused deadlocks in the past.
  // Also make sure to dump stacks before the alarm goes off.
  PstackWatcher watcher(MonoDelta::FromSeconds(30));
  alarm(60);
  work.StopAndJoin();
}

} // namespace kudu
