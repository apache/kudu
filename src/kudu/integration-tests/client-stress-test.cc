// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/test_util.h"

namespace kudu {

class ClientStressTest : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    cluster_.reset(new ExternalMiniCluster(opts));
    ASSERT_OK(cluster_->Start());
  }

  virtual void TearDown() OVERRIDE {
    cluster_->Shutdown();
    KuduTest::TearDown();
  }
 protected:
  gscoped_ptr<ExternalMiniCluster> cluster_;
};


// Stress test a case where most of the operations are expected to time out.
// This is a regression test for KUDU-614 - it would cause a deadlock prior
// to fixing that bug.
TEST_F(ClientStressTest, TestLookupTimeouts) {
  const int kSleepMillis = AllowSlowTests() ? 5000 : 100;

  // Set a timeout for the expected time plus a slack of 30 seconds, in case
  // it takes a while to elect leaders, etc. This test has caused deadlocks
  // in the past, but also generates lots of logs while it's running,
  // so an explicit timeout makes sense to avoid generating GBs of logs on
  // failure.
  alarm(kSleepMillis / 1000 + 30);

  TestWorkload work(cluster_.get());
  work.set_num_write_threads(64);
  work.set_write_timeout_millis(10);
  work.set_timeout_allowed(true);
  work.Setup();
  work.Start();
  SleepFor(MonoDelta::FromMilliseconds(kSleepMillis));
  work.StopAndJoin();

  ClusterVerifier(cluster_.get()).CheckCluster();
}

} // namespace kudu
