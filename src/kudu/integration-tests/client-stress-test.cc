// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign/list_of.hpp>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pstack_watcher.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_counter(leader_memory_pressure_rejections);
METRIC_DECLARE_counter(follower_memory_pressure_rejections);

using strings::Substitute;

namespace kudu {

class ClientStressTest : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();

    ExternalMiniClusterOptions opts = default_opts();
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

  virtual ExternalMiniClusterOptions default_opts() const {
    return ExternalMiniClusterOptions();
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
}


// Override the base test to start a cluster with a low memory limit.
class ClientStressTest_LowMemory : public ClientStressTest {
 protected:
  virtual ExternalMiniClusterOptions default_opts() const OVERRIDE {
    // There's nothing scientific about this number; it must be low enough to
    // trigger memory pressure request rejection yet high enough for the
    // servers to make forward progress.
    const int kMemLimitMB = 64;
    ExternalMiniClusterOptions opts;
    opts.extra_tserver_flags.push_back(Substitute(
        "--memory_limit_hard_mb=$0", kMemLimitMB));
    opts.extra_tserver_flags.push_back(
        "--memory_limit_soft_percentage=0");
    return opts;
  }
};

// Stress test where, due to absurdly low memory limits, many client requests
// are rejected, forcing the client to retry repeatedly.
TEST_F(ClientStressTest_LowMemory, TestMemoryThrottling) {
  const int64_t kMinRejections = 100;
  const MonoDelta kMaxWaitTime = MonoDelta::FromSeconds(60);

  TestWorkload work(cluster_.get());
  work.Setup();
  work.Start();

  // Wait until we've rejected some number of requests.
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(kMaxWaitTime);
  while (true) {
    int64_t total_num_rejections = 0;

    // It can take some time for the tablets (and their metric entities) to
    // appear on every server. Rather than explicitly wait for that above,
    // we'll just treat the lack of a metric as non-fatal. If the entity
    // or metric is truly missing, we'll eventually timeout and fail.
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      int64_t value;
      Status s = cluster_->tablet_server(i)->GetInt64Metric(
          &METRIC_ENTITY_tablet,
          NULL,
          &METRIC_leader_memory_pressure_rejections,
          "value",
          &value);
      if (!s.IsNotFound()) {
        ASSERT_OK(s);
        total_num_rejections += value;
      }
      s = cluster_->tablet_server(i)->GetInt64Metric(
          &METRIC_ENTITY_tablet,
          NULL,
          &METRIC_follower_memory_pressure_rejections,
          "value",
          &value);
      if (!s.IsNotFound()) {
        ASSERT_OK(s);
        total_num_rejections += value;
      }
    }
    if (total_num_rejections >= kMinRejections) {
      break;
    } else if (deadline.ComesBefore(MonoTime::Now(MonoTime::FINE))) {
      FAIL() << "Ran for " << kMaxWaitTime.ToString() << ", deadline expired";
    }
    SleepFor(MonoDelta::FromMilliseconds(200));
  }
}

} // namespace kudu
