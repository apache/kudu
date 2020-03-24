// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(server);
METRIC_DECLARE_histogram(
    handler_latency_kudu_tserver_TabletCopyService_BeginTabletCopySession);

using kudu::master::ChangeTServerStateRequestPB;
using kudu::master::ChangeTServerStateResponsePB;
using kudu::cluster::ExternalDaemon;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::ExternalTabletServer;
using kudu::cluster::LocationInfo;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::HealthReportPB;
using kudu::consensus::IncludeHealthReport;
using kudu::itest::GetInt64Metric;
using kudu::master::MasterServiceProxy;
using kudu::master::TServerStateChangePB;
using kudu::tools::RunActionPrependStdoutStderr;
using std::function;
using std::pair;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

typedef pair<unordered_map<string, TServerDetails*>, unique_ptr<ValueDeleter>> MapAndDeleter;
static const vector<string> kTServerFlags = {
  // Set a low unavailability timeout so replicas are considered failed and can
  // be re-replicated more quickly.
  "--raft_heartbeat_interval_ms=100",
  "--follower_unavailable_considered_failed_sec=2",
  // Disable log GC in case our write workloads lead to eviction because
  // consensus will consider replicas that are too far behind unrecoverable
  // and will evict them regardless of maintenance mode.
  "--enable_log_gc=false",
};
static const MonoDelta kDurationForSomeHeartbeats = MonoDelta::FromSeconds(3);

class MaintenanceModeITest : public ExternalMiniClusterITestBase {
 public:
  void SetUpCluster(int num_tservers) {
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = num_tservers;
    opts.extra_tserver_flags = kTServerFlags;
    NO_FATALS(StartClusterWithOpts(std::move(opts)));
    const auto& addr = cluster_->master(0)->bound_rpc_addr();
    m_proxy_.reset(new MasterServiceProxy(cluster_->messenger(), addr, addr.host()));
  }

  // Perform the given state change on the given tablet server.
  Status ChangeTServerState(const string& uuid, TServerStateChangePB::StateChange change) {
    ChangeTServerStateRequestPB req;
    ChangeTServerStateResponsePB resp;
    TServerStateChangePB* state_change = req.mutable_change();
    state_change->set_uuid(uuid);
    state_change->set_change(change);
    rpc::RpcController rpc;
    return cluster_->master_proxy()->ChangeTServerState(req, &resp, &rpc);
  }

  // Checks whether tablet copies have started.
  void ExpectStartedTabletCopies(bool should_have_started) {
    bool has_started = false;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      ExternalTabletServer* tserver = cluster_->tablet_server(i);
      if (tserver->IsShutdown()) {
        continue;
      }
      int64_t copies_started = 0;
      ASSERT_OK(GetInt64Metric(tserver->bound_http_hostport(), &METRIC_ENTITY_server,
          /*entity_id=*/nullptr,
          &METRIC_handler_latency_kudu_tserver_TabletCopyService_BeginTabletCopySession,
          "total_count", &copies_started));
      if (copies_started > 0) {
        has_started = true;
        break;
      }
    }
    ASSERT_EQ(should_have_started, has_started);
  }

  // Return the number of failed replicas there are in the cluster, according
  // to the tablet leaders.
  Status GetReplicaHealthCounts(const unordered_map<string, TServerDetails*>& ts_map,
                                int* num_replicas_failed, int* num_replicas_healthy) {
    int num_failed = 0;
    int num_healthy = 0;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      ExternalTabletServer* tserver = cluster_->tablet_server(i);
      if (tserver->IsShutdown()) {
        continue;
      }
      const string& uuid = tserver->uuid();
      const TServerDetails* ts_details = FindOrDie(ts_map, uuid);
      vector<string> tablet_ids;
      RETURN_NOT_OK(ListRunningTabletIds(ts_details, MonoDelta::FromSeconds(30), &tablet_ids));
      for (const auto& tablet_id : tablet_ids) {
        ConsensusStatePB consensus_state;
        RETURN_NOT_OK(GetConsensusState(ts_details, tablet_id, MonoDelta::FromSeconds(30),
            IncludeHealthReport::INCLUDE_HEALTH_REPORT, &consensus_state));
        // Only consider the health states reported by the leaders.
        if (consensus_state.leader_uuid() != uuid) {
          continue;
        }
        // Go through all the peers and tally up any that are failed.
        const auto& committed_config = consensus_state.committed_config();
        for (int p = 0; p < committed_config.peers_size(); p++) {
          const auto& peer = committed_config.peers(p);
          if (peer.has_health_report()) {
            switch (peer.health_report().overall_health()) {
              case HealthReportPB::FAILED:
                num_failed++;
                break;
              case HealthReportPB::HEALTHY:
                num_healthy++;
                break;
              default:
                continue;
            }
          }
        }
      }
    }
    *num_replicas_failed = num_failed;
    *num_replicas_healthy = num_healthy;
    return Status::OK();
  }

  // Asserts that the ts_map will eventually have the given number of failed and healthy replicas
  // across the cluster.
  void AssertEventuallyHealthCounts(const unordered_map<string, TServerDetails*>& ts_map,
                                    int expected_failed, int expected_healthy) {
    ASSERT_EVENTUALLY([&] {
      int num_failed;
      int num_healthy;
      ASSERT_OK(GetReplicaHealthCounts(ts_map, &num_failed, &num_healthy));
      ASSERT_EQ(expected_failed, num_failed);
      ASSERT_EQ(expected_healthy, num_healthy);
    });
  }

  void GenerateTServerMap(MapAndDeleter* map_and_deleter) {
    unordered_map<string, TServerDetails*> ts_map;
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(CreateTabletServerMap(m_proxy_, cluster_->messenger(), &ts_map));
      auto cleanup = MakeScopedCleanup([&] {
        STLDeleteValues(&ts_map);
      });
      ASSERT_EQ(cluster_->num_tablet_servers(), ts_map.size());
      cleanup.cancel();
    });
    map_and_deleter->first = std::move(ts_map);
    map_and_deleter->second.reset(new ValueDeleter(&map_and_deleter->first));
  }

 protected:
  shared_ptr<MasterServiceProxy> m_proxy_;
};

class MaintenanceModeRF3ITest : public MaintenanceModeITest {
 public:
  void SetUp() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    NO_FATALS(MaintenanceModeITest::SetUp());
    NO_FATALS(SetUpCluster(3));
  }
  void TearDown() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    NO_FATALS(MaintenanceModeITest::TearDown());
  }
};

// Test that placing a tablet server in maintenance mode leads to the failed
// replicas on that server not being re-replicated.
TEST_F(MaintenanceModeRF3ITest, TestFailedTServerInMaintenanceModeDoesntRereplicate) {
  // This test will sleep a bit to ensure the master has had some time to
  // receive heartbeats.
  SKIP_IF_SLOW_NOT_ALLOWED();
  const int kNumTablets = 6;
  const int total_replicas = kNumTablets * 3;

  // Create the table with three tablet servers and then add one so we're
  // guaranteed that the replicas are all on the first three servers.
  // Restarting our only master may lead to network errors and timeouts, but
  // that shouldn't matter w.r.t maintenance mode.
  TestWorkload create_table(cluster_.get());
  create_table.set_num_tablets(kNumTablets);
  create_table.set_network_error_allowed(true);
  create_table.set_timeout_allowed(true);
  create_table.Setup();
  create_table.Start();
  // Add a server so there's one we could move to after bringing down a
  // tserver.
  ASSERT_OK(cluster_->AddTabletServer());
  MapAndDeleter ts_map_and_deleter;
  NO_FATALS(GenerateTServerMap(&ts_map_and_deleter));
  const auto& ts_map = ts_map_and_deleter.first;

  // Do a sanity check that all our replicas are healthy.
  NO_FATALS(AssertEventuallyHealthCounts(ts_map, 0, total_replicas));

  // Put one of the servers in maintenance mode.
  ExternalTabletServer* maintenance_ts = cluster_->tablet_server(0);
  const string maintenance_uuid = maintenance_ts->uuid();
  ASSERT_OK(ChangeTServerState(maintenance_uuid, TServerStateChangePB::ENTER_MAINTENANCE_MODE));

  // Bringing the tablet server down shouldn't lead to re-replication.
  //
  // Note: it's possible to set up re-replication scenarios in other ways (e.g.
  // by hitting an IO error, or falling too far behind when replicating); these
  // should all be treated the same way by virtue of them all using the same
  // health reporting.
  NO_FATALS(maintenance_ts->Shutdown());

  // Now wait a bit for this failure to make its way to the master. The failure
  // shouldn't have led to any re-replication.
  SleepFor(kDurationForSomeHeartbeats);
  NO_FATALS(AssertEventuallyHealthCounts(ts_map, kNumTablets, total_replicas - kNumTablets));
  NO_FATALS(ExpectStartedTabletCopies(/*should_have_started*/false));

  // Restarting the masters shouldn't lead to re-replication either, even
  // though the tablet server is still down.
  NO_FATALS(cluster_->ShutdownNodes(cluster::ClusterNodes::MASTERS_ONLY));
  ASSERT_OK(cluster_->master()->Restart());
  SleepFor(kDurationForSomeHeartbeats);
  NO_FATALS(ExpectStartedTabletCopies(/*should_have_started*/false));

  // Now bring the server back up and wait for it to become healthy. It should
  // be able to do this without tablet copies.
  ASSERT_OK(maintenance_ts->Restart());
  NO_FATALS(AssertEventuallyHealthCounts(ts_map, 0, total_replicas));

  // Since our server is healthy, leaving maintenance mode shouldn't trigger
  // any re-replication either.
  ASSERT_OK(ChangeTServerState(maintenance_uuid, TServerStateChangePB::EXIT_MAINTENANCE_MODE));
  SleepFor(kDurationForSomeHeartbeats);
  NO_FATALS(ExpectStartedTabletCopies(/*should_have_started*/false));

  // Now set maintenance mode, bring the tablet server down, and then exit
  // maintenance mode without bringing the tablet server back up. This should
  // result in tablet copies.
  ASSERT_OK(ChangeTServerState(maintenance_uuid, TServerStateChangePB::ENTER_MAINTENANCE_MODE));
  NO_FATALS(maintenance_ts->Shutdown());
  NO_FATALS(AssertEventuallyHealthCounts(ts_map, kNumTablets, total_replicas - kNumTablets));
  ASSERT_OK(ChangeTServerState(maintenance_uuid, TServerStateChangePB::EXIT_MAINTENANCE_MODE));
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(ExpectStartedTabletCopies(/*should_have_started*/true));
  });

  // All the while, our workload should not have been interrupted. Assert
  // eventually to wait for the rows to converge.
  NO_FATALS(create_table.StopAndJoin());
  ClusterVerifier v(cluster_.get());
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(v.CheckRowCount(create_table.table_name(),
                              ClusterVerifier::EXACTLY, create_table.rows_inserted()));
  });
}

TEST_F(MaintenanceModeRF3ITest, TestMaintenanceModeDoesntObstructMove) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  TestWorkload create_table(cluster_.get());
  create_table.set_num_tablets(1);
  create_table.Setup();

  // Add a tablet server.
  ASSERT_OK(cluster_->AddTabletServer());
  const string& added_uuid = cluster_->tablet_server(3)->uuid();
  MapAndDeleter ts_map_and_deleter;
  NO_FATALS(GenerateTServerMap(&ts_map_and_deleter));
  const auto& ts_map = ts_map_and_deleter.first;

  // Put a tablet server into maintenance mode.
  const string maintenance_uuid = cluster_->tablet_server(0)->uuid();
  const TServerDetails* maintenance_details = FindOrDie(ts_map, maintenance_uuid);
  vector<string> mnt_tablet_ids;
  ASSERT_OK(ListRunningTabletIds(maintenance_details, MonoDelta::FromSeconds(30), &mnt_tablet_ids));
  ASSERT_EQ(1, mnt_tablet_ids.size());

  ASSERT_OK(ChangeTServerState(maintenance_uuid, TServerStateChangePB::ENTER_MAINTENANCE_MODE));

  // While the maintenance mode tserver is still online, move a tablet from it.
  // This should succeed, because maintenance mode will not obstruct manual
  // movement of replicas.
  ASSERT_OK(RunActionPrependStdoutStderr(Substitute(
      "tablet change_config move_replica $0 $1 $2 $3",
      cluster_->master()->bound_rpc_addr().ToString(),
      mnt_tablet_ids[0],
      maintenance_uuid,
      added_uuid)));
  const TServerDetails* added_details = FindOrDie(ts_map, added_uuid);
  ASSERT_EVENTUALLY([&] {
    vector<string> added_tablet_ids;
    ASSERT_OK(ListRunningTabletIds(added_details, MonoDelta::FromSeconds(30), &added_tablet_ids));
    ASSERT_EQ(1, added_tablet_ids.size());
  });
}

// Test that the health state FAILED_UNRECOVERABLE (e.g. if there's a disk
// error, or if a replica is lagging too much) is still re-replicated during
// maintenance mode.
TEST_F(MaintenanceModeRF3ITest, TestMaintenanceModeDoesntObstructFailedUnrecoverable) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  TestWorkload create_table(cluster_.get());
  create_table.set_num_tablets(1);
  create_table.Setup();
  create_table.Start();

  // Add a tablet server.
  ASSERT_OK(cluster_->AddTabletServer());
  const string& added_uuid = cluster_->tablet_server(3)->uuid();
  MapAndDeleter ts_map_and_deleter;
  NO_FATALS(GenerateTServerMap(&ts_map_and_deleter));
  const auto& ts_map = ts_map_and_deleter.first;

  // Put a tablet server into maintenance mode.
  ExternalDaemon* maintenance_ts = cluster_->tablet_server(0);
  const string maintenance_uuid = maintenance_ts->uuid();
  const TServerDetails* maintenance_details = FindOrDie(ts_map, maintenance_uuid);
  vector<string> mnt_tablet_ids;
  ASSERT_OK(ListRunningTabletIds(maintenance_details, MonoDelta::FromSeconds(30), &mnt_tablet_ids));
  ASSERT_OK(ChangeTServerState(maintenance_uuid, TServerStateChangePB::ENTER_MAINTENANCE_MODE));

  // Now fail the tablet on the server in maintenance mode by injecting a disk
  // error. Also speed up flushes so we actually hit an IO error.
  ASSERT_OK(cluster_->SetFlag(maintenance_ts, "flush_threshold_secs", "1"));
  ASSERT_OK(cluster_->SetFlag(maintenance_ts, "env_inject_eio_globs",
      JoinPathSegments(maintenance_ts->data_dirs()[0], "**")));
  ASSERT_OK(cluster_->SetFlag(maintenance_ts, "env_inject_eio", "1"));

  // Eventually the disk failure will be noted and a copy will be made at the
  // added server.
  const TServerDetails* added_details = FindOrDie(ts_map, added_uuid);
  ASSERT_EVENTUALLY([&] {
    vector<string> added_tablet_ids;
    ASSERT_OK(ListRunningTabletIds(added_details, MonoDelta::FromSeconds(30), &added_tablet_ids));
    ASSERT_EQ(1, added_tablet_ids.size());
  });
  NO_FATALS(create_table.StopAndJoin());
  ClusterVerifier v(cluster_.get());
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(v.CheckRowCount(create_table.table_name(),
                              ClusterVerifier::EXACTLY, create_table.rows_inserted()));
  });
}

class MaintenanceModeRF5ITest : public MaintenanceModeITest {
 public:
  void SetUp() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    NO_FATALS(MaintenanceModeITest::SetUp());
    NO_FATALS(SetUpCluster(5));
  }
  void TearDown() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    NO_FATALS(MaintenanceModeITest::TearDown());
  }
};

// Test that a table with RF=5 will still be available through the failure of
// two nodes if one is put in maintenance mode.
TEST_F(MaintenanceModeRF5ITest, TestBackgroundFailureDuringMaintenanceMode) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  // Create some tables with RF=5.
  const int kNumTablets = 3;
  const int total_replicas = kNumTablets * 5;
  TestWorkload create_table(cluster_.get());
  create_table.set_num_tablets(kNumTablets);
  create_table.set_num_replicas(5);
  create_table.Setup();
  create_table.Start();

  // Add a server so we have one empty server to replicate to.
  ASSERT_OK(cluster_->AddTabletServer());
  MapAndDeleter ts_map_and_deleter;
  NO_FATALS(GenerateTServerMap(&ts_map_and_deleter));
  const auto& ts_map = ts_map_and_deleter.first;

  // Do a sanity check that all our replicas are healthy.
  NO_FATALS(AssertEventuallyHealthCounts(ts_map, 0, total_replicas));

  // Enter maintenance mode on a tserver and shut it down.
  ExternalTabletServer* maintenance_ts = cluster_->tablet_server(0);
  const string maintenance_uuid = maintenance_ts->uuid();
  ASSERT_OK(ChangeTServerState(maintenance_uuid, TServerStateChangePB::ENTER_MAINTENANCE_MODE));
  NO_FATALS(maintenance_ts->Shutdown());
  SleepFor(kDurationForSomeHeartbeats);

  // Wait for the failure to be recognized by the other replicas.
  NO_FATALS(AssertEventuallyHealthCounts(ts_map, kNumTablets, total_replicas - kNumTablets));
  NO_FATALS(ExpectStartedTabletCopies(/*should_have_started*/false));

  // Now kill another server. We should be able to see some copies.
  NO_FATALS(cluster_->tablet_server(1)->Shutdown());
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(ExpectStartedTabletCopies(/*should_have_started*/true));
  });
  // Eventually we'll be left with just the unresponsive maintenance mode
  // failed replicas.
  NO_FATALS(AssertEventuallyHealthCounts(ts_map, kNumTablets, total_replicas - kNumTablets));
  // The previously empty tablet server should hold all the replicas that were
  // re-replicated.
  const TServerDetails* added_details = FindOrDie(ts_map, cluster_->tablet_server(5)->uuid());
  ASSERT_EVENTUALLY([&] {
    vector<string> added_tablet_ids;
    ASSERT_OK(ListRunningTabletIds(added_details, MonoDelta::FromSeconds(30), &added_tablet_ids));
    ASSERT_EQ(kNumTablets, added_tablet_ids.size());
  });
  // Now exit maintenance mode and restart the maintenance tserver. The
  // original tablets on the maintenance mode tserver should still exist.
  ASSERT_OK(ChangeTServerState(maintenance_uuid, TServerStateChangePB::EXIT_MAINTENANCE_MODE));
  ASSERT_OK(maintenance_ts->Restart());
  const TServerDetails* maintenance_details = FindOrDie(ts_map, maintenance_uuid);
  vector<string> mnt_tablet_ids;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(ListRunningTabletIds(
        maintenance_details, MonoDelta::FromSeconds(30), &mnt_tablet_ids));
    ASSERT_EQ(kNumTablets, mnt_tablet_ids.size());
  });

  // All the while, our workload should not have been interrupted. Assert
  // eventually to wait for the rows to converge.
  NO_FATALS(create_table.StopAndJoin());
  ClusterVerifier v(cluster_.get());
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(v.CheckRowCount(create_table.table_name(),
                              ClusterVerifier::EXACTLY, create_table.rows_inserted()));
  });
}

namespace {

// Performs the given tasks in parallel, returning an error if any of them
// return a non-OK Status.
Status DoInParallel(vector<function<Status()>> tasks,
                    const string& task_description) {
  int num_in_parallel = tasks.size();
  vector<Status> results(num_in_parallel);
  vector<thread> threads;
  for (int i = 0; i < num_in_parallel; i++) {
    threads.emplace_back([&, i] {
      Status s = tasks[i]();
      if (PREDICT_FALSE(!s.ok())) {
        results[i] = s;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  bool has_errors = false;
  for (int i = 0; i < num_in_parallel; i++) {
    const auto& s = results[i];
    if (!s.ok()) {
      LOG(ERROR) << s.ToString();
      has_errors = true;
    }
  }
  if (has_errors) {
    return Status::IllegalState(
        Substitute("errors while running $0 $1 tasks in parallel",
                    num_in_parallel, task_description));
  }
  return Status::OK();
}

// Repeats 'cmd' until it succeeds, with the given timeout and interval.
Status RunUntilSuccess(const string& cmd, int timeout_secs, int repeat_interval_secs) {
  Status s;
  const MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(timeout_secs);
  const MonoDelta retry_interval = MonoDelta::FromSeconds(repeat_interval_secs);
  while (true) {
    s = RunActionPrependStdoutStderr(cmd);
    if (s.ok()) {
      return Status::OK();
    }
    if (MonoTime::Now() + retry_interval > deadline) {
      return Status::TimedOut(
          Substitute("Running '$0' did not succeed in $1 seconds: $2",
                      cmd, timeout_secs, s.ToString()));
    }
    SleepFor(retry_interval);
  }
  return Status::OK();
}

// Generates locations such that there are a total of 'num_tservers' tablet
// servers spread as evenly as possible across 'num_locs' locations.
ExternalMiniClusterOptions GenerateOpts(int num_tservers, int num_locs) {
  int tservers_per_loc = num_tservers / num_locs;
  int tservers_with_one_more = num_tservers % num_locs;
  CHECK_LT(0, tservers_per_loc);
  LocationInfo locations;
  for (int l = 0; l < num_locs; l++) {
    string loc = Substitute("/L$0", l);
    EmplaceOrDie(&locations, Substitute("/L$0", l),
        tservers_with_one_more < l ? tservers_per_loc + 1 : tservers_per_loc);
  }

  ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = num_tservers;
  opts.extra_master_flags = {
    // Don't bother assigning locations to clients; it's hard to do that
    // correctly with external mini clusters.
    "--master_client_location_assignment_enabled=false",
  };
  opts.extra_tserver_flags = {
    // To speed up leadership transfers from quiescing, let's make heartbeats
    // more frequent.
    "--raft_heartbeat_interval_ms=100",
    // Let's emulate long-running scans by setting a low scan batch size.
    "--scanner_default_batch_size_bytes=100",
  };
  opts.location_info = std::move(locations);
  return opts;
}

struct RollingRestartTestArgs {
  // Cluster opts.
  ExternalMiniClusterOptions opts;
  // Upper bound on the number of tablet servers to restart at the same time.
  int batch_size;
  // Replication to use for the test workload.
  int num_replicas;
  // Whether the rolling restart should fail.
  bool restart_fails;
};

// Convenience builder for more readable test composition.
class ArgsBuilder {
 public:
  ArgsBuilder& batch_size(int batch_size) {
    args_.batch_size = batch_size;
    return *this;
  }
  ArgsBuilder& num_locations(int num_locations) {
    num_locations_ = num_locations;
    return *this;
  }
  ArgsBuilder& num_replicas(int num_replicas) {
    args_.num_replicas = num_replicas;
    return *this;
  }
  ArgsBuilder& num_tservers(int num_tservers) {
    num_tservers_ = num_tservers;
    return *this;
  }
  ArgsBuilder& restart_fails(bool fails) {
    args_.restart_fails = fails;
    return *this;
  }
  RollingRestartTestArgs build() {
    args_.opts = GenerateOpts(num_tservers_, num_locations_);
    return args_;
  }
 private:
  RollingRestartTestArgs args_;
  int num_tservers_ = 0;
  int num_locations_ = 0;
};

} // anonymous namespace

class RollingRestartITest : public MaintenanceModeITest,
                            public ::testing::WithParamInterface<RollingRestartTestArgs> {
 public:
  void SetUp() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    NO_FATALS(MaintenanceModeITest::SetUp());
    const auto& args = GetParam();
    ExternalMiniClusterOptions opts = args.opts;
    NO_FATALS(StartClusterWithOpts(std::move(opts)));
    const auto& addr = cluster_->master(0)->bound_rpc_addr();
    m_proxy_.reset(new MasterServiceProxy(cluster_->messenger(), addr, addr.host()));
    NO_FATALS(GenerateTServerMap(&ts_map_and_deleter_));
  }
  void TearDown() override {
    SKIP_IF_SLOW_NOT_ALLOWED();
    NO_FATALS(MaintenanceModeITest::TearDown());
  }

  // Create a read-write workload that doesn't use a fault-tolerant scanner.
  unique_ptr<TestWorkload> CreateFaultIntolerantRWWorkload(int num_replicas) {
    unique_ptr<TestWorkload> rw_workload(new TestWorkload(cluster_.get()));
    rw_workload->set_scanner_fault_tolerant(false);
    rw_workload->set_num_replicas(num_replicas);
    rw_workload->set_num_read_threads(3);
    rw_workload->set_num_write_threads(3);
    rw_workload->set_verify_num_rows(false);
    return rw_workload;
  }

  // Returns a list of batches of tablet server UUIDs to restart in parallel,
  // each of size at most 'batch_size'. Batches are generated within each
  // location.
  vector<vector<string>> GetRestartBatches(int batch_size) {
    unordered_map<string, vector<string>> cur_tservers_by_loc;
    vector<vector<string>> restart_batches;
    for (const auto& ts_and_details : ts_map_and_deleter_.first) {
      const auto& uuid = ts_and_details.first;
      const auto& loc = ts_and_details.second->location;
      auto& cur_batch = LookupOrInsert(&cur_tservers_by_loc, loc, {});
      cur_batch.emplace_back(uuid);
      // If we've reached our desired batch size for this location, put it in
      // the return set.
      if (cur_batch.size() >= batch_size) {
        restart_batches.emplace_back(std::move(cur_batch));
        CHECK_EQ(1, cur_tservers_by_loc.erase(loc));
      }
    }
    // Create batches out of the remaining, suboptimally-sized batches.
    for (auto& ts_and_batch : cur_tservers_by_loc) {
      restart_batches.emplace_back(std::move(ts_and_batch.second));
    }
    return restart_batches;
  }

  // Takes the list of tablet servers and restarts them in parallel in such a
  // way that shouldn't affect on-going workloads.
  Status RollingRestartTServers(const vector<string>& ts_to_restart) {
    // Begin maintenance mode on the servers so we'll stop assigning replicas
    // to them, and begin quiescing them so the server itself stops accepting
    // more work.
    vector<function<Status()>> setup_rr_tasks;
    for (const auto& ts_id : ts_to_restart) {
      setup_rr_tasks.emplace_back([&, ts_id] {
        RETURN_NOT_OK(RunActionPrependStdoutStderr(
            Substitute("tserver state enter_maintenance $0 $1",
                       cluster_->master()->bound_rpc_addr().ToString(), ts_id)));
        return RunUntilSuccess(
            Substitute("tserver quiesce start $0 --error_if_not_fully_quiesced",
                       cluster_->tablet_server_by_uuid(ts_id)->bound_rpc_addr().ToString()),
            /*timeout_secs*/30, /*repeat_interval_secs*/1);
      });
    }
    RETURN_NOT_OK(DoInParallel(std::move(setup_rr_tasks), "rolling restart setup"));

    // Restart the tservers. Note: we don't do this in parallel because
    // wrangling multiple processes from different threads is messy.
    for (const auto& ts_id : ts_to_restart) {
      ExternalTabletServer* ts = cluster_->tablet_server_by_uuid(ts_id);
      ts->Shutdown();
      RETURN_NOT_OK(ts->Restart());
    }

    // Wait for ksck to become healthy.
    RETURN_NOT_OK_PREPEND(RunUntilSuccess(
        Substitute("cluster ksck $0", cluster_->master()->bound_rpc_addr().ToString()),
        /*timeout_secs*/60, /*repeat_interval_secs*/5),
        "cluster didn't become healthy");

    // Now clean up persistent state.
    vector<function<Status()>> cleanup_tasks;
    for (const auto& ts_id : ts_to_restart) {
      cleanup_tasks.emplace_back([&, ts_id] {
        return RunActionPrependStdoutStderr(
            Substitute("tserver state exit_maintenance $0 $1",
                       cluster_->master()->bound_rpc_addr().ToString(),
                       ts_id));

      });
    }
    return DoInParallel(std::move(cleanup_tasks), "rolling restart cleanup");
  }

 protected:
  MapAndDeleter ts_map_and_deleter_;
};

TEST_P(RollingRestartITest, TestWorkloads) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  const auto& args = GetParam();
  unique_ptr<TestWorkload> rw = CreateFaultIntolerantRWWorkload(args.num_replicas);
  rw->set_read_timeout_millis(10000);
  // If we're expecting the rolling restart to fail, e.g. because we can't
  // fully quiesce our servers, chances are our workload can't complete either
  // because too many servers are quiescing and none can serve scans or writes.
  rw->set_read_errors_allowed(args.restart_fails);
  rw->set_timeout_allowed(args.restart_fails);
  rw->Setup();
  rw->Start();
  vector<vector<string>> restart_batches = GetRestartBatches(args.batch_size);
  for (const auto& batch : restart_batches) {
    LOG(INFO) << Substitute("Restarting batch of $0 tservers: $1",
                            batch.size(), JoinStrings(batch, ","));
    if (args.restart_fails) {
      ASSERT_EVENTUALLY([&] {
        Status s = RollingRestartTServers(batch);
        ASSERT_FALSE(s.ok());
      });
    } else {
      ASSERT_OK(RollingRestartTServers(batch));
    }
  }
  NO_FATALS(rw->StopAndJoin());
  if (args.restart_fails) {
    ASSERT_FALSE(rw->read_errors().empty());
  }
}

INSTANTIATE_TEST_CASE_P(RollingRestartArgs, RollingRestartITest, ::testing::Values(
    // Basic RF=3 case.
    ArgsBuilder().num_tservers(4)
                 .num_locations(1)
                 .batch_size(1)
                 .num_replicas(3)
                 .restart_fails(false)
                 .build(),
    // Basic RF=5 case. The larger replication factor lets us increase the
    // restart batch size.
    ArgsBuilder().num_tservers(6)
                 .num_locations(1)
                 .batch_size(2)
                 .num_replicas(5)
                 .restart_fails(false)
                 .build(),
    // RF=3 case with location awareness. The location awareness lets us
    // increase the restart batch size.
    ArgsBuilder().num_tservers(6)
                 .num_locations(3)
                 .batch_size(2)
                 .num_replicas(3)
                 .restart_fails(false)
                 .build(),
    // RF=3 case with location awareness, but with an even larger batch size.
    ArgsBuilder().num_tservers(9)
                 .num_locations(3)
                 .batch_size(3)
                 .num_replicas(3)
                 .restart_fails(false)
                 .build(),
    // Basic RF=3 case, but with too large a batch size. With too large a batch
    // size, the tablet servers won't be able to fully relinquish leadership
    // and quiesce; the restart process should thus fail.
    ArgsBuilder().num_tservers(4)
                 .num_locations(1)
                 .batch_size(4)
                 .num_replicas(3)
                 .restart_fails(true)
                 .build(),
    // The same goes for the case with a single replica.
    ArgsBuilder().num_tservers(1)
                 .num_locations(1)
                 .batch_size(1)
                 .num_replicas(1)
                 .restart_fails(true)
                 .build()
));

} // namespace itest
} // namespace kudu
