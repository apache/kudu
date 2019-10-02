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
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
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
#include "kudu/util/path_util.h"
#include "kudu/util/net/sockaddr.h"
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
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::HealthReportPB;
using kudu::consensus::IncludeHealthReport;
using kudu::itest::GetInt64Metric;
using kudu::master::MasterServiceProxy;
using kudu::master::TServerStateChangePB;
using kudu::tools::RunKuduTool;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace itest {

typedef pair<unordered_map<string, TServerDetails*>, unique_ptr<ValueDeleter>> MapAndDeleter;
static const vector<string> kTServerFlags = {
  // Set a low unavailability timeout so replicas are considered failed and can
  // be re-replicated more quickly.
  "--raft_heartbeat_interval_ms=100",
  "--follower_unavailable_considered_failed_sec=2",
  // Disable log GC in case our write workloads lead to eviction because
  // consensus will consider replicas that are too fare behind unrecoverable
  // and will evict them regardless of maintenance mode.
  "--enable_log_gc=false",
};
static const MonoDelta kDurationForSomeHeartbeats = MonoDelta::FromSeconds(3);

class MaintenanceModeITest : public ExternalMiniClusterITestBase {
 public:
  void SetUpCluster(int num_tservers) {
    ExternalMiniClusterOptions opts;
    opts.num_tablet_servers = num_tservers;
    opts.extra_master_flags = { "--master_support_maintenance_mode=true" };
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
  Status GetNumFailedReplicas(const unordered_map<string, TServerDetails*>& ts_map,
                              int* num_replicas_failed) {
    int num_failed = 0;
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
          if (peer.has_health_report() &&
              peer.health_report().overall_health() == HealthReportPB::FAILED) {
            num_failed++;
          }
        }
      }
    }
    *num_replicas_failed = num_failed;
    return Status::OK();
  }

  void AssertEventuallyNumFailedReplicas(const unordered_map<string, TServerDetails*>& ts_map,
                                         int expected_failed) {
    ASSERT_EVENTUALLY([&] {
      int num_failed;
      ASSERT_OK(GetNumFailedReplicas(ts_map, &num_failed));
      ASSERT_EQ(expected_failed, num_failed);
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
  NO_FATALS(AssertEventuallyNumFailedReplicas(ts_map, 0));

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
  NO_FATALS(AssertEventuallyNumFailedReplicas(ts_map, kNumTablets));
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
  NO_FATALS(AssertEventuallyNumFailedReplicas(ts_map, 0));

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
  NO_FATALS(AssertEventuallyNumFailedReplicas(ts_map, kNumTablets));
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
  {
    vector<string> move_cmd = {
      "tablet",
      "change_config",
      "move_replica",
      cluster_->master()->bound_rpc_addr().ToString(),
      mnt_tablet_ids[0],
      maintenance_uuid,
      added_uuid,
    };
    string stdout, stderr;
    ASSERT_OK(RunKuduTool(move_cmd, &stdout, &stderr));
  }
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
  NO_FATALS(AssertEventuallyNumFailedReplicas(ts_map, 0));

  // Enter maintenance mode on a tserver and shut it down.
  ExternalTabletServer* maintenance_ts = cluster_->tablet_server(0);
  const string maintenance_uuid = maintenance_ts->uuid();
  ASSERT_OK(ChangeTServerState(maintenance_uuid, TServerStateChangePB::ENTER_MAINTENANCE_MODE));
  NO_FATALS(maintenance_ts->Shutdown());
  SleepFor(kDurationForSomeHeartbeats);

  // Wait for the failure to be recognized by the other replicas.
  NO_FATALS(AssertEventuallyNumFailedReplicas(ts_map, kNumTablets));
  NO_FATALS(ExpectStartedTabletCopies(/*should_have_started*/false));

  // Now kill another server. We should be able to see some copies.
  NO_FATALS(cluster_->tablet_server(1)->Shutdown());
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(ExpectStartedTabletCopies(/*should_have_started*/true));
  });
  NO_FATALS(AssertEventuallyNumFailedReplicas(ts_map, 0));
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
  SleepFor(kDurationForSomeHeartbeats);
  const TServerDetails* maintenance_details = FindOrDie(ts_map, maintenance_uuid);
  vector<string> mnt_tablet_ids;
  ASSERT_OK(ListRunningTabletIds(maintenance_details, MonoDelta::FromSeconds(30), &mnt_tablet_ids));
  ASSERT_EQ(kNumTablets, mnt_tablet_ids.size());

  // All the while, our workload should not have been interrupted. Assert
  // eventually to wait for the rows to converge.
  NO_FATALS(create_table.StopAndJoin());
  ClusterVerifier v(cluster_.get());
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(v.CheckRowCount(create_table.table_name(),
                              ClusterVerifier::EXACTLY, create_table.rows_inserted()));
  });
}

} // namespace itest
} // namespace kudu
