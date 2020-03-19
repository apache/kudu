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

#include <stdint.h>

#include <cstdlib>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/table_metrics.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(allow_unsafe_replication_factor);
DECLARE_bool(catalog_manager_evict_excess_replicas);
DECLARE_bool(catalog_manager_wait_for_new_tablets_to_elect_leader);
DECLARE_bool(enable_leader_failure_detection);
DECLARE_bool(raft_prepare_replacement_before_eviction);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_int32(metrics_retirement_age_ms);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_int32(tablet_open_inject_latency_ms);
DEFINE_int32(num_election_test_loops, 3,
             "Number of random EmulateElectionForTests() loops to execute in "
             "TestReportNewLeaderOnLeaderChange");

using kudu::client::KuduClient;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::GetConsensusRole;
using kudu::consensus::HealthReportPB;
using kudu::consensus::INCLUDE_HEALTH_REPORT;
using kudu::consensus::RaftConfigPB;
using kudu::consensus::RaftConsensus;
using kudu::consensus::RaftPeerPB;
using kudu::itest::SimpleIntKeyKuduSchema;
using kudu::KuduPartialRow;
using kudu::master::CatalogManager;
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::GetTableLocationsRequestPB;
using kudu::master::Master;
using kudu::master::MasterServiceProxy;
using kudu::master::ReportedTabletPB;
using kudu::master::TableInfo;
using kudu::master::TabletReportPB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::tablet::TabletReplica;
using kudu::tserver::MiniTabletServer;
using kudu::ClusterVerifier;
using std::map;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

static const char* const kTableName = "test-table";

class TsTabletManagerITest : public KuduTest {
 public:
  TsTabletManagerITest()
      : schema_(SimpleIntKeyKuduSchema()) {
  }
  void SetUp() override {
    KuduTest::SetUp();

    MessengerBuilder bld("client");
    ASSERT_OK(bld.Build(&client_messenger_));
  }

  void StartCluster(InternalMiniClusterOptions opts) {
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

 protected:
  void DisableHeartbeatingToMaster();

  // Populate the 'replicas' container with corresponding objects representing
  // tablet replicas running at tablet servers in the test cluster. It's assumed
  // there is at least one tablet replica per tablet server. Also, this utility
  // method awaits up to the specified timeout for the consensus to be running
  // before adding an element into the output container.
  Status PrepareTabletReplicas(MonoDelta timeout,
                               vector<scoped_refptr<TabletReplica>>* replicas);

  // Generate incremental tablet reports using test-specific method
  // GenerateIncrementalTabletReportsForTests() of the specified heartbeater.
  void GetIncrementalTabletReports(Heartbeater* heartbeater,
                                   vector<TabletReportPB>* reports);

  const KuduSchema schema_;

  unique_ptr<InternalMiniCluster> cluster_;
  client::sp::shared_ptr<KuduClient> client_;
  std::shared_ptr<Messenger> client_messenger_;
};

void TsTabletManagerITest::DisableHeartbeatingToMaster() {
  for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
    MiniTabletServer* ts = cluster_->mini_tablet_server(i);
    ts->FailHeartbeats();
  }
}

Status TsTabletManagerITest::PrepareTabletReplicas(
    MonoDelta timeout, vector<scoped_refptr<TabletReplica>>* replicas) {
  const MonoTime deadline = MonoTime::Now() + timeout;
  for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
    MiniTabletServer* ts = cluster_->mini_tablet_server(i);
    vector<scoped_refptr<TabletReplica>> ts_replicas;
    // The replicas may not have been created yet, so loop until we see them.
    while (MonoTime::Now() < deadline) {
      ts->server()->tablet_manager()->GetTabletReplicas(&ts_replicas);
      if (!ts_replicas.empty()) {
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    if (ts_replicas.empty()) {
      return Status::TimedOut("waiting for tablet replicas register with ts manager");
    }
    RETURN_NOT_OK(ts_replicas.front()->WaitUntilConsensusRunning(
        deadline - MonoTime::Now()));
    replicas->insert(replicas->end(), ts_replicas.begin(), ts_replicas.end());
  }
  return Status::OK();
}

void TsTabletManagerITest::GetIncrementalTabletReports(
    Heartbeater* heartbeater, vector<TabletReportPB>* reports) {
  vector<TabletReportPB> r;
  // The MarkDirty() callback is on an async thread so it might take the
  // follower a few milliseconds to execute it. Wait for that to happen.
  ASSERT_EVENTUALLY([&] {
    r = heartbeater->GenerateIncrementalTabletReportsForTests();
    ASSERT_EQ(1, r.size());
    ASSERT_FALSE(r.front().updated_tablets().empty());
  });
  reports->swap(r);
}

class FailedTabletsAreReplacedITest :
    public TsTabletManagerITest,
    public ::testing::WithParamInterface<bool> {
};
// Test that when a tablet replica is marked as failed, it will eventually be
// evicted and replaced.
TEST_P(FailedTabletsAreReplacedITest, OneReplica) {
  const bool is_3_4_3_mode = GetParam();
  FLAGS_raft_prepare_replacement_before_eviction = is_3_4_3_mode;
  const auto kNumReplicas = 3;
  const auto kNumTabletServers = kNumReplicas + (is_3_4_3_mode ? 1 : 0);

  {
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = kNumTabletServers;
    NO_FATALS(StartCluster(std::move(opts)));
  }
  TestWorkload work(cluster_.get());
  work.set_num_replicas(kNumReplicas);
  work.Setup();
  work.Start();

  // Insert data until the tablet becomes visible to the server.
  string tablet_id;
  ASSERT_EVENTUALLY([&] {
    auto idx = rand() % kNumTabletServers;
    vector<string> tablet_ids = cluster_->mini_tablet_server(idx)->ListTablets();
    ASSERT_EQ(1, tablet_ids.size());
    tablet_id = tablet_ids[0];
  });
  work.StopAndJoin();

  // Wait until all the replicas are running before failing one arbitrarily.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());

  {
    // Inject an error into one of replicas. Shutting it down will leave it in
    // the FAILED state.
    scoped_refptr<TabletReplica> replica;
    ASSERT_EVENTUALLY([&] {
      auto idx = rand() % kNumTabletServers;
      MiniTabletServer* ts = cluster_->mini_tablet_server(idx);
      ASSERT_OK(ts->server()->tablet_manager()->GetTabletReplica(tablet_id, &replica));
    });
    replica->SetError(Status::IOError("INJECTED ERROR: tablet failed"));
    replica->Shutdown();
    ASSERT_EQ(tablet::FAILED, replica->state());
  }

  // Ensure the tablet eventually is replicated.
  NO_FATALS(v.CheckCluster());
}
INSTANTIATE_TEST_CASE_P(,
                        FailedTabletsAreReplacedITest,
                        ::testing::Bool());

class LeadershipChangeReportingTest : public TsTabletManagerITest {
 public:
  const int kNumReplicas = 2;
  void SetUp() override {
    NO_FATALS(TsTabletManagerITest::SetUp());

    // For tests that heartbeat, set a lower interval to speed things up.
    FLAGS_heartbeat_interval_ms = 100;

    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = kNumReplicas;
    NO_FATALS(StartCluster(std::move(opts)));

    // We need to control elections precisely for this test since we're using
    // EmulateElectionForTests() with a distributed consensus configuration.
    FLAGS_enable_leader_failure_detection = false;
    FLAGS_catalog_manager_wait_for_new_tablets_to_elect_leader = false;

    // Allow creating table with even replication factor.
    FLAGS_allow_unsafe_replication_factor = true;

    // Run a few more iters in slow-test mode.
    OverrideFlagForSlowTests("num_election_test_loops", "10");

    // Build a TServerDetails map so we can check for convergence.
    const auto& addr = cluster_->mini_master()->bound_rpc_addr();
    master_proxy_.reset(new MasterServiceProxy(client_messenger_, addr, addr.host()));
  }

  // Creates 'num_hashes' tablets with two replicas each.
  Status CreateTable(int num_hashes) {
    // Create the table.
    client::sp::shared_ptr<KuduTable> table;
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    if (num_hashes > 1) {
      RETURN_NOT_OK(table_creator->table_name(kTableName)
          .schema(&schema_)
          .set_range_partition_columns({ "key" })
          .num_replicas(kNumReplicas)
          .add_hash_partitions({ "key" }, num_hashes)
          .Create());
    } else {
      RETURN_NOT_OK(table_creator->table_name(kTableName)
          .schema(&schema_)
          .set_range_partition_columns({ "key" })
          .num_replicas(kNumReplicas)
          .Create());
    }
    RETURN_NOT_OK(client_->OpenTable(kTableName, &table));

    RETURN_NOT_OK(CreateTabletServerMap(master_proxy_, client_messenger_, &ts_map_));

    // Collect the TabletReplicas so we get direct access to RaftConsensus.
    RETURN_NOT_OK(PrepareTabletReplicas(MonoDelta::FromSeconds(60), &tablet_replicas_));
    CHECK_EQ(kNumReplicas * num_hashes, tablet_replicas_.size());
    return Status::OK();
  }
  void TearDown() override {
    ValueDeleter deleter(&ts_map_);
    NO_FATALS(TsTabletManagerITest::TearDown());
  }

  Status TriggerElection(int min_term = 0, int* new_leader_idx = nullptr) {
    int leader_idx = rand() % tablet_replicas_.size();
    LOG(INFO) << "Electing peer " << leader_idx << "...";
    RaftConsensus* con = CHECK_NOTNULL(tablet_replicas_[leader_idx]->consensus());
    RETURN_NOT_OK(con->EmulateElectionForTests());
    LOG(INFO) << "Waiting for servers to agree...";
    RETURN_NOT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(5), ts_map_,
        tablet_replicas_[leader_idx]->tablet_id(), min_term));
    if (new_leader_idx) {
      *new_leader_idx = leader_idx;
    }
    return Status::OK();
  }

 protected:
  shared_ptr<MasterServiceProxy> master_proxy_;
  vector<scoped_refptr<TabletReplica>> tablet_replicas_;
  itest::TabletServerMap ts_map_;
};

// Regression test for KUDU-2842: concurrent calls to GetTableLocations()
// shouldn't lead to data races with the changes in reported state.
TEST_F(LeadershipChangeReportingTest, TestConcurrentGetTableLocations) {
  // KUDU-2842 requires there to be multiple tablets in a given report, so
  // create multiple tablets.
  int kNumTablets = 2;
  ASSERT_OK(CreateTable(kNumTablets));
  CountDownLatch latch(1);
  thread t([&] {
    master::GetTableLocationsRequestPB req;
    req.mutable_table()->set_table_name(kTableName);
    req.set_intern_ts_infos_in_response(true);
    while (!latch.WaitFor(MonoDelta::FromMilliseconds(10))) {
      master::GetTableLocationsResponsePB resp;
      rpc::RpcController rpc;
      // Note: we only really care about data races, rather than the responses.
      ignore_result(master_proxy_->GetTableLocations(req, &resp, &rpc));
    }
  });
  SCOPED_CLEANUP({
    latch.CountDown();
    t.join();
  });
  for (int i = 0; i < FLAGS_num_election_test_loops; i++) {
    for (int t = 0; t < kNumTablets; t++) {
      // Note: we only really care about data races, rather than the success of
      // all the elections.
      ignore_result(TriggerElection());
    }
    SleepFor(MonoDelta::FromMilliseconds(2 * FLAGS_heartbeat_interval_ms));
  }
}

// Regression test for KUDU-2952, where a leadership change racing with the
// creation of a tablet report could lead to a non-leader report with stats.
TEST_F(LeadershipChangeReportingTest, TestReportStatsDuringLeadershipChange) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ASSERT_OK(CreateTable(1));
  // Stop heartbeating we don't race against the Master.
  DisableHeartbeatingToMaster();

  // Create a thread that just keeps triggering elections.
  CountDownLatch latch(1);
  thread t([&] {
    while (!latch.WaitFor(MonoDelta::FromMilliseconds(1))) {
      ignore_result(TriggerElection());
    }
  });
  SCOPED_CLEANUP({
    latch.CountDown(1);
    t.join();
  });

  // Generate a bunch of reports and keep checking that only the leaders have
  // stats.
  const auto deadline = MonoTime::Now() + MonoDelta::FromSeconds(10);
  while (MonoTime::Now() < deadline) {
    for (int t = 0; t < cluster_->num_tablet_servers(); t++) {
      ReportedTabletPB reported_tablet;
      MiniTabletServer* mts = cluster_->mini_tablet_server(0);
      mts->server()->tablet_manager()->CreateReportedTabletPB(tablet_replicas_[0],
                                                              &reported_tablet);
      // Only reports from leaders should have stats.
      const string& reported_leader_uuid = reported_tablet.consensus_state().leader_uuid();
      const string& mts_uuid = mts->uuid();
      if (reported_tablet.has_stats()) {
        ASSERT_EQ(reported_leader_uuid, mts_uuid);
      }
    }
  }
}

// Test that when the leader changes, the tablet manager gets notified and
// includes that information in the next tablet report.
TEST_F(LeadershipChangeReportingTest, TestUpdatedConsensusState) {
  ASSERT_OK(CreateTable(1));
  // Stop heartbeating we don't race against the Master.
  DisableHeartbeatingToMaster();

  // Loop and cause elections and term changes from different servers.
  // TSTabletManager should acknowledge the role changes via tablet reports.
  for (int i = 0; i < FLAGS_num_election_test_loops; i++) {
    SCOPED_TRACE(Substitute("Iter: $0", i));
    int new_leader_idx;
    ASSERT_OK(TriggerElection(i + 1, &new_leader_idx));

    // Now check that the tablet report reports the correct role for both servers.
    for (int replica = 0; replica < kNumReplicas; replica++) {
      vector<TabletReportPB> reports;
      NO_FATALS(GetIncrementalTabletReports(
          cluster_->mini_tablet_server(replica)->server()->heartbeater(),
          &reports));

      // Ensure that our tablet reports are consistent.
      TabletReportPB& report = reports[0];
      ASSERT_EQ(1, report.updated_tablets_size())
          << "Wrong report size:\n" << pb_util::SecureDebugString(report);
      const ReportedTabletPB& reported_tablet = report.updated_tablets(0);
      ASSERT_TRUE(reported_tablet.has_consensus_state());

      string uuid = tablet_replicas_[replica]->permanent_uuid();
      RaftPeerPB::Role role = GetConsensusRole(uuid, reported_tablet.consensus_state());
      if (replica == new_leader_idx) {
        ASSERT_EQ(RaftPeerPB::LEADER, role)
            << "Tablet report: " << pb_util::SecureShortDebugString(report);
      } else {
        ASSERT_EQ(RaftPeerPB::FOLLOWER, role)
            << "Tablet report: " << pb_util::SecureShortDebugString(report);
      }
    }
  }
}

// Test that the tablet manager generates reports on replica health status
// in accordance with observed changes on replica status, and that the tablet
// manager includes that information into the next tablet report. Specifically,
// verify that:
//   1. The leader replica provides the health status report in its consensus
//      state, if requested.
//   2. The health report information matches the state of tablet replicas.
//   3. The tablet manager generates appropriate tablet reports with updated
//      health information when replicas change their state.
TEST_F(TsTabletManagerITest, ReportOnReplicaHealthStatus) {
  constexpr int kNumReplicas = 3;
  const auto kTimeout = MonoDelta::FromSeconds(60);

  // This test is specific to the 3-4-3 replica management scheme.
  FLAGS_raft_prepare_replacement_before_eviction = true;
  FLAGS_catalog_manager_evict_excess_replicas = false;
  {
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = kNumReplicas;
    NO_FATALS(StartCluster(std::move(opts)));
  }

  // Create the table.
  client::sp::shared_ptr<KuduTable> table;
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&schema_)
            .set_range_partition_columns({})  // need just one tablet
            .num_replicas(kNumReplicas)
            .Create());
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Build a TServerDetails map so we can check for convergence.
  const auto& addr = cluster_->mini_master()->bound_rpc_addr();
  shared_ptr<MasterServiceProxy> master_proxy(
      new MasterServiceProxy(client_messenger_, addr, addr.host()));

  itest::TabletServerMap ts_map;
  ASSERT_OK(CreateTabletServerMap(master_proxy, client_messenger_, &ts_map));
  ValueDeleter deleter(&ts_map);

  // Collect the TabletReplicas so we get direct access to RaftConsensus.
  vector<scoped_refptr<TabletReplica>> tablet_replicas;
  ASSERT_OK(PrepareTabletReplicas(kTimeout, &tablet_replicas));
  ASSERT_EQ(kNumReplicas, tablet_replicas.size());

  // Don't send heartbeats to master, otherwise it would be a race in
  // acknowledging the heartbeats and generating new tablet reports. Clearing
  // the 'dirty' flag on a tablet before the generated tablet report is
  // introspected makes the test scenario very flaky. Also, this scenario does
  // not assume that the catalog manager initiates the replacement of failed
  // voter replicas.
  DisableHeartbeatingToMaster();

  // Generate health reports for every element of the 'tablet_replicas'
  // container.  Also, output the leader replica UUID from the consensus
  // state into 'leader_replica_uuid'.
  auto get_health_reports = [&](map<string, HealthReportPB>* reports,
                                string* leader_replica_uuid = nullptr) {
    ConsensusStatePB cstate;
    string leader_uuid;
    for (const auto& replica : tablet_replicas) {
      RaftConsensus* consensus = CHECK_NOTNULL(replica->consensus());
      ConsensusStatePB cs;
      Status s = consensus->ConsensusState(&cs, INCLUDE_HEALTH_REPORT);
      if (!s.ok()) {
        ASSERT_TRUE(s.IsIllegalState()) << s.ToString(); // Replica is shut down.
        continue;
      }
      if (consensus->peer_uuid() == cs.leader_uuid()) {
        // Only the leader replica has the up-to-date health report.
        leader_uuid = cs.leader_uuid();
        cstate = std::move(cs);
        break;
      }
    }
    ASSERT_FALSE(leader_uuid.empty());
    ASSERT_TRUE(cstate.has_committed_config());
    const RaftConfigPB& config = cstate.committed_config();
    ASSERT_EQ(kNumReplicas, config.peers_size());
    if (reports) {
      reports->clear();
      for (const auto& peer : config.peers()) {
        ASSERT_TRUE(peer.has_health_report());
        reports->emplace(peer.permanent_uuid(), peer.health_report());
      }
    }
    if (leader_replica_uuid) {
      *leader_replica_uuid = leader_uuid;
    }
  };

  // Get the information on committed Raft configuration from tablet reports
  // generated by the heartbeater of the server running the specified leader
  // tablet replica.
  auto get_committed_config_from_reports = [&](const string& leader_replica_uuid,
                                               RaftConfigPB* config) {
    TabletServer* leader_server = nullptr;
    for (auto i = 0; i < kNumReplicas; ++i) {
      MiniTabletServer* mts = cluster_->mini_tablet_server(i);
      if (mts->uuid() == leader_replica_uuid) {
        leader_server = mts->server();
        break;
      }
    }
    ASSERT_NE(nullptr, leader_server);

    // TSTabletManager should acknowledge the status change via tablet reports.
    Heartbeater* heartbeater = leader_server->heartbeater();
    ASSERT_NE(nullptr, heartbeater);

    vector<TabletReportPB> reports;
    NO_FATALS(GetIncrementalTabletReports(heartbeater, &reports));
    ASSERT_EQ(1, reports.size());
    const TabletReportPB& report = reports[0];
    SCOPED_TRACE("Tablet report: " + pb_util::SecureDebugString(report));
    ASSERT_EQ(1, report.updated_tablets_size());
    const ReportedTabletPB& reported_tablet = report.updated_tablets(0);
    ASSERT_TRUE(reported_tablet.has_consensus_state());
    const ConsensusStatePB& cstate = reported_tablet.consensus_state();
    ASSERT_EQ(RaftPeerPB::LEADER, GetConsensusRole(leader_replica_uuid, cstate));
    ASSERT_TRUE(cstate.has_committed_config());
    RaftConfigPB cfg = cstate.committed_config();
    config->Swap(&cfg);
  };

  // All replicas are up and running, so the leader replica should eventually
  // report their health status as HEALTHY.
  {
    string leader_replica_uuid;
    ASSERT_EVENTUALLY(([&] {
      map<string, HealthReportPB> reports;
      NO_FATALS(get_health_reports(&reports, &leader_replica_uuid));
      for (const auto& e : reports) {
        SCOPED_TRACE("replica UUID: " + e.first);
        ASSERT_EQ(HealthReportPB::HEALTHY, e.second.overall_health());
      }
    }));

    // Other replicas are seen by the leader in UNKNOWN health state first.
    // At this point of the test scenario, since the replicas went from the
    // UNKNOWN to the HEALTHY state, an incremental tablet reports should
    // reflect those health status changes.
    RaftConfigPB config;
    NO_FATALS(get_committed_config_from_reports(leader_replica_uuid,
                                                &config));
    ASSERT_EQ(kNumReplicas, config.peers_size());
    for (const auto& p : config.peers()) {
      ASSERT_TRUE(p.has_health_report());
      const HealthReportPB& report(p.health_report());
      ASSERT_EQ(HealthReportPB::HEALTHY, report.overall_health());
    }
  }

  // Inject an error to the replica and make sure its status is eventually
  // reported as FAILED.
  string failed_replica_uuid;
  {
    auto replica = tablet_replicas.front();
    failed_replica_uuid = replica->consensus()->peer_uuid();
    replica->SetError(Status::IOError("INJECTED ERROR: tablet failed"));
    replica->Shutdown();
    ASSERT_EQ(tablet::FAILED, replica->state());
  }

  ASSERT_EVENTUALLY(([&] {
    map<string, HealthReportPB> reports;
    NO_FATALS(get_health_reports(&reports));
    for (const auto& e : reports) {
      const auto& replica_uuid = e.first;
      SCOPED_TRACE("replica UUID: " + replica_uuid);
      if (replica_uuid == failed_replica_uuid) {
        ASSERT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, e.second.overall_health());
      } else {
        ASSERT_EQ(HealthReportPB::HEALTHY, e.second.overall_health());
      }
    }
  }));

  // The scenario below assumes the leader replica does not change anymore.
  FLAGS_enable_leader_failure_detection = false;

  {
    string leader_replica_uuid;
    NO_FATALS(get_health_reports(nullptr, &leader_replica_uuid));
    RaftConfigPB config;
    NO_FATALS(get_committed_config_from_reports(leader_replica_uuid,
                                                &config));
    for (const auto& peer : config.peers()) {
      ASSERT_TRUE(peer.has_permanent_uuid());
      const auto& uuid = peer.permanent_uuid();
      ASSERT_TRUE(peer.has_health_report());
      const HealthReportPB& report(peer.health_report());
      if (uuid == failed_replica_uuid) {
        EXPECT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, report.overall_health());
      } else {
        EXPECT_EQ(HealthReportPB::HEALTHY, report.overall_health());
      }
    }
  }
}

TEST_F(TsTabletManagerITest, TestDeduplicateMasterAddrsForHeartbeaters) {
  InternalMiniClusterOptions cluster_opts;
  NO_FATALS(StartCluster(std::move(cluster_opts)));
  // Set up the tablet server so it points to a single master, but that master
  // is specified multiple times.
  int kNumDupes = 20;
  const Sockaddr& master_addr = cluster_->mini_master()->bound_rpc_addr();
  tserver::MiniTabletServer* mini_tserver = cluster_->mini_tablet_server(0);
  tserver::TabletServerOptions* opts = cluster_->mini_tablet_server(0)->options();
  opts->master_addresses.clear();
  for (int i = 0; i < kNumDupes; i++) {
    opts->master_addresses.emplace_back(master_addr.host(), master_addr.port());
  }
  mini_tserver->Shutdown();
  ASSERT_OK(mini_tserver->Restart());
  ASSERT_OK(mini_tserver->WaitStarted());

  // The master addresses should be deduplicated and only one heartbeater
  // thread should exist.
  tserver::Heartbeater* hb = mini_tserver->server()->heartbeater();
  ASSERT_EQ(1, hb->threads_.size());
}

TEST_F(TsTabletManagerITest, TestTableStats) {
  const int kNumMasters = 3;
  const int kNumTservers = 3;
  const int kNumReplicas = 3;
  const int kNumHashBuckets = 3;
  const int kRowsCount = rand() % 1000;
  const string kNewTableName = "NewTableName";
  client::sp::shared_ptr<KuduTable> table;

  // Shorten the time and speed up the test.
  FLAGS_heartbeat_interval_ms = 100;
  FLAGS_raft_heartbeat_interval_ms = 100;
  FLAGS_leader_failure_max_missed_heartbeat_periods = 2;
  int64_t kMaxElectionTime =
      FLAGS_raft_heartbeat_interval_ms * FLAGS_leader_failure_max_missed_heartbeat_periods;

  // Get the LEADER master.
  const auto GetLeaderMaster = [&] () -> Master* {
    int idx = 0;
    Master* master = nullptr;
    Status s = cluster_->GetLeaderMasterIndex(&idx);
    if (s.ok()) {
      master = cluster_->mini_master(idx)->master();
    }
    return CHECK_NOTNULL(master);
  };
  // Get the LEADER master's service proxy.
  const auto GetLeaderMasterServiceProxy = [&] () -> shared_ptr<MasterServiceProxy> {
    const auto& addr = GetLeaderMaster()->first_rpc_address();
    shared_ptr<MasterServiceProxy> proxy(
        new MasterServiceProxy(client_messenger_, addr, addr.host()));
    return proxy;
  };
  // Get the LEADER master and run the check function.
  const auto GetLeaderMasterAndRun = [&] (int64_t live_row_count,
      const std::function<void(TableInfo*, int64_t)>& check_function) {
    CatalogManager* catalog = GetLeaderMaster()->catalog_manager();
    master::CatalogManager::ScopedLeaderSharedLock l(catalog);
    ASSERT_OK(l.first_failed_status());
    // Get the TableInfo.
    vector<scoped_refptr<TableInfo>> table_infos;
    ASSERT_OK(catalog->GetAllTables(&table_infos));
    ASSERT_EQ(1, table_infos.size());
    // Run the check function.
    NO_FATALS(check_function(table_infos[0].get(), live_row_count));
  };
  // Check the stats.
  const auto CheckStats = [&] (int64_t live_row_count) {
    // Trigger heartbeat.
    for (int i = 0; i < kNumTservers; ++i) {
      TabletServer* tserver = CHECK_NOTNULL(cluster_->mini_tablet_server(i)->server());
      tserver->tablet_manager()->SetNextUpdateTimeForTests();
      tserver->heartbeater()->TriggerASAP();
    }

    // Check the stats.
    NO_FATALS(GetLeaderMasterAndRun(live_row_count, [&] (
      TableInfo* table_info, int64_t live_row_count) {
        ASSERT_EVENTUALLY([&] () {
          ASSERT_TRUE(table_info->GetMetrics()->TableSupportsLiveRowCount());
          ASSERT_EQ(live_row_count, table_info->GetMetrics()->live_row_count->value());
        });
      }));
  };

  // 1. Start a cluster.
  {
    InternalMiniClusterOptions opts;
    opts.num_masters = kNumMasters;
    opts.num_tablet_servers = kNumTservers;
    NO_FATALS(StartCluster(std::move(opts)));
    ASSERT_EQ(kNumMasters, cluster_->num_masters());
    ASSERT_EQ(kNumTservers, cluster_->num_tablet_servers());
  }

  // 2. Create a table.
  {
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(kTableName)
                            .schema(&schema_)
                            .add_hash_partitions({ "key" }, kNumHashBuckets)
                            .num_replicas(kNumReplicas)
                            .timeout(MonoDelta::FromSeconds(60))
                            .Create());
    ASSERT_OK(client_->OpenTable(kTableName, &table));
    NO_FATALS(CheckStats(0));
  }

  // 3. Write some rows and verify the stats.
  {
    client::sp::shared_ptr<KuduSession> session(client_->NewSession());
    ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    for (int i = 0; i < kRowsCount; i++) {
      KuduInsert* insert = table->NewInsert();
      KuduPartialRow* row = insert->mutable_row();
      ASSERT_OK(row->SetInt32(0, i));
      ASSERT_OK(session->Apply(insert));
    }
    ASSERT_OK(session->Flush());
    NO_FATALS(CheckStats(kRowsCount));
  }

  // 4. Change the tablet leadership.
  {
    // Build a TServerDetails map so we can check for convergence.
    itest::TabletServerMap ts_map;
    ASSERT_OK(CreateTabletServerMap(GetLeaderMasterServiceProxy(), client_messenger_, &ts_map));
    ValueDeleter deleter(&ts_map);

    // Collect the TabletReplicas so we get direct access to RaftConsensus.
    vector<scoped_refptr<TabletReplica>> tablet_replicas;
    ASSERT_OK(PrepareTabletReplicas(MonoDelta::FromSeconds(60), &tablet_replicas));
    ASSERT_EQ(kNumReplicas * kNumHashBuckets, tablet_replicas.size());

    for (int i = 0; i < kNumReplicas * kNumHashBuckets; ++i) {
      scoped_refptr<TabletReplica> replica = tablet_replicas[i];
      if (consensus::RaftPeerPB_Role_LEADER == replica->consensus()->role()) {
        // Step down.
        itest::TServerDetails* tserver =
            CHECK_NOTNULL(FindOrDie(ts_map, replica->permanent_uuid()));
        TabletServerErrorPB error;
        auto s = LeaderStepDown(tserver, replica->tablet_id(),
                                MonoDelta::FromSeconds(10), &error);
        // In rare cases, the leader replica can change its role right before
        // the step-down request is received.
        if (s.IsIllegalState() &&
            error.code() == TabletServerErrorPB::NOT_THE_LEADER) {
          LOG(INFO) << Substitute("T:$0 P:$1: not a leader replica anymore",
                                  replica->tablet_id(), replica->permanent_uuid());
          continue;
        }
        ASSERT_OK(s);
        SleepFor(MonoDelta::FromMilliseconds(kMaxElectionTime));
        // Check stats after every election.
        NO_FATALS(CheckStats(kRowsCount));
      }
    }
  }

  // 5. Rename the table.
  {
    ASSERT_OK(itest::AlterTableName(GetLeaderMasterServiceProxy(),
        table->id(), kTableName, kNewTableName, MonoDelta::FromSeconds(5)));

    // Check table id, table name and stats.
    NO_FATALS(GetLeaderMasterAndRun(kRowsCount ,[&] (
      TableInfo* table_info, int64_t live_row_count) {
        std::ostringstream out;
        JsonWriter writer(&out, JsonWriter::PRETTY);
        ASSERT_OK(table_info->metric_entity_->WriteAsJson(&writer, MetricJsonOptions()));
        string metric_attrs_str = out.str();
        ASSERT_STR_NOT_CONTAINS(metric_attrs_str, kTableName);
        ASSERT_STR_CONTAINS(metric_attrs_str, kNewTableName);
        ASSERT_EQ(table->id(), table_info->metric_entity_->id());
        ASSERT_TRUE(table_info->GetMetrics()->TableSupportsLiveRowCount());
        ASSERT_EQ(live_row_count, table_info->GetMetrics()->live_row_count->value());
      }));
  }

  // 6. Restart the masters.
  {
    for (int i = 0; i < kNumMasters; ++i) {
      int idx = 0;
      ASSERT_OK(cluster_->GetLeaderMasterIndex(&idx));
      master::MiniMaster* mini_master = CHECK_NOTNULL(cluster_->mini_master(idx));
      mini_master->Shutdown();
      SleepFor(MonoDelta::FromMilliseconds(kMaxElectionTime));
      ASSERT_OK(mini_master->Restart());
      // Sometimes the election fails until the node restarts.
      // And the restarted node is elected leader again.
      // So, it is necessary to wait for all tservers to report.
      SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
      NO_FATALS(CheckStats(kRowsCount));
    }
  }

  // 7. Restart the tservers.
  {
    for (int i = 0; i < kNumTservers; ++i) {
      tserver::MiniTabletServer* mini_tserver = CHECK_NOTNULL(cluster_->mini_tablet_server(i));
      mini_tserver->Shutdown();
      ASSERT_OK(mini_tserver->Start());
      SleepFor(MonoDelta::FromMilliseconds(kMaxElectionTime));
      ASSERT_OK(mini_tserver->WaitStarted());
      NO_FATALS(CheckStats(kRowsCount));
    }
  }

  // 8. Delete the table.
  {
    ASSERT_OK(itest::DeleteTable(GetLeaderMasterServiceProxy(),
        table->id(), kNewTableName, MonoDelta::FromSeconds(5)));

    FLAGS_metrics_retirement_age_ms = -1;
    MetricRegistry* metric_registry = CHECK_NOTNULL(GetLeaderMaster()->metric_registry());
    const auto GetMetricsString = [&] (string* ret) {
      std::ostringstream out;
      JsonWriter writer(&out, JsonWriter::PRETTY);
      ASSERT_OK(metric_registry->WriteAsJson(&writer, MetricJsonOptions()));
      *ret = out.str();
    };

    string metrics_str;
    // On the first call, the metric is returned and internally retired, but it is not deleted.
    NO_FATALS(GetMetricsString(&metrics_str));
    ASSERT_STR_CONTAINS(metrics_str, kNewTableName);
    // On the second call, the metric is returned and then deleted.
    NO_FATALS(GetMetricsString(&metrics_str));
    ASSERT_STR_CONTAINS(metrics_str, kNewTableName);
    // On the third call, the metric is no longer available.
    NO_FATALS(GetMetricsString(&metrics_str));
    ASSERT_STR_NOT_CONTAINS(metrics_str, kNewTableName);
  }
}

TEST_F(TsTabletManagerITest, TestDeleteTableDuringTabletCopy) {
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kNumTabletServers = 3;
  const int kNumReplicas = 3;
  {
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = kNumTabletServers;
    NO_FATALS(StartCluster(std::move(opts)));
  }
  string tablet_id;
  client::sp::shared_ptr<KuduTable> table;
  itest::TabletServerMap ts_map;
  ASSERT_OK(CreateTabletServerMap(cluster_->master_proxy(), client_messenger_, &ts_map));
  ValueDeleter deleter(&ts_map);

  auto tserver_tablets_count = [&](int index) {
    return cluster_->mini_tablet_server(index)->ListTablets().size();
  };

  // Inject latency to test whether we could delete copying tablet.
  FLAGS_tablet_open_inject_latency_ms = 50;

  // Create a table with one tablet.
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
      .schema(&schema_)
      .set_range_partition_columns({})
      .num_replicas(kNumReplicas)
      .Create());
  ASSERT_EVENTUALLY([&] {
    for (int i = 0; i < kNumTabletServers; ++i) {
      ASSERT_EQ(1, tserver_tablets_count(i));
    }
  });
  tablet_id = cluster_->mini_tablet_server(0)->ListTablets()[0];
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Get the leader and follower tablet servers.
  itest::TServerDetails* leader_ts;
  ASSERT_OK(FindTabletLeader(ts_map, tablet_id, kTimeout, &leader_ts));
  int leader_index = cluster_->tablet_server_index_by_uuid(leader_ts->uuid());
  int follower_index = (leader_index + 1) % kNumTabletServers;
  MiniTabletServer* follower = cluster_->mini_tablet_server(follower_index);
  itest::TServerDetails* follower_ts = ts_map[follower->uuid()];

  // Tombstone the tablet on the follower.
  ASSERT_OK(itest::DeleteTabletWithRetries(follower_ts, tablet_id,
                                           tablet::TabletDataState::TABLET_DATA_TOMBSTONED,
                                           kTimeout));
  // Copy tablet from leader_ts to follower_ts.
  HostPort leader_addr;
  ASSERT_OK(HostPortFromPB(leader_ts->registration.rpc_addresses(0), &leader_addr));
  ASSERT_OK(itest::StartTabletCopy(follower_ts, tablet_id, leader_ts->uuid(),
                                   leader_addr, std::numeric_limits<int64_t>::max(), kTimeout));

  // Delete table during tablet copying.
  ASSERT_OK(itest::DeleteTable(cluster_->master_proxy(), table->id(), kTableName, kTimeout));

  // Finally all tablets deleted.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(0, tserver_tablets_count(follower_index));
  });
}

} // namespace tserver
} // namespace kudu
