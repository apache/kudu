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

#include <cstdlib>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(allow_unsafe_replication_factor);
DECLARE_bool(catalog_manager_wait_for_new_tablets_to_elect_leader);
DECLARE_bool(enable_leader_failure_detection);
DECLARE_bool(raft_prepare_replacement_before_eviction);
DEFINE_int32(num_election_test_loops, 3,
             "Number of random EmulateElection() loops to execute in "
             "TestReportNewLeaderOnLeaderChange");

using kudu::client::KuduClient;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::GetConsensusRole;
using kudu::consensus::HealthReportPB;
using kudu::consensus::RaftConfigPB;
using kudu::consensus::RaftConsensus;
using kudu::consensus::RaftPeerPB;
using kudu::itest::SimpleIntKeyKuduSchema;
using kudu::master::MasterServiceProxy;
using kudu::master::ReportedTabletPB;
using kudu::master::TabletReportPB;
using kudu::rpc::Messenger;
using kudu::rpc::MessengerBuilder;
using kudu::tablet::TabletReplica;
using kudu::tserver::MiniTabletServer;
using std::map;
using std::shared_ptr;
using std::string;
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

  void StartCluster(int num_replicas) {
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = num_replicas;
    cluster_.reset(new InternalMiniCluster(env_, opts));
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

// Test that when a tablet is marked as failed, it will eventually be evicted
// and replaced.
TEST_F(TsTabletManagerITest, TestFailedTabletsAreReplaced) {
  const int kNumReplicas = 3;
  NO_FATALS(StartCluster(kNumReplicas));

  InternalMiniClusterOptions opts;
  opts.num_tablet_servers = kNumReplicas;
  unique_ptr<InternalMiniCluster> cluster(new InternalMiniCluster(env_, opts));
  ASSERT_OK(cluster->Start());
  TestWorkload work(cluster.get());
  work.set_num_replicas(3);
  work.Setup();
  work.Start();

  // Insert data until the tablet becomes visible to the server.
  // We'll operate on the first tablet server, chosen arbitrarily.
  MiniTabletServer* ts = cluster->mini_tablet_server(0);
  string tablet_id;
  ASSERT_EVENTUALLY([&] {
    vector<string> tablet_ids = ts->ListTablets();
    ASSERT_EQ(1, tablet_ids.size());
    tablet_id = tablet_ids[0];
  });

  // Wait until the replica is running before failing it.
  const auto wait_until_running = [&]() {
    AssertEventually([&]{
      scoped_refptr<TabletReplica> replica;
      ASSERT_OK(ts->server()->tablet_manager()->GetTabletReplica(tablet_id, &replica));
      ASSERT_EQ(replica->state(), tablet::RUNNING);
    }, MonoDelta::FromSeconds(60));
    NO_PENDING_FATALS();
  };
  wait_until_running();

  {
    // Inject an error to the replica. Shutting it down will leave it FAILED.
    scoped_refptr<TabletReplica> replica;
    ASSERT_OK(ts->server()->tablet_manager()->GetTabletReplica(tablet_id, &replica));
    replica->SetError(Status::IOError("INJECTED ERROR: tablet failed"));
    replica->Shutdown();
    ASSERT_EQ(tablet::FAILED, replica->state());
  }

  // Ensure the tablet eventually is replicated.
  wait_until_running();
  work.StopAndJoin();
}

// Test that when the leader changes, the tablet manager gets notified and
// includes that information in the next tablet report.
TEST_F(TsTabletManagerITest, TestReportNewLeaderOnLeaderChange) {
  const int kNumReplicas = 2;
  NO_FATALS(StartCluster(kNumReplicas));

  // We need to control elections precisely for this test since we're using
  // EmulateElection() with a distributed consensus configuration.
  FLAGS_enable_leader_failure_detection = false;
  FLAGS_catalog_manager_wait_for_new_tablets_to_elect_leader = false;

  // Allow creating table with even replication factor.
  FLAGS_allow_unsafe_replication_factor = true;

  // Run a few more iters in slow-test mode.
  OverrideFlagForSlowTests("num_election_test_loops", "10");

  // Create the table.
  client::sp::shared_ptr<KuduTable> table;
  unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&schema_)
            .set_range_partition_columns({ "key" })
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
  ASSERT_OK(PrepareTabletReplicas(MonoDelta::FromSeconds(60), &tablet_replicas));
  ASSERT_EQ(kNumReplicas, tablet_replicas.size());

  // Stop heartbeating we don't race against the Master.
  DisableHeartbeatingToMaster();

  // Loop and cause elections and term changes from different servers.
  // TSTabletManager should acknowledge the role changes via tablet reports.
  for (int i = 0; i < FLAGS_num_election_test_loops; i++) {
    SCOPED_TRACE(Substitute("Iter: $0", i));
    int new_leader_idx = rand() % 2;
    LOG(INFO) << "Electing peer " << new_leader_idx << "...";
    RaftConsensus* con = CHECK_NOTNULL(tablet_replicas[new_leader_idx]->consensus());
    ASSERT_OK(con->EmulateElection());
    LOG(INFO) << "Waiting for servers to agree...";
    ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(5),
                                    ts_map, tablet_replicas[0]->tablet_id(), i + 1));

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

      string uuid = tablet_replicas[replica]->permanent_uuid();
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

  FLAGS_raft_prepare_replacement_before_eviction = true;
  NO_FATALS(StartCluster(kNumReplicas));

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
      Status s = consensus->ConsensusState(&cs, RaftConsensus::INCLUDE_HEALTH_REPORT);
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
        ASSERT_EQ(HealthReportPB::FAILED, e.second.overall_health());
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
        EXPECT_EQ(HealthReportPB::FAILED, report.overall_health());
      } else {
        EXPECT_EQ(HealthReportPB::HEALTHY, report.overall_health());
      }
    }
  }
}

} // namespace tserver
} // namespace kudu
