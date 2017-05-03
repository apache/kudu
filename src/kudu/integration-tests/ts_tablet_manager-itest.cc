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

#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "kudu/client/client.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/rpc/messenger.h"
#include "kudu/server/server_base.proxy.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/heartbeater.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/test_util.h"

DECLARE_bool(enable_leader_failure_detection);
DECLARE_bool(catalog_manager_wait_for_new_tablets_to_elect_leader);
DECLARE_bool(allow_unsafe_replication_factor);
DEFINE_int32(num_election_test_loops, 3,
             "Number of random EmulateElection() loops to execute in "
             "TestReportNewLeaderOnLeaderChange");

namespace kudu {
namespace tserver {

using client::KuduClient;
using client::KuduSchema;
using client::KuduTable;
using client::KuduTableCreator;
using consensus::GetConsensusRole;
using consensus::RaftPeerPB;
using itest::SimpleIntKeyKuduSchema;
using master::MasterServiceProxy;
using master::ReportedTabletPB;
using master::TabletReportPB;
using rpc::Messenger;
using rpc::MessengerBuilder;
using std::shared_ptr;
using strings::Substitute;
using tablet::TabletReplica;
using tserver::MiniTabletServer;
using tserver::TSTabletManager;

static const char* const kTableName = "test-table";
static const int kNumReplicas = 2;

class TsTabletManagerITest : public KuduTest {
 public:
  TsTabletManagerITest()
      : schema_(SimpleIntKeyKuduSchema()) {
  }
  virtual void SetUp() OVERRIDE;

 protected:
  const KuduSchema schema_;

  gscoped_ptr<MiniCluster> cluster_;
  client::sp::shared_ptr<KuduClient> client_;
  std::shared_ptr<Messenger> client_messenger_;
};

void TsTabletManagerITest::SetUp() {
  KuduTest::SetUp();

  MessengerBuilder bld("client");
  ASSERT_OK(bld.Build(&client_messenger_));

  MiniClusterOptions opts;
  opts.num_tablet_servers = kNumReplicas;
  cluster_.reset(new MiniCluster(env_, opts));
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
}

// Test that when the leader changes, the tablet manager gets notified and
// includes that information in the next tablet report.
TEST_F(TsTabletManagerITest, TestReportNewLeaderOnLeaderChange) {
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
  gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kTableName)
            .schema(&schema_)
            .set_range_partition_columns({ "key" })
            .num_replicas(kNumReplicas)
            .Create());
  ASSERT_OK(client_->OpenTable(kTableName, &table));

  // Build a TServerDetails map so we can check for convergence.
  shared_ptr<MasterServiceProxy> master_proxy(
      new MasterServiceProxy(client_messenger_, cluster_->mini_master()->bound_rpc_addr()));

  itest::TabletServerMap ts_map;
  ASSERT_OK(CreateTabletServerMap(master_proxy, client_messenger_, &ts_map));
  ValueDeleter deleter(&ts_map);

  // Collect the TabletReplicas so we get direct access to Consensus.
  vector<scoped_refptr<TabletReplica> > tablet_replicas;
  for (int replica = 0; replica < kNumReplicas; replica++) {
    MiniTabletServer* ts = cluster_->mini_tablet_server(replica);
    ts->FailHeartbeats(); // Stop heartbeating we don't race against the Master.
    vector<scoped_refptr<TabletReplica> > cur_ts_tablet_replicas;
    // The replicas may not have been created yet, so loop until we see them.
    while (true) {
      ts->server()->tablet_manager()->GetTabletReplicas(&cur_ts_tablet_replicas);
      if (!cur_ts_tablet_replicas.empty()) break;
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    ASSERT_EQ(1, cur_ts_tablet_replicas.size()); // Each TS should only have 1 tablet.
    ASSERT_OK(cur_ts_tablet_replicas[0]->WaitUntilConsensusRunning(MonoDelta::FromSeconds(10)));
    tablet_replicas.push_back(cur_ts_tablet_replicas[0]);
  }

  // Loop and cause elections and term changes from different servers.
  // TSTabletManager should acknowledge the role changes via tablet reports.
  for (int i = 0; i < FLAGS_num_election_test_loops; i++) {
    SCOPED_TRACE(Substitute("Iter: $0", i));
    int new_leader_idx = rand() % 2;
    LOG(INFO) << "Electing peer " << new_leader_idx << "...";
    consensus::Consensus* con = CHECK_NOTNULL(tablet_replicas[new_leader_idx]->consensus());
    ASSERT_OK(con->EmulateElection());
    LOG(INFO) << "Waiting for servers to agree...";
    ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(5),
                                    ts_map, tablet_replicas[0]->tablet_id(), i + 1));

    // Now check that the tablet report reports the correct role for both servers.
    for (int replica = 0; replica < kNumReplicas; replica++) {
      // The MarkDirty() callback is on an async thread so it might take the
      // follower a few milliseconds to execute it. Wait for that to happen.
      Heartbeater* heartbeater =
          cluster_->mini_tablet_server(replica)->server()->heartbeater();
      vector<TabletReportPB> reports;
      for (int retry = 0; retry <= 12; retry++) {
        reports = heartbeater->GenerateIncrementalTabletReportsForTests();
        ASSERT_EQ(1, reports.size());
        if (!reports[0].updated_tablets().empty()) break;
        SleepFor(MonoDelta::FromMilliseconds(1 << retry));
      }

      // Ensure that our tablet reports are consistent.
      TabletReportPB& report = reports[0];
      ASSERT_EQ(1, report.updated_tablets_size())
          << "Wrong report size:\n" << SecureDebugString(report);
      ReportedTabletPB reported_tablet = report.updated_tablets(0);
      ASSERT_TRUE(reported_tablet.has_committed_consensus_state());

      string uuid = tablet_replicas[replica]->permanent_uuid();
      RaftPeerPB::Role role = GetConsensusRole(uuid, reported_tablet.committed_consensus_state());
      if (replica == new_leader_idx) {
        ASSERT_EQ(RaftPeerPB::LEADER, role)
            << "Tablet report: " << SecureShortDebugString(report);
      } else {
        ASSERT_EQ(RaftPeerPB::FOLLOWER, role)
            << "Tablet report: " << SecureShortDebugString(report);
      }
    }
  }
}

} // namespace tserver
} // namespace kudu
