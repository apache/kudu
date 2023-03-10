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
#include "kudu/master/auto_leader_rebalancer.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h" // IWYU pragma: keep
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace master {
class AutoRebalancerTask;
}  // namespace master
}  // namespace kudu


using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::tserver::ListTabletsResponsePB;
using std::string;
using std::unique_ptr;
using std::vector;

DECLARE_bool(auto_leader_rebalancing_enabled);
DECLARE_bool(auto_rebalancing_enabled);
DECLARE_int32(heartbeat_interval_ms);
DECLARE_uint32(auto_leader_rebalancing_interval_seconds);
DECLARE_uint32(auto_rebalancing_interval_seconds);
DECLARE_uint32(auto_rebalancing_max_moves_per_server);
DECLARE_uint32(auto_rebalancing_wait_for_replica_moves_seconds);
DECLARE_uint32(leader_rebalancing_max_moves_per_round);

namespace kudu {
namespace master {

enum class BalanceThreadType { REPLICA_REBALANCE, LEADER_REBALANCE };

class LeaderRebalancerTest : public KuduTest {
 public:
  Status CreateAndStartCluster() {
    // Disable replica rebalancing, we'll do it manually
    FLAGS_auto_rebalancing_enabled = true;
    FLAGS_auto_rebalancing_interval_seconds = 1;                // Shorten for testing.
    FLAGS_auto_rebalancing_wait_for_replica_moves_seconds = 0;  // Shorten for testing.
    // Disable leader rebalancing, we'll do it manually.
    FLAGS_auto_leader_rebalancing_enabled = false;
    cluster_.reset(new InternalMiniCluster(env_, cluster_opts_));
    return cluster_->Start();
  }

  void CreateWorkloadTable(int num_tablets, int num_replicas) {
    workload_.reset(new TestWorkload(cluster_.get()));
    workload_->set_num_tablets(num_tablets);
    workload_->set_num_replicas(num_replicas);
    workload_->Setup();
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  std::string table_name() { return workload_->table_name(); }

  Status CheckLeaderBalance() {
    // Leader master
    master::Master* master = cluster_->mini_master()->master();
    master::CatalogManager* catalog_manager = master->catalog_manager();
    scoped_refptr<master::TableInfo> table_info;
    {
      CatalogManager::ScopedLeaderSharedLock leaderlock(catalog_manager);
      catalog_manager->GetTableInfoByName(table_name(), &table_info);
    }

    TSDescriptorVector descriptors;
    master->ts_manager()->GetAllDescriptors(&descriptors);

    std::vector<std::string> tserver_uuids;
    for (const auto& e : descriptors) {
      if (e->PresumedDead()) {
        continue;
      }
      tserver_uuids.emplace_back(e->permanent_uuid());
    }

    return catalog_manager->auto_leader_rebalancer()->RunLeaderRebalanceForTable(
        table_info, tserver_uuids, {}, AutoLeaderRebalancerTask::ExecuteMode::TEST);
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
  InternalMiniClusterOptions cluster_opts_;
  unique_ptr<TestWorkload> workload_;
};

// Create a cluster, and create a table,
// whether tablets is balanced, which decided by creating table process.
// Bring up another tserver, the table is not balanced and leaders is also
// not balanced. We verify that moves are scheduled,
// since the cluster is no longer balanced.
TEST_F(LeaderRebalancerTest, AddTserver) {
  const int kNumTServers = 3;
  const int kNumTablets = 59;

  cluster_opts_.num_tablet_servers = kNumTServers;
  FLAGS_leader_rebalancing_max_moves_per_round = 5;
  ASSERT_OK(CreateAndStartCluster());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Add a tablet server and verify that the master schedules some moves, and
  // the tablet servers copy bytes as appropriate.
  ASSERT_OK(cluster_->AddTabletServer());
  int tserver_size = cluster_->num_tablet_servers();
  LOG(INFO) << "add a tserver: " << cluster_->mini_tablet_server(tserver_size - 1)->uuid();

  // Leader master
  master::Master* master = cluster_->mini_master()->master();

  master::AutoRebalancerTask* replica_rebalancer = master->catalog_manager()->auto_rebalancer();
  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  ASSERT_NE(replica_rebalancer, nullptr);
  ASSERT_NE(leader_rebalancer, nullptr);

  // To wait replica_rebalancer execute some runs and reach balanced.
  SleepFor(MonoDelta::FromSeconds(20 * FLAGS_auto_rebalancing_interval_seconds));
  constexpr const int32_t retries = 40;
  for (int i = 0; i < retries; i++) {
    leader_rebalancer->RunLeaderRebalancer();
    if (CheckLeaderBalance().ok()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }

  ASSERT_OK(CheckLeaderBalance());
}

TEST_F(LeaderRebalancerTest, RestartTserver) {
  const int kNumTServers = 4;
  const int kNumTablets = 59;
  cluster_opts_.num_tablet_servers = kNumTServers;
  FLAGS_leader_rebalancing_max_moves_per_round = 5;
  ASSERT_OK(CreateAndStartCluster());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Leader master
  master::Master* master = cluster_->mini_master()->master();

  master::AutoRebalancerTask* replica_rebalancer = master->catalog_manager()->auto_rebalancer();
  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  ASSERT_NE(replica_rebalancer, nullptr);
  ASSERT_NE(leader_rebalancer, nullptr);

  cluster_->mini_tablet_server(0)->Restart();
  // To wait replica_rebalancer execute some runs and reach balanced.
  SleepFor(MonoDelta::FromSeconds(10 * FLAGS_auto_rebalancing_interval_seconds));
  constexpr const int32_t retries = 20;
  for (int i = 0; i < retries; i++) {
    leader_rebalancer->RunLeaderRebalancer();
    if (CheckLeaderBalance().ok()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }
  ASSERT_OK(CheckLeaderBalance());
}

TEST_F(LeaderRebalancerTest, TestMaintenanceMode) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  constexpr const int kNumTServers = 3;
  constexpr const int kNumTablets = 10;
  cluster_opts_.num_tablet_servers = kNumTServers;
  FLAGS_leader_rebalancing_max_moves_per_round = 5;
  ASSERT_OK(CreateAndStartCluster());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Leader master
  master::Master* master = cluster_->mini_master()->master();

  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  ASSERT_NE(leader_rebalancer, nullptr);

  constexpr const int kCurrentTserverIndex = 0;
  tserver::MiniTabletServer* mini_tserver = cluster_->mini_tablet_server(kCurrentTserverIndex);
  // Sets the tserver state for a tserver to 'MAINTENANCE_MODE'.
  ASSERT_OK(
      master->ts_manager()->SetTServerState(mini_tserver->uuid(),
                                            TServerStatePB::MAINTENANCE_MODE,
                                            ChangeTServerStateRequestPB::ALLOW_MISSING_TSERVER,
                                            master->catalog_manager()->sys_catalog()));
  ASSERT_EQ(TServerStatePB::MAINTENANCE_MODE,
            master->ts_manager()->GetTServerState(mini_tserver->uuid()));
  // Restart the tserver to force transferring all leaders on it to make leaders not balanced.
  mini_tserver->Shutdown();
  SleepFor(MonoDelta::FromMilliseconds(5 * FLAGS_heartbeat_interval_ms));
  ASSERT_OK(mini_tserver->Start());

  // Try to run some 'leader rebalance' iterations. If mini_tserver is not in MAINTENANCE_MODE,
  // it's enough to reach leader balanced, more tries is not necessary and less tries
  // may not reach leader rebalanced.
  constexpr const int32_t retries = std::max(kNumTablets / 2, 3);
  for (int i = 0; i < retries; i++) {
    ASSERT_OK(leader_rebalancer->RunLeaderRebalancer());
    if (CheckLeaderBalance().ok()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_heartbeat_interval_ms));
  }
  // This cluster cannot reach the state of rebalanced leadership distribution since 1 of the 3
  // tservers is in maintenance mode.
  Status status = CheckLeaderBalance();
  ASSERT_TRUE(status.IsIllegalState()) << status.ToString();

  {
    // The tserver in maintenance mode should have no leaders.
    std::shared_ptr<tserver::TabletServerServiceProxy> proxy =
        cluster_->tserver_proxy(kCurrentTserverIndex);
    tserver::ListTabletsRequestPB req;
    ListTabletsResponsePB resp;
    rpc::RpcController rpc;
    rpc.set_timeout(MonoDelta::FromMilliseconds(1000));
    ASSERT_OK(proxy->ListTablets(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_FALSE(resp.status_and_schema().empty());
    for (const auto& replica : resp.status_and_schema()) {
      ASSERT_NE(consensus::RaftPeerPB::LEADER, replica.role());
    }
  }
}

}  // namespace master
}  // namespace kudu
