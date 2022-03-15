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


#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/auto_rebalancer.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace master {
class GetTableLocationsResponsePB;
}  // namespace master
}  // namespace kudu

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::consensus::RaftPeerPB;
using kudu::itest::GetTableLocations;
using kudu::itest::ListTabletServers;
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::TSDescriptor;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

DECLARE_bool(auto_leader_rebalancing_enabled);
DECLARE_bool(auto_rebalancing_enabled);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(follower_unavailable_considered_failed_sec);
DECLARE_int32(raft_heartbeat_interval_ms);
DECLARE_int32(tablet_copy_download_file_inject_latency_ms);
DECLARE_int32(tserver_unresponsive_timeout_ms);
DECLARE_uint32(auto_leader_rebalancing_interval_seconds);
DECLARE_uint32(auto_rebalancing_interval_seconds);
DECLARE_uint32(auto_rebalancing_max_moves_per_server);
DECLARE_uint32(auto_rebalancing_wait_for_replica_moves_seconds);

METRIC_DECLARE_gauge_int32(tablet_copy_open_client_sessions);
METRIC_DECLARE_counter(tablet_copy_bytes_fetched);
METRIC_DECLARE_counter(tablet_copy_bytes_sent);

namespace kudu {
namespace master {

enum class BalanceThreadType { REPLICA_REBALANCE, LEADER_REBALANCE };

class LeaderRebalancerTest : public KuduTest {
 public:
  Status CreateAndStartCluster() {
    // Disable replica rebalancing, we'll do it manually
    FLAGS_auto_rebalancing_enabled = false;
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
    catalog_manager->GetTableInfoByName(table_name(), &table_info);

    TSDescriptorVector descriptors;
    master->ts_manager()->GetAllDescriptors(&descriptors);

    std::set<std::string> tserver_uuid_set;
    for (const auto& e : descriptors) {
      if (e->PresumedDead()) {
        continue;
      }
      tserver_uuid_set.insert(e->permanent_uuid());
    }

    // tablet_id of leader -> uuid
    map<string, string> leader_uuid_map;
    // tablet_id of follower -> vector<uuid>
    map<string, vector<string>> follower_uuid_map;
    // tserver uuid -> vector<all leaders' tablet id>
    map<string, vector<string>> uuid_leaders_map;
    // tserver uuid -> vector<all replicas' tablet id>
    map<string, vector<string>> uuid_replicas_map;

    map<string, kudu::HostPort> leader_uuid_host_port_map;

    std::vector<scoped_refptr<TabletInfo>> tablet_infos;
    table_info->GetAllTablets(&tablet_infos);

    // step 1. Get basic statistics
    for (const auto& tablet : tablet_infos) {
      TabletMetadataLock tablet_l(tablet.get(), LockMode::READ);

      // Retrieve all replicas of the tablet.
      TabletLocationsPB locs_pb;
      CatalogManager::TSInfosDict ts_infos_dict;

      // GetTabletLocations() will fail if the catalog manager is not the
      // leader.
      {
        CatalogManager::ScopedLeaderSharedLock leaderlock(catalog_manager);
        RETURN_NOT_OK(leaderlock.first_failed_status());
        // This will only return tablet replicas in the RUNNING state, and
        // filter to only retrieve voter replicas.
        RETURN_NOT_OK(catalog_manager->GetTabletLocations(
            tablet->id(), ReplicaTypeFilter::VOTER_REPLICA, &locs_pb,
            &ts_infos_dict, boost::none));
      }

      // Build a summary for each replica of the tablet.
      for (const auto& r : locs_pb.interned_replicas()) {
        int index = r.ts_info_idx();
        const TSInfoPB& ts_info = *(ts_infos_dict.ts_info_pbs()[index]);
        string uuid = ts_info.permanent_uuid();
        if (r.role() == RaftPeerPB::LEADER) {
          auto it = uuid_leaders_map.find(uuid);
          if (it != uuid_leaders_map.end()) {
            it->second.emplace_back(tablet->id());
          } else {
            uuid_leaders_map.insert(
                {uuid, std::vector<string>({tablet->id()})});
          }
          leader_uuid_map.insert({tablet->id(), uuid});
          leader_uuid_host_port_map.insert(
              {uuid, HostPortFromPB(ts_info.rpc_addresses(0))});
        } else if (r.role() == RaftPeerPB::FOLLOWER) {
          auto it = follower_uuid_map.find(tablet->id());
          if (it != follower_uuid_map.end()) {
            follower_uuid_map[tablet->id()].push_back(uuid);
          } else {
            follower_uuid_map.insert(
                {tablet->id(), std::vector<string>({uuid})});
          }
        } else {
          LOG(INFO) << "not voter role: " << RaftPeerPB::Role_Name(r.role());
          continue;
        }

        auto it = uuid_replicas_map.find(ts_info.permanent_uuid());
        if (it == uuid_replicas_map.end()) {
          uuid_replicas_map.insert(
              {ts_info.permanent_uuid(), std::vector<string>({tablet->id()})});
        } else {
          it->second.emplace_back(tablet->id());
        }
      }
    }

    // <uuid, number of replica, number of leader>
    std::map<string, std::pair<int32_t, int32_t>> tserver_statistics;
    // uuid->leader should transfer count
    map<string, int32_t> leader_transfer_source;
    for (const auto& uuid : tserver_uuid_set) {
      int32_t leader_count = 0;
      int32_t replica_count = 0;
      auto it1 = uuid_leaders_map.find(uuid);
      if (it1 != uuid_leaders_map.end()) {
        leader_count = it1->second.size();
      } else {
        leader_count = 0;
      }
      auto it2 = uuid_replicas_map.find(uuid);
      if (it2 != uuid_replicas_map.end()) {
        replica_count = it2->second.size();
      } else {
        // no replica, skip it
        continue;
      }
      tserver_statistics.insert(
          {uuid, std::pair<int32_t, int32_t>(replica_count, leader_count)});
      LOG(INFO) << "uuid: " << uuid
                << ", replica_count: " << replica_count
                << ", leader_count:" << leader_count;
    }
    return Status::OK();
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
  InternalMiniClusterOptions cluster_opts_;
  unique_ptr<TestWorkload> workload_;
};

// Create a cluster that is initially balanced and leader balanced.
// Bring up another tserver, and verify that moves are scheduled,
// since the cluster is no longer balanced.
TEST_F(LeaderRebalancerTest, AddTserver) {
  const int kNumTServers = 3;
  const int kNumTablets = 59;

  cluster_opts_.num_tablet_servers = kNumTServers;
  ASSERT_OK(CreateAndStartCluster());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Add a tablet server and verify that the master schedules some moves, and
  // the tablet servers copy bytes as appropriate.
  ASSERT_OK(cluster_->AddTabletServer());
  int tserver_size = cluster_->num_tablet_servers();
  LOG(INFO) << "add a tserver: "
            << cluster_->mini_tablet_server(tserver_size - 1)->uuid();

  // Leader master
  master::Master* master = cluster_->mini_master()->master();

  master::AutoRebalancerTask* replica_rebalancer =
      master->catalog_manager()->auto_rebalancer();
  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  ASSERT_NE(replica_rebalancer, nullptr);
  ASSERT_NE(leader_rebalancer, nullptr);

  SleepFor(MonoDelta::FromSeconds(3));
  for (int i = 0; i < 6; i++) {
    replica_rebalancer->RunReplicaRebalancer();
    SleepFor(MonoDelta::FromSeconds(2));
    leader_rebalancer->RunLeaderRebalancer();
    CheckLeaderBalance();
  }
  LOG(INFO) << "LeaderRebalancerTest, AddTserver finish";
}

TEST_F(LeaderRebalancerTest, RestartTserver) {
  const int kNumTServers = 4;
  const int kNumTablets = 59;
  cluster_opts_.num_tablet_servers = kNumTServers;
  ASSERT_OK(CreateAndStartCluster());

  CreateWorkloadTable(kNumTablets, /*num_replicas*/ 3);

  // Leader master
  master::Master* master = cluster_->mini_master()->master();

  master::AutoRebalancerTask* replica_rebalancer =
      master->catalog_manager()->auto_rebalancer();
  master::AutoLeaderRebalancerTask* leader_rebalancer =
      master->catalog_manager()->auto_leader_rebalancer();
  ASSERT_NE(replica_rebalancer, nullptr);
  ASSERT_NE(leader_rebalancer, nullptr);

  SleepFor(MonoDelta::FromSeconds(5));
  cluster_->mini_tablet_server(0)->Restart();
  for (int i = 0; i < 6; i++) {
    LOG(INFO) << "RunLeaderRebalancer b";
    leader_rebalancer->RunLeaderRebalancer();
    SleepFor(MonoDelta::FromSeconds(2));
    CheckLeaderBalance();
  }
  LOG(INFO) << "LeaderRebalancerTest, RestartTserver finish";
}

}  // namespace master
}  // namespace kudu
