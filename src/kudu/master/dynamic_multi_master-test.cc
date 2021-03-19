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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_histogram(log_gc_duration);
METRIC_DECLARE_entity(tablet);

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalDaemonOptions;
using kudu::cluster::ExternalMaster;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::MiniCluster;
using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::HealthReportPB;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::RaftConfigPB;
using kudu::consensus::RaftPeerPB;
using kudu::rpc::RpcController;
using std::set;
using std::string;
using std::tuple;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

static Status CreateTable(ExternalMiniCluster* cluster,
                          const std::string& table_name) {
  shared_ptr<KuduClient> client;
      RETURN_NOT_OK(cluster->CreateClient(nullptr, &client));
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
      RETURN_NOT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  return table_creator->table_name(table_name)
      .schema(&schema)
      .set_range_partition_columns({ "key" })
      .num_replicas(1)
      .Create();
}

// Test class for testing addition/removal of masters to a Kudu cluster.
class DynamicMultiMasterTest : public KuduTest {
 protected:
  void SetUpWithNumMasters(int num_masters) {
    // Initial number of masters in the cluster before adding a master.
    orig_num_masters_ = num_masters;

    // Reserving a port upfront for the new master that'll be added to the cluster.
    ASSERT_OK(MiniCluster::ReserveDaemonSocket(MiniCluster::MASTER, orig_num_masters_ /* index */,
                                               kDefaultBindMode, &reserved_socket_));

    ASSERT_OK(reserved_socket_->GetSocketAddress(&reserved_addr_));
    reserved_hp_ = HostPort(reserved_addr_);
  }

  void StartCluster(const vector<string>& extra_master_flags,
                    bool supply_single_master_addr = true) {
    opts_.num_masters = orig_num_masters_;
    opts_.supply_single_master_addr = supply_single_master_addr;
    opts_.extra_master_flags = extra_master_flags;

    cluster_.reset(new ExternalMiniCluster(opts_));
    ASSERT_OK(cluster_->Start());
    ASSERT_OK(cluster_->WaitForTabletServerCount(cluster_->num_tablet_servers(),
                                                 MonoDelta::FromSeconds(5)));
  }

  // Bring up a cluster with bunch of tables and ensure the system catalog WAL
  // has been GC'ed.
  // Out parameter 'master_hps' returns the HostPort of the masters in the original
  // cluster.
  void StartClusterWithSysCatalogGCed(vector<HostPort>* master_hps,
                                      const vector<string>& extra_flags = {}) {
    // Using low values of log flush threshold and segment size to trigger GC of the
    // sys catalog WAL
    vector<string> flags = {"--master_support_change_config", "--flush_threshold_secs=0",
                            "--log_segment_size_mb=1"};
    flags.insert(flags.end(), extra_flags.begin(), extra_flags.end());
    NO_FATALS(StartCluster(flags));

    // Verify that masters are running as VOTERs and collect their addresses to be used
    // for starting the new master.
    NO_FATALS(VerifyVoterMasters(orig_num_masters_, master_hps));

    // Function to fetch the GC count of the system catalog WAL.
    auto get_sys_catalog_wal_gc_count = [&] (int master_idx) {
      int64_t sys_catalog_wal_gc_count = 0;
      CHECK_OK(itest::GetInt64Metric(cluster_->master(master_idx)->bound_http_hostport(),
                                     &METRIC_ENTITY_tablet,
                                     master::SysCatalogTable::kSysCatalogTabletId,
                                     &METRIC_log_gc_duration,
                                     "total_count",
                                     &sys_catalog_wal_gc_count));
      return sys_catalog_wal_gc_count;
    };

    vector<int64_t> orig_gc_count(orig_num_masters_);
    for (int master_idx = 0; master_idx < orig_num_masters_; master_idx++) {
      orig_gc_count[master_idx] = get_sys_catalog_wal_gc_count(master_idx);
    }

    // Function to compute whether all masters have updated the system catalog WAL GC count.
    // Ideally we could just check against the leader master but the leader master could
    // potentially change hence checking across all masters.
    auto all_masters_updated_wal_gc_count = [&] {
      int num_masters_gc_updated = 0;
      for (int master_idx = 0; master_idx < orig_num_masters_; master_idx++) {
        if (get_sys_catalog_wal_gc_count(master_idx) > orig_gc_count[master_idx]) {
          num_masters_gc_updated++;
        }
      }
      return num_masters_gc_updated == orig_num_masters_;
    };

    // Create a bunch of tables to ensure sys catalog WAL gets GC'ed.
    // Need to create around 1k tables even with lowest flush threshold and log segment size.
    int i;
    bool wal_gc_counts_updated = false;
    for (i = 1; i < 1000; i++) {
      if (all_masters_updated_wal_gc_count()) {
        wal_gc_counts_updated = true;
        break;
      }
      string table_name = Substitute("Table.$0.$1", CURRENT_TEST_NAME(), std::to_string(i));
      ASSERT_OK(CreateTable(cluster_.get(), table_name));
    }
    LOG(INFO) << "Number of tables created: " << i - 1;
    if (wal_gc_counts_updated) {
      // We are done here and no need to wait further.
      return;
    }

    MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(2);
    while (MonoTime::Now() < deadline) {
      if (all_masters_updated_wal_gc_count()) {
        wal_gc_counts_updated = true;
        break;
      }
      SleepFor(MonoDelta::FromMilliseconds(100));
    }
    ASSERT_TRUE(wal_gc_counts_updated) << "Timed out waiting for system catalog WAL to be GC'ed";
  }

  // Functor that takes a leader_master_idx and runs the desired master RPC against
  // the leader master returning the RPC status and the optional MasterErrorPB::Code.
  typedef std::function<
      std::pair<Status, boost::optional<MasterErrorPB::Code>>(int leader_master_idx)> MasterRPC;

  // Helper function that runs the master RPC against the leader master and retries the RPC
  // if the expected leader master returns NOT_THE_LEADER error due to leadership change.
  // Returns a single combined Status:
  //   - RPC return status if not OK.
  //   - IllegalState for a master response error other than NOT_THE_LEADER error.
  //   - TimedOut if all attempts to run the RPC against leader master are exhausted.
  //   - OK if the master RPC is successful.
  Status RunLeaderMasterRPC(const MasterRPC& master_rpc, ExternalMiniCluster* cluster = nullptr) {
    if (cluster == nullptr) {
      cluster = cluster_.get();
    }

    int64_t time_left_to_sleep_msecs = 2000;
    while (time_left_to_sleep_msecs > 0) {
      int leader_master_idx;
      RETURN_NOT_OK(cluster->GetLeaderMasterIndex(&leader_master_idx));
      const auto& rpc_result = master_rpc(leader_master_idx);
      RETURN_NOT_OK(rpc_result.first);
      const auto& master_error = rpc_result.second;
      if (!master_error) {
        return Status::OK();
      }
      if (master_error != MasterErrorPB::NOT_THE_LEADER) {
        // Some other master error.
        return Status::IllegalState(Substitute("Master error: $0"),
                                    MasterErrorPB_Code_Name(*master_error));
      }
      // NOT_THE_LEADER error, so retry after some duration.
      static const MonoDelta kSleepDuration = MonoDelta::FromMilliseconds(100);
      SleepFor(kSleepDuration);
      time_left_to_sleep_msecs -= kSleepDuration.ToMilliseconds();
    }
    return Status::TimedOut("Failed contacting the right leader master after multiple attempts");
  }

  // Run ListMasters RPC, retrying on leadership change, returning the response in 'resp'.
  void RunListMasters(ListMastersResponsePB* resp, ExternalMiniCluster* cluster = nullptr) {
    if (cluster == nullptr) {
      cluster = cluster_.get();
    }
    auto list_masters = [&] (int leader_master_idx) {
      ListMastersRequestPB req;
      RpcController rpc;
      Status s = cluster->master_proxy(leader_master_idx)->ListMasters(req, resp, &rpc);
      boost::optional<MasterErrorPB::Code> err_code(resp->has_error(), resp->error().code());
      return std::make_pair(s, err_code);
    };
    ASSERT_OK(RunLeaderMasterRPC(list_masters, cluster));
  }

  // Verify the ExternalMiniCluster 'cluster' contains 'num_masters' overall
  // and are all VOTERS.
  // Return master addresses in 'master_hps', if not nullptr.
  void VerifyVoterMasters(int num_masters, vector<HostPort>* master_hps = nullptr,
                          ExternalMiniCluster* cluster = nullptr) {
    ListMastersResponsePB resp;
    NO_FATALS(RunListMasters(&resp, cluster));
    ASSERT_EQ(num_masters, resp.masters_size());
    if (master_hps) master_hps->clear();
    for (const auto& master : resp.masters()) {
      ASSERT_TRUE(master.role() == RaftPeerPB::LEADER || master.role() == RaftPeerPB::FOLLOWER);
      ASSERT_EQ(RaftPeerPB::VOTER, master.member_type());
      ASSERT_EQ(1, master.registration().rpc_addresses_size());
      if (master_hps) {
        master_hps->emplace_back(HostPortFromPB(master.registration().rpc_addresses(0)));
      }
    }
  }

  // Brings up a new master 'new_master_hp' where 'master_hps' contains master addresses including
  // the new master to be added at the index 'new_master_idx' in the ExternalMiniCluster.
  void StartNewMaster(const vector<HostPort>& master_hps,
                      const HostPort& new_master_hp,
                      int new_master_idx,
                      bool master_supports_change_config = true) {
    vector<string> master_addresses;
    master_addresses.reserve(master_hps.size());
    for (const auto& hp : master_hps) {
      master_addresses.emplace_back(hp.ToString());
    }

    // Start with an existing master daemon's options, but modify them for use in a new master
    ExternalDaemonOptions new_master_opts = cluster_->master(0)->opts();
    const string new_master_id = Substitute("master-$0", new_master_idx);
    new_master_opts.wal_dir = cluster_->GetWalPath(new_master_id);
    new_master_opts.data_dirs = cluster_->GetDataPaths(new_master_id);
    new_master_opts.log_dir = cluster_->GetLogPath(new_master_id);
    new_master_opts.rpc_bind_address = new_master_hp;
    auto& flags = new_master_opts.extra_flags;
    flags.insert(flags.end(),
                 {"--master_addresses=" + JoinStrings(master_addresses, ","),
                  "--master_address_add_new_master=" + new_master_hp.ToString()});

    LOG(INFO) << "Bringing up the new master at: " << new_master_hp.ToString();
    new_master_.reset(new ExternalMaster(new_master_opts));
    ASSERT_OK(new_master_->Start());
    ASSERT_OK(new_master_->WaitForCatalogManager());

    Sockaddr new_master_addr;
    ASSERT_OK(SockaddrFromHostPort(new_master_hp, &new_master_addr));
    new_master_proxy_.reset(
        new MasterServiceProxy(new_master_opts.messenger, new_master_addr, new_master_hp.host()));
    {
      GetMasterRegistrationRequestPB req;
      GetMasterRegistrationResponsePB resp;
      RpcController rpc;

      ASSERT_OK(new_master_proxy_->GetMasterRegistration(req, &resp, &rpc));
      ASSERT_FALSE(resp.has_error());
      if (master_supports_change_config) {
        ASSERT_EQ(RaftPeerPB::NON_VOTER, resp.member_type());
        ASSERT_EQ(RaftPeerPB::LEARNER, resp.role());
      } else {
        // For a new master brought that doesn't support change config, it'll be started
        // as a VOTER and become FOLLOWER if the other masters are reachable.
        ASSERT_EQ(RaftPeerPB::VOTER, resp.member_type());
        ASSERT_EQ(RaftPeerPB::FOLLOWER, resp.role());
      }
    }
  }

  // Fetch a follower (non-leader) master index from the cluster.
  Status GetFollowerMasterIndex(int* follower_master_idx) {
    int leader_master_idx;
    RETURN_NOT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
    int follower = -1;
    for (int i = 0; i < cluster_->num_masters(); i++) {
      if (i != leader_master_idx) {
        follower = i;
        break;
      }
    }
    if (follower == -1) {
      return Status::NotFound("No follower master found");
    }
    *follower_master_idx = follower;
    return Status::OK();
  }

  // Adds the specified master to the cluster using the CLI tool.
  // Unset 'master' can be used to indicate to not supply master address.
  // Optional 'wait_secs' can be used to supply wait timeout to the master add CLI tool.
  // Returns generic RuntimeError() on failure with the actual error in the optional 'err'
  // output parameter.
  Status AddMasterToClusterUsingCLITool(const HostPort& master, string* err = nullptr,
                                        int wait_secs = 0) {
    auto hps = cluster_->master_rpc_addrs();
    vector<string> addresses;
    addresses.reserve(hps.size());
    for (const auto& hp : hps) {
      addresses.emplace_back(hp.ToString());
    }

    vector<string> cmd = {"master", "add", JoinStrings(addresses, ",")};
    if (master.Initialized()) {
      cmd.emplace_back(master.ToString());
    }
    if (wait_secs != 0) {
      cmd.emplace_back("-wait_secs=" + std::to_string(wait_secs));
    }
    RETURN_NOT_OK(tools::RunKuduTool(cmd, nullptr, err));
    // master add CLI doesn't return an error if the master is already present.
    // So don't try adding to the ExternalMiniCluster.
    if (err != nullptr && err->find("Master already present") != string::npos)  {
      return Status::OK();
    }
    return cluster_->AddMaster(new_master_);
  }

  // Removes the specified master from the cluster using the CLI tool.
  // Unset 'master_to_remove' can be used to indicate to not supply master address.
  // Returns generic RuntimeError() on failure with the actual error in the optional 'err'
  // output parameter.
  Status RemoveMasterFromClusterUsingCLITool(const HostPort& master_to_remove,
                                             string* err = nullptr,
                                             const string& master_uuid = "") {
    auto hps = cluster_->master_rpc_addrs();
    vector<string> addresses;
    addresses.reserve(hps.size());
    for (const auto& hp : hps) {
      addresses.emplace_back(hp.ToString());
    }

    vector<string> args = {"master", "remove", JoinStrings(addresses, ",")};
    if (master_to_remove.Initialized()) {
      args.push_back(master_to_remove.ToString());
    }
    if (!master_uuid.empty()) {
      args.push_back("--master_uuid=" + master_uuid);
    }
    RETURN_NOT_OK(tools::RunKuduTool(args, nullptr, err));
    return cluster_->RemoveMaster(master_to_remove);
  }

  // Verify one of the 'expected_roles' and 'expected_member_type' of the new master by
  // making RPC to it directly.
  void VerifyNewMasterDirectly(const set<RaftPeerPB::Role>& expected_roles,
                               RaftPeerPB::MemberType expected_member_type) {
    GetMasterRegistrationRequestPB req;
    GetMasterRegistrationResponsePB resp;
    RpcController rpc;

    ASSERT_OK(new_master_proxy_->GetMasterRegistration(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(ContainsKey(expected_roles, resp.role()));
    ASSERT_EQ(expected_member_type, resp.member_type());
  }

  // Fetch consensus state of the leader master.
  void GetLeaderMasterConsensusState(RaftConfigPB* consensus_config) {
    int leader_master_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
    auto leader_master_addr = cluster_->master(leader_master_idx)->bound_rpc_addr();
    ConsensusServiceProxy consensus_proxy(cluster_->messenger(), leader_master_addr,
                                          leader_master_addr.host());
    consensus::GetConsensusStateRequestPB req;
    consensus::GetConsensusStateResponsePB resp;
    RpcController rpc;
    req.set_dest_uuid(cluster_->master(leader_master_idx)->uuid());
    req.set_report_health(consensus::INCLUDE_HEALTH_REPORT);
    ASSERT_OK(consensus_proxy.GetConsensusState(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_EQ(1, resp.tablets_size());

    // Lookup the new_master from the consensus state of the system catalog.
    const auto& sys_catalog = resp.tablets(0);
    ASSERT_EQ(master::SysCatalogTable::kSysCatalogTabletId, sys_catalog.tablet_id());
    const auto& cstate = sys_catalog.cstate();
    *consensus_config = cstate.has_pending_config() ?
                        cstate.pending_config() : cstate.committed_config();
  }

  // Verify the newly added master is in FAILED_UNRECOVERABLE state and can't be caught up
  // from WAL.
  void VerifyNewMasterInFailedUnrecoverableState(int expected_num_masters) {
    RaftConfigPB config;
    NO_FATALS(GetLeaderMasterConsensusState(&config));
    ASSERT_EQ(expected_num_masters, config.peers_size());
    int num_new_masters_found = 0;
    for (const auto& peer : config.peers()) {
      if (peer.permanent_uuid() == new_master_->uuid()) {
        ASSERT_EQ(HealthReportPB::FAILED_UNRECOVERABLE, peer.health_report().overall_health());
        num_new_masters_found++;
      }
    }
    ASSERT_EQ(1, num_new_masters_found);
  }

  void VerifyDeadMasterInSpecifiedState(const string& dead_master_uuid,
                                        HealthReportPB::HealthStatus expected_state) {
    RaftConfigPB config;
    NO_FATALS(GetLeaderMasterConsensusState(&config));
    ASSERT_EQ(orig_num_masters_, config.peers_size());
    bool dead_master_found = false;
    for (const auto& peer : config.peers()) {
      if (peer.permanent_uuid() == dead_master_uuid) {
        dead_master_found = true;
        ASSERT_EQ(expected_state, peer.health_report().overall_health());
        break;
      }
    }
    ASSERT_TRUE(dead_master_found);
  }

  // Initiates leadership transfer to the specified master returning status of the asynchronous
  // request.
  static Status TransferMasterLeadershipAsync(ExternalMiniCluster* cluster,
                                              const string& master_uuid) {
    LOG(INFO) << "Transferring leadership to master: " << master_uuid;

    int leader_master_idx;
    RETURN_NOT_OK(cluster->GetLeaderMasterIndex(&leader_master_idx));
    auto leader_master_addr = cluster->master(leader_master_idx)->bound_rpc_addr();
    ConsensusServiceProxy consensus_proxy(cluster->messenger(), leader_master_addr,
                                          leader_master_addr.host());
    LeaderStepDownRequestPB req;
    req.set_dest_uuid(cluster->master(leader_master_idx)->uuid());
    req.set_tablet_id(master::SysCatalogTable::kSysCatalogTabletId);
    req.set_new_leader_uuid(master_uuid);
    req.set_mode(consensus::GRACEFUL);
    LeaderStepDownResponsePB resp;
    RpcController rpc;
    RETURN_NOT_OK(consensus_proxy.LeaderStepDown(req, &resp, &rpc));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
  }

  // Transfers leadership among masters in the 'cluster' to the specified 'new_master_uuid'
  // verifies the transfer is successful.
  static void TransferMasterLeadership(ExternalMiniCluster* cluster,
                                       const string& new_master_uuid) {
    ASSERT_OK(TransferMasterLeadershipAsync(cluster, new_master_uuid));
    // LeaderStepDown request is asynchronous, hence using ASSERT_EVENTUALLY.
    ASSERT_EVENTUALLY([&] {
      int leader_master_idx = -1;
      ASSERT_OK(cluster->GetLeaderMasterIndex(&leader_master_idx));
      ASSERT_EQ(new_master_uuid, cluster->master(leader_master_idx)->uuid());
    });
  }

  // Verification steps after the new master has been added successfully and it's promoted
  // as VOTER. The supplied 'master_hps' includes the new_master as well.
  void VerifyClusterAfterMasterAddition(const vector<HostPort>& master_hps,
                                        int expected_num_masters) {
    // Collect information about the cluster for verification later before shutting
    // it down.
    UnorderedHostPortSet master_hps_set(master_hps.begin(), master_hps.end());
    ASSERT_EQ(master_hps.size(), master_hps_set.size()) << "Duplicates found in master_hps";

    unordered_set<string> master_uuids;
    for (int i = 0; i < cluster_->num_masters(); i++) {
      master_uuids.emplace(cluster_->master(i)->uuid());
    }
    master_uuids.emplace(new_master_->uuid());

    // Shutdown the cluster and the new master daemon process.
    // This allows ExternalMiniCluster to manage the newly added master and allows
    // client to connect to the new master if it's elected the leader.
    LOG(INFO) << "Shutting down the old cluster";
    new_master_->Shutdown();
    cluster_.reset();

    LOG(INFO) << "Bringing up the migrated cluster";
    opts_.num_masters = expected_num_masters;
    opts_.master_rpc_addresses = master_hps;
    ExternalMiniCluster migrated_cluster(opts_);
    ASSERT_OK(migrated_cluster.Start());
    for (int i = 0; i < migrated_cluster.num_masters(); i++) {
      ASSERT_OK(migrated_cluster.master(i)->WaitForCatalogManager());
    }

    // Verify the cluster still has the same masters.
    {
      ListMastersResponsePB resp;
      NO_FATALS(RunListMasters(&resp, &migrated_cluster));
      ASSERT_EQ(expected_num_masters, resp.masters_size());

      UnorderedHostPortSet hps_found;
      unordered_set<string> uuids_found;
      for (const auto& master : resp.masters()) {
        ASSERT_EQ(RaftPeerPB::VOTER, master.member_type());
        ASSERT_TRUE(master.role() == RaftPeerPB::LEADER || master.role() == RaftPeerPB::FOLLOWER);
        ASSERT_EQ(1, master.registration().rpc_addresses_size());
        HostPort actual_hp = HostPortFromPB(master.registration().rpc_addresses(0));
        ASSERT_TRUE(ContainsKey(master_hps_set, actual_hp));
        hps_found.insert(actual_hp);
        ASSERT_TRUE(ContainsKey(master_uuids, master.instance_id().permanent_uuid()));
        uuids_found.emplace(master.instance_id().permanent_uuid());
      }
      ASSERT_EQ(master_hps_set, hps_found);
      ASSERT_EQ(master_uuids, uuids_found);
    }

    // Transfer leadership to the new master.
    NO_FATALS(TransferMasterLeadership(&migrated_cluster, new_master_->uuid()));

    shared_ptr<KuduClient> client;
    ASSERT_OK(migrated_cluster.CreateClient(nullptr, &client));

    ClusterVerifier cv(&migrated_cluster);
    NO_FATALS(cv.CheckCluster());
    LOG(INFO) << "Verifying the first table";
    NO_FATALS(cv.CheckRowCount(kTableName, ClusterVerifier::EXACTLY, 0));

    LOG(INFO) << "Creating and verifying the second table";
    // Perform an operation that requires replication to masters.
    ASSERT_OK(CreateTable(&migrated_cluster, "second_table"));
    NO_FATALS(cv.CheckRowCount("second_table", ClusterVerifier::EXACTLY, 0));

    // Pause one master at a time and create table at the same time which will allow
    // new leader to be elected if the paused master is a leader.
    // Need at least 3 masters to form consensus and elect a new leader.
    if (expected_num_masters >= 3) {
      LOG(INFO) << "Pausing and resuming individual masters";
      string table_name = kTableName;
      for (int i = 0; i < expected_num_masters; i++) {
        auto* master = migrated_cluster.master(i);
        LOG(INFO) << Substitute("Pausing master $0, $1", master->uuid(),
                                master->bound_rpc_hostport().ToString());
        ASSERT_OK(master->Pause());
        cluster::ScopedResumeExternalDaemon resume_daemon(master);

        // We can run into table not found error in cases where the
        // previously paused master that's leader of prior term resumes
        // and the up to date follower doesn't become leader and the resumed
        // master from previous term isn't up to date. See KUDU-3266 for details.
        ASSERT_EVENTUALLY([&] {
          NO_FATALS(cv.CheckRowCount(table_name, ClusterVerifier::EXACTLY, 0));
        });

        // See MasterFailoverTest.TestCreateTableSync to understand why we must
        // check for IsAlreadyPresent as well.
        table_name = Substitute("table-$0", i);
        Status s = CreateTable(&migrated_cluster, table_name);
        ASSERT_TRUE(s.ok() || s.IsAlreadyPresent());
      }
    }
  }

  // Function to prevent leadership changes among masters for quick tests.
  void DisableMasterLeadershipTransfer() {
    for (int i = 0 ; i < cluster_->num_masters(); i++) {
      // Starting the cluster with following flag leads to a case sometimes
      // wherein no leader gets elected leading to failure in ConnectToMaster() RPC.
      // So instead set the flag after the cluster is running.
      ASSERT_OK(cluster_->SetFlag(cluster_->master(i),
                                  "leader_failure_max_missed_heartbeat_periods", "10.0"));
    }
  }

  // Helper function for recovery tests that shuts down and deletes state of a follower master
  // and removes it from the Raft configuration.
  // Output parameters:
  //   dead_master_idx: Index of the follower master selected for recovery in the
  //                    ExternalMiniCluster
  //   dead_master_hp: HostPort of the follower master selected for recovery
  //   src_master_hp: HostPort of an existing master that can be used as a source for recovery
  void FailAndRemoveFollowerMaster(const vector<HostPort>& master_hps, int* dead_master_idx,
                                   HostPort* dead_master_hp, HostPort* src_master_hp) {
    // We'll be selecting a follower master to be shutdown to simulate a dead master
    // and to prevent the test from being flaky we disable master leadership change.
    NO_FATALS(DisableMasterLeadershipTransfer());

    int master_to_recover_idx = -1;
    ASSERT_OK(GetFollowerMasterIndex(&master_to_recover_idx));
    ASSERT_NE(master_to_recover_idx, -1);
    scoped_refptr<ExternalMaster> master_to_recover(cluster_->master(master_to_recover_idx));
    const auto master_to_recover_hp = master_to_recover->bound_rpc_hostport();

    LOG(INFO) << Substitute("Shutting down and deleting the state of the master to be recovered "
                            "HostPort: $0, UUID: $1, index : $2", master_to_recover_hp.ToString(),
                            master_to_recover->uuid(), master_to_recover_idx);

    NO_FATALS(master_to_recover->Shutdown());
    ASSERT_OK(master_to_recover->DeleteFromDisk());

    LOG(INFO) << "Detecting transition to terminal FAILED state";
    ASSERT_EVENTUALLY([&] {
      VerifyDeadMasterInSpecifiedState(master_to_recover->uuid(), HealthReportPB::FAILED);
    });

    // Verify the master to be removed is part of the list of masters.
    ASSERT_NE(std::find(master_hps.begin(), master_hps.end(), master_to_recover_hp),
              master_hps.end());

    // Source master to copy system catalog out of at least 2 masters in the cluster.
    // Recovery tests start with at least 2 masters.
    // Need to capture the source master before removing master as once the master
    // is removed it won't be tracked by ExternalMiniCluster and the indices would change.
    ASSERT_GE(cluster_->num_masters(), 2);
    int src_master_idx = master_to_recover_idx == 0 ? 1 : 0;
    *src_master_hp = cluster_->master(src_master_idx)->bound_rpc_hostport();

    ASSERT_OK(RemoveMasterFromClusterUsingCLITool(master_to_recover_hp));

    // Verify we have one master less and the desired master was removed.
    vector<HostPort> updated_master_hps;
    NO_FATALS(VerifyVoterMasters(orig_num_masters_ - 1, &updated_master_hps));
    UnorderedHostPortSet expected_master_hps(master_hps.begin(), master_hps.end());
    expected_master_hps.erase(master_to_recover_hp);
    UnorderedHostPortSet actual_master_hps(updated_master_hps.begin(), updated_master_hps.end());
    ASSERT_EQ(expected_master_hps, actual_master_hps);

    ClusterVerifier cv(cluster_.get());
    NO_FATALS(cv.CheckCluster());
    NO_FATALS(cv.CheckRowCount(kTableName, ClusterVerifier::EXACTLY, 0));

    // Set the remaining output parameters.
    *dead_master_idx = master_to_recover_idx;
    *dead_master_hp = master_to_recover_hp;
  }

  // Helper function that verifies that the newly added master can't be caught up from WAL
  // and remains as NON_VOTER.
  void VerifyNewNonVoterMaster(const HostPort& new_master_hp,
                               int expected_num_masters) {
    // Newly added master will be added to the master Raft config but won't be caught up
    // from the WAL and hence remain as a NON_VOTER.
    ListMastersResponsePB list_resp;
    NO_FATALS(RunListMasters(&list_resp));
    ASSERT_EQ(expected_num_masters, list_resp.masters_size());

    for (const auto& master : list_resp.masters()) {
      ASSERT_EQ(1, master.registration().rpc_addresses_size());
      auto hp = HostPortFromPB(master.registration().rpc_addresses(0));
      if (hp == new_master_hp) {
        ASSERT_EQ(RaftPeerPB::NON_VOTER, master.member_type());
        ASSERT_TRUE(master.role() == RaftPeerPB::LEARNER);
      }
    }

    // Double check by directly contacting the new master.
    NO_FATALS(VerifyNewMasterDirectly({ RaftPeerPB::LEARNER }, RaftPeerPB::NON_VOTER));

    // Verify new master is in FAILED_UNRECOVERABLE state.
    // This health state update may take some round trips between the masters and
    // hence wrapping it under ASSERT_EVENTUALLY.
    ASSERT_EVENTUALLY([&] {
      NO_FATALS(VerifyNewMasterInFailedUnrecoverableState(expected_num_masters));
    });
  }

  // Helper function to copy system catalog from 'src_master_hp' master.
  void CopySystemCatalog(const HostPort& src_master_hp) {
    LOG(INFO) << "Shutting down the new master";
    new_master_->Shutdown();

    LOG(INFO) << "Deleting the system catalog";
    // Delete sys catalog on local master
    ASSERT_OK(tools::RunKuduTool({"local_replica", "delete",
                                  master::SysCatalogTable::kSysCatalogTabletId,
                                  "-fs_data_dirs=" + JoinStrings(new_master_->data_dirs(), ","),
                                  "-fs_wal_dir=" + new_master_->wal_dir(),
                                  "-clean_unsafe"}));

    // Copy from remote system catalog from specified master
    LOG(INFO) << "Copying from remote master: " << src_master_hp.ToString();
    ASSERT_OK(tools::RunKuduTool({"local_replica", "copy_from_remote",
                                  master::SysCatalogTable::kSysCatalogTabletId,
                                  src_master_hp.ToString(),
                                  "-fs_data_dirs=" + JoinStrings(new_master_->data_dirs(), ","),
                                  "-fs_wal_dir=" + new_master_->wal_dir()}));

    LOG(INFO) << "Restarting the new master";
    ASSERT_OK(new_master_->Restart());
  }

  // Tracks the current number of masters in the cluster
  int orig_num_masters_;
  ExternalMiniClusterOptions opts_;
  unique_ptr<ExternalMiniCluster> cluster_;

  // Socket, HostPort, proxy etc. for the new master to be added
  unique_ptr<Socket> reserved_socket_;
  Sockaddr reserved_addr_;
  HostPort reserved_hp_;
  unique_ptr<MasterServiceProxy> new_master_proxy_;
  scoped_refptr<ExternalMaster> new_master_;

  static const char* const kTableName;
};

const char* const DynamicMultiMasterTest::kTableName = "first_table";

// Parameterized DynamicMultiMasterTest class that works with different initial number of masters.
class ParameterizedAddMasterTest : public DynamicMultiMasterTest,
                                   public ::testing::WithParamInterface<int> {
 public:
  void SetUp() override {
    NO_FATALS(SetUpWithNumMasters(GetParam()));
  }
};

INSTANTIATE_TEST_SUITE_P(, ParameterizedAddMasterTest,
                         // Initial number of masters in the cluster before adding a new master
                         ::testing::Values(1, 2));

// This test starts a cluster, creates a table and then adds a new master.
// For a system catalog with little data, the new master can be caught up from WAL and
// promoted to a VOTER without requiring tablet copy.
TEST_P(ParameterizedAddMasterTest, TestAddMasterCatchupFromWAL) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  ASSERT_OK(CreateTable(cluster_.get(), kTableName));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps, reserved_hp_, orig_num_masters_));
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));
  ASSERT_OK(AddMasterToClusterUsingCLITool(reserved_hp_, nullptr, 4));

  // Newly added master will be caught up from WAL itself without requiring tablet copy
  // since the system catalog is fresh with a single table.
  // Catching up from WAL and promotion to VOTER will not be instant after adding the
  // new master. Hence using ASSERT_EVENTUALLY.
  ASSERT_EVENTUALLY([&] {
    VerifyVoterMasters(orig_num_masters_ + 1);
  });

  // Double check by directly contacting the new master.
  NO_FATALS(VerifyNewMasterDirectly(
      { RaftPeerPB::FOLLOWER, RaftPeerPB::LEADER }, RaftPeerPB::VOTER));

  // Adding the same master again should print a message but not throw an error.
  {
    string err;
    ASSERT_OK(AddMasterToClusterUsingCLITool(reserved_hp_, &err));
    ASSERT_STR_CONTAINS(err, "Master already present");
  }

  // Adding one of the former masters should print a message but not throw an error.
  {
    string err;
    ASSERT_OK(AddMasterToClusterUsingCLITool(master_hps[0], &err));
    ASSERT_STR_CONTAINS(err, "Master already present");
  }

  NO_FATALS(VerifyClusterAfterMasterAddition(master_hps, orig_num_masters_ + 1));
}

// This test goes through the workflow required to copy system catalog to the newly added master.
TEST_P(ParameterizedAddMasterTest, TestAddMasterSysCatalogCopy) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  vector<HostPort> master_hps;
  NO_FATALS(StartClusterWithSysCatalogGCed(
      &master_hps,
      {"--master_consensus_allow_status_msg_for_failed_peer"}));

  ASSERT_OK(CreateTable(cluster_.get(), kTableName));
  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps, reserved_hp_, orig_num_masters_));
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));
  string err;
  ASSERT_OK(AddMasterToClusterUsingCLITool(reserved_hp_, &err));
  ASSERT_STR_MATCHES(err, Substitute("New master $0 part of the Raft configuration.*"
                                     "Please follow the next steps which includes system catalog "
                                     "tablet copy", reserved_hp_.ToString()));

  // Newly added master will be added to the master Raft config but won't be caught up
  // from the WAL and hence remain as a NON_VOTER and transition to FAILED_UNRECOVERABLE state.
  NO_FATALS(VerifyNewNonVoterMaster(reserved_hp_, orig_num_masters_ + 1));

  // Adding the same master again should print a message but not throw an error.
  {
    string err;
    ASSERT_OK(AddMasterToClusterUsingCLITool(reserved_hp_, &err));
    ASSERT_STR_CONTAINS(err, "Master already present");
  }

  // Adding one of the former masters should print a message but not throw an error.
  {
    string err;
    ASSERT_OK(AddMasterToClusterUsingCLITool(master_hps[0], &err));
    ASSERT_STR_CONTAINS(err, "Master already present");
  }

  // Without system catalog copy, the new master will remain in the FAILED_UNRECOVERABLE state.
  // So lets proceed with the tablet copy process for system catalog.
  NO_FATALS(CopySystemCatalog(cluster_->master(0)->bound_rpc_hostport()));

  // Wait for the new master to be up and running and the leader master to send status only Raft
  // message to allow the new master to be considered caught up and promoted to be being a VOTER.
  ASSERT_EVENTUALLY([&] {
    VerifyVoterMasters(orig_num_masters_ + 1);
  });

  // Verify the same state from the new master directly.
  // Double check by directly contacting the new master.
  NO_FATALS(VerifyNewMasterDirectly(
      { RaftPeerPB::FOLLOWER, RaftPeerPB::LEADER }, RaftPeerPB::VOTER));

  NO_FATALS(VerifyClusterAfterMasterAddition(master_hps, orig_num_masters_ + 1));
}

class ParameterizedRemoveMasterTest : public DynamicMultiMasterTest,
                                      public ::testing::WithParamInterface<tuple<int, bool>> {
 public:
  void SetUp() override {
    NO_FATALS(SetUpWithNumMasters(std::get<0>(GetParam())));
  }
};

INSTANTIATE_TEST_SUITE_P(, ParameterizedRemoveMasterTest,
                         ::testing::Combine(
                             // Initial number of masters in the cluster before removing a master
                             ::testing::Values(2, 3),
                             // Whether the master to be removed is dead/shutdown
                             ::testing::Bool()));

// Tests removing a non-leader master from the cluster.
TEST_P(ParameterizedRemoveMasterTest, TestRemoveMaster) {
  NO_FATALS(StartCluster({"--master_support_change_config",
                          // Keeping RPC timeouts short to quickly detect downed servers.
                          // This will put the health status into an UNKNOWN state until the point
                          // where they are considered FAILED.
                          "--consensus_rpc_timeout_ms=2000",
                          "--follower_unavailable_considered_failed_sec=4"}));

  // Verify that existing masters are running as VOTERs.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // When an ExternalMiniCluster is restarted after removal of a master then one of the
  // remaining masters can get reassigned to the same wal dir which was previously assigned
  // to the removed master. This causes problems during verification, so we always try to
  // remove the last master in terms of index for test purposes.
  int leader_master_idx = -1;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
  ASSERT_NE(leader_master_idx, -1);
  const int non_leader_master_idx = orig_num_masters_ - 1;
  if (leader_master_idx == non_leader_master_idx) {
    // Move the leader to the first master index
    auto first_master_uuid = cluster_->master(0)->uuid();
    NO_FATALS(TransferMasterLeadership(cluster_.get(), first_master_uuid));
  }
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
  ASSERT_NE(leader_master_idx, non_leader_master_idx);
  const auto master_to_remove = cluster_->master(non_leader_master_idx)->bound_rpc_hostport();
  const auto master_to_remove_uuid = cluster_->master(non_leader_master_idx)->uuid();

  // A NO_OP operation is issued after assuming leadership so that ChangeConfig operation
  // can be issued against the new leader in the current term.
  // Don't know of a good way to wait/verify that the NO_OP operation has completed. Table
  // creation helps with a new operation in the current term and is used later for verification.
  // Hence creating a table after possible master leadership transfer and before initiating remove
  // master ChangeConfig request.
  ASSERT_OK(CreateTable(cluster_.get(), kTableName));

  bool shutdown = std::get<1>(GetParam());
  if (shutdown) {
    LOG(INFO) << "Shutting down the master to be removed";
    cluster_->master(non_leader_master_idx)->Shutdown();
    LOG(INFO) << "Detecting transition to terminal FAILED state";
    ASSERT_EVENTUALLY([&] {
      VerifyDeadMasterInSpecifiedState(master_to_remove_uuid, HealthReportPB::FAILED);
    });
  }

  // Verify the master to be removed is part of the list of masters.
  ASSERT_NE(std::find(master_hps.begin(), master_hps.end(), master_to_remove), master_hps.end());
  ASSERT_OK(RemoveMasterFromClusterUsingCLITool(master_to_remove, nullptr, master_to_remove_uuid));

  // Verify we have one master less and the desired master was removed.
  vector<HostPort> updated_master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_ - 1, &updated_master_hps));
  UnorderedHostPortSet expected_master_hps(master_hps.begin(), master_hps.end());
  expected_master_hps.erase(master_to_remove);
  UnorderedHostPortSet actual_master_hps(updated_master_hps.begin(), updated_master_hps.end());
  ASSERT_EQ(expected_master_hps, actual_master_hps);

  ClusterVerifier cv(cluster_.get());
  NO_FATALS(cv.CheckCluster());
  NO_FATALS(cv.CheckRowCount(kTableName, ClusterVerifier::EXACTLY, 0));

  // Removing the same master again should result in an error
  string err;
  Status s = RemoveMasterFromClusterUsingCLITool(master_to_remove, &err, master_to_remove_uuid);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(err, Substitute("Master $0 not found", master_to_remove.ToString()));

  // Attempt transferring leadership to the removed master
  s = TransferMasterLeadershipAsync(cluster_.get(), master_to_remove_uuid);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(),
                      Substitute("tablet server $0 is not a voter in the active config",
                                 master_to_remove_uuid));

  LOG(INFO) << "Shutting down the old cluster";
  cluster_.reset();

  LOG(INFO) << "Bringing up the migrated cluster";
  opts_.num_masters = orig_num_masters_ - 1;
  opts_.master_rpc_addresses = updated_master_hps;
  ExternalMiniCluster migrated_cluster(opts_);
  ASSERT_OK(migrated_cluster.Start());
  for (int i = 0; i < migrated_cluster.num_masters(); i++) {
    ASSERT_OK(migrated_cluster.master(i)->WaitForCatalogManager());
  }

  vector<HostPort> migrated_master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_ - 1, &migrated_master_hps,
                                            &migrated_cluster));
  actual_master_hps.clear();
  actual_master_hps.insert(migrated_master_hps.begin(), migrated_master_hps.end());
  ASSERT_EQ(expected_master_hps, actual_master_hps);

  ClusterVerifier mcv(&migrated_cluster);
  NO_FATALS(mcv.CheckCluster());
  NO_FATALS(mcv.CheckRowCount(kTableName, ClusterVerifier::EXACTLY, 0));
}

class ParameterizedRecoverMasterTest : public DynamicMultiMasterTest,
                                       public ::testing::WithParamInterface<int> {
 public:
  void SetUp() override {
    NO_FATALS(SetUpWithNumMasters(GetParam()));
  }
};

INSTANTIATE_TEST_SUITE_P(, ParameterizedRecoverMasterTest,
                         // Number of masters in a cluster
                         ::testing::Values(2, 3));

// Tests recovering a dead master at the same HostPort without explicit system catalog copy
TEST_P(ParameterizedRecoverMasterTest, TestRecoverDeadMasterCatchupfromWAL) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  NO_FATALS(StartCluster({"--master_support_change_config",
                          // Keeping RPC timeouts short to quickly detect downed servers.
                          // This will put the health status into an UNKNOWN state until the point
                          // where they are considered FAILED.
                          "--consensus_rpc_timeout_ms=2000",
                          "--follower_unavailable_considered_failed_sec=4"}));

  // Verify that existing masters are running as VOTERs.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  ASSERT_OK(CreateTable(cluster_.get(), kTableName));

  int master_to_recover_idx = -1;
  HostPort master_to_recover_hp;
  HostPort src_master_hp;
  NO_FATALS(FailAndRemoveFollowerMaster(master_hps, &master_to_recover_idx, &master_to_recover_hp,
                                        &src_master_hp));

  // Add new master at the same HostPort
  NO_FATALS(StartNewMaster(master_hps, master_to_recover_hp, master_to_recover_idx));
  NO_FATALS(VerifyVoterMasters(orig_num_masters_ - 1));
  ASSERT_OK(AddMasterToClusterUsingCLITool(master_to_recover_hp));

  // Newly added master will be caught up from WAL itself without requiring tablet copy
  // since the system catalog is fresh with a single table.
  // Catching up from WAL and promotion to VOTER will not be instant after adding the
  // new master. Hence using ASSERT_EVENTUALLY.
  ASSERT_EVENTUALLY([&] {
    VerifyVoterMasters(orig_num_masters_);
  });

  // Double check by directly contacting the new master.
  NO_FATALS(VerifyNewMasterDirectly(
      { RaftPeerPB::FOLLOWER, RaftPeerPB::LEADER }, RaftPeerPB::VOTER));

  NO_FATALS(VerifyClusterAfterMasterAddition(master_hps, orig_num_masters_));
}

// Tests recovering a dead master at the same HostPort with explicit system catalog copy
TEST_P(ParameterizedRecoverMasterTest, TestRecoverDeadMasterSysCatalogCopy) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  vector<HostPort> master_hps;
  NO_FATALS(StartClusterWithSysCatalogGCed(
      &master_hps,
      {"--master_consensus_allow_status_msg_for_failed_peer",
       // Keeping RPC timeouts short to quickly detect downed servers.
       // This will put the health status into an UNKNOWN state until the point
       // where they are considered FAILED.
       "--consensus_rpc_timeout_ms=2000",
       "--follower_unavailable_considered_failed_sec=4"}));

  // Verify that existing masters are running as VOTERs.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  ASSERT_OK(CreateTable(cluster_.get(), kTableName));

  int master_to_recover_idx = -1;
  HostPort master_to_recover_hp;
  HostPort src_master_hp;
  NO_FATALS(FailAndRemoveFollowerMaster(master_hps, &master_to_recover_idx, &master_to_recover_hp,
                                        &src_master_hp));

  // Add new master at the same HostPort
  NO_FATALS(StartNewMaster(master_hps, master_to_recover_hp, master_to_recover_idx));
  NO_FATALS(VerifyVoterMasters(orig_num_masters_ - 1));
  string err;
  ASSERT_OK(AddMasterToClusterUsingCLITool(master_to_recover_hp, &err));
  ASSERT_STR_MATCHES(err, Substitute("New master $0 part of the Raft configuration.*"
                                     "Please follow the next steps which includes system catalog "
                                     "tablet copy", master_to_recover_hp.ToString()));

  // Verify the new master will remain as NON_VOTER and transition to FAILED_UNRECOVERABLE state.
  NO_FATALS(VerifyNewNonVoterMaster(master_to_recover_hp, orig_num_masters_));

  // Without system catalog copy, the new master will remain in the FAILED_UNRECOVERABLE state.
  // So lets proceed with the tablet copy process for system catalog.
  NO_FATALS(CopySystemCatalog(src_master_hp));

  // Wait for the new master to be up and running and the leader master to send status only Raft
  // message to allow the new master to be considered caught up and promoted to be being a VOTER.
  ASSERT_EVENTUALLY([&] {
    VerifyVoterMasters(orig_num_masters_);
  });

  VerifyNewMasterDirectly({ RaftPeerPB::FOLLOWER, RaftPeerPB::LEADER }, RaftPeerPB::VOTER);

  NO_FATALS(VerifyClusterAfterMasterAddition(master_hps, orig_num_masters_));
}

// Test that brings up a single master cluster with 'last_known_addr' not populated by
// not specifying '--master_addresses' and then attempts to add a new master which is
// expected to fail due to invalid Raft config.
TEST_F(DynamicMultiMasterTest, TestAddMasterWithNoLastKnownAddr) {
  NO_FATALS(SetUpWithNumMasters(1));
  NO_FATALS(
      StartCluster({"--master_support_change_config"}, false /* supply_single_master_addr */));

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps, reserved_hp_, orig_num_masters_));
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  string err;
  Status actual = AddMasterToClusterUsingCLITool(reserved_hp_, &err);
  ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
  ASSERT_STR_MATCHES(err, "Invalid config to set as pending: Peer:.* has no address");

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));
}

// Test that attempts to add a new master without enabling the feature flag for master Raft
// change config.
TEST_F(DynamicMultiMasterTest, TestAddMasterFeatureFlagNotSpecified) {
  NO_FATALS(SetUpWithNumMasters(1));
  NO_FATALS(StartCluster({ "--master_support_change_config=false" }));

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps, reserved_hp_, orig_num_masters_,
                           false /* master_supports_change_config */));
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  string err;
  Status actual = AddMasterToClusterUsingCLITool(reserved_hp_, &err);
  ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
  ASSERT_STR_MATCHES(err, "Cluster does not support AddMaster");

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));
}

// Test that attempts to remove an existing master without enabling the feature flag for master
// Raft change config.
TEST_F(DynamicMultiMasterTest, TestRemoveMasterFeatureFlagNotSpecified) {
  NO_FATALS(SetUpWithNumMasters(2));
  NO_FATALS(StartCluster({"--master_support_change_config=false"}));

  // Verify that existing masters are running as VOTERs.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // Try removing non-leader master.
  {
    int non_leader_master_idx = -1;
    ASSERT_OK(GetFollowerMasterIndex(&non_leader_master_idx));
    auto master_to_remove = cluster_->master(non_leader_master_idx)->bound_rpc_hostport();
    string err;
    Status s = RemoveMasterFromClusterUsingCLITool(master_to_remove, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_MATCHES(err, "Cluster does not support RemoveMaster");
  }

  // Try removing leader master
  {
    int leader_master_idx = -1;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
    auto master_to_remove = cluster_->master(leader_master_idx)->bound_rpc_hostport();
    string err;
    Status s = RemoveMasterFromClusterUsingCLITool(master_to_remove, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_MATCHES(err, "Cluster does not support RemoveMaster");
  }

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));
}

// Test that attempts to request a non-leader master to add a new master.
TEST_F(DynamicMultiMasterTest, TestAddMasterToNonLeader) {
  NO_FATALS(SetUpWithNumMasters(2));
  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps, reserved_hp_, orig_num_masters_));
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  // Verify sending add master request to a non-leader master returns NOT_THE_LEADER error.
  // It's possible there is a leadership change between querying for leader master and
  // sending the add master request to non-leader master and hence using ASSERT_EVENTUALLY.
  ASSERT_EVENTUALLY([&] {
    AddMasterRequestPB req;
    AddMasterResponsePB resp;
    RpcController rpc;
    *req.mutable_rpc_addr() = HostPortToPB(reserved_hp_);
    rpc.RequireServerFeature(MasterFeatures::DYNAMIC_MULTI_MASTER);

    int leader_master_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
    ASSERT_TRUE(leader_master_idx == 0 || leader_master_idx == 1);
    int non_leader_master_idx = !leader_master_idx;
    ASSERT_OK(cluster_->master_proxy(non_leader_master_idx)->AddMaster(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_error());
    ASSERT_EQ(MasterErrorPB::NOT_THE_LEADER, resp.error().code());
  });

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));
}

// Test that attempts to request a non-leader master to remove a master.
TEST_F(DynamicMultiMasterTest, TestRemoveMasterToNonLeader) {
  NO_FATALS(SetUpWithNumMasters(2));
  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // In test below we use the master RPC directly to the non-leader master and a retry
  // will have unintended consequences hence disabling master leadership transfer.
  NO_FATALS(DisableMasterLeadershipTransfer());

  // Verify sending remove master request to a non-leader master returns NOT_THE_LEADER error.
  RemoveMasterRequestPB req;
  RemoveMasterResponsePB resp;
  RpcController rpc;

  int leader_master_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
  ASSERT_TRUE(leader_master_idx == 0 || leader_master_idx == 1);
  int non_leader_master_idx = !leader_master_idx;
  *req.mutable_rpc_addr() = HostPortToPB(cluster_->master(leader_master_idx)->bound_rpc_hostport());
  rpc.RequireServerFeature(MasterFeatures::DYNAMIC_MULTI_MASTER);
  // Using the master proxy directly instead of using CLI as this test wants to test
  // invoking RemoveMaster RPC to non-leader master.
  ASSERT_OK(cluster_->master_proxy(non_leader_master_idx)->RemoveMaster(req, &resp, &rpc));
  ASSERT_TRUE(resp.has_error());
  ASSERT_EQ(MasterErrorPB::NOT_THE_LEADER, resp.error().code());

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));
}

// Test that attempts to add a master with missing master address and a non-routable incorrect
// address.
TEST_F(DynamicMultiMasterTest, TestAddMasterMissingAndIncorrectAddress) {
  NO_FATALS(SetUpWithNumMasters(1));
  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps, reserved_hp_, orig_num_masters_));
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  // Empty HostPort
  {
    string err;
    Status actual = AddMasterToClusterUsingCLITool(HostPort(), &err);
    ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
    ASSERT_STR_CONTAINS(err, "must provide positional argument master_address");
  }

  // Non-routable incorrect hostname.
  {
    string err;
    Status actual = AddMasterToClusterUsingCLITool(
        HostPort("non-existent-path.local", Master::kDefaultPort), &err);
    ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
    ASSERT_STR_CONTAINS(err,
                        "Network error: unable to resolve address for non-existent-path.local");
  }

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));
}

// Test that attempts to remove a master with missing master address and a non-existent
// hostname.
TEST_F(DynamicMultiMasterTest, TestRemoveMasterMissingAndIncorrectHostname) {
  NO_FATALS(SetUpWithNumMasters(2));
  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that existing masters are running as VOTERs.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // Empty HostPort.
  {
    string err;
    Status actual = RemoveMasterFromClusterUsingCLITool(HostPort(), &err);
    ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
    ASSERT_STR_CONTAINS(err, "must provide positional argument master_address");
  }

  // Non-existent hostname.
  {
    HostPort dummy_hp = HostPort("non-existent-path.local", Master::kDefaultPort);
    string err;
    Status actual = RemoveMasterFromClusterUsingCLITool(dummy_hp, &err);
    ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
    ASSERT_STR_CONTAINS(err, Substitute("Master $0 not found", dummy_hp.ToString()));
  }

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));
}

// Test that attempts to remove a master with mismatching hostname and uuid.
TEST_F(DynamicMultiMasterTest, TestRemoveMasterMismatchHostnameAndUuid) {
  NO_FATALS(SetUpWithNumMasters(2));
  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that existing masters are running as VOTERs.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // Master leadership transfer could result in a different error and hence disabling it.
  NO_FATALS(DisableMasterLeadershipTransfer());

  // Random uuid
  Random rng(SeedRandom());
  auto random_uuid = std::to_string(rng.Next64());
  int non_leader_idx = -1;
  ASSERT_OK(GetFollowerMasterIndex(&non_leader_idx));
  auto master_to_remove = cluster_->master(non_leader_idx)->bound_rpc_hostport();
  ASSERT_NE(random_uuid, cluster_->master(non_leader_idx)->uuid());
  string err;
  Status actual = RemoveMasterFromClusterUsingCLITool(master_to_remove, &err, random_uuid);
  ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
  ASSERT_STR_CONTAINS(err,
                      Substitute("Mismatch in UUID of the master $0 to be removed. "
                                 "Expected: $1, supplied: $2.", master_to_remove.ToString(),
                                 cluster_->master(non_leader_idx)->uuid(), random_uuid));

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));
}

// Test that attempts removing a leader master itself from a cluster with
// 1 or 2 masters.
class ParameterizedRemoveLeaderMasterTest : public DynamicMultiMasterTest,
                                            public ::testing::WithParamInterface<int> {
 public:
  void SetUp() override {
    NO_FATALS(SetUpWithNumMasters(GetParam()));
  }
};

INSTANTIATE_TEST_SUITE_P(, ParameterizedRemoveLeaderMasterTest,
                         ::testing::Values(1, 2));

TEST_P(ParameterizedRemoveLeaderMasterTest, TestRemoveLeaderMaster) {
  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // In test below a retry in case of master leadership transfer will have unintended
  // consequences and hence disabling master leadership transfer.
  NO_FATALS(DisableMasterLeadershipTransfer());

  int leader_master_idx;
  ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
  const auto master_to_remove = cluster_->master(leader_master_idx)->bound_rpc_hostport();
  string err;
  Status s = RemoveMasterFromClusterUsingCLITool(master_to_remove, &err);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  if (orig_num_masters_ == 1) {
    ASSERT_STR_CONTAINS(err, Substitute("Can't remove master $0 in a single master "
                                        "configuration", master_to_remove.ToString()));
  } else {
    ASSERT_GT(orig_num_masters_, 1);
    ASSERT_STR_CONTAINS(err, Substitute("Can't remove the leader master $0",
                                        master_to_remove.ToString()));
  }

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));
}

} // namespace master
} // namespace kudu
