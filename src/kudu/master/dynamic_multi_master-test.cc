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
#include <atomic>
#include <cstdint>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
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
#include "kudu/fs/dir_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_string(fs_wal_dir);
DECLARE_string(fs_data_dirs);

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
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcController;
using std::atomic;
using std::map;
using std::string;
using std::thread;
using std::tuple;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

namespace {
Status CreateTableWithClient(KuduClient* client, const std::string& table_name) {
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
Status CreateTable(ExternalMiniCluster* cluster,
                   const std::string& table_name) {
  shared_ptr<KuduClient> client;
  RETURN_NOT_OK(cluster->CreateClient(nullptr, &client));
  return CreateTableWithClient(client.get(), table_name);
}

Status ReserveSocketForMaster(int master_idx, unique_ptr<Socket>* socket,
                              Sockaddr* addr, HostPort* hp) {
  unique_ptr<Socket> s;
  Sockaddr a;
  RETURN_NOT_OK(MiniCluster::ReserveDaemonSocket(MiniCluster::MASTER, master_idx,
                                                 kDefaultBindMode, &s));
  RETURN_NOT_OK(s->GetSocketAddress(&a));
  *socket = std::move(s);
  *addr = a;
  *hp = HostPort(a);
  return Status::OK();
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
Status RunLeaderMasterRPC(const MasterRPC& master_rpc, ExternalMiniCluster* cluster) {
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

// Run ListMasters RPC, retrying on leadership change, returning the response
// in 'resp'.
Status RunListMasters(ListMastersResponsePB* resp, ExternalMiniCluster* cluster) {
  auto list_masters = [&] (int leader_master_idx) {
    ListMastersRequestPB req;
    RpcController rpc;
    Status s = cluster->master_proxy(leader_master_idx)->ListMasters(req, resp, &rpc);
    boost::optional<MasterErrorPB::Code> err_code(resp->has_error(), resp->error().code());
    return std::make_pair(s, err_code);
  };
  return RunLeaderMasterRPC(list_masters, cluster);
}

// Verify the ExternalMiniCluster 'cluster' contains 'num_masters' overall and
// are all VOTERS. Populates the new master addresses in 'master_hps', if not
// nullptr. Returns an error if the expected state is not present.
Status VerifyVoterMastersForCluster(int num_masters, vector<HostPort>* master_hps,
                                    ExternalMiniCluster* cluster) {
  ListMastersResponsePB resp;
  RETURN_NOT_OK(RunListMasters(&resp, cluster));
  if (num_masters != resp.masters_size()) {
    return Status::IllegalState(Substitute("expected $0 masters but got $1",
                                           num_masters, resp.masters_size()));
  }
  vector<HostPort> hps;
  for (const auto& master : resp.masters()) {
    if ((master.role() != RaftPeerPB::LEADER && master.role() != RaftPeerPB::FOLLOWER) ||
        master.member_type() != RaftPeerPB::VOTER ||
        master.registration().rpc_addresses_size() != 1) {
      return Status::IllegalState(Substitute("bad master: $0", SecureShortDebugString(master)));
    }
    hps.emplace_back(HostPortFromPB(master.registration().rpc_addresses(0)));
  }
  if (master_hps) {
    *master_hps = std::move(hps);
  }
  return Status::OK();
}

// Initiates leadership transfer to the specified master returning status of
// the request. The request is performed synchronously, though the transfer of
// leadership is asynchronous -- callers need to wait to ensure leadership is
// actually transferred.
Status TransferMasterLeadershipAsync(ExternalMiniCluster* cluster, const string& master_uuid) {
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
void TransferMasterLeadership(ExternalMiniCluster* cluster, const string& new_master_uuid) {
  ASSERT_OK(TransferMasterLeadershipAsync(cluster, new_master_uuid));
  // It takes some time for the leadership transfer to complete, hence the
  // ASSERT_EVENTUALLY.
  ASSERT_EVENTUALLY([&] {
    int leader_master_idx = -1;
    ASSERT_OK(cluster->GetLeaderMasterIndex(&leader_master_idx));
    ASSERT_EQ(new_master_uuid, cluster->master(leader_master_idx)->uuid());
  });
}

// Fetch uuid of the specified 'fs_wal_dir' and 'fs_data_dirs'.
Status GetFsUuid(const string& fs_wal_dir, const vector<string>& fs_data_dirs, string* uuid) {
  google::FlagSaver saver;
  FLAGS_fs_wal_dir = fs_wal_dir;
  FLAGS_fs_data_dirs = JoinStrings(fs_data_dirs, ",");
  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  fs_opts.update_instances = fs::UpdateInstanceBehavior::DONT_UPDATE;
  FsManager fs_manager(Env::Default(), std::move(fs_opts));
  RETURN_NOT_OK(fs_manager.PartialOpen());
  *uuid = fs_manager.uuid();
  return Status::OK();
}

} // anonymous namespace

// Test class for testing addition/removal of masters to a Kudu cluster.
class DynamicMultiMasterTest : public KuduTest {
 protected:
  typedef map<string, string> EnvVars;

  void SetUpWithNumMasters(int num_masters) {
    // Initial number of masters in the cluster before adding a master.
    orig_num_masters_ = num_masters;

    // Reserving a port upfront for the new master that'll be added to the cluster.
    ASSERT_OK(ReserveSocketForMaster(/*index*/orig_num_masters_, &reserved_socket_,
                                     &reserved_addr_, &reserved_hp_));
  }

  void StartCluster(const vector<string>& extra_master_flags = {},
                    bool supply_single_master_addr = true) {
    opts_.num_masters = orig_num_masters_;
    opts_.supply_single_master_addr = supply_single_master_addr;
    opts_.extra_master_flags = extra_master_flags;
    opts_.extra_master_flags.emplace_back("--master_auto_join_cluster=false");

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
    vector<string> flags = {"--flush_threshold_secs=0", "--log_segment_size_mb=1"};
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

  // Brings up a new master 'new_master_hp' where 'master_hps' contains master addresses including
  // the new master to be added at the index 'new_master_idx' in the ExternalMiniCluster.
  void StartNewMaster(const vector<HostPort>& master_hps,
                      const HostPort& new_master_hp,
                      int new_master_idx,
                      scoped_refptr<ExternalMaster>* new_master_out) {
    vector<string> master_addresses;
    master_addresses.reserve(master_hps.size());
    for (const auto& hp : master_hps) {
      master_addresses.emplace_back(hp.ToString());
    }

    ExternalDaemonOptions new_master_opts;
    ASSERT_OK(BuildMasterOpts(new_master_idx, new_master_hp, &new_master_opts));
    auto& flags = new_master_opts.extra_flags;
    flags.insert(flags.end(), {
        "--master_addresses=" + JoinStrings(master_addresses, ","),
        "--master_address_add_new_master=" + new_master_hp.ToString(),
        "--master_auto_join_cluster=false",
    });

    LOG(INFO) << "Bringing up the new master at: " << new_master_hp.ToString();
    scoped_refptr<ExternalMaster> master = new ExternalMaster(new_master_opts);
    ASSERT_OK(master->Start());
    ASSERT_OK(master->WaitForCatalogManager());

    Sockaddr new_master_addr;
    ASSERT_OK(SockaddrFromHostPort(new_master_hp, &new_master_addr));
    MasterServiceProxy new_master_proxy(new_master_opts.messenger, new_master_addr,
                                        new_master_hp.host());

    GetMasterRegistrationRequestPB req;
    GetMasterRegistrationResponsePB resp;
    RpcController rpc;
    ASSERT_OK(new_master_proxy.GetMasterRegistration(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_EQ(RaftPeerPB::NON_VOTER, resp.member_type());
    ASSERT_EQ(RaftPeerPB::LEARNER, resp.role());
    *new_master_out = std::move(master);
  }

  void VerifyVoterMasters(int num_masters, vector<HostPort>* master_hps = nullptr,
                          ExternalMiniCluster* cluster = nullptr) {
    if (cluster == nullptr) {
      cluster = cluster_.get();
    }
    NO_FATALS(VerifyVoterMastersForCluster(num_masters, master_hps, cluster));
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

  // Builds and returns ExternalDaemonOptions used to add master with address 'rpc_addr' and
  // 'master_idx' which helps with naming master's directories.
  // Output 'kerberos_env_vars' parameter must be supplied for a secure cluster i.e. when Kerberos
  // is enabled and returns Kerberos related environment variables.
  Status BuildMasterOpts(int master_idx, const HostPort& rpc_addr,
                         ExternalDaemonOptions* master_opts,
                         EnvVars* kerberos_env_vars = nullptr) {
    // Start with an existing master daemon's options, but modify them for use in a new master.
    const string new_master_id = Substitute("master-$0", master_idx);
    ExternalDaemonOptions opts = cluster_->master(0)->opts();
    opts.rpc_bind_address = rpc_addr;
    opts.wal_dir = cluster_->GetWalPath(new_master_id);
    opts.data_dirs = cluster_->GetDataPaths(new_master_id);
    opts.log_dir = cluster_->GetLogPath(new_master_id);
    if (opts_.enable_kerberos) {
      CHECK(kerberos_env_vars);
      vector<string> flags;
      RETURN_NOT_OK(cluster::ExternalDaemon::CreateKerberosConfig(
          cluster_->kdc(), opts_.principal, rpc_addr.host(), &flags, kerberos_env_vars));
      // Inserting the Kerberos related flags at the end will override flags from the master
      // which was used as a basis for the new master.
      opts.extra_flags.insert(opts.extra_flags.end(), std::make_move_iterator(flags.begin()),
                              std::make_move_iterator(flags.end()));
    }
    *master_opts = std::move(opts);
    return Status::OK();
  }

  // Adds the specified master to the cluster using the CLI tool.
  // Unset 'rpc_bind_address' in 'opts' can be used to indicate to not supply master address.
  // Optional 'env_vars' can be used to set environment variables while running kudu tool.
  // Optional 'wait_secs' can be used to supply wait timeout to the master add CLI tool.
  // Optional 'kudu_binary' can be used to supply the path to the kudu binary.
  // Returns generic RuntimeError() on failure with the actual error in the optional 'err'
  // output parameter.
  Status AddMasterToClusterUsingCLITool(const ExternalDaemonOptions& opts, string* err = nullptr,
                                        EnvVars env_vars = {}, int wait_secs = 10,
                                        const string& kudu_binary = "") {
    auto hps = cluster_->master_rpc_addrs();
    vector<string> addresses;
    addresses.reserve(hps.size());
    for (const auto& hp : hps) {
      addresses.emplace_back(hp.ToString());
    }

    vector<string> cmd = {"master", "add", JoinStrings(addresses, ",")};
    if (opts.rpc_bind_address.Initialized()) {
      cmd.emplace_back(opts.rpc_bind_address.ToString());
    }

    vector<string> new_master_flags = ExternalMaster::GetMasterFlags(opts);
    cmd.insert(cmd.end(), std::make_move_iterator(new_master_flags.begin()),
               std::make_move_iterator(new_master_flags.end()));
    cmd.insert(cmd.end(), opts.extra_flags.begin(), opts.extra_flags.end());

    if (wait_secs != 0) {
      cmd.emplace_back("-wait_secs=" + std::to_string(wait_secs));
    }
    if (!kudu_binary.empty()) {
      cmd.emplace_back("-kudu_abs_path=" + kudu_binary);
    }

    RETURN_NOT_OK(env_util::CreateDirsRecursively(Env::Default(), opts.log_dir));
    Status s = tools::RunKuduTool(cmd, nullptr, err, "", std::move(env_vars));
    if (!s.ok() && err != nullptr) {
      LOG(INFO) << "Add master stderr: " << *err;
    }
    RETURN_NOT_OK(s);
    return Status::OK();
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
    string new_master_uuid;
    ASSERT_OK(GetFsUuid(new_master_opts_.wal_dir, new_master_opts_.data_dirs, &new_master_uuid));
    master_uuids.emplace(new_master_uuid);

    // Shutdown the cluster and the new master daemon process.
    // This allows ExternalMiniCluster to manage the newly added master and allows
    // client to connect to the new master if it's elected the leader.
    LOG(INFO) << "Shutting down the old cluster";
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
      ASSERT_OK(RunListMasters(&resp, &migrated_cluster));
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
    LOG(INFO) << "Transferring leadership to master: " << new_master_uuid;
    NO_FATALS(TransferMasterLeadership(&migrated_cluster, new_master_uuid));

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

  // Tracks the current number of masters in the cluster
  int orig_num_masters_;
  ExternalMiniClusterOptions opts_;
  unique_ptr<ExternalMiniCluster> cluster_;

  // Socket, HostPort, proxy etc. for the new master to be added
  unique_ptr<Socket> reserved_socket_;
  Sockaddr reserved_addr_;
  HostPort reserved_hp_;
  ExternalDaemonOptions new_master_opts_;

  static const char* const kTableName;
};

const char* const DynamicMultiMasterTest::kTableName = "first_table";

// Parameterized DynamicMultiMasterTest class that works with different initial number of masters
// and secure/unsecure clusters.
class ParameterizedAddMasterTest : public DynamicMultiMasterTest,
                                   public ::testing::WithParamInterface<tuple<int, bool>> {
 public:
  void SetUp() override {
    NO_FATALS(SetUpWithNumMasters(std::get<0>(GetParam())));
    opts_.enable_kerberos = std::get<1>(GetParam());
  }
};

INSTANTIATE_TEST_SUITE_P(, ParameterizedAddMasterTest,
                           ::testing::Combine(
                               // Initial number of masters in the cluster before adding a new
                               // master
                               ::testing::Values(1, 2),
                               // Whether Kerberos is enabled
                               ::testing::Bool()));

// This test starts a cluster, creates a table and then adds a new master.
// For a system catalog with little data, the new master can be caught up from WAL and
// promoted to a VOTER without requiring tablet copy.
TEST_P(ParameterizedAddMasterTest, TestAddMasterCatchupFromWAL) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  NO_FATALS(StartCluster());

  // Verify that masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  ASSERT_OK(CreateTable(cluster_.get(), kTableName));

  // Add new master to the cluster.
  {
    string err;
    EnvVars env_vars;
    ASSERT_OK(BuildMasterOpts(orig_num_masters_, reserved_hp_, &new_master_opts_, &env_vars));
    ASSERT_OK(AddMasterToClusterUsingCLITool(new_master_opts_, &err, std::move(env_vars),
                                             4 /* wait_secs */, tools::GetKuduToolAbsolutePath()));
    ASSERT_STR_CONTAINS(err, Substitute("Master $0 successfully caught up from WAL.",
                                        new_master_opts_.rpc_bind_address.ToString()));
  }

  // Adding the same master again should print a message but not throw an error.
  {
    string err;
    ExternalDaemonOptions opts;
    EnvVars env_vars;
    ASSERT_OK(BuildMasterOpts(orig_num_masters_, reserved_hp_, &opts, &env_vars));
    ASSERT_OK(AddMasterToClusterUsingCLITool(opts, &err, std::move(env_vars)));
    ASSERT_STR_CONTAINS(err, "Master already present");
  }

  // Adding one of the running former masters will throw an error.
  {
    string err;
    const auto& hp = master_hps[0];
    ExternalDaemonOptions opts;
    EnvVars env_vars;
    ASSERT_OK(BuildMasterOpts(0, hp, &opts, &env_vars));
    Status s = AddMasterToClusterUsingCLITool(opts, &err, std::move(env_vars));
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(err, Substitute("Master $0 already running", hp.ToString()));
  }

  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(VerifyClusterAfterMasterAddition(master_hps, orig_num_masters_ + 1));
}

// This test goes through the workflow required to copy system catalog to the newly added master.
TEST_P(ParameterizedAddMasterTest, TestAddMasterSysCatalogCopy) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  vector<HostPort> master_hps;
  NO_FATALS(StartClusterWithSysCatalogGCed(&master_hps));

  ASSERT_OK(CreateTable(cluster_.get(), kTableName));

  // Add new master to the cluster.
  {
    string err;
    EnvVars env_vars;
    ASSERT_OK(BuildMasterOpts(orig_num_masters_, reserved_hp_, &new_master_opts_, &env_vars));
    ASSERT_OK(AddMasterToClusterUsingCLITool(new_master_opts_, &err, std::move(env_vars),
                                             4 /* wait_secs */));
    ASSERT_STR_CONTAINS(err, Substitute("Master $0 could not be caught up from WAL.",
                                        reserved_hp_.ToString()));
    ASSERT_STR_CONTAINS(err, "Successfully copied system catalog and new master is healthy");
  }

  // Adding the same master again should print a message but not throw an error.
  {
    string err;
    ExternalDaemonOptions opts;
    EnvVars env_vars;
    ASSERT_OK(BuildMasterOpts(orig_num_masters_, reserved_hp_, &opts, &env_vars));
    ASSERT_OK(AddMasterToClusterUsingCLITool(opts, &err, std::move(env_vars)));
    ASSERT_STR_CONTAINS(err, "Master already present");
  }

  // Adding one of the running former masters will throw an error.
  {
    string err;
    const auto& hp = master_hps[0];
    ExternalDaemonOptions opts;
    EnvVars env_vars;
    ASSERT_OK(BuildMasterOpts(0, hp, &opts, &env_vars));
    Status s = AddMasterToClusterUsingCLITool(opts, &err, std::move(env_vars));
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(err, Substitute("Master $0 already running", hp.ToString()));
  }

  master_hps.emplace_back(reserved_hp_);
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
  NO_FATALS(StartCluster({// Keeping RPC timeouts short to quickly detect downed servers.
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
    LOG(INFO) << "Transferring leadership to master: " << first_master_uuid;
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
  LOG(INFO) << "Transferring leadership to master: " << master_to_remove_uuid;
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

  NO_FATALS(StartCluster({// Keeping RPC timeouts short to quickly detect downed servers.
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
  NO_FATALS(VerifyVoterMasters(orig_num_masters_ - 1));

  // Add new master at the same HostPort
  {
    string err;
    ASSERT_OK(BuildMasterOpts(master_to_recover_idx, master_to_recover_hp, &new_master_opts_));
    ASSERT_OK(AddMasterToClusterUsingCLITool(new_master_opts_, &err));
    ASSERT_STR_CONTAINS(err, Substitute("Master $0 successfully caught up from WAL.",
                                        new_master_opts_.rpc_bind_address.ToString()));
  }

  NO_FATALS(VerifyClusterAfterMasterAddition(master_hps, orig_num_masters_));
}

// Tests recovering a dead master at the same HostPort with explicit system catalog copy
TEST_P(ParameterizedRecoverMasterTest, TestRecoverDeadMasterSysCatalogCopy) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  vector<HostPort> master_hps;
  NO_FATALS(StartClusterWithSysCatalogGCed(
      &master_hps,
      // Keeping RPC timeouts short to quickly detect downed servers.
      // This will put the health status into an UNKNOWN state until the point
      // where they are considered FAILED.
      {"--consensus_rpc_timeout_ms=2000",
       "--follower_unavailable_considered_failed_sec=4"}));

  // Verify that existing masters are running as VOTERs.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  ASSERT_OK(CreateTable(cluster_.get(), kTableName));

  int master_to_recover_idx = -1;
  HostPort master_to_recover_hp;
  HostPort src_master_hp;
  NO_FATALS(FailAndRemoveFollowerMaster(master_hps, &master_to_recover_idx, &master_to_recover_hp,
                                        &src_master_hp));
  NO_FATALS(VerifyVoterMasters(orig_num_masters_ - 1));

  // Add new master at the same HostPort
  string err;
  ASSERT_OK(BuildMasterOpts(master_to_recover_idx, master_to_recover_hp, &new_master_opts_));
  ASSERT_OK(AddMasterToClusterUsingCLITool(new_master_opts_, &err));
  ASSERT_STR_CONTAINS(err, Substitute("Master $0 could not be caught up from WAL.",
                                      master_to_recover_hp.ToString()));
  ASSERT_STR_CONTAINS(err, "Successfully copied system catalog and new master is healthy");

  NO_FATALS(VerifyClusterAfterMasterAddition(master_hps, orig_num_masters_));
}

// Test that brings up a single master cluster with 'last_known_addr' not populated by
// not specifying '--master_addresses' and then attempts to add a new master which is
// expected to fail due to invalid Raft config.
TEST_F(DynamicMultiMasterTest, TestAddMasterWithNoLastKnownAddr) {
  NO_FATALS(SetUpWithNumMasters(1));
  NO_FATALS(StartCluster({}, false/* supply_single_master_addr */));

  // Verify that existing masters are running as VOTERs.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  // Add master to the cluster.
  string err;
  ExternalDaemonOptions opts;
  ASSERT_OK(BuildMasterOpts(orig_num_masters_, reserved_hp_, &opts));
  Status actual = AddMasterToClusterUsingCLITool(opts, &err);
  ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
  ASSERT_STR_MATCHES(err, "'last_known_addr' field in single master Raft configuration not set. "
                          "Please restart master with --master_addresses flag");

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));
}

// Test that attempts to add a new master without enabling the feature flag for master Raft
// change config.
TEST_F(DynamicMultiMasterTest, TestAddMasterFeatureFlagNotSpecified) {
  NO_FATALS(SetUpWithNumMasters(1));
  NO_FATALS(StartCluster({ "--master_support_change_config=false" }));

  // Verify that existing masters are running as VOTERs.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  // Add master to the cluster.
  string err;
  ExternalDaemonOptions opts;
  ASSERT_OK(BuildMasterOpts(orig_num_masters_, reserved_hp_, &opts));
  Status actual = AddMasterToClusterUsingCLITool(opts, &err);
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
  NO_FATALS(StartCluster());

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyVoterMasters(orig_num_masters_, &master_hps));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  scoped_refptr<ExternalMaster> master;
  NO_FATALS(StartNewMaster(master_hps, reserved_hp_, orig_num_masters_, &master));
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
  NO_FATALS(StartCluster());

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
  NO_FATALS(StartCluster());

  // Verify that existing masters are running as VOTERs.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  // Empty HostPort
  {
    string err;
    ExternalDaemonOptions opts;
    ASSERT_OK(BuildMasterOpts(orig_num_masters_, HostPort(), &opts));
    Status actual = AddMasterToClusterUsingCLITool(opts, &err);
    ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
    ASSERT_STR_CONTAINS(err, "must provide positional argument master_address");
  }

  // Non-routable incorrect hostname.
  {
    string err;
    ExternalDaemonOptions opts;
    ASSERT_OK(BuildMasterOpts(orig_num_masters_,
                              HostPort("non-existent-path.local", Master::kDefaultPort), &opts));
    Status actual = AddMasterToClusterUsingCLITool(opts, &err);
    ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
    ASSERT_STR_CONTAINS(err, "unable to resolve address for non-existent-path.local");
  }

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));
}

// Test that attempts to remove a master with missing master address and a non-existent
// hostname.
TEST_F(DynamicMultiMasterTest, TestRemoveMasterMissingAndIncorrectHostname) {
  NO_FATALS(SetUpWithNumMasters(2));
  NO_FATALS(StartCluster());

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
  NO_FATALS(StartCluster());

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

// Test that attempts to add a master with non-existent kudu executable path.
TEST_F(DynamicMultiMasterTest, TestAddMasterIncorrectKuduBinary) {
  NO_FATALS(SetUpWithNumMasters(1));
  NO_FATALS(StartCluster());

  // Verify that existing masters are running as VOTERs.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));

  // Add master to the cluster.
  string err;
  string kudu_abs_path = "/tmp/path/to/nowhere";
  ExternalDaemonOptions opts;
  ASSERT_OK(BuildMasterOpts(orig_num_masters_, reserved_hp_, &opts));
  Status actual = AddMasterToClusterUsingCLITool(opts, &err, {} /* env_vars */, 4, kudu_abs_path);
  ASSERT_TRUE(actual.IsRuntimeError()) << actual.ToString();
  ASSERT_STR_CONTAINS(err, Substitute("kudu binary not found at $0", kudu_abs_path));

  // Verify no change in number of masters.
  NO_FATALS(VerifyVoterMasters(orig_num_masters_));
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
  NO_FATALS(StartCluster());

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

struct MultiMasterClusterArgs {
  int orig_num_masters;
  bool is_secure;
};

class AutoAddMasterTest : public KuduTest {
 public:
  Status SetUpWithTestArgs(const MultiMasterClusterArgs& args) {
    opts_.num_masters = args.orig_num_masters;
    opts_.enable_kerberos = args.is_secure;
    args_ = args;
    cluster_.reset(new ExternalMiniCluster(opts_));
    RETURN_NOT_OK(cluster_->Start());
    return cluster_->WaitForTabletServerCount(cluster_->num_tablet_servers(),
                                              MonoDelta::FromSeconds(10));
  }
  void SetUp() override {
    ASSERT_OK(SetUpWithTestArgs({ /*orig_num_masters*/2, /*is_secure*/false }));
    TestWorkload w(cluster_.get());
    w.set_num_replicas(1);
    w.Setup();
  }
 protected:
  MultiMasterClusterArgs args_;
  ExternalMiniClusterOptions opts_;
  unique_ptr<ExternalMiniCluster> cluster_;
};

constexpr const int64_t kShortRetryIntervalSecs = 1;

// Test that nothing goes wrong when starting up masters but the entire cluster
// isn't fully healthy. The auto-add checks should still run, but should be
// inconsequential if they fail because the entire cluster isn't healthy.
TEST_F(AutoAddMasterTest, TestRestartMastersWhileSomeDown) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  // We'll start with three masters, and then restart two, leaving one down.
  ASSERT_OK(cluster_->AddMaster());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(VerifyVoterMastersForCluster(cluster_->num_masters(), nullptr, cluster_.get()));
  });

  // Emulate one of the masters going down by only restarting two.
  cluster_->Shutdown();
  for (int i = 1; i < cluster_->num_masters(); i++) {
    ASSERT_OK(cluster_->master(i)->Restart());
  }
  int table_idx = 0;
  constexpr const char* kTablePrefix = "default.table";
  const auto& deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  while (MonoTime::Now() > deadline) {
    SleepFor(MonoDelta::FromSeconds(1));
    // Nothing sinister should happen despite one master being down. The
    // remaining masters should be operable and alive.
    ASSERT_OK(CreateTable(cluster_.get(), Substitute("$0-$1", kTablePrefix, ++table_idx)));
    for (int i = 1; i < cluster_->num_masters(); i++) {
      ASSERT_TRUE(cluster_->master(i)->IsProcessAlive());
    }
  }
  ASSERT_OK(cluster_->master(0)->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(VerifyVoterMastersForCluster(cluster_->num_masters(), nullptr, cluster_.get()));
  });
}

// Test the procedure when some masters aren't reachable.
TEST_F(AutoAddMasterTest, TestSomeMastersUnreachable) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  auto* stopped_master = cluster_->master(0);
  ASSERT_OK(stopped_master->Pause());
  // Adding a master to a cluster wherein a master is already down will fail.
  // This is similar behavior to starting a new master while some are down
  // since the new master can't resolve all peers' UUIDs. Shorten the time
  // masters will wait to communicate to all peers to speed up this test.
  ASSERT_OK(cluster_->AddMaster({ "--raft_get_node_instance_timeout_ms=3000" }));
  auto* new_master = cluster_->master(args_.orig_num_masters);
  ASSERT_EVENTUALLY([&] {
    ASSERT_FALSE(new_master->IsProcessAlive());
  });
  ASSERT_OK(stopped_master->Resume());

  // Even after restarting, we still won't be quite able to start healthily
  // because our previous crashes will have left an unusable set of metadata
  // (i.e. no consensus metadata).
  new_master->Shutdown();
  ASSERT_OK(new_master->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_FALSE(new_master->IsProcessAlive());
  });
  // If we blow away our new master and start anew, we should be able to
  // proceed.
  new_master->Shutdown();
  ASSERT_OK(new_master->DeleteFromDisk());
  ASSERT_OK(new_master->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(VerifyVoterMastersForCluster(cluster_->num_masters(), nullptr, cluster_.get()));
  });
  // Ensure that even after waiting a bit, our cluster is stable.
  SleepFor(MonoDelta::FromSeconds(3));
  ASSERT_OK(VerifyVoterMastersForCluster(cluster_->num_masters(), nullptr, cluster_.get()));
}

// Test if we fail to replicate the AddMaster request.
TEST_F(AutoAddMasterTest, TestFailWithoutReplicatingAddMaster) {
  // Make master followers unable to accept updates, including config changes.
  // We'll set this for all masters including leaders for simplicity.
  for (int i = 0; i < cluster_->num_masters(); i++) {
    ASSERT_OK(cluster_->SetFlag(cluster_->master(i),
                                "follower_reject_update_consensus_requests", "true"));
  }
  // Upon starting, the master will attempt to add itself, but fail to do so.
  // Even after several attempts.
  ASSERT_OK(cluster_->AddMaster({ Substitute("--master_auto_join_retry_interval_secs=$0",
                                             kShortRetryIntervalSecs) }));
  auto* new_master = cluster_->master(args_.orig_num_masters);
  SleepFor(MonoDelta::FromSeconds(5 * kShortRetryIntervalSecs));

  // The new master should still be around, but not be added as a part of the
  // Raft group.
  ASSERT_TRUE(new_master->IsProcessAlive());
  Status s = VerifyVoterMastersForCluster(cluster_->num_masters(), nullptr, cluster_.get());
  ASSERT_FALSE(s.ok());
  ASSERT_STR_CONTAINS(s.ToString(), "expected 3 masters but got 2");

  // Since nothing was successfully replicated, it shouldn't be a problem to
  // start up again and re-add.
  new_master->Shutdown();
  for (int i = 0; i < cluster_->num_masters() - 1; i++) {
    ASSERT_OK(cluster_->SetFlag(cluster_->master(i),
                                "follower_reject_update_consensus_requests", "false"));
  }
  ASSERT_OK(new_master->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(VerifyVoterMastersForCluster(cluster_->num_masters(), nullptr, cluster_.get()));
  });
}

// Test when the new master fails to copy.
TEST_F(AutoAddMasterTest, TestFailTabletCopy) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  ASSERT_OK(cluster_->AddMaster({ "--tablet_copy_fault_crash_during_download_wal=1" }));
  auto* new_master = cluster_->master(args_.orig_num_masters);
  ASSERT_EVENTUALLY([&] {
    ASSERT_FALSE(new_master->IsProcessAlive());
  });
  // We should have been able to add the master to the Raft quorum, but been
  // able to copy. Upon doing so, the new master should fail to come up.
  new_master->Shutdown();
  new_master->mutable_flags()->emplace_back("--tablet_copy_fault_crash_during_download_wal=0");
  ASSERT_OK(new_master->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_FALSE(new_master->IsProcessAlive());
  });

  // Even blowing the new master away entirely will result in a new master
  // being unable to join. The cluster already believes there to be a new
  // master, but no live majority, so we're unable to add _another_ master.
  ASSERT_OK(new_master->DeleteFromDisk());
  new_master->Shutdown();
  ASSERT_OK(new_master->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_FALSE(new_master->IsProcessAlive());
  });
  new_master->Shutdown();

  // So, we first need to remove the master from the quorum, and then restart,
  // at which point the new master should be able to join the cluster.
  vector<string> addresses;
  for (const auto& hp : cluster_->master_rpc_addrs()) {
    addresses.emplace_back(hp.ToString());
  }
  // TODO(awong): we should really consider automating this step from the
  // leader master.
  ASSERT_OK(tools::RunKuduTool({ "master", "remove", JoinStrings(addresses, ","),
                                 new_master->bound_rpc_hostport().ToString() }));
  ASSERT_OK(new_master->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(VerifyVoterMastersForCluster(cluster_->num_masters(), nullptr, cluster_.get()));
  });
  SleepFor(MonoDelta::FromSeconds(3));
  ASSERT_OK(VerifyVoterMastersForCluster(cluster_->num_masters(), nullptr, cluster_.get()));
}

TEST_F(AutoAddMasterTest, TestAddWithOnGoingDdl) {
  simple_spinlock master_addrs_lock;
  vector<string> master_addrs_unlocked;
  for (const auto& hp : cluster_->master_rpc_addrs()) {
    master_addrs_unlocked.emplace_back(hp.ToString());
  }

  // Start a thread that creates a client and tries to create tables.
  const auto generate_client = [&] (shared_ptr<KuduClient>* c) {
    vector<string> master_addrs;
    {
      std::lock_guard<simple_spinlock> l(master_addrs_lock);
      master_addrs = master_addrs_unlocked;
    }
    shared_ptr<KuduClient> client;
    RETURN_NOT_OK(client::KuduClientBuilder()
        .master_server_addrs(master_addrs)
        .Build(&client));
    *c = std::move(client);
    return Status::OK();
  };

  atomic<bool> proceed = true;
  constexpr const int kNumThreads = 2;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  vector<Status> errors(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, i] {
      int idx = 0;
      while (proceed) {
        client::sp::shared_ptr<KuduClient> c;
        Status s = generate_client(&c).AndThen([&] {
          return CreateTableWithClient(c.get(), Substitute("default.$0_$1", i, ++idx));
        });
        if (!s.ok()) {
          errors[i] = s;
        }
        SleepFor(MonoDelta::FromSeconds(1));
      }
    });
  }
  auto thread_joiner = MakeScopedCleanup([&] {
    proceed = false;
    for (auto& t : threads) {
      t.join();
    }
  });

  int num_masters = args_.orig_num_masters;
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(cluster_->AddMaster());
    auto* new_master = cluster_->master(args_.orig_num_masters);
    ASSERT_OK(new_master->WaitForCatalogManager());
    num_masters++;
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(VerifyVoterMastersForCluster(num_masters, nullptr, cluster_.get()));
    });
    {
      std::lock_guard<simple_spinlock> l(master_addrs_lock);
      master_addrs_unlocked.emplace_back(new_master->bound_rpc_hostport().ToString());
    }
    cluster_->Shutdown();
    ASSERT_OK(cluster_->Restart());
    ASSERT_OK(cluster_->WaitForTabletServerCount(cluster_->num_tablet_servers(),
                                                 MonoDelta::FromSeconds(5)));
  }
  proceed = false;
  thread_joiner.cancel();
  for (auto& t : threads) {
    t.join();
  }
  for (const auto& e : errors) {
    if (e.ok() || e.IsTimedOut()) {
      continue;
    }
    // TODO(awong): we should relax the need for clients to have the precise
    // list of masters.
    if (e.IsConfigurationError()) {
      ASSERT_STR_CONTAINS(e.ToString(), "cluster indicates it expects");
      continue;
    }
    // TODO(KUDU-1358): we should probably allow clients to retry if the RF is
    // within some normal-looking range.
    ASSERT_TRUE(e.IsInvalidArgument()) << e.ToString();
    ASSERT_STR_CONTAINS(e.ToString(), "not enough live tablet servers");
  }
}

class ParameterizedAutoAddMasterTest : public AutoAddMasterTest,
                                       public ::testing::WithParamInterface<tuple<int, bool>> {
 public:
  void SetUp() override {
    ASSERT_OK(SetUpWithTestArgs({ /*orig_num_masters*/std::get<0>(GetParam()),
                                  /*is_secure*/std::get<1>(GetParam()) }));
  }
};

TEST_P(ParameterizedAutoAddMasterTest, TestBasicAddition) {
  TestWorkload w(cluster_.get());
  w.set_num_replicas(1);
  w.Setup();
  w.Start();
  int num_masters = args_.orig_num_masters;
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(cluster_->AddMaster());
    auto* new_master = cluster_->master(args_.orig_num_masters);
    ASSERT_OK(new_master->WaitForCatalogManager());
    num_masters++;
    ASSERT_EVENTUALLY([&] {
      ASSERT_OK(VerifyVoterMastersForCluster(num_masters, nullptr, cluster_.get()));
    });
  }
  w.StopAndJoin();
  ClusterVerifier cv(cluster_.get());
  NO_FATALS(cv.CheckCluster());
  NO_FATALS(cv.CheckRowCount(w.kDefaultTableName, ClusterVerifier::EXACTLY, w.rows_inserted()));

  cluster_->Shutdown();
  ASSERT_OK(cluster_->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(VerifyVoterMastersForCluster(num_masters, nullptr, cluster_.get()));
  });

  NO_FATALS(cv.CheckCluster());
  NO_FATALS(cv.CheckRowCount(w.kDefaultTableName, ClusterVerifier::EXACTLY, w.rows_inserted()));
}

INSTANTIATE_TEST_SUITE_P(,
    ParameterizedAutoAddMasterTest, ::testing::Combine(
                                        ::testing::Values(1, 2),
                                        ::testing::Bool()),
    [] (const ::testing::TestParamInfo<ParameterizedAutoAddMasterTest::ParamType>& info) {
      return Substitute("$0_orig_masters_$1secure", std::get<0>(info.param),
                        std::get<1>(info.param) ? "" : "not_");
    });

} // namespace master
} // namespace kudu
