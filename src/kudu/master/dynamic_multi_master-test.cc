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
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
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
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_histogram(log_gc_duration);
METRIC_DECLARE_entity(tablet);

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalDaemonOptions;
using kudu::cluster::ExternalMaster;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::MiniCluster;
using kudu::rpc::RpcController;
using std::set;
using std::string;
using std::unique_ptr;
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
 public:
  void SetUpWithNumMasters(int num_masters) {
    // Initial number of masters in the cluster before adding a master.
    orig_num_masters_ = num_masters;

    // Reserving a port upfront for the new master that'll be added to the cluster.
    ASSERT_OK(MiniCluster::ReserveDaemonSocket(MiniCluster::MASTER, orig_num_masters_ /* index */,
                                               kDefaultBindMode, &reserved_socket_));

    ASSERT_OK(reserved_socket_->GetSocketAddress(&reserved_addr_));
    reserved_hp_ = HostPort(reserved_addr_);
    reserved_hp_str_ = reserved_hp_.ToString();
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

  // Verify the cluster contains 'num_masters' and returns the master addresses in 'master_hps'.
  void VerifyNumMastersAndGetAddresses(int num_masters, vector<HostPort>* master_hps) {
    ListMastersResponsePB resp;
    NO_FATALS(RunListMasters(&resp));
    ASSERT_EQ(num_masters, resp.masters_size());
    master_hps->clear();
    for (const auto& master : resp.masters()) {
      ASSERT_TRUE(master.role() == consensus::RaftPeerPB::LEADER ||
                  master.role() == consensus::RaftPeerPB::FOLLOWER);
      ASSERT_EQ(consensus::RaftPeerPB::VOTER, master.member_type());
      ASSERT_EQ(1, master.registration().rpc_addresses_size());
      master_hps->emplace_back(HostPortFromPB(master.registration().rpc_addresses(0)));
    }
  }

  // Brings up a new master where 'master_hps' contains master addresses including
  // the new master to be added.
  void StartNewMaster(const vector<HostPort>& master_hps,
                      bool master_supports_change_config = true) {
    vector<string> master_addresses;
    master_addresses.reserve(master_hps.size());
    for (const auto& hp : master_hps) {
      master_addresses.emplace_back(hp.ToString());
    }

    // Start with an existing master daemon's options, but modify them for use in a new master
    ExternalDaemonOptions new_master_opts = cluster_->master(0)->opts();
    const string new_master_id = Substitute("master-$0", orig_num_masters_);
    new_master_opts.wal_dir = cluster_->GetWalPath(new_master_id);
    new_master_opts.data_dirs = cluster_->GetDataPaths(new_master_id);
    new_master_opts.log_dir = cluster_->GetLogPath(new_master_id);
    new_master_opts.rpc_bind_address = reserved_hp_;
    auto& flags = new_master_opts.extra_flags;
    flags.insert(flags.end(),
                 {"--master_addresses=" + JoinStrings(master_addresses, ","),
                  "--master_address_add_new_master=" + reserved_hp_str_});

    LOG(INFO) << "Bringing up the new master at: " << reserved_hp_str_;
    new_master_.reset(new ExternalMaster(new_master_opts));
    ASSERT_OK(new_master_->Start());
    ASSERT_OK(new_master_->WaitForCatalogManager());

    new_master_proxy_.reset(
        new MasterServiceProxy(new_master_opts.messenger, reserved_addr_, reserved_addr_.host()));
    {
      GetMasterRegistrationRequestPB req;
      GetMasterRegistrationResponsePB resp;
      RpcController rpc;

      ASSERT_OK(new_master_proxy_->GetMasterRegistration(req, &resp, &rpc));
      ASSERT_FALSE(resp.has_error());
      if (master_supports_change_config) {
        ASSERT_EQ(consensus::RaftPeerPB::NON_VOTER, resp.member_type());
        ASSERT_EQ(consensus::RaftPeerPB::LEARNER, resp.role());
      } else {
        // For a new master brought that doesn't support change config, it'll be started
        // as a VOTER and become FOLLOWER if the other masters are reachable.
        ASSERT_EQ(consensus::RaftPeerPB::VOTER, resp.member_type());
        ASSERT_EQ(consensus::RaftPeerPB::FOLLOWER, resp.role());
      }
    }

    // Verify the cluster still has the same number of masters.
    ListMastersResponsePB resp;
    NO_FATALS(RunListMasters(&resp));
    ASSERT_EQ(orig_num_masters_, resp.masters_size());
  }

  // Adds the specified master to the cluster returning the appropriate error Status for negative
  // test cases.
  Status AddMasterToCluster(const HostPort& master) {
    auto add_master = [&] (int leader_master_idx) {
      AddMasterRequestPB req;
      AddMasterResponsePB resp;
      RpcController rpc;
      if (master != HostPort()) {
        *req.mutable_rpc_addr() = HostPortToPB(master);
      }
      rpc.RequireServerFeature(MasterFeatures::DYNAMIC_MULTI_MASTER);
      Status s = cluster_->master_proxy(leader_master_idx)->AddMaster(req, &resp, &rpc);
      boost::optional<MasterErrorPB::Code> err_code(resp.has_error(), resp.error().code());
      return std::make_pair(s, err_code);
    };

    RETURN_NOT_OK(RunLeaderMasterRPC(add_master));
    return cluster_->AddMaster(new_master_);
  }

  // Verify one of the 'expected_roles' and 'expected_member_type' of the new master by
  // making RPC to it directly.
  void VerifyNewMasterDirectly(const set<consensus::RaftPeerPB::Role>& expected_roles,
                               consensus::RaftPeerPB::MemberType expected_member_type) {
    GetMasterRegistrationRequestPB req;
    GetMasterRegistrationResponsePB resp;
    RpcController rpc;

    ASSERT_OK(new_master_proxy_->GetMasterRegistration(req, &resp, &rpc));
    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(ContainsKey(expected_roles, resp.role()));
    ASSERT_EQ(expected_member_type, resp.member_type());
  }

 protected:
  // Tracks the current number of masters in the cluster
  int orig_num_masters_;
  ExternalMiniClusterOptions opts_;
  unique_ptr<ExternalMiniCluster> cluster_;

  // Socket, HostPort, proxy etc. for the new master to be added
  unique_ptr<Socket> reserved_socket_;
  Sockaddr reserved_addr_;
  HostPort reserved_hp_;
  string reserved_hp_str_;
  unique_ptr<MasterServiceProxy> new_master_proxy_;
  scoped_refptr<ExternalMaster> new_master_;
};

// Parameterized DynamicMultiMasterTest class that works with different initial number of masters.
class ParameterizedDynamicMultiMasterTest : public DynamicMultiMasterTest,
                                            public ::testing::WithParamInterface<int> {
 public:
  void SetUp() override {
    NO_FATALS(SetUpWithNumMasters(GetParam()));
  }
};

INSTANTIATE_TEST_CASE_P(, ParameterizedDynamicMultiMasterTest,
                        // Initial number of masters in the cluster before adding a new master
                        ::testing::Values(1, 2));

// This test starts a cluster, creates a table and then adds a new master.
// For a system catalog with little data, the new master can be caught up from WAL and
// promoted to a VOTER without requiring tablet copy.
TEST_P(ParameterizedDynamicMultiMasterTest, TestAddMasterCatchupFromWAL) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));

  const string kTableName = "first_table";
  ASSERT_OK(CreateTable(cluster_.get(), kTableName));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps));
  ASSERT_OK(AddMasterToCluster(reserved_hp_));

  // Newly added master will be caught up from WAL itself without requiring tablet copy
  // since the system catalog is fresh with a single table.
  // Catching up from WAL and promotion to VOTER will not be instantly after adding the
  // new master. Hence using ASSERT_EVENTUALLY.
  ASSERT_EVENTUALLY([&] {
    ListMastersResponsePB resp;
    NO_FATALS(RunListMasters(&resp));
    ASSERT_EQ(orig_num_masters_ + 1, resp.masters_size());

    int num_leaders = 0;
    for (const auto& master : resp.masters()) {
      ASSERT_EQ(consensus::RaftPeerPB::VOTER, master.member_type());
      ASSERT_TRUE(master.role() == consensus::RaftPeerPB::LEADER ||
                  master.role() == consensus::RaftPeerPB::FOLLOWER);
      if (master.role() == consensus::RaftPeerPB::LEADER) {
        num_leaders++;
      }
    }
    ASSERT_EQ(1, num_leaders);
  });

  // Double check by directly contacting the new master.
  VerifyNewMasterDirectly({ consensus::RaftPeerPB::FOLLOWER, consensus::RaftPeerPB::LEADER },
                          consensus::RaftPeerPB::VOTER);

  // Adding the same master again should return an error.
  {
    Status s = AddMasterToCluster(reserved_hp_);
    ASSERT_TRUE(s.IsRemoteError());
    ASSERT_STR_CONTAINS(s.message().ToString(), "Master already present");
  }

  // Adding one of the former masters should return an error.
  {
    Status s = AddMasterToCluster(master_hps[0]);
    ASSERT_TRUE(s.IsRemoteError());
    ASSERT_STR_CONTAINS(s.message().ToString(), "Master already present");
  }

  // Shutdown the cluster and the new master daemon process.
  // This allows ExternalMiniCluster to manage the newly added master and allows
  // client to connect to the new master if it's elected the leader.
  new_master_->Shutdown();
  cluster_.reset();

  opts_.num_masters = orig_num_masters_ + 1;
  opts_.master_rpc_addresses = master_hps;
  ExternalMiniCluster migrated_cluster(opts_);
  ASSERT_OK(migrated_cluster.Start());
  ASSERT_OK(migrated_cluster.WaitForTabletServerCount(migrated_cluster.num_tablet_servers(),
                                                      MonoDelta::FromSeconds(5)));

  // Verify the cluster still has the same 3 masters.
  {
    ListMastersResponsePB resp;
    NO_FATALS(RunListMasters(&resp, &migrated_cluster));
    ASSERT_EQ(orig_num_masters_ + 1, resp.masters_size());

    for (const auto& master : resp.masters()) {
      ASSERT_EQ(consensus::RaftPeerPB::VOTER, master.member_type());
      ASSERT_TRUE(master.role() == consensus::RaftPeerPB::LEADER ||
                  master.role() == consensus::RaftPeerPB::FOLLOWER);
      ASSERT_EQ(1, master.registration().rpc_addresses_size());
      HostPort actual_hp = HostPortFromPB(master.registration().rpc_addresses(0));
      ASSERT_TRUE(std::find(master_hps.begin(), master_hps.end(), actual_hp) != master_hps.end());
    }
  }

  shared_ptr<KuduClient> client;
  ASSERT_OK(migrated_cluster.CreateClient(nullptr, &client));

  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_EQ(0, CountTableRows(table.get()));

  // Perform an operation that requires replication to masters.
  ASSERT_OK(CreateTable(&migrated_cluster, "second_table"));
  ASSERT_OK(client->OpenTable("second_table", &table));
  ASSERT_EQ(0, CountTableRows(table.get()));

  // Pause master one at a time and create table at the same time which will allow
  // new leader to be elected if the paused master is a leader.
  // Need at least 3 masters to form consensus and elect a new leader.
  if (orig_num_masters_ + 1 >= 3) {
    for (int i = 0; i < orig_num_masters_ + 1; i++) {
      ASSERT_OK(migrated_cluster.master(i)->Pause());
      cluster::ScopedResumeExternalDaemon resume_daemon(migrated_cluster.master(i));
      ASSERT_OK(client->OpenTable(kTableName, &table));
      ASSERT_EQ(0, CountTableRows(table.get()));

      // See MasterFailoverTest.TestCreateTableSync to understand why we must
      // check for IsAlreadyPresent as well.
      Status s = CreateTable(&migrated_cluster, Substitute("table-$0", i));
      ASSERT_TRUE(s.ok() || s.IsAlreadyPresent());
    }
  }
}

// This test starts a cluster with low values for log flush and segment size to force GC
// of the system catalog WAL. When a new master is added, test verifies that the new master
// can't be caught up from WAL and as a result the new master, though added to the master Raft
// config, remains a NON_VOTER.
TEST_P(ParameterizedDynamicMultiMasterTest, TestAddMasterCatchupFromWALNotPossible) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  // Using low values of log flush threshold and segment size to trigger GC of the sys catalog WAL.
  NO_FATALS(StartCluster({"--master_support_change_config", "--flush_threshold_secs=1",
                          "--log_segment_size_mb=1"}));

  // Verify that masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));

  auto get_sys_catalog_wal_gc_count = [&] {
    int64_t sys_catalog_wal_gc_count = 0;
    CHECK_OK(itest::GetInt64Metric(cluster_->master(0)->bound_http_hostport(),
                                   &METRIC_ENTITY_tablet,
                                   master::SysCatalogTable::kSysCatalogTabletId,
                                   &METRIC_log_gc_duration,
                                   "total_count",
                                   &sys_catalog_wal_gc_count));
    return sys_catalog_wal_gc_count;
  };
  auto orig_gc_count = get_sys_catalog_wal_gc_count();

  // Create a bunch of tables to ensure sys catalog WAL gets GC'ed.
  // Need to create around 1k tables even with lowest flush threshold and log segment size.
  for (int i = 1; i < 1000; i++) {
    string table_name = "Table.TestAddMasterCatchupFromWALNotPossible." + std::to_string(i);
    ASSERT_OK(CreateTable(cluster_.get(), table_name));
  }

  int64_t time_left_to_sleep_msecs = 2000;
  while (time_left_to_sleep_msecs > 0 && orig_gc_count == get_sys_catalog_wal_gc_count()) {
    static const MonoDelta kSleepDuration = MonoDelta::FromMilliseconds(100);
    SleepFor(kSleepDuration);
    time_left_to_sleep_msecs -= kSleepDuration.ToMilliseconds();
  }
  ASSERT_GT(time_left_to_sleep_msecs, 0) << "Timed out waiting for system catalog WAL to be GC'ed";

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps));
  ASSERT_OK(AddMasterToCluster(reserved_hp_));

  // Newly added master will be added to the master Raft config but won't be caught up
  // from the WAL and hence remain as a NON_VOTER.
  ListMastersResponsePB list_resp;
  NO_FATALS(RunListMasters(&list_resp));
  ASSERT_EQ(orig_num_masters_ + 1, list_resp.masters_size());

  for (const auto& master : list_resp.masters()) {
    ASSERT_EQ(1, master.registration().rpc_addresses_size());
    auto hp = HostPortFromPB(master.registration().rpc_addresses(0));
    if (hp == reserved_hp_) {
      ASSERT_EQ(consensus::RaftPeerPB::NON_VOTER, master.member_type());
      ASSERT_TRUE(master.role() == consensus::RaftPeerPB::LEARNER);
    }
  }

  // Double check by directly contacting the new master.
  VerifyNewMasterDirectly({ consensus::RaftPeerPB::LEARNER }, consensus::RaftPeerPB::NON_VOTER);

  // Verify FAILED_UNRECOVERABLE health error about the new master that can't be caught up
  // from WAL. This health state update may take some round trips between the masters and
  // hence wrapping it under ASSERT_EVENTUALLY.
  ASSERT_EVENTUALLY([&] {
    // GetConsensusState() RPC can be made to any master and not necessarily the leader master.
    int leader_master_idx;
    ASSERT_OK(cluster_->GetLeaderMasterIndex(&leader_master_idx));
    auto leader_master_addr = cluster_->master(leader_master_idx)->bound_rpc_addr();
    consensus::ConsensusServiceProxy consensus_proxy(cluster_->messenger(), leader_master_addr,
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
    const auto& config = cstate.has_pending_config() ?
        cstate.pending_config() : cstate.committed_config();
    ASSERT_EQ(orig_num_masters_ + 1, config.peers_size());
    int num_new_masters_found = 0;
    for (const auto& peer : config.peers()) {
      if (peer.permanent_uuid() == new_master_->uuid()) {
        ASSERT_EQ(consensus::HealthReportPB::FAILED_UNRECOVERABLE,
                  peer.health_report().overall_health());
        num_new_masters_found++;
      }
    }
    ASSERT_EQ(1, num_new_masters_found);
  });
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
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps));

  Status actual = AddMasterToCluster(reserved_hp_);
  ASSERT_TRUE(actual.IsRemoteError());
  ASSERT_STR_MATCHES(actual.ToString(),
                     "Invalid config to set as pending: Peer:.* has no address");

  // Verify no change in number of masters.
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));
}

// Test that attempts to add a new master without enabling the feature flag for master Raft
// change config.
TEST_F(DynamicMultiMasterTest, TestAddMasterFeatureFlagNotSpecified) {
  NO_FATALS(SetUpWithNumMasters(1));
  NO_FATALS(StartCluster({ /* Omitting "--master_support_change_config" */ }));

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps, false /* master_supports_change_config */));

  Status actual = AddMasterToCluster(reserved_hp_);
  ASSERT_TRUE(actual.IsRemoteError());
  ASSERT_STR_MATCHES(actual.ToString(), "unsupported feature flags");

  // Verify no change in number of masters.
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));
}

// Test that attempts to request a non-leader master to add a new master.
TEST_F(DynamicMultiMasterTest, TestAddMasterToNonLeader) {
  NO_FATALS(SetUpWithNumMasters(2));
  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps));

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
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));
}

// Test that attempts to add a master with missing master address and a non-routable incorrect
// address.
TEST_F(DynamicMultiMasterTest, TestAddMasterMissingAndIncorrectAddress) {
  NO_FATALS(SetUpWithNumMasters(1));
  NO_FATALS(StartCluster({"--master_support_change_config"}));

  // Verify that existing masters are running as VOTERs and collect their addresses to be used
  // for starting the new master.
  vector<HostPort> master_hps;
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));

  // Bring up the new master and add to the cluster.
  master_hps.emplace_back(reserved_hp_);
  NO_FATALS(StartNewMaster(master_hps));

  // Empty HostPort
  Status actual = AddMasterToCluster(HostPort());
  ASSERT_TRUE(actual.IsRemoteError());
  ASSERT_STR_CONTAINS(actual.ToString(), "RPC address of master to be added not supplied");

  // Non-routable incorrect hostname.
  actual = AddMasterToCluster(HostPort("foo", Master::kDefaultPort));
  ASSERT_TRUE(actual.IsRemoteError());
  ASSERT_STR_CONTAINS(actual.ToString(),
                      "Network error: unable to resolve address for foo");

  // Verify no change in number of masters.
  NO_FATALS(VerifyNumMastersAndGetAddresses(orig_num_masters_, &master_hps));
}

} // namespace master
} // namespace kudu
