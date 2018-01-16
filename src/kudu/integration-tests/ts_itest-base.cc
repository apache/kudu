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

#include "kudu/integration-tests/ts_itest-base.h"

#include <algorithm>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(consensus_rpc_timeout_ms);

DEFINE_string(ts_flags, "", "Flags to pass through to tablet servers");
DEFINE_string(master_flags, "", "Flags to pass through to masters");

DEFINE_int32(num_tablet_servers, 3, "Number of tablet servers to start");
DEFINE_int32(num_replicas, 3, "Number of replicas per tablet server");

using kudu::client::sp::shared_ptr;
using kudu::itest::TServerDetails;
using kudu::cluster::ExternalTabletServer;
using std::pair;
using std::set;
using std::string;
using std::unordered_multimap;
using std::unordered_set;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {
namespace tserver {

static const int kMaxRetries = 20;

TabletServerIntegrationTestBase::
TabletServerIntegrationTestBase()
    : random_(SeedRandom()) {
}

void TabletServerIntegrationTestBase::SetUp() {
  TabletServerTestBase::SetUp();
}

void TabletServerIntegrationTestBase::AddExtraFlags(
    const string& flags_str, vector<string>* flags) {
  if (flags_str.empty()) {
    return;
  }
  vector<string> split_flags = Split(flags_str, " ");
  for (const string& flag : split_flags) {
    flags->push_back(flag);
  }
}

void TabletServerIntegrationTestBase::CreateCluster(
    const string& cluster_root_path,
    const vector<string>& non_default_ts_flags,
    const vector<string>& non_default_master_flags,
    uint32_t num_data_dirs) {

  LOG(INFO) << "Starting cluster with:";
  LOG(INFO) << "--------------";
  LOG(INFO) << FLAGS_num_tablet_servers << " tablet servers";
  LOG(INFO) << FLAGS_num_replicas << " replicas per TS";
  LOG(INFO) << "--------------";

  cluster::ExternalMiniClusterOptions opts;
  opts.num_tablet_servers = FLAGS_num_tablet_servers;
  opts.cluster_root = GetTestPath(cluster_root_path);
  opts.num_data_dirs = num_data_dirs;

  // Enable exactly once semantics for tests.

  // If the caller passed no flags use the default ones, where we stress
  // consensus by setting low timeouts and frequent cache misses.
  if (non_default_ts_flags.empty()) {
    opts.extra_tserver_flags.emplace_back("--log_cache_size_limit_mb=10");
    opts.extra_tserver_flags.push_back(
        Substitute("--consensus_rpc_timeout_ms=$0",
                   FLAGS_consensus_rpc_timeout_ms));
  } else {
    for (const string& flag : non_default_ts_flags) {
      opts.extra_tserver_flags.push_back(flag);
    }
  }
  for (const string& flag : non_default_master_flags) {
    opts.extra_master_flags.push_back(flag);
  }

  AddExtraFlags(FLAGS_ts_flags, &opts.extra_tserver_flags);
  AddExtraFlags(FLAGS_master_flags, &opts.extra_master_flags);

  cluster_.reset(new cluster::ExternalMiniCluster(std::move(opts)));
  ASSERT_OK(cluster_->Start());
  inspect_.reset(new itest::ExternalMiniClusterFsInspector(cluster_.get()));
  CreateTSProxies();
}

// Creates TSServerDetails instance for each TabletServer and stores them
// in 'tablet_servers_'.
void TabletServerIntegrationTestBase::CreateTSProxies() {
  CHECK(tablet_servers_.empty());
  CHECK_OK(itest::CreateTabletServerMap(cluster_->master_proxy(),
                                        client_messenger_,
                                        &tablet_servers_));
}

// Waits that all replicas for a all tablets of 'table_id' table are online
// and creates the tablet_replicas_ map.
void TabletServerIntegrationTestBase::WaitForReplicasAndUpdateLocations(
    const string& table_id) {
  bool replicas_missing = true;
  for (int num_retries = 0; replicas_missing && num_retries < kMaxRetries; num_retries++) {
    unordered_multimap<string, TServerDetails*> tablet_replicas;
    master::GetTableLocationsRequestPB req;
    master::GetTableLocationsResponsePB resp;
    rpc::RpcController controller;
    req.mutable_table()->set_table_name(table_id);
    req.set_replica_type_filter(master::ANY_REPLICA);
    controller.set_timeout(MonoDelta::FromSeconds(1));
    CHECK_OK(cluster_->master_proxy()->GetTableLocations(req, &resp, &controller));
    CHECK_OK(controller.status());
    if (resp.has_error()) {
      switch (resp.error().code()) {
        case master::MasterErrorPB::TABLET_NOT_RUNNING:
          LOG(WARNING)<< "At least one tablet is not yet running";
          break;

        case master::MasterErrorPB::NOT_THE_LEADER:   // fallthrough
        case master::MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED:
          LOG(WARNING)<< "CatalogManager is not yet ready to serve requests";
          break;

        default:
          FAIL() << "Response had a fatal error: "
                 << pb_util::SecureShortDebugString(resp.error());
          break;  // unreachable
      }
      SleepFor(MonoDelta::FromSeconds(1));
      continue;
    }

    for (const master::TabletLocationsPB& location : resp.tablet_locations()) {
      for (const master::TabletLocationsPB_ReplicaPB& replica : location.replicas()) {
        TServerDetails* server =
            FindOrDie(tablet_servers_, replica.ts_info().permanent_uuid());
        tablet_replicas.insert(pair<string, TServerDetails*>(
            location.tablet_id(), server));
      }

      if (tablet_replicas.count(location.tablet_id()) < FLAGS_num_replicas) {
        LOG(WARNING)<< "Couldn't find the leader and/or replicas. Location: "
            << pb_util::SecureShortDebugString(location);
        replicas_missing = true;
        SleepFor(MonoDelta::FromSeconds(1));
        break;
      }

      replicas_missing = false;
    }
    if (!replicas_missing) {
      tablet_replicas_ = tablet_replicas;
    }
  }

  // GetTableLocations() does not guarantee that all replicas are actually
  // running. Some may still be bootstrapping. Wait for them before
  // returning.
  //
  // Just as with the above loop and its behavior once kMaxRetries is
  // reached, the wait here is best effort only. That is, if the wait
  // deadline expires, the resulting timeout failure is ignored.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ExternalTabletServer* ts = cluster_->tablet_server(i);
    int expected_tablet_count = 0;
    for (const auto& e : tablet_replicas_) {
      if (ts->uuid() == e.second->uuid()) {
        ++expected_tablet_count;
      }
    }
    LOG(INFO) << Substitute(
        "Waiting for $0 tablets on tserver $1 to finish bootstrapping",
        expected_tablet_count, ts->uuid());
    cluster_->WaitForTabletsRunning(ts, expected_tablet_count,
                                    MonoDelta::FromSeconds(20));
  }
}

// Returns the last committed leader of the consensus configuration. Tries to get it from master
// but then actually tries to the get the committed consensus configuration to make sure.
TServerDetails* TabletServerIntegrationTestBase::GetLeaderReplicaOrNull(
    const string& tablet_id) {
  string leader_uuid;
  Status master_found_leader_result = GetTabletLeaderUUIDFromMaster(
      tablet_id, &leader_uuid);

  // See if the master is up to date. I.e. if it does report a leader and if the
  // replica it reports as leader is still alive and (at least thinks) its still
  // the leader.
  TServerDetails* leader;
  if (master_found_leader_result.ok()) {
    leader = GetReplicaWithUuidOrNull(tablet_id, leader_uuid);
    if (leader && itest::GetReplicaStatusAndCheckIfLeader(
          leader, tablet_id, MonoDelta::FromMilliseconds(100)).ok()) {
      return leader;
    }
  }

  // The replica we got from the master (if any) is either dead or not the leader.
  // Find the actual leader.
  pair<itest::TabletReplicaMap::iterator, itest::TabletReplicaMap::iterator> range =
      tablet_replicas_.equal_range(tablet_id);
  vector<TServerDetails*> replicas_copy;
  for (;range.first != range.second; ++range.first) {
    replicas_copy.push_back((*range.first).second);
  }

  std::random_shuffle(replicas_copy.begin(), replicas_copy.end());
  for (TServerDetails* replica : replicas_copy) {
    if (itest::GetReplicaStatusAndCheckIfLeader(
          replica, tablet_id, MonoDelta::FromMilliseconds(100)).ok()) {
      return replica;
    }
  }
  return nullptr;
}

// For the last committed consensus configuration, return the last committed
// leader of the consensus configuration and its followers.
Status TabletServerIntegrationTestBase::GetTabletLeaderAndFollowers(
    const string& tablet_id,
    TServerDetails** leader,
    vector<TServerDetails*>* followers) {

  pair<itest::TabletReplicaMap::iterator, itest::TabletReplicaMap::iterator> range =
      tablet_replicas_.equal_range(tablet_id);
  vector<TServerDetails*> replicas;
  for (; range.first != range.second; ++range.first) {
    replicas.push_back((*range.first).second);
  }

  TServerDetails* leader_replica = nullptr;
  auto it = replicas.begin();
  for (; it != replicas.end(); ++it) {
    TServerDetails* replica = *it;
    bool found_leader_replica = false;
    for (auto i = 0; i < kMaxRetries; ++i) {
      if (itest::GetReplicaStatusAndCheckIfLeader(
            replica, tablet_id, MonoDelta::FromMilliseconds(100)).ok()) {
        leader_replica = replica;
        found_leader_replica = true;
        break;
      }
    }
    if (found_leader_replica) {
      break;
    }
  }
  if (!leader_replica) {
    return Status::NotFound("leader replica not found");
  }

  if (leader) {
    *leader = leader_replica;
  }
  if (followers) {
    CHECK(replicas.end() != it);
    replicas.erase(it);
    followers->swap(replicas);
  }
  return Status::OK();
}

Status TabletServerIntegrationTestBase::GetLeaderReplicaWithRetries(
    const string& tablet_id,
    TServerDetails** leader,
    int max_attempts) {
  int attempts = 0;
  while (attempts < max_attempts) {
    *leader = GetLeaderReplicaOrNull(tablet_id);
    if (*leader) {
      return Status::OK();
    }
    attempts++;
    SleepFor(MonoDelta::FromMilliseconds(100L * attempts));
  }
  return Status::NotFound("leader replica not found");
}

Status TabletServerIntegrationTestBase::GetTabletLeaderUUIDFromMaster(
    const string& tablet_id, string* leader_uuid) {
  master::GetTableLocationsRequestPB req;
  master::GetTableLocationsResponsePB resp;
  rpc::RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(100));
  req.mutable_table()->set_table_name(kTableId);

  RETURN_NOT_OK(cluster_->master_proxy()->GetTableLocations(req, &resp, &controller));
  for (const master::TabletLocationsPB& loc : resp.tablet_locations()) {
    if (loc.tablet_id() == tablet_id) {
      for (const master::TabletLocationsPB::ReplicaPB& replica : loc.replicas()) {
        if (replica.role() == consensus::RaftPeerPB::LEADER) {
          *leader_uuid = replica.ts_info().permanent_uuid();
          return Status::OK();
        }
      }
    }
  }
  return Status::NotFound("Unable to find leader for tablet", tablet_id);
}

TServerDetails* TabletServerIntegrationTestBase::GetReplicaWithUuidOrNull(
    const string& tablet_id, const string& uuid) {
  pair<itest::TabletReplicaMap::iterator, itest::TabletReplicaMap::iterator> range =
      tablet_replicas_.equal_range(tablet_id);
  for (;range.first != range.second; ++range.first) {
    if ((*range.first).second->instance_id.permanent_uuid() == uuid) {
      return (*range.first).second;
    }
  }
  return nullptr;
}

// Gets the the locations of the consensus configuration and waits until all replicas
// are available for all tablets.
void TabletServerIntegrationTestBase::WaitForTSAndReplicas(const string& table_id) {
  int num_retries = 0;
  // make sure the replicas are up and find the leader
  while (true) {
    if (num_retries >= kMaxRetries) {
      FAIL() << " Reached max. retries while looking up the config.";
    }

    Status status = cluster_->WaitForTabletServerCount(FLAGS_num_tablet_servers,
                                                       MonoDelta::FromSeconds(5));
    if (status.IsTimedOut()) {
      LOG(WARNING)<< "Timeout waiting for all replicas to be online, retrying...";
      num_retries++;
      continue;
    }
    break;
  }
  WaitForReplicasAndUpdateLocations(table_id);
}

// Removes a set of servers from the replicas_ list.
// Handy for controlling who to validate against after killing servers.
void TabletServerIntegrationTestBase::PruneFromReplicas(
    const unordered_set<string>& uuids) {
  auto iter = tablet_replicas_.begin();
  while (iter != tablet_replicas_.end()) {
    if (uuids.count((*iter).second->instance_id.permanent_uuid()) != 0) {
      iter = tablet_replicas_.erase(iter);
      continue;
    }
    ++iter;
  }

  for (const string& uuid : uuids) {
    delete EraseKeyReturnValuePtr(&tablet_servers_, uuid);
  }
}

void TabletServerIntegrationTestBase::GetOnlyLiveFollowerReplicas(
    const string& tablet_id, vector<TServerDetails*>* followers) {
  followers->clear();
  TServerDetails* leader;
  CHECK_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));

  vector<TServerDetails*> replicas;
  pair<itest::TabletReplicaMap::iterator, itest::TabletReplicaMap::iterator> range =
      tablet_replicas_.equal_range(tablet_id);
  for (;range.first != range.second; ++range.first) {
    replicas.push_back((*range.first).second);
  }

  for (TServerDetails* replica : replicas) {
    if (leader != nullptr &&
        replica->instance_id.permanent_uuid() == leader->instance_id.permanent_uuid()) {
      continue;
    }
    Status s = itest::GetReplicaStatusAndCheckIfLeader(
        replica, tablet_id, MonoDelta::FromMilliseconds(100));
    if (s.IsIllegalState()) {
      followers->push_back(replica);
    }
  }
}

// Return the index within 'replicas' for the replica which is farthest ahead.
int64_t TabletServerIntegrationTestBase::GetFurthestAheadReplicaIdx(
    const string& tablet_id, const vector<TServerDetails*>& replicas) {
  vector<consensus::OpId> op_ids;
  CHECK_OK(GetLastOpIdForEachReplica(tablet_id, replicas, consensus::RECEIVED_OPID,
                                     MonoDelta::FromSeconds(10), &op_ids));
  int64_t max_index = 0;
  int max_replica_index = -1;
  for (int i = 0; i < op_ids.size(); i++) {
    if (op_ids[i].index() > max_index) {
      max_index = op_ids[i].index();
      max_replica_index = i;
    }
  }

  CHECK_NE(max_replica_index, -1);

  return max_replica_index;
}

Status TabletServerIntegrationTestBase::ShutdownServerWithUUID(const string& uuid) {
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ExternalTabletServer* ts = cluster_->tablet_server(i);
    if (ts->instance_id().permanent_uuid() == uuid) {
      ts->Shutdown();
      return Status::OK();
    }
  }
  return Status::NotFound("Unable to find server with UUID", uuid);
}

Status TabletServerIntegrationTestBase::RestartServerWithUUID(const string& uuid) {
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ExternalTabletServer* ts = cluster_->tablet_server(i);
    if (ts->instance_id().permanent_uuid() == uuid) {
      ts->Shutdown();
      RETURN_NOT_OK(CheckTabletServersAreAlive(tablet_servers_.size()-1));
      RETURN_NOT_OK(ts->Restart());
      RETURN_NOT_OK(CheckTabletServersAreAlive(tablet_servers_.size()));
      return Status::OK();
    }
  }
  return Status::NotFound("Unable to find server with UUID", uuid);
}

// Since we're fault-tolerant we might mask when a tablet server is
// dead. This returns Status::IllegalState() if fewer than 'num_tablet_servers'
// are alive.
Status TabletServerIntegrationTestBase::CheckTabletServersAreAlive(int num_tablet_servers) {
  int live_count = 0;
  string error = Substitute("Fewer than $0 TabletServers were alive. Dead TSs: ",
                            num_tablet_servers);
  rpc::RpcController controller;
  for (const itest::TabletServerMap::value_type& entry : tablet_servers_) {
    controller.Reset();
    controller.set_timeout(MonoDelta::FromSeconds(10));
    PingRequestPB req;
    PingResponsePB resp;
    Status s = entry.second->tserver_proxy->Ping(req, &resp, &controller);
    if (!s.ok()) {
      error += "\n" + entry.second->ToString() +  " (" + s.ToString() + ")";
      continue;
    }
    live_count++;
  }
  if (live_count < num_tablet_servers) {
    return Status::IllegalState(error);
  }
  return Status::OK();
}

void TabletServerIntegrationTestBase::TearDown() {
  STLDeleteValues(&tablet_servers_);
}

void TabletServerIntegrationTestBase::CreateClient(shared_ptr<client::KuduClient>* client) {
  // Connect to the cluster.
  ASSERT_OK(client::KuduClientBuilder()
            .add_master_server_addr(cluster_->master()->bound_rpc_addr().ToString())
            .Build(client));
}

// Create a table with a single tablet, with 'num_replicas'.
void TabletServerIntegrationTestBase::CreateTable(const string& table_id) {
  // The tests here make extensive use of server schemas, but we need
  // a client schema to create the table.
  client::KuduSchema client_schema(client::KuduSchemaFromSchema(schema_));
  gscoped_ptr<client::KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(table_id)
           .schema(&client_schema)
           .set_range_partition_columns({ "key" })
           .num_replicas(FLAGS_num_replicas)
           .Create());
  ASSERT_OK(client_->OpenTable(table_id, &table_));
}

// Starts an external cluster with a single tablet and a number of replicas equal
// to 'FLAGS_num_replicas'. The caller can pass 'ts_flags' to specify non-default
// flags to pass to the tablet servers.
void TabletServerIntegrationTestBase::BuildAndStart(
    const vector<string>& ts_flags, const vector<string>& master_flags) {
  NO_FATALS(CreateCluster("raft_consensus-itest-cluster", ts_flags, master_flags));
  NO_FATALS(CreateClient(&client_));
  NO_FATALS(CreateTable());
  WaitForTSAndReplicas();
  ASSERT_FALSE(tablet_replicas_.empty());
  tablet_id_ = (*tablet_replicas_.begin()).first;
}

void TabletServerIntegrationTestBase::AssertAllReplicasAgree(int expected_result_count) {
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(kTableId, ClusterVerifier::EXACTLY, expected_result_count));
}

// Check for and restart any TS that have crashed.
// Returns the number of servers restarted.
int TabletServerIntegrationTestBase::RestartAnyCrashedTabletServers() {
  int restarted = 0;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    if (!cluster_->tablet_server(i)->IsProcessAlive()) {
      LOG(INFO) << "TS " << i << " appears to have crashed. Restarting.";
      cluster_->tablet_server(i)->Shutdown();
      CHECK_OK(cluster_->tablet_server(i)->Restart());
      restarted++;
    }
  }
  return restarted;
}

// Assert that no tablet servers have crashed.
// Tablet servers that have been manually Shutdown() are allowed.
void TabletServerIntegrationTestBase::AssertNoTabletServersCrashed() {
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    if (cluster_->tablet_server(i)->IsShutdown()) {
      continue;
    }
    ASSERT_TRUE(cluster_->tablet_server(i)->IsProcessAlive())
        << "Tablet server " << i << " crashed";
  }
}

Status TabletServerIntegrationTestBase::WaitForLeaderWithCommittedOp(
    const string& tablet_id,
    const MonoDelta& timeout,
    TServerDetails** leader) {
  TServerDetails* leader_res = nullptr;
  RETURN_NOT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader_res));

  RETURN_NOT_OK(WaitForOpFromCurrentTerm(leader_res, tablet_id,
                                         consensus::COMMITTED_OPID, timeout));
  *leader = leader_res;
  return Status::OK();
}

vector<string> TabletServerIntegrationTestBase::GetServersWithReplica(
    const string& tablet_id) const {
  std::set<string> uuids;
  for (const auto& e : tablet_replicas_) {
    if (e.first == tablet_id) {
      uuids.insert(e.second->uuid());
    }
  }
  return vector<string>(uuids.begin(), uuids.end());
}

vector<string> TabletServerIntegrationTestBase::GetServersWithoutReplica(
    const string& tablet_id) const {
  std::set<string> uuids;
  for (const auto& e : tablet_servers_) {
    uuids.insert(e.first);
  }
  for (const auto& e : tablet_replicas_) {
    if (e.first == tablet_id) {
      uuids.erase(e.second->uuid());
    }
  }
  return vector<string>(uuids.begin(), uuids.end());
}

}  // namespace tserver
}  // namespace kudu
