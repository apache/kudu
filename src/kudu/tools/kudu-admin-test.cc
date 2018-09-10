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
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/tool_test_util.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalTabletServer;
using kudu::consensus::COMMITTED_OPID;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::EXCLUDE_HEALTH_REPORT;
using kudu::consensus::OpId;
using kudu::itest::FindTabletFollowers;
using kudu::itest::FindTabletLeader;
using kudu::itest::GetConsensusState;
using kudu::itest::StartElection;
using kudu::itest::WaitUntilLeader;
using kudu::itest::TabletServerMap;
using kudu::itest::TServerDetails;
using kudu::itest::WAIT_FOR_LEADER;
using kudu::itest::WaitForReplicasReportedToMaster;
using kudu::itest::WaitForServersToAgree;
using kudu::itest::WaitUntilCommittedConfigNumVotersIs;
using kudu::itest::WaitUntilCommittedOpIdIndexIs;
using kudu::itest::WaitUntilTabletInState;
using kudu::itest::WaitUntilTabletRunning;
using kudu::master::VOTER_REPLICA;
using kudu::pb_util::SecureDebugString;
using kudu::tserver::ListTabletsResponsePB;
using std::atomic;
using std::back_inserter;
using std::copy;
using std::deque;
using std::endl;
using std::ostringstream;
using std::string;
using std::thread;
using std::tuple;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {

class Schema;

namespace tools {

  // Helper to format info when a tool action fails.
static string ToolRunInfo(const Status& s, const string& out, const string& err) {
  ostringstream str;
  str << s.ToString() << endl;
  str << "stdout: " << out << endl;
  str << "stderr: " << err << endl;
  return str.str();
}

// Helper macro for tool tests. Use as follows:
//
// ASSERT_TOOL_OK("cluster", "ksck", master_addrs);
//
// The failure Status result of RunKuduTool is usually useless, so this macro
// also logs the stdout and stderr in case of failure, for easier diagnosis.
// TODO(wdberkeley): Add a macro to retrieve stdout or stderr, or a macro for
//                   when one of those should match a string.
#define ASSERT_TOOL_OK(...) do { \
  const vector<string>& _args{__VA_ARGS__}; \
  string _out, _err; \
  const Status& _s = RunKuduTool(_args, &_out, &_err); \
  if (_s.ok()) { \
    SUCCEED(); \
  } else { \
    FAIL() << ToolRunInfo(_s, _out, _err); \
  } \
} while (0);

class AdminCliTest : public tserver::TabletServerIntegrationTestBase {
};

// Test config change while running a workload.
// 1. Instantiate external mini cluster with 3 TS.
// 2. Create table with 2 replicas.
// 3. Invoke CLI to trigger a config change.
// 4. Wait until the new server bootstraps.
// 5. Profit!
TEST_F(AdminCliTest, TestChangeConfig) {
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
    "--allow_unsafe_replication_factor=true",

    // If running with the 3-4-3 replication scheme, the catalog manager removes
    // excess replicas, so it's necessary to disable that default behavior
    // since this test manages replicas on its own.
    "--catalog_manager_evict_excess_replicas=false",
  };
  const vector<string> kTserverFlags = {
    "--enable_leader_failure_detection=false",
  };

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 2;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());

  TabletServerMap active_tablet_servers;
  TabletServerMap::const_iterator iter = tablet_replicas_.find(tablet_id_);
  TServerDetails* leader = iter->second;
  TServerDetails* follower = (++iter)->second;
  InsertOrDie(&active_tablet_servers, leader->uuid(), leader);
  InsertOrDie(&active_tablet_servers, follower->uuid(), follower);

  TServerDetails* new_node = nullptr;
  for (TServerDetails* ts : tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_node = ts;
      break;
    }
  }
  ASSERT_NE(nullptr, new_node);

  // Elect the leader (still only a consensus config size of 2).
  ASSERT_OK(StartElection(leader, tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(30), active_tablet_servers,
                                  tablet_id_, 1));

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_timeout_allowed(true);
  workload.set_write_timeout_millis(10000);
  workload.set_num_replicas(FLAGS_num_replicas);
  workload.set_num_write_threads(1);
  workload.set_write_batch_size(1);
  workload.Setup();
  workload.Start();

  // Wait until the Master knows about the leader tserver.
  TServerDetails* master_observed_leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &master_observed_leader));
  ASSERT_EQ(leader->uuid(), master_observed_leader->uuid());

  LOG(INFO) << "Adding replica at tserver with UUID "
            << new_node->uuid() << " as VOTER...";
  ASSERT_TOOL_OK(
    "tablet",
    "change_config",
    "add_replica",
    cluster_->master()->bound_rpc_addr().ToString(),
    tablet_id_,
    new_node->uuid(),
    "VOTER"
  );

  InsertOrDie(&active_tablet_servers, new_node->uuid(), new_node);
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(active_tablet_servers.size(),
                                                leader, tablet_id_,
                                                MonoDelta::FromSeconds(10)));

  workload.StopAndJoin();
  int num_batches = workload.batches_completed();

  LOG(INFO) << "Waiting for replicas to agree...";
  // Wait for all servers to replicate everything up through the last write op.
  // Since we don't batch, there should be at least # rows inserted log entries,
  // plus the initial leader's no-op, plus 1 for
  // the added replica for a total == #rows + 2.
  int min_log_index = num_batches + 2;
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(30),
                                  active_tablet_servers, tablet_id_,
                                  min_log_index));

  int rows_inserted = workload.rows_inserted();
  LOG(INFO) << "Number of rows inserted: " << rows_inserted;

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(kTableId, ClusterVerifier::AT_LEAST, rows_inserted));

  // Now remove the server.
  LOG(INFO) << "Removing replica at tserver with UUID "
            << new_node->uuid() << " from the config...";
  ASSERT_TOOL_OK(
    "tablet",
    "change_config",
    "remove_replica",
    cluster_->master()->bound_rpc_addr().ToString(),
    tablet_id_,
    new_node->uuid()
  );

  ASSERT_EQ(1, active_tablet_servers.erase(new_node->uuid()));
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(active_tablet_servers.size(),
                                                leader, tablet_id_,
                                                MonoDelta::FromSeconds(10)));
}

enum class Kudu1097 {
  Disable,
  Enable,
};
enum class DownTS {
  None,
  TabletPeer,
  // Regression case for KUDU-2331.
  UninvolvedTS,
};
class MoveTabletParamTest :
    public AdminCliTest,
    public ::testing::WithParamInterface<tuple<Kudu1097, DownTS>> {
};

TEST_P(MoveTabletParamTest, Test) {
  const MonoDelta timeout = MonoDelta::FromSeconds(30);
  const auto& param = GetParam();
  const auto enable_kudu_1097 = std::get<0>(param);
  const auto downTS = std::get<1>(param);

  FLAGS_num_tablet_servers = 5;
  FLAGS_num_replicas = 3;

  vector<string> ts_flags, master_flags;
  ts_flags = master_flags = {
      Substitute("--raft_prepare_replacement_before_eviction=$0",
                 enable_kudu_1097 == Kudu1097::Enable) };
  NO_FATALS(BuildAndStart(ts_flags, master_flags));

  vector<string> tservers;
  AppendKeysFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());

  deque<string> active_tservers;
  for (auto iter = tablet_replicas_.find(tablet_id_); iter != tablet_replicas_.cend(); ++iter) {
    active_tservers.push_back(iter->second->uuid());
  }
  ASSERT_EQ(FLAGS_num_replicas, active_tservers.size());

  deque<string> inactive_tservers;
  std::sort(tservers.begin(), tservers.end());
  std::sort(active_tservers.begin(), active_tservers.end());
  std::set_difference(tservers.cbegin(), tservers.cend(),
                      active_tservers.cbegin(), active_tservers.cend(),
                      std::back_inserter(inactive_tservers));
  ASSERT_EQ(FLAGS_num_tablet_servers - FLAGS_num_replicas, inactive_tservers.size());

  // The workload is light (1 thread, 1 op batches) so that new replicas
  // bootstrap and converge quickly.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_num_replicas(FLAGS_num_replicas);
  workload.set_num_write_threads(1);
  workload.set_write_batch_size(1);
  workload.set_write_timeout_millis(timeout.ToMilliseconds());
  workload.Setup();
  workload.Start();

  if (downTS == DownTS::TabletPeer) {
    // To test that the move fails if any peer is down, when downTS is
    // 'TabletPeer', shut down a server besides 'add' that hosts a replica.
    NO_FATALS(cluster_->tablet_server_by_uuid(active_tservers.back())->Shutdown());
  } else if (downTS == DownTS::UninvolvedTS) {
    // Regression case for KUDU-2331, where move_replica would fail if any tablet
    // server is down, even if that tablet server was not involved in the move.
    NO_FATALS(cluster_->tablet_server_by_uuid(inactive_tservers.back())->Shutdown());
  }

  // If we're not bringing down a tablet server, do 3 moves.
  // Assuming no ad hoc leadership changes, 3 guarantees the leader is moved at least once.
  int num_moves = AllowSlowTests() && (downTS == DownTS::None) ? 3 : 1;
  for (int i = 0; i < num_moves; i++) {
    const string remove = active_tservers.front();
    const string add = inactive_tservers.front();
    vector<string> tool_command = {
      "tablet",
      "change_config",
      "move_replica",
      cluster_->master()->bound_rpc_addr().ToString(),
      tablet_id_,
      remove,
      add,
    };

    string stdout, stderr;
    Status s = RunKuduTool(tool_command, &stdout, &stderr);
    if (downTS == DownTS::TabletPeer) {
      ASSERT_TRUE(s.IsRuntimeError());
      workload.StopAndJoin();
      return;
    }
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, stdout, stderr);

    active_tservers.pop_front();
    active_tservers.push_back(add);
    inactive_tservers.pop_front();
    inactive_tservers.push_back(remove);

    // Allow the added server time to catch up so it applies the newest configuration.
    // If we don't wait, the initial ksck of move_tablet can fail with consensus conflict.
    TabletServerMap active_tservers_map;
    for (const string& uuid : active_tservers) {
      InsertOrDie(&active_tservers_map, uuid, tablet_servers_[uuid]);
    }
    ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(/*num_voters=*/ FLAGS_num_replicas,
                                                  active_tservers_map[add],
                                                  tablet_id_, timeout));
    NO_FATALS(WaitUntilCommittedConfigNumMembersIs(/*num_members=*/ FLAGS_num_replicas,
                                                   active_tservers_map[add],
                                                   tablet_id_, timeout));

  }
  workload.StopAndJoin();
  NO_FATALS(cluster_->AssertNoCrashes());

  // If a tablet server is down, we need to skip the ClusterVerifier.
  if (downTS == DownTS::None) {
    ClusterVerifier v(cluster_.get());
    NO_FATALS(v.CheckCluster());
  }
}

INSTANTIATE_TEST_CASE_P(EnableKudu1097AndDownTS, MoveTabletParamTest,
                        ::testing::Combine(::testing::Values(Kudu1097::Disable,
                                                             Kudu1097::Enable),
                                           ::testing::Values(DownTS::None,
                                                             DownTS::TabletPeer,
                                                             DownTS::UninvolvedTS)));

Status RunUnsafeChangeConfig(const string& tablet_id,
                             const string& dst_host,
                             const vector<string>& peer_uuid_list) {
  vector<string> command_args = {
      "remote_replica",
      "unsafe_change_config",
      dst_host,
      tablet_id
  };
  copy(peer_uuid_list.begin(), peer_uuid_list.end(), back_inserter(command_args));
  return RunKuduTool(command_args);
}

// Test unsafe config change when there is one follower survivor in the cluster.
// 1. Instantiate external mini cluster with 1 tablet having 3 replicas and 5 TS.
// 2. Shut down leader and follower1.
// 3. Trigger unsafe config change on follower2 having follower2 in the config.
// 4. Wait until the new config is populated on follower2(new leader) and master.
// 5. Bring up leader and follower1 and verify replicas are deleted.
// 6. Verify that new config doesn't contain old leader and follower1.
TEST_F(AdminCliTest, TestUnsafeChangeConfigOnSingleFollower) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  FLAGS_num_tablet_servers = 5;
  FLAGS_num_replicas = 3;
  // tserver_unresponsive_timeout_ms is useful so that master considers
  // the live tservers for tablet re-replication.
  NO_FATALS(BuildAndStart());

  LOG(INFO) << "Finding tablet leader and waiting for things to start...";
  string tablet_id = tablet_replicas_.begin()->first;

  // Determine the list of tablet servers currently in the config.
  TabletServerMap active_tablet_servers;
  auto iter = tablet_replicas_.equal_range(tablet_id);
  for (auto it = iter.first; it != iter.second; ++it) {
    InsertOrDie(&active_tablet_servers, it->second->uuid(), it->second);
  }

  // Get a baseline config reported to the master.
  LOG(INFO) << "Waiting for Master to see the current replicas...";
  master::TabletLocationsPB tablet_locations;
  bool has_leader;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id, kTimeout,
                                            WAIT_FOR_LEADER,
                                            VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);

  // Wait for initial NO_OP to be committed by the leader.
  TServerDetails* leader_ts;
  vector<TServerDetails*> followers;
  ASSERT_OK(FindTabletLeader(active_tablet_servers, tablet_id, kTimeout, &leader_ts));
  ASSERT_OK(FindTabletFollowers(active_tablet_servers, tablet_id, kTimeout, &followers));
  OpId opid;
  ASSERT_OK(WaitForOpFromCurrentTerm(leader_ts, tablet_id, COMMITTED_OPID, kTimeout, &opid));

  // Shut down master so it doesn't interfere while we shut down the leader and
  // one of the other followers.
  cluster_->master()->Shutdown();
  cluster_->tablet_server_by_uuid(leader_ts->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(followers[1]->uuid())->Shutdown();


  LOG(INFO) << "Forcing unsafe config change on remaining follower " << followers[0]->uuid();
  const string& follower0_addr =
      cluster_->tablet_server_by_uuid(followers[0]->uuid())->bound_rpc_addr().ToString();
  ASSERT_OK(RunUnsafeChangeConfig(tablet_id, follower0_addr, { followers[0]->uuid() }));
  ASSERT_OK(WaitUntilLeader(followers[0], tablet_id, kTimeout));
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(1, followers[0], tablet_id, kTimeout));

  LOG(INFO) << "Restarting master...";

  // Restart master so it can re-replicate the tablet to remaining tablet servers.
  ASSERT_OK(cluster_->master()->Restart());

  // Wait for master to re-replicate.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(3, followers[0], tablet_id, kTimeout));
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  ASSERT_OK(WaitForOpFromCurrentTerm(followers[0], tablet_id, COMMITTED_OPID, kTimeout, &opid));

  active_tablet_servers.clear();
  std::unordered_set<string> replica_uuids;
  for (const auto& loc : tablet_locations.replicas()) {
    const string& uuid = loc.ts_info().permanent_uuid();
    InsertOrDie(&active_tablet_servers, uuid, tablet_servers_[uuid]);
  }
  ASSERT_OK(WaitForServersToAgree(kTimeout, active_tablet_servers, tablet_id, opid.index()));

  // Verify that two new servers are part of new config and old
  // servers are gone.
  for (const master::TabletLocationsPB_ReplicaPB& replica :
      tablet_locations.replicas()) {
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[1]->uuid());
    ASSERT_NE(replica.ts_info().permanent_uuid(), leader_ts->uuid());
  }

  // Also verify that when we bring back followers[1] and leader,
  // we should see the tablet in TOMBSTONED state on these servers.
  ASSERT_OK(cluster_->tablet_server_by_uuid(leader_ts->uuid())->Restart());
  ASSERT_OK(cluster_->tablet_server_by_uuid(followers[1]->uuid())->Restart());
  ASSERT_OK(WaitUntilTabletInState(leader_ts, tablet_id, tablet::STOPPED, kTimeout));
  ASSERT_OK(WaitUntilTabletInState(followers[1], tablet_id, tablet::STOPPED, kTimeout));
}

// Test unsafe config change when there is one leader survivor in the cluster.
// 1. Instantiate external mini cluster with 1 tablet having 3 replicas and 5 TS.
// 2. Shut down both followers.
// 3. Trigger unsafe config change on leader having leader in the config.
// 4. Wait until the new config is populated on leader and master.
// 5. Verify that new config does not contain old followers.
TEST_F(AdminCliTest, TestUnsafeChangeConfigOnSingleLeader) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  FLAGS_num_tablet_servers = 5;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart());

  // Determine the list of tablet servers currently in the config.
  TabletServerMap active_tablet_servers;
  auto iter = tablet_replicas_.equal_range(tablet_id_);
  for (auto it = iter.first; it != iter.second; ++it) {
    InsertOrDie(&active_tablet_servers, it->second->uuid(), it->second);
  }

  // Get a baseline config reported to the master.
  LOG(INFO) << "Waiting for Master to see the current replicas...";
  master::TabletLocationsPB tablet_locations;
  bool has_leader;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);

  // Wait for initial NO_OP to be committed by the leader.
  TServerDetails* leader_ts;
  ASSERT_OK(FindTabletLeader(active_tablet_servers, tablet_id_, kTimeout, &leader_ts));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_ts, tablet_id_, kTimeout));
  vector<TServerDetails*> followers;
  ASSERT_OK(FindTabletFollowers(active_tablet_servers, tablet_id_, kTimeout, &followers));

  // Shut down servers follower1 and follower2,
  // so that we can force new config on remaining leader.
  cluster_->tablet_server_by_uuid(followers[0]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(followers[1]->uuid())->Shutdown();
  // Restart master to cleanup cache of dead servers from its list of candidate
  // servers to trigger placement of new replicas on healthy servers.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());

  LOG(INFO) << "Forcing unsafe config change on tserver " << leader_ts->uuid();
  const string& leader_addr = Substitute("$0:$1",
                                         leader_ts->registration.rpc_addresses(0).host(),
                                         leader_ts->registration.rpc_addresses(0).port());
  const vector<string> peer_uuid_list = { leader_ts->uuid() };
  ASSERT_OK(RunUnsafeChangeConfig(tablet_id_, leader_addr, peer_uuid_list));

  // Check that new config is populated to a new follower.
  vector<TServerDetails*> all_tservers;
  TServerDetails *new_follower = nullptr;
  AppendValuesFromMap(tablet_servers_, &all_tservers);
  for (const auto& ts :all_tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_follower = ts;
      break;
    }
  }
  ASSERT_TRUE(new_follower != nullptr);

  // Master may try to add the servers which are down until tserver_unresponsive_timeout_ms,
  // so it is safer to wait until consensus metadata has 3 voters on new_follower.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(3, new_follower, tablet_id_, kTimeout));

  // Wait for the master to be notified of the config change.
  LOG(INFO) << "Waiting for Master to see new config...";
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  for (const master::TabletLocationsPB_ReplicaPB& replica :
      tablet_locations.replicas()) {
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[0]->uuid());
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[1]->uuid());
  }
}

// Test unsafe config change when the unsafe config contains 2 nodes.
// 1. Instantiate external minicluster with 1 tablet having 3 replicas and 5 TS.
// 2. Shut down leader.
// 3. Trigger unsafe config change on follower1 having follower1 and follower2 in the config.
// 4. Wait until the new config is populated on new_leader and master.
// 5. Verify that new config does not contain old leader.
TEST_F(AdminCliTest, TestUnsafeChangeConfigForConfigWithTwoNodes) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  FLAGS_num_tablet_servers = 4;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart());

  // Determine the list of tablet servers currently in the config.
  TabletServerMap active_tablet_servers;
  auto iter = tablet_replicas_.equal_range(tablet_id_);
  for (auto it = iter.first; it != iter.second; ++it) {
    InsertOrDie(&active_tablet_servers, it->second->uuid(), it->second);
  }

  // Get a baseline config reported to the master.
  LOG(INFO) << "Waiting for Master to see the current replicas...";
  master::TabletLocationsPB tablet_locations;
  bool has_leader;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);

  // Wait for initial NO_OP to be committed by the leader.
  TServerDetails* leader_ts;
  ASSERT_OK(FindTabletLeader(active_tablet_servers, tablet_id_, kTimeout, &leader_ts));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_ts, tablet_id_, kTimeout));
  vector<TServerDetails*> followers;
  ASSERT_OK(FindTabletFollowers(active_tablet_servers, tablet_id_, kTimeout, &followers));

  // Shut down leader and prepare 2-node config.
  cluster_->tablet_server_by_uuid(leader_ts->uuid())->Shutdown();
  // Restart master to cleanup cache of dead servers from its list of candidate
  // servers to trigger placement of new replicas on healthy servers.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());

  LOG(INFO) << "Forcing unsafe config change on tserver " << followers[1]->uuid();
  const string& follower1_addr = Substitute("$0:$1",
                                            followers[1]->registration.rpc_addresses(0).host(),
                                            followers[1]->registration.rpc_addresses(0).port());
  const vector<string> peer_uuid_list = { followers[0]->uuid(),
                                          followers[1]->uuid(), };
  ASSERT_OK(RunUnsafeChangeConfig(tablet_id_, follower1_addr, peer_uuid_list));

  // Find a remaining node which will be picked for re-replication.
  vector<TServerDetails*> all_tservers;
  AppendValuesFromMap(tablet_servers_, &all_tservers);
  TServerDetails* new_node = nullptr;
  for (TServerDetails* ts : all_tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_node = ts;
      break;
    }
  }
  ASSERT_TRUE(new_node != nullptr);

  // Master may try to add the servers which are down until tserver_unresponsive_timeout_ms,
  // so it is safer to wait until consensus metadata has 3 voters on follower1.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(3, new_node, tablet_id_, kTimeout));

  // Wait for the master to be notified of the config change.
  LOG(INFO) << "Waiting for Master to see new config...";
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  for (const master::TabletLocationsPB_ReplicaPB& replica :
      tablet_locations.replicas()) {
    ASSERT_NE(replica.ts_info().permanent_uuid(), leader_ts->uuid());
  }
}

// Test unsafe config change on a 5-replica tablet when the unsafe config contains 2 nodes.
// 1. Instantiate external minicluster with 1 tablet having 5 replicas and 8 TS.
// 2. Shut down leader and 2 followers.
// 3. Trigger unsafe config change on a surviving follower with those
//    2 surviving followers in the new config.
// 4. Wait until the new config is populated on new_leader and master.
// 5. Verify that new config does not contain old leader and old followers.
TEST_F(AdminCliTest, TestUnsafeChangeConfigWithFiveReplicaConfig) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  // Retire the dead servers early with these settings.
  FLAGS_num_tablet_servers = 8;
  FLAGS_num_replicas = 5;
  NO_FATALS(BuildAndStart());

  vector<TServerDetails*> tservers;
  vector<ExternalTabletServer*> external_tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  for (TServerDetails* ts : tservers) {
    external_tservers.push_back(cluster_->tablet_server_by_uuid(ts->uuid()));
  }

  // Determine the list of tablet servers currently in the config.
  TabletServerMap active_tablet_servers;
  auto iter = tablet_replicas_.equal_range(tablet_id_);
  for (auto it = iter.first; it != iter.second; ++it) {
    InsertOrDie(&active_tablet_servers, it->second->uuid(), it->second);
  }

  // Get a baseline config reported to the master.
  LOG(INFO) << "Waiting for Master to see the current replicas...";
  master::TabletLocationsPB tablet_locations;
  bool has_leader;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            5, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);

  // Wait for initial NO_OP to be committed by the leader.
  TServerDetails* leader_ts;
  ASSERT_OK(FindTabletLeader(active_tablet_servers, tablet_id_, kTimeout, &leader_ts));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_ts, tablet_id_, kTimeout));
  vector<TServerDetails*> followers;
  ASSERT_OK(FindTabletFollowers(active_tablet_servers, tablet_id_, kTimeout, &followers));
  ASSERT_EQ(followers.size(), 4);
  cluster_->tablet_server_by_uuid(followers[2]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(followers[3]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(leader_ts->uuid())->Shutdown();
  // Restart master to cleanup cache of dead servers from its list of candidate
  // servers to trigger placement of new replicas on healthy servers.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());

  LOG(INFO) << "Forcing unsafe config change on tserver " << followers[1]->uuid();
  const string& follower1_addr = Substitute("$0:$1",
                                         followers[1]->registration.rpc_addresses(0).host(),
                                         followers[1]->registration.rpc_addresses(0).port());
  const vector<string> peer_uuid_list = { followers[0]->uuid(),
                                          followers[1]->uuid(), };
  ASSERT_OK(RunUnsafeChangeConfig(tablet_id_, follower1_addr, peer_uuid_list));

  // Find a remaining node which will be picked for re-replication.
  vector<TServerDetails*> all_tservers;
  AppendValuesFromMap(tablet_servers_, &all_tservers);
  TServerDetails* new_node = nullptr;
  for (TServerDetails* ts : all_tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_node = ts;
      break;
    }
  }
  ASSERT_TRUE(new_node != nullptr);

  // Master may try to add the servers which are down until tserver_unresponsive_timeout_ms,
  // so it is safer to wait until consensus metadata has 5 voters back on new_node.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(5, new_node, tablet_id_, kTimeout));

  // Wait for the master to be notified of the config change.
  LOG(INFO) << "Waiting for Master to see new config...";
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            5, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  for (const master::TabletLocationsPB_ReplicaPB& replica :
      tablet_locations.replicas()) {
    ASSERT_NE(replica.ts_info().permanent_uuid(), leader_ts->uuid());
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[2]->uuid());
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[3]->uuid());
  }
}

// Test unsafe config change when there is a pending config on a surviving leader.
// 1. Instantiate external minicluster with 1 tablet having 3 replicas and 5 TS.
// 2. Shut down both the followers.
// 3. Trigger a regular config change on the leader which remains pending on leader.
// 4. Trigger unsafe config change on the surviving leader.
// 5. Wait until the new config is populated on leader and master.
// 6. Verify that new config does not contain old followers and a standby node
//    has populated the new config.
TEST_F(AdminCliTest, TestUnsafeChangeConfigLeaderWithPendingConfig) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  FLAGS_num_tablet_servers = 5;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart());

  // Determine the list of tablet servers currently in the config.
  TabletServerMap active_tablet_servers;
  auto iter = tablet_replicas_.equal_range(tablet_id_);
  for (auto it = iter.first; it != iter.second; ++it) {
    InsertOrDie(&active_tablet_servers, it->second->uuid(), it->second);
  }

  // Get a baseline config reported to the master.
  LOG(INFO) << "Waiting for Master to see the current replicas...";
  master::TabletLocationsPB tablet_locations;
  bool has_leader;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);

  // Wait for initial NO_OP to be committed by the leader.
  TServerDetails* leader_ts;
  ASSERT_OK(FindTabletLeader(active_tablet_servers, tablet_id_, kTimeout, &leader_ts));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_ts, tablet_id_, kTimeout));
  vector<TServerDetails*> followers;
  ASSERT_OK(FindTabletFollowers(active_tablet_servers, tablet_id_, kTimeout, &followers));
  ASSERT_EQ(followers.size(), 2);

  // Shut down servers follower1 and follower2,
  // so that leader can't replicate future config change ops.
  cluster_->tablet_server_by_uuid(followers[0]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(followers[1]->uuid())->Shutdown();

  // Now try to replicate a ChangeConfig operation. This should get stuck and time out
  // because the server can't replicate any operations.
  Status s = RemoveServer(leader_ts, tablet_id_, followers[1],
                          MonoDelta::FromSeconds(2), -1);
  ASSERT_TRUE(s.IsTimedOut());

  LOG(INFO) << "Change Config Op timed out, Sending a Replace config "
            << "command when change config op is pending on the leader.";
  const string& leader_addr = Substitute("$0:$1",
                                         leader_ts->registration.rpc_addresses(0).host(),
                                         leader_ts->registration.rpc_addresses(0).port());
  const vector<string> peer_uuid_list = { leader_ts->uuid() };
  ASSERT_OK(RunUnsafeChangeConfig(tablet_id_, leader_addr, peer_uuid_list));

  // Restart master to cleanup cache of dead servers from its list of candidate
  // servers to trigger placement of new replicas on healthy servers.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());

  // Find a remaining node which will be picked for re-replication.
  vector<TServerDetails*> all_tservers;
  AppendValuesFromMap(tablet_servers_, &all_tservers);
  TServerDetails* new_node = nullptr;
  for (TServerDetails* ts : all_tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_node = ts;
      break;
    }
  }
  ASSERT_TRUE(new_node != nullptr);

  // Master may try to add the servers which are down until tserver_unresponsive_timeout_ms,
  // so it is safer to wait until consensus metadata has 3 voters on new_node.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(3, new_node, tablet_id_, kTimeout));

  // Wait for the master to be notified of the config change.
  LOG(INFO) << "Waiting for Master to see new config...";
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  for (const master::TabletLocationsPB_ReplicaPB& replica :
      tablet_locations.replicas()) {
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[0]->uuid());
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[1]->uuid());
  }
}

// Test unsafe config change when there is a pending config on a surviving follower.
// 1. Instantiate external minicluster with 1 tablet having 3 replicas and 5 TS.
// 2. Shut down both the followers.
// 3. Trigger a regular config change on the leader which remains pending on leader.
// 4. Trigger a leader_step_down command such that leader is forced to become follower.
// 5. Trigger unsafe config change on the follower.
// 6. Wait until the new config is populated on leader and master.
// 7. Verify that new config does not contain old followers and a standby node
//    has populated the new config.
TEST_F(AdminCliTest, TestUnsafeChangeConfigFollowerWithPendingConfig) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  FLAGS_num_tablet_servers = 5;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart());

  // Determine the list of tablet servers currently in the config.
  TabletServerMap active_tablet_servers;
  auto iter = tablet_replicas_.equal_range(tablet_id_);
  for (auto it = iter.first; it != iter.second; ++it) {
    InsertOrDie(&active_tablet_servers, it->second->uuid(), it->second);
  }

  // Get a baseline config reported to the master.
  LOG(INFO) << "Waiting for Master to see the current replicas...";
  master::TabletLocationsPB tablet_locations;
  bool has_leader;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);

  // Wait for initial NO_OP to be committed by the leader.
  TServerDetails* leader_ts;
  ASSERT_OK(FindTabletLeader(active_tablet_servers, tablet_id_, kTimeout, &leader_ts));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_ts, tablet_id_, kTimeout));
  vector<TServerDetails*> followers;
  ASSERT_OK(FindTabletFollowers(active_tablet_servers, tablet_id_, kTimeout, &followers));

  // Shut down servers follower1 and follower2,
  // so that leader can't replicate future config change ops.
  cluster_->tablet_server_by_uuid(followers[0]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(followers[1]->uuid())->Shutdown();
  // Restart master to cleanup cache of dead servers from its
  // list of candidate servers to place the new replicas.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());

  // Now try to replicate a ChangeConfig operation. This should get stuck and time out
  // because the server can't replicate any operations.
  Status s = RemoveServer(leader_ts, tablet_id_, followers[1],
                          MonoDelta::FromSeconds(2), -1);
  ASSERT_TRUE(s.IsTimedOut());

  // Force leader to step down, best effort command since the leadership
  // could change anytime during cluster lifetime.
  string stderr;
  s = RunKuduTool({
    "tablet",
    "leader_step_down",
    cluster_->master()->bound_rpc_addr().ToString(),
    tablet_id_
  }, nullptr, &stderr);
  bool not_currently_leader = stderr.find(
      Status::IllegalState("").CodeAsString()) != string::npos;
  ASSERT_TRUE(s.ok() || not_currently_leader) << "stderr: " << stderr;

  LOG(INFO) << "Change Config Op timed out, Sending a Replace config "
            << "command when change config op is pending on the leader.";
  const string& leader_addr = Substitute("$0:$1",
                                         leader_ts->registration.rpc_addresses(0).host(),
                                         leader_ts->registration.rpc_addresses(0).port());
  const vector<string> peer_uuid_list = { leader_ts->uuid() };
  ASSERT_OK(RunUnsafeChangeConfig(tablet_id_, leader_addr, peer_uuid_list));

  // Find a remaining node which will be picked for re-replication.
  vector<TServerDetails*> all_tservers;
  AppendValuesFromMap(tablet_servers_, &all_tservers);
  TServerDetails* new_node = nullptr;
  for (TServerDetails* ts : all_tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_node = ts;
      break;
    }
  }
  ASSERT_TRUE(new_node != nullptr);

  // Master may try to add the servers which are down until tserver_unresponsive_timeout_ms,
  // so it is safer to wait until consensus metadata has 3 voters on new_node.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(3, new_node, tablet_id_, kTimeout));

  // Wait for the master to be notified of the config change.
  LOG(INFO) << "Waiting for Master to see new config...";
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  for (const master::TabletLocationsPB_ReplicaPB& replica :
      tablet_locations.replicas()) {
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[1]->uuid());
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[0]->uuid());
  }
}

// Test unsafe config change when there are back to back pending configs on leader logs.
// 1. Instantiate external minicluster with 1 tablet having 3 replicas and 5 TS.
// 2. Shut down both the followers.
// 3. Trigger a regular config change on the leader which remains pending on leader.
// 4. Set a fault crash flag to trigger upon next commit of config change.
// 5. Trigger unsafe config change on the surviving leader which should trigger
//    the fault while the old config change is being committed.
// 6. Shutdown and restart the leader and verify that tablet bootstrapped on leader.
// 7. Verify that a new node has populated the new config with 3 voters.
TEST_F(AdminCliTest, TestUnsafeChangeConfigWithPendingConfigsOnWAL) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  FLAGS_num_tablet_servers = 5;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart());

  // Determine the list of tablet servers currently in the config.
  TabletServerMap active_tablet_servers;
  auto iter = tablet_replicas_.equal_range(tablet_id_);
  for (auto it = iter.first; it != iter.second; ++it) {
    InsertOrDie(&active_tablet_servers, it->second->uuid(), it->second);
  }

  // Get a baseline config reported to the master.
  LOG(INFO) << "Waiting for Master to see the current replicas...";
  master::TabletLocationsPB tablet_locations;
  bool has_leader;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);

  // Wait for initial NO_OP to be committed by the leader.
  TServerDetails* leader_ts;
  ASSERT_OK(FindTabletLeader(active_tablet_servers, tablet_id_, kTimeout, &leader_ts));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_ts, tablet_id_, kTimeout));
  vector<TServerDetails*> followers;
  ASSERT_OK(FindTabletFollowers(active_tablet_servers, tablet_id_, kTimeout, &followers));

  // Shut down servers follower1 and follower2,
  // so that leader can't replicate future config change ops.
  cluster_->tablet_server_by_uuid(followers[0]->uuid())->Shutdown();
  cluster_->tablet_server_by_uuid(followers[1]->uuid())->Shutdown();

  // Now try to replicate a ChangeConfig operation. This should get stuck and time out
  // because the server can't replicate any operations.
  Status s = RemoveServer(leader_ts, tablet_id_, followers[1],
                          MonoDelta::FromSeconds(2), -1);
  ASSERT_TRUE(s.IsTimedOut());

  LOG(INFO) << "Change Config Op timed out, Sending a Replace config "
            << "command when change config op is pending on the leader.";
  const string& leader_addr = Substitute("$0:$1",
                                         leader_ts->registration.rpc_addresses(0).host(),
                                         leader_ts->registration.rpc_addresses(0).port());
  const vector<string> peer_uuid_list = { leader_ts->uuid() };
  ASSERT_OK(RunUnsafeChangeConfig(tablet_id_, leader_addr, peer_uuid_list));

  // Inject the crash via fault_crash_before_cmeta_flush flag.
  // Tablet will find 2 pending configs back to back during bootstrap,
  // one from ChangeConfig (RemoveServer) and another from UnsafeChangeConfig.
  ASSERT_OK(cluster_->SetFlag(
      cluster_->tablet_server_by_uuid(leader_ts->uuid()),
      "fault_crash_before_cmeta_flush", "1.0"));

  // Find a remaining node which will be picked for re-replication.
  vector<TServerDetails*> all_tservers;
  AppendValuesFromMap(tablet_servers_, &all_tservers);
  TServerDetails* new_node = nullptr;
  for (TServerDetails* ts : all_tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_node = ts;
      break;
    }
  }
  ASSERT_TRUE(new_node != nullptr);
  // Restart master to cleanup cache of dead servers from its list of candidate
  // servers to trigger placement of new replicas on healthy servers.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());

  ASSERT_OK(cluster_->tablet_server_by_uuid(
      leader_ts->uuid())->WaitForInjectedCrash(kTimeout));

  cluster_->tablet_server_by_uuid(leader_ts->uuid())->Shutdown();
  ASSERT_OK(cluster_->tablet_server_by_uuid(
      leader_ts->uuid())->Restart());
  ASSERT_OK(WaitForNumTabletsOnTS(leader_ts, 1, kTimeout, nullptr));
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(3, new_node, tablet_id_, kTimeout));

  // Wait for the master to be notified of the config change.
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            3, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  for (const master::TabletLocationsPB_ReplicaPB& replica :
      tablet_locations.replicas()) {
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[0]->uuid());
    ASSERT_NE(replica.ts_info().permanent_uuid(), followers[1]->uuid());
  }
}

// Test unsafe config change on a 5-replica tablet when the mulitple pending configs
// on the surviving node.
// 1. Instantiate external minicluster with 1 tablet having 5 replicas and 9 TS.
// 2. Shut down all the followers.
// 3. Trigger unsafe config changes on the surviving leader with those
//    dead followers in the new config.
// 4. Wait until the new config is populated on the master and the new leader.
// 5. Verify that new config does not contain old followers.
TEST_F(AdminCliTest, TestUnsafeChangeConfigWithMultiplePendingConfigs) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  FLAGS_num_tablet_servers = 9;
  FLAGS_num_replicas = 5;
  // Retire the dead servers early with these settings.
  NO_FATALS(BuildAndStart());

  vector<TServerDetails*> tservers;
  vector<ExternalTabletServer*> external_tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  for (TServerDetails* ts : tservers) {
    external_tservers.push_back(cluster_->tablet_server_by_uuid(ts->uuid()));
  }

  // Determine the list of tablet servers currently in the config.
  TabletServerMap active_tablet_servers;
  auto iter = tablet_replicas_.equal_range(tablet_id_);
  for (auto it = iter.first; it != iter.second; ++it) {
    InsertOrDie(&active_tablet_servers, it->second->uuid(), it->second);
  }

  // Get a baseline config reported to the master.
  LOG(INFO) << "Waiting for Master to see the current replicas...";
  master::TabletLocationsPB tablet_locations;
  bool has_leader;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            5, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  ASSERT_TRUE(has_leader) << SecureDebugString(tablet_locations);

  TServerDetails* leader_ts;
  ASSERT_OK(FindTabletLeader(active_tablet_servers, tablet_id_, kTimeout, &leader_ts));
  vector<TServerDetails*> followers;
  for (const auto& elem : active_tablet_servers) {
    if (elem.first == leader_ts->uuid()) {
      continue;
    }
    followers.push_back(elem.second);
    cluster_->tablet_server_by_uuid(elem.first)->Shutdown();
  }
  ASSERT_EQ(4, followers.size());

  // Shutdown master to cleanup cache of dead servers from its list of candidate
  // servers to trigger placement of new replicas on healthy servers when we restart later.
  cluster_->master()->Shutdown();

  const string& leader_addr = Substitute("$0:$1",
                                         leader_ts->registration.rpc_addresses(0).host(),
                                         leader_ts->registration.rpc_addresses(0).port());

  // This should keep the multiple pending configs on the node since we are
  // adding all the dead followers to the new config, and then eventually we write
  // just one surviving node to the config.
  // New config write sequences are: {ABCDE}, {ABCD}, {ABC}, {AB}, {A},
  // A being the leader node where config is written and rest of the nodes are
  // dead followers.
  for (int num_replicas = followers.size(); num_replicas >= 0; num_replicas--) {
    vector<string> peer_uuid_list;
    peer_uuid_list.push_back(leader_ts->uuid());
    for (int i = 0; i < num_replicas; i++) {
      peer_uuid_list.push_back(followers[i]->uuid());
    }
    ASSERT_OK(RunUnsafeChangeConfig(tablet_id_, leader_addr, peer_uuid_list));
  }

  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(1, leader_ts, tablet_id_, kTimeout));
  ASSERT_OK(cluster_->master()->Restart());

  // Find a remaining node which will be picked for re-replication.
  vector<TServerDetails*> all_tservers;
  AppendValuesFromMap(tablet_servers_, &all_tservers);
  TServerDetails* new_node = nullptr;
  for (TServerDetails* ts : all_tservers) {
    if (!ContainsKey(active_tablet_servers, ts->uuid())) {
      new_node = ts;
      break;
    }
  }
  ASSERT_TRUE(new_node != nullptr);

  // Master may try to add the servers which are down until tserver_unresponsive_timeout_ms,
  // so it is safer to wait until consensus metadata has 5 voters on new_node.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(5, new_node, tablet_id_, kTimeout));

  // Wait for the master to be notified of the config change.
  LOG(INFO) << "Waiting for Master to see new config...";
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            5, tablet_id_, kTimeout,
                                            WAIT_FOR_LEADER, VOTER_REPLICA,
                                            &has_leader, &tablet_locations));
  LOG(INFO) << "Tablet locations:\n" << SecureDebugString(tablet_locations);
  for (const master::TabletLocationsPB_ReplicaPB& replica :
      tablet_locations.replicas()) {
    // Verify that old followers aren't part of new config.
    for (const auto& old_follower : followers) {
      ASSERT_NE(replica.ts_info().permanent_uuid(), old_follower->uuid());
    }
  }
}

Status GetTermFromConsensus(const vector<TServerDetails*>& tservers,
                            const string& tablet_id,
                            int64_t *current_term) {
  ConsensusStatePB cstate;
  for (auto& ts : tservers) {
    RETURN_NOT_OK(
        GetConsensusState(ts, tablet_id, MonoDelta::FromSeconds(10), EXCLUDE_HEALTH_REPORT,
                          &cstate));
    if (!cstate.leader_uuid().empty() &&
        IsRaftConfigMember(cstate.leader_uuid(), cstate.committed_config()) &&
        cstate.has_current_term()) {
      *current_term = cstate.current_term();
      return Status::OK();
    }
  }
  return Status::NotFound(Substitute(
      "No leader replica found for tablet $0", tablet_id));
}

TEST_F(AdminCliTest, TestLeaderStepDown) {
  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart());

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());
  for (auto& ts : tservers) {
    ASSERT_OK(WaitUntilTabletRunning(ts,
                                     tablet_id_,
                                     MonoDelta::FromSeconds(10)));
  }

  int64_t current_term;
  ASSERT_OK(GetTermFromConsensus(tservers, tablet_id_,
                                 &current_term));

  // The leader for the given tablet may change anytime, resulting in
  // the command returning an error code. Hence checking for term advancement
  // only if the leader_step_down succeeds. It is also unsafe to check
  // the term advancement without honoring status of the command since
  // there may not have been another election in the meanwhile.
  string stderr;
  Status s = RunKuduTool({
    "tablet",
    "leader_step_down",
    cluster_->master()->bound_rpc_addr().ToString(),
    tablet_id_
  }, nullptr, &stderr);
  bool not_currently_leader = stderr.find(
      Status::IllegalState("").CodeAsString()) != string::npos;
  ASSERT_TRUE(s.ok() || not_currently_leader) << "stderr: " << stderr;
  if (s.ok()) {
    int64_t new_term;
    ASSERT_EVENTUALLY([&]() {
        ASSERT_OK(GetTermFromConsensus(tservers, tablet_id_,
                                       &new_term));
        ASSERT_GT(new_term, current_term);
      });
  }
}

TEST_F(AdminCliTest, TestLeaderStepDownWhenNotPresent) {
  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;
  NO_FATALS(BuildAndStart(
      { "--enable_leader_failure_detection=false" },
      { "--catalog_manager_wait_for_new_tablets_to_elect_leader=false" }));
  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());
  for (auto& ts : tservers) {
    ASSERT_OK(WaitUntilTabletRunning(ts,
                                     tablet_id_,
                                     MonoDelta::FromSeconds(10)));
  }

  int64_t current_term;
  ASSERT_TRUE(GetTermFromConsensus(tservers, tablet_id_,
                                   &current_term).IsNotFound());
  string stdout;
  ASSERT_OK(RunKuduTool({
    "tablet",
    "leader_step_down",
    cluster_->master()->bound_rpc_addr().ToString(),
    tablet_id_
  }, &stdout));
  ASSERT_STR_CONTAINS(stdout,
                      Substitute("No leader replica found for tablet $0",
                                 tablet_id_));
}

TEST_F(AdminCliTest, TestDeleteTable) {
  FLAGS_num_tablet_servers = 1;
  FLAGS_num_replicas = 1;
  NO_FATALS(BuildAndStart());

  string master_address = cluster_->master()->bound_rpc_addr().ToString();
  shared_ptr<KuduClient> client;
  ASSERT_OK(KuduClientBuilder()
            .add_master_server_addr(master_address)
            .Build(&client));

  ASSERT_TOOL_OK(
    "table",
    "delete",
    master_address,
    kTableId
  );

  vector<string> tables;
  ASSERT_OK(client->ListTables(&tables));
  ASSERT_TRUE(tables.empty());
}

TEST_F(AdminCliTest, TestListTables) {
  FLAGS_num_tablet_servers = 1;
  FLAGS_num_replicas = 1;

  NO_FATALS(BuildAndStart());

  string stdout;
  ASSERT_OK(RunKuduTool({
    "table",
    "list",
    cluster_->master()->bound_rpc_addr().ToString()
  }, &stdout));

  vector<string> stdout_lines = Split(stdout, ",", strings::SkipEmpty());
  ASSERT_EQ(1, stdout_lines.size());
  ASSERT_EQ(Substitute("$0\n", kTableId), stdout_lines[0]);
}

TEST_F(AdminCliTest, TestListTablesDetail) {
  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = 3;

  NO_FATALS(BuildAndStart());

  // Add another table to test multiple tables output.
  const string kAnotherTableId = "TestAnotherTable";
  KuduSchema client_schema(client::KuduSchemaFromSchema(schema_));
  gscoped_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
  ASSERT_OK(table_creator->table_name(kAnotherTableId)
           .schema(&client_schema)
           .set_range_partition_columns({ "key" })
           .num_replicas(FLAGS_num_replicas)
           .Create());

  // Grab list of tablet_ids from any tserver.
  vector<TServerDetails*> tservers;
  vector<string> tablet_ids;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ListRunningTabletIds(tservers.front(),
                       MonoDelta::FromSeconds(30), &tablet_ids);

  string stdout;
  ASSERT_OK(RunKuduTool({
    "table",
    "list",
    "--list_tablets",
    cluster_->master()->bound_rpc_addr().ToString()
  }, &stdout));

  vector<string> stdout_lines = Split(stdout, "\n", strings::SkipEmpty());

  // Verify multiple tables along with their tablets and replica-uuids.
  ASSERT_EQ(10, stdout_lines.size());
  ASSERT_STR_CONTAINS(stdout, kTableId);
  ASSERT_STR_CONTAINS(stdout, kAnotherTableId);
  ASSERT_STR_CONTAINS(stdout, tablet_ids.front());
  ASSERT_STR_CONTAINS(stdout, tablet_ids.back());

  for (auto& ts : tservers) {
    ASSERT_STR_CONTAINS(stdout, ts->uuid());
    ASSERT_STR_CONTAINS(stdout, ts->uuid());
  }
}

TEST_F(AdminCliTest, RebalancerReportOnly) {
  static const char kReferenceOutput[] =
    R"***(Per-server replica distribution summary:
       Statistic       |  Value
-----------------------+----------
 Minimum Replica Count | 0
 Maximum Replica Count | 1
 Average Replica Count | 0.600000

Per-table replica distribution summary:
 Replica Skew |  Value
--------------+----------
 Minimum      | 1
 Maximum      | 1
 Average      | 1.000000)***";

  FLAGS_num_tablet_servers = 5;
  NO_FATALS(BuildAndStart());

  string out;
  string err;
  Status s = RunKuduTool({
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
    "--report_only",
  }, &out, &err);
  ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
  // The rebalancer should report on tablet replica distribution. The output
  // should match the reference report: the distribution of the replicas
  // is 100% repeatable given the number of tables created by the test,
  // the replication factor and the number of tablet servers.
  ASSERT_STR_CONTAINS(out, kReferenceOutput);
  // The actual rebalancing should not run.
  ASSERT_STR_NOT_CONTAINS(out, "rebalancing is complete:")
      << ToolRunInfo(s, out, err);
}

// Make sure the rebalancer doesn't start if a tablet server is down.
class RebalanceStartCriteriaTest :
    public AdminCliTest,
    public ::testing::WithParamInterface<Kudu1097> {
};
INSTANTIATE_TEST_CASE_P(, RebalanceStartCriteriaTest,
                        ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(RebalanceStartCriteriaTest, TabletServerIsDown) {
  const bool is_343_scheme = (GetParam() == Kudu1097::Enable);
  const vector<string> kMasterFlags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };
  const vector<string> kTserverFlags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };

  FLAGS_num_tablet_servers = 5;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));

  // Shutdown one of the tablet servers.
  HostPort ts_host_port;
  {
    auto* ts = cluster_->tablet_server(0);
    ASSERT_NE(nullptr, ts);
    ts_host_port = ts->bound_rpc_hostport();
    ts->Shutdown();
  }

  string out;
  string err;
  Status s = RunKuduTool({
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString()
  }, &out, &err);
  ASSERT_TRUE(s.IsRuntimeError()) << ToolRunInfo(s, out, err);
  const auto err_msg_pattern = Substitute(
      "Illegal state: tablet server .* \\($0\\): "
      "unacceptable health status UNAVAILABLE",
      ts_host_port.ToString());
  ASSERT_STR_MATCHES(err, err_msg_pattern);
}

// Create tables with unbalanced replica distribution: useful in
// rebalancer-related tests.
static Status CreateUnbalancedTables(
    cluster::ExternalMiniCluster* cluster,
    client::KuduClient* client,
    const Schema& table_schema,
    const string& table_name_pattern,
    int num_tables,
    int rep_factor,
    int tserver_idx_from,
    int tserver_num,
    int tserver_unresponsive_ms) {
  // Keep running only some tablet servers and shut down the rest.
  for (auto i = tserver_idx_from; i < tserver_num; ++i) {
    cluster->tablet_server(i)->Shutdown();
  }

  // Wait for the catalog manager to understand that not all tablet servers
  // are available.
  SleepFor(MonoDelta::FromMilliseconds(5 * tserver_unresponsive_ms / 4));

  // Create tables with their tablet replicas landing only on the tablet servers
  // which are up and running.
  KuduSchema client_schema(client::KuduSchemaFromSchema(table_schema));
  for (auto i = 0; i < num_tables; ++i) {
    const string table_name = Substitute(table_name_pattern, i);
    unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
    RETURN_NOT_OK(table_creator->table_name(table_name)
                  .schema(&client_schema)
                  .add_hash_partitions({ "key" }, 3)
                  .num_replicas(rep_factor)
                  .Create());
    RETURN_NOT_OK(RunKuduTool({
      "perf",
      "loadgen",
      cluster->master()->bound_rpc_addr().ToString(),
      Substitute("--table_name=$0", table_name),
      Substitute("--table_num_replicas=$0", rep_factor),
      "--string_fixed=unbalanced_tables_test",
    }));
  }

  for (auto i = tserver_idx_from; i < tserver_num; ++i) {
    RETURN_NOT_OK(cluster->tablet_server(i)->Restart());
  }

  return Status::OK();
}

// A test to verify that rebalancing works for both 3-4-3 and 3-2-3 replica
// management schemes. During replica movement, a light workload is run against
// every table being rebalanced. This test covers different replication factors.
class RebalanceParamTest :
    public AdminCliTest,
    public ::testing::WithParamInterface<tuple<int, Kudu1097>> {
};
INSTANTIATE_TEST_CASE_P(, RebalanceParamTest,
    ::testing::Combine(::testing::Values(1, 2, 3, 5),
                       ::testing::Values(Kudu1097::Disable, Kudu1097::Enable)));
TEST_P(RebalanceParamTest, Rebalance) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const auto& param = GetParam();
  const auto kRepFactor = std::get<0>(param);
  const auto is_343_scheme = (std::get<1>(param) == Kudu1097::Enable);
  constexpr auto kNumTservers = 7;
  constexpr auto kNumTables = 5;
  const string table_name_pattern = "rebalance_test_table_$0";
  constexpr auto kTserverUnresponsiveMs = 3000;
  const auto timeout = MonoDelta::FromSeconds(30);
  const vector<string> kMasterFlags = {
    "--allow_unsafe_replication_factor",
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
    Substitute("--tserver_unresponsive_timeout_ms=$0", kTserverUnresponsiveMs),
  };
  const vector<string> kTserverFlags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };

  FLAGS_num_tablet_servers = kNumTservers;
  FLAGS_num_replicas = kRepFactor;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));

  ASSERT_OK(CreateUnbalancedTables(
      cluster_.get(), client_.get(), schema_, table_name_pattern, kNumTables,
      kRepFactor, kRepFactor + 1, kNumTservers, kTserverUnresponsiveMs));

  // Workloads aren't run for 3-2-3 replica movement with RF = 1 because
  // the tablet is unavailable during the move until the target voter replica
  // is up and running. That might take some time, and to avoid flakiness or
  // setting longer timeouts, RF=1 replicas are moved with no concurrent
  // workload running.
  //
  // TODO(aserbin): clarify why even with 3-4-3 it's a bit flaky now.
  vector<unique_ptr<TestWorkload>> workloads;
  //if (kRepFactor > 1 || is_343_scheme) {
  if (kRepFactor > 1) {
    for (auto i = 0; i < kNumTables; ++i) {
      const string table_name = Substitute(table_name_pattern, i);
      // The workload is light (1 thread, 1 op batches) so that new replicas
      // bootstrap and converge quickly.
      unique_ptr<TestWorkload> workload(new TestWorkload(cluster_.get()));
      workload->set_table_name(table_name);
      workload->set_num_replicas(kRepFactor);
      workload->set_num_write_threads(1);
      workload->set_write_batch_size(1);
      workload->set_write_timeout_millis(timeout.ToMilliseconds());
      workload->set_already_present_allowed(true);
      workload->Setup();
      workload->Start();
      workloads.emplace_back(std::move(workload));
    }
  }

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
    "--move_single_replicas=enabled",
  };

  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
  }

  // Next run should report the cluster as balanced and no replica movement
  // should be attempted.
  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  }

  for (auto& workload : workloads) {
    workload->StopAndJoin();
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());

  // Now add a new tablet server into the cluster and make sure the rebalancer
  // will re-distribute replicas.
  ASSERT_OK(cluster_->AddTabletServer());
  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
    // The cluster was un-balanced, so many replicas should have been moved.
    ASSERT_STR_NOT_CONTAINS(out, "(moved 0 replicas)");
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
}

// Common base for the rebalancer-related test below.
class RebalancingTest :
    public tserver::TabletServerIntegrationTestBase,
    public ::testing::WithParamInterface<Kudu1097> {
 public:
  RebalancingTest(int num_tables = 10,
                  int rep_factor = 3,
                  int num_tservers = 8,
                  int tserver_unresponsive_ms = 3000,
                  const string& table_name_pattern = "rebalance_test_table_$0")
      : TabletServerIntegrationTestBase(),
        is_343_scheme_(GetParam() == Kudu1097::Enable),
        num_tables_(num_tables),
        rep_factor_(rep_factor),
        num_tservers_(num_tservers),
        tserver_unresponsive_ms_(tserver_unresponsive_ms),
        table_name_pattern_(table_name_pattern) {
    master_flags_ = {
      Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme_),
      Substitute("--tserver_unresponsive_timeout_ms=$0", tserver_unresponsive_ms_),
    };
    tserver_flags_ = {
      Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme_),
    };
  }

 protected:
  static const char* const kExitOnSignalStr;

  void Prepare(const vector<string>& extra_tserver_flags = {},
               const vector<string>& extra_master_flags = {}) {
    copy(extra_tserver_flags.begin(), extra_tserver_flags.end(),
         back_inserter(tserver_flags_));
    copy(extra_master_flags.begin(), extra_master_flags.end(),
         back_inserter(master_flags_));

    FLAGS_num_tablet_servers = num_tservers_;
    FLAGS_num_replicas = rep_factor_;
    NO_FATALS(BuildAndStart(tserver_flags_, master_flags_));

    ASSERT_OK(CreateUnbalancedTables(
        cluster_.get(), client_.get(), schema_, table_name_pattern_,
        num_tables_, rep_factor_, rep_factor_ + 1, num_tservers_,
        tserver_unresponsive_ms_));
  }

  // When the rebalancer starts moving replicas, ksck detects corruption
  // (that's why RuntimeError), seeing affected tables as non-healthy
  // with data state of corresponding tablets as TABLET_DATA_COPYING. If using
  // this method, it's a good idea to inject some latency into tablet copying
  // to be able to spot the TABLET_DATA_COPYING state, see the
  // '--tablet_copy_download_file_inject_latency_ms' flag for tservers.
  bool IsRebalancingInProgress() {
    string out;
    const auto s = RunKuduTool({
      "cluster",
      "ksck",
      cluster_->master()->bound_rpc_addr().ToString(),
    }, &out);
    if (s.IsRuntimeError() &&
        out.find("Data state:  TABLET_DATA_COPYING") != string::npos) {
      return true;
    }
    return false;
  }

  const bool is_343_scheme_;
  const int num_tables_;
  const int rep_factor_;
  const int num_tservers_;
  const int tserver_unresponsive_ms_;
  const string table_name_pattern_;
  vector<string> tserver_flags_;
  vector<string> master_flags_;
};
const char* const RebalancingTest::kExitOnSignalStr = "kudu: process exited on signal";

// Make sure the rebalancer is able to do its job if running concurrently
// with DDL activity on the cluster.
class DDLDuringRebalancingTest : public RebalancingTest {
 public:
  DDLDuringRebalancingTest()
      : RebalancingTest(20 /* num_tables */) {
  }
};
INSTANTIATE_TEST_CASE_P(, DDLDuringRebalancingTest,
                        ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(DDLDuringRebalancingTest, TablesCreatedAndDeletedDuringRebalancing) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  NO_FATALS(Prepare());

  // The latch that controls the lifecycle of the concurrent DDL activity.
  CountDownLatch run_latch(1);

  thread creator([&]() {
    KuduSchema client_schema(client::KuduSchemaFromSchema(schema_));
    for (auto idx = 0; ; ++idx) {
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(500))) {
        break;
      }
      const string table_name = Substitute("rebalancer_extra_table_$0", idx++);
      unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
      CHECK_OK(table_creator->table_name(table_name)
               .schema(&client_schema)
               .add_hash_partitions({ "key" }, 3)
               .num_replicas(rep_factor_)
               .Create());
    }
  });
  auto creator_cleanup = MakeScopedCleanup([&]() {
    run_latch.CountDown();
    creator.join();
  });

  thread deleter([&]() {
    for (auto idx = 0; idx < num_tables_; ++idx) {
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(500))) {
        break;
      }
      CHECK_OK(client_->DeleteTable(Substitute(table_name_pattern_, idx++)));
    }
  });
  auto deleter_cleanup = MakeScopedCleanup([&]() {
    run_latch.CountDown();
    deleter.join();
  });

  thread alterer([&]() {
    const string kTableName = "rebalancer_dynamic_table";
    const string kNewTableName = "rebalancer_dynamic_table_new_name";
    while (true) {
      // Create table.
      {
        KuduSchema schema;
        KuduSchemaBuilder builder;
        builder.AddColumn("key")->Type(KuduColumnSchema::INT64)->
            NotNull()->
            PrimaryKey();
        builder.AddColumn("a")->Type(KuduColumnSchema::INT64);
        CHECK_OK(builder.Build(&schema));
        unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
        CHECK_OK(table_creator->table_name(kTableName)
                 .schema(&schema)
                 .set_range_partition_columns({})
                 .num_replicas(rep_factor_)
                 .Create());
      }
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }

      // Drop a column.
      {
        unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
        alt->DropColumn("a");
        CHECK_OK(alt->Alter());
      }
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }

      // Add back the column with different type.
      {
        unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
        alt->AddColumn("a")->Type(KuduColumnSchema::STRING);
        CHECK_OK(alt->Alter());
      }
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }

      // Rename the table.
      {
        unique_ptr<KuduTableAlterer> alt(client_->NewTableAlterer(kTableName));
        alt->RenameTo(kNewTableName);
        CHECK_OK(alt->Alter());
      }
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }

      // Drop the renamed table.
      CHECK_OK(client_->DeleteTable(kNewTableName));
      if (run_latch.WaitFor(MonoDelta::FromMilliseconds(100))) {
          break;
      }
    }
  });
  auto alterer_cleanup = MakeScopedCleanup([&]() {
    run_latch.CountDown();
    alterer.join();
  });

  thread timer([&]() {
    SleepFor(MonoDelta::FromSeconds(30));
    run_latch.CountDown();
  });
  auto timer_cleanup = MakeScopedCleanup([&]() {
    timer.join();
  });

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  // Run the rebalancer concurrently with the DDL operations. The second run
  // of the rebalancer (the second run starts after joining the timer thread)
  // is necessary to balance the cluster after the DDL activity stops: that's
  // the easiest way to make sure the rebalancer will take into account
  // all DDL changes that happened.
  //
  // The signal to terminate the DDL activity (done via run_latch.CountDown())
  // is sent from a separate timer thread instead of doing SleepFor() after
  // the first run of the rebalancer followed by run_latch.CountDown().
  // That's to avoid dependency on the rebalancer behavior if it spots on-going
  // DDL activity and continues running over and over again.
  for (auto i = 0; i < 2; ++i) {
    if (i == 1) {
      timer.join();
      timer_cleanup.cancel();

      // Wait for all the DDL activity to complete.
      alterer.join();
      alterer_cleanup.cancel();

      deleter.join();
      deleter_cleanup.cancel();

      creator.join();
      creator_cleanup.cancel();
    }

    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
  }

  // Next (3rd) run should report the cluster as balanced and
  // no replica movement should be attempted.
  {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
}

// Make sure it's safe to run multiple rebalancers concurrently. The rebalancers
// might report errors, but they should not get stuck and the cluster should
// remain in good shape (i.e. no crashes, no data inconsistencies). Re-running a
// single rebalancer session again should bring the cluster to a balanced state.
class ConcurrentRebalancersTest : public RebalancingTest {
 public:
  ConcurrentRebalancersTest()
      : RebalancingTest(10 /* num_tables */) {
  }
};
INSTANTIATE_TEST_CASE_P(, ConcurrentRebalancersTest,
    ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(ConcurrentRebalancersTest, TwoConcurrentRebalancers) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  NO_FATALS(Prepare());

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  const auto runner_func = [&]() {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    if (!s.ok()) {
      // One might expect a bad status returned: e.g., due to some race so
      // the rebalancer didn't able to make progress for more than
      // --max_staleness_interval_sec, etc.
      LOG(INFO) << "rebalancer run info: " << ToolRunInfo(s, out, err);
    }

    // Should not exit on a signal: not expecting SIGSEGV, SIGABRT, etc.
    return s.ToString().find(kExitOnSignalStr) == string::npos;
  };

  CountDownLatch start_synchronizer(1);
  vector<thread> concurrent_runners;
  for (auto i = 0; i < 5; ++i) {
    concurrent_runners.emplace_back([&]() {
      start_synchronizer.Wait();
      CHECK(runner_func());
    });
  }

  // Run rebalancers concurrently and wait for their completion.
  start_synchronizer.CountDown();
  for (auto& runner : concurrent_runners) {
    runner.join();
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());

  {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << "stderr: " << err;
  }

  // Next run should report the cluster as balanced and no replica movement
  // should be attempted: at least one run of the rebalancer prior to this
  // should succeed, so next run is about running the tool against already
  // balanced cluster.
  {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
}

// The rebalancer should stop and exit upon detecting a tablet server that
// went down. That's a simple and effective way of preventing concurrent replica
// movement by the rebalancer and the automatic re-replication (the catalog
// manager tries to move replicas from the unreachable tablet server).
class TserverGoesDownDuringRebalancingTest : public RebalancingTest {
 public:
  TserverGoesDownDuringRebalancingTest() :
      RebalancingTest(5 /* num_tables */) {
  }
};
INSTANTIATE_TEST_CASE_P(, TserverGoesDownDuringRebalancingTest,
    ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(TserverGoesDownDuringRebalancingTest, TserverDown) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const vector<string> kTserverExtraFlags = {
    // Slow down tablet copy to make rebalancing step running longer
    // and become observable via tablet data states output by ksck.
    "--tablet_copy_download_file_inject_latency_ms=1500",

    "--follower_unavailable_considered_failed_sec=30",
  };
  NO_FATALS(Prepare(kTserverExtraFlags));

  // Pre-condition: 'kudu cluster ksck' should be happy with the cluster state
  // shortly after initial setup.
  ASSERT_EVENTUALLY([&]() {
    ASSERT_TOOL_OK(
      "cluster",
      "ksck",
      cluster_->master()->bound_rpc_addr().ToString()
    )
  });

  Random r(SeedRandom());
  const uint32_t shutdown_tserver_idx = r.Next() % num_tservers_;

  atomic<bool> run(true);
  // The thread that shuts down the selected tablet server.
  thread stopper([&]() {
    while (run && !IsRebalancingInProgress()) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }

    // All right, it's time to stop the selected tablet server.
    cluster_->tablet_server(shutdown_tserver_idx)->Shutdown();
  });
  auto stopper_cleanup = MakeScopedCleanup([&]() {
    run = false;
    stopper.join();
  });

  {
    string out;
    string err;
    const auto s = RunKuduTool({
      "cluster",
      "rebalance",
      cluster_->master()->bound_rpc_addr().ToString(),
      // Limiting the number of replicas to move. This is to make the rebalancer
      // run longer, making sure the rebalancing is in progress when the tablet
      // server goes down.
      "--max_moves_per_server=1",
    }, &out, &err);
    ASSERT_TRUE(s.IsRuntimeError()) << ToolRunInfo(s, out, err);

    // The rebalancer tool should not crash.
    ASSERT_STR_NOT_CONTAINS(s.ToString(), kExitOnSignalStr);
    ASSERT_STR_MATCHES(
        err, "Illegal state: tablet server .* \\(.*\\): "
             "unacceptable health status UNAVAILABLE");
  }

  run = false;
  stopper.join();
  stopper_cleanup.cancel();

  ASSERT_OK(cluster_->tablet_server(shutdown_tserver_idx)->Restart());
  NO_FATALS(cluster_->AssertNoCrashes());
}

// The rebalancer should continue working and complete rebalancing successfully
// if a new tablet server is added while the cluster is being rebalanced.
class TserverAddedDuringRebalancingTest : public RebalancingTest {
 public:
  TserverAddedDuringRebalancingTest()
      : RebalancingTest(10 /* num_tables */) {
  }
};
INSTANTIATE_TEST_CASE_P(, TserverAddedDuringRebalancingTest,
    ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(TserverAddedDuringRebalancingTest, TserverStarts) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const vector<string> kTserverExtraFlags = {
    // Slow down tablet copy to make rebalancing step running longer
    // and become observable via tablet data states output by ksck.
    "--tablet_copy_download_file_inject_latency_ms=1500",

    "--follower_unavailable_considered_failed_sec=30",
  };
  NO_FATALS(Prepare(kTserverExtraFlags));

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  atomic<bool> run(true);
  thread runner([&]() {
    while (run) {
      string out;
      string err;
      const auto s = RunKuduTool(tool_args, &out, &err);
      CHECK(s.ok()) << ToolRunInfo(s, out, err);
    }
  });
  auto runner_cleanup = MakeScopedCleanup([&]() {
    run = false;
    runner.join();
  });

  while (!IsRebalancingInProgress()) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // It's time to sneak in and add new tablet server.
  ASSERT_OK(cluster_->AddTabletServer());
  run = false;
  runner.join();
  runner_cleanup.cancel();

  // The rebalancer should not fail, and eventually, after a new tablet server
  // is added, the cluster should become balanced.
  ASSERT_EVENTUALLY([&]() {
    string out;
    string err;
    const auto s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
  });

  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
}

// Run rebalancer in 'election storms' environment and make sure the rebalancer
// does not exit prematurely or exhibit any other unexpected behavior.
class RebalancingDuringElectionStormTest : public RebalancingTest {
};
INSTANTIATE_TEST_CASE_P(, RebalancingDuringElectionStormTest,
    ::testing::Values(Kudu1097::Disable, Kudu1097::Enable));
TEST_P(RebalancingDuringElectionStormTest, RoundRobin) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  NO_FATALS(Prepare());

  atomic<bool> elector_run(true);
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
  // The timeout is a time-to-run for the stormy elector thread as well.
  // Making longer timeout for workload in case of TSAN/ASAN is not needed:
  // having everything generated written is not required.
  const auto timeout = MonoDelta::FromSeconds(5);
#else
  const auto timeout = MonoDelta::FromSeconds(10);
#endif
  const auto start_time = MonoTime::Now();
  thread elector([&]() {
    // Mininum viable divider for modulo ('%') to allow the result to grow by
    // the rules below.
    auto max_sleep_ms = 2.0;
    while (elector_run && MonoTime::Now() < start_time + timeout) {
      for (const auto& e : tablet_servers_) {
        const auto& ts_uuid = e.first;
        const auto* ts = e.second;
        vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
        auto const s = itest::ListTablets(ts, timeout, &tablets);
        if (!s.ok()) {
          LOG(WARNING) << ts_uuid << ": failed to get tablet list :"
                       << s.ToString();
          continue;
        }
        consensus::ConsensusServiceProxy proxy(
            cluster_->messenger(),
            cluster_->tablet_server_by_uuid(ts_uuid)->bound_rpc_addr(),
            "tserver " + ts_uuid);
        for (const auto& tablet : tablets) {
          const auto& tablet_id = tablet.tablet_status().tablet_id();
          consensus::RunLeaderElectionRequestPB req;
          req.set_tablet_id(tablet_id);
          req.set_dest_uuid(ts_uuid);
          rpc::RpcController rpc;
          rpc.set_timeout(timeout);
          consensus::RunLeaderElectionResponsePB resp;
          WARN_NOT_OK(proxy.RunLeaderElection(req, &resp, &rpc),
                      Substitute("failed to start election for tablet $0",
                                 tablet_id));
        }
        if (!elector_run || start_time + timeout <= MonoTime::Now()) {
          break;
        }
        auto sleep_ms = rand() % static_cast<int>(max_sleep_ms);
        SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
        max_sleep_ms = std::min(max_sleep_ms * 1.1, 2000.0);
      }
    }
  });
  auto elector_cleanup = MakeScopedCleanup([&]() {
    elector_run = false;
    elector.join();
  });

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  vector<unique_ptr<TestWorkload>> workloads;
  for (auto i = 0; i < num_tables_; ++i) {
    const string table_name = Substitute(table_name_pattern_, i);
    // The workload is light (1 thread, 1 op batches) and lenient to failures.
    unique_ptr<TestWorkload> workload(new TestWorkload(cluster_.get()));
    workload->set_table_name(table_name);
    workload->set_num_replicas(rep_factor_);
    workload->set_num_write_threads(1);
    workload->set_write_batch_size(1);
    workload->set_write_timeout_millis(timeout.ToMilliseconds());
    workload->set_already_present_allowed(true);
    workload->set_remote_error_allowed(true);
    workload->set_timeout_allowed(true);
    workload->Setup();
    workload->Start();
    workloads.emplace_back(std::move(workload));
  }
#endif

  const vector<string> tool_args = {
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
  };

  while (MonoTime::Now() < start_time + timeout) {
    // Rebalancer should not report any errors even if it's an election storm
    // unless a tablet server is reported as unavailable by ksck: the latter
    // usually happens because GetConsensusState requests are dropped due to
    // backpressure.
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    if (s.ok()) {
      ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
          << ToolRunInfo(s, out, err);
    } else {
      ASSERT_STR_CONTAINS(err, "unacceptable health status UNAVAILABLE")
          << ToolRunInfo(s, out, err);
    }
  }

  elector_run = false;
  elector.join();
  elector_cleanup.cancel();
#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  for (auto& workload : workloads) {
    workload->StopAndJoin();
  }
#endif

  // There might be some re-replication started as a result of election storm,
  // etc. Eventually, the system should heal itself and 'kudu cluster ksck'
  // should report no issues.
  ASSERT_EVENTUALLY([&]() {
    ASSERT_TOOL_OK(
      "cluster",
      "ksck",
      cluster_->master()->bound_rpc_addr().ToString()
    )
  });

  // The rebalancer should successfully rebalance the cluster after ksck
  // reported 'all is well'.
  {
    string out;
    string err;
    const Status s = RunKuduTool(tool_args, &out, &err);
    ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
    ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
        << ToolRunInfo(s, out, err);
  }

  NO_FATALS(cluster_->AssertNoCrashes());
}

// A test to verify how the rebalancer handles replicas of single-replica
// tablets in case of various values of the '--move_single_replicas' flag
// and replica management schemes.
class RebalancerAndSingleReplicaTablets :
    public AdminCliTest,
    public ::testing::WithParamInterface<tuple<string, Kudu1097>> {
};
INSTANTIATE_TEST_CASE_P(, RebalancerAndSingleReplicaTablets,
    ::testing::Combine(::testing::Values("auto", "enabled", "disabled"),
                       ::testing::Values(Kudu1097::Disable, Kudu1097::Enable)));
TEST_P(RebalancerAndSingleReplicaTablets, SingleReplicasStayOrMove) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  constexpr auto kRepFactor = 1;
  constexpr auto kNumTservers = 2 * (kRepFactor + 1);
  constexpr auto kNumTables = kNumTservers;
  constexpr auto kTserverUnresponsiveMs = 3000;
  const auto& param = GetParam();
  const auto& move_single_replica = std::get<0>(param);
  const auto is_343_scheme = (std::get<1>(param) == Kudu1097::Enable);
  const string table_name_pattern = "rebalance_test_table_$0";
  const vector<string> master_flags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
    Substitute("--tserver_unresponsive_timeout_ms=$0", kTserverUnresponsiveMs),
  };
  const vector<string> tserver_flags = {
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_343_scheme),
  };

  FLAGS_num_tablet_servers = kNumTservers;
  FLAGS_num_replicas = kRepFactor;
  NO_FATALS(BuildAndStart(tserver_flags, master_flags));

  // Keep running only (kRepFactor + 1) tablet servers and shut down the rest.
  for (auto i = kRepFactor + 1; i < kNumTservers; ++i) {
    cluster_->tablet_server(i)->Shutdown();
  }

  // Wait for the catalog manager to understand that only (kRepFactor + 1)
  // tablet servers are available.
  SleepFor(MonoDelta::FromMilliseconds(5 * kTserverUnresponsiveMs / 4));

  // Create few tables with their tablet replicas landing only on those
  // (kRepFactor + 1) running tablet servers.
  KuduSchema client_schema(client::KuduSchemaFromSchema(schema_));
  for (auto i = 0; i < kNumTables; ++i) {
    const string table_name = Substitute(table_name_pattern, i);
    unique_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    ASSERT_OK(table_creator->table_name(table_name)
              .schema(&client_schema)
              .add_hash_partitions({ "key" }, 3)
              .num_replicas(kRepFactor)
              .Create());
    ASSERT_TOOL_OK(
      "perf",
      "loadgen",
      cluster_->master()->bound_rpc_addr().ToString(),
      Substitute("--table_name=$0", table_name),
      // Don't need much data in there.
      "--num_threads=1",
      "--num_rows_per_thread=1",
    );
  }
  for (auto i = kRepFactor + 1; i < kNumTservers; ++i) {
    ASSERT_OK(cluster_->tablet_server(i)->Restart());
  }

  string out;
  string err;
  const Status s = RunKuduTool({
    "cluster",
    "rebalance",
    cluster_->master()->bound_rpc_addr().ToString(),
    Substitute("--move_single_replicas=$0", move_single_replica),
  }, &out, &err);
  ASSERT_TRUE(s.ok()) << ToolRunInfo(s, out, err);
  ASSERT_STR_CONTAINS(out, "rebalancing is complete: cluster is balanced")
      << "stderr: " << err;
  if (move_single_replica == "enabled" ||
      (move_single_replica == "auto" && is_343_scheme)) {
    // Should move appropriate replicas of single-replica tablets.
    ASSERT_STR_NOT_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
    ASSERT_STR_NOT_CONTAINS(err, "has single replica, skipping");
  } else {
    ASSERT_STR_CONTAINS(out,
        "rebalancing is complete: cluster is balanced (moved 0 replicas)")
        << "stderr: " << err;
    ASSERT_STR_MATCHES(err, "tablet .* of table '.*' (.*) has single replica, skipping");
  }

  NO_FATALS(cluster_->AssertNoCrashes());
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
}

} // namespace tools
} // namespace kudu
