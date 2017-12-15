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
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(raft_prepare_replacement_before_eviction);

using kudu::consensus::RaftPeerPB;
using kudu::itest::AddServer;
using kudu::itest::RemoveServer;
using kudu::itest::StartElection;
using kudu::itest::TServerDetails;
using kudu::itest::WaitForNumTabletsOnTS;
using kudu::itest::WaitForServersToAgree;
using kudu::itest::WaitUntilCommittedOpIdIndexIs;
using kudu::itest::WaitUntilTabletRunning;
using kudu::tablet::TABLET_DATA_READY;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tserver::ListTabletsResponsePB;
using std::map;
using std::set;
using std::string;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {

enum InstabilityType {
  NODE_DOWN,
  NODE_STOPPED
};

class TabletReplacementITest : public ExternalMiniClusterITestBase {
 protected:
  // Maps tablet identifier (UUID) into the set of TS UUIDs that host
  // replicas of the tablet.
  typedef map<string, vector<string>> TabletToReplicaUUIDs;

  Status GetTabletToReplicaUUIDsMapping(const MonoDelta& timeout,
                                        TabletToReplicaUUIDs* mappings) const;

  // Depending on replica management mode the test is running, not all elements
  // of ts_map_ are relevant. So, construct ts_map containing information on
  // tablet servers which host tablet replicas.
  void GetTsMapForReplicas(const vector<string>& replica_uuids,
                           unordered_map<string, TServerDetails*>* ts_map) const;

  void TestDontEvictIfRemainingConfigIsUnstable(InstabilityType type,
                                                bool is_3_4_3_mode);
};

Status TabletReplacementITest::GetTabletToReplicaUUIDsMapping(
    const MonoDelta& timeout,
    TabletToReplicaUUIDs* mappings) const {
  map<string, set<string>> tablet_to_replicas;
  for (const auto& e : ts_map_) {
    vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
    RETURN_NOT_OK(itest::ListTablets(e.second, timeout, &tablets));
    for (const auto& tablet : tablets) {
      const auto& tablet_id = tablet.tablet_status().tablet_id();
      tablet_to_replicas[tablet_id].insert(e.first);
    }
  }
  TabletToReplicaUUIDs ret;
  for (const auto& e : tablet_to_replicas) {
    ret.emplace(e.first, vector<string>(e.second.begin(), e.second.end()));
  }
  mappings->swap(ret);
  return Status::OK();
}

void TabletReplacementITest::GetTsMapForReplicas(
    const vector<string>& replica_uuids,
    unordered_map<string, TServerDetails*>* ts_map) const {
  decltype(ts_map_) ret;
  for (const auto& uuid : replica_uuids) {
    const auto it = ts_map_.find(uuid);
    ASSERT_NE(ts_map_.end(), it);
    ret[uuid] = it->second;
  }
  ts_map->swap(ret);
}

void TabletReplacementITest::TestDontEvictIfRemainingConfigIsUnstable(
    InstabilityType type, bool is_3_4_3_mode) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  constexpr auto kUnavailableSec = 3;
  constexpr auto kConsensusRpcTimeoutSec = 2;
  constexpr auto kNumReplicas = 3;
  const vector<string> ts_flags = {
    "--enable_leader_failure_detection=false",
    Substitute("--follower_unavailable_considered_failed_sec=$0", kUnavailableSec),
    Substitute("--consensus_rpc_timeout_ms=$0", kConsensusRpcTimeoutSec * 1000),
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_3_4_3_mode),
  };
  const vector<string> master_flags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_3_4_3_mode),
  };
  // Additional tablet server is needed when running in 3-4-3 replica management
  // scheme to allow for eviction of failed tablet replicas.
  const auto kNumTservers = is_3_4_3_mode ? kNumReplicas + 1 : kNumReplicas;
  NO_FATALS(StartCluster(ts_flags, master_flags, kNumTservers));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(kNumReplicas);
  workload.Setup(); // Easy way to create a new tablet.

  TabletToReplicaUUIDs tablet_to_replicas;
  ASSERT_OK(GetTabletToReplicaUUIDsMapping(kTimeout, &tablet_to_replicas));
  // There should be only one tablet.
  ASSERT_EQ(1, tablet_to_replicas.size());
  const string tablet_id = tablet_to_replicas.cbegin()->first;
  const auto& replica_uuids = tablet_to_replicas.cbegin()->second;
  ASSERT_EQ(kNumReplicas, replica_uuids.size());

  // Wait until all replicas are up and running.
  for (const auto& uuid : replica_uuids) {
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[uuid], tablet_id, kTimeout));
  }

  // Elect a leader.
  const auto& kLeaderId = replica_uuids[0];
  TServerDetails* leader_ts = ts_map_[kLeaderId];
  ASSERT_OK(StartElection(leader_ts, tablet_id, kTimeout));
  {
    decltype(ts_map_) ts_map;
    NO_FATALS(GetTsMapForReplicas(replica_uuids, &ts_map));
    // Wait for NO_OP.
    ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map, tablet_id, 1));
  }

  consensus::ConsensusStatePB cstate_initial;
  ASSERT_OK(GetConsensusState(leader_ts, tablet_id, kTimeout, &cstate_initial));

  const auto& kFollower1Id = replica_uuids[1];
  const auto& kFollower2Id = replica_uuids[2];

  // Shut down both followers and wait for enough time that the leader thinks they are
  // unresponsive. It should not trigger a config change to evict either one.
  switch (type) {
    case NODE_DOWN:
      cluster_->tablet_server_by_uuid(kFollower1Id)->Shutdown();
      cluster_->tablet_server_by_uuid(kFollower2Id)->Shutdown();
      break;
    case NODE_STOPPED:
      ASSERT_OK(cluster_->tablet_server_by_uuid(kFollower1Id)->Pause());
      ASSERT_OK(cluster_->tablet_server_by_uuid(kFollower2Id)->Pause());
      break;
  }

  // Sleep to make sure the leader replica recognized the stopped/shutdown
  // followers as unresponsive according to
  // --follower_unavailable_considered_failed_sec. Since unreachable peers
  // are not considered viable per PeerMessageQueue::SafeToEvictUnlocked(),
  // which makes that calculation based on --consensus_rpc_timeout_ms, we also
  // wait until that timeout expires to proceed. This ensures that later, when
  // we resume a follower, the leader does not consider itself unreachable,
  // which was a bug that we had (KUDU-2230) and that this test also serves as
  // a regression test for.
  auto min_sleep_required_sec = std::max(kUnavailableSec, kConsensusRpcTimeoutSec);
  SleepFor(MonoDelta::FromSeconds(2 * min_sleep_required_sec));

  {
    consensus::ConsensusStatePB cstate;
    ASSERT_OK(GetConsensusState(leader_ts, tablet_id, kTimeout, &cstate));
    SCOPED_TRACE(cstate.DebugString());
    ASSERT_FALSE(cstate.has_pending_config())
        << "Leader should not have issued any config change";
    ASSERT_EQ(cstate_initial.committed_config().opid_index(),
              cstate.committed_config().opid_index())
        << "Leader should not have issued any config change";
  }

  switch (type) {
    case NODE_DOWN:
      ASSERT_OK(cluster_->tablet_server_by_uuid(kFollower1Id)->Restart());
      break;
    case NODE_STOPPED:
      ASSERT_OK(cluster_->tablet_server_by_uuid(kFollower1Id)->Resume());
      break;
  }

  // At this point the majority of voters is back online, so the leader should
  // evict the failed replica, resulting in Raft configuration update.
  ASSERT_EVENTUALLY([&] {
    consensus::ConsensusStatePB cstate;
    ASSERT_OK(GetConsensusState(leader_ts, tablet_id, kTimeout, &cstate));
    ASSERT_GT(cstate.committed_config().opid_index(),
              cstate_initial.committed_config().opid_index() +
              (is_3_4_3_mode ? 1 : 0))
        << "Leader should have issued config change to evict failed follower;"
        << " the consensus state is: " << cstate.DebugString();
  });
}

// Test that the Master will tombstone a newly-evicted replica.
// Then, test that the Master will NOT tombstone a newly-added replica that is
// not part of the committed config yet (only the pending config).
TEST_F(TabletReplacementITest, TestMasterTombstoneEvictedReplica) {
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  vector<string> ts_flags = { "--enable_leader_failure_detection=false" };
  int num_tservers = 5;
  vector<string> master_flags = { "--master_add_server_when_underreplicated=false" };
  master_flags.emplace_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags, num_tservers));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(num_tservers);
  workload.Setup(); // Easy way to create a new tablet.

  const int kLeaderIndex = 0;
  TServerDetails* leader_ts = ts_map_[cluster_->tablet_server(kLeaderIndex)->uuid()];
  const int kFollowerIndex = 4;
  TServerDetails* follower_ts = ts_map_[cluster_->tablet_server(kFollowerIndex)->uuid()];

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(leader_ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ASSERT_OK(StartElection(leader_ts, tablet_id, timeout));
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 1)); // Wait for NO_OP.

  // Wait until it has committed its NO_OP, so that we can perform a config change.
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_ts, tablet_id, timeout));

  // Remove a follower from the config.
  ASSERT_OK(RemoveServer(leader_ts, tablet_id, follower_ts, timeout));

  // Wait for the Master to tombstone the replica.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kFollowerIndex, tablet_id,
                                                 { TABLET_DATA_TOMBSTONED },
                                                 timeout));

  if (!AllowSlowTests()) {
    // The rest of this test has multi-second waits, so we do it in slow test mode.
    LOG(WARNING) << "not verifying that a newly-added replica won't be tombstoned; "
                    "run with KUDU_ALLOW_SLOW_TESTS=1 to verify";
    return;
  }

  // Shut down a majority of followers (3 servers) and then try to add the
  // follower back to the config. This will cause the config change to end up
  // in a pending state.
  unordered_map<string, TServerDetails*> active_ts_map = ts_map_;
  for (int i = 1; i <= 3; i++) {
    cluster_->tablet_server(i)->Shutdown();
    ASSERT_EQ(1, active_ts_map.erase(cluster_->tablet_server(i)->uuid()));
  }
  // This will time out, but should take effect.
  Status s = AddServer(leader_ts, tablet_id, follower_ts, RaftPeerPB::VOTER,
                       MonoDelta::FromSeconds(5));
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kFollowerIndex, tablet_id, { TABLET_DATA_READY },
                                                 timeout));
  ASSERT_OK(WaitForServersToAgree(timeout, active_ts_map, tablet_id, 3));

  // Sleep for a few more seconds and check again to ensure that the Master
  // didn't end up tombstoning the replica.
  SleepFor(MonoDelta::FromSeconds(3));
  ASSERT_OK(inspect_->CheckTabletDataStateOnTS(kFollowerIndex, tablet_id, { TABLET_DATA_READY }));
}

// Test for KUDU-2138: ensure that the master will tombstone failed tablets
// that have previously been evicted.
TEST_F(TabletReplacementITest, TestMasterTombstoneFailedEvictedReplicaOnReport) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kNumServers = 4;
  NO_FATALS(StartCluster({"--follower_unavailable_considered_failed_sec=5"},
      {"--master_tombstone_evicted_tablet_replicas=false"}, kNumServers));

  TestWorkload workload(cluster_.get());
  workload.Setup(); // Easy way to create a new tablet.

  // Determine the tablet id.
  string tablet_id;
  ASSERT_EVENTUALLY([&] {
      vector<string> tablets = inspect_->ListTablets();
      ASSERT_FALSE(tablets.empty());
      tablet_id = tablets[0];
  });

  // Determine which tablet servers have data. One should be empty.
  unordered_map<string, TServerDetails*> active_ts_map = ts_map_;
  int empty_server_idx = -1;
  string empty_ts_uuid;
  consensus::ConsensusMetadataPB cmeta_pb;
  for (int i = 0; i < kNumServers; i++) {
    consensus::ConsensusMetadataPB cmeta_pb;
    if (inspect_->ReadConsensusMetadataOnTS(i, tablet_id, &cmeta_pb).IsNotFound()) {
      empty_ts_uuid = cluster_->tablet_server(i)->uuid();
      ASSERT_EQ(1, active_ts_map.erase(empty_ts_uuid));
      empty_server_idx = i;
      break;
    }
  }
  ASSERT_NE(empty_server_idx, -1);

  // Wait until all replicas are up and running.
  for (const auto& e : active_ts_map) {
    ASSERT_OK(WaitUntilTabletRunning(e.second, tablet_id, kTimeout));
  }

  // Select a replica to fail by shutting it down and mucking with its
  // metadata. When it restarts, it will fail to open.
  int idx_to_fail = (empty_server_idx + 1) % kNumServers;
  auto* ts = cluster_->tablet_server(idx_to_fail);
  ts->Shutdown();
  ASSERT_OK(inspect_->ReadConsensusMetadataOnTS(idx_to_fail, tablet_id, &cmeta_pb));
  cmeta_pb.set_current_term(-1);
  ASSERT_OK(inspect_->WriteConsensusMetadataOnTS(idx_to_fail, tablet_id, cmeta_pb));

  // Wait until the replica is evicted and replicated to the empty server.
  ASSERT_OK(WaitUntilTabletInState(ts_map_[empty_ts_uuid],
                                   tablet_id,
                                   tablet::RUNNING,
                                   kTimeout));

  // Restart the tserver and ensure the tablet is failed.
  ASSERT_OK(ts->Restart());
  ASSERT_OK(WaitUntilTabletInState(ts_map_[ts->uuid()],
                                   tablet_id,
                                   tablet::FAILED,
                                   kTimeout));

  // Upon restarting, the master will request a report and notice the failed
  // replica. Wait for the master to tombstone the failed follower.
  cluster_->master()->Shutdown();
  cluster_->master()->mutable_flags()->emplace_back(
      "--master_tombstone_evicted_tablet_replicas=true");
  ASSERT_OK(cluster_->master()->Restart());
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(idx_to_fail, tablet_id,
                                                 { TABLET_DATA_TOMBSTONED },
                                                 kTimeout));
}

// Ensure that the Master will tombstone a replica if it reports in with an old
// config. This tests a slightly different code path in the catalog manager
// than TestMasterTombstoneEvictedReplica does.
TEST_F(TabletReplacementITest, TestMasterTombstoneOldReplicaOnReport) {
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  vector<string> ts_flags = { "--enable_leader_failure_detection=false" };
  vector<string> master_flags = { "--master_add_server_when_underreplicated=false" };
  master_flags.emplace_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  TestWorkload workload(cluster_.get());
  workload.Setup(); // Easy way to create a new tablet.

  const int kLeaderIndex = 0;
  TServerDetails* leader_ts = ts_map_[cluster_->tablet_server(kLeaderIndex)->uuid()];
  const int kFollowerIndex = 2;
  TServerDetails* follower_ts = ts_map_[cluster_->tablet_server(kFollowerIndex)->uuid()];

  // Figure out the tablet id of the created tablet.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(leader_ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ASSERT_OK(StartElection(leader_ts, tablet_id, timeout));
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 1)); // Wait for NO_OP.

  // Wait until it has committed its NO_OP, so that we can perform a config change.
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, leader_ts, tablet_id, timeout));

  // Shut down the follower to be removed, then remove it from the config.
  // We will wait for the Master to be notified of the config change, then shut
  // down the rest of the cluster and bring the follower back up. The follower
  // will heartbeat to the Master and then be tombstoned.
  cluster_->tablet_server(kFollowerIndex)->Shutdown();

  // Remove the follower from the config and wait for the Master to notice the
  // config change.
  ASSERT_OK(RemoveServer(leader_ts, tablet_id, follower_ts, timeout));
  ASSERT_OK(itest::WaitForNumVotersInConfigOnMaster(cluster_->master_proxy(), tablet_id, 2,
                                                    timeout));

  // Shut down the remaining tablet servers and restart the dead one.
  cluster_->tablet_server(0)->Shutdown();
  cluster_->tablet_server(1)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(kFollowerIndex)->Restart());

  // Wait for the Master to tombstone the revived follower.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(kFollowerIndex, tablet_id,
                                                 { TABLET_DATA_TOMBSTONED },
                                                 timeout));
}

/////////////////////////////////////////////////////////////////////////////

class EvictAndReplaceDeadFollowerITest :
    public TabletReplacementITest,
    public ::testing::WithParamInterface<bool> {
};

// Test that unreachable followers are evicted and replaced.
TEST_P(EvictAndReplaceDeadFollowerITest, UnreachableFollower) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const bool is_3_4_3_mode = GetParam();
  MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const vector<string> ts_flags = {
    "--enable_leader_failure_detection=false",
    "--follower_unavailable_considered_failed_sec=5",
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_3_4_3_mode),
  };
  const vector<string> master_flags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
    Substitute("--raft_prepare_replacement_before_eviction=$0", is_3_4_3_mode),
  };
  constexpr auto kNumReplicas = 3;

  // Additional tablet server is needed when running in 3-4-3 replica management
  // scheme to allow for eviction of failed tablet replicas.
  const auto kNumTservers = is_3_4_3_mode ? kNumReplicas + 1 : kNumReplicas;

  NO_FATALS(StartCluster(ts_flags, master_flags, kNumTservers));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(kNumReplicas);
  workload.Setup(); // Easy way to create a new tablet.

  TabletToReplicaUUIDs tablet_to_replicas;
  ASSERT_OK(GetTabletToReplicaUUIDsMapping(kTimeout, &tablet_to_replicas));
  // There should be only one tablet.
  ASSERT_EQ(1, tablet_to_replicas.size());
  const string tablet_id = tablet_to_replicas.cbegin()->first;
  const auto& replica_uuids = tablet_to_replicas.cbegin()->second;
  ASSERT_EQ(kNumReplicas, replica_uuids.size());

  // Wait until all replicas are up and running.
  for (const auto& uuid : replica_uuids) {
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[uuid], tablet_id, kTimeout));
  }

  // Elect a leader.
  const auto& kLeaderId = replica_uuids.front();
  TServerDetails* leader_ts = ts_map_[kLeaderId];
  ASSERT_OK(StartElection(leader_ts, tablet_id, kTimeout));
  {
    decltype(ts_map_) ts_map;
    NO_FATALS(GetTsMapForReplicas(replica_uuids, &ts_map));
    // Wait for NO_OP.
    ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map, tablet_id, 1));
  }

  // Shut down the follower to be removed. It should be evicted.
  const auto& kFollowerId = replica_uuids.back();
  cluster_->tablet_server_by_uuid(kFollowerId)->Shutdown();

  // Expected OpId index of the committed config:
  //   * with AddServer, Promote and RemoveServer, the opid_index will be 4.
  //   * with RemoveServer and AddServer, the opid_index will be 3.
  const auto expected_opid_index = is_3_4_3_mode ? 4 : 3;
  ASSERT_OK(itest::WaitUntilCommittedConfigOpIdIndexIs(
      expected_opid_index, leader_ts, tablet_id, kTimeout));
  ASSERT_OK(cluster_->tablet_server_by_uuid(kFollowerId)->Restart());
}

INSTANTIATE_TEST_CASE_P(,
                        EvictAndReplaceDeadFollowerITest,
                        ::testing::Bool());

/////////////////////////////////////////////////////////////////////////////

class DontEvictIfRemainingConfigIsUnstableITest :
    public TabletReplacementITest,
    public ::testing::WithParamInterface<bool> {
};

// Regression test for KUDU-2048 and KUDU-2230. If a majority of followers are
// unresponsive, the leader should not evict any of them.
TEST_P(DontEvictIfRemainingConfigIsUnstableITest, NodesDown) {
  TestDontEvictIfRemainingConfigIsUnstable(NODE_DOWN, GetParam());
}

TEST_P(DontEvictIfRemainingConfigIsUnstableITest, NodesStopped) {
  TestDontEvictIfRemainingConfigIsUnstable(NODE_STOPPED, GetParam());
}

INSTANTIATE_TEST_CASE_P(,
                        DontEvictIfRemainingConfigIsUnstableITest,
                        ::testing::Bool());

/////////////////////////////////////////////////////////////////////////////

// Regression test for KUDU-1233. This test creates a situation in which tablet
// bootstrap will attempt to replay committed (and applied) config change
// operations. This is achieved by delaying application of a write at the
// tablet level that precedes the config change operations in the WAL, then
// initiating a tablet copy to a follower. The follower will not have the
// COMMIT for the write operation, so will ignore COMMIT messages for the
// applied config change operations. At startup time, the newly
// copied tablet should detect that these config change
// operations have already been applied and skip them.
TEST_F(TabletReplacementITest, TestRemoteBoostrapWithPendingConfigChangeCommits) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const MonoDelta timeout = MonoDelta::FromSeconds(30);
  vector<string> ts_flags;
  ts_flags.emplace_back("--enable_leader_failure_detection=false");
  vector<string> master_flags;
  // We will manage doing the AddServer() manually, in order to make this test
  // more deterministic.
  master_flags.emplace_back("--master_add_server_when_underreplicated=false");
  master_flags.emplace_back("--master_tombstone_evicted_tablet_replicas=false");
  master_flags.emplace_back("--catalog_manager_wait_for_new_tablets_to_elect_leader=false");
  NO_FATALS(StartCluster(ts_flags, master_flags));

  TestWorkload workload(cluster_.get());
  workload.Setup(); // Convenient way to create a table.

  const int kLeaderIndex = 0;
  TServerDetails* leader_ts = ts_map_[cluster_->tablet_server(kLeaderIndex)->uuid()];
  const int kFollowerIndex = 2;
  TServerDetails* ts_to_remove = ts_map_[cluster_->tablet_server(kFollowerIndex)->uuid()];

  // Wait for tablet creation and then identify the tablet id.
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(leader_ts, 1, timeout, &tablets));
  string tablet_id = tablets[0].tablet_status().tablet_id();

  // Wait until all replicas are up and running.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                     tablet_id, timeout));
  }

  // Elect a leader (TS 0)
  ASSERT_OK(StartElection(leader_ts, tablet_id, timeout));
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 1)); // Wait for NO_OP.

  // Write a single row.
  ASSERT_OK(WriteSimpleTestRow(leader_ts, tablet_id, RowOperationsPB::INSERT, 0, 0, "", timeout));

  // Delay tablet applies in order to delay COMMIT messages to trigger KUDU-1233.
  // Then insert another row.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server_by_uuid(leader_ts->uuid()),
                              "tablet_inject_latency_on_apply_write_txn_ms", "5000"));

  // Kick off an async insert, which will be delayed for 5 seconds. This is
  // normally enough time to evict a replica, tombstone it, add it back, and
  // Tablet Copy a new replica to it when the log is only a few entries.
  tserver::WriteRequestPB req;
  tserver::WriteResponsePB resp;
  CountDownLatch latch(1);
  rpc::RpcController rpc;
  rpc.set_timeout(timeout);
  req.set_tablet_id(tablet_id);
  Schema schema = GetSimpleTestSchema();
  ASSERT_OK(SchemaToPB(schema, req.mutable_schema()));
  AddTestRowToPB(RowOperationsPB::INSERT, schema, 1, 1, "", req.mutable_row_operations());
  leader_ts->tserver_proxy->WriteAsync(req, &resp, &rpc,
                                       boost::bind(&CountDownLatch::CountDown, &latch));

  // Wait for the replicate to show up (this doesn't wait for COMMIT messages).
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, 3));

  // Manually evict the server from the cluster, tombstone the replica, then
  // add the replica back to the cluster. Without the fix for KUDU-1233, this
  // will cause the replica to fail to start up.
  ASSERT_OK(RemoveServer(leader_ts, tablet_id, ts_to_remove, timeout));
  ASSERT_OK(itest::DeleteTablet(ts_to_remove, tablet_id, TABLET_DATA_TOMBSTONED,
                                timeout));
  ASSERT_OK(AddServer(leader_ts, tablet_id, ts_to_remove, RaftPeerPB::VOTER, timeout));
  ASSERT_OK(WaitUntilTabletRunning(ts_to_remove, tablet_id, timeout));

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(),
                            ClusterVerifier::EXACTLY, 2));

  latch.Wait(); // Avoid use-after-free on the response from the delayed RPC callback.
}

} // namespace kudu
