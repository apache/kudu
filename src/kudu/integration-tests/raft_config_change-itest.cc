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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using boost::none;
using kudu::consensus::ADD_PEER;
using kudu::consensus::COMMITTED_OPID;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::GetRaftConfigMember;
using kudu::consensus::MODIFY_PEER;
using kudu::consensus::RaftPeerAttrsPB;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::REMOVE_PEER;
using kudu::itest::BulkChangeConfig;
using kudu::itest::GetTableLocations;
using kudu::itest::TServerDetails;
using kudu::master::VOTER_REPLICA;
using kudu::pb_util::SecureShortDebugString;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

class RaftConfigChangeITest : public ExternalMiniClusterITestBase {
};

// Regression test for KUDU-2147. In this test we cause an initial leader to
// heartbeat to a master with a new configuration change immediately after it
// has lost its leadership, reporting a new configuration with no leader. The
// master should update its record of which replica is the leader after a new
// leader is elected in that term.
//
// Steps followed in this test:
//
// 1. Inject latency into TS heartbeats to the master and reduce Raft leader
//    heartbeat intervals to followers to be able to carefully control the
//    sequence of events in this test.
// 2. Evict a follower from the config.
// 3. Immediately start an election on the remaining follower in the config.
//    We know that this follower should win the election because in order to
//    commit a removal from 3 voters, the removal op must be replicated to both
//    of the two remaining voters in the config.
// 4. The master will get the config change heartbeat from the initial leader,
//    which will indicate a new term and no current leader.
// 5. The master will then get a heartbeat from the new leader to report that
//    it's now the leader of the config.
// 6. Once the master knows who the new leader is, the master will instruct the
//    new leader to add a new replica to its config to bring it back up to 3
//    voters. That new replica will be the follower that we evicted in step 2.
// 7. If that succeeds then the leader will replicate the eviction, the
//    leader's own no-op, and the adding-back-in of the evicted replica to that
//    evicted replica's log.
// 8. Once that process completes, all 3 replicas will have identical logs,
//    which is what we wait for at the end of the test.
TEST_F(RaftConfigChangeITest, TestKudu2147) {
  if (!AllowSlowTests()) {
    // This test injects seconds of latency so can take a while to converge.
    LOG(WARNING) << "Skipping test in fast-test mode.";
    return;
  }
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  // Slow down leader heartbeats so that in the explicit election below, the
  // second leader does not immediately heartbeat to the initial leader. If
  // that occurs, the initial leader will not report to the master that there
  // is currently no leader.
  NO_FATALS(StartCluster({"--raft_heartbeat_interval_ms=3000"}));

  // Create a new table.
  TestWorkload workload(cluster_.get());
  workload.Setup();
  workload.Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(workload.batches_completed(), 10);
  });
  workload.StopAndJoin();

  // The table should have replicas on three tservers.
  ASSERT_OK(inspect_->WaitForReplicaCount(3));
  master::GetTableLocationsResponsePB table_locations;
  ASSERT_OK(GetTableLocations(cluster_->master_proxy(), TestWorkload::kDefaultTableName,
                              kTimeout, VOTER_REPLICA, /*table_id=*/none, &table_locations));
  ASSERT_EQ(1, table_locations.tablet_locations_size()); // Only 1 tablet.
  ASSERT_EQ(3, table_locations.tablet_locations(0).interned_replicas_size()); // 3 replicas.
  string tablet_id = table_locations.tablet_locations(0).tablet_id();

  // Wait for all 3 replicas to converge before we start the test.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));

  // Retry as needed to counter normal leader election activity.
  ASSERT_EVENTUALLY([&] {
    // Find initial leader.
    TServerDetails* leader;
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader));
    ASSERT_OK(WaitForOpFromCurrentTerm(leader, tablet_id, COMMITTED_OPID, kTimeout));

    vector<TServerDetails*> followers;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      // Dynamically set the latency injection flag to induce TS -> master
      // heartbeat delays. The leader delays its master heartbeats by 2 sec
      // each time; followers delay their master heartbeats by even longer.
      int ms_delay = cluster_->tablet_server(i)->uuid() == leader->uuid() ? 2000 : 5000;
      ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(i),
                "heartbeat_inject_latency_before_heartbeat_ms",
                Substitute("$0", ms_delay)));
      // Keep track of the followers.
      if (cluster_->tablet_server(i)->uuid() != leader->uuid()) {
        followers.push_back(ts_map_[cluster_->tablet_server(i)->uuid()]);
      }
    }
    ASSERT_EQ(2, followers.size());

    // Now that heartbeat injection is enabled, evict one follower and trigger
    // an election on another follower immediately thereafter.
    ASSERT_OK(itest::RemoveServer(leader, tablet_id, followers[0], kTimeout));

    // Immediately start an election on the remaining follower. This will cause
    // the initial leader's term to rev and it will have to step down. When it
    // sends a tablet report to the master with the new configuration excluding
    // the removed tablet it will report an unknown leader in the new term.
    ASSERT_OK(itest::StartElection(followers[1], tablet_id, kTimeout));
  });

  // Wait until the master re-adds the evicted replica and it is back up and
  // running. If the master hit KUDU-2147, this would fail because the master
  // would be unable to add the removed server back, and that replica would be
  // missing the config change op that removed it from the config.
  ASSERT_OK(WaitForServersToAgree(kTimeout, ts_map_, tablet_id, 1));
}

// Test automatic promotion of a non-voter replica in a 3-4-3 re-replication
// (KUDU-1097) paradigm.
TEST_F(RaftConfigChangeITest, TestNonVoterPromotion) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  // Enable 3-4-3 re-replication.
  NO_FATALS(StartCluster({"--raft_prepare_replacement_before_eviction=true"},
                         {"--raft_prepare_replacement_before_eviction=true",
                          "--catalog_manager_evict_excess_replicas=false"},
                         /*num_tablet_servers=*/ 4));
  TestWorkload workload(cluster_.get());
  workload.Setup();

  // The table should initially have replicas on three tservers.
  ASSERT_OK(inspect_->WaitForReplicaCount(3));
  master::GetTableLocationsResponsePB table_locations;
  ASSERT_OK(GetTableLocations(cluster_->master_proxy(), TestWorkload::kDefaultTableName,
                              kTimeout, VOTER_REPLICA, /*table_id=*/none, &table_locations));
  ASSERT_EQ(1, table_locations.tablet_locations_size()); // Only 1 tablet.
  ASSERT_EQ(3, table_locations.tablet_locations(0).interned_replicas_size()); // 3 replicas.
  string tablet_id = table_locations.tablet_locations(0).tablet_id();

  // Find the TS that does not have a replica.
  unordered_set<string> initial_replicas;
  for (const auto& replica : table_locations.tablet_locations(0).interned_replicas()) {
    const auto& uuid = table_locations.ts_infos(replica.ts_info_idx()).permanent_uuid();
    initial_replicas.insert(uuid);
  }
  TServerDetails* new_replica = nullptr;
  for (const auto& entry : ts_map_) {
    if (!ContainsKey(initial_replicas, entry.first)) {
      new_replica = entry.second;
      break;
    }
  }
  ASSERT_NE(nullptr, new_replica);

  TServerDetails* leader_replica = nullptr;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(FindTabletLeader(ts_map_, tablet_id, kTimeout, &leader_replica));
    ASSERT_NE(new_replica, leader_replica);

    // Add the 4th replica as a NON_VOTER with promote=true.
    RaftPeerAttrsPB attrs;
    attrs.set_promote(true);
    ASSERT_OK(AddServer(leader_replica, tablet_id, new_replica,
                        RaftPeerPB::NON_VOTER, kTimeout, attrs));
  });

  // Wait for there to be 4 voters in the config.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(/*num_voters=*/ 4,
                                                leader_replica,
                                                tablet_id,
                                                kTimeout));

  NO_FATALS(cluster_->AssertNoCrashes());
}

// Functional test for the BulkChangeConfig RPC API.
TEST_F(RaftConfigChangeITest, TestBulkChangeConfig) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const int kNumTabletServers = 4;
  const int kNumInitialReplicas = 3;
  NO_FATALS(StartCluster({"--enable_leader_failure_detection=false",
                          "--raft_prepare_replacement_before_eviction=true"},
                         {"--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
                          "--raft_prepare_replacement_before_eviction=true"},
                         kNumTabletServers));

  // Create a table.
  TestWorkload workload(cluster_.get());
  workload.Setup();

  ASSERT_OK(inspect_->WaitForReplicaCount(kNumInitialReplicas));
  master::GetTableLocationsResponsePB table_locations;
  ASSERT_OK(GetTableLocations(cluster_->master_proxy(), TestWorkload::kDefaultTableName,
                              kTimeout, VOTER_REPLICA, /*table_id=*/none, &table_locations));
  ASSERT_EQ(1, table_locations.tablet_locations_size()); // Only 1 tablet.
  ASSERT_EQ(kNumInitialReplicas, table_locations.tablet_locations(0).interned_replicas_size());
  string tablet_id = table_locations.tablet_locations(0).tablet_id();
  unordered_set<int> replica_indexes;
  for (const auto& replica : table_locations.tablet_locations(0).interned_replicas()) {
    const auto& uuid = table_locations.ts_infos(replica.ts_info_idx()).permanent_uuid();
    int idx = cluster_->tablet_server_index_by_uuid(uuid);
    replica_indexes.emplace(idx);
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(idx)->uuid()],
                                            tablet_id, kTimeout));
  }
  ASSERT_EQ(kNumInitialReplicas, replica_indexes.size());
  const int kLeaderIndex = *replica_indexes.begin();
  int new_replica_index = -1;
  for (int i = 0; i < kNumTabletServers; i++) {
    if (!ContainsKey(replica_indexes, i)) {
      new_replica_index = i;
    }
  }
  ASSERT_NE(-1, new_replica_index);

  string leader_uuid = cluster_->tablet_server(kLeaderIndex)->uuid();
  auto* leader_replica = ts_map_[leader_uuid];
  ASSERT_OK(itest::StartElection(leader_replica, tablet_id, kTimeout));
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // We don't want the master interfering with the rest of the test.
  cluster_->master()->Shutdown();

  struct BulkSpec {
    consensus::ChangeConfigType change_type;
    int tserver_index;
    RaftPeerPB::MemberType member_type;
    bool replace;
    bool promote;
  };

  // Now comes the actual config change testing.
  auto bulk_change = [&](const vector<BulkSpec>& changes,
                         boost::optional<int64_t> cas_config_index = boost::none) {
    vector<consensus::BulkChangeConfigRequestPB::ConfigChangeItemPB> changes_pb;
    for (const auto& chg : changes) {
      const auto& ts_uuid = cluster_->tablet_server(chg.tserver_index)->uuid();
      auto* replica = ts_map_[ts_uuid];

      consensus::BulkChangeConfigRequestPB::ConfigChangeItemPB change_pb;
      change_pb.set_type(chg.change_type);

      RaftPeerPB* peer = change_pb.mutable_peer();
      peer->set_permanent_uuid(ts_uuid);
      peer->set_member_type(chg.member_type);
      peer->mutable_attrs()->set_replace(chg.replace);
      peer->mutable_attrs()->set_promote(chg.promote);
      *peer->mutable_last_known_addr() = replica->registration.rpc_addresses(0);
      changes_pb.emplace_back(std::move(change_pb));
    }

    LOG(INFO) << "submitting config change with changes:";
    for (const auto& change_pb : changes_pb) {
      LOG(INFO) << SecureShortDebugString(change_pb);
    }
    return BulkChangeConfig(leader_replica, tablet_id, changes_pb,
                            kTimeout, cas_config_index);
  };

  // 1) Add a voter. Change config to: V, V, V, V.
  ASSERT_OK(bulk_change({ { ADD_PEER, new_replica_index, RaftPeerPB::VOTER,
                            /*replace=*/ false, /*promote=*/ false } }));
  ConsensusStatePB cstate;
  ASSERT_OK(WaitUntilNoPendingConfig(leader_replica, tablet_id, kTimeout, &cstate));
  ASSERT_EQ(kNumTabletServers, cstate.committed_config().peers_size());
  ASSERT_EQ(kNumTabletServers, CountVoters(cstate.committed_config()));

  // 2) Simultaneous voter modification and attribute modification.
  //    Change config to: V, V, N, V+p.
  //    Note: setting a VOTER's attribute promote=true is meaningless.
  int replica1_idx = (kLeaderIndex + 1) % kNumTabletServers;
  int replica2_idx = (kLeaderIndex + 2) % kNumTabletServers;
  ASSERT_OK(bulk_change({ { MODIFY_PEER, replica1_idx, RaftPeerPB::NON_VOTER,
                            /*replace=*/ false, /*promote=*/ false },
                          { MODIFY_PEER, replica2_idx, RaftPeerPB::VOTER,
                            /*replace=*/ false,  /*promote=*/ true } }));

  ASSERT_OK(WaitUntilNoPendingConfig(leader_replica, tablet_id, kTimeout, &cstate));
  ASSERT_EQ(kNumTabletServers, cstate.committed_config().peers_size());
  ASSERT_EQ(kNumInitialReplicas, CountVoters(cstate.committed_config()));

  RaftPeerPB* peer;
  ASSERT_OK(GetRaftConfigMember(cstate.mutable_committed_config(),
                                cluster_->tablet_server(replica2_idx)->uuid(), &peer));
  ASSERT_EQ(RaftPeerPB::VOTER, peer->member_type());
  ASSERT_TRUE(peer->attrs().promote()) << SecureShortDebugString(*peer);

  // 3) Single-attribute modification. Change config to: V, V, N+r, V+p.
  //    Note: at the time of writing, if the master is disabled this
  //    configuration will not trigger any actions such as promotion or
  //    eviction.
  ASSERT_OK(bulk_change({ { MODIFY_PEER, replica1_idx, RaftPeerPB::NON_VOTER,
                            /*replace=*/ true, /*promote=*/ false } }));

  ASSERT_OK(WaitUntilNoPendingConfig(leader_replica, tablet_id, kTimeout, &cstate));
  ASSERT_EQ(kNumTabletServers, cstate.committed_config().peers_size())
      << SecureShortDebugString(cstate);
  ASSERT_EQ(kNumInitialReplicas, CountVoters(cstate.committed_config()))
      << SecureShortDebugString(cstate);

  ASSERT_OK(GetRaftConfigMember(cstate.mutable_committed_config(),
                                cluster_->tablet_server(replica1_idx)->uuid(), &peer));
  ASSERT_EQ(RaftPeerPB::NON_VOTER, peer->member_type());
  ASSERT_TRUE(peer->attrs().replace()) << SecureShortDebugString(*peer);

  // 4) Deny changing config (illegally) from: { V, V, N, V } to: { V, V, V, N }
  //    because that would be both a promotion and a demotion in one step.
  Status s = bulk_change({ { MODIFY_PEER, replica1_idx, RaftPeerPB::VOTER,
                             /*replace=*/ false, /*promote=*/ false },
                           { MODIFY_PEER, replica2_idx, RaftPeerPB::NON_VOTER,
                             /*replace=*/ false, /*promote=*/ false } });
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "not safe to modify the VOTER status of "
                                    "more than one peer at a time");

  // 5) The caller must not be allowed to make the leader a NON_VOTER.
  s = bulk_change({ { MODIFY_PEER, kLeaderIndex, RaftPeerPB::NON_VOTER,
                      /*replace=*/ false, /*promote=*/ false } });
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "Cannot modify member type of peer .* because it is the leader");

  // 6) The 'cas_config_index' flag must be respected, if set.
  int64_t committed_config_opid_index = cstate.committed_config().opid_index();
  s = bulk_change({ { MODIFY_PEER, replica1_idx, RaftPeerPB::NON_VOTER,
                      /*replace=*/ false, /*promote=*/ true } }, committed_config_opid_index + 1);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "specified cas_config_opid_index of .* but "
                                   "the committed config has opid_index of .*");

  // 7) Evict down to 2 voters. We will evict a voter and a non-voter at once.
  ASSERT_OK(bulk_change({ { REMOVE_PEER, replica1_idx, RaftPeerPB::UNKNOWN_MEMBER_TYPE,
                            /*replace=*/ false, /*promote=*/ false },
                          { REMOVE_PEER, replica2_idx, RaftPeerPB::UNKNOWN_MEMBER_TYPE,
                            /*replace=*/ false, /*promote=*/ false } }));
  ASSERT_OK(WaitUntilNoPendingConfig(leader_replica, tablet_id, kTimeout, &cstate));
  ASSERT_EQ(2, cstate.committed_config().peers_size());
  ASSERT_EQ(2, CountVoters(cstate.committed_config()));

  // 8) We should reject adding multiple voters at once.
  s = bulk_change({ { ADD_PEER, replica1_idx, RaftPeerPB::VOTER,
                      /*replace=*/ false, /*promote=*/ false },
                    { ADD_PEER, replica2_idx, RaftPeerPB::VOTER,
                      /*replace=*/ false, /*promote=*/ false } });
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "not safe to modify the VOTER status of "
                                   "more than one peer at a time");

  // 9) Add them back one at a time so we get to full strength (4 voters) again.
  auto to_restore = { replica1_idx, replica2_idx };
  for (auto r : to_restore) {
    ASSERT_OK(bulk_change({ { ADD_PEER, r, RaftPeerPB::VOTER,
                              /*replace=*/ false, /*promote=*/ false } }));
    ASSERT_OK(WaitUntilNoPendingConfig(leader_replica, tablet_id, kTimeout, &cstate));
  }
  ASSERT_EQ(kNumTabletServers, cstate.committed_config().peers_size());
  ASSERT_EQ(kNumTabletServers, CountVoters(cstate.committed_config()));

  // 10) We should reject removing multiple voters at once.
  s = bulk_change({ { REMOVE_PEER, replica1_idx, RaftPeerPB::UNKNOWN_MEMBER_TYPE,
                      /*replace=*/ false, /*promote=*/ false },
                    { REMOVE_PEER, replica2_idx, RaftPeerPB::UNKNOWN_MEMBER_TYPE,
                      /*replace=*/ false, /*promote=*/ false } });
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "not safe to modify the VOTER status of "
                                   "more than one peer at a time");

  // 11) Reject no-ops.
  s = bulk_change({ { MODIFY_PEER, replica1_idx, RaftPeerPB::VOTER,
                      /*replace=*/ false, /*promote=*/ false } });
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "must modify a field when calling MODIFY_PEER");

  // 12) Reject empty bulk change config operations.
  s = bulk_change({ });
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "requested configuration change does not "
                                   "actually modify the config");

  // 13) Reject multiple changes to the same peer in a single request.
  s = bulk_change({ { MODIFY_PEER, replica1_idx, RaftPeerPB::VOTER,
                      /*replace=*/ true, /*promote=*/ false },
                    { MODIFY_PEER, replica1_idx, RaftPeerPB::VOTER,
                      /*replace=*/ false, /*promote=*/ true } });
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(), "only one change allowed per peer");
}

} // namespace kudu
