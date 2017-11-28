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
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/raft_consensus-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int64(client_inserts_per_thread);
DECLARE_int64(client_num_batches_per_thread);
DECLARE_int32(consensus_rpc_timeout_ms);
DECLARE_int32(num_client_threads);
DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);

METRIC_DECLARE_entity(tablet);
METRIC_DECLARE_counter(transaction_memory_pressure_rejections);
METRIC_DECLARE_gauge_int64(raft_term);

using kudu::cluster::ExternalTabletServer;
using kudu::consensus::RaftPeerPB;
using kudu::itest::AddServer;
using kudu::itest::GetReplicaStatusAndCheckIfLeader;
using kudu::itest::LeaderStepDown;
using kudu::itest::RemoveServer;
using kudu::itest::StartElection;
using kudu::itest::TabletServerMap;
using kudu::itest::TServerDetails;
using kudu::itest::WaitUntilLeader;
using kudu::itest::WriteSimpleTestRow;
using kudu::pb_util::SecureShortDebugString;
using kudu::tablet::TABLET_DATA_COPYING;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using std::string;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

static const int kTestRowKey = 1234;
static const int kTestRowIntVal = 5678;

class RaftConsensusElectionITest : public RaftConsensusITestBase {
 protected:
  void CreateClusterForChurnyElectionsTests(const vector<string>& extra_ts_flags);
  void DoTestChurnyElections(TestWorkload* workload, int max_rows_to_insert);
};

void RaftConsensusElectionITest::CreateClusterForChurnyElectionsTests(
    const vector<string>& extra_ts_flags) {
  vector<string> ts_flags;

#ifdef THREAD_SANITIZER
  // On TSAN builds, we need to be a little bit less churny in order to make
  // any progress at all.
  ts_flags.push_back("--raft_heartbeat_interval_ms=5");
#else
  ts_flags.emplace_back("--raft_heartbeat_interval_ms=1");
#endif

  ts_flags.insert(ts_flags.end(), extra_ts_flags.cbegin(), extra_ts_flags.cend());

  CreateCluster("raft_consensus-itest-cluster", ts_flags, {});
}

void RaftConsensusElectionITest::DoTestChurnyElections(TestWorkload* workload,
                                                       int max_rows_to_insert) {
  workload->set_num_replicas(FLAGS_num_replicas);
  // Set a really high write timeout so that even in the presence of many failures we
  // can verify an exact number of rows in the end, thanks to exactly once semantics.
  workload->set_write_timeout_millis(60 * 1000 /* 60 seconds */);
  workload->set_num_write_threads(2);
  workload->set_write_batch_size(1);
  workload->Setup();
  workload->Start();

  // Run for either a prescribed number of writes, or 30 seconds,
  // whichever comes first. This prevents test timeouts on slower
  // build machines, TSAN builds, etc.
  Stopwatch sw;
  sw.start();
  while (workload->rows_inserted() < max_rows_to_insert &&
      sw.elapsed().wall_seconds() < 30) {
    SleepFor(MonoDelta::FromMilliseconds(10));
        NO_FATALS(AssertNoTabletServersCrashed());
  }
  workload->StopAndJoin();
  ASSERT_GT(workload->rows_inserted(), 0) << "No rows inserted";

  // Ensure that the replicas converge.
  // We expect an exact result due to exactly once semantics and snapshot scans.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload->table_name(),
                            ClusterVerifier::EXACTLY,
                            workload->rows_inserted()));
  NO_FATALS(AssertNoTabletServersCrashed());
}

TEST_F(RaftConsensusElectionITest, RunLeaderElection) {
  // Reset consensus RPC timeout to the default value, otherwise the election
  // might fail often, making the test flaky.
  FLAGS_consensus_rpc_timeout_ms = 1000;
  NO_FATALS(BuildAndStart());

  int num_iters = AllowSlowTests() ? 10 : 1;

  InsertTestRowsRemoteThread(0,
                             FLAGS_client_inserts_per_thread * num_iters,
                             FLAGS_client_num_batches_per_thread,
                             {});

  NO_FATALS(AssertAllReplicasAgree(FLAGS_client_inserts_per_thread * num_iters));

  // Select the last follower to be new leader.
  vector<TServerDetails*> followers;
  GetOnlyLiveFollowerReplicas(tablet_id_, &followers);

  // Now shutdown the current leader.
  TServerDetails* leader = DCHECK_NOTNULL(GetLeaderReplicaOrNull(tablet_id_));
  ExternalTabletServer* leader_ets = cluster_->tablet_server_by_uuid(leader->uuid());
  leader_ets->Shutdown();

  TServerDetails* replica = followers.back();
  CHECK_NE(leader->instance_id.permanent_uuid(), replica->instance_id.permanent_uuid());

  // Make the new replica leader.
  ASSERT_OK(StartElection(replica, tablet_id_, MonoDelta::FromSeconds(10)));

  // Insert a bunch more rows.
  InsertTestRowsRemoteThread(FLAGS_client_inserts_per_thread * num_iters,
                             FLAGS_client_inserts_per_thread * num_iters,
                             FLAGS_client_num_batches_per_thread,
                             {});

  // Restart the original replica and make sure they all agree.
  ASSERT_OK(leader_ets->Restart());

  NO_FATALS(AssertAllReplicasAgree(FLAGS_client_inserts_per_thread * num_iters * 2));
}

// This test sets all of the election timers to be very short, resulting
// in a lot of churn. We expect to make some progress and not diverge or
// crash, despite the frequent re-elections and races.
TEST_F(RaftConsensusElectionITest, ChurnyElections) {
  const int kNumWrites = AllowSlowTests() ? 10000 : 1000;
  CreateClusterForChurnyElectionsTests({});
  TestWorkload workload(cluster_.get());
  workload.set_write_batch_size(1);
  workload.set_num_read_threads(2);
  DoTestChurnyElections(&workload, kNumWrites);
}

// The same test, except inject artificial latency when propagating notifications
// from the queue back to consensus. This previously reproduced bugs like KUDU-1078 which
// normally only appear under high load.
TEST_F(RaftConsensusElectionITest, ChurnyElections_WithNotificationLatency) {
  CreateClusterForChurnyElectionsTests({"--consensus_inject_latency_ms_in_notifications=50"});
  const int kNumWrites = AllowSlowTests() ? 10000 : 1000;
  TestWorkload workload(cluster_.get());
  workload.set_write_batch_size(1);
  workload.set_num_read_threads(2);
  DoTestChurnyElections(&workload, kNumWrites);
}

// The same as TestChurnyElections except insert many duplicated rows.
// This emulates cases where there are many duplicate keys which, due to two phase
// locking, may cause deadlocks and other anomalies that cannot be observed when
// keys are unique.
TEST_F(RaftConsensusElectionITest, ChurnyElections_WithDuplicateKeys) {
  CreateClusterForChurnyElectionsTests({});
  const int kNumWrites = AllowSlowTests() ? 10000 : 1000;
  TestWorkload workload(cluster_.get());
  workload.set_write_pattern(TestWorkload::INSERT_WITH_MANY_DUP_KEYS);
  // Increase the number of rows per batch to get a higher chance of key collision.
  workload.set_write_batch_size(3);
  DoTestChurnyElections(&workload, kNumWrites);
}

// Test automatic leader election by killing leaders.
TEST_F(RaftConsensusElectionITest, AutomaticLeaderElection) {
  if (AllowSlowTests()) {
    FLAGS_num_tablet_servers = 5;
    FLAGS_num_replicas = 5;
  }
  NO_FATALS(BuildAndStart());

  TServerDetails* leader;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));

  unordered_set<TServerDetails*> killed_leaders;

  const int kNumLeadersToKill = FLAGS_num_replicas / 2;
  const int kFinalNumReplicas = FLAGS_num_replicas / 2 + 1;

  for (int leaders_killed = 0; leaders_killed < kFinalNumReplicas; leaders_killed++) {
    LOG(INFO) << Substitute("Writing data to leader of $0-node config ($1 alive)...",
                            FLAGS_num_replicas, FLAGS_num_replicas - leaders_killed);

    InsertTestRowsRemoteThread(leaders_killed * FLAGS_client_inserts_per_thread,
                               FLAGS_client_inserts_per_thread,
                               FLAGS_client_num_batches_per_thread,
                               {});

    // At this point, the writes are flushed but the commit index may not be
    // propagated to all replicas. We kill the leader anyway.
    if (leaders_killed < kNumLeadersToKill) {
      LOG(INFO) << "Killing current leader " << leader->instance_id.permanent_uuid() << "...";
      cluster_->tablet_server_by_uuid(leader->uuid())->Shutdown();
      InsertOrDie(&killed_leaders, leader);

      LOG(INFO) << "Waiting for new guy to be elected leader.";
      ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
    }
  }

  // Restart every node that was killed, and wait for the nodes to converge
  for (TServerDetails* killed_node : killed_leaders) {
    CHECK_OK(cluster_->tablet_server_by_uuid(killed_node->uuid())->Restart());
  }
  // Verify the data on the remaining replicas.
  NO_FATALS(AssertAllReplicasAgree(FLAGS_client_inserts_per_thread * kFinalNumReplicas));
}

// Single-replica leader election test.
TEST_F(RaftConsensusElectionITest, AutomaticLeaderElectionOneReplica) {
  FLAGS_num_tablet_servers = 1;
  FLAGS_num_replicas = 1;
  NO_FATALS(BuildAndStart());
  // Ensure that single-node Raft configs elect themselves as leader
  // immediately upon Consensus startup.
  ASSERT_OK(GetReplicaStatusAndCheckIfLeader(tablet_servers_[cluster_->tablet_server(0)->uuid()],
                                             tablet_id_, MonoDelta::FromMilliseconds(500)));
}

TEST_F(RaftConsensusElectionITest, LeaderStepDown) {
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false"
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false"
  };

  FLAGS_num_replicas = 3;
  FLAGS_num_tablet_servers = 3;
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);

  // Start with no leader.
  Status s = GetReplicaStatusAndCheckIfLeader(tservers[0], tablet_id_, MonoDelta::FromSeconds(10));
  ASSERT_TRUE(s.IsIllegalState()) << "TS #0 should not be leader yet: " << s.ToString();

  // Become leader.
  ASSERT_OK(StartElection(tservers[0], tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitUntilLeader(tservers[0], tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WriteSimpleTestRow(tservers[0], tablet_id_, RowOperationsPB::INSERT,
                               kTestRowKey, kTestRowIntVal, "foo", MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10), tablet_servers_, tablet_id_, 2));

  // Step down and test that a 2nd stepdown returns the expected result.
  ASSERT_OK(LeaderStepDown(tservers[0], tablet_id_, MonoDelta::FromSeconds(10)));
  TabletServerErrorPB error;
  s = LeaderStepDown(tservers[0], tablet_id_, MonoDelta::FromSeconds(10), &error);
  ASSERT_TRUE(s.IsIllegalState()) << "TS #0 should not be leader anymore: " << s.ToString();
  ASSERT_EQ(TabletServerErrorPB::NOT_THE_LEADER, error.code()) << SecureShortDebugString(error);

  s = WriteSimpleTestRow(tservers[0], tablet_id_, RowOperationsPB::INSERT,
                         kTestRowKey, kTestRowIntVal, "foo", MonoDelta::FromSeconds(10));
  ASSERT_TRUE(s.IsIllegalState()) << "TS #0 should not accept writes as follower: "
                                  << s.ToString();
}

// Test for KUDU-699: sets the consensus RPC timeout to be long,
// and freezes both followers before asking the leader to step down.
// Prior to fixing KUDU-699, the step-down process would block
// until the pending requests timed out.
TEST_F(RaftConsensusElectionITest, StepDownWithSlowFollower) {
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
    // Bump up the RPC timeout, so that we can verify that the stepdown responds
    // quickly even when an outbound request is hung.
    "--consensus_rpc_timeout_ms=15000"
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false"
  };

  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_OK(StartElection(tservers[0], tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitUntilLeader(tservers[0], tablet_id_, MonoDelta::FromSeconds(10)));

  // Stop both followers.
  for (int i = 1; i < 3; i++) {
    ASSERT_OK(cluster_->tablet_server_by_uuid(tservers[i]->uuid())->Pause());
  }

  // Sleep a little bit of time to make sure that the leader has outstanding heartbeats
  // to the paused followers before requesting the stepdown.
  SleepFor(MonoDelta::FromSeconds(1));

  // Step down should respond quickly despite the hung requests.
  ASSERT_OK(LeaderStepDown(tservers[0], tablet_id_, MonoDelta::FromSeconds(3)));
}

// Ensure that we can elect a server that is in the "pending" configuration.
// This is required by the Raft protocol. See Diego Ongaro's PhD thesis, section
// 4.1, where it states that "it is the caller’s configuration that is used in
// reaching consensus, both for voting and for log replication".
//
// This test also tests the case where a node comes back from the dead to a
// leader that was not in its configuration when it died. That should also work, i.e.
// the revived node should accept writes from the new leader.
TEST_F(RaftConsensusElectionITest, ElectPendingVoter) {
  // Test plan:
  //  1. Disable failure detection to avoid non-deterministic behavior.
  //  2. Start with a configuration size of 5, all servers synced.
  //  3. Remove one server from the configuration, wait until committed.
  //  4. Pause the 3 remaining non-leaders (SIGSTOP).
  //  5. Run a config change to add back the previously-removed server.
  //     Ensure that, while the op cannot be committed yet due to lack of a
  //     majority in the new config (only 2 out of 5 servers are alive), the op
  //     has been replicated to both the local leader and the new member.
  //  6. Force the existing leader to step down.
  //  7. Resume one of the paused nodes so that a majority (of the 5-node
  //     configuration, but not the original 4-node configuration) will be available.
  //  8. Start a leader election on the new (pending) node. It should win.
  //  9. Unpause the two remaining stopped nodes.
  // 10. Wait for all nodes to sync to the new leader's log.
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false",
  };

  FLAGS_num_tablet_servers = 5;
  FLAGS_num_replicas = 5;
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags));

  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(FLAGS_num_tablet_servers, tservers.size());

  // Elect server 0 as leader and wait for log index 1 to propagate to all servers.
  TServerDetails* initial_leader = tservers[0];
  ASSERT_OK(StartElection(initial_leader, tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitUntilCommittedOpIdIndexIs(1, initial_leader, tablet_id_,
                                          MonoDelta::FromSeconds(10)));

  // The server we will remove and then bring back.
  TServerDetails* final_leader = tservers[4];

  // Kill the master, so we can change the config without interference.
  cluster_->master()->Shutdown();

  // Now remove server 4 from the configuration.
  TabletServerMap active_tablet_servers = tablet_servers_;
  LOG(INFO) << "Removing tserver with uuid " << final_leader->uuid();
  ASSERT_OK(RemoveServer(initial_leader, tablet_id_, final_leader,
                         MonoDelta::FromSeconds(10)));
  ASSERT_EQ(1, active_tablet_servers.erase(final_leader->uuid()));
  int64_t cur_log_index = 2;
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10),
                                  active_tablet_servers, tablet_id_, cur_log_index));

  // Pause tablet servers 1 through 3, so they won't see the operation to add
  // server 4 back.
  LOG(INFO) << "Pausing 3 replicas...";
  for (int i = 1; i <= 3; i++) {
    ExternalTabletServer* replica_ts = cluster_->tablet_server_by_uuid(tservers[i]->uuid());
    ASSERT_OK(replica_ts->Pause());
  }

  // Now add server 4 back to the peers.
  // This operation will time out on the client side.
  LOG(INFO) << "Adding back Peer " << final_leader->uuid() << " and expecting timeout...";
  Status s = AddServer(initial_leader, tablet_id_, final_leader, RaftPeerPB::VOTER,
                       MonoDelta::FromMilliseconds(100));
  ASSERT_TRUE(s.IsTimedOut()) << "Expected AddServer() to time out. Result: " << s.ToString();
  LOG(INFO) << "Timeout achieved.";
  active_tablet_servers = tablet_servers_; // Reset to the unpaused servers.
  for (int i = 1; i <= 3; i++) {
    ASSERT_EQ(1, active_tablet_servers.erase(tservers[i]->uuid()));
  }
  // Only wait for TS 0 and 4 to agree that the new change config op has been
  // replicated.
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10),
                                  active_tablet_servers, tablet_id_, ++cur_log_index));

  // Now that TS 4 is electable (and pending), have TS 0 step down.
  LOG(INFO) << "Forcing Peer " << initial_leader->uuid() << " to step down...";
  ASSERT_OK(LeaderStepDown(initial_leader, tablet_id_, MonoDelta::FromSeconds(10)));

  // Resume TS 1 so we have a majority of 3 to elect a new leader.
  LOG(INFO) << "Resuming Peer " << tservers[1]->uuid() << " ...";
  ASSERT_OK(cluster_->tablet_server_by_uuid(tservers[1]->uuid())->Resume());
  InsertOrDie(&active_tablet_servers, tservers[1]->uuid(), tservers[1]);

  // Now try to get TS 4 elected. It should succeed and push a NO_OP.
  LOG(INFO) << "Trying to elect Peer " << tservers[4]->uuid() << " ...";
  ASSERT_OK(StartElection(final_leader, tablet_id_, MonoDelta::FromSeconds(10)));
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10),
                                  active_tablet_servers, tablet_id_, ++cur_log_index));

  // Resume the remaining paused nodes.
  LOG(INFO) << "Resuming remaining nodes...";
  ASSERT_OK(cluster_->tablet_server_by_uuid(tservers[2]->uuid())->Resume());
  ASSERT_OK(cluster_->tablet_server_by_uuid(tservers[3]->uuid())->Resume());
  active_tablet_servers = tablet_servers_;

  // Do one last operation on the new leader: an insert.
  ASSERT_OK(WriteSimpleTestRow(final_leader, tablet_id_, RowOperationsPB::INSERT,
                               kTestRowKey, kTestRowIntVal, "Ob-La-Di, Ob-La-Da",
                               MonoDelta::FromSeconds(10)));

  // Wait for all servers to replicate everything up through the last write op.
  ASSERT_OK(WaitForServersToAgree(MonoDelta::FromSeconds(10),
                                  active_tablet_servers, tablet_id_, ++cur_log_index));
}

// Have a replica fall behind the leader's log, then fail a tablet copy. It
// should still be able to vote in leader elections.
TEST_F(RaftConsensusElectionITest, TombstonedVoteAfterFailedTabletCopy) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);

  vector<string> ts_flags;
  AddFlagsForLogRolls(&ts_flags); // For CauseFollowerToFallBehindLogGC().
  const vector<string> kMasterFlags = {
    "--master_add_server_when_underreplicated=false",
  };
  NO_FATALS(BuildAndStart(ts_flags, kMasterFlags));

  TabletServerMap active_tablet_servers = tablet_servers_;
  ASSERT_EQ(3, FLAGS_num_replicas);
  ASSERT_EQ(3, active_tablet_servers.size());

  string leader_uuid;
  int64_t orig_term;
  string follower_uuid;
  NO_FATALS(CauseFollowerToFallBehindLogGC(
      active_tablet_servers, &leader_uuid, &orig_term, &follower_uuid));

  // Wait for the abandoned follower to be evicted.
  ASSERT_OK(WaitUntilCommittedConfigNumVotersIs(2, tablet_servers_[leader_uuid],
                                                tablet_id_, kTimeout));
  ASSERT_EQ(1, active_tablet_servers.erase(follower_uuid));

  // Wait for the deleted tablet to be tombstoned, meaning the catalog manager
  // has already sent DeleteTablet request and it has been processed. A race
  // between adding a new server and processing DeleteTablet request is
  // possible: the DeleteTablet request might be sent/processed _after_
  // a new server has been added.
  const int follower_idx =
      cluster_->tablet_server_index_by_uuid(follower_uuid);
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
      follower_idx, tablet_id_, { TABLET_DATA_TOMBSTONED }, kTimeout));

  // Add the evicted follower back to the config and then make it crash because
  // of injected fault during tablet copy.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server_by_uuid(follower_uuid),
            "tablet_copy_fault_crash_on_fetch_all", "1.0"));
  auto* leader_ts = tablet_servers_[leader_uuid];
  auto* follower_ts = tablet_servers_[follower_uuid];
  ASSERT_OK(itest::AddServer(leader_ts, tablet_id_, follower_ts, RaftPeerPB::VOTER,
                             kTimeout));
  ASSERT_OK(cluster_->tablet_server_by_uuid(follower_uuid)->WaitForInjectedCrash(kTimeout));

  // Shut down the rest of the cluster, then only bring back the node we
  // crashed, plus one other node. The tombstoned replica should still be able
  // to vote and the tablet should come online.
  cluster_->Shutdown();

  ASSERT_OK(inspect_->CheckTabletDataStateOnTS(follower_idx, tablet_id_,
                                               { TABLET_DATA_COPYING }));

  ASSERT_OK(cluster_->master()->Restart());
  ASSERT_OK(cluster_->tablet_server_by_uuid(leader_uuid)->Restart());
  ASSERT_OK(cluster_->tablet_server_by_uuid(follower_uuid)->Restart());

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_timeout_allowed(true);
  workload.Setup();
  workload.Start();
  ASSERT_EVENTUALLY([&] {
    ASSERT_GE(workload.rows_inserted(), 100);
  });
  workload.StopAndJoin();
}

}  // namespace tserver
}  // namespace kudu

