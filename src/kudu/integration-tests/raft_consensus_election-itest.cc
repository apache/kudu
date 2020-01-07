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
#include <initializer_list>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
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

METRIC_DECLARE_entity(server);
METRIC_DECLARE_gauge_int32(num_raft_leaders);

using kudu::cluster::ExternalTabletServer;
using kudu::consensus::COMMITTED_OPID;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::MakeOpId;
using kudu::consensus::OpId;
using kudu::consensus::RaftPeerPB;
using kudu::itest::AddServer;
using kudu::itest::GetConsensusState;
using kudu::itest::GetInt64Metric;
using kudu::itest::GetLastOpIdForReplica;
using kudu::itest::GetReplicaStatusAndCheckIfLeader;
using kudu::itest::LeaderStepDown;
using kudu::itest::RemoveServer;
using kudu::itest::RequestVote;
using kudu::itest::StartElection;
using kudu::itest::TServerDetails;
using kudu::itest::TabletServerMap;
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
  void DoTestChurnyElections(TestWorkload* workload);
};

void RaftConsensusElectionITest::CreateClusterForChurnyElectionsTests(
    const vector<string>& extra_ts_flags) {
  vector<string> ts_flags;

#ifdef THREAD_SANITIZER
  // On TSAN builds, we need to be a little bit less churny in order to make
  // any progress at all.
  ts_flags.push_back("--raft_heartbeat_interval_ms=5");
  ts_flags.emplace_back("--inject_latency_ms_before_starting_txn=100");
#else
  ts_flags.emplace_back("--raft_heartbeat_interval_ms=2");
  ts_flags.emplace_back("--inject_latency_ms_before_starting_txn=1000");
#endif

  ts_flags.insert(ts_flags.end(), extra_ts_flags.cbegin(), extra_ts_flags.cend());

  NO_FATALS(CreateCluster("raft_consensus-itest-cluster", std::move(ts_flags)));
}

void RaftConsensusElectionITest::DoTestChurnyElections(TestWorkload* workload) {
  const int max_rows_to_insert = AllowSlowTests() ? 10000 : 1000;
  const MonoDelta timeout = AllowSlowTests() ? MonoDelta::FromSeconds(120)
                                             : MonoDelta::FromSeconds(60);
  workload->set_num_replicas(FLAGS_num_replicas);
  // Set a really high write timeout so that even in the presence of many failures we
  // can verify an exact number of rows in the end, thanks to exactly once semantics.
  workload->set_write_timeout_millis(timeout.ToMilliseconds());
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
  NO_FATALS(CreateClusterForChurnyElectionsTests({}));
  TestWorkload workload(cluster_.get());
  workload.set_write_batch_size(1);
  workload.set_num_read_threads(2);
  NO_FATALS(DoTestChurnyElections(&workload));
}

// The same test, except inject artificial latency when propagating notifications
// from the queue back to consensus. This previously reproduced bugs like KUDU-1078 which
// normally only appear under high load.
TEST_F(RaftConsensusElectionITest, ChurnyElectionsWithNotificationLatency) {
  NO_FATALS(CreateClusterForChurnyElectionsTests(
      {"--consensus_inject_latency_ms_in_notifications=50"}));
  TestWorkload workload(cluster_.get());
  workload.set_write_batch_size(1);
  workload.set_num_read_threads(2);
  NO_FATALS(DoTestChurnyElections(&workload));
}

// The same as TestChurnyElections except insert many duplicated rows.
// This emulates cases where there are many duplicate keys which, due to two phase
// locking, may cause deadlocks and other anomalies that cannot be observed when
// keys are unique.
TEST_F(RaftConsensusElectionITest, ChurnyElectionsWithDuplicateKeys) {
  NO_FATALS(CreateClusterForChurnyElectionsTests({}));
  TestWorkload workload(cluster_.get());
  workload.set_write_pattern(TestWorkload::INSERT_WITH_MANY_DUP_KEYS);
  // Increase the number of rows per batch to get a higher chance of key collision.
  workload.set_write_batch_size(3);
  NO_FATALS(DoTestChurnyElections(&workload));
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
  const auto kTimeout = MonoDelta::FromSeconds(10);
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
  const auto* ts = tservers[0];
  Status s = GetReplicaStatusAndCheckIfLeader(ts, tablet_id_, kTimeout);
  ASSERT_TRUE(s.IsIllegalState()) << "TS #0 should not be leader yet: " << s.ToString();

  // Become leader.
  ASSERT_OK(StartElection(ts, tablet_id_, kTimeout));
  ASSERT_OK(WaitUntilLeader(ts, tablet_id_, kTimeout));
  ASSERT_OK(WriteSimpleTestRow(ts, tablet_id_, RowOperationsPB::INSERT,
                               kTestRowKey, kTestRowIntVal, "foo", kTimeout));
  ASSERT_OK(WaitForServersToAgree(kTimeout, tablet_servers_, tablet_id_, 2));

  // Get the Raft term from the newly established leader.
  ConsensusStatePB cstate_before;
  ASSERT_OK(GetConsensusState(ts, tablet_id_, kTimeout,
                              consensus::EXCLUDE_HEALTH_REPORT, &cstate_before));

  // Step down and test that a 2nd stepdown returns the expected result.
  ASSERT_OK(LeaderStepDown(ts, tablet_id_, kTimeout));

  // Get the Raft term from the leader that has just stepped down.
  ConsensusStatePB cstate_after;
  ASSERT_OK(GetConsensusState(ts, tablet_id_, kTimeout,
                              consensus::EXCLUDE_HEALTH_REPORT, &cstate_after));
  // The stepped-down leader should increment its run-time Raft term.
  EXPECT_GT(cstate_after.current_term(), cstate_before.current_term());

  TabletServerErrorPB error;
  s = LeaderStepDown(ts, tablet_id_, kTimeout, &error);
  ASSERT_TRUE(s.IsIllegalState()) << "TS #0 should not be leader anymore: " << s.ToString();
  ASSERT_EQ(TabletServerErrorPB::NOT_THE_LEADER, error.code()) << SecureShortDebugString(error);

  s = WriteSimpleTestRow(ts, tablet_id_, RowOperationsPB::INSERT,
                         kTestRowKey, kTestRowIntVal, "foo", kTimeout);
  ASSERT_TRUE(s.IsIllegalState()) << "TS #0 should not accept writes as follower: "
                                  << s.ToString();
}

class RaftConsensusNumLeadersMetricTest : public RaftConsensusElectionITest,
                                          public testing::WithParamInterface<int> {};

TEST_P(RaftConsensusNumLeadersMetricTest, TestNumLeadersMetric) {
  const int kNumTablets = 10;
  FLAGS_num_tablet_servers = GetParam();
  // We'll trigger elections manually, so turn off leader failure detection.
  const vector<string> kTsFlags = {
    "--enable_leader_failure_detection=false"
  };
  const vector<string> kMasterFlags = {
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false"
  };
  NO_FATALS(BuildAndStart(kTsFlags, kMasterFlags, /*location_info*/{}, /*create_table*/false));

  // Create some tablet replias.
  TestWorkload workload(cluster_.get());
  workload.set_num_tablets(kNumTablets);
  workload.set_num_replicas(FLAGS_num_tablet_servers);
  workload.Setup();

  const auto kTimeout = MonoDelta::FromSeconds(10);
  const auto* ts = tablet_servers_.begin()->second;
  vector<string> tablet_ids;
  ASSERT_EVENTUALLY([&] {
    vector<string> tablets;
    for (const auto& ts_iter : tablet_servers_) {
      ASSERT_OK(ListRunningTabletIds(ts_iter.second, kTimeout, &tablets));
      ASSERT_EQ(kNumTablets, tablets.size());
    }
    tablet_ids = std::move(tablets);
  });

  // Do a sanity check that there are no leaders yet.
  for (const auto& id : tablet_ids) {
    Status s = GetReplicaStatusAndCheckIfLeader(ts, id, kTimeout);
    ASSERT_TRUE(s.IsIllegalState()) << "TS #0 should not be leader yet: " << s.ToString();
  }
  const auto get_num_leaders_metric = [&] (int64_t* num_leaders) {
    return GetInt64Metric(cluster_->tablet_server_by_uuid(ts->uuid())->bound_http_hostport(),
                          &METRIC_ENTITY_server, nullptr, &METRIC_num_raft_leaders, "value",
                          num_leaders);
  };
  int64_t num_leaders_metric;
  ASSERT_OK(get_num_leaders_metric(&num_leaders_metric));
  ASSERT_EQ(0, num_leaders_metric);

  // Begin triggering elections and ensure we get the correct values for the
  // metric.
  int num_leaders_expected = 0;
  for (const auto& id : tablet_ids) {
    ASSERT_OK(StartElection(ts, id, kTimeout));
    ASSERT_OK(WaitUntilLeader(ts, id, kTimeout));
    ASSERT_OK(get_num_leaders_metric(&num_leaders_metric));
    ASSERT_EQ(++num_leaders_expected, num_leaders_metric);
  }

  // Delete half of the leaders and ensure that the metric goes down.
  int idx = 0;
  int halfway_idx = tablet_ids.size() / 2;
  for (; idx < halfway_idx; idx++) {
    ASSERT_OK(DeleteTablet(ts, tablet_ids[idx], tablet::TABLET_DATA_TOMBSTONED, kTimeout));
    ASSERT_OK(get_num_leaders_metric(&num_leaders_metric));
    ASSERT_EQ(--num_leaders_expected, num_leaders_metric);
  }

  // Renounce leadership on the rest.
  for (; idx < tablet_ids.size(); idx++) {
    ASSERT_OK(LeaderStepDown(ts, tablet_ids[idx], kTimeout));
    ASSERT_OK(get_num_leaders_metric(&num_leaders_metric));
    ASSERT_EQ(--num_leaders_expected, num_leaders_metric);

    // Also delete them and ensure that they don't affect the metric, since
    // they're already non-leaders.
    ASSERT_OK(DeleteTablet(ts, tablet_ids[idx], tablet::TABLET_DATA_TOMBSTONED, kTimeout));
    ASSERT_OK(get_num_leaders_metric(&num_leaders_metric));
    ASSERT_EQ(num_leaders_expected, num_leaders_metric);
  }
}

INSTANTIATE_TEST_CASE_P(NumReplicas, RaftConsensusNumLeadersMetricTest, ::testing::Values(1, 3));

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
// This test is relevant only for 3-2-3 replica management scheme when the
// replica with tombstoned tablet is added back into the tablet as a voter
// (that's why the flag "--raft_prepare_replacement_before_eviction=false"
//  is added). In case of 3-4-3 replica management scheme the newly added
// replica is a non-voter and it's irrelevant whether it can or cannot vote.
TEST_F(RaftConsensusElectionITest, TombstonedVoteAfterFailedTabletCopy) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  const vector<string> kMasterFlags = {
    "--master_add_server_when_underreplicated=false",
    "--raft_prepare_replacement_before_eviction=false",
  };
  vector<string> ts_flags {
    "--raft_prepare_replacement_before_eviction=false",
  };
  AddFlagsForLogRolls(&ts_flags); // For CauseFollowerToFallBehindLogGC().
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
  ASSERT_OK(AddServer(leader_ts, tablet_id_, follower_ts, RaftPeerPB::VOTER, kTimeout));
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

// A test scenario to verify that a disruptive server doesn't start needless
// elections in case if it takes a long time to replicate Raft transactions
// from leader to follower replicas (e.g., due to slowness in WAL IO ops).
TEST_F(RaftConsensusElectionITest, DisruptiveServerAndSlowWAL) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  // Shorten the heartbeat interval for faster test run time.
  const auto kHeartbeatIntervalMs = 200;
  const auto kMaxMissedHeartbeatPeriods = 3;
  const vector<string> ts_flags {
    Substitute("--raft_heartbeat_interval_ms=$0", kHeartbeatIntervalMs),
    Substitute("--leader_failure_max_missed_heartbeat_periods=$0",
               kMaxMissedHeartbeatPeriods),
  };
  NO_FATALS(BuildAndStart(ts_flags));

  // Sanity check: this scenario assumes there are 3 tablet servers. The test
  // table is created with RF=FLAGS_num_replicas.
  ASSERT_EQ(3, FLAGS_num_replicas);
  ASSERT_EQ(3, tablet_servers_.size());

  // A convenience array to access each tablet server as TServerDetails.
  vector<TServerDetails*> tservers;
  AppendValuesFromMap(tablet_servers_, &tservers);
  ASSERT_EQ(cluster_->num_tablet_servers(), tservers.size());

  // The leadership might fluctuate before shutting down the third tablet
  // server, so ASSERT_EVENTUALLY() below is for those rare cases.
  //
  // However, after one of the tablet servers is shutdown, the leadership should
  // not fluctuate because:
  //   1) only two voters out of three are alive
  //   2) current leader should not be disturbed by any rogue votes -- that's
  //      the whole premise of this test scenario
  //
  // So, for this scenario the leadership can fluctuate only if significantly
  // delaying or dropping Raft heartbeats sent from leader to follower replicas.
  // However, minicluster components send heartbeats via the loopback interface,
  // so no real network layer that might significantly delay heartbeats
  // is involved. Also, the consensus RPC queues should not overflow
  // in this scenario because the number of consensus RPCs is relatively low.
  TServerDetails* leader_tserver = nullptr;
  TServerDetails* other_tserver = nullptr;
  TServerDetails* shutdown_tserver = nullptr;
  ASSERT_EVENTUALLY([&] {
    // This is a clean-up in case of retry.
    if (shutdown_tserver != nullptr) {
      auto* ts = cluster_->tablet_server_by_uuid(shutdown_tserver->uuid());
      if (ts->IsShutdown()) {
        ASSERT_OK(ts->Restart());
      }
    }
    for (auto idx = 0; idx < cluster_->num_tablet_servers(); ++idx) {
      auto* ts = cluster_->tablet_server(idx);
      ASSERT_OK(cluster_->SetFlag(ts, "log_inject_latency", "false"));
    }
    leader_tserver = nullptr;
    other_tserver = nullptr;
    shutdown_tserver = nullptr;

    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader_tserver));
    ASSERT_OK(WriteSimpleTestRow(leader_tserver, tablet_id_,
                                 RowOperationsPB::UPSERT, 0, 0, "", kTimeout));
    OpId op_id;
    ASSERT_OK(GetLastOpIdForReplica(tablet_id_, leader_tserver,
                                    consensus::COMMITTED_OPID, kTimeout,
                                    &op_id));
    ASSERT_OK(WaitForServersToAgree(kTimeout, tablet_servers_, tablet_id_,
                                    op_id.index()));
    // Shutdown one tablet server that doesn't host the leader replica of the
    // target tablet and inject WAL latency to others.
    for (const auto& server : tservers) {
      auto* ts = cluster_->tablet_server_by_uuid(server->uuid());
      if (server->uuid() != leader_tserver->uuid() && shutdown_tserver == nullptr) {
        shutdown_tserver = server;
        continue;
      }
      if (server->uuid() != leader_tserver->uuid() && other_tserver == nullptr) {
        other_tserver = server;
      }
      // For this scenario it's important to reserve some inactivity intervals
      // for the follower between processing Raft messages from the leader.
      // If a vote request arrives while replica is busy with processing
      // Raft message from leader, it is rejected with 'busy' status before
      // evaluating the vote withholding interval.
      const auto mult = (server->uuid() == leader_tserver->uuid()) ? 2 : 1;
      const auto latency_ms = mult *
          kHeartbeatIntervalMs * kMaxMissedHeartbeatPeriods;
      ASSERT_OK(cluster_->SetFlag(ts, "log_inject_latency_ms_mean",
                                  std::to_string(latency_ms)));
      ASSERT_OK(cluster_->SetFlag(ts, "log_inject_latency_ms_stddev", "0"));
      ASSERT_OK(cluster_->SetFlag(ts, "log_inject_latency", "true"));
    }

    // Shutdown the third tablet server.
    cluster_->tablet_server_by_uuid(shutdown_tserver->uuid())->Shutdown();

    // Sanity check: make sure the leadership hasn't changed since the leader
    // has been determined.
    TServerDetails* current_leader;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &current_leader));
    ASSERT_EQ(cluster_->tablet_server_index_by_uuid(leader_tserver->uuid()),
              cluster_->tablet_server_index_by_uuid(current_leader->uuid()));
  });

  // Get the Raft term from the established leader.
  ConsensusStatePB cstate;
  ASSERT_OK(GetConsensusState(leader_tserver, tablet_id_, kTimeout,
                              consensus::EXCLUDE_HEALTH_REPORT, &cstate));

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.set_timeout_allowed(true);
  workload.set_network_error_allowed(true);
  workload.set_already_present_allowed(true);
  workload.set_num_read_threads(0);
  workload.set_num_write_threads(1);
  workload.set_write_batch_size(1);
  // Make a 'space' for the artificial vote requests (see below) to arrive
  // while there is no activity on RaftConsensus::Update().
  workload.set_write_interval_millis(kHeartbeatIntervalMs);
  workload.Setup();
  workload.Start();

  // Issue rogue vote requests, imitating a disruptive tablet replica.
  const auto& shutdown_server_uuid = shutdown_tserver->uuid();
  const auto next_term = cstate.current_term() + 1;
  const auto targets = { leader_tserver, other_tserver };
  for (auto i = 0; i < 100; ++i) {
    SleepFor(MonoDelta::FromMilliseconds(kHeartbeatIntervalMs / 4));
    for (const auto* ts : targets) {
      auto s = RequestVote(ts, tablet_id_, shutdown_server_uuid,
                           next_term, MakeOpId(next_term + i, 0),
                           /*ignore_live_leader=*/ false,
                           /*is_pre_election=*/ false,
                           kTimeout);
      // Neither leader nor follower replica should grant 'yes' vote
      // since the healthy leader is there and doing well, sending Raft
      // transactions to be replicated.
      ASSERT_TRUE(s.IsInvalidArgument() || s.IsServiceUnavailable())
          << s.ToString();
      ASSERT_STR_MATCHES(s.ToString(),
          "("
              "because replica is either leader or "
              "believes a valid leader to be alive"
          "|"
              "because replica is already servicing an update "
              "from a current leader or another vote"
          ")");
    }
  }
}

}  // namespace tserver
}  // namespace kudu

