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
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/raft_consensus-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tablet_server-test-base.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);

METRIC_DECLARE_gauge_int32(tablet_copy_open_client_sessions);
METRIC_DECLARE_gauge_int32(tablet_copy_open_source_sessions);

using kudu::cluster::ExternalDaemon;
using kudu::cluster::ExternalTabletServer;
using kudu::consensus::RaftPeerPB;
using kudu::itest::AddServer;
using kudu::itest::GetInt64Metric;
using kudu::itest::LeaderStepDown;
using kudu::itest::RemoveServer;
using kudu::itest::StartElection;
using kudu::itest::TServerDetails;
using kudu::itest::TabletServerMap;
using kudu::itest::WAIT_FOR_LEADER;
using kudu::itest::WaitForReplicasReportedToMaster;
using kudu::master::TabletLocationsPB;
using kudu::tablet::TABLET_DATA_COPYING;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

// Integration test for the raft consensus implementation.
// Uses the whole tablet server stack with ExternalMiniCluster.
class RaftConsensusNonVoterITest : public RaftConsensusITestBase {
 public:
  RaftConsensusNonVoterITest() = default;

 protected:
  // Get number of source tablet copy sessions at the specified server.
  Status GetTabletCopySourceSessionsCount(const ExternalDaemon& server,
                                          int64_t* count);

  // Get number of target/client tablet copy sessions at the specified server.
  Status GetTabletCopyTargetSessionsCount(const ExternalDaemon& server,
                                          int64_t* count);

  // Add replica of the specified type for the specified tablet.
  Status AddReplica(const string& tablet_id,
                    const TServerDetails* replica,
                    RaftPeerPB::MemberType replica_type,
                    const MonoDelta& timeout);

  // Remove replica of the specified tablet.
  Status RemoveReplica(const string& tablet_id,
                       const TServerDetails* replica,
                       const MonoDelta& timeout);

  // Change replica membership to the specified type, i.e. promote a replica
  // in case of RaftPeerPB::VOTER member_type, and demote a replica in case of
  // RaftPeerPB::NON_VOTER member_type.
  Status ChangeReplicaMembership(RaftPeerPB::MemberType member_type,
                                 const string& tablet_id,
                                 const TServerDetails* replica,
                                 const MonoDelta& timeout);
};

Status RaftConsensusNonVoterITest::GetTabletCopySourceSessionsCount(
    const ExternalDaemon& server, int64_t* count) {
  return GetInt64Metric(server.bound_http_hostport(),
      &METRIC_ENTITY_server, "kudu.tabletserver",
      &METRIC_tablet_copy_open_source_sessions, "value", count);
}

Status RaftConsensusNonVoterITest::GetTabletCopyTargetSessionsCount(
    const ExternalDaemon& server, int64_t* count) {
  return GetInt64Metric(server.bound_http_hostport(),
      &METRIC_ENTITY_server, "kudu.tabletserver",
      &METRIC_tablet_copy_open_client_sessions, "value", count);
}

Status RaftConsensusNonVoterITest::AddReplica(const string& tablet_id,
                                              const TServerDetails* replica,
                                              RaftPeerPB::MemberType replica_type,
                                              const MonoDelta& timeout) {
  TServerDetails* leader = nullptr;
  RETURN_NOT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));

  // Wait for at least one operation committed by the leader in current term.
  // Otherwise, any Raft configuration change attempt might end up with error
  // 'Illegal state: Leader has not yet committed an operation in its own term'.
  RETURN_NOT_OK(WaitForOpFromCurrentTerm(leader, tablet_id,
                                         consensus::COMMITTED_OPID, timeout));
  return AddServer(leader, tablet_id, replica, replica_type, timeout);
}

Status RaftConsensusNonVoterITest::RemoveReplica(const string& tablet_id,
                                                 const TServerDetails* replica,
                                                 const MonoDelta& timeout) {
  TServerDetails* leader = nullptr;
  RETURN_NOT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));

  // Wait for at least one operation committed by the leader in current term.
  // Otherwise, any Raft configuration change attempt might end up with error
  // 'Illegal state: Leader has not yet committed an operation in its own term'.
  RETURN_NOT_OK(WaitForOpFromCurrentTerm(leader, tablet_id,
                                         consensus::COMMITTED_OPID, timeout));
  return RemoveServer(leader, tablet_id, replica, timeout);
}

Status RaftConsensusNonVoterITest::ChangeReplicaMembership(
    RaftPeerPB::MemberType member_type,
    const string& tablet_id,
    const TServerDetails* replica,
    const MonoDelta& timeout) {
  TServerDetails* leader = nullptr;
  RETURN_NOT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));

  // Wait for at least one operation committed by the leader in current term.
  // Otherwise, any Raft configuration change attempt might end up with error
  // 'Illegal state: Leader has not yet committed an operation in its own term'.
  RETURN_NOT_OK(WaitForOpFromCurrentTerm(leader, tablet_id,
                                         consensus::COMMITTED_OPID, timeout));
  return ChangeReplicaType(leader, tablet_id, replica, member_type, timeout);
}

// Ensure that adding a NON_VOTER replica is properly handled by the system:
//
//   * Updating Raft configuration for tablet by adding a NON_VOTER replica
//     succeeds, no errors reported.
//
//   * After adding a replica, the system should start tablet copying
//     to the newly added replica: both the source and the target copy sessions
//     should be active for some time.
//
//   * By the time the newly added replica changes its state to RUNNING,
//     the tablet copy session should end at both sides.
//
//   * Tablet leader reports about the newly added replica to the master.
//
//   * If the leader steps down, a new one can be elected and it's possible
//     to insert data into the table which contains the tablet.
//
//   * The tablet stays consistent: ksck verification reports no error,
//     replicated operation indices match across all replicas,
//     tablet row count matches the expected number.
//
TEST_F(RaftConsensusNonVoterITest, AddNonVoterReplica) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);
  const vector<string> kMasterFlags = {
    // Allow replication factor of 2.
    "--allow_unsafe_replication_factor=true",
  };
  const vector<string> kTserverFlags = {
    // Slow down tablet copy to observe active source and target sessions.
    "--tablet_copy_download_file_inject_latency_ms=1000",
  };
  const int kOriginalReplicasNum = 2;

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = kOriginalReplicasNum;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));
  ASSERT_EQ(3, tablet_servers_.size());
  ASSERT_EQ(kOriginalReplicasNum, tablet_replicas_.size());

  const string& tablet_id = tablet_id_;
  TabletServerMap replica_servers;
  for (const auto& e : tablet_replicas_) {
    if (e.first == tablet_id) {
      replica_servers.emplace(e.second->uuid(), e.second);
    }
  }
  ASSERT_EQ(FLAGS_num_replicas, replica_servers.size());

  // Create a test table and insert some data into the table,
  // so the special flag --tablet_copy_download_file_inject_latency_ms
  // could take affect while tablet copy happens down the road.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  ASSERT_OK(WaitForServersToAgree(kTimeout, replica_servers, tablet_id, 1));

  TServerDetails* new_replica = nullptr;
  for (const auto& ts : tablet_servers_) {
    if (replica_servers.find(ts.first) == replica_servers.end()) {
      new_replica = ts.second;
      break;
    }
  }
  ASSERT_NE(nullptr, new_replica);

  ASSERT_OK(AddReplica(tablet_id, new_replica, RaftPeerPB::NON_VOTER, kTimeout));

  const int new_replica_idx =
      cluster_->tablet_server_index_by_uuid(new_replica->uuid());
  // Wait for the tablet copying to start.
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
      new_replica_idx, tablet_id, { TABLET_DATA_COPYING }, kTimeout));

  TServerDetails* leader = nullptr;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
  const ExternalDaemon& ed_leader =
      *cluster_->tablet_server_by_uuid(leader->uuid());
  const ExternalDaemon& ed_new_replica =
      *cluster_->tablet_server_by_uuid(new_replica->uuid());
  {
    int64_t num_sessions;
    ASSERT_OK(GetTabletCopySourceSessionsCount(ed_leader, &num_sessions));
    EXPECT_EQ(1, num_sessions);
  }
  {
    int64_t num_sessions;
    ASSERT_OK(GetTabletCopyTargetSessionsCount(ed_new_replica, &num_sessions));
    EXPECT_EQ(1, num_sessions);
  }

  // The newly copied replica should be able to start.
  ASSERT_OK(WaitForNumTabletsOnTS(
      new_replica, 1, kTimeout, nullptr, tablet::RUNNING));

  // The tablet copying should complete shortly after tablet state becomes
  // RUNNING. Sampling the counters right after seeing RUNNING tablet status
  // is a little racy: it takes some time to end tablet copy sessions at both
  // sides. So, using ASSERT_EVENTUALLY here to avoid flakiness.
  ASSERT_EVENTUALLY([&]() {
      int64_t num_sessions;
      ASSERT_OK(GetTabletCopySourceSessionsCount(ed_leader, &num_sessions));
      EXPECT_EQ(0, num_sessions);
      ASSERT_OK(GetTabletCopyTargetSessionsCount(ed_new_replica, &num_sessions));
      EXPECT_EQ(0, num_sessions);
    });

  // The master should report about the newly added NON_VOTER tablet replica
  // to the established leader.
  bool has_leader;
  master::TabletLocationsPB tablet_locations;
  ASSERT_OK(WaitForReplicasReportedToMaster(
      cluster_->master_proxy(), kOriginalReplicasNum + 1, tablet_id, kTimeout,
      WAIT_FOR_LEADER, &has_leader, &tablet_locations));
  ASSERT_TRUE(has_leader);

  // Check the update cluster is able to elect a leader.
  {
    TServerDetails* leader = nullptr;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
    ASSERT_OK(LeaderStepDown(leader, tablet_id, kTimeout));
  }

  // Make sure it's possible to insert more data into the table once it's backed
  // by one more (NON_VOTER) replica.
  const int64_t prev_inserted = workload.rows_inserted();
  workload.Start();
  while (workload.rows_inserted() < 2 * prev_inserted) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  NO_FATALS(cluster_->AssertNoCrashes());
  // Ensure that the replicas converge. Along with other verification steps,
  // ClusterVerifier employs VerifyCommittedOpIdsMatch() to verify that
  // all OpIds match in local files under all tablet servers of the cluster,
  // so NON_VOTER replicas are covered by this check as well.
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(),
                            ClusterVerifier::EXACTLY,
                            workload.rows_inserted()));
}

// Test how the system reacts on removing a NON_VOTER replica from
// tablet cluster:
//
//   * First, add a NON_VOTER member into the cluster (covered by other test).
//
//   * Make sure that changing Raft configuration by removing a NON_VOTER
//     replica does not return errors.
//
//   * After removing such a non-voter replica, the system should not try
//     to add a new replica instead of the removed one.
//
//   * Tablet leader is established and it reports about the removed replica
//     to the master.
//
//   * The updated tablet is still available: it's possible to insert data
//     into the table which is hosted by the tablet.
//
//   * The tablet stays consistent: ksck verification reports no error,
//     replicated operation indices match across all remaining replicas,
//     tablet row count matches the expected number.
//
TEST_F(RaftConsensusNonVoterITest, AddThenRemoveNonVoterReplica) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);
  const vector<string> kMasterFlags = {
    // Allow replication factor of 2.
    "--allow_unsafe_replication_factor=true",
  };
  const int kOriginalReplicasNum = 2;

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = kOriginalReplicasNum;
  NO_FATALS(BuildAndStart({}, kMasterFlags));
  ASSERT_EQ(3, tablet_servers_.size());
  ASSERT_EQ(kOriginalReplicasNum, tablet_replicas_.size());

  const string& tablet_id = tablet_id_;
  TabletServerMap replica_servers;
  for (const auto& e : tablet_replicas_) {
    if (e.first == tablet_id) {
      replica_servers.emplace(e.second->uuid(), e.second);
    }
  }
  ASSERT_EQ(FLAGS_num_replicas, replica_servers.size());

  TServerDetails* new_replica = nullptr;
  for (const auto& ts : tablet_servers_) {
    if (replica_servers.find(ts.first) == replica_servers.end()) {
      new_replica = ts.second;
      break;
    }
  }
  ASSERT_NE(nullptr, new_replica);
  ASSERT_OK(AddReplica(tablet_id, new_replica, RaftPeerPB::NON_VOTER, kTimeout));

  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(kTimeout, replica_servers, tablet_id, 1));

  // The newly copied replica should be able to start.
  ASSERT_OK(WaitForNumTabletsOnTS(
      new_replica, 1, kTimeout, nullptr, tablet::RUNNING));

  // Ensure that nothing crashes and the replicas converge.
  NO_FATALS(cluster_->AssertNoCrashes());
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(),
                            ClusterVerifier::EXACTLY,
                            workload.rows_inserted()));

  // Remove the newly added replica.
  ASSERT_OK(RemoveReplica(tablet_id, new_replica, kTimeout));
  ASSERT_OK(WaitForServersToAgree(kTimeout, replica_servers, tablet_id, 1));

  // Verify the removed replica gets tombstoned.
  const int new_replica_idx =
      cluster_->tablet_server_index_by_uuid(new_replica->uuid());
  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
      new_replica_idx, tablet_id, { TABLET_DATA_TOMBSTONED }, kTimeout));

  // The added and then removed tablet replica should be gone, and the master
  // should report appropriate replica count at this point. The tablet leader
  // should be established.
  bool has_leader;
  master::TabletLocationsPB tablet_locations;
  ASSERT_OK(WaitForReplicasReportedToMaster(
      cluster_->master_proxy(), kOriginalReplicasNum, tablet_id, kTimeout,
      WAIT_FOR_LEADER, &has_leader, &tablet_locations));
  ASSERT_TRUE(has_leader);

  // Make sure it's possible to insert data into the tablet once the NON_VOTER
  // replica is gone.
  const int64_t prev_inserted = workload.rows_inserted();
  workload.Start();
  while (workload.rows_inserted() < 2 * prev_inserted) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // Ensure that nothing crashed and the replicas converge.
  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(),
                            ClusterVerifier::EXACTLY,
                            workload.rows_inserted()));
}

// Test to ensure that a non-voter replica:
//  * does not vote
//  * does not start leader elections
//  * returns an error on RunLeaderElection() RPC call
TEST_F(RaftConsensusNonVoterITest, NonVoterReplicasDoNotVote) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);
  const int kOriginalReplicasNum = 2;
  const int kHbIntervalMs = 64;
  const int kHbLeaderMissedNum = 1;
  const vector<string> kMasterFlags = {
    // Allow replication factor of 2.
    "--allow_unsafe_replication_factor=true",
  };
  const vector<string> kTserverFlags = {
    Substitute("--raft_heartbeat_interval_ms=$0", kHbIntervalMs),
    Substitute("--leader_failure_max_missed_heartbeat_periods=$0",
        kHbLeaderMissedNum),
  };

  FLAGS_num_tablet_servers = 3;
  FLAGS_num_replicas = kOriginalReplicasNum;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));
  ASSERT_EQ(3, tablet_servers_.size());
  ASSERT_EQ(kOriginalReplicasNum, tablet_replicas_.size());

  const string& tablet_id = tablet_id_;
  TabletServerMap replica_servers;
  for (const auto& e : tablet_replicas_) {
    if (e.first == tablet_id) {
      replica_servers.emplace(e.second->uuid(), e.second);
    }
  }
  ASSERT_EQ(FLAGS_num_replicas, replica_servers.size());

  ASSERT_OK(WaitForServersToAgree(kTimeout, replica_servers, tablet_id, 1));

  TServerDetails* new_replica = nullptr;
  for (const auto& ts : tablet_servers_) {
    if (replica_servers.find(ts.first) == replica_servers.end()) {
      new_replica = ts.second;
      break;
    }
  }
  ASSERT_NE(nullptr, new_replica);

  ASSERT_OK(AddReplica(tablet_id, new_replica, RaftPeerPB::NON_VOTER, kTimeout));

  // The newly copied replica should be able to start.
  ASSERT_OK(WaitForNumTabletsOnTS(
      new_replica, 1, kTimeout, nullptr, tablet::RUNNING));

  // Ensure that nothing crashes: all tservers must be alive for next step
  // of the scenario.
  NO_FATALS(cluster_->AssertNoCrashes());

  // Make sure a NON_VOTER replica doesn't vote.
  {
    // Pause the current leader and make sure the majority is not achievable.
    // It would not be the case if the non-voter replica could vote in the
    // election initiated after the failure of the current leader was detected.
    TServerDetails* leader;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
    ExternalTabletServer* leader_ts =
        cluster_->tablet_server_by_uuid(leader->uuid());

    int64_t term_leader;
    ASSERT_OK(GetTermMetricValue(leader_ts, &term_leader));

    ASSERT_OK(leader_ts->Pause());
    auto cleanup = MakeScopedCleanup([&]() {
      ASSERT_OK(leader_ts->Resume());
    });
    TServerDetails* new_leader;
    const Status s = GetLeaderReplicaWithRetries(tablet_id, &new_leader, 10);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_OK(leader_ts->Resume());

    // The majority should be achievable once the leader replica is resumed.
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &new_leader));
    NO_FATALS(cluster_->AssertNoCrashes());
  }

  // Make sure a NON_VOTER replica does not start leader election on start-up.
  {
    // Disable failure detection for all replicas.
    for (int i = 0; i < cluster_->num_tablet_servers(); ++i) {
      ExternalTabletServer* ts = cluster_->tablet_server(i);
      ASSERT_OK(cluster_->SetFlag(ts,
          "enable_leader_failure_detection", "false"));
    }
    ExternalTabletServer* new_replica_ts =
        cluster_->tablet_server_by_uuid(new_replica->uuid());

    // Get the current Raft term for the tablet.
    int64_t term_before_restart = 0;
    ASSERT_OK(GetTermMetricValue(new_replica_ts, &term_before_restart));

    new_replica_ts->Shutdown();
    ASSERT_OK(new_replica_ts->Restart());
    // Wait for the tablet server to start up.
    ASSERT_OK(cluster_->WaitForTabletsRunning(new_replica_ts, 1, kTimeout));

    // Once restarted, the tablet server will have the default disposition
    // for the enable_leader_failure_detection flag, i.e. our new NON_VOTER
    // replica will have leader failure detection enabled. That said,
    // the leader election could trigger if the replica was of VOTER type.
    // However, it's not and no election should be started, and the term
    // must be the same as before starting this NON_VOTER replica.
    // So, give a chance for a new election to happen and compare the terms.
    SleepFor(MonoDelta::FromMilliseconds(
        3L * kHbLeaderMissedNum * kHbIntervalMs));

    int64_t term_after_restart = 0;
    ASSERT_OK(GetTermMetricValue(new_replica_ts, &term_after_restart));
    EXPECT_EQ(term_before_restart, term_after_restart);
  }

  // Make sure a non-voter replica returns an error on RunLeaderElection()
  // RPC call.
  {
    const Status s = StartElection(new_replica, tablet_id_, kTimeout);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  }
}

// Test that it's possible to promote a NON_VOTER replica into a VOTER one
// and demote a VOTER replica into NON_VOTER (if not a leader). The new VOTER
// replica should be able to participate in elections, and the quorum should
// change accordingly. Promote and demote a replica under active workload.
// Promote a replica and remove it, making sure it gets tombstoned.
TEST_F(RaftConsensusNonVoterITest, PromoteAndDemote) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);
  const int kInitialReplicasNum = 3;
  const int kHbIntervalMs = 50;
  const vector<string> kTserverFlags = {
    Substitute("--raft_heartbeat_interval_ms=$0", kHbIntervalMs),
  };

  FLAGS_num_tablet_servers = 4;
  FLAGS_num_replicas = kInitialReplicasNum;
  NO_FATALS(BuildAndStart(kTserverFlags));
  ASSERT_EQ(4, tablet_servers_.size());
  ASSERT_EQ(kInitialReplicasNum, tablet_replicas_.size());

  const string& tablet_id = tablet_id_;
  TabletServerMap replica_servers;
  for (const auto& e : tablet_replicas_) {
    if (e.first == tablet_id) {
      replica_servers.emplace(e.second->uuid(), e.second);
    }
  }
  ASSERT_EQ(FLAGS_num_replicas, replica_servers.size());

  ASSERT_OK(WaitForServersToAgree(kTimeout, replica_servers, tablet_id, 1));

  TServerDetails* new_replica = nullptr;
  for (const auto& ts : tablet_servers_) {
    if (replica_servers.find(ts.first) == replica_servers.end()) {
      new_replica = ts.second;
      break;
    }
  }
  ASSERT_NE(nullptr, new_replica);

  ASSERT_OK(AddReplica(tablet_id, new_replica, RaftPeerPB::NON_VOTER, kTimeout));

  {
    // It should not be possible to demote the tablet leader.
    TServerDetails* leader;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
    const Status s_demote = ChangeReplicaMembership(RaftPeerPB::NON_VOTER,
                                                    tablet_id, leader, kTimeout);
    const auto s_demote_str = s_demote.ToString();
    ASSERT_TRUE(s_demote.IsInvalidArgument()) << s_demote_str;
    ASSERT_STR_MATCHES(s_demote_str,
        "Cannot change the replica type of peer .* because it is the leader");

    // It should not be possible to promote a leader replica since it's
    // already a voter.
    const Status s_promote = ChangeReplicaMembership(RaftPeerPB::VOTER,
                                                     tablet_id, leader, kTimeout);
    const auto s_promote_str = s_promote.ToString();
    ASSERT_TRUE(s_promote.IsInvalidArgument()) << s_promote_str;
    ASSERT_STR_MATCHES(s_promote_str,
        "Cannot change the replica type of peer .* because it is the leader");
  }

  {
    // It should not be possible to demote a non-voter.
    Status s = ChangeReplicaMembership(RaftPeerPB::NON_VOTER,
                                       tablet_id, new_replica, kTimeout);
    const auto s_str = s.ToString();
    ASSERT_TRUE(s.IsInvalidArgument()) << s_str;
    ASSERT_STR_CONTAINS(s_str, "Cannot change replica type to same type");
  }

  // It should be possible to promote a non-voter to voter.
  ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::VOTER, tablet_id,
                                    new_replica, kTimeout));

  {
    // It should not be possible to promote a voter since it's a voter already.
    Status s = ChangeReplicaMembership(RaftPeerPB::VOTER,
                                       tablet_id, new_replica, kTimeout);
    const auto s_str = s.ToString();
    ASSERT_TRUE(s.IsInvalidArgument()) << s_str;
    ASSERT_STR_CONTAINS(s_str, "Cannot change replica type to same type");
  }

  // Make sure the promoted replica is in the quorum.
  {
    // Pause both the current leader and the new voter, and make sure
    // no new leader can be elected since the majority is now 3 out of 4 voters.
    TServerDetails* leader;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
    ExternalTabletServer* leader_ts =
        cluster_->tablet_server_by_uuid(leader->uuid());
    ASSERT_OK(leader_ts->Pause());
    auto cleanup_leader = MakeScopedCleanup([&]() {
      ASSERT_OK(leader_ts->Resume());
    });

    ExternalTabletServer* new_replica_ts =
        cluster_->tablet_server_by_uuid(new_replica->uuid());
    ASSERT_OK(new_replica_ts->Pause());
    auto cleanup_new_replica = MakeScopedCleanup([&]() {
      ASSERT_OK(new_replica_ts->Resume());
    });

    {
      TServerDetails* leader;
      Status s = GetLeaderReplicaWithRetries(tablet_id, &leader, 10);
      const auto s_str = s.ToString();
      ASSERT_TRUE(s.IsNotFound()) << s_str;
      ASSERT_STR_CONTAINS(s_str, "leader replica not found");
    }

    // Resume the newly promoted replica: there should be 3 out of 4 voters
    // available now, so they should be able to elect a leader among them.
    ASSERT_OK(new_replica_ts->Resume());
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));

    // It should be possible to demote a replica back: 2 out of 3 voters
    // of the result configuration are available, so 2 active voters out of 3
    // (1 is still paused) and 1 non-votes in the tablet constitute a functional
    // tablet, so they should be able to elect a leader replica among them.
    ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::NON_VOTER,
                                      tablet_id, new_replica, kTimeout));
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
  }

  // Promote/demote a replica under active workload.
  {
    TestWorkload workload(cluster_.get());
    workload.set_table_name(kTableId);
    workload.Setup();
    workload.Start();
    while (workload.rows_inserted() < 10) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }

    ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::VOTER,
                                      tablet_id, new_replica, kTimeout));
    auto rows_inserted = workload.rows_inserted();
    while (workload.rows_inserted() < rows_inserted * 2) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }

    ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::NON_VOTER,
                                      tablet_id, new_replica, kTimeout));
    rows_inserted = workload.rows_inserted();
    while (workload.rows_inserted() < rows_inserted * 2) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    workload.StopAndJoin();

    ASSERT_OK(WaitForServersToAgree(kTimeout, replica_servers, tablet_id, 1));

    ClusterVerifier v(cluster_.get());
    NO_FATALS(v.CheckCluster());
    NO_FATALS(v.CheckRowCount(workload.table_name(),
                              ClusterVerifier::EXACTLY,
                              workload.rows_inserted()));
  }

  // Promote the non-voter replica again and then remove it when it's a voter,
  // making sure it gets tombstoned.
  {
    ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::VOTER,
                                      tablet_id, new_replica, kTimeout));
    ASSERT_OK(WaitForServersToAgree(kTimeout, replica_servers, tablet_id, 1));
    ASSERT_OK(RemoveReplica(tablet_id, new_replica, kTimeout));

    // Verify the removed replica gets tombstoned.
    const int new_replica_idx =
        cluster_->tablet_server_index_by_uuid(new_replica->uuid());
    ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
        new_replica_idx, tablet_id, { TABLET_DATA_TOMBSTONED }, kTimeout));

    // The removed tablet replica should be gone, and the master should report
    // appropriate replica count at this point.
    bool has_leader;
    master::TabletLocationsPB tablet_locations;
    ASSERT_OK(WaitForReplicasReportedToMaster(
        cluster_->master_proxy(), kInitialReplicasNum, tablet_id, kTimeout,
        WAIT_FOR_LEADER, &has_leader, &tablet_locations));
    ASSERT_TRUE(has_leader);
  }

  NO_FATALS(cluster_->AssertNoCrashes());
}

}  // namespace tserver
}  // namespace kudu
