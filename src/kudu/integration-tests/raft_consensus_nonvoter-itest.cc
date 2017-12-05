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

#include <cstddef>
#include <cstdint>
#include <numeric>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/replica_controller-internal.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
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
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);

METRIC_DECLARE_gauge_int32(tablet_copy_open_client_sessions);
METRIC_DECLARE_gauge_int32(tablet_copy_open_source_sessions);

using kudu::client::sp::shared_ptr;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduScanToken;
using kudu::client::KuduScanTokenBuilder;
using kudu::client::KuduTable;
using kudu::client::internal::ReplicaController;
using kudu::cluster::ExternalDaemon;
using kudu::cluster::ExternalMaster;
using kudu::cluster::ExternalTabletServer;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::IsRaftConfigMember;
using kudu::consensus::IsRaftConfigVoter;
using kudu::itest::AddServer;
using kudu::itest::GetInt64Metric;
using kudu::itest::GetTableLocations;
using kudu::itest::GetTabletLocations;
using kudu::itest::LeaderStepDown;
using kudu::itest::RemoveServer;
using kudu::itest::StartElection;
using kudu::itest::TServerDetails;
using kudu::itest::TabletServerMap;
using kudu::itest::WAIT_FOR_LEADER;
using kudu::itest::WaitForReplicasReportedToMaster;
using kudu::master::ANY_REPLICA;
using kudu::master::GetTableLocationsResponsePB;
using kudu::master::TabletLocationsPB;
using kudu::master::VOTER_REPLICA;
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

  // Get a tablet server with at least one replica of the test tablet identified
  // by the 'tablet_id_' member.
  ExternalTabletServer* GetServerWithReplica() const;

  // Get tablet servers not hosting replicas of the test tablet identified
  // by the 'tablet_id_' member.
  void GetServersWithoutReplica(vector<ExternalTabletServer*>* servers) const;
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

static bool IsConfigurationLeaderError(const Status& s) {
  static const string kPattern = "*Replica * is not leader of this config*";
  return s.IsIllegalState() && MatchPattern(s.ToString(), kPattern);
}

Status RaftConsensusNonVoterITest::AddReplica(const string& tablet_id,
                                              const TServerDetails* replica,
                                              RaftPeerPB::MemberType replica_type,
                                              const MonoDelta& timeout) {
  const MonoTime deadline = MonoTime::Now() + timeout;
  while (MonoTime::Now() < deadline) {
    TServerDetails* leader = nullptr;
    RETURN_NOT_OK(WaitForLeaderWithCommittedOp(tablet_id, timeout, &leader));
    Status s = AddServer(leader, tablet_id, replica, replica_type, timeout);
    if (IsConfigurationLeaderError(s)) {
      // The leader has changed, retry.
      continue;
    }
    return s;
  }
  return Status::TimedOut(Substitute("timeout adding replica $0 for tablet $1",
                                     replica->uuid(), tablet_id));
}

Status RaftConsensusNonVoterITest::RemoveReplica(const string& tablet_id,
                                                 const TServerDetails* replica,
                                                 const MonoDelta& timeout) {
  const MonoTime deadline = MonoTime::Now() + timeout;
  while (MonoTime::Now() < deadline) {
    TServerDetails* leader = nullptr;
    RETURN_NOT_OK(WaitForLeaderWithCommittedOp(tablet_id, timeout, &leader));
    Status s = RemoveServer(leader, tablet_id, replica, timeout);
    if (IsConfigurationLeaderError(s)) {
      // The leader has changed, retry.
      continue;
    }
    return s;
  }
  return Status::TimedOut(Substitute("timeout removing replica $0 for tablet $1",
                                     replica->uuid(), tablet_id));
}

Status RaftConsensusNonVoterITest::ChangeReplicaMembership(
    RaftPeerPB::MemberType member_type,
    const string& tablet_id,
    const TServerDetails* replica,
    const MonoDelta& timeout) {
  const MonoTime deadline = MonoTime::Now() + timeout;
  while (MonoTime::Now() < deadline) {
    TServerDetails* leader = nullptr;
    RETURN_NOT_OK(WaitForLeaderWithCommittedOp(tablet_id, timeout, &leader));
    Status s = ChangeReplicaType(leader, tablet_id, replica, member_type, timeout);
    if (IsConfigurationLeaderError(s)) {
      // The leader has changed, retry.
      continue;
    }
    return s;
  }
  return Status::TimedOut(
      Substitute("tablet $0: timeout changing replica $0 membership type",
                 tablet_id, replica->uuid()));
}

ExternalTabletServer* RaftConsensusNonVoterITest::GetServerWithReplica() const {
  ExternalTabletServer* ts = nullptr;
  for (const auto& e : tablet_replicas_) {
    if (e.first == tablet_id_) {
      ts = cluster_->tablet_server_by_uuid(e.second->uuid());
      break;
    }
  }
  return ts;
}

void RaftConsensusNonVoterITest::GetServersWithoutReplica(
    vector<ExternalTabletServer*>* servers) const {
  ASSERT_NE(nullptr, servers);
  servers->clear();

  TabletServerMap replica_servers;
  for (const auto& e : tablet_replicas_) {
    if (e.first == tablet_id_) {
      replica_servers.emplace(e.second->uuid(), e.second);
    }
  }

  for (const auto& ts : tablet_servers_) {
    if (replica_servers.find(ts.first) == replica_servers.end()) {
      auto* s = cluster_->tablet_server_by_uuid(ts.second->uuid());
      ASSERT_NE(nullptr, s);
      servers->push_back(s);
    }
  }
}

// Verify that GetTableLocations() and GetTabletLocations() return the expected
// set of replicas when for the specified replica type filter.
TEST_F(RaftConsensusNonVoterITest, GetTableAndTabletLocations) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);
  const int kOriginalReplicasNum = 3;
  FLAGS_num_tablet_servers = kOriginalReplicasNum + 1;
  FLAGS_num_replicas = kOriginalReplicasNum;
  NO_FATALS(BuildAndStart());
  ASSERT_EQ(FLAGS_num_tablet_servers, tablet_servers_.size());
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
  // Wait the newly added replica to start.
  ASSERT_OK(WaitForNumTabletsOnTS(
      new_replica, 1, kTimeout, nullptr, tablet::RUNNING));

  const auto count_roles = [this](const TabletLocationsPB& tablet_locations,
      int* num_leaders, int* num_followers, int* num_learners) {
    *num_leaders = 0;
    *num_followers = 0;
    *num_learners = 0;
    for (const auto& r : tablet_locations.replicas()) {
      *num_leaders += (r.role() == RaftPeerPB::LEADER) ? 1 : 0;
      *num_followers += (r.role() == RaftPeerPB::FOLLOWER) ? 1 : 0;
      *num_learners += (r.role() == RaftPeerPB::LEARNER) ? 1 : 0;
    }
  };

  // Verify that replica type filter yields appropriate results for
  // GetTableLocations() RPC.
  {
    GetTableLocationsResponsePB table_locations;
    ASSERT_OK(GetTableLocations(cluster_->master_proxy(), table_->name(),
                                kTimeout, VOTER_REPLICA, &table_locations));
    ASSERT_EQ(1, table_locations.tablet_locations().size());
    const TabletLocationsPB& locations = table_locations.tablet_locations(0);
    ASSERT_EQ(tablet_id_, locations.tablet_id());
    ASSERT_EQ(kOriginalReplicasNum, locations.replicas_size());
    int num_leaders = 0, num_followers = 0, num_learners = 0;
    count_roles(locations, &num_leaders, &num_followers, &num_learners);
    ASSERT_EQ(kOriginalReplicasNum, num_leaders + num_followers);
    ASSERT_EQ(0, num_learners);
  }
  {
    GetTableLocationsResponsePB table_locations;
    ASSERT_OK(GetTableLocations(cluster_->master_proxy(), table_->name(),
                                kTimeout, ANY_REPLICA, &table_locations));
    ASSERT_EQ(1, table_locations.tablet_locations().size());
    const TabletLocationsPB& locations = table_locations.tablet_locations(0);
    ASSERT_EQ(tablet_id_, locations.tablet_id());
    ASSERT_EQ(kOriginalReplicasNum + 1, locations.replicas_size());
    int num_leaders = 0, num_followers = 0, num_learners = 0;
    count_roles(locations, &num_leaders, &num_followers, &num_learners);
    ASSERT_EQ(kOriginalReplicasNum, num_leaders + num_followers);
    ASSERT_EQ(1, num_learners);
  }

  // Verify that replica type filter yields appropriate results for
  // GetTabletLocations() RPC.
  {
    TabletLocationsPB tablet_locations;
    ASSERT_OK(GetTabletLocations(cluster_->master_proxy(), tablet_id_,
                                 kTimeout, VOTER_REPLICA, &tablet_locations));
    ASSERT_EQ(kOriginalReplicasNum, tablet_locations.replicas_size());
    int num_leaders = 0, num_followers = 0, num_learners = 0;
    count_roles(tablet_locations, &num_leaders, &num_followers, &num_learners);
    ASSERT_EQ(kOriginalReplicasNum, num_leaders + num_followers);
    ASSERT_EQ(0, num_learners);
  }
  {
    TabletLocationsPB tablet_locations;
    ASSERT_OK(GetTabletLocations(cluster_->master_proxy(), tablet_id_,
                                 kTimeout, ANY_REPLICA, &tablet_locations));
    ASSERT_EQ(kOriginalReplicasNum + 1, tablet_locations.replicas_size());
    int num_leaders = 0, num_followers = 0, num_learners = 0;
    count_roles(tablet_locations, &num_leaders, &num_followers, &num_learners);
    ASSERT_EQ(kOriginalReplicasNum, num_leaders + num_followers);
    ASSERT_EQ(1, num_learners);
  }
}

// Verify that non-voters replicas are not exposed to a regular Kudu client.
// However, it's possible to see the replicas.
TEST_F(RaftConsensusNonVoterITest, ReplicaMatchPolicy) {
  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);
  const int kOriginalReplicasNum = 3;
  FLAGS_num_tablet_servers = kOriginalReplicasNum + 1;
  FLAGS_num_replicas = kOriginalReplicasNum;
  NO_FATALS(BuildAndStart());
  ASSERT_EQ(FLAGS_num_tablet_servers, tablet_servers_.size());
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
  // Wait the newly added replica to start.
  ASSERT_OK(WaitForNumTabletsOnTS(
      new_replica, 1, kTimeout, nullptr, tablet::RUNNING));

  auto count_replicas = [this](KuduTable* table, size_t* count) {
    vector<KuduScanToken*> tokens;
    ElementDeleter deleter(&tokens);
    KuduScanTokenBuilder builder(table);
    RETURN_NOT_OK(builder.Build(&tokens));
    size_t replicas_count = std::accumulate(
        tokens.begin(), tokens.end(), 0,
        [](size_t sum, const KuduScanToken* token) {
          return sum + token->tablet().replicas().size();
        });
    *count = replicas_count;
    return Status::OK();
  };

  // The case of regular client: the non-voter replica should not be seen.
  {
    size_t count = 0;
    ASSERT_OK(count_replicas(table_.get(), &count));
    EXPECT_EQ(kOriginalReplicasNum, count);
  }

  // The case of special client used for internal tools, etc.: non-voter
  // replicas should be visible.
  {
    KuduClientBuilder builder;
    ReplicaController::SetVisibility(&builder, ReplicaController::Visibility::ALL);
    shared_ptr<KuduClient> client;
    ASSERT_OK(builder
              .add_master_server_addr(cluster_->master()->bound_rpc_addr().ToString())
              .Build(&client));
    ASSERT_NE(nullptr, client.get());
    shared_ptr<KuduTable> t;
    ASSERT_OK(client->OpenTable(table_->name(), &t));

    size_t count = 0;
    ASSERT_OK(count_replicas(t.get(), &count));
    EXPECT_EQ(kOriginalReplicasNum + 1, count);
  }
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
  TabletLocationsPB tablet_locations;
  ASSERT_OK(WaitForReplicasReportedToMaster(
      cluster_->master_proxy(), kOriginalReplicasNum + 1, tablet_id, kTimeout,
      WAIT_FOR_LEADER, ANY_REPLICA, &has_leader, &tablet_locations));
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
  v.SetOperationsTimeout(kTimeout);
  v.SetVerificationTimeout(kTimeout);
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
  v.SetOperationsTimeout(kTimeout);
  v.SetVerificationTimeout(kTimeout);
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
  TabletLocationsPB tablet_locations;
  ASSERT_OK(WaitForReplicasReportedToMaster(
      cluster_->master_proxy(), kOriginalReplicasNum, tablet_id, kTimeout,
      WAIT_FOR_LEADER, ANY_REPLICA, &has_leader, &tablet_locations));
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
    SCOPED_CLEANUP({
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

// Test that it's possible to promote a NON_VOTER replica into a VOTER
// and demote a VOTER replica into NON_VOTER (if not a leader).
// Promote and demote a replica under active workload.
// Promote a replica and remove it, making sure it gets tombstoned.
TEST_F(RaftConsensusNonVoterITest, PromoteAndDemote) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const MonoDelta kTimeout = MonoDelta::FromSeconds(120);
  const int kInitialReplicasNum = 3;
  const int kHbIntervalMs = 50;
  const vector<string> kTserverFlags = {
    // Run the test faster: shorten the default interval for Raft heartbeats.
    Substitute("--raft_heartbeat_interval_ms=$0", kHbIntervalMs),

    // The scenario does not assume replicas might be evicted while running.
    "--evict_failed_followers=false",

    // To avoid flakiness, this scenario disables leader re-elections.
    "--enable_leader_failure_detection=false",
  };
  const vector<string> kMasterFlags = {
    // The election is run manually after BuildAndStart().
    "--catalog_manager_wait_for_new_tablets_to_elect_leader=false"
  };

  FLAGS_num_tablet_servers = 4;
  FLAGS_num_replicas = kInitialReplicasNum;
  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));
  ASSERT_FALSE(tablet_replicas_.empty());
  ASSERT_OK(StartElection(tablet_replicas_.begin()->second, tablet_id_, kTimeout));
  ASSERT_OK(WaitUntilLeader(tablet_replicas_.begin()->second, tablet_id_, kTimeout));

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
  TServerDetails* replica = nullptr;
  for (const auto& ts : tablet_servers_) {
    if (replica_servers.find(ts.first) == replica_servers.end()) {
      replica = ts.second;
      break;
    }
  }
  ASSERT_NE(nullptr, replica);

  ASSERT_OK(AddReplica(tablet_id, replica, RaftPeerPB::NON_VOTER, kTimeout));

  // Wait for tablet copy to complete: the scenario assumes the newly added
  // replica is up-to-date.
  ASSERT_OK(WaitForNumTabletsOnTS(
      replica, 1, kTimeout, nullptr, tablet::RUNNING));

  // It should be impossible to demote a non-voter.
  {
    Status s = ChangeReplicaMembership(RaftPeerPB::NON_VOTER,
                                       tablet_id, replica, kTimeout);
    const auto s_str = s.ToString();
    ASSERT_TRUE(s.IsInvalidArgument()) << s_str;
    ASSERT_STR_CONTAINS(s_str, "must modify a field when calling MODIFY_PEER");
  }

  // It should be possible to promote a non-voter to voter.
  ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::VOTER, tablet_id,
                                    replica, kTimeout));

  // It should be impossible to promote a voter since it's a voter already.
  {
    Status s = ChangeReplicaMembership(RaftPeerPB::VOTER,
                                       tablet_id, replica, kTimeout);
    const auto s_str = s.ToString();
    ASSERT_TRUE(s.IsInvalidArgument()) << s_str;
    ASSERT_STR_CONTAINS(s_str, "must modify a field when calling MODIFY_PEER");
  }

  {
    // It should be impossible to demote the tablet leader.
    TServerDetails* leader;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
    const Status s_demote = ChangeReplicaMembership(RaftPeerPB::NON_VOTER,
                                                    tablet_id, leader, kTimeout);
    const auto s_demote_str = s_demote.ToString();
    ASSERT_TRUE(s_demote.IsInvalidArgument()) << s_demote_str;
    ASSERT_STR_MATCHES(s_demote_str,
        "Cannot modify member type of peer .* because it is the leader");

    // It should be impossible to promote a leader replica since it's
    // already a voter.
    const Status s_promote = ChangeReplicaMembership(RaftPeerPB::VOTER,
                                                     tablet_id, leader, kTimeout);
    const auto s_promote_str = s_promote.ToString();
    ASSERT_TRUE(s_promote.IsInvalidArgument()) << s_promote_str;
    ASSERT_STR_CONTAINS(s_promote_str, "must modify a field when calling MODIFY_PEER");
  }

  // Demote the replica back.
  ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::NON_VOTER,
                                    tablet_id, replica, kTimeout));

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
                                      tablet_id, replica, kTimeout));
    auto rows_inserted = workload.rows_inserted();
    while (workload.rows_inserted() < rows_inserted * 2) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }

    ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::NON_VOTER,
                                      tablet_id, replica, kTimeout));
    rows_inserted = workload.rows_inserted();
    while (workload.rows_inserted() < rows_inserted * 2) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    workload.StopAndJoin();

    ASSERT_OK(WaitForServersToAgree(kTimeout, replica_servers, tablet_id, 1));

    ClusterVerifier v(cluster_.get());
    v.SetOperationsTimeout(kTimeout);
    v.SetVerificationTimeout(kTimeout);
    NO_FATALS(v.CheckCluster());
    NO_FATALS(v.CheckRowCount(workload.table_name(),
                              ClusterVerifier::EXACTLY,
                              workload.rows_inserted()));
  }

  // Promote the non-voter replica again and then remove it when it's a voter,
  // making sure it gets tombstoned.
  {
    ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::VOTER,
                                      tablet_id, replica, kTimeout));
    ASSERT_OK(WaitForServersToAgree(kTimeout, replica_servers, tablet_id, 1));

    ASSERT_OK(RemoveReplica(tablet_id, replica, kTimeout));

    // Verify the removed replica gets tombstoned.
    const int replica_idx =
        cluster_->tablet_server_index_by_uuid(replica->uuid());
    ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(
        replica_idx, tablet_id, { TABLET_DATA_TOMBSTONED }, kTimeout));

    // The removed tablet replica should be gone, and the master should report
    // appropriate replica count at this point.
    bool has_leader;
    TabletLocationsPB tablet_locations;
    ASSERT_OK(WaitForReplicasReportedToMaster(
        cluster_->master_proxy(), kInitialReplicasNum, tablet_id, kTimeout,
        WAIT_FOR_LEADER, ANY_REPLICA, &has_leader, &tablet_locations));
    ASSERT_TRUE(has_leader);
  }

  NO_FATALS(cluster_->AssertNoCrashes());
}

// Verify that a newly promoted replica is a full-fledged voting member:
//
// a) The newly promoted replica should be able to participate in elections,
//    and its vote is counted in.
//
// b) The newly promoted replica can become a leader. This is to verify that
//    once the non-voter replica is promoted to voter membership,
//    its failure detection mechanism works as expected.
//
TEST_F(RaftConsensusNonVoterITest, PromotedReplicaCanVote) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);
  const int kInitialReplicasNum = 3;
  const int kHbIntervalMs = 50;
  const vector<string> kTserverFlags = {
    // Run the test faster: shorten the default interval for Raft heartbeats.
    Substitute("--raft_heartbeat_interval_ms=$0", kHbIntervalMs),

    // The scenario does not assume replicas might be evicted while running.
    "--evict_failed_followers=false",
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

  // Find the tablet server without a replica of the tablet (yet).
  TServerDetails* replica = nullptr;
  for (const auto& ts : tablet_servers_) {
    if (!ContainsKey(replica_servers, ts.first)) {
      replica = ts.second;
      break;
    }
  }
  ASSERT_NE(nullptr, replica);
  const auto& new_replica_uuid = replica->uuid();

  ASSERT_OK(AddReplica(tablet_id, replica, RaftPeerPB::NON_VOTER, kTimeout));

  // Wait for tablet copy to complete. Otherwise, in the sequence below,
  // the leader may be paused before the copying is complete.
  ASSERT_OK(WaitForNumTabletsOnTS(
      replica, 1, kTimeout, nullptr, tablet::RUNNING));

  ASSERT_OK(ChangeReplicaMembership(RaftPeerPB::VOTER, tablet_id,
                                    replica, kTimeout));
  // The newly added voter needs to be registered in the internal replica map
  // tablet_replicas_: this is necessary for the future calls like
  // GetLeaderReplicaWithRetries() when the replica becomes a leader.
  NO_FATALS(WaitForReplicasAndUpdateLocations(table_->name()));
  ASSERT_EQ(kInitialReplicasNum + 1, tablet_replicas_.size());

  // Verify that the newly promoted replica's vote counts to achieve
  // the 'strict majority' of the voters.
  {
    // First, make sure the newly added replica is not the tablet leader.
    TServerDetails* leader;
    while (true) {
      ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
      if (leader->uuid() != new_replica_uuid) {
        break;
      }
      ASSERT_OK(LeaderStepDown(leader, tablet_id, kTimeout));
    }

    // Pause the newly added replica and the leader.
    ExternalTabletServer* replica_ts =
        cluster_->tablet_server_by_uuid(replica->uuid());
    ASSERT_OK(replica_ts->Pause());
    auto cleanup_replica = MakeScopedCleanup([&]() {
      ASSERT_OK(replica_ts->Resume());
    });

    ExternalTabletServer* leader_ts =
        cluster_->tablet_server_by_uuid(leader->uuid());
    ASSERT_OK(leader_ts->Pause());
    auto cleanup_leader = MakeScopedCleanup([&]() {
      ASSERT_OK(leader_ts->Resume());
    });

    // With both the current leader and the new voter paused, no new leader
    // can be elected since the majority is now 3 out of 4 voters, while
    // just 2 voters are alive.
    Status s = GetLeaderReplicaWithRetries(tablet_id, &leader, 10);
    const auto s_str = s.ToString();
    ASSERT_TRUE(s.IsNotFound()) << s_str;
    ASSERT_STR_CONTAINS(s_str, "leader replica not found");

    // Resume the newly added voter: with the paused leader, the rest of the
    // replicas should be able to elect a new leader, since the majority is 3
    // out of 4 replicas.
    ASSERT_OK(replica_ts->Resume());
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
  }

  // Make sure the promoted replica can become a leader using the mechanism
  // of leader failure detection and subsequent re-election.
  TServerDetails* leader = nullptr;
  for (auto i = 0; i < 250; ++i) {
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id, &leader));
    if (leader->uuid() == new_replica_uuid) {
      // Success: the newly added replica has become a leader.
      break;
    }

    ExternalTabletServer* leader_ts =
        cluster_->tablet_server_by_uuid(leader->uuid());
    // Pause the current leader to induce leader re-election. The rest of the
    // replicas should be able to elect a new leader, since the majority is 3
    // out of 4 replicas.
    ASSERT_OK(leader_ts->Pause());
    auto cleanup_leader = MakeScopedCleanup([&]() {
      ASSERT_OK(leader_ts->Resume());
    });
    SleepFor(MonoDelta::FromMilliseconds(kHbIntervalMs *
        static_cast<int64_t>(FLAGS_leader_failure_max_missed_heartbeat_periods + 1.0)));
  }
  ASSERT_NE(nullptr, leader);
  ASSERT_EQ(new_replica_uuid, leader->uuid());
}

// Add an extra non-voter replica to the tablet and make sure it's evicted
// by the catalog manager once catalog manager sees its state updated.
TEST_F(RaftConsensusNonVoterITest, CatalogManagerEvictsExcessNonVoter) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const int kReplicaUnavailableSec = 5;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);
  const int kReplicasNum = 3;
  FLAGS_num_replicas = kReplicasNum;
  // Need one extra tserver for a new non-voter replica.
  FLAGS_num_tablet_servers = kReplicasNum + 1;
  const vector<string> kMasterFlags = {
    // The scenario runs with the new replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",
    // Don't evict excess replicas to avoid races in the scenario.
    "--catalog_manager_evict_excess_replicas=false",
  };
  const vector<string> kTserverFlags = {
    // The scenario runs with the new replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",
    Substitute("--consensus_rpc_timeout_ms=$0", 1000 * kReplicaUnavailableSec),
    Substitute("--follower_unavailable_considered_failed_sec=$0",
              kReplicaUnavailableSec),
  };

  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));
  ASSERT_EQ(kReplicasNum + 1, tablet_servers_.size());
  ASSERT_EQ(kReplicasNum, tablet_replicas_.size());

  TabletServerMap replica_servers;
  for (const auto& e : tablet_replicas_) {
    if (e.first == tablet_id_) {
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
  ASSERT_OK(AddReplica(tablet_id_, new_replica, RaftPeerPB::NON_VOTER, kTimeout));

  bool has_leader = false;
  TabletLocationsPB tablet_locations;
  // Make sure the extra replica is seen by the master.
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            kReplicasNum + 1,
                                            tablet_id_,
                                            kTimeout,
                                            WAIT_FOR_LEADER,
                                            ANY_REPLICA,
                                            &has_leader,
                                            &tablet_locations));

  // Switch the catalog manager to start evicting excess replicas
  // (that's how it runs by default in the new replica management scheme).
  // Prior to this point, that might lead to a race, since the newly added
  // non-voter replica might be evicted before it's spotted by the
  // WaitForReplicasReportedToMaster() call above.
  for (auto i = 0; i < cluster_->num_masters(); ++i) {
    ExternalMaster* m = cluster_->master(i);
    ASSERT_OK(cluster_->SetFlag(m,
        "catalog_manager_evict_excess_replicas", "true"));
  }

  ExternalTabletServer* new_replica_ts =
      cluster_->tablet_server_by_uuid(new_replica->uuid());
  ASSERT_NE(nullptr, new_replica_ts);
  ASSERT_OK(new_replica_ts->Pause());
  SCOPED_CLEANUP({
    ASSERT_OK(new_replica_ts->Resume());
  });
  SleepFor(MonoDelta::FromSeconds(2 * kReplicaUnavailableSec));
  ASSERT_OK(new_replica_ts->Resume());

  // Make sure the excess non-voter replica is gone.
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            kReplicasNum,
                                            tablet_id_,
                                            kTimeout,
                                            WAIT_FOR_LEADER,
                                            ANY_REPLICA,
                                            &has_leader,
                                            &tablet_locations));
  NO_FATALS(cluster_->AssertNoCrashes());
}

// Check that the catalog manager adds a non-voter replica to replace failed
// voter replica in a tablet.
//
// TODO(aserbin): and make it run for 5 tablet servers.
TEST_F(RaftConsensusNonVoterITest, CatalogManagerAddsNonVoter) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const int kReplicaUnavailableSec = 10;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(6 * kReplicaUnavailableSec);
  const int kReplicasNum = 3;
  FLAGS_num_replicas = kReplicasNum;
  // Need one extra tserver after the tserver with on of the replicas stopped.
  // Otherwise, the catalog manager would not be able to spawn a new non-voter
  // replacement replica.
  FLAGS_num_tablet_servers = kReplicasNum + 1;
  const vector<string> kMasterFlags = {
    // The scenario runs with the new replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",
    // Don't evict excess replicas to avoid races in the scenario.
    "--catalog_manager_evict_excess_replicas=false",
  };
  const vector<string> kTserverFlags = {
    // The scenario runs with the new replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",
    Substitute("--follower_unavailable_considered_failed_sec=$0",
               kReplicaUnavailableSec),
  };

  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));
  ASSERT_EQ(kReplicasNum + 1, tablet_servers_.size());
  ASSERT_EQ(kReplicasNum, tablet_replicas_.size());

  ExternalTabletServer* ts_with_replica = GetServerWithReplica();
  ASSERT_NE(nullptr, ts_with_replica);
  ts_with_replica->Shutdown();

  // Wait for a new non-voter replica added by the catalog manager to
  // replace the failed one.
  bool has_leader = false;
  TabletLocationsPB tablet_locations;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            kReplicasNum + 1,
                                            tablet_id_,
                                            kTimeout,
                                            WAIT_FOR_LEADER,
                                            ANY_REPLICA,
                                            &has_leader,
                                            &tablet_locations));
  NO_FATALS(cluster_->AssertNoCrashes());
}

// Verify the behavior of the catalog manager for the gone-and-back tablet
// server in --raft_prepare_replacement_before_eviction=true case. This scenario
// addresses the situation when a tablet server hosting tablet replicas has not
// been running for some time (e.g., a bit over the time interval specified by
// the 'follower_unavailable_considered_failed_sec' flag), and then it comes
// back before the newly added non-voter replicas are promoted. As a result, the
// original voter replicas from the tablet server should stay, but the newly
// added non-voter replicas should be evicted.
TEST_F(RaftConsensusNonVoterITest, TabletServerIsGoneAndBack) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const auto kReplicasNum = 3;
  const auto kReplicaUnavailableSec = 5;
  const auto kTimeoutSec = 60;
  const auto kTimeout = MonoDelta::FromSeconds(kTimeoutSec);
  FLAGS_num_replicas = kReplicasNum;
  // Need one extra tserver after the tserver with on of the replicas stopped.
  // Otherwise, the catalog manager would not be able to spawn a new non-voter
  // replacement replicas elsewhere.
  FLAGS_num_tablet_servers = kReplicasNum + 1;
  const vector<string> kMasterFlags = {
    // The scenario runs with the 3-4-3 replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",
  };
  const vector<string> kTserverFlags = {
    // The scenario runs with the 3-4-3 replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",
    Substitute("--follower_unavailable_considered_failed_sec=$0",
               kReplicaUnavailableSec),
    // Slow down tablet copy to avoid new non-voter replicas catching up with
    // the leader replicas, otherwise they might be promoted to voters before
    // the replicas from the 'failed' tablet server is back.
    Substitute("--tablet_copy_download_file_inject_latency_ms=$0",
               MonoDelta::FromSeconds(3 * kTimeoutSec).ToMilliseconds()),
    // Don't wait for the RPC timeout for too long.
    Substitute("--consensus_rpc_timeout_ms=$0", 1000 * kReplicaUnavailableSec),
  };

  NO_FATALS(BuildAndStart(kTserverFlags, kMasterFlags));
  ASSERT_EQ(kReplicasNum + 1, tablet_servers_.size());
  ASSERT_EQ(kReplicasNum, tablet_replicas_.size());

  // Create a test table and insert some data into the table,
  // so the special flag --tablet_copy_download_file_inject_latency_ms
  // could take affect while tablet copy happens down the road.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  ExternalTabletServer* ts_with_replica = GetServerWithReplica();
  ASSERT_NE(nullptr, ts_with_replica);
  ts_with_replica->Shutdown();

  // The leader replica marks the non-responsive replica as failed after
  // FLAGS_follower_unavailable_considered_failed_sec time interval. The
  // catalog manager should spot that and add a new non-voter replica as a
  // replacement.
  bool has_leader = false;
  TabletLocationsPB tablet_locations;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            kReplicasNum + 1,
                                            tablet_id_,
                                            kTimeout,
                                            WAIT_FOR_LEADER,
                                            ANY_REPLICA,
                                            &has_leader,
                                            &tablet_locations));

  // Restart the tablet server with the replica which has been marked as failed.
  ASSERT_OK(ts_with_replica->Restart());

  // Since the new non-voter replica is still not ready for promotion because
  // the tablet copy is in progress, and all the original voter replicas are in
  // good health, the catalog manager should evict an excess non-voter replica.
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            kReplicasNum,
                                            tablet_id_,
                                            kTimeout,
                                            WAIT_FOR_LEADER,
                                            ANY_REPLICA,
                                            &has_leader,
                                            &tablet_locations));
  // Make sure the replica from the gone-and-back server is part of the config.
  consensus::ConsensusStatePB cstate;
  ASSERT_EVENTUALLY([&] {
    TServerDetails* leader = nullptr;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
    // The reason for the outside ASSERT_EVENTUALLY is that the leader might
    // have changed in between of these two calls.
    ASSERT_OK(GetConsensusState(leader, tablet_id_, kTimeout, &cstate));
  });
  ASSERT_TRUE(IsRaftConfigMember(ts_with_replica->uuid(), cstate.committed_config()));

  NO_FATALS(cluster_->AssertNoCrashes());
}

// A two-step sceanario: first, an existing tablet replica fails because it
// fails behind the threshold of the GCed WAL segment threshold. The catalog
// manager should notice that and add a new non-voter replica in attempt to
// replace the failed replica. Then, the newly added non-voter replica becomes
// unavailable before completing the tablet copy phase. The catalog manager
// should add a new non-voter replica to make it possible to replace the failed
// voter replica, so eventually the tablet has appropriate number of functional
// replicas to guarantee the tablet's replication factor.
TEST_F(RaftConsensusNonVoterITest, FailedTabletCopy) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const auto kReplicasNum = 3;
  const auto kConsensusRpcTimeout = MonoDelta::FromSeconds(5);
  const auto kTimeoutSec = 60;
  const auto kTimeout = MonoDelta::FromSeconds(kTimeoutSec);
  FLAGS_num_replicas = kReplicasNum;
  // Need two extra tablet servers for the scenario.
  FLAGS_num_tablet_servers = kReplicasNum + 2;

  const vector<string> kMasterFlags = {
    // The scenario runs with the 3-4-3 replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",

    // Detect unavailable tablet servers faster.
    Substitute("--tserver_unresponsive_timeout_ms=$0",
        kConsensusRpcTimeout.ToMilliseconds() / 2),
  };
  vector<string> tserver_flags = {
    // The scenario runs with the 3-4-3 replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",

    Substitute("--follower_unavailable_considered_failed_sec=$0",
        2 * static_cast<int>(kConsensusRpcTimeout.ToSeconds())),

    // Don't wait for the RPC timeout for too long.
    Substitute("--consensus_rpc_timeout_ms=$0", kConsensusRpcTimeout.ToMilliseconds()),
  };
  AddFlagsForLogRolls(&tserver_flags); // For CauseFollowerToFallBehindLogGC().

  NO_FATALS(BuildAndStart(tserver_flags, kMasterFlags));
  ASSERT_EQ(kReplicasNum + 2, tablet_servers_.size());
  ASSERT_EQ(kReplicasNum, tablet_replicas_.size());

  // Make sure the tablet copy will fail on one of tablet servers without
  // tablet replicas.
  vector<ExternalTabletServer*> servers_without_replica;
  NO_FATALS(GetServersWithoutReplica(&servers_without_replica));
  ASSERT_EQ(2, servers_without_replica.size());
  ExternalTabletServer* ts0 = *servers_without_replica.begin();
  ASSERT_OK(cluster_->SetFlag(ts0,
                              "tablet_copy_fault_crash_on_fetch_all", "1.0"));
  // Make sure the second tablet server is not available as a candidate for
  // the new non-voter replica.
  ExternalTabletServer* ts1 = *servers_without_replica.rbegin();
  ts1->Shutdown();
  SleepFor(kConsensusRpcTimeout);

  // Cause follower 'follower_uuid' to fail.
  string follower_uuid;
  {
    TabletServerMap replica_servers;
    for (const auto& e : tablet_replicas_) {
      if (e.first == tablet_id_) {
        replica_servers.emplace(e.second->uuid(), e.second);
      }
    }
    string leader_uuid;
    int64_t orig_term;
    NO_FATALS(CauseFollowerToFallBehindLogGC(
        replica_servers, &leader_uuid, &orig_term, &follower_uuid));
  }

  // The leader replica marks the non-responsive replica as failed after it
  // realizes the replica would not be able to catch up, and the catalog
  // manager should add a new non-voter as a replacement.
  bool has_leader = false;
  TabletLocationsPB tablet_locations;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            kReplicasNum + 1,
                                            tablet_id_,
                                            kTimeout,
                                            WAIT_FOR_LEADER,
                                            ANY_REPLICA,
                                            &has_leader,
                                            &tablet_locations));

  // However, the tablet server with the new non-voter replica crashes during
  // the tablet copy phase. Give the catalog manager some time to detect that
  // and purge the failed non-voter from the configuration. Also, the TS manager
  // should detect that the crashed server is not a candidate for new replicas
  // anymore.
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            kReplicasNum,
                                            tablet_id_,
                                            kTimeout,
                                            WAIT_FOR_LEADER,
                                            ANY_REPLICA,
                                            &has_leader,
                                            &tablet_locations));
  consensus::ConsensusStatePB cstate;
  ASSERT_EVENTUALLY([&] {
    TServerDetails* leader = nullptr;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
    // The reason for the outside ASSERT_EVENTUALLY is that the leader might
    // have changed in between of these two calls.
    ASSERT_OK(GetConsensusState(leader, tablet_id_, kTimeout, &cstate));
  });
  // The original voter replica that fell behind WAL catchup threshold should
  // still be there.
  EXPECT_TRUE(IsRaftConfigMember(follower_uuid, cstate.committed_config()))
      << pb_util::SecureDebugString(cstate.committed_config())
      << "fell behind WAL replica UUID: " << follower_uuid;
  // The first non-voter replica failed on tablet copy should be evicted.
  EXPECT_FALSE(IsRaftConfigMember(ts0->uuid(), cstate.committed_config()))
      << pb_util::SecureDebugString(cstate.committed_config())
      << "failed tablet copy replica UUID: " << ts0->uuid();
  // No replicas from the second tablet server should be in the config yet.
  EXPECT_FALSE(IsRaftConfigMember(ts1->uuid(), cstate.committed_config()))
      << pb_util::SecureDebugString(cstate.committed_config())
      << "new replacement replica UUID: " << ts1->uuid();

  // Make the second server available for placing a non-voter replica. Tablet
  // copy for replicas on this with this server should succeed.
  ASSERT_OK(ts1->Restart());

  // The system should be able to recover, replacing the failed replica with
  // the replica on ts1. Since it's hard to predict when the replacement takes
  // place, the easiest way is to wait until it's converges into the required
  // state.
  ASSERT_EVENTUALLY([&] {
    bool has_leader = false;
    TabletLocationsPB tablet_locations;
    ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                              kReplicasNum,
                                              tablet_id_,
                                              kTimeout,
                                              WAIT_FOR_LEADER,
                                              ANY_REPLICA,
                                              &has_leader,
                                              &tablet_locations));

    TServerDetails* leader = nullptr;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
    ASSERT_OK(GetConsensusState(leader, tablet_id_, kTimeout, &cstate));
    // The original voter replica that fell behind the WAL catchup threshold
    // should be evicted.
    ASSERT_FALSE(IsRaftConfigMember(follower_uuid, cstate.committed_config()))
        << pb_util::SecureDebugString(cstate.committed_config())
        << "fell behind WAL replica UUID: " << follower_uuid;
    // The first non-voter replica failed during the tablet copy phase
    // should not be present.
    ASSERT_FALSE(IsRaftConfigMember(ts0->uuid(), cstate.committed_config()))
        << pb_util::SecureDebugString(cstate.committed_config())
        << "failed tablet copy replica UUID: " << ts0->uuid();
    // The tablet copy on the restarted server should succeed and this replica
    // should replace the original failed replica.
    ASSERT_TRUE(IsRaftConfigMember(ts1->uuid(), cstate.committed_config()))
        << pb_util::SecureDebugString(cstate.committed_config())
        << "new replacement replica UUID: " << ts1->uuid();
  });
}

// This test verifies that the replica replacement process continues after
// restarting tablet servers, where non-voter replicas which completed tablet
// copy before the restart of the cluster stay in the configuration and then
// are promoted to voter replica.
//
// The following scenario is used: a voter replica fails, and the catalog
// manager adds a new non-voter replica to replace the failed one. Before the
// new replica completes its tablet copy, the majority of tablet servers hosting
// the tablet replicas (except for the leader replica) is shutdown. The leader
// replica and the new non-voter are shutdown after the tablet copy completes.
// After that, all tablet servers except for the former leader replica's server
// are started again.
TEST_F(RaftConsensusNonVoterITest, RestartClusterWithNonVoter) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  const auto kReplicasNum = 3;
  const auto kConsensusRpcTimeout = MonoDelta::FromSeconds(5);
  const auto kTimeoutSec = 30;
  const auto kTimeout = MonoDelta::FromSeconds(kTimeoutSec);
  FLAGS_num_replicas = kReplicasNum;
  // Need some extra tablet servers to ensure the catalog manager does not
  // replace almost all the replicas, even if it could.
  FLAGS_num_tablet_servers = 2 * kReplicasNum + 1;

  const vector<string> kMasterFlags = {
    // The scenario runs with the 3-4-3 replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",
  };
  vector<string> tserver_flags = {
    // The scenario runs with the 3-4-3 replica management scheme.
    "--raft_prepare_replacement_before_eviction=true",

    // Inject delay into the tablet copying: this is to allow for shutting
    // down the all the replicas but the leader and the new non-voter before
    // the tablet copy completes.
    Substitute("--tablet_copy_download_file_inject_latency_ms=$0",
               kConsensusRpcTimeout.ToMilliseconds() * 2),

    Substitute("--follower_unavailable_considered_failed_sec=$0",
               static_cast<int>(kConsensusRpcTimeout.ToSeconds() / 2)),

    // Don't wait for the RPC timeout for too long.
    Substitute("--consensus_rpc_timeout_ms=$0",
               kConsensusRpcTimeout.ToMilliseconds()),
  };

  NO_FATALS(BuildAndStart(tserver_flags, kMasterFlags));
  ASSERT_EQ(kReplicasNum + 4, tablet_servers_.size());
  ASSERT_EQ(kReplicasNum, tablet_replicas_.size());

  // Create a test table and insert some data into the table, so the special
  // flag --tablet_copy_download_file_inject_latency_ms could take affect while
  // tablet copy happens down the road.
  TestWorkload workload(cluster_.get());
  workload.set_table_name(kTableId);
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  vector<ExternalTabletServer*> servers_without_replica;
  NO_FATALS(GetServersWithoutReplica(&servers_without_replica));
  ASSERT_EQ(4, servers_without_replica.size());

  ExternalTabletServer* ts_with_replica = GetServerWithReplica();
  ASSERT_NE(nullptr, ts_with_replica);
  ts_with_replica->Shutdown();
  const string& failed_replica_uuid = ts_with_replica->uuid();

  bool has_leader = false;
  TabletLocationsPB tablet_locations;
  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            kReplicasNum + 1,
                                            tablet_id_,
                                            kTimeout,
                                            WAIT_FOR_LEADER,
                                            ANY_REPLICA,
                                            &has_leader,
                                            &tablet_locations));
  // Find the location of the new non-voter replica.
  string new_replica_uuid;
  for (const auto& r : tablet_locations.replicas()) {
    if (r.role() != RaftPeerPB::LEARNER) {
      continue;
    }
    new_replica_uuid = r.ts_info().permanent_uuid();
    break;
  }
  ASSERT_FALSE(new_replica_uuid.empty());

  TServerDetails* ts_new_replica = FindOrDie(tablet_servers_, new_replica_uuid);
  ASSERT_NE(nullptr, ts_new_replica);

  // Shutdown all servers with tablet replicas but the leader and the non-voter.
  TServerDetails* leader = nullptr;
  ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
  const ExternalDaemon* ts_leader = cluster_->tablet_server_by_uuid(leader->uuid());

  // There should be only one tablet.
  ASSERT_EQ(tablet_replicas_.count(tablet_id_), tablet_replicas_.size());
  for (const auto& e : tablet_replicas_) {
    const auto& uuid = e.second->uuid();
    if (uuid != ts_leader->uuid() && uuid != new_replica_uuid) {
      cluster_->tablet_server_by_uuid(uuid)->Shutdown();
    }
  }

  // Wait for the newly copied replica to start.
  ASSERT_OK(WaitForNumTabletsOnTS(
      ts_new_replica, 1, kTimeout, nullptr, tablet::RUNNING));

  cluster_->Shutdown();

  // Start all tablet servers except for the server with the 'failed' replica.
  for (auto i = 0; i < cluster_->num_tablet_servers(); ++i) {
    auto* server = cluster_->tablet_server(i);
    if (server->uuid() == failed_replica_uuid) {
      continue;
    }
    ASSERT_OK(server->Restart());
  }
  for (auto i = 0; i < cluster_->num_masters(); ++i) {
    ASSERT_OK(cluster_->master(i)->Restart());
  }
  ASSERT_OK(cluster_->WaitForTabletServerCount(cluster_->num_tablet_servers() - 1,
                                               kTimeout));

  ASSERT_OK(WaitForReplicasReportedToMaster(cluster_->master_proxy(),
                                            kReplicasNum,
                                            tablet_id_,
                                            kTimeout,
                                            WAIT_FOR_LEADER,
                                            ANY_REPLICA,
                                            &has_leader,
                                            &tablet_locations));
  consensus::ConsensusStatePB cstate;
  ASSERT_EVENTUALLY([&] {
    TServerDetails* leader = nullptr;
    ASSERT_OK(GetLeaderReplicaWithRetries(tablet_id_, &leader));
    // The reason for the outside ASSERT_EVENTUALLY is that the leader might
    // have changed in between of these two calls.
    ASSERT_OK(GetConsensusState(leader, tablet_id_, kTimeout, &cstate));
  });
  // The failed voter replica should be evicted.
  EXPECT_FALSE(IsRaftConfigMember(failed_replica_uuid, cstate.committed_config()))
      << pb_util::SecureDebugString(cstate.committed_config())
      << "failed replica UUID: " << failed_replica_uuid;
  // The replacement replica should become a voter.
  EXPECT_TRUE(IsRaftConfigVoter(new_replica_uuid, cstate.committed_config()))
      << pb_util::SecureDebugString(cstate.committed_config())
      << "replacement replica UUID: " << new_replica_uuid;
}

}  // namespace tserver
}  // namespace kudu
