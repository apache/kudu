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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_participant-test-util.h"
#include "kudu/tablet/txn_participant.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(raft_enable_pre_election);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(follower_unavailable_considered_failed_sec);
DECLARE_int32(raft_heartbeat_interval_ms);

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::tablet::kCommitSequence;
using kudu::tablet::kDummyCommitTimestamp;
using kudu::tablet::TabletReplica;
using kudu::tablet::Txn;
using kudu::tablet::TxnParticipant;
using kudu::tserver::ParticipantOpPB;
using kudu::tserver::ParticipantRequestPB;
using kudu::tserver::ParticipantResponsePB;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

namespace {
vector<Status> RunOnReplicas(const vector<TabletReplica*>& replicas,
                             int64_t txn_id,
                             ParticipantOpPB::ParticipantOpType type) {
  vector<Status> statuses(replicas.size(), Status::Incomplete(""));
  vector<thread> threads;
  for (int i = 0; i < replicas.size(); i++) {
    threads.emplace_back([&, i] {
      ParticipantResponsePB resp;
      statuses[i] = CallParticipantOp(replicas[i], txn_id, type, kDummyCommitTimestamp, &resp);
      if (resp.has_error()) {
        DCHECK_OK(statuses[i]);
        statuses[i] = StatusFromPB(resp.error().status());
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  return statuses;
}
} // anonymous namespace

class TxnParticipantITest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    NO_FATALS(SetUpTable());
  }

  // Creates a single-tablet replicated table.
  void SetUpTable() {
    TestWorkload w(cluster_.get());
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 1) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    w.StopAndJoin();
  }

  // Quiesces servers such that the tablet server at index 'ts_idx' is set up
  // to become leader. Returns a list of all TabletReplicas.
  vector<TabletReplica*> SetUpLeaderGetReplicas(int ts_idx) {
    vector<TabletReplica*> replicas;
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      auto* ts = cluster_->mini_tablet_server(i);
      const auto& tablets = ts->ListTablets();
      CHECK_EQ(1, tablets.size());
      scoped_refptr<TabletReplica> r;
      CHECK(ts->server()->tablet_manager()->LookupTablet(tablets[0], &r));
      replicas.emplace_back(r.get());
      if (i != ts_idx) {
        *ts->server()->mutable_quiescing() = true;
      }
    }
    return replicas;
  }

 protected:
  unique_ptr<InternalMiniCluster> cluster_;
};

// Test that participant ops only applied to followers via replication from
// leaders.
TEST_F(TxnParticipantITest, TestReplicateParticipantOps) {
  // Keep track of all the participant replicas, and quiesce all but one
  // tserver so we can ensure a specific leader.
  const int kLeaderIdx = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  ASSERT_OK(replicas[kLeaderIdx]->consensus()->WaitUntilLeaderForTests(MonoDelta::FromSeconds(10)));
  // Try submitting the ops on all replicas. They should succeed on the leaders
  // and fail on followers.
  const int64_t kTxnId = 1;
  for (const auto& op : kCommitSequence) {
    vector<Status> statuses = RunOnReplicas(replicas, kTxnId, op);
    for (int i = 0; i < statuses.size(); i++) {
      const auto& s = statuses[i];
      if (i == kLeaderIdx) {
        ASSERT_OK(s);
      } else {
        ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
        ASSERT_STR_CONTAINS(s.ToString(), "not leader of this config");
      }
    }
  }

  // Attempt to make calls on just the followers.
  vector<TabletReplica*> followers;
  for (int i = 0; i < replicas.size(); i++) {
    if (i != kLeaderIdx) {
      followers.emplace_back(replicas[i]);
    }
  }
  // Try with a transaction ID we've already tried, and with a new one. Both
  // should fail because we aren't leader.
  for (int txn_id = kTxnId; txn_id <= kTxnId + 1; txn_id++) {
    for (const auto& op : kCommitSequence) {
      vector<Status> statuses = RunOnReplicas(followers, txn_id, op);
      for (int i = 0; i < statuses.size(); i++) {
        const auto& s = statuses[i];
        ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
        ASSERT_STR_CONTAINS(s.ToString(), "not leader of this config");
      }
    }
  }
  // Now try just on the leader. This should succeed.
  for (const auto& op : kCommitSequence) {
    vector<Status> statuses = RunOnReplicas({ replicas[kLeaderIdx] }, kTxnId + 1, op);
    ASSERT_EQ(1, statuses.size());
    ASSERT_OK(statuses[0]);
  }
}

// Test that participant ops are copied when performing a tablet copy,
// resulting in identical transaction states on the new copy.
TEST_F(TxnParticipantITest, TestCopyParticipantOps) {
  NO_FATALS(SetUpTable());

  constexpr const int kNumTxns = 10;
  constexpr const int kLeaderIdx = 0;
  constexpr const int kDeadServerIdx = kLeaderIdx + 1;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(10);
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kTimeout));

  // Apply some operations.
  vector<TxnParticipant::TxnEntry> expected_txns;
  for (int i = 0; i < kNumTxns; i++) {
    for (const auto& op : kCommitSequence) {
      vector<Status> statuses = RunOnReplicas({ leader_replica }, i, op);
      for (const auto& s : statuses) {
        SCOPED_TRACE(Substitute("Transaction $0, Op $1", i,
                                ParticipantOpPB::ParticipantOpType_Name(op)));
        ASSERT_OK(s);
      }
    }
    expected_txns.emplace_back(
        TxnParticipant::TxnEntry({ i, Txn::kCommitted, kDummyCommitTimestamp }));
  }
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(expected_txns, replicas[i]->tablet()->txn_participant()->GetTxnsForTests());
    });
  }

  // Set ourselves up to make a copy.
  cluster_->mini_tablet_server(kDeadServerIdx)->Shutdown();
  ASSERT_OK(cluster_->AddTabletServer());
  FLAGS_follower_unavailable_considered_failed_sec = 1;

  // Eventually, a copy should be made on the new server.
  ASSERT_EVENTUALLY([&] {
    auto* new_ts = cluster_->mini_tablet_server(cluster_->num_tablet_servers() - 1);
    const auto& tablets = new_ts->ListTablets();
    ASSERT_EQ(1, tablets.size());
    scoped_refptr<TabletReplica> r;
    ASSERT_TRUE(new_ts->server()->tablet_manager()->LookupTablet(tablets[0], &r));
    ASSERT_OK(r->WaitUntilConsensusRunning(kTimeout));
    ASSERT_EQ(expected_txns, r->tablet()->txn_participant()->GetTxnsForTests());
  });
}

class TxnParticipantElectionStormITest : public TxnParticipantITest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    // Make leader elections more frequent to get through this test a bit more
    // quickly.
    FLAGS_leader_failure_max_missed_heartbeat_periods = 1;
    FLAGS_raft_heartbeat_interval_ms = 30;
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    NO_FATALS(SetUpTable());
  }
};

TEST_F(TxnParticipantElectionStormITest, TestFrequentElections) {
  vector<TabletReplica*> replicas;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    auto* ts = cluster_->mini_tablet_server(i);
    const auto& tablets = ts->ListTablets();
    scoped_refptr<TabletReplica> r;
    ASSERT_TRUE(ts->server()->tablet_manager()->LookupTablet(tablets[0], &r));
    replicas.emplace_back(r.get());
  }
  // Inject latency so elections become more frequent and wait a bit for our
  // latency injection to kick in.
  FLAGS_raft_enable_pre_election = false;
  FLAGS_consensus_inject_latency_ms_in_notifications = 1.5 * FLAGS_raft_heartbeat_interval_ms;;
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms * 2));

  // Send a participant op to all replicas concurrently. Leaders should attempt
  // to replicate, and followers should immediately reject the request. There
  // will be a real chance that the leader op will not be successfully
  // replicated because of an election change.
  constexpr const int kNumTxns = 10;
  vector<ParticipantOpPB::ParticipantOpType> last_successful_ops_per_txn(kNumTxns);
  for (int txn_id = 0; txn_id < kNumTxns; txn_id++) {
    ParticipantOpPB::ParticipantOpType last_successful_op = ParticipantOpPB::UNKNOWN;;
    for (const auto& op : kCommitSequence) {
      vector<thread> threads;
      vector<Status> statuses(cluster_->num_tablet_servers(), Status::Incomplete(""));
      for (int r = 0; r < cluster_->num_tablet_servers(); r++) {
        threads.emplace_back([&, r] {
          ParticipantResponsePB resp;
          Status s = CallParticipantOp(replicas[r], txn_id, op, kDummyCommitTimestamp, &resp);
          if (resp.has_error()) {
            s = StatusFromPB(resp.error().status());
          }
          statuses[r] = s;
        });
      }
      for (auto& t : threads) {
        t.join();
      }
      // If this op succeeded on any replica, keep track of it -- it indicates
      // what the final state of each transaction should be.
      for (const auto& s : statuses) {
        if (s.ok()) {
          last_successful_op = op;
        }
      }
      last_successful_ops_per_txn[txn_id] = last_successful_op;
    }
  }
  // Validate that each replica has each transaction in the appropriate state.
  vector<TxnParticipant::TxnEntry> expected_txns;
  for (int txn_id = 0; txn_id < kNumTxns; txn_id++) {
    const auto& last_successful_op = last_successful_ops_per_txn[txn_id];
    if (last_successful_op == ParticipantOpPB::UNKNOWN) {
      continue;
    }
    Txn::State expected_state;
    switch (last_successful_op) {
      case ParticipantOpPB::BEGIN_TXN:
        expected_state = Txn::kOpen;
        break;
      case ParticipantOpPB::BEGIN_COMMIT:
        expected_state = Txn::kCommitInProgress;
        break;
      case ParticipantOpPB::FINALIZE_COMMIT:
        expected_state = Txn::kCommitted;
        break;
      default:
        FAIL() << "Unexpected successful op " << last_successful_op;
    }
    expected_txns.emplace_back(TxnParticipant::TxnEntry({
        txn_id, expected_state, expected_state == Txn::kCommitted ? kDummyCommitTimestamp : -1}));
  }
  for (int i = 0; i < replicas.size(); i++) {
    // NOTE: We ASSERT_EVENTUALLY here because having completed the participant
    // op only guarantees successful replication on a majority. We need to wait
    // a bit for the state to fully quiesce.
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(expected_txns, replicas[i]->tablet()->txn_participant()->GetTxnsForTests());
    });
  }

  // Now restart the cluster and ensure the transaction state is restored.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->StartSync());
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    auto* ts = cluster_->mini_tablet_server(i);
    const auto& tablets = ts->ListTablets();
    scoped_refptr<TabletReplica> r;
    ASSERT_TRUE(ts->server()->tablet_manager()->LookupTablet(tablets[0], &r));
    ASSERT_EQ(expected_txns, r->tablet()->txn_participant()->GetTxnsForTests());
  }
}

} // namespace itest
} // namespace kudu
