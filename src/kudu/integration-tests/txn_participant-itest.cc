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
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_participant-test-util.h"
#include "kudu/tablet/txn_participant.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(raft_enable_pre_election);
DECLARE_double(leader_failure_max_missed_heartbeat_periods);
DECLARE_int32(consensus_inject_latency_ms_in_notifications);
DECLARE_int32(flush_threshold_secs);
DECLARE_int32(flush_threshold_mb);
DECLARE_int32(follower_unavailable_considered_failed_sec);
DECLARE_int32(log_segment_size_mb);
DECLARE_int32(maintenance_manager_polling_interval_ms);
DECLARE_int32(raft_heartbeat_interval_ms);

METRIC_DECLARE_histogram(log_gc_duration);

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::tablet::kCommitSequence;
using kudu::tablet::kDummyCommitTimestamp;
using kudu::tablet::TabletReplica;
using kudu::tablet::Txn;
using kudu::tablet::TxnParticipant;
using kudu::tserver::ParticipantOpPB;
using kudu::tserver::ParticipantResponsePB;
using kudu::tserver::WriteRequestPB;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace itest {

namespace {
vector<Status> RunOnReplicas(const vector<TabletReplica*>& replicas,
                             int64_t txn_id,
                             ParticipantOpPB::ParticipantOpType type,
                             int64_t commit_timestamp = kDummyCommitTimestamp) {
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

string TxnsAsString(const vector<TxnParticipant::TxnEntry>& txns) {
  return JoinMapped(txns,
      [](const TxnParticipant::TxnEntry& txn) {
        return Substitute("(txn_id=$0: $1, $2)",
            txn.txn_id, Txn::StateToString(txn.state), txn.commit_timestamp);
      },
      ",");
}

Status RunOnReplica(TabletReplica* replica, int64_t txn_id,
                    ParticipantOpPB::ParticipantOpType type,
                    int64_t commit_timestamp = kDummyCommitTimestamp) {
  return RunOnReplicas({ replica }, txn_id, type, commit_timestamp)[0];
}

// Emulates a snapshot scan by waiting for the safe time to be advanced past
// 't' and for all ops before 't' to complete.
Status WaitForCompletedOps(TabletReplica* replica, Timestamp t, MonoDelta timeout) {
  const auto deadline = MonoTime::Now() + timeout;
  RETURN_NOT_OK_PREPEND(
      replica->time_manager()->WaitUntilSafe(t, deadline), "Failed to wait for safe time");
  tablet::MvccSnapshot snap;
  RETURN_NOT_OK_PREPEND(
      replica->tablet()->mvcc_manager()->WaitForSnapshotWithAllApplied(t, &snap, deadline),
      "Failed to wait for ops to complete");
  return Status::OK();
}
} // anonymous namespace

class TxnParticipantITest : public KuduTest {
 public:
  ~TxnParticipantITest() {
    STLDeleteValues(&ts_map_);
  }
  void SetUp() override {
    KuduTest::SetUp();
    InternalMiniClusterOptions opts;
    opts.num_tablet_servers = 3;
    cluster_.reset(new InternalMiniCluster(env_, std::move(opts)));
    ASSERT_OK(cluster_->Start());
    NO_FATALS(SetUpTable());
    ASSERT_OK(CreateTabletServerMap(cluster_->master_proxy(), cluster_->messenger(), &ts_map_));
  }

  // Creates a single-tablet replicated table.
  void SetUpTable(string* table_name = nullptr,
                  TestWorkload::WritePattern pattern = TestWorkload::INSERT_RANDOM_ROWS) {
    TestWorkload w(cluster_.get());
    w.set_write_pattern(pattern);
    w.Setup();
    w.Start();
    while (w.rows_inserted() < 1) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    if (table_name) {
      *table_name = w.table_name();
    }
    w.StopAndJoin();
    initial_row_count_ = w.rows_inserted();
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
  unordered_map<string, TServerDetails*> ts_map_;
  int initial_row_count_;
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

class ParticipantCopyITest : public TxnParticipantITest,
                             public ::testing::WithParamInterface<bool> {
 public:
  void SetUp() override {
    // To test the behavior with WAL GC, encourage flushing so our WALs don't
    // stay anchored for too long.
    FLAGS_flush_threshold_mb = 1;
    FLAGS_flush_threshold_secs = 1;
    FLAGS_maintenance_manager_polling_interval_ms = 10;
    // Additionally, make the WAL segments smaller to encourage more frequent
    // roll-over onto WAL segments.
    FLAGS_log_segment_size_mb = 1;
    NO_FATALS(TxnParticipantITest::SetUp());
  }
};

// Test that participant ops are copied when performing a tablet copy,
// resulting in identical transaction states on the new copy.
TEST_P(ParticipantCopyITest, TestCopyParticipantOps) {
  string table_name;
  NO_FATALS(SetUpTable(&table_name));

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

  // If we're meant to GC WALs, insert rows to the tablet until a WAL GC op
  // happens.
  if (GetParam()) {
    for (int i = 0; i < kNumTxns; i++) {
      ASSERT_TRUE(leader_replica->tablet_metadata()->HasTxnMetadata(i));
    }
    TestWorkload w(cluster_.get());
    w.set_already_present_allowed(true);
    w.set_table_name(table_name);
    w.Setup();
    w.Start();
    auto gc_ops = leader_replica->tablet()->GetMetricEntity()->FindOrCreateHistogram(
        &METRIC_log_gc_duration);
    const auto initial_gcs = gc_ops->TotalCount();
    while (gc_ops->TotalCount() == initial_gcs) {
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
    w.StopAndJoin();
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
    for (int i = 0; i < kNumTxns; i++) {
      ASSERT_TRUE(r->tablet_metadata()->HasTxnMetadata(i));
    }
  });
}
INSTANTIATE_TEST_CASE_P(ShouldGCWals, ParticipantCopyITest, ::testing::Values(true, false));

// Test to ensure that the mechanisms built to allow snapshot scans to wait for
// safe time advancement will actually wait for transactions to commit.
TEST_F(TxnParticipantITest, TestWaitOnFinalizeCommit) {
  const int kLeaderIdx = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);

  auto* leader_replica = replicas[kLeaderIdx];
  auto* follower_replica = replicas[kLeaderIdx + 1];
  auto* clock = leader_replica->clock();
  const int64_t kTxnId = 1;
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(MonoDelta::FromSeconds(10)));
  ASSERT_OK(RunOnReplica(leader_replica, kTxnId, ParticipantOpPB::BEGIN_TXN));
  const MonoDelta kAgreeTimeout = MonoDelta::FromSeconds(10);
  const auto& tablet_id = leader_replica->tablet()->tablet_id();
  ASSERT_OK(WaitForServersToAgree(kAgreeTimeout, ts_map_, tablet_id, /*minimum_index*/1));

  // We should consistently be able to scan a bit in the future just by
  // waiting on leaders. Leader safe times move forward with its clock, while
  // followers need to wait to be heartbeated to. Wait on heartbeats (wait 2x
  // the heartbeat time to avoid flakiness).
  const auto kShortTimeout = MonoDelta::FromMilliseconds(10);
  const auto before_commit_ts = clock->Now();
  ASSERT_OK(WaitForCompletedOps(leader_replica, before_commit_ts, kShortTimeout));
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms * 2));
  ASSERT_OK(WaitForCompletedOps(follower_replica, before_commit_ts, kShortTimeout));

  // Once we begin committing, safe time will be pinned. To ensure repeatable
  // reads, scans asking for a timestamp higher than the BEGIN_COMMIT op's
  // timestamp should wait.
  ASSERT_OK(RunOnReplica(leader_replica, kTxnId, ParticipantOpPB::BEGIN_COMMIT));
  ASSERT_OK(WaitForServersToAgree(kAgreeTimeout, ts_map_, tablet_id, /*minimum_index*/1));
  const auto before_finalize_ts = clock->Now();
  Status s;
  s = WaitForCompletedOps(leader_replica, before_finalize_ts, kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  s = WaitForCompletedOps(follower_replica, before_finalize_ts, kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // Even if we wait for heartbeats to happen, safe time will not be advanced
  // until the commit is finalized.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms * 2));
  s = WaitForCompletedOps(leader_replica, before_finalize_ts, kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  s = WaitForCompletedOps(follower_replica, before_finalize_ts, kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // Just because safe time is pinned doesn't mean we can't scan anything. We
  // should still be able to wait for older timestamps.
  ASSERT_OK(WaitForCompletedOps(leader_replica, before_commit_ts, kShortTimeout));
  ASSERT_OK(WaitForCompletedOps(follower_replica, before_commit_ts, kShortTimeout));

  // Once we finalize the commit, safe time will continue forward.
  const auto commit_ts_val = clock->Now().value() + 10000;
  const auto commit_ts = Timestamp(commit_ts_val);
  ASSERT_OK(RunOnReplica(leader_replica, kTxnId, ParticipantOpPB::FINALIZE_COMMIT, commit_ts_val));
  ASSERT_OK(WaitForServersToAgree(kAgreeTimeout, ts_map_, tablet_id, /*minimum_index*/1));
  ASSERT_OK(WaitForCompletedOps(leader_replica, before_finalize_ts, kShortTimeout));
  ASSERT_OK(WaitForCompletedOps(follower_replica, before_finalize_ts, kShortTimeout));
  ASSERT_OK(WaitForCompletedOps(leader_replica, commit_ts, kShortTimeout));
  ASSERT_OK(WaitForCompletedOps(follower_replica, commit_ts, kShortTimeout));
}

// Like the above test, but ensures we can wait properly even if the
// transaction is aborted.
TEST_F(TxnParticipantITest, TestWaitOnAbortCommit) {
  const int kLeaderIdx = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  auto* follower_replica = replicas[kLeaderIdx + 1];
  auto* clock = leader_replica->clock();
  const int64_t kTxnId = 1;
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(MonoDelta::FromSeconds(10)));
  ASSERT_OK(RunOnReplica(leader_replica, kTxnId, ParticipantOpPB::BEGIN_TXN));
  const MonoDelta kAgreeTimeout = MonoDelta::FromSeconds(10);
  const auto& tablet_id = leader_replica->tablet()->tablet_id();
  ASSERT_OK(WaitForServersToAgree(kAgreeTimeout, ts_map_, tablet_id, /*minimum_index*/1));

  const auto kShortTimeout = MonoDelta::FromMilliseconds(10);
  const auto before_commit_ts = clock->Now();
  // Once we begin committing, safe time will be pinned. To ensure repeatable
  // reads, scans asking for a timestamp higher than the BEGIN_COMMIT op's
  // timestamp should wait.
  ASSERT_OK(RunOnReplica(leader_replica, kTxnId, ParticipantOpPB::BEGIN_COMMIT));
  ASSERT_OK(WaitForServersToAgree(kAgreeTimeout, ts_map_, tablet_id, /*minimum_index*/1));
  const auto before_abort_ts = clock->Now();
  Status s = WaitForCompletedOps(leader_replica, before_abort_ts, kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  s = WaitForCompletedOps(follower_replica, before_abort_ts, kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // Just because safe time is pinned doesn't mean we can't scan anything. We
  // should still be able to wait for older timestamps.
  ASSERT_OK(WaitForCompletedOps(leader_replica, before_commit_ts, kShortTimeout));
  ASSERT_OK(WaitForCompletedOps(follower_replica, before_commit_ts, kShortTimeout));

  // When we abort the transaction, safe time should continue to move forward.
  // On followers, this will happen via heartbeats, so we need to wait to
  // heartbeat (wait 2x the heartbeat time to avoid flakiness).
  ASSERT_OK(RunOnReplica(leader_replica, kTxnId, ParticipantOpPB::ABORT_TXN));
  ASSERT_OK(WaitForServersToAgree(kAgreeTimeout, ts_map_, tablet_id, /*minimum_index*/1));
  ASSERT_OK(WaitForCompletedOps(leader_replica, before_abort_ts, kShortTimeout));
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms * 2));
  ASSERT_OK(WaitForCompletedOps(follower_replica, before_abort_ts, kShortTimeout));
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
    // We'll continue writing from where we left off, so write in order.
    NO_FATALS(SetUpTable(nullptr, TestWorkload::INSERT_SEQUENTIAL_ROWS));
  }
};

TEST_F(TxnParticipantElectionStormITest, TestFrequentElections) {
  vector<TabletReplica*> replicas;
  const string tablet_id = cluster_->mini_tablet_server(0)->ListTablets()[0];
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    scoped_refptr<TabletReplica> r;
    ASSERT_TRUE(cluster_->mini_tablet_server(i)->server()->tablet_manager()->LookupTablet(
        tablet_id, &r));
    replicas.emplace_back(r.get());
  }

  const auto write = [&] (int64_t txn_id, int row_id, TabletReplica* replica) {
    WriteRequestPB req;
    req.set_txn_id(txn_id);
    req.set_tablet_id(tablet_id);
    const auto& schema = GetSimpleTestSchema();
    RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
    KuduPartialRow row(&schema);
    RETURN_NOT_OK(row.SetInt32(0, row_id));
    RETURN_NOT_OK(row.SetInt32(1, row_id));
    RowOperationsPBEncoder enc(req.mutable_row_operations());
    enc.Add(RowOperationsPB::INSERT, row);
    return tablet::TabletReplicaTestBase::ExecuteWrite(replica, req);
  };

  // Inject latency so elections become more frequent and wait a bit for our
  // latency injection to kick in.
  FLAGS_raft_enable_pre_election = false;
  FLAGS_consensus_inject_latency_ms_in_notifications = 1.5 * FLAGS_raft_heartbeat_interval_ms;;
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms * 2));

  constexpr const int kNumTxns = 10;
  vector<ParticipantOpPB::ParticipantOpType> last_successful_ops_per_txn(kNumTxns);
  vector<int> num_successful_writes_per_txn(kNumTxns);
  for (int txn_id = 0; txn_id < kNumTxns; txn_id++) {
    // Send some writes in the background and keep track of the number of
    // successful rows.
    CountDownLatch prt_ops_done(1);
    thread writers_thread([&] {
      int num_successful_writes = 0;
      for (int cur_row = initial_row_count_; prt_ops_done.count() > 0; cur_row++) {
        vector<Status> statuses(cluster_->num_tablet_servers());
        vector<thread> write_threads;
        for (int r = 0; r < cluster_->num_tablet_servers(); r++) {
          write_threads.emplace_back([&, r, cur_row] {
            statuses[r] = write(txn_id, cur_row, replicas[r]);
          });
        }
        for (auto& wt : write_threads) {
          wt.join();
        }
        for (const auto& s : statuses) {
          if (s.ok()) {
            num_successful_writes++;
            break;
          }
        }
      }
      num_successful_writes_per_txn[txn_id] = num_successful_writes;
    });

    // Send a participant op to all replicas concurrently. Leaders should
    // attempt to replicate, and followers should immediately reject the
    // request. There will be a real chance that the leader op will not be
    // successfully replicated because of a leadership change.
    ParticipantOpPB::ParticipantOpType last_successful_op = ParticipantOpPB::UNKNOWN;;
    for (const auto& op : kCommitSequence) {
      vector<Status> statuses = RunOnReplicas(replicas, txn_id, op);
      // If this op succeeded on any replica, keep track of it -- it indicates
      // what the final state of each transaction should be.
      for (const auto& s : statuses) {
        if (s.ok()) {
          last_successful_op = op;
        }
      }
      last_successful_ops_per_txn[txn_id] = last_successful_op;
    }
    prt_ops_done.CountDown();
    writers_thread.join();
  }
  // Validate that each replica has each transaction in the appropriate state.
  vector<TxnParticipant::TxnEntry> expected_txns;
  int expected_rows = initial_row_count_;
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
        expected_rows += num_successful_writes_per_txn[txn_id];
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
      const auto& actual_txns = replicas[i]->tablet()->txn_participant()->GetTxnsForTests();
      ASSERT_EQ(expected_txns, actual_txns)
          << Substitute("Expected: $0,\nActual: $1",
                        TxnsAsString(expected_txns), TxnsAsString(actual_txns));
    });
  }
  for (int i = 0; i < replicas.size(); i++) {
    ASSERT_EQ(expected_rows, replicas[i]->CountLiveRowsNoFail());
  }

  // Now restart the cluster and ensure the transaction state is restored.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->StartSync());
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    auto* ts = cluster_->mini_tablet_server(i);
    const auto& tablets = ts->ListTablets();
    scoped_refptr<TabletReplica> r;
    ASSERT_TRUE(ts->server()->tablet_manager()->LookupTablet(tablets[0], &r));
    ASSERT_OK(r->WaitUntilConsensusRunning(MonoDelta::FromSeconds(10)));
    auto actual_txns = r->tablet()->txn_participant()->GetTxnsForTests();
    // Upon bootstrapping, we may end up replaying a REPLICATE message with no
    // COMMIT message, starting it as a follower op. If it's a BEGIN_TXN op,
    // this leaves us with an initialized Txn that isn't in the expected set,
    // as it was completed on a majority. The Txn is benign: either it will be
    // replicated on a majority and will complete, leaving the Txn as kOpen; or
    // it doesn't, and the op will be aborted by the next leader, removing the
    // Txn. Ignore such Txns and just assert the ones we know to have
    // successfully initialized.
    vector<TxnParticipant::TxnEntry> actual_txns_not_initting;
    for (const auto& txn : actual_txns) {
      if (txn.state != Txn::kInitializing) {
        actual_txns_not_initting.emplace_back(txn);
      }
    }
    ASSERT_EQ(expected_txns, actual_txns_not_initting)
        << Substitute("Expected: $0,\nActual: $1",
                      TxnsAsString(expected_txns),
                      TxnsAsString(actual_txns_not_initting));
    ASSERT_EQ(expected_rows, r->CountLiveRowsNoFail());
  }
}

} // namespace itest
} // namespace kudu
