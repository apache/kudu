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
#include <initializer_list>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
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
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_participant-test-util.h"
#include "kudu/tablet/txn_participant.h"
#include "kudu/transactions/txn_system_client.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
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
DECLARE_int32(tablet_bootstrap_inject_latency_ms);

METRIC_DECLARE_histogram(log_gc_duration);

using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::tablet::kAborted;
using kudu::tablet::kAbortSequence;
using kudu::tablet::kCommitSequence;
using kudu::tablet::kDummyCommitTimestamp;
using kudu::tablet::kCommitted;
using kudu::tablet::kCommitInProgress;
using kudu::tablet::kInitializing;
using kudu::tablet::kOpen;
using kudu::tablet::MakeParticipantOp;
using kudu::tablet::TabletReplica;
using kudu::tablet::TxnState;
using kudu::tablet::TxnParticipant;
using kudu::tablet::TxnState;
using kudu::transactions::TxnSystemClient;
using kudu::tserver::ParticipantOpPB;
using kudu::tserver::ParticipantRequestPB;
using kudu::tserver::ParticipantResponsePB;
using kudu::tserver::TabletServerAdminServiceProxy;
using kudu::tserver::TabletServerErrorPB;
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
const MonoDelta kDefaultTimeout = MonoDelta::FromSeconds(10);
const MonoDelta kLongTimeout = MonoDelta::FromSeconds(30);
ParticipantRequestPB ParticipantRequest(const string& tablet_id, int64_t txn_id,
                                        ParticipantOpPB::ParticipantOpType type) {
  ParticipantRequestPB req;
  req.set_tablet_id(tablet_id);
  auto* op_pb = req.mutable_op();
  op_pb->set_txn_id(txn_id);
  op_pb->set_type(type);
  if (type == ParticipantOpPB::FINALIZE_COMMIT) {
    op_pb->set_finalized_commit_timestamp(kDummyCommitTimestamp);
  }
  return req;
}

Status ParticipateInTransaction(TabletServerAdminServiceProxy* admin_proxy,
                                const string& tablet_id, int64_t txn_id,
                                ParticipantOpPB::ParticipantOpType type,
                                ParticipantResponsePB* resp) {
  rpc::RpcController rpc;
  return admin_proxy->ParticipateInTransaction(
      ParticipantRequest(tablet_id, txn_id, type), resp, &rpc);
}

Status ParticipateInTransactionCheckResp(TabletServerAdminServiceProxy* admin_proxy,
                                         const string& tablet_id, int64_t txn_id,
                                         ParticipantOpPB::ParticipantOpType type,
                                         TabletServerErrorPB::Code* code = nullptr,
                                         Timestamp* begin_commit_ts = nullptr) {
  ParticipantResponsePB resp;
  RETURN_NOT_OK(ParticipateInTransaction(admin_proxy, tablet_id, txn_id, type, &resp));
  if (begin_commit_ts) {
    *begin_commit_ts = Timestamp(resp.timestamp());
  }
  if (resp.has_error()) {
    if (code) {
      *code = resp.error().code();
    }
    return StatusFromPB(resp.error().status());
  }
  return Status::OK();
}

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
            txn.txn_id, TxnStateToString(txn.state), txn.commit_timestamp);
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
      SleepFor(kDefaultTimeout);
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

  // Stops quiescing all tablet servers, allowing a new leader to be elected if
  // necessary.
  void StopQuiescingServers() {
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      *cluster_->mini_tablet_server(i)->server()->mutable_quiescing() = false;
    }
  }

  // Ensures that every replica has the same set of transactions.
  void CheckReplicasMatchTxns(const vector<TabletReplica*> replicas,
                              vector<TxnParticipant::TxnEntry> txns) {
    DCHECK(!replicas.empty());
    const auto& tablet_id = replicas[0]->tablet_id();
    for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
      DCHECK_EQ(tablet_id, replicas[i]->tablet_id());
      ASSERT_EVENTUALLY([&] {
        ASSERT_EQ(txns, replicas[i]->tablet()->txn_participant()->GetTxnsForTests());
      });
    }
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
  ASSERT_OK(replicas[kLeaderIdx]->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
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
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));

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
        TxnParticipant::TxnEntry({ i, kCommitted, kDummyCommitTimestamp }));
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
    ASSERT_OK(r->WaitUntilConsensusRunning(kDefaultTimeout));
    ASSERT_EQ(expected_txns, r->tablet()->txn_participant()->GetTxnsForTests());
    for (int i = 0; i < kNumTxns; i++) {
      ASSERT_TRUE(r->tablet_metadata()->HasTxnMetadata(i));
    }
  });
}
INSTANTIATE_TEST_SUITE_P(ShouldGCWals, ParticipantCopyITest, ::testing::Values(true, false));

// Test to ensure that the mechanisms built to allow snapshot scans to wait for
// safe time advancement will actually wait for transactions to commit.
TEST_F(TxnParticipantITest, TestWaitOnFinalizeCommit) {
  const int kLeaderIdx = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);

  auto* leader_replica = replicas[kLeaderIdx];
  auto* follower_replica = replicas[kLeaderIdx + 1];
  auto* clock = leader_replica->clock();
  const int64_t kTxnId = 1;
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  ASSERT_OK(RunOnReplica(leader_replica, kTxnId, ParticipantOpPB::BEGIN_TXN));
  const MonoDelta kAgreeTimeout = kDefaultTimeout;
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
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  ASSERT_OK(RunOnReplica(leader_replica, kTxnId, ParticipantOpPB::BEGIN_TXN));
  const MonoDelta kAgreeTimeout = kDefaultTimeout;
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

TEST_F(TxnParticipantITest, TestProxyBasicCalls) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kTxnId = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  ASSERT_OK(replicas[kLeaderIdx]->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  auto admin_proxy = cluster_->tserver_admin_proxy(kLeaderIdx);
  for (const auto& op : kCommitSequence) {
    const auto req = ParticipantRequest(replicas[kLeaderIdx]->tablet_id(), kTxnId, op);
    ParticipantResponsePB resp;
    rpc::RpcController rpc;
    ASSERT_OK(admin_proxy->ParticipateInTransaction(req, &resp, &rpc));
  }
}

TEST_F(TxnParticipantITest, TestBeginCommitAfterFinalize) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kTxnId = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  ASSERT_OK(replicas[kLeaderIdx]->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  auto admin_proxy = cluster_->tserver_admin_proxy(kLeaderIdx);
  const auto tablet_id = replicas[kLeaderIdx]->tablet_id();
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    ASSERT_OK(ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_TXN, &code));
    ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, code);
  }
  // Commit the transaction.
  Timestamp begin_commit_ts;
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    ASSERT_OK(ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_COMMIT,
        &code, &begin_commit_ts));
    ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, code);
    ASSERT_NE(Timestamp::kInvalidTimestamp, begin_commit_ts);
  }
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    ASSERT_OK(ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::FINALIZE_COMMIT, &code));
    ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, code);
  }
  // A call to BEGIN_COMMIT should yield the finalized commit timestamp.
  Timestamp refetched_begin_commit_ts;
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_COMMIT, &code,
        &refetched_begin_commit_ts);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_OP_ALREADY_APPLIED, code);
    ASSERT_NE(Timestamp::kInvalidTimestamp, refetched_begin_commit_ts);
    ASSERT_EQ(Timestamp(kDummyCommitTimestamp), refetched_begin_commit_ts);
  }
}

TEST_F(TxnParticipantITest, TestProxyErrorWhenNotBegun) {
  constexpr const int kLeaderIdx = 0;
  auto txn_id = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  ASSERT_OK(replicas[kLeaderIdx]->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  auto admin_proxy = cluster_->tserver_admin_proxy(kLeaderIdx);
  const auto tablet_id = replicas[kLeaderIdx]->tablet_id();
  for (auto type : { ParticipantOpPB::BEGIN_COMMIT,
                     ParticipantOpPB::FINALIZE_COMMIT }) {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, txn_id++, type, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_ILLEGAL_STATE, code);
  }
  TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
  ASSERT_OK(ParticipateInTransactionCheckResp(
      admin_proxy.get(), tablet_id, txn_id++, ParticipantOpPB::ABORT_TXN, &code));
  ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, code);
}

TEST_F(TxnParticipantITest, TestProxyIllegalStatesInCommitSequence) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kTxnId = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  ASSERT_OK(replicas[kLeaderIdx]->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  auto admin_proxy = cluster_->tserver_admin_proxy(kLeaderIdx);

  // Begin after already beginning.
  const auto tablet_id = replicas[kLeaderIdx]->tablet_id();
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    ASSERT_OK(ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_TXN, &code));
    ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, code);
  }
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_TXN, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_OP_ALREADY_APPLIED, code);
  }

  // We can't finalize the commit without beginning to commit first.
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::FINALIZE_COMMIT, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_ILLEGAL_STATE, code);
  }

  // Start committing and ensure we can't start another transaction.
  Timestamp begin_commit_ts;
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    ASSERT_OK(ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_COMMIT,
        &code, &begin_commit_ts));
    ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, code);
    ASSERT_NE(Timestamp::kInvalidTimestamp, begin_commit_ts);
  }
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Timestamp refetched_begin_commit_ts;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_COMMIT,
        &code, &refetched_begin_commit_ts);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_OP_ALREADY_APPLIED, code);
    ASSERT_EQ(begin_commit_ts, refetched_begin_commit_ts);
  }
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_TXN, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_ILLEGAL_STATE, code);
  }

  // Finalize the commit and ensure we can't begin or abort.
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    ASSERT_OK(ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::FINALIZE_COMMIT, &code));
    ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, code);
  }
  for (auto type : { ParticipantOpPB::BEGIN_COMMIT, ParticipantOpPB::FINALIZE_COMMIT }) {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, type, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_OP_ALREADY_APPLIED, code);
  }
  for (auto type : { ParticipantOpPB::BEGIN_TXN,
                     ParticipantOpPB::ABORT_TXN }) {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, type, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_ILLEGAL_STATE, code);
  }
}

TEST_F(TxnParticipantITest, TestProxyIllegalStatesInAbortSequence) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kTxnId = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  ASSERT_OK(replicas[kLeaderIdx]->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  auto admin_proxy = cluster_->tserver_admin_proxy(kLeaderIdx);

  // Try our illegal ops when our transaction is open.
  const auto tablet_id = replicas[kLeaderIdx]->tablet_id();
  ASSERT_OK(ParticipateInTransactionCheckResp(
      admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_TXN));
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::BEGIN_TXN, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_OP_ALREADY_APPLIED, code);
  }
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::FINALIZE_COMMIT, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_ILLEGAL_STATE, code);
  }

  // Abort the transaction and ensure we can do nothing else.
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    ASSERT_OK(ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::ABORT_TXN, &code));
    ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, code);
  }
  {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, ParticipantOpPB::ABORT_TXN, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_OP_ALREADY_APPLIED, code);
  }
  for (auto type : { ParticipantOpPB::FINALIZE_COMMIT,
                     ParticipantOpPB::BEGIN_TXN,
                     ParticipantOpPB::BEGIN_COMMIT}) {
    TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
    Status s = ParticipateInTransactionCheckResp(
        admin_proxy.get(), tablet_id, kTxnId, type, &code);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    ASSERT_EQ(TabletServerErrorPB::TXN_ILLEGAL_STATE, code);
  }
}

TEST_F(TxnParticipantITest, TestProxyNonLeader) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kNonLeaderIdx = kLeaderIdx + 1;
  constexpr const int kTxnId = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  ASSERT_OK(replicas[kLeaderIdx]->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  auto admin_proxy = cluster_->tserver_admin_proxy(kNonLeaderIdx);
  for (const auto& op : kCommitSequence) {
    const auto req = ParticipantRequest(replicas[kLeaderIdx]->tablet_id(), kTxnId, op);
    ParticipantResponsePB resp;
    rpc::RpcController rpc;
    ASSERT_OK(admin_proxy->ParticipateInTransaction(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_error());
    auto resp_error = StatusFromPB(resp.error().status());
    ASSERT_TRUE(resp_error.IsIllegalState()) << resp_error.ToString();
    ASSERT_STR_CONTAINS(resp_error.ToString(), "not leader");
  }
}

TEST_F(TxnParticipantITest, TestProxyTabletBootstrapping) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kTxnId = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));

  FLAGS_tablet_bootstrap_inject_latency_ms = 1000;
  cluster_->mini_tablet_server(kLeaderIdx)->Shutdown();
  ASSERT_OK(cluster_->mini_tablet_server(kLeaderIdx)->Restart());
  replicas = SetUpLeaderGetReplicas(kLeaderIdx);

  auto admin_proxy = cluster_->tserver_admin_proxy(kLeaderIdx);
  for (const auto& op : kCommitSequence) {
    const auto req = ParticipantRequest(replicas[kLeaderIdx]->tablet_id(), kTxnId, op);
    ParticipantResponsePB resp;
    rpc::RpcController rpc;
    ASSERT_OK(admin_proxy->ParticipateInTransaction(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_error());
    auto resp_error = StatusFromPB(resp.error().status());
    ASSERT_TRUE(resp_error.IsIllegalState()) << resp_error.ToString();
    ASSERT_STR_CONTAINS(resp_error.ToString(), "not RUNNING");
  }
}

TEST_F(TxnParticipantITest, TestProxyTabletNotRunning) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kTxnId = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  auto* tablet_manager = cluster_->mini_tablet_server(kLeaderIdx)->server()->tablet_manager();
  ASSERT_OK(tablet_manager->DeleteTablet(leader_replica->tablet_id(),
      tablet::TABLET_DATA_TOMBSTONED, boost::none));

  auto admin_proxy = cluster_->tserver_admin_proxy(kLeaderIdx);
  for (const auto& op : kCommitSequence) {
    const auto req = ParticipantRequest(replicas[kLeaderIdx]->tablet_id(), kTxnId, op);
    ParticipantResponsePB resp;
    rpc::RpcController rpc;
    ASSERT_OK(admin_proxy->ParticipateInTransaction(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_error());
    auto resp_error = StatusFromPB(resp.error().status());
    ASSERT_TRUE(resp_error.IsIllegalState()) << resp_error.ToString();
    ASSERT_STR_CONTAINS(resp_error.ToString(), "not RUNNING");
  }
}

TEST_F(TxnParticipantITest, TestProxyTabletNotFound) {
  constexpr const int kTxnId = 0;
  auto admin_proxy = cluster_->tserver_admin_proxy(0);
  for (const auto& op : kCommitSequence) {
    const auto req = ParticipantRequest("dummy-tablet-id", kTxnId, op);
    ParticipantResponsePB resp;
    rpc::RpcController rpc;
    ASSERT_OK(admin_proxy->ParticipateInTransaction(req, &resp, &rpc));
    ASSERT_TRUE(resp.has_error());
    auto resp_error = StatusFromPB(resp.error().status());
    ASSERT_TRUE(resp_error.IsNotFound()) << resp_error.ToString();
    ASSERT_STR_CONTAINS(resp_error.ToString(), "not found");
  }
}

TEST_F(TxnParticipantITest, TestTxnSystemClientCommitSequence) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kTxnId = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  const auto tablet_id = leader_replica->tablet_id();
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));

  // Start a transaction and make sure it results in the expected state
  // server-side.
  unique_ptr<TxnSystemClient> txn_client;
  ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_client));
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN), kDefaultTimeout));
  NO_FATALS(CheckReplicasMatchTxns(replicas, { { kTxnId, kOpen, -1 } }));

  // Try some illegal ops and ensure we get an error.
  Status s = txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT), kDefaultTimeout);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  NO_FATALS(CheckReplicasMatchTxns(replicas, { { kTxnId, kOpen, -1 } }));

  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN), kDefaultTimeout));
  NO_FATALS(CheckReplicasMatchTxns(replicas, { { kTxnId, kOpen, -1 } }));

  // Progress the transaction forward, and perform similar checks that we get
  // errors when we attempt illegal ops.
  Timestamp begin_commit_ts;
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnId, ParticipantOpPB::BEGIN_COMMIT),
      kDefaultTimeout, &begin_commit_ts));
  ASSERT_NE(Timestamp::kInvalidTimestamp, begin_commit_ts);
  NO_FATALS(CheckReplicasMatchTxns(replicas, { { kTxnId, kCommitInProgress, -1 } }));

  s = txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN), kDefaultTimeout);
  ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  NO_FATALS(CheckReplicasMatchTxns(replicas, { { kTxnId, kCommitInProgress, -1 } }));

  // But we should be able to BEGIN_COMMIT again and get back the same
  // timestamp.
  Timestamp refetched_begin_commit_ts;
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnId, ParticipantOpPB::BEGIN_COMMIT),
      kDefaultTimeout, &refetched_begin_commit_ts));
  ASSERT_EQ(refetched_begin_commit_ts, begin_commit_ts);
  NO_FATALS(CheckReplicasMatchTxns(replicas, { { kTxnId, kCommitInProgress, -1 } }));

  // Once we finish committing, we should be unable to begin or abort.
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT, kDummyCommitTimestamp),
      kDefaultTimeout));
  NO_FATALS(CheckReplicasMatchTxns(replicas, {{kTxnId, kCommitted, kDummyCommitTimestamp}}));
  for (const auto type : { ParticipantOpPB::BEGIN_TXN, ParticipantOpPB::ABORT_TXN }) {
    Status s = txn_client->ParticipateInTransaction(
        tablet_id, MakeParticipantOp(kTxnId, type), kDefaultTimeout);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  }
  NO_FATALS(CheckReplicasMatchTxns(replicas, {{kTxnId, kCommitted, kDummyCommitTimestamp}}));
  for (const auto type : { ParticipantOpPB::BEGIN_COMMIT, ParticipantOpPB::FINALIZE_COMMIT }) {
    ASSERT_OK(txn_client->ParticipateInTransaction(
        tablet_id, MakeParticipantOp(kTxnId, type, kDummyCommitTimestamp),
        kDefaultTimeout));
  }
  NO_FATALS(CheckReplicasMatchTxns(replicas, {{kTxnId, kCommitted, kDummyCommitTimestamp}}));
}

TEST_F(TxnParticipantITest, TestTxnSystemClientAbortSequence) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kTxnOne = 0;
  constexpr const int kTxnTwo = 1;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  const auto tablet_id = leader_replica->tablet_id();
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  unique_ptr<TxnSystemClient> txn_client;
  ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_client));
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnOne, ParticipantOpPB::BEGIN_TXN),
      kDefaultTimeout));
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnTwo, ParticipantOpPB::BEGIN_TXN),
      kDefaultTimeout));
  NO_FATALS(CheckReplicasMatchTxns(replicas, { { kTxnOne, kOpen, -1 }, { kTxnTwo, kOpen, -1 } }));

  // Once we abort, we should be unable to do anything further.
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnOne, ParticipantOpPB::ABORT_TXN),
      kDefaultTimeout));
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnTwo, ParticipantOpPB::BEGIN_COMMIT),
      kDefaultTimeout));
  NO_FATALS(CheckReplicasMatchTxns(replicas,
      { { kTxnOne, kAborted, -1 }, { kTxnTwo, kCommitInProgress, -1 } }));

  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnTwo, ParticipantOpPB::ABORT_TXN),
      kDefaultTimeout));
  NO_FATALS(CheckReplicasMatchTxns(replicas,
      { { kTxnOne, kAborted, -1 }, { kTxnTwo, kAborted, -1 } }));
  for (const auto type : { ParticipantOpPB::BEGIN_TXN, ParticipantOpPB::BEGIN_COMMIT,
                           ParticipantOpPB::FINALIZE_COMMIT }) {
    Status s = txn_client->ParticipateInTransaction(
        tablet_id, MakeParticipantOp(kTxnOne, type), kDefaultTimeout);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    s = txn_client->ParticipateInTransaction(
        tablet_id, MakeParticipantOp(kTxnTwo, type), kDefaultTimeout);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
  }
  NO_FATALS(CheckReplicasMatchTxns(replicas,
      { { kTxnOne, kAborted, -1 }, { kTxnTwo, kAborted, -1 } }));
  // Repeated abort calls are idempotent.
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnOne, ParticipantOpPB::ABORT_TXN),
      kDefaultTimeout));
  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnTwo, ParticipantOpPB::ABORT_TXN),
      kDefaultTimeout));
  NO_FATALS(CheckReplicasMatchTxns(replicas,
      { { kTxnOne, kAborted, -1 }, { kTxnTwo, kAborted, -1 } }));
}

TEST_F(TxnParticipantITest, TestTxnSystemClientErrorWhenNotBegun) {
  constexpr const int kLeaderIdx = 0;
  int txn_id = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  const auto tablet_id = leader_replica->tablet_id();
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  unique_ptr<TxnSystemClient> txn_client;
  ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_client));

  for (auto type : { ParticipantOpPB::BEGIN_COMMIT,
                     ParticipantOpPB::FINALIZE_COMMIT }) {
    Status s = txn_client->ParticipateInTransaction(
        tablet_id, MakeParticipantOp(txn_id++, type), kDefaultTimeout);
    ASSERT_TRUE(s.IsIllegalState()) << s.ToString();
    NO_FATALS(CheckReplicasMatchTxns(replicas, {}));
  }

  ASSERT_OK(txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(txn_id++, ParticipantOpPB::ABORT_TXN), kDefaultTimeout));
  NO_FATALS(CheckReplicasMatchTxns(replicas, { { 2, kAborted, -1 } }));
}

TEST_F(TxnParticipantITest, TestTxnSystemClientTimeoutWhenNoMajority) {
  constexpr const int kLeaderIdx = 0;
  constexpr const int kTxnId = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  const auto tablet_id = leader_replica->tablet_id();
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  // Bring down the other servers so we can't get a majority.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    if (i == kLeaderIdx) continue;
    cluster_->mini_tablet_server(i)->Shutdown();
  }
  unique_ptr<TxnSystemClient> txn_client;
  ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_client));
  Status s = txn_client->ParticipateInTransaction(
      tablet_id, MakeParticipantOp(kTxnId, ParticipantOpPB::BEGIN_TXN),
      MonoDelta::FromSeconds(1));
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // We should have an initializing transaction until a majority is achieved,
  // at which point the BEGIN_TXN should complete and we end up with an open
  // transaction.
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({ { kTxnId, kInitializing, -1 } }),
      leader_replica->tablet()->txn_participant()->GetTxnsForTests());
  replicas.clear();
  // Restart the down servers and check that we get to the right state.
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    auto* ts = cluster_->mini_tablet_server(i);
    if (i != kLeaderIdx) {
      ASSERT_OK(ts->Restart());
    }
    scoped_refptr<TabletReplica> r;
    auto* ts_manager = ts->server()->tablet_manager();
    ASSERT_OK(ts_manager->WaitForAllBootstrapsToFinish());
    ASSERT_TRUE(ts_manager->LookupTablet(tablet_id, &r));
    replicas.emplace_back(r.get());
  }
  ASSERT_EVENTUALLY([&] {
    NO_FATALS(CheckReplicasMatchTxns(replicas, { { kTxnId, kOpen, -1 } }));
  });
}

namespace {
// Sends participant ops to the given tablet until failure, or until
// 'stop_latch' counts down.
Status SendParticipantOps(TxnSystemClient* txn_client, const string& tablet_id,
    CountDownLatch* stop_latch, int* next_txn_id) {
  while (stop_latch->count() > 0) {
    int txn_id = (*next_txn_id)++;
    for (const auto& op : kCommitSequence) {
      RETURN_NOT_OK(txn_client->ParticipateInTransaction(
          tablet_id, MakeParticipantOp(txn_id, op), kLongTimeout));
    }
  }
  return Status::OK();
}
} // anonymous namespace

// Test that the transaction system client retries when the tablet server
// hosting the targeted tablet is shutting down and starting up.
TEST_F(TxnParticipantITest, TestTxnSystemClientSucceedsOnBootstrap) {
  constexpr const int kLeaderIdx = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  const auto tablet_id = leader_replica->tablet_id();
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  // Start a thread that sends participant ops to the tablet.
  int next_txn_id = 0;
  unique_ptr<TxnSystemClient> txn_client;
  ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_client));
  CountDownLatch stop(1);
  Status client_error;
  thread t([&] {
    client_error = SendParticipantOps(txn_client.get(), tablet_id, &stop, &next_txn_id);
  });
  auto thread_joiner = MakeScopedCleanup([&] {
    stop.CountDown();
    t.join();
  });

  // Stop quiescing so proper failover can happen, as we kill each server.
  StopQuiescingServers();
  for (int i = kLeaderIdx; i < cluster_->num_tablet_servers(); i++) {
    auto* ts = cluster_->mini_tablet_server(i);
    ts->Shutdown();
    ASSERT_OK(ts->Restart());

    // Wait for proper recovery to happen, and throw in some mandatory sleep to
    // ensure we have time to replicate some ops.
    ASSERT_OK(ts->server()->tablet_manager()->WaitForAllBootstrapsToFinish());
    SleepFor(MonoDelta::FromMilliseconds(500));
  }
  stop.CountDown();
  thread_joiner.cancel();
  t.join();

  // None of our transactions should have failed.
  ASSERT_OK(client_error);
  vector<TxnParticipant::TxnEntry> expected_txns;
  for (int i = 0; i < next_txn_id; i++) {
    expected_txns.emplace_back(TxnParticipant::TxnEntry({ i, kCommitted, kDummyCommitTimestamp }));
  }
  replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  NO_FATALS(CheckReplicasMatchTxns(replicas, expected_txns));
}

// Test that the transaction system client retries when a replica is deleted
// and recovered.
TEST_F(TxnParticipantITest, TestTxnSystemClientRetriesWhenReplicaNotFound) {
  // Speed up the time it takes to determine that we need to copy.
  FLAGS_follower_unavailable_considered_failed_sec = 1;

  // We're going to delete the leader and stop quiescing.
  constexpr const int kLeaderIdx = 0;
  vector<TabletReplica*> replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  auto* leader_replica = replicas[kLeaderIdx];
  const auto tablet_id = leader_replica->tablet_id();
  ASSERT_OK(leader_replica->consensus()->WaitUntilLeaderForTests(kDefaultTimeout));
  // Start a thread that sends participant ops to the tablet.
  int next_txn_id = 0;
  unique_ptr<TxnSystemClient> txn_client;
  ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_client));
  CountDownLatch stop(1);
  Status client_error;
  thread t([&] {
    client_error = SendParticipantOps(txn_client.get(), tablet_id, &stop, &next_txn_id);
  });
  auto thread_joiner = MakeScopedCleanup([&] {
    stop.CountDown();
    t.join();
  });

  // Stop quiescing so proper failover can happen, as we kill each server.
  StopQuiescingServers();
  auto* tablet_manager = cluster_->mini_tablet_server(kLeaderIdx)->server()->tablet_manager();
  ASSERT_OK(tablet_manager->DeleteTablet(tablet_id, tablet::TABLET_DATA_TOMBSTONED, boost::none));
  ASSERT_EVENTUALLY([&] {
    scoped_refptr<TabletReplica> r;
    ASSERT_TRUE(tablet_manager->LookupTablet(tablet_id, &r));
    ASSERT_OK(r->WaitUntilConsensusRunning(kDefaultTimeout));
  });

  stop.CountDown();
  thread_joiner.cancel();
  t.join();

  // None of our transactions should have failed.
  ASSERT_OK(client_error);
  vector<TxnParticipant::TxnEntry> expected_txns;
  for (int i = 0; i < next_txn_id; i++) {
    expected_txns.emplace_back(TxnParticipant::TxnEntry({ i, kCommitted, kDummyCommitTimestamp }));
  }
  replicas = SetUpLeaderGetReplicas(kLeaderIdx);
  NO_FATALS(CheckReplicasMatchTxns(replicas, expected_txns));
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
    TxnState expected_state;
    switch (last_successful_op) {
      case ParticipantOpPB::BEGIN_TXN:
        expected_state = kOpen;
        break;
      case ParticipantOpPB::BEGIN_COMMIT:
        expected_state = kCommitInProgress;
        break;
      case ParticipantOpPB::FINALIZE_COMMIT:
        expected_rows += num_successful_writes_per_txn[txn_id];
        expected_state = kCommitted;
        break;
      default:
        FAIL() << "Unexpected successful op " << last_successful_op;
    }
    expected_txns.emplace_back(TxnParticipant::TxnEntry({
        txn_id, expected_state, expected_state == kCommitted ? kDummyCommitTimestamp : -1}));
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
    ASSERT_OK(r->WaitUntilConsensusRunning(kDefaultTimeout));
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
      if (txn.state != kInitializing) {
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

TEST_F(TxnParticipantElectionStormITest, TestTxnSystemClientRetriesThroughStorm) {
  vector<TabletReplica*> replicas;
  const string tablet_id = cluster_->mini_tablet_server(0)->ListTablets()[0];
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    scoped_refptr<TabletReplica> r;
    ASSERT_TRUE(cluster_->mini_tablet_server(i)->server()->tablet_manager()->LookupTablet(
        tablet_id, &r));
    replicas.emplace_back(r.get());
  }
  const auto kTimeout = MonoDelta::FromSeconds(10);
  unique_ptr<TxnSystemClient> txn_client;
  ASSERT_OK(TxnSystemClient::Create(cluster_->master_rpc_addrs(), &txn_client));

  // Start injecting latency to Raft-related traffic to spur elections.
  FLAGS_raft_enable_pre_election = false;
  FLAGS_consensus_inject_latency_ms_in_notifications = 1.5 * FLAGS_raft_heartbeat_interval_ms;;
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_raft_heartbeat_interval_ms * 2));
  constexpr const int64_t kCommittedTxnId = 0;
  constexpr const int64_t kAbortedTxnId = 1;
  for (const auto& op : kCommitSequence) {
    ASSERT_OK(txn_client->ParticipateInTransaction(
        tablet_id, MakeParticipantOp(kCommittedTxnId, op), kTimeout));
  }
  for (const auto& op : kAbortSequence) {
    ASSERT_OK(txn_client->ParticipateInTransaction(
        tablet_id, MakeParticipantOp(kAbortedTxnId, op), kTimeout));
  }
  const vector<TxnParticipant::TxnEntry> expected_txns = {
      { kCommittedTxnId, kCommitted, kDummyCommitTimestamp },
      { kAbortedTxnId, kAborted, -1 },
  };
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
}

} // namespace itest
} // namespace kudu
