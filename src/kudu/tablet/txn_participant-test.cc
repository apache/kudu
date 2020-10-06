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

#include "kudu/tablet/txn_participant.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/op_driver.h"
#include "kudu/tablet/ops/op_tracker.h"
#include "kudu/tablet/ops/participant_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_participant-test-util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using kudu::consensus::CommitMsg;
using kudu::consensus::ConsensusBootstrapInfo;
using kudu::pb_util::SecureShortDebugString;
using kudu::tserver::ParticipantRequestPB;
using kudu::tserver::ParticipantResponsePB;
using kudu::tserver::ParticipantOpPB;
using kudu::tserver::WriteRequestPB;
using std::map;
using std::thread;
using std::unique_ptr;
using std::vector;

DECLARE_bool(enable_maintenance_manager);
DECLARE_bool(log_preallocate_segments);
DECLARE_bool(log_async_preallocate_segments);

namespace kudu {
namespace tablet {

namespace {
Schema GetTestSchema() {
  return Schema({ ColumnSchema("key", INT32) }, 1);
}

// A participant op that waits to start and finish applying based on input
// latches.
class DelayedParticipantOp : public ParticipantOp {
 public:
  DelayedParticipantOp(CountDownLatch* apply_started,
                       CountDownLatch* apply_continue,
                       unique_ptr<ParticipantOpState> state)
    : ParticipantOp(std::move(state), consensus::LEADER),
      apply_started_(apply_started),
      apply_continue_(apply_continue) {}

  Status Apply(CommitMsg** commit_msg) override {
    apply_started_->CountDown();
    LOG(INFO) << "Delaying apply...";
    apply_continue_->Wait();
    return ParticipantOp::Apply(commit_msg);
  }

 private:
  CountDownLatch* apply_started_;
  CountDownLatch* apply_continue_;
};
} // anonymous namespace

class TxnParticipantTest : public TabletReplicaTestBase {
 public:
  TxnParticipantTest()
      : TabletReplicaTestBase(GetTestSchema()) {}

  void SetUp() override {
    // Some of these tests will test the durability semantics of participants.
    // So we have finer-grained control of on-disk state, disable anything that
    // might write to disk in the background.
    FLAGS_enable_maintenance_manager = false;
    FLAGS_log_preallocate_segments = false;
    FLAGS_log_async_preallocate_segments = false;

    NO_FATALS(TabletReplicaTestBase::SetUp());
    ConsensusBootstrapInfo info;
    ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  }

  Status Write(int key) {
    WriteRequestPB req;
    req.set_tablet_id(tablet_replica_->tablet_id());
    const auto& schema = GetTestSchema();
    RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
    KuduPartialRow row(&schema);
    RETURN_NOT_OK(row.SetInt32(0, key));
    RowOperationsPBEncoder enc(req.mutable_row_operations());
    enc.Add(RowOperationsPB::INSERT, row);
    return ExecuteWrite(tablet_replica_.get(), req);
  }

  // Writes an op to the WAL, rolls over onto a new WAL segment, and flushes
  // the MRS, leaving us with a new WAL segment that should be GC-able unless
  // previous WAL segments are anchored.
  Status WriteRolloverAndFlush(int key) {
    RETURN_NOT_OK(Write(key));
    RETURN_NOT_OK(tablet_replica_->log()->WaitUntilAllFlushed());
    RETURN_NOT_OK(tablet_replica_->log()->AllocateSegmentAndRollOverForTests());
    return tablet_replica_->tablet()->Flush();
  }

  TxnParticipant* txn_participant() {
    return tablet_replica_->tablet()->txn_participant();
  }
};

TEST_F(TxnParticipantTest, TestSuccessfulSequences) {
  const auto check_valid_sequence = [&] (const vector<ParticipantOpPB::ParticipantOpType>& ops,
                                         int64_t txn_id) {
    for (const auto& type : ops) {
      ParticipantResponsePB resp;
      ASSERT_OK(CallParticipantOp(
          tablet_replica_.get(), txn_id, type, kDummyCommitTimestamp, &resp));
      SCOPED_TRACE(SecureShortDebugString(resp));
      ASSERT_FALSE(resp.has_error());
      ASSERT_TRUE(resp.has_timestamp());
    }
  };
  // Check the happy path where the transaction is committed.
  NO_FATALS(check_valid_sequence({
      ParticipantOpPB::BEGIN_TXN,
      ParticipantOpPB::BEGIN_COMMIT,
      ParticipantOpPB::FINALIZE_COMMIT,
  }, 0));

  // Check the case where a transaction is aborted after beginning to commit.
  NO_FATALS(check_valid_sequence({
      ParticipantOpPB::BEGIN_TXN,
      ParticipantOpPB::BEGIN_COMMIT,
      ParticipantOpPB::ABORT_TXN,
  }, 1));

  // Check the case where a transaction is aborted after starting but before
  // committing.
  NO_FATALS(check_valid_sequence({
      ParticipantOpPB::BEGIN_TXN,
      ParticipantOpPB::ABORT_TXN,
  }, 2));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { 0, Txn::kCommitted, kDummyCommitTimestamp },
      { 1, Txn::kAborted, -1 },
      { 2, Txn::kAborted, -1 },
  }), txn_participant()->GetTxnsForTests());
}

TEST_F(TxnParticipantTest, TestTransactionNotFound) {
  const auto check_bad_ops = [&] (const vector<ParticipantOpPB::ParticipantOpType>& ops,
                                       int64_t txn_id) {
    for (const auto& type : ops) {
      ParticipantResponsePB resp;
      ASSERT_OK(CallParticipantOp(
          tablet_replica_.get(), txn_id, type, kDummyCommitTimestamp, &resp));
      SCOPED_TRACE(SecureShortDebugString(resp));
      ASSERT_TRUE(resp.has_error());
      ASSERT_TRUE(resp.error().has_status());
      ASSERT_EQ(AppStatusPB::NOT_FOUND, resp.error().status().code());
      ASSERT_FALSE(resp.has_timestamp());
    }
  };
  NO_FATALS(check_bad_ops({
    ParticipantOpPB::BEGIN_COMMIT,
    ParticipantOpPB::FINALIZE_COMMIT,
    ParticipantOpPB::ABORT_TXN,
  }, 1));
  ASSERT_TRUE(txn_participant()->GetTxnsForTests().empty());
}

TEST_F(TxnParticipantTest, TestIllegalTransitions) {
  const int64_t kTxnId = 1;
  const auto check_valid_op = [&] (const ParticipantOpPB::ParticipantOpType& type, int64_t txn_id) {
    ParticipantResponsePB resp;
    ASSERT_OK(CallParticipantOp(
        tablet_replica_.get(), txn_id, type, kDummyCommitTimestamp, &resp));
    SCOPED_TRACE(SecureShortDebugString(resp));
    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.has_timestamp());
  };
  const auto check_bad_ops = [&] (const vector<ParticipantOpPB::ParticipantOpType>& ops,
                                       int64_t txn_id) {
    for (const auto& type : ops) {
      ParticipantResponsePB resp;
      ASSERT_OK(CallParticipantOp(
          tablet_replica_.get(), txn_id, type, kDummyCommitTimestamp, &resp));
      SCOPED_TRACE(SecureShortDebugString(resp));
      ASSERT_TRUE(resp.has_error());
      ASSERT_TRUE(resp.error().has_status());
      ASSERT_EQ(AppStatusPB::ILLEGAL_STATE, resp.error().status().code());
      ASSERT_FALSE(resp.has_timestamp());
    }
  };
  // Once we've begun the transaction, we can't finalize without beginning to
  // commit.
  NO_FATALS(check_valid_op(ParticipantOpPB::BEGIN_TXN, kTxnId));
  NO_FATALS(check_bad_ops({ ParticipantOpPB::FINALIZE_COMMIT }, kTxnId));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kOpen, -1 },
  }), txn_participant()->GetTxnsForTests());

  // Once we begin committing, we can't start the transaction again.
  NO_FATALS(check_valid_op(ParticipantOpPB::BEGIN_COMMIT, kTxnId));
  NO_FATALS(check_bad_ops({ ParticipantOpPB::BEGIN_TXN }, kTxnId));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitInProgress, -1 },
  }), txn_participant()->GetTxnsForTests());

  // Once we've begun finalizing, we can't do anything.
  NO_FATALS(check_valid_op(ParticipantOpPB::FINALIZE_COMMIT, kTxnId));
  NO_FATALS(check_bad_ops({ ParticipantOpPB::BEGIN_TXN,
                            ParticipantOpPB::BEGIN_COMMIT,
                            ParticipantOpPB::ABORT_TXN }, kTxnId));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitted, kDummyCommitTimestamp },
  }), txn_participant()->GetTxnsForTests());

  // Once we've aborted, we can't do anything.
  const int64_t kAbortedTxnId = 2;
  NO_FATALS(check_valid_op(ParticipantOpPB::BEGIN_TXN, kAbortedTxnId));
  NO_FATALS(check_valid_op(ParticipantOpPB::ABORT_TXN, kAbortedTxnId));
  NO_FATALS(check_bad_ops({ ParticipantOpPB::BEGIN_TXN,
                            ParticipantOpPB::BEGIN_COMMIT,
                            ParticipantOpPB::FINALIZE_COMMIT }, kAbortedTxnId));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitted, kDummyCommitTimestamp },
      { kAbortedTxnId, Txn::kAborted, -1 },
  }), txn_participant()->GetTxnsForTests());
}

// Test that we have no trouble operating on separate transactions.
TEST_F(TxnParticipantTest, TestConcurrentTransactions) {
  const int kNumTxns = 10;
  vector<thread> threads;
  Status statuses[kNumTxns];
  for (int i = 0; i < kNumTxns; i++) {
    threads.emplace_back([&, i] {
      for (const auto& type : kCommitSequence) {
        ParticipantResponsePB resp;
        Status s = CallParticipantOp(
            tablet_replica_.get(), i, type, kDummyCommitTimestamp, &resp);
        if (s.ok() && resp.has_error()) {
          s = StatusFromPB(resp.error().status());
        }
        statuses[i] = s;
      }
    });
  }
  std::for_each(threads.begin(), threads.end(), [] (thread& t) { t.join(); });
  for (const auto& s : statuses) {
    EXPECT_OK(s);
  }
  const auto& txns = txn_participant()->GetTxnsForTests();
  for (int i = 0; i < kNumTxns; i++) {
    ASSERT_EQ(TxnParticipant::TxnEntry({ i, Txn::kCommitted, kDummyCommitTimestamp }), txns[i]);
  }
}

// Concurrently try to apply every op and test, based on the results, that some
// invariants are maintained.
TEST_F(TxnParticipantTest, TestConcurrentOps) {
  const int64_t kTxnId = 1;
  const map<ParticipantOpPB::ParticipantOpType, int> kIndexByOps = {
    { ParticipantOpPB::BEGIN_TXN, 0 },
    { ParticipantOpPB::BEGIN_COMMIT, 1},
    { ParticipantOpPB::FINALIZE_COMMIT, 2},
    { ParticipantOpPB::ABORT_TXN, 3},
  };
  vector<thread> threads;
  vector<Status> statuses(kIndexByOps.size(), Status::Incomplete(""));
  for (const auto& op_and_idx : kIndexByOps) {
    const auto& op_type = op_and_idx.first;
    const auto& idx = op_and_idx.second;
    threads.emplace_back([&, op_type, idx] {
      ParticipantResponsePB resp;
      Status s = CallParticipantOp(
          tablet_replica_.get(), kTxnId, op_type, kDummyCommitTimestamp, &resp);
      if (s.ok() && resp.has_error()) {
         s = StatusFromPB(resp.error().status());
      }
      statuses[idx] = s;
    });
  }
  std::for_each(threads.begin(), threads.end(), [] (thread& t) { t.join(); });
  const auto status_for_op = [&] (ParticipantOpPB::ParticipantOpType type) {
    return statuses[FindOrDie(kIndexByOps, type)];
  };
  // Regardless of order, we should have been able to begin the transaction.
  ASSERT_OK(status_for_op(ParticipantOpPB::BEGIN_TXN));

  // If we finalized the commit, we should have begun committing, and we must
  // not have been able to abort.
  if (status_for_op(ParticipantOpPB::FINALIZE_COMMIT).ok()) {
    ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
        { kTxnId, Txn::kCommitted, kDummyCommitTimestamp },
    }), txn_participant()->GetTxnsForTests());
    ASSERT_OK(statuses[FindOrDie(kIndexByOps, ParticipantOpPB::BEGIN_COMMIT)]);
    ASSERT_FALSE(statuses[FindOrDie(kIndexByOps, ParticipantOpPB::ABORT_TXN)].ok());

  // If we aborted the commit, we could not have finalized the commit.
  } else if (status_for_op(ParticipantOpPB::ABORT_TXN).ok()) {
    ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
        { kTxnId, Txn::kAborted, -1 },
    }), txn_participant()->GetTxnsForTests());
    ASSERT_FALSE(statuses[FindOrDie(kIndexByOps, ParticipantOpPB::FINALIZE_COMMIT)].ok());

  // If we neither aborted nor finalized, but we began to commit, we should be
  // left with the commit in progress.
  } else if (status_for_op(ParticipantOpPB::BEGIN_COMMIT).ok()) {
    ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
        { kTxnId, Txn::kCommitInProgress, -1 },
    }), txn_participant()->GetTxnsForTests());

  // Finally, if nothing else succeeded, at least we should have been able to
  // start the transaction.
  } else {
    ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
        { kTxnId, Txn::kOpen, -1 },
    }), txn_participant()->GetTxnsForTests());
  }
}

TEST_F(TxnParticipantTest, TestReplayParticipantOps) {
  constexpr const int64_t kTxnId = 1;
  for (const auto& type : kCommitSequence) {
    ParticipantResponsePB resp;
    ASSERT_OK(CallParticipantOp(
        tablet_replica_.get(), kTxnId, type, kDummyCommitTimestamp, &resp));
    SCOPED_TRACE(SecureShortDebugString(resp));
    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.has_timestamp());
  }
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitted, kDummyCommitTimestamp }
  }), txn_participant()->GetTxnsForTests());
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitted, kDummyCommitTimestamp }
  }), txn_participant()->GetTxnsForTests());
}

// Test that each transaction has a single anchor that gets updated as
// participant ops land.
TEST_F(TxnParticipantTest, TestAllOpsRegisterAnchors) {
  int64_t expected_index = 1;
  // Validates that each op in the given sequence updates the single anchor
  // maintained for the transaction.
  const auto check_participant_ops_are_anchored =
    [&] (int64_t txn_id, const vector<ParticipantOpPB::ParticipantOpType>& ops) {
      for (const auto& op : ops) {
        ParticipantResponsePB resp;
        ASSERT_OK(CallParticipantOp(tablet_replica_.get(), txn_id, op,
                                    kDummyCommitTimestamp, &resp));
        ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
        expected_index++;
        if (op == ParticipantOpPB::BEGIN_COMMIT) {
          ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
          int64_t log_index = -1;
          tablet_replica_->log_anchor_registry()->GetEarliestRegisteredLogIndex(&log_index);
          ASSERT_EQ(expected_index, log_index);
        } else {
          ASSERT_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
        }
      }
      ASSERT_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
    };
  NO_FATALS(check_participant_ops_are_anchored(1, {
      ParticipantOpPB::BEGIN_TXN,
      ParticipantOpPB::BEGIN_COMMIT,
      ParticipantOpPB::FINALIZE_COMMIT
  }));
  NO_FATALS(check_participant_ops_are_anchored(2, {
      ParticipantOpPB::BEGIN_TXN,
      ParticipantOpPB::BEGIN_COMMIT,
      ParticipantOpPB::ABORT_TXN
  }));
}

// Test that participant ops result in tablet metadata updates that can survive
// restarts, and that the appropriate anchors are in place as we progress
// through a transaction's life cycle.
TEST_F(TxnParticipantTest, TestTxnMetadataSurvivesRestart) {
  const int64_t kTxnId = 1;
  // First, do a sanity check that there's nothing GCable.
  int64_t gcable_size;
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_EQ(0, gcable_size);

  // Perform some initial participant ops and roll the WAL segments so there
  // are some candidates for WAL GC.
  ParticipantResponsePB resp;
  ASSERT_OK(CallParticipantOp(tablet_replica_.get(), kTxnId, ParticipantOpPB::BEGIN_TXN,
                              kDummyCommitTimestamp, &resp));
  ASSERT_FALSE(resp.has_error());
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  ASSERT_OK(tablet_replica_->log()->WaitUntilAllFlushed());
  ASSERT_OK(tablet_replica_->log()->AllocateSegmentAndRollOverForTests());
  ASSERT_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());

  // We can't GC down to 0 segments, so write some rows and roll onto new
  // segments so we have a segment to GC.
  int current_key = 0;
  ASSERT_OK(WriteRolloverAndFlush(current_key++));
  ASSERT_OK(WriteRolloverAndFlush(current_key++));
  ASSERT_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());

  // We should be able to GC the WALs that have the BEGIN_TXN op.
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_GT(gcable_size, 0);
  ASSERT_OK(tablet_replica_->RunLogGC());
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_EQ(0, gcable_size);

  // At this point, we have a single segment with some writes in it.
  // Write and flush a BEGIN_COMMIT op. Once we GC, our WAL will start on this
  // op, and WALs should be anchored until the commit is finalized, regardless
  // of whether there are more segments.
  ASSERT_OK(CallParticipantOp(tablet_replica_.get(), kTxnId, ParticipantOpPB::BEGIN_COMMIT,
                              kDummyCommitTimestamp, &resp));
  ASSERT_FALSE(resp.has_error());
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  ASSERT_OK(tablet_replica_->log()->WaitUntilAllFlushed());
  ASSERT_OK(tablet_replica_->log()->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_GT(gcable_size, 0);
  ASSERT_OK(tablet_replica_->RunLogGC());
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_EQ(0, gcable_size);

  // Even if we write and roll over, we shouldn't be able to GC because of the
  // anchor.
  ASSERT_OK(WriteRolloverAndFlush(current_key++));
  ASSERT_OK(WriteRolloverAndFlush(current_key++));
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_EQ(0, gcable_size);
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitInProgress, -1 }
  }), txn_participant()->GetTxnsForTests());

  // Once we finalize the commit, the BEGIN_COMMIT anchor should be released.
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(CallParticipantOp(tablet_replica_.get(), kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                              kDummyCommitTimestamp, &resp));
  ASSERT_FALSE(resp.has_error());
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  ASSERT_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());

  // Because we rebuilt the WAL, we only have one segment and thus shouldn't be
  // able to GC anything until we add more segments.
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_EQ(0, gcable_size);
  ASSERT_OK(WriteRolloverAndFlush(current_key++));
  ASSERT_OK(WriteRolloverAndFlush(current_key++));
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_GT(gcable_size, 0);
  ASSERT_OK(tablet_replica_->RunLogGC());
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_EQ(0, gcable_size);

  // Ensure the transaction bootstraps to the expected state.
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitted, kDummyCommitTimestamp }
  }), txn_participant()->GetTxnsForTests());
}

class MetadataFlushTxnParticipantTest : public TxnParticipantTest,
                                        public ::testing::WithParamInterface<bool> {};

// Test rebuilding transaction state from the WALs and metadata.
TEST_P(MetadataFlushTxnParticipantTest, TestRebuildTxnMetadata) {
  const bool should_flush = GetParam();
  const int64_t kTxnId = 1;
  ParticipantResponsePB resp;
  ASSERT_OK(CallParticipantOp(tablet_replica_.get(), kTxnId, ParticipantOpPB::BEGIN_TXN,
                              kDummyCommitTimestamp, &resp));
  ASSERT_FALSE(resp.has_error());
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }

  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kOpen, -1 }
  }), txn_participant()->GetTxnsForTests());
  ASSERT_OK(CallParticipantOp(tablet_replica_.get(), kTxnId, ParticipantOpPB::BEGIN_COMMIT,
                              kDummyCommitTimestamp, &resp));
  ASSERT_FALSE(resp.has_error());
  // NOTE: BEGIN_COMMIT ops don't anchor on metadata flush, so don't bother
  // flushing.

  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitInProgress, -1 }
  }), txn_participant()->GetTxnsForTests());
  ASSERT_OK(CallParticipantOp(tablet_replica_.get(), kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                              kDummyCommitTimestamp, &resp));
  ASSERT_FALSE(resp.has_error());
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }

  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitted, kDummyCommitTimestamp }
  }), txn_participant()->GetTxnsForTests());

  // Now perform the same validation but for a transaction that gets aborted.
  const int64_t kAbortedTxnId = 2;
  ASSERT_OK(CallParticipantOp(tablet_replica_.get(), kAbortedTxnId, ParticipantOpPB::BEGIN_TXN,
                              kDummyCommitTimestamp, &resp));
  ASSERT_FALSE(resp.has_error());
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitted, kDummyCommitTimestamp },
      { kAbortedTxnId, Txn::kOpen, -1 }
  }), txn_participant()->GetTxnsForTests());
  ASSERT_OK(CallParticipantOp(tablet_replica_.get(), kAbortedTxnId, ParticipantOpPB::ABORT_TXN,
                              kDummyCommitTimestamp, &resp));
  ASSERT_FALSE(resp.has_error());
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kCommitted, kDummyCommitTimestamp },
      { kAbortedTxnId, Txn::kAborted, -1 }
  }), txn_participant()->GetTxnsForTests());
}
INSTANTIATE_TEST_CASE_P(ShouldFlushMetadata, MetadataFlushTxnParticipantTest,
    ::testing::Values(true, false));

// Similar to the above test, but checking that in-flight ops anchor the WALs.
TEST_F(TxnParticipantTest, TestActiveParticipantOpsAnchorWALs) {
  const int64_t kTxnId = 1;
  ParticipantRequestPB req;
  ParticipantResponsePB resp;
  auto op_state = NewParticipantOp(tablet_replica_.get(), kTxnId, ParticipantOpPB::BEGIN_TXN,
                                   kDummyCommitTimestamp, &req, &resp);
  CountDownLatch latch(1);
  CountDownLatch apply_start(1);
  CountDownLatch apply_continue(1);
  op_state->set_completion_callback(std::unique_ptr<OpCompletionCallback>(
      new LatchOpCompletionCallback<tserver::ParticipantResponsePB>(&latch, &resp)));
  scoped_refptr<OpDriver> driver;
  unique_ptr<DelayedParticipantOp> op(
      new DelayedParticipantOp(&apply_start, &apply_continue, std::move(op_state)));
  ASSERT_OK(tablet_replica_->NewLeaderOpDriver(std::move(op), &driver));
  ASSERT_OK(driver->ExecuteAsync());
  // Wait for the apply to start, indicating that we have persisted and
  // replicated but not yet Raft committed the participant op.
  apply_start.Wait();
  ASSERT_TRUE(driver->GetOpId().IsInitialized());
  ASSERT_EQ(1, tablet_replica_->op_tracker()->GetNumPendingForTests());

  // Create some WAL segments to ensure some would-be-GC-able segments.
  int current_key = 0;
  ASSERT_OK(WriteRolloverAndFlush(current_key++));
  ASSERT_OK(WriteRolloverAndFlush(current_key++));

  // Our participant op is still pending, and nothing should be GC-able.
  ASSERT_EQ(1, tablet_replica_->op_tracker()->GetNumPendingForTests());
  int64_t gcable_size;
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_EQ(0, gcable_size);

  // Finish applying the participant op and proceed to completion.
  // Even with more segments added, the WAL should be anchored until we flush
  // the tablet metadata to include the transaction.
  apply_continue.CountDown();
  latch.Wait();
  ASSERT_FALSE(resp.has_error());
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  ASSERT_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());

  // Add some segments to ensure there are enough segments to GC.
  ASSERT_OK(WriteRolloverAndFlush(current_key++));
  ASSERT_OK(WriteRolloverAndFlush(current_key++));
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_GT(gcable_size, 0);

  // Now that we've completed the op, we can get rid of the WAL segments that
  // had the participant op.
  ASSERT_OK(tablet_replica_->RunLogGC());
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_EQ(0, gcable_size);

  // As a sanity check, ensure we get to the expected state if we reboot.
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, Txn::kOpen, -1 }
  }), txn_participant()->GetTxnsForTests());
}

} // namespace tablet
} // namespace kudu
