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
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
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
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/tablet/txn_participant-test-util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using kudu::consensus::CommitMsg;
using kudu::consensus::ConsensusBootstrapInfo;
using kudu::pb_util::SecureShortDebugString;
using kudu::tserver::ParticipantRequestPB;
using kudu::tserver::ParticipantResponsePB;
using kudu::tserver::ParticipantOpPB;
using kudu::tserver::TabletServerErrorPB;
using kudu::tserver::WriteRequestPB;
using std::map;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

DECLARE_bool(enable_maintenance_manager);
DECLARE_bool(log_preallocate_segments);
DECLARE_bool(log_async_preallocate_segments);

namespace kudu {
namespace tablet {

namespace {

constexpr const int64_t kTxnId = 1;

constexpr const int64_t kTxnOne = 1;
constexpr const int64_t kTxnTwo = 2;

Schema GetTestSchema() {
  return Schema({ ColumnSchema("key", INT32), ColumnSchema("val", INT32) }, 1);
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

  Status Write(int key, boost::optional<int64_t> txn_id = boost::none,
               RowOperationsPB::Type type = RowOperationsPB::INSERT) {
    WriteRequestPB req;
    if (txn_id) {
      req.set_txn_id(*txn_id);
    }
    req.set_tablet_id(tablet_replica_->tablet_id());
    const auto& schema = GetTestSchema();
    RETURN_NOT_OK(SchemaToPB(schema, req.mutable_schema()));
    KuduPartialRow row(&schema);
    RETURN_NOT_OK(row.SetInt32(0, key));
    if (type != RowOperationsPB::DELETE &&
        type != RowOperationsPB::DELETE_IGNORE) {
      RETURN_NOT_OK(row.SetInt32(1, key));
    }
    RowOperationsPBEncoder enc(req.mutable_row_operations());
    enc.Add(type, row);
    return ExecuteWrite(tablet_replica_.get(), req);
  }

  Status Delete(int key) {
    return Write(key, boost::none, RowOperationsPB::DELETE);
  }

  Status CallParticipantOpCheckResp(int64_t txn_id, ParticipantOpPB::ParticipantOpType op_type,
                                    int64_t ts_val) {
    ParticipantResponsePB resp;
    RETURN_NOT_OK(CallParticipantOp(tablet_replica_.get(), txn_id, op_type, ts_val, &resp));
    if (resp.has_error()) {
      return StatusFromPB(resp.error().status());
    }
    return Status::OK();
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

  Status IterateToStrings(vector<string>* ret) {
    unique_ptr<RowwiseIterator> iter;
    RETURN_NOT_OK(tablet_replica_->tablet()->NewRowIterator(GetTestSchema(), &iter));
    RETURN_NOT_OK(iter->Init(nullptr));
    vector<string> out;
    RETURN_NOT_OK(IterateToStringList(iter.get(), &out));
    *ret = std::move(out);
    return Status::OK();
  }

  clock::Clock* clock() {
    return tablet_replica_->tablet()->clock();
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
      { 0, kCommitted, kDummyCommitTimestamp },
      { 1, kAborted, -1 },
      { 2, kAborted, -1 },
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
      ASSERT_EQ(TabletServerErrorPB::UNKNOWN_ERROR, resp.error().code());
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
  const auto check_valid_op = [&] (const ParticipantOpPB::ParticipantOpType& type,
                                   int64_t txn_id) {
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
      ASSERT_EQ(TabletServerErrorPB::TXN_ILLEGAL_STATE, resp.error().code());
      ASSERT_FALSE(resp.has_timestamp());
    }
  };
  const auto check_already_applied = [&] (const vector<ParticipantOpPB::ParticipantOpType>& ops,
                                          int64_t txn_id, bool expect_timestamp) {
    for (const auto& type : ops) {
      ParticipantResponsePB resp;
      ASSERT_OK(CallParticipantOp(
          tablet_replica_.get(), txn_id, type, kDummyCommitTimestamp, &resp));
      SCOPED_TRACE(SecureShortDebugString(resp));
      ASSERT_TRUE(resp.has_error());
      ASSERT_TRUE(resp.error().has_status());
      ASSERT_EQ(AppStatusPB::ILLEGAL_STATE, resp.error().status().code());
      ASSERT_EQ(TabletServerErrorPB::TXN_OP_ALREADY_APPLIED, resp.error().code());
      if (expect_timestamp) {
        ASSERT_TRUE(resp.has_timestamp());
      } else {
        ASSERT_FALSE(resp.has_timestamp());
      }
    }
  };
  // Once we've begun the transaction, we can't finalize without beginning to
  // commit.
  NO_FATALS(check_valid_op(ParticipantOpPB::BEGIN_TXN, kTxnId));
  NO_FATALS(check_bad_ops({ ParticipantOpPB::FINALIZE_COMMIT }, kTxnId));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kOpen, -1 },
  }), txn_participant()->GetTxnsForTests());

  // Once we begin committing, we can't start the transaction again.
  NO_FATALS(check_valid_op(ParticipantOpPB::BEGIN_COMMIT, kTxnId));
  NO_FATALS(check_already_applied({ ParticipantOpPB::BEGIN_COMMIT }, kTxnId, true));
  NO_FATALS(check_bad_ops({ ParticipantOpPB::BEGIN_TXN }, kTxnId));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kCommitInProgress, -1 },
  }), txn_participant()->GetTxnsForTests());

  // Once we've begun finalizing, we can't do anything.
  NO_FATALS(check_valid_op(ParticipantOpPB::FINALIZE_COMMIT, kTxnId));
  NO_FATALS(check_already_applied({ ParticipantOpPB::BEGIN_COMMIT }, kTxnId, true));
  NO_FATALS(check_already_applied({ ParticipantOpPB::FINALIZE_COMMIT }, kTxnId, false));
  NO_FATALS(check_bad_ops({ ParticipantOpPB::BEGIN_TXN,
                            ParticipantOpPB::ABORT_TXN }, kTxnId));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kCommitted, kDummyCommitTimestamp },
  }), txn_participant()->GetTxnsForTests());

  // Once we've aborted, we can't do anything.
  const int64_t kAbortedTxnId = 2;
  NO_FATALS(check_valid_op(ParticipantOpPB::BEGIN_TXN, kAbortedTxnId));
  NO_FATALS(check_valid_op(ParticipantOpPB::ABORT_TXN, kAbortedTxnId));
  NO_FATALS(check_bad_ops({ ParticipantOpPB::BEGIN_TXN,
                            ParticipantOpPB::BEGIN_COMMIT,
                            ParticipantOpPB::FINALIZE_COMMIT }, kAbortedTxnId));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kCommitted, kDummyCommitTimestamp },
      { kAbortedTxnId, kAborted, -1 },
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
    ASSERT_EQ(TxnParticipant::TxnEntry({ i, kCommitted, kDummyCommitTimestamp }), txns[i]);
  }
}

// Concurrently try to apply every op and test, based on the results, that some
// invariants are maintained.
TEST_F(TxnParticipantTest, TestConcurrentOps) {
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
        { kTxnId, kCommitted, kDummyCommitTimestamp },
    }), txn_participant()->GetTxnsForTests());
    ASSERT_OK(statuses[FindOrDie(kIndexByOps, ParticipantOpPB::BEGIN_COMMIT)]);
    ASSERT_FALSE(statuses[FindOrDie(kIndexByOps, ParticipantOpPB::ABORT_TXN)].ok());

  // If we aborted the commit, we could not have finalized the commit.
  } else if (status_for_op(ParticipantOpPB::ABORT_TXN).ok()) {
    ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
        { kTxnId, kAborted, -1 },
    }), txn_participant()->GetTxnsForTests());
    ASSERT_FALSE(statuses[FindOrDie(kIndexByOps, ParticipantOpPB::FINALIZE_COMMIT)].ok());

  // If we neither aborted nor finalized, but we began to commit, we should be
  // left with the commit in progress.
  } else if (status_for_op(ParticipantOpPB::BEGIN_COMMIT).ok()) {
    ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
        { kTxnId, kCommitInProgress, -1 },
    }), txn_participant()->GetTxnsForTests());

  // Finally, if nothing else succeeded, at least we should have been able to
  // start the transaction.
  } else {
    ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
        { kTxnId, kOpen, -1 },
    }), txn_participant()->GetTxnsForTests());
  }
}

TEST_F(TxnParticipantTest, TestReplayParticipantOps) {
  for (const auto& type : kCommitSequence) {
    ParticipantResponsePB resp;
    ASSERT_OK(CallParticipantOp(
        tablet_replica_.get(), kTxnId, type, kDummyCommitTimestamp, &resp));
    SCOPED_TRACE(SecureShortDebugString(resp));
    ASSERT_FALSE(resp.has_error());
    ASSERT_TRUE(resp.has_timestamp());
  }
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kCommitted, kDummyCommitTimestamp }
  }), txn_participant()->GetTxnsForTests());
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kCommitted, kDummyCommitTimestamp }
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
        ASSERT_OK(CallParticipantOpCheckResp(txn_id, op, kDummyCommitTimestamp));
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
  // First, do a sanity check that there's nothing GCable.
  int64_t gcable_size;
  ASSERT_OK(tablet_replica_->GetGCableDataSize(&gcable_size));
  ASSERT_EQ(0, gcable_size);

  // Perform some initial participant ops and roll the WAL segments so there
  // are some candidates for WAL GC.
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN,
                                       kDummyCommitTimestamp));
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
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT,
                                       kDummyCommitTimestamp));
  ASSERT_OK(tablet_replica_->log()->WaitUntilAllFlushed());
  ASSERT_OK(tablet_replica_->log()->AllocateSegmentAndRollOverForTests());
  // There should be two anchors for this op: one that is in place until the
  // FINALIZE_COMMIT op, another until we flush.
  ASSERT_EQ(2, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
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
      { kTxnId, kCommitInProgress, -1 }
  }), txn_participant()->GetTxnsForTests());

  // Once we finalize the commit, the BEGIN_COMMIT anchor should be released.
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       kDummyCommitTimestamp));
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
      { kTxnId, kCommitted, kDummyCommitTimestamp }
  }), txn_participant()->GetTxnsForTests());
}

// Test that we can replay BEGIN_COMMIT ops, given it anchors WALs until
// metadata flush _and_ until the transaction is finalized or aborted.
TEST_F(TxnParticipantTest, TestBeginCommitAnchorsOnFlush) {
  // Start a transaction and begin committing.
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, kDummyCommitTimestamp));
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  auto txn_meta = FindOrDie(tablet_replica_->tablet_metadata()->GetTxnMetadata(), kTxnId);
  ASSERT_EQ(boost::none, txn_meta->commit_mvcc_op_timestamp());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT,
                                       kDummyCommitTimestamp));
  // We should have two anchors: one that lasts until we flush, another that
  // lasts until we finalize the commit.
  ASSERT_EQ(2, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());

  // We should have an MVCC op timestamp in the metadata, even after
  // restarting.
  ASSERT_NE(boost::none, txn_meta->commit_mvcc_op_timestamp());
  const auto orig_mvcc_op_timestamp = *txn_meta->commit_mvcc_op_timestamp();
  txn_meta.reset();
  RestartReplica(/*reset_tablet*/true);
  txn_meta = FindOrDie(tablet_replica_->tablet_metadata()->GetTxnMetadata(), kTxnId);
  ASSERT_NE(boost::none, txn_meta->commit_mvcc_op_timestamp());
  ASSERT_EQ(orig_mvcc_op_timestamp, *txn_meta->commit_mvcc_op_timestamp());

  // Once we flush, we should drop down to one anchor.
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       kDummyCommitTimestamp));

  // The anchor from the BEGIN_COMMIT op should be gone, but we should have
  // another anchor for the FINALIZE_COMMIT op until we flush the metadata.
  ASSERT_EQ(1, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  ASSERT_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());

  // As another sanity check, we should still have metadata for the MVCC op
  // after restarting.
  ASSERT_NE(boost::none, txn_meta->commit_mvcc_op_timestamp());
  txn_meta.reset();
  RestartReplica(/*reset_tablet*/true);
  txn_meta = FindOrDie(tablet_replica_->tablet_metadata()->GetTxnMetadata(), kTxnId);
  ASSERT_NE(boost::none, txn_meta->commit_mvcc_op_timestamp());
  ASSERT_EQ(orig_mvcc_op_timestamp, *txn_meta->commit_mvcc_op_timestamp());
}

// Like the above test but finalizing the commit before flushing the metadata.
TEST_F(TxnParticipantTest, TestBeginCommitAnchorsOnFinalize) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, kDummyCommitTimestamp));
  auto txn_meta = FindOrDie(tablet_replica_->tablet_metadata()->GetTxnMetadata(), kTxnId);
  ASSERT_EQ(boost::none, txn_meta->commit_mvcc_op_timestamp());
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT,
                                       kDummyCommitTimestamp));
  ASSERT_NE(boost::none, txn_meta->commit_mvcc_op_timestamp());
  const auto orig_mvcc_op_timestamp = *txn_meta->commit_mvcc_op_timestamp();

  // Restarting shouldn't affect our metadata.
  txn_meta.reset();
  RestartReplica(/*reset_tablet*/true);
  txn_meta = FindOrDie(tablet_replica_->tablet_metadata()->GetTxnMetadata(), kTxnId);
  ASSERT_NE(boost::none, txn_meta->commit_mvcc_op_timestamp());
  ASSERT_EQ(orig_mvcc_op_timestamp, *txn_meta->commit_mvcc_op_timestamp());

  // We should have two anchors, one that lasts until we flush, another that
  // lasts until we finalize.
  ASSERT_EQ(2, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       kDummyCommitTimestamp));

  // Finalizing the commit shouldn't affect our metadata.
  txn_meta.reset();
  RestartReplica(/*reset_tablet*/true);
  txn_meta = FindOrDie(tablet_replica_->tablet_metadata()->GetTxnMetadata(), kTxnId);
  ASSERT_NE(boost::none, txn_meta->commit_mvcc_op_timestamp());
  ASSERT_EQ(orig_mvcc_op_timestamp, *txn_meta->commit_mvcc_op_timestamp());

  // One anchor should be gone and another should be registered in its place
  // that lasts until we flush the finalized metadata.
  ASSERT_EQ(2, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
  ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  ASSERT_EQ(0, tablet_replica_->log_anchor_registry()->GetAnchorCountForTests());
}

class MetadataFlushTxnParticipantTest : public TxnParticipantTest,
                                        public ::testing::WithParamInterface<bool> {};

// Test rebuilding transaction state from the WALs and metadata.
TEST_P(MetadataFlushTxnParticipantTest, TestRebuildTxnMetadata) {
  const bool should_flush = GetParam();
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, kDummyCommitTimestamp));
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }

  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kOpen, -1 }
  }), txn_participant()->GetTxnsForTests());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT,
                                       kDummyCommitTimestamp));
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }

  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kCommitInProgress, -1 }
  }), txn_participant()->GetTxnsForTests());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       kDummyCommitTimestamp));
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }

  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kCommitted, kDummyCommitTimestamp }
  }), txn_participant()->GetTxnsForTests());

  // Now perform the same validation but for a transaction that gets aborted.
  const int64_t kAbortedTxnId = 2;
  ASSERT_OK(CallParticipantOpCheckResp(kAbortedTxnId, ParticipantOpPB::BEGIN_TXN,
                                       kDummyCommitTimestamp));
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kCommitted, kDummyCommitTimestamp },
      { kAbortedTxnId, kOpen, -1 }
  }), txn_participant()->GetTxnsForTests());
  ASSERT_OK(CallParticipantOpCheckResp(kAbortedTxnId, ParticipantOpPB::ABORT_TXN,
                                       kDummyCommitTimestamp));
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_EQ(vector<TxnParticipant::TxnEntry>({
      { kTxnId, kCommitted, kDummyCommitTimestamp },
      { kAbortedTxnId, kAborted, -1 }
  }), txn_participant()->GetTxnsForTests());
}

// Test rebuilding transaction state, including writes, from WALs and metadata.
TEST_P(MetadataFlushTxnParticipantTest, TestReplayTransactionalInserts) {
  const bool should_flush = GetParam();
  constexpr const int64_t kAbortedTxnId = 2;
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kAbortedTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(Write(0, kTxnId));
  ASSERT_OK(Write(0, kAbortedTxnId));
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }

  // As long as we haven't finalized the transaction, we shouldn't be able to
  // iterate through its mutations, even across restarts.
  vector<string> rows;
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());

  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kAbortedTxnId, ParticipantOpPB::BEGIN_COMMIT, -1));
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());
  ASSERT_OK(CallParticipantOpCheckResp(kAbortedTxnId, ParticipantOpPB::ABORT_TXN,
                                       clock()->Now().value()));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());

  // Once we committed the transaction, we should see the rows.
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());
}

// Test replaying mutations to transactional MRSs.
TEST_P(MetadataFlushTxnParticipantTest, TestReplayUpdatesToTransactionalMRS) {
  const bool should_flush = GetParam();
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(Write(0, kTxnId));
  ASSERT_OK(Write(1, kTxnId));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  if (should_flush) {
    ASSERT_OK(tablet_replica_->tablet_metadata()->Flush());
  }
  ASSERT_OK(Delete(0));
  vector<string> rows;
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());

  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());
}

INSTANTIATE_TEST_SUITE_P(ShouldFlushMetadata, MetadataFlushTxnParticipantTest,
                         ::testing::Values(true, false));

// Similar to the above test, but checking that in-flight ops anchor the WALs.
TEST_F(TxnParticipantTest, TestActiveParticipantOpsAnchorWALs) {
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
  driver->ExecuteAsync();
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
  ASSERT_EQ(0, gcable_size);
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
      { kTxnId, kOpen, -1 }
  }), txn_participant()->GetTxnsForTests());
}

// Test that we can only write to transactions if they are open.
TEST_F(TxnParticipantTest, TestWriteToOpenTransactionsOnly) {
  constexpr const int64_t kAbortedTxnId = 2;
  Status s = Write(0, kTxnId);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(Write(0, kTxnId));

  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT, -1));
  // Even if the row already exists, we shouldn't get an AlreadyPresent error;
  // the transaction's state is checked much earlier than the presence check.
  s = Write(0, kTxnId);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = Write(1, kTxnId);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       kDummyCommitTimestamp));
  s = Write(0, kTxnId);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = Write(1, kTxnId);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  ASSERT_OK(CallParticipantOpCheckResp(kAbortedTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(Write(2, kAbortedTxnId));
  ASSERT_OK(CallParticipantOpCheckResp(kAbortedTxnId, ParticipantOpPB::ABORT_TXN, -1));
  s = Write(2, kAbortedTxnId);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  s = Write(3, kAbortedTxnId);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

// Test that we get an appropriate error when attempting transactional ops that
// are not supported.
TEST_F(TxnParticipantTest, TestUnsupportedOps) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  Status s = Write(0, kTxnId, RowOperationsPB::UPSERT);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  s = Write(0, kTxnId, RowOperationsPB::UPDATE);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  s = Write(0, kTxnId, RowOperationsPB::UPDATE_IGNORE);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();

  // None of the ops should have done anything.
  ASSERT_EQ(0, tablet_replica_->CountLiveRowsNoFail());

  s = Write(0, kTxnId, RowOperationsPB::DELETE);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  s = Write(0, kTxnId, RowOperationsPB::DELETE_IGNORE);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
}

// Test that rows inserted to transactional stores only show up when the
// transactions complete.
TEST_F(TxnParticipantTest, TestInsertToTransactionMRS) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(Write(0, kTxnOne));
  ASSERT_OK(Write(1, kTxnTwo));
  ASSERT_OK(Write(2, kTxnTwo));

  vector<string> rows;
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  // Only after we finalize a transaction's commit should we see its rows.
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());

  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(3, rows.size());
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(3, rows.size());
}

// Test that rows inserted to transactional stores don't show up if the
// transaction is aborted.
TEST_F(TxnParticipantTest, TestDontReadAbortedInserts) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(Write(0, kTxnOne));
  ASSERT_OK(Write(1, kTxnTwo));

  vector<string> rows;
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());

  // Even if we begin committing, if the transaction is ultimately aborted, we
  // should see nothing.
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::ABORT_TXN, -1));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());

  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::ABORT_TXN, -1));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());

  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());
}

// Test that rows inserted as a part of a transaction cannot be updated if the
// transaction is aborted.
TEST_F(TxnParticipantTest, TestUpdateAfterAborting) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(Write(0, kTxnId));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::ABORT_TXN, -1));
  Status s = Write(0, boost::none, RowOperationsPB::UPDATE);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(Write(0, boost::none, RowOperationsPB::UPDATE_IGNORE));
  ASSERT_EQ(1, tablet_replica_->tablet()->metrics()->update_ignore_errors->value());
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_EQ(0, tablet_replica_->CountLiveRowsNoFail());

  s = Write(0, boost::none, RowOperationsPB::DELETE);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_OK(Write(0, boost::none, RowOperationsPB::DELETE_IGNORE));
  ASSERT_EQ(1, tablet_replica_->tablet()->metrics()->delete_ignore_errors->value());
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_EQ(0, tablet_replica_->CountLiveRowsNoFail());

  ASSERT_OK(Write(0, boost::none, RowOperationsPB::UPSERT));
  ASSERT_EQ(1, tablet_replica_->CountLiveRowsNoFail());
}

// Test that we can update rows that were inserted and committed as a part of a
// transaction.
TEST_F(TxnParticipantTest, TestUpdateCommittedTransactionMRS) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(Write(0, kTxnId));

  // Since we haven't committed yet, we should see no rows.
  vector<string> rows;
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());
  Status s = Delete(0);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT, -1));

  // We still haven't finished committing, so we should see no rows.
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());
  s = Delete(0);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());

  // We should be able to update committed, transactional stores.
  ASSERT_OK(Delete(0));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());

  // We should be able to re-insert the deleted row, even if to a row written
  // during a transaction.
  ASSERT_OK(Write(0));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());

  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());
}

// Test that we can flush multiple MRSs, and that when restarting, ops are
// replayed (or not) as appropriate.
TEST_F(TxnParticipantTest, TestFlushMultipleMRSs) {
  const int kNumTxns = 3;
  const int kNumRowsPerTxn = 100;
  vector<string> rows;
  Tablet* tablet = tablet_replica_->tablet();
  scoped_refptr<TabletComponents> comps;
  for (int t = 0; t < kNumTxns; t++) {
    ASSERT_OK(CallParticipantOpCheckResp(t, ParticipantOpPB::BEGIN_TXN, kDummyCommitTimestamp));

    // Since we haven't committed anything, the tablet components shouldn't
    // have any transactional MRSs.
    tablet->GetComponents(&comps);
    ASSERT_TRUE(comps->txn_memrowsets.empty());
  }
  for (int t = 0; t < kNumTxns; t++) {
    for (int r = 0; r < kNumRowsPerTxn; r++) {
      ASSERT_OK(Write(t * kNumRowsPerTxn + r, t));
    }
    ASSERT_OK(CallParticipantOpCheckResp(t, ParticipantOpPB::BEGIN_COMMIT, kDummyCommitTimestamp));
    ASSERT_OK(CallParticipantOpCheckResp(t, ParticipantOpPB::FINALIZE_COMMIT,
                                         clock()->Now().value()));
    ASSERT_OK(IterateToStrings(&rows));
    ASSERT_EQ((t + 1) * kNumRowsPerTxn, rows.size());
    tablet->GetComponents(&comps);
    ASSERT_EQ(t + 1, comps->txn_memrowsets.size());
  }
  // After restarting, we should have the same number of rows and MRSs.
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  tablet = tablet_replica_->tablet();

  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(kNumTxns * kNumRowsPerTxn, rows.size());
  tablet->GetComponents(&comps);
  ASSERT_EQ(kNumTxns, comps->txn_memrowsets.size());

  // Once flushed, we should have the same number of rows, but no txn MRSs.
  ASSERT_OK(tablet_replica_->tablet()->Flush());
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(kNumTxns * kNumRowsPerTxn, rows.size());
  tablet->GetComponents(&comps);
  ASSERT_TRUE(comps->txn_memrowsets.empty());

  // The verifications should hold after restarting the replica after flushing.
  ASSERT_OK(RestartReplica(/*reset_tablet*/true));
  tablet = tablet_replica_->tablet();

  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(kNumTxns * kNumRowsPerTxn, rows.size());
  tablet->GetComponents(&comps);
  ASSERT_TRUE(comps->txn_memrowsets.empty());
}

// Test that INSERT_IGNORE ops work when the row exists in the transactional
// MRS.
TEST_F(TxnParticipantTest, TestInsertIgnoreInTransactionMRS) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, -1));

  // Insert into a new transactional MRS, and then INSERT_IGNORE as a part of a
  // transaction.
  vector<string> rows;
  ASSERT_OK(Write(0, kTxnId));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_TRUE(rows.empty());

  Status s = Write(0, kTxnId);
  ASSERT_TRUE(s.IsAlreadyPresent());
  ASSERT_EQ(0, tablet_replica_->tablet()->metrics()->insert_ignore_errors->value());

  ASSERT_OK(Write(0, kTxnId, RowOperationsPB::INSERT_IGNORE));
  ASSERT_EQ(1, tablet_replica_->tablet()->metrics()->insert_ignore_errors->value());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());
}

// Test that INSERT_IGNORE ops work when the row exists in the main MRS.
TEST_F(TxnParticipantTest, TestInsertIgnoreInMainMRS) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  // Insert into the main MRS, and then INSERT_IGNORE as a part of a
  // transaction.
  vector<string> rows;
  ASSERT_OK(Write(0));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());

  Status s = Write(0, kTxnId);
  ASSERT_TRUE(s.IsAlreadyPresent());
  ASSERT_EQ(0, tablet_replica_->tablet()->metrics()->insert_ignore_errors->value());

  ASSERT_OK(Write(0, kTxnId, RowOperationsPB::INSERT_IGNORE));
  ASSERT_EQ(1, tablet_replica_->tablet()->metrics()->insert_ignore_errors->value());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(1, rows.size());
}

// Test that the live row count accounts for transactional MRSs.
TEST_F(TxnParticipantTest, TestLiveRowCountAccountsForTransactionalMRSs) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(Write(0));
  ASSERT_OK(Write(1, kTxnOne));
  ASSERT_OK(Write(2, kTxnTwo));
  ASSERT_EQ(1, tablet_replica_->CountLiveRowsNoFail());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_EQ(1, tablet_replica_->CountLiveRowsNoFail());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  ASSERT_EQ(2, tablet_replica_->CountLiveRowsNoFail());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_EQ(2, tablet_replica_->CountLiveRowsNoFail());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  ASSERT_EQ(3, tablet_replica_->CountLiveRowsNoFail());
  ASSERT_OK(Delete(1));
  ASSERT_OK(Delete(2));
  ASSERT_EQ(1, tablet_replica_->CountLiveRowsNoFail());
}

// Test that the MRS size metrics account for transactional MRSs.
TEST_F(TxnParticipantTest, TestSizeAccountsForTransactionalMRS) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_TRUE(tablet_replica_->tablet()->MemRowSetEmpty());

  auto* tablet = tablet_replica_->tablet();
  auto mrs_size_with_empty = tablet->MemRowSetSize();

  ASSERT_OK(Write(1, kTxnOne));
  ASSERT_OK(Write(2, kTxnTwo));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_TRUE(tablet->MemRowSetEmpty());

  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  auto mrs_size_with_one = tablet->MemRowSetSize();
  ASSERT_GT(mrs_size_with_one, mrs_size_with_empty);
  ASSERT_FALSE(tablet->MemRowSetEmpty());

  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  auto mrs_size_with_two = tablet->MemRowSetSize();
  ASSERT_GT(mrs_size_with_two, mrs_size_with_one);

  // The MRSs shouldn't be considered empty even if their rows are deleted,
  // since they still contain mutations.
  ASSERT_OK(Delete(1));
  ASSERT_OK(Delete(2));
  ASSERT_FALSE(tablet_replica_->tablet()->MemRowSetEmpty());
}

// Test that the MRS anchored WALs metric accounts for transactional MRSs.
TEST_F(TxnParticipantTest, TestWALsAnchoredAccountsForTransactionalMRS) {
  const auto mrs_wal_size = [&] {
    map<int64_t, int64_t> replay_size_map;
    CHECK_OK(tablet_replica_->GetReplaySizeMap(&replay_size_map));
    return tablet_replica_->tablet()->MemRowSetLogReplaySize(replay_size_map);
  };
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_TXN, -1));

  // Write a row and roll over onto a new WAL segment so there are bytes to GC.
  ASSERT_OK(Write(0, kTxnOne));
  ASSERT_OK(tablet_replica_->log()->WaitUntilAllFlushed());
  ASSERT_OK(tablet_replica_->log()->AllocateSegmentAndRollOverForTests());

  // Nothing should be considered anchored, as the transaction hasn't been
  // committed -- it thus wouldn't make sense to perform maintenance ops based
  // on WAL segments for the uncommitted write.
  ASSERT_EQ(0, mrs_wal_size());

  // Once we commit, we should see some GCable bytes.
  ASSERT_OK(Write(1, kTxnOne));
  ASSERT_OK(tablet_replica_->log()->WaitUntilAllFlushed());
  ASSERT_OK(tablet_replica_->log()->AllocateSegmentAndRollOverForTests());
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  auto mrs_wal_size_with_first_committed = mrs_wal_size();
  ASSERT_GT(mrs_wal_size_with_first_committed, 0);

  ASSERT_OK(Write(2, kTxnTwo));
  ASSERT_OK(tablet_replica_->log()->WaitUntilAllFlushed());
  ASSERT_OK(tablet_replica_->log()->AllocateSegmentAndRollOverForTests());
  auto mrs_wal_size_with_both_written = mrs_wal_size();

  // Despite not having committed the second transaction, we still wrote new
  // WAL segments, and that's enough to bump the MRS WALs anchored value.
  ASSERT_GT(mrs_wal_size_with_both_written, mrs_wal_size_with_first_committed);

  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));

  auto mrs_wal_size_with_both_committed = mrs_wal_size();
  ASSERT_EQ(mrs_wal_size_with_both_committed, mrs_wal_size_with_both_written);
}

// Test racing writes with commits, ensuring that we cease writing once
// beginning to commit.
TEST_F(TxnParticipantTest, TestRacingCommitAndWrite) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_TXN, -1));
  int first_error_row = -1;
  CountDownLatch first_write(1);
  thread t([&] {
    for (int row = 0 ;; row++) {
      Status s = Write(row, kTxnId);
      if (!s.ok()) {
        first_error_row = row;
        break;
      }
      first_write.CountDown();
    }
  });
  first_write.Wait();
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnId, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  t.join();
  ASSERT_GT(first_error_row, 0);
  ASSERT_EQ(first_error_row, tablet_replica_->CountLiveRowsNoFail());
}

// Test that the write metrics account for transactional rowsets.
TEST_F(TxnParticipantTest, TestMRSLookupsMetric) {
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_TXN, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnTwo, ParticipantOpPB::BEGIN_TXN, -1));

  // A non-transactional write should not check any MRSs -- it should just be
  // inserted into the main MRS.
  ASSERT_OK(Write(0));
  ASSERT_EQ(0, tablet_replica_->tablet()->metrics()->mrs_lookups->value());

  // A transactional write will check the main MRS before trying to insert to
  // the transactional MRS.
  ASSERT_OK(Write(1, kTxnOne));
  ASSERT_EQ(1, tablet_replica_->tablet()->metrics()->mrs_lookups->value());
  ASSERT_OK(Write(2, kTxnTwo));
  ASSERT_EQ(2, tablet_replica_->tablet()->metrics()->mrs_lookups->value());

  // Once a transaction is committed, its MRS and the main MRS will be checked
  // for new transactional writes.
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::BEGIN_COMMIT, -1));
  ASSERT_OK(CallParticipantOpCheckResp(kTxnOne, ParticipantOpPB::FINALIZE_COMMIT,
                                       clock()->Now().value()));
  ASSERT_OK(Write(3, kTxnTwo));
  ASSERT_EQ(4, tablet_replica_->tablet()->metrics()->mrs_lookups->value());

  // Non-transactional writes will only check the committed transactional MRS,
  // before attempting to insert to the main MRS.
  ASSERT_OK(Write(4));
  ASSERT_EQ(5, tablet_replica_->tablet()->metrics()->mrs_lookups->value());

  // Trying to delete a row that doesn't exist will consult just the committed
  // transactional MRS before attempting to delete from the main MRS.
  Status s = Delete(10);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(6, tablet_replica_->tablet()->metrics()->mrs_lookups->value());

  // Deleting a row that exists in a MRS, the committed transactional MRS is
  // checked, and the successful deletion from the MRS increments the lookup
  // value, regardless of which MRS the row is in.
  ASSERT_OK(Delete(0));
  ASSERT_EQ(8, tablet_replica_->tablet()->metrics()->mrs_lookups->value());
  ASSERT_OK(Delete(1));
  ASSERT_EQ(10, tablet_replica_->tablet()->metrics()->mrs_lookups->value());
}

struct ConcurrencyParams {
  int num_txns;
  int num_rows_per_thread;
};
class TxnParticipantConcurrencyTest : public TxnParticipantTest,
                                      public ::testing::WithParamInterface<ConcurrencyParams> {};

// Test inserting into multiple transactions from multiple threads.
TEST_P(TxnParticipantConcurrencyTest, TestConcurrentDisjointInsertsTxn) {
  const auto& params = GetParam();
  const auto& num_txns = params.num_txns;
  const int kNumThreads = 10;
  const auto& rows_per_thread = params.num_rows_per_thread;
  for (int txn_id = 0; txn_id < num_txns; txn_id++) {
    ASSERT_OK(CallParticipantOpCheckResp(txn_id, ParticipantOpPB::BEGIN_TXN, -1));
  }
  // Insert to multiple transactions concurrently.
  vector<thread> threads;
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([&, i] {
      for (int r = 0; r < rows_per_thread; r++) {
        int row = i * rows_per_thread + r;
        ASSERT_OK(Write(row, row % num_txns));
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  vector<string> rows;
  for (int txn_id = 0; txn_id < num_txns; txn_id++) {
    ASSERT_OK(CallParticipantOpCheckResp(txn_id, ParticipantOpPB::BEGIN_COMMIT, -1));
  }
  ASSERT_OK(IterateToStrings(&rows));
  ASSERT_EQ(0, rows.size());

  // As we commit our transactions, we should see more and more rows show up.
  for (int txn_id = 0; txn_id < num_txns; txn_id++) {
    ASSERT_OK(CallParticipantOpCheckResp(txn_id, ParticipantOpPB::FINALIZE_COMMIT,
                                         clock()->Now().value()));
    ASSERT_OK(IterateToStrings(&rows));
    ASSERT_EQ(kNumThreads * rows_per_thread * (txn_id + 1) / num_txns, rows.size());
  }
}
INSTANTIATE_TEST_SUITE_P(ConcurrencyParams, TxnParticipantConcurrencyTest,
    ::testing::Values(
      ConcurrencyParams{ /*num_txns*/1, /*num_rows_per_thread*/1 },
      ConcurrencyParams{ /*num_txns*/10, /*num_rows_per_thread*/1 },
      ConcurrencyParams{ /*num_txns*/1, /*num_rows_per_thread*/10 }
    ));

} // namespace tablet
} // namespace kudu
