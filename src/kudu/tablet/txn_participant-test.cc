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
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica-test-base.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_participant-test-util.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using kudu::consensus::ConsensusBootstrapInfo;
using kudu::pb_util::SecureShortDebugString;
using kudu::tserver::ParticipantRequestPB;
using kudu::tserver::ParticipantResponsePB;
using kudu::tserver::ParticipantOpPB;
using std::map;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

class TxnParticipantTest : public TabletReplicaTestBase {
 public:
  TxnParticipantTest()
      : TabletReplicaTestBase(Schema({ ColumnSchema("key", INT32) }, 1)) {}

  void SetUp() override {
    NO_FATALS(TabletReplicaTestBase::SetUp());
    ConsensusBootstrapInfo info;
    ASSERT_OK(StartReplicaAndWaitUntilLeader(info));
  }

  const TxnParticipant* txn_participant() const {
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
}

} // namespace tablet
} // namespace kudu
