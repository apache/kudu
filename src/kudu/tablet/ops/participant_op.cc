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

#include "kudu/tablet/ops/participant_op.h"

#include <memory>
#include <ostream>

#include <glog/logging.h>
#include <google/protobuf/arena.h>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/txn_participant.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

using kudu::consensus::CommitMsg;
using kudu::consensus::ReplicateMsg;
using kudu::consensus::OperationType;
using kudu::consensus::OpId;
using kudu::pb_util::SecureShortDebugString;
using kudu::tablet::TabletReplica;
using kudu::tserver::ParticipantOpPB;
using kudu::tserver::TabletServerErrorPB;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
class rw_semaphore;

namespace tablet {

ParticipantOpState::ParticipantOpState(TabletReplica* tablet_replica,
                                       TxnParticipant* txn_participant,
                                       const tserver::ParticipantRequestPB* request,
                                       tserver::ParticipantResponsePB* response)
    : OpState(tablet_replica),
      txn_participant_(txn_participant),
      request_(DCHECK_NOTNULL(request)),
      response_(response) {}

void ParticipantOpState::AcquireTxnAndLock() {
  DCHECK(!txn_lock_);
  DCHECK(!txn_);
  int64_t txn_id = request_->op().txn_id();
  txn_ = txn_participant_->GetOrCreateTransaction(txn_id,
                                                  tablet_replica_->log_anchor_registry().get());
  txn_->AcquireWriteLock(&txn_lock_);
}

void ParticipantOpState::ReleaseTxn() {
  if (txn_lock_.owns_lock()) {
    txn_lock_ = std::unique_lock<rw_semaphore>();
  }
  txn_.reset();
  TRACE("Released txn lock");
}

string ParticipantOpState::ToString() const {
  const string ts_str = has_timestamp() ? timestamp().ToString() : "<unassigned>";
  DCHECK(request_);
  return Substitute("ParticipantOpState $0 [op_id=($1), ts=$2, type=$3]",
      this, SecureShortDebugString(op_id()), ts_str,
      ParticipantOpPB::ParticipantOpType_Name(request_->op().type()));
}

Status ParticipantOpState::ValidateOp() {
  const auto& op = request()->op();
  DCHECK(txn_);
  TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
  Status s;
  switch (op.type()) {
    case ParticipantOpPB::BEGIN_TXN:
      s = txn_->ValidateBeginTransaction(&code);
      break;
    case ParticipantOpPB::BEGIN_COMMIT: {
      Timestamp begin_commit_ts;
      s = txn_->ValidateBeginCommit(&code, &begin_commit_ts);
      if (PREDICT_FALSE(begin_commit_ts != Timestamp::kInvalidTimestamp)) {
        DCHECK(s.IsIllegalState()) << s.ToString();
        response_->set_timestamp(begin_commit_ts.value());
      }
      break;
    }
    case ParticipantOpPB::FINALIZE_COMMIT:
      s = txn_->ValidateFinalize(&code);
      break;
    case ParticipantOpPB::ABORT_TXN:
      s = txn_->ValidateAbort(&code);
      break;
    default:
      s = Status::InvalidArgument("unknown op type");
      break;
  }
  if (PREDICT_FALSE(!s.ok())) {
    completion_callback()->set_error(s, code);
    return s;
  }
  return Status::OK();
}

void ParticipantOpState::SetMvccOp(unique_ptr<ScopedOp> mvcc_op) {
  DCHECK_EQ(ParticipantOpPB::BEGIN_COMMIT, request()->op().type());
  DCHECK(nullptr == begin_commit_mvcc_op_);
  begin_commit_mvcc_op_ = std::move(mvcc_op);
}

void ParticipantOpState::ReleaseMvccOpToTxn() {
  DCHECK_EQ(ParticipantOpPB::BEGIN_COMMIT, request()->op().type());
  DCHECK(begin_commit_mvcc_op_);
  txn_->SetCommitOp(std::move(begin_commit_mvcc_op_));
}

void ParticipantOp::NewReplicateMsg(unique_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(OperationType::PARTICIPANT_OP);
  (*replicate_msg)->mutable_participant_request()->CopyFrom(*state()->request());
  if (state()->are_results_tracked()) {
    (*replicate_msg)->mutable_request_id()->CopyFrom(state()->request_id());
  }
}

Status ParticipantOp::Prepare() {
  TRACE_EVENT0("op", "ParticipantOp::Prepare");
  TRACE("PREPARE: Starting.");
  state_->AcquireTxnAndLock();
  RETURN_NOT_OK(state_->ValidateOp());

  const auto& op = state_->request()->op();
  auto* replica = state_->tablet_replica();

  // Before we assign a timestamp, bump the clock so further ops get assigned
  // higher timestamps (including this one).
  switch (op.type()) {
    case ParticipantOpPB::BEGIN_COMMIT:
      // To avoid inconsistencies, TxnOpDispatcher should not contain any
      // pending write requests at this point. Those pending requests must be
      // submitted and replied accordingly before BEGIN_COMMIT can be processed.
      // Even if UnregisterTxnOpDispatcher() returns non-OK, the TxnOpDispatcher
      // is marked for removal, so no write requests are accepted by the replica
      // in the context of the specified transaction after a call to
      // TabletReplica::UnregisterTxnOpDispatcher().
      RETURN_NOT_OK(replica->UnregisterTxnOpDispatcher(
          op.txn_id(), false/*abort_pending_ops*/));
      break;
    case ParticipantOpPB::FINALIZE_COMMIT:
      if (type() == consensus::LEADER) {
        DCHECK(!state_->consensus_round()->replicate_msg()->has_timestamp());
        RETURN_NOT_OK(state_->tablet_replica()->time_manager()->
            UpdateClockAndLastAssignedTimestamp(state_->commit_timestamp()));
      }
      break;
    case ParticipantOpPB::ABORT_TXN:
      RETURN_NOT_OK(replica->UnregisterTxnOpDispatcher(
          op.txn_id(), true/*abort_pending_ops*/));
      break;
    default:
      // Nothing to do in all other cases.
      break;
  }
  TRACE("PREPARE: Finished.");
  return Status::OK();
}

Status ParticipantOp::Start() {
  DCHECK(!state_->has_timestamp());
  DCHECK(state_->consensus_round()->replicate_msg()->has_timestamp());
  state_->set_timestamp(Timestamp(state_->consensus_round()->replicate_msg()->timestamp()));
  if (state_->request()->op().type() == ParticipantOpPB::BEGIN_COMMIT) {
    // When beginning to commit, register an MVCC op so scanners at later
    // timestamps wait for the commit to complete.
    state_->tablet_replica()->tablet()->StartOp(state_.get());
  }
  TRACE("START. Timestamp: $0", clock::HybridClock::GetPhysicalValueMicros(state_->timestamp()));
  return Status::OK();
}

Status ParticipantOpState::PerformOp(const consensus::OpId& op_id, Tablet* tablet) {
  const auto& op = request()->op();
  const auto& op_type = op.type();
  Status s;
  switch (op_type) {
    // NOTE: these can currently never fail because we are only updating
    // metadata. When we begin validating write ops before committing, we'll
    // need to populate the response with errors.
    case ParticipantOpPB::BEGIN_TXN: {
      tablet->BeginTransaction(txn_.get(), op_id);
      break;
    }
    case ParticipantOpPB::BEGIN_COMMIT: {
      tablet->BeginCommit(txn_.get(), begin_commit_mvcc_op_->timestamp(), op_id);
      ReleaseMvccOpToTxn();
      break;
    }
    case ParticipantOpPB::FINALIZE_COMMIT: {
      DCHECK(op.has_finalized_commit_timestamp());
      const auto& commit_ts = op.finalized_commit_timestamp();
      tablet->CommitTransaction(txn_.get(), Timestamp(commit_ts), op_id);
      // NOTE: we may not have a commit op if we are bootstrapping and we GCed
      // the BEGIN_COMMIT op before flushing the finalized commit timestamp.
      if (txn_->commit_op()) {
        txn_->commit_op()->FinishApplying();
      }
      break;
    }
    case ParticipantOpPB::ABORT_TXN: {
      tablet->AbortTransaction(txn_.get(), op_id);
      // NOTE: we may not have a commit op if we are aborting before beginning
      // to commit.
      if (txn_->commit_op()) {
        txn_->commit_op()->Abort();
      }
      break;
    }
    case ParticipantOpPB::UNKNOWN: {
      return Status::InvalidArgument("unknown op type");
    }
  }
  return Status::OK();
}

Status ParticipantOp::Apply(CommitMsg** commit_msg) {
  TRACE_EVENT0("op", "ParticipantOp::Apply");
  TRACE("APPLY: Starting.");
  state_->tablet_replica()->tablet()->StartApplying(state_.get());
  CHECK_OK(state_->PerformOp(state()->op_id(), state_->tablet_replica()->tablet()));
  *commit_msg = google::protobuf::Arena::CreateMessage<CommitMsg>(state_->pb_arena());
  (*commit_msg)->set_op_type(OperationType::PARTICIPANT_OP);
  TRACE("APPLY: Finished.");
  return Status::OK();
}

void ParticipantOp::Finish(OpResult result) {
  auto txn_id = state_->request()->op().txn_id();
  state_->ReleaseTxn();
  TxnParticipant* txn_participant = state_->txn_participant_;

  // If the transaction is complete, get rid of the in-flight Txn.
  txn_participant->ClearIfComplete(txn_id);

  if (PREDICT_FALSE(result == Op::ABORTED)) {
    // NOTE: The only way we end up with an init failure is if we ran a
    // BEGIN_TXN op but aborted mid-way, leaving the Txn in the kInitialized
    // state and no further ops attempting to drive the state change to kOpen.
    txn_participant->ClearIfInitFailed(txn_id);
    TRACE("FINISH: Op aborted");
    return;
  }
  DCHECK_EQ(result, Op::APPLIED);
  TRACE("FINISH: Op applied");
}

string ParticipantOp::ToString() const {
  return Substitute("ParticipantOp [type=$0, state=$1]",
      DriverType_Name(type()), state_->ToString());
}

} // namespace tablet
} // namespace kudu
