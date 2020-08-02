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

#include <glog/logging.h>
#include <google/protobuf/arena.h>

#include "kudu/clock/hybrid_clock.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/txn_participant.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

using kudu::consensus::CommitMsg;
using kudu::consensus::ReplicateMsg;
using kudu::consensus::OperationType;
using kudu::pb_util::SecureShortDebugString;
using kudu::tablet::TabletReplica;
using kudu::tserver::ParticipantOpPB;
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
  txn_ = txn_participant_->GetOrCreateTransaction(txn_id);
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

Status ParticipantOpState::ValidateOp() const {
  const auto& op = request()->op();
  DCHECK(txn_);
  switch (op.type()) {
    case ParticipantOpPB::BEGIN_TXN:
      return txn_->ValidateBeginTransaction();
    case ParticipantOpPB::BEGIN_COMMIT:
      return txn_->ValidateBeginCommit();
    case ParticipantOpPB::FINALIZE_COMMIT:
      return txn_->ValidateFinalize();
    case ParticipantOpPB::ABORT_TXN:
      return txn_->ValidateAbort();
    case ParticipantOpPB::UNKNOWN:
      return Status::InvalidArgument("unknown op type");
  }
  return Status::OK();
}

Status ParticipantOpState::PerformOp() {
  const auto& op = request()->op();
  Txn* txn = txn_.get();
  Status s;
  switch (op.type()) {
    // NOTE: these can currently never fail because we are only updating
    // metadata. When we begin validating write ops before committing, we'll
    // need to populate the response with errors.
    case ParticipantOpPB::BEGIN_TXN: {
      txn->BeginTransaction();
      break;
    }
    case ParticipantOpPB::BEGIN_COMMIT: {
      txn->BeginCommit();
      break;
    }
    case ParticipantOpPB::FINALIZE_COMMIT: {
      txn->FinalizeCommit(op.finalized_commit_timestamp());
      break;
    }
    case ParticipantOpPB::ABORT_TXN: {
      txn->AbortTransaction();
      break;
    }
    case ParticipantOpPB::UNKNOWN: {
      return Status::InvalidArgument("unknown op type");
    }
  }
  return Status::OK();
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
  TRACE("PREPARE: Finished.");
  return Status::OK();
}

Status ParticipantOp::Start() {
  DCHECK(!state_->has_timestamp());
  DCHECK(state_->consensus_round()->replicate_msg()->has_timestamp());
  state_->set_timestamp(Timestamp(state_->consensus_round()->replicate_msg()->timestamp()));
  TRACE("START. Timestamp: $0", clock::HybridClock::GetPhysicalValueMicros(state_->timestamp()));
  return Status::OK();
}

Status ParticipantOp::Apply(CommitMsg** commit_msg) {
  TRACE_EVENT0("op", "ParticipantOp::Apply");
  TRACE("APPLY: Starting.");
  CHECK_OK(state_->PerformOp());
  *commit_msg = google::protobuf::Arena::CreateMessage<CommitMsg>(state_->pb_arena());
  (*commit_msg)->set_op_type(OperationType::PARTICIPANT_OP);
  TRACE("APPLY: Finished.");
  return Status::OK();
}

void ParticipantOp::Finish(OpResult result) {
  auto txn_id = state_->request()->op().txn_id();
  state_->ReleaseTxn();
  TxnParticipant* txn_participant = state_->txn_participant_;
  if (PREDICT_FALSE(result == Op::ABORTED)) {
    txn_participant->ClearIfInitFailed(txn_id);
    TRACE("FINISH: Op aborted");
    return;
  }
  DCHECK_EQ(result, Op::COMMITTED);
  // TODO(awong): when implementing transaction cleanup on participants, clean
  // up finalized and aborted transactions here.
  TRACE("FINISH: Op committed");
}

string ParticipantOp::ToString() const {
  return Substitute("ParticipantOp [type=$0, state=$1]",
      DriverType_Name(type()), state_->ToString());
}

} // namespace tablet
} // namespace kudu
