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

#include <algorithm>
#include <memory>

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus_state.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/logging.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace consensus {

using std::string;
using std::unique_ptr;
using strings::Substitute;
using strings::SubstituteAndAppend;

//////////////////////////////////////////////////
// ReplicaState
//////////////////////////////////////////////////

ReplicaState::ReplicaState(ConsensusOptions options, string peer_uuid,
                           unique_ptr<ConsensusMetadata> cmeta)
    : options_(std::move(options)),
      peer_uuid_(std::move(peer_uuid)),
      cmeta_(std::move(cmeta)),
      state_(kInitialized) {
  CHECK(cmeta_) << "ConsensusMeta passed as NULL";
}

Status ReplicaState::StartUnlocked(const OpId& last_id_in_wal) {
  DCHECK(update_lock_.is_locked());

  // Our last persisted term can be higher than the last persisted operation
  // (i.e. if we called an election) but reverse should never happen.
  if (last_id_in_wal.term() > GetCurrentTermUnlocked()) {
    return Status::Corruption(Substitute(
        "The last op in the WAL with id $0 has a term ($1) that is greater "
        "than the latest recorded term, which is $2",
        OpIdToString(last_id_in_wal),
        last_id_in_wal.term(),
        GetCurrentTermUnlocked()));
  }

  state_ = kRunning;
  return Status::OK();
}

Status ReplicaState::LockForStart(UniqueLock* lock) const {
  ThreadRestrictions::AssertWaitAllowed();
  UniqueLock l(update_lock_);
  CHECK_EQ(state_, kInitialized) << "Illegal state for Start()."
      << " Replica is not in kInitialized state";
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::LockForRead(UniqueLock* lock) const {
  ThreadRestrictions::AssertWaitAllowed();
  UniqueLock l(update_lock_);
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::LockForReplicate(UniqueLock* lock, const ReplicateMsg& msg) const {
  ThreadRestrictions::AssertWaitAllowed();
  DCHECK(!msg.has_id()) << "Should not have an ID yet: " << msg.ShortDebugString();
  UniqueLock l(update_lock_);
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Replica not in running state");
  }

  RETURN_NOT_OK(CheckActiveLeaderUnlocked());
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::LockForCommit(UniqueLock* lock) const {
  TRACE_EVENT0("consensus", "ReplicaState::LockForCommit");
  ThreadRestrictions::AssertWaitAllowed();
  UniqueLock l(update_lock_);
  if (PREDICT_FALSE(state_ != kRunning && state_ != kShuttingDown)) {
    return Status::IllegalState("Replica not in running state");
  }
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::CheckActiveLeaderUnlocked() const {
  RaftPeerPB::Role role = GetActiveRoleUnlocked();
  switch (role) {
    case RaftPeerPB::LEADER:
      return Status::OK();
    default:
      ConsensusStatePB cstate = ConsensusStateUnlocked(CONSENSUS_CONFIG_ACTIVE);
      return Status::IllegalState(Substitute("Replica $0 is not leader of this config. Role: $1. "
                                             "Consensus state: $2",
                                             peer_uuid_,
                                             RaftPeerPB::Role_Name(role),
                                             cstate.ShortDebugString()));
  }
}

Status ReplicaState::LockForConfigChange(UniqueLock* lock) const {
  TRACE_EVENT0("consensus", "ReplicaState::LockForConfigChange");

  ThreadRestrictions::AssertWaitAllowed();
  UniqueLock l(update_lock_);
  // Can only change the config on running replicas.
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Unable to lock ReplicaState for config change",
                                Substitute("State = $0", state_));
  }
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::LockForUpdate(UniqueLock* lock) const {
  TRACE_EVENT0("consensus", "ReplicaState::LockForUpdate");
  ThreadRestrictions::AssertWaitAllowed();
  UniqueLock l(update_lock_);
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Replica not in running state");
  }
  if (!IsRaftConfigVoter(peer_uuid_, cmeta_->active_config())) {
    LOG_WITH_PREFIX_UNLOCKED(INFO) << "Allowing update even though not a member of the config";
  }
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::LockForShutdown(UniqueLock* lock) {
  TRACE_EVENT0("consensus", "ReplicaState::LockForShutdown");
  ThreadRestrictions::AssertWaitAllowed();
  UniqueLock l(update_lock_);
  if (state_ != kShuttingDown && state_ != kShutDown) {
    state_ = kShuttingDown;
  }
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::ShutdownUnlocked() {
  DCHECK(update_lock_.is_locked());
  CHECK_EQ(state_, kShuttingDown);
  state_ = kShutDown;
  return Status::OK();
}

RaftPeerPB::Role ReplicaState::GetActiveRoleUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->active_role();
}

bool ReplicaState::IsConfigChangePendingUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->has_pending_config();
}

Status ReplicaState::CheckNoConfigChangePendingUnlocked() const {
  DCHECK(update_lock_.is_locked());
  if (IsConfigChangePendingUnlocked()) {
    return Status::IllegalState(
        Substitute("RaftConfig change currently pending. Only one is allowed at a time.\n"
                   "  Committed config: $0.\n  Pending config: $1",
                   GetCommittedConfigUnlocked().ShortDebugString(),
                   GetPendingConfigUnlocked().ShortDebugString()));
  }
  return Status::OK();
}

Status ReplicaState::SetPendingConfigUnlocked(const RaftConfigPB& new_config) {
  DCHECK(update_lock_.is_locked());
  RETURN_NOT_OK_PREPEND(VerifyRaftConfig(new_config, UNCOMMITTED_QUORUM),
                        "Invalid config to set as pending");
  CHECK(!cmeta_->has_pending_config())
      << "Attempt to set pending config while another is already pending! "
      << "Existing pending config: " << cmeta_->pending_config().ShortDebugString() << "; "
      << "Attempted new pending config: " << new_config.ShortDebugString();
  cmeta_->set_pending_config(new_config);
  return Status::OK();
}

void ReplicaState::ClearPendingConfigUnlocked() {
  cmeta_->clear_pending_config();
}

const RaftConfigPB& ReplicaState::GetPendingConfigUnlocked() const {
  DCHECK(update_lock_.is_locked());
  CHECK(IsConfigChangePendingUnlocked()) << "No pending config";
  return cmeta_->pending_config();
}

Status ReplicaState::SetCommittedConfigUnlocked(const RaftConfigPB& committed_config) {
  TRACE_EVENT0("consensus", "ReplicaState::SetCommittedConfigUnlocked");
  DCHECK(update_lock_.is_locked());
  DCHECK(committed_config.IsInitialized());
  RETURN_NOT_OK_PREPEND(VerifyRaftConfig(committed_config, COMMITTED_QUORUM),
                        "Invalid config to set as committed");

  // Compare committed with pending configuration, ensure they are the same.
  DCHECK(cmeta_->has_pending_config());
  const RaftConfigPB& pending_config = GetPendingConfigUnlocked();
  // Quorums must be exactly equal, even w.r.t. peer ordering.
  CHECK_EQ(GetPendingConfigUnlocked().SerializeAsString(), committed_config.SerializeAsString())
      << Substitute("New committed config must equal pending config, but does not. "
                    "Pending config: $0, committed config: $1",
                    pending_config.ShortDebugString(), committed_config.ShortDebugString());

  cmeta_->set_committed_config(committed_config);
  cmeta_->clear_pending_config();
  CHECK_OK(cmeta_->Flush());
  return Status::OK();
}

const RaftConfigPB& ReplicaState::GetCommittedConfigUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->committed_config();
}

const RaftConfigPB& ReplicaState::GetActiveConfigUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->active_config();
}

Status ReplicaState::SetCurrentTermUnlocked(int64_t new_term,
                                            FlushToDisk flush) {
  TRACE_EVENT1("consensus", "ReplicaState::SetCurrentTermUnlocked",
               "term", new_term);
  DCHECK(update_lock_.is_locked());
  if (PREDICT_FALSE(new_term <= GetCurrentTermUnlocked())) {
    return Status::IllegalState(
        Substitute("Cannot change term to a term that is lower than or equal to the current one. "
                   "Current: $0, Proposed: $1", GetCurrentTermUnlocked(), new_term));
  }
  cmeta_->set_current_term(new_term);
  cmeta_->clear_voted_for();
  if (flush == FLUSH_TO_DISK) {
    CHECK_OK(cmeta_->Flush());
  }
  ClearLeaderUnlocked();
  return Status::OK();
}

const int64_t ReplicaState::GetCurrentTermUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->current_term();
}

void ReplicaState::SetLeaderUuidUnlocked(const std::string& uuid) {
  DCHECK(update_lock_.is_locked());
  cmeta_->set_leader_uuid(uuid);
}

const string& ReplicaState::GetLeaderUuidUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->leader_uuid();
}

const bool ReplicaState::HasVotedCurrentTermUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->has_voted_for();
}

Status ReplicaState::SetVotedForCurrentTermUnlocked(const std::string& uuid) {
  TRACE_EVENT1("consensus", "ReplicaState::SetVotedForCurrentTermUnlocked",
               "uuid", uuid);
  DCHECK(update_lock_.is_locked());
  cmeta_->set_voted_for(uuid);
  CHECK_OK(cmeta_->Flush());
  return Status::OK();
}

const std::string& ReplicaState::GetVotedForCurrentTermUnlocked() const {
  DCHECK(update_lock_.is_locked());
  DCHECK(cmeta_->has_voted_for());
  return cmeta_->voted_for();
}

const string& ReplicaState::GetPeerUuid() const {
  return peer_uuid_;
}

const ConsensusOptions& ReplicaState::GetOptions() const {
  return options_;
}

string ReplicaState::LogPrefix() {
  ReplicaState::UniqueLock lock;
  CHECK_OK(LockForRead(&lock));
  return LogPrefixUnlocked();
}

string ReplicaState::LogPrefixUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return Substitute("T $0 P $1 [term $2 $3]: ",
                    options_.tablet_id,
                    peer_uuid_,
                    GetCurrentTermUnlocked(),
                    RaftPeerPB::Role_Name(GetActiveRoleUnlocked()));
}

string ReplicaState::LogPrefixThreadSafe() const {
  return Substitute("T $0 P $1: ",
                    options_.tablet_id,
                    peer_uuid_);
}

ReplicaState::State ReplicaState::state() const {
  DCHECK(update_lock_.is_locked());
  return state_;
}

string ReplicaState::ToString() const {
  ThreadRestrictions::AssertWaitAllowed();
  ReplicaState::UniqueLock lock(update_lock_);
  return ToStringUnlocked();
}

string ReplicaState::ToStringUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return Substitute("Replica: $0, State: $1, Role: $2",
                    peer_uuid_, state_, RaftPeerPB::Role_Name(GetActiveRoleUnlocked()));
}

//------------------------------------------------------------
// PendingRounds
// TODO(todd): move to its own file
//------------------------------------------------------------

PendingRounds::PendingRounds(string log_prefix)
    : log_prefix_(std::move(log_prefix)),
      last_committed_op_id_(MinimumOpId()) {
}
PendingRounds::~PendingRounds() {
}

Status PendingRounds::CancelPendingTransactions() {
  ThreadRestrictions::AssertWaitAllowed();
  if (pending_txns_.empty()) {
    return Status::OK();
  }

  LOG_WITH_PREFIX(INFO) << "Trying to abort " << pending_txns_.size()
                                 << " pending transactions.";
  for (const auto& txn : pending_txns_) {
    const scoped_refptr<ConsensusRound>& round = txn.second;
    // We cancel only transactions whose applies have not yet been triggered.
    LOG_WITH_PREFIX(INFO) << "Aborting transaction as it isn't in flight: "
                                   << txn.second->replicate_msg()->ShortDebugString();
    round->NotifyReplicationFinished(Status::Aborted("Transaction aborted"));
  }
  return Status::OK();
}

void PendingRounds::AbortOpsAfter(int64_t index) {
  LOG_WITH_PREFIX(INFO) << "Aborting all transactions after (but not including) "
                                 << index;

  DCHECK_GE(index, 0);
  OpId new_preceding;

  auto iter = pending_txns_.lower_bound(index);

  // Either the new preceding id is in the pendings set or it must be equal to the
  // committed index since we can't truncate already committed operations.
  if (iter != pending_txns_.end() && (*iter).first == index) {
    new_preceding = (*iter).second->replicate_msg()->id();
    ++iter;
  } else {
    CHECK_EQ(index, last_committed_op_id_.index());
    new_preceding = last_committed_op_id_;
  }

  for (; iter != pending_txns_.end();) {
    const scoped_refptr<ConsensusRound>& round = (*iter).second;
    auto op_type = round->replicate_msg()->op_type();
    LOG_WITH_PREFIX(INFO)
        << "Aborting uncommitted " << OperationType_Name(op_type)
        << " operation due to leader change: " << round->replicate_msg()->id();

    round->NotifyReplicationFinished(Status::Aborted("Transaction aborted by new leader"));
    // Erase the entry from pendings.
    pending_txns_.erase(iter++);
  }
}

Status PendingRounds::AddPendingOperation(const scoped_refptr<ConsensusRound>& round) {
  InsertOrDie(&pending_txns_, round->replicate_msg()->id().index(), round);
  return Status::OK();
}

scoped_refptr<ConsensusRound> PendingRounds::GetPendingOpByIndexOrNull(int64_t index) {
  return FindPtrOrNull(pending_txns_, index);
}

bool PendingRounds::IsOpCommittedOrPending(const OpId& op_id, bool* term_mismatch) {

  *term_mismatch = false;

  if (op_id.index() <= GetCommittedIndex()) {
    return true;
  }

  scoped_refptr<ConsensusRound> round = GetPendingOpByIndexOrNull(op_id.index());
  if (!round) {
    return false;
  }

  if (round->id().term() != op_id.term()) {
    *term_mismatch = true;
    return false;
  }
  return true;
}

OpId PendingRounds::GetLastPendingTransactionOpId() const {
  return pending_txns_.empty()
      ? MinimumOpId() : (--pending_txns_.end())->second->id();
}

Status PendingRounds::AdvanceCommittedIndex(int64_t committed_index) {
  // If we already committed up to (or past) 'id' return.
  // This can happen in the case that multiple UpdateConsensus() calls end
  // up in the RPC queue at the same time, and then might get interleaved out
  // of order.
  if (last_committed_op_id_.index() >= committed_index) {
    VLOG_WITH_PREFIX(1)
      << "Already marked ops through " << last_committed_op_id_ << " as committed. "
      << "Now trying to mark " << committed_index << " which would be a no-op.";
    return Status::OK();
  }

  if (pending_txns_.empty()) {
    LOG(ERROR) << "Advancing commit index to " << committed_index
               << " from " << last_committed_op_id_
               << " we have no pending txns"
               << GetStackTrace();
    VLOG_WITH_PREFIX(1) << "No transactions to mark as committed up to: "
                                 << committed_index;
    return Status::OK();
  }

  // Start at the operation after the last committed one.
  auto iter = pending_txns_.upper_bound(last_committed_op_id_.index());
  // Stop at the operation after the last one we must commit.
  auto end_iter = pending_txns_.upper_bound(committed_index);
  CHECK(iter != pending_txns_.end());

  VLOG_WITH_PREFIX(1) << "Last triggered apply was: "
      <<  last_committed_op_id_
      << " Starting to apply from log index: " << (*iter).first;

  while (iter != end_iter) {
    scoped_refptr<ConsensusRound> round = (*iter).second; // Make a copy.
    DCHECK(round);
    const OpId& current_id = round->id();

    if (PREDICT_TRUE(!OpIdEquals(last_committed_op_id_, MinimumOpId()))) {
      CHECK_OK(CheckOpInSequence(last_committed_op_id_, current_id));
    }

    pending_txns_.erase(iter++);
    last_committed_op_id_ = round->id();
    round->NotifyReplicationFinished(Status::OK());
  }

  return Status::OK();
}

Status PendingRounds::SetInitialCommittedOpId(const OpId& committed_op) {
  CHECK_EQ(last_committed_op_id_.index(), 0);
  if (!pending_txns_.empty()) {
    int64_t first_pending_index = pending_txns_.begin()->first;
    if (committed_op.index() < first_pending_index) {
      if (committed_op.index() != first_pending_index - 1) {
        return Status::Corruption(Substitute(
            "pending operations should start at first operation "
            "after the committed operation (committed=$0, first pending=$1)",
            OpIdToString(committed_op), first_pending_index));
      }
      last_committed_op_id_ = committed_op;
    }

    RETURN_NOT_OK(AdvanceCommittedIndex(committed_op.index()));
    CHECK_EQ(last_committed_op_id_.ShortDebugString(),
             committed_op.ShortDebugString());

  } else {
    last_committed_op_id_ = committed_op;
  }
  return Status::OK();
}

Status PendingRounds::CheckOpInSequence(const OpId& previous, const OpId& current) {
  if (current.term() < previous.term()) {
    return Status::Corruption(Substitute("New operation's term is not >= than the previous "
        "op's term. Current: $0. Previous: $1", OpIdToString(current), OpIdToString(previous)));
  }
  if (current.index() != previous.index() + 1) {
    return Status::Corruption(Substitute("New operation's index does not follow the previous"
        " op's index. Current: $0. Previous: $1", OpIdToString(current), OpIdToString(previous)));
  }
  return Status::OK();
}

int64_t PendingRounds::GetCommittedIndex() const {
  return last_committed_op_id_.index();
}

int64_t PendingRounds::GetTermWithLastCommittedOp() const {
  return last_committed_op_id_.term();
}

int PendingRounds::GetNumPendingTxns() const {
  return pending_txns_.size();
}

}  // namespace consensus
}  // namespace kudu

