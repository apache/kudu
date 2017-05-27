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

#include "kudu/consensus/raft_consensus_state.h"

#include "kudu/consensus/quorum_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

namespace kudu {
namespace consensus {

using std::string;
using std::unique_ptr;
using strings::Substitute;

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
  DCHECK(!msg.has_id()) << "Should not have an ID yet: " << SecureShortDebugString(msg);
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
      ConsensusStatePB cstate = ConsensusStateUnlocked();
      return Status::IllegalState(Substitute("Replica $0 is not leader of this config. Role: $1. "
                                             "Consensus state: $2",
                                             peer_uuid_,
                                             RaftPeerPB::Role_Name(role),
                                             SecureShortDebugString(cstate)));
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
                   SecureShortDebugString(GetCommittedConfigUnlocked()),
                   SecureShortDebugString(GetPendingConfigUnlocked())));
  }
  return Status::OK();
}

Status ReplicaState::SetPendingConfigUnlocked(const RaftConfigPB& new_config) {
  DCHECK(update_lock_.is_locked());
  RETURN_NOT_OK_PREPEND(VerifyRaftConfig(new_config, PENDING_CONFIG),
                        "Invalid config to set as pending");
  if (!new_config.unsafe_config_change()) {
    CHECK(!cmeta_->has_pending_config())
        << "Attempt to set pending config while another is already pending! "
        << "Existing pending config: " << SecureShortDebugString(cmeta_->pending_config()) << "; "
        << "Attempted new pending config: " << SecureShortDebugString(new_config);
  } else if (cmeta_->has_pending_config()) {
    LOG_WITH_PREFIX_UNLOCKED(INFO)
        << "Allowing unsafe config change even though there is a pending config! "
        << "Existing pending config: " << SecureShortDebugString(cmeta_->pending_config()) << "; "
        << "New pending config: " << SecureShortDebugString(new_config);
  }
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

Status ReplicaState::SetCommittedConfigUnlocked(const RaftConfigPB& config_to_commit) {
  TRACE_EVENT0("consensus", "ReplicaState::SetCommittedConfigUnlocked");
  DCHECK(update_lock_.is_locked());
  DCHECK(config_to_commit.IsInitialized());
  RETURN_NOT_OK_PREPEND(VerifyRaftConfig(config_to_commit, COMMITTED_CONFIG),
                        "Invalid config to set as committed");

  // Compare committed with pending configuration, ensure that they are the same.
  // In the event of an unsafe config change triggered by an administrator,
  // it is possible that the config being committed may not match the pending config
  // because unsafe config change allows multiple pending configs to exist.
  // Therefore we only need to validate that 'config_to_commit' matches the pending config
  // if the pending config does not have its 'unsafe_config_change' flag set.
  if (IsConfigChangePendingUnlocked()) {
    const RaftConfigPB& pending_config = GetPendingConfigUnlocked();
    if (!pending_config.unsafe_config_change()) {
      // Quorums must be exactly equal, even w.r.t. peer ordering.
      CHECK_EQ(GetPendingConfigUnlocked().SerializeAsString(),
               config_to_commit.SerializeAsString())
          << Substitute("New committed config must equal pending config, but does not. "
                        "Pending config: $0, committed config: $1",
                        SecureShortDebugString(pending_config),
                        SecureShortDebugString(config_to_commit));
    }
  }
  cmeta_->set_committed_config(config_to_commit);
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

}  // namespace consensus
}  // namespace kudu

