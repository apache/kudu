// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <algorithm>
#include <boost/foreach.hpp>

#include "kudu/consensus/log_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus_state.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/logging.h"
#include "kudu/util/status.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace consensus {

using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using std::string;
using std::tr1::shared_ptr;
using std::tr1::unordered_set;
using strings::Substitute;
using strings::SubstituteAndAppend;

//////////////////////////////////////////////////
// QuorumState
//////////////////////////////////////////////////

gscoped_ptr<QuorumState> QuorumState::Build(const QuorumPB& quorum, const string& self_uuid) {
  // Default this peer's role to non-participant.
  QuorumPeerPB::Role role = QuorumPeerPB::NON_PARTICIPANT;

  // Try to find the role set in the provided quorum.
  std::tr1::unordered_set<string> voting_peers;
  string leader_uuid;
  BOOST_FOREACH(const QuorumPeerPB& peer_pb, quorum.peers()) {
    if (peer_pb.permanent_uuid() == self_uuid) {
      role = peer_pb.role();
    }
    if (peer_pb.role() == QuorumPeerPB::LEADER ||
        peer_pb.role() == QuorumPeerPB::FOLLOWER) {
      voting_peers.insert(peer_pb.permanent_uuid());
    }
    if (peer_pb.role() == QuorumPeerPB::LEADER) {
      leader_uuid = peer_pb.permanent_uuid();
    }

  }

  // TODO: Calculating the majority from the number of peers can cause problems
  // without joint consensus. We should add a configuration parameter to
  // QuorumPB defining what constitutes the majority.
  int majority_size = (voting_peers.size() / 2) + 1;
  int quorum_size = quorum.peers_size();
  int64_t config_seqno = quorum.seqno();

  gscoped_ptr<QuorumState> state(new QuorumState(role, leader_uuid, voting_peers,
                                                 majority_size, quorum_size, config_seqno));
  return state.Pass();
}

QuorumState::QuorumState(metadata::QuorumPeerPB::Role role,
                         const std::string& leader_uuid,
                         const std::tr1::unordered_set<std::string>& voting_peers,
                         int majority_size,
                         int quorum_size,
                         int64_t config_seqno)
  : role(role),
    leader_uuid(leader_uuid),
    voting_peers(voting_peers),
    majority_size(majority_size),
    quorum_size(quorum_size),
    config_seqno(config_seqno) {
}

//////////////////////////////////////////////////
// ReplicaState
//////////////////////////////////////////////////

ReplicaState::ReplicaState(const ConsensusOptions& options,
                           const string& peer_uuid,
                           gscoped_ptr<ConsensusMetadata> cmeta,
                           ReplicaTransactionFactory* txn_factory)
  : options_(options),
    peer_uuid_(peer_uuid),
    cmeta_(cmeta.Pass()),
    next_index_(0),
    txn_factory_(txn_factory),
    state_(kInitialized) {
  CHECK(cmeta_) << "ConsensusMeta passed as NULL";
  UniqueLock l(&update_lock_);
  // Now that we know the peer UUID, refresh acting state from persistent state.
  ResetActiveQuorumStateUnlocked(GetCommittedQuorumUnlocked());
}

Status ReplicaState::StartUnlocked(const OpId& last_id_in_wal) {
  DCHECK(update_lock_.is_locked());

  // Our last persisted term can be higher than the last persisted operation
  // (i.e. if we called an election) but reverse should never happen.
  CHECK_LE(last_id_in_wal.term(), cmeta_->mutable_pb()->current_term())
      << "Last op in wal " << last_id_in_wal.term()
      << "has a term  which is greater than last recorded term "
      << cmeta_->mutable_pb()->current_term();

  next_index_ = last_id_in_wal.index() + 1;
  received_op_id_.CopyFrom(last_id_in_wal);
  last_committed_index_.CopyFrom(MinimumOpId());

  state_ = kRunning;
  return Status::OK();
}

Status ReplicaState::LockForStart(UniqueLock* lock) const {
  UniqueLock l(&update_lock_);
  CHECK_EQ(state_, kInitialized) << "Illegal state for Start()."
      << " Replica is not in kInitialized state";
  lock->swap(&l);
  return Status::OK();
}

Status ReplicaState::LockForRead(UniqueLock* lock) const {
  UniqueLock l(&update_lock_);
  lock->swap(&l);
  return Status::OK();
}

Status ReplicaState::LockForReplicate(UniqueLock* lock, const ReplicateMsg& msg) const {
  DCHECK(!msg.has_id()) << "Should not have an ID yet: " << msg.ShortDebugString();
  UniqueLock l(&update_lock_);
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Replica not in running state");
  }
  switch (active_quorum_state_->role) {
    case QuorumPeerPB::LEADER:
      lock->swap(&l);
      return Status::OK();
    case QuorumPeerPB::CANDIDATE:
      if (msg.op_type() != CHANGE_CONFIG_OP) {
        return Status::IllegalState("Only a change config round can be pushed while CANDIDATE.");
      }
      // TODO support true config change. Right now we only allow
      // replicate calls while CANDIDATE if our term is 0, meaning
      // we're the first CANDIDATE/LEADER of the quorum.
      CHECK_EQ(GetCurrentTermUnlocked(), 0);
      lock->swap(&l);
      return Status::OK();
    default:
      return Status::IllegalState(Substitute("Replica $0 is not leader of this quorum. Role: $1. "
                                             "Quorum: $2",
                                             peer_uuid_,
                                             QuorumPeerPB::Role_Name(active_quorum_state_->role),
                                             GetActiveQuorumUnlocked().ShortDebugString()));
  }
}

Status ReplicaState::LockForCommit(UniqueLock* lock) const {
  UniqueLock l(&update_lock_);
  if (PREDICT_FALSE(state_ != kRunning && state_ != kShuttingDown)) {
    return Status::IllegalState("Replica not in running state");
  }
  lock->swap(&l);
  return Status::OK();
}

Status ReplicaState::LockForMajorityReplicatedIndexUpdate(
    UniqueLock* lock) const {
  UniqueLock l(&update_lock_);

  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Replica not in running state");
  }

  if (PREDICT_FALSE(active_quorum_state_->role != QuorumPeerPB::CANDIDATE &&
                    active_quorum_state_->role != QuorumPeerPB::LEADER)) {
    return Status::IllegalState("Replica not LEADER or CANDIDATE");
  }
  lock->swap(&l);
  return Status::OK();
}

Status ReplicaState::LockForConfigChange(UniqueLock* lock) const {
  UniqueLock l(&update_lock_);
  // Can only change the config on running replicas.
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Unable to lock ReplicaState for config change",
                                Substitute("State = $0", state_));
  }
  lock->swap(&l);
  return Status::OK();
}

Status ReplicaState::LockForUpdate(UniqueLock* lock) const {
  UniqueLock l(&update_lock_);
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Replica not in running state");
  }
  switch (active_quorum_state_->role) {
    case QuorumPeerPB::NON_PARTICIPANT:
      return Status::IllegalState("Replica is not a participant of this quorum.");
    default:
      lock->swap(&l);
      return Status::OK();
  }
}

Status ReplicaState::LockForShutdown(UniqueLock* lock) {
  UniqueLock l(&update_lock_);
  if (state_ != kShuttingDown && state_ != kShutDown) {
    state_ = kShuttingDown;
  }
  lock->swap(&l);
  return Status::OK();
}

Status ReplicaState::ShutdownUnlocked() {
  DCHECK(update_lock_.is_locked());
  CHECK_EQ(state_, kShuttingDown);
  state_ = kShutDown;
  return Status::OK();
}

const QuorumState& ReplicaState::GetActiveQuorumStateUnlocked() const {
  DCHECK(update_lock_.is_locked());
  DCHECK(active_quorum_state_) << "Quorum state is not set";
  return *active_quorum_state_;
}

bool ReplicaState::IsQuorumChangePendingUnlocked() const {
  return pending_quorum_.get() != NULL;
}

// TODO check that the role change is legal.
Status ReplicaState::SetPendingQuorumUnlocked(const metadata::QuorumPB& new_quorum) {
  DCHECK(update_lock_.is_locked());
  pending_quorum_.reset(new metadata::QuorumPB(new_quorum));
  ResetActiveQuorumStateUnlocked(new_quorum);
  return Status::OK();
}

const metadata::QuorumPB& ReplicaState::GetPendingQuorumUnlocked() const {
  DCHECK(update_lock_.is_locked());
  CHECK(IsQuorumChangePendingUnlocked()) << "No pending quorum";
  return *pending_quorum_;
}

Status ReplicaState::SetCommittedQuorumUnlocked(const metadata::QuorumPB& new_quorum) {
  DCHECK(update_lock_.is_locked());
  DCHECK(new_quorum.IsInitialized());

  ResetActiveQuorumStateUnlocked(new_quorum);

  cmeta_->mutable_pb()->mutable_committed_quorum()->CopyFrom(new_quorum);
  RETURN_NOT_OK(cmeta_->Flush());
  pending_quorum_.reset();

  return Status::OK();
}

const metadata::QuorumPB& ReplicaState::GetCommittedQuorumUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->pb().committed_quorum();
}

const metadata::QuorumPB& ReplicaState::GetActiveQuorumUnlocked() const {
  DCHECK(update_lock_.is_locked());
  if (IsQuorumChangePendingUnlocked()) {
    return GetPendingQuorumUnlocked();
  }
  return GetCommittedQuorumUnlocked();
}


bool ReplicaState::IsOpCommittedOrPending(const OpId& op_id, bool* term_mismatch) {

  *term_mismatch = false;

  if (op_id.index() <= GetCommittedOpIdUnlocked().index()) {
    return true;
  }

  if (op_id.index() > GetLastReceivedOpIdUnlocked().index()) {
    return false;
  }

  ConsensusRound* round = DCHECK_NOTNULL(GetPendingOpByIndexOrNullUnlocked(op_id.index()));

  if (round->id().term() != op_id.term()) {
    *term_mismatch = true;
    return false;
  }
  return true;
}


Status ReplicaState::IncrementTermUnlocked() {
  DCHECK(update_lock_.is_locked());
  cmeta_->mutable_pb()->set_current_term(cmeta_->pb().current_term() + 1);
  cmeta_->mutable_pb()->clear_voted_for();
  RETURN_NOT_OK(cmeta_->Flush());
  return Status::OK();
}

Status ReplicaState::SetCurrentTermUnlocked(uint64_t new_term) {
  DCHECK(update_lock_.is_locked());
  if (PREDICT_FALSE(new_term < GetCurrentTermUnlocked())) {
    return Status::IllegalState(
        Substitute("Cannot change term to a term that is lower than the current one. "
            "Current: $0, Proposed: $1", GetCurrentTermUnlocked(), new_term));
  }
  cmeta_->mutable_pb()->set_current_term(new_term);
  cmeta_->mutable_pb()->clear_voted_for();
  RETURN_NOT_OK(cmeta_->Flush());
  return Status::OK();
}

const uint64_t ReplicaState::GetCurrentTermUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->pb().current_term();
}

const bool ReplicaState::HasVotedCurrentTermUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->pb().has_voted_for();
}

Status ReplicaState::SetVotedForCurrentTermUnlocked(const std::string& uuid) {
  DCHECK(update_lock_.is_locked());
  cmeta_->mutable_pb()->set_voted_for(uuid);
  RETURN_NOT_OK_PREPEND(cmeta_->Flush(),
                        "Unable to flush consensus metadata after recording vote");
  return Status::OK();
}

const std::string& ReplicaState::GetVotedForCurrentTermUnlocked() const {
  DCHECK(update_lock_.is_locked());
  DCHECK(cmeta_->pb().has_voted_for());
  return cmeta_->pb().voted_for();
}

ReplicaTransactionFactory* ReplicaState::GetReplicaTransactionFactoryUnlocked() const {
  return txn_factory_;
}

const string& ReplicaState::GetPeerUuid() const {
  return peer_uuid_;
}

const ConsensusOptions& ReplicaState::GetOptions() const {
  return options_;
}

int ReplicaState::GetNumPendingTxnsUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return pending_txns_.size();
}

Status ReplicaState::CancelPendingTransactions() {
  {
    UniqueLock lock(&update_lock_);
    if (state_ != kShuttingDown) {
      return Status::IllegalState("Can only wait for pending commits on kShuttingDown state.");
    }
    LOG_WITH_PREFIX(INFO) << "Trying to abort " << pending_txns_.size() << " pending transactions.";
    for (IndexToRoundMap::iterator iter = pending_txns_.begin();
         iter != pending_txns_.end(); iter++) {
      ConsensusRound* round = (*iter).second;
      // We cancel only transactions whose applies have not yet been triggered.
      if (in_flight_commits_.count((*iter).first) == 0) {
        LOG_WITH_PREFIX(INFO) << "Aborting transaction as it isn't in flight: "
            << (*iter).second->replicate_msg()->ShortDebugString();
        round->NotifyReplicationFinished(Status::Aborted("Transaction aborted"));
      } else {
        // In this case we can't assume that the ConsensusRound for the pending transaction
        // is still live. The commit callback might have already been triggered and the
        // ConsensusRound deleted on transaction cleanup.
        LOG_WITH_PREFIX(INFO) << "Skipping txn abort as the apply already in flight for op index: "
            << (*iter).first;
      }
    }
  }
  return Status::OK();
}

Status ReplicaState::WaitForOustandingApplies() {
  {
    UniqueLock lock(&update_lock_);
    if (state_ != kShuttingDown) {
      return Status::IllegalState("Can only wait for pending commits on kShuttingDown state.");
    }
    LOG_WITH_PREFIX(INFO) << "Waiting on " << in_flight_commits_.size()
                          << " commits in progress...";
  }
  for (int wait_attempt = 0; ; wait_attempt++) {
    UniqueLock lock(&update_lock_);
    if (in_flight_commits_.empty()) {
      return Status::OK();
    }
    usleep(std::min(10000, wait_attempt * 1000));
  }

  LOG_WITH_PREFIX_LK(INFO) << "All local commits completed.";
  return Status::OK();
}

Status ReplicaState::GetUncommittedPendingOperationsUnlocked(vector<ConsensusRound*>* ops) {
  BOOST_FOREACH(const IndexToRoundMap::value_type& entry, pending_txns_) {
    if (entry.first > last_committed_index_.index()) {
      ops->push_back(entry.second);
    }
  }
  return Status::OK();
}

Status ReplicaState::AbortOpsAfterUnlocked(int64_t new_preceding_idx) {
  DCHECK(update_lock_.is_locked());
  LOG_WITH_PREFIX(INFO) << "Aborting all transactions after (but not including): "
      << new_preceding_idx << ". Current State: " << ToStringUnlocked();

  DCHECK_GE(new_preceding_idx, 0);
  OpId new_preceding;

  IndexToRoundMap::iterator iter = pending_txns_.lower_bound(new_preceding_idx);

  // Either the new preceding id is in the pendings set or it must be equal to the
  // committed index since we can't truncate already committed operations.
  if (iter != pending_txns_.end() && (*iter).first == new_preceding_idx) {
    new_preceding = (*iter).second->replicate_msg()->id();
    ++iter;
  } else {
    CHECK_EQ(new_preceding_idx, last_committed_index_.index());
    new_preceding = last_committed_index_;
  }

  // This is the same as UpdateLastReceivedOpIdUnlocked() but we do it
  // here to avoid the bounds check, since we're breaking monotonicity.
  received_op_id_ = new_preceding;
  next_index_ = new_preceding.index() + 1;

  for (; iter != pending_txns_.end();) {
    ConsensusRound* round = (*iter).second;
    LOG(INFO) << "Aborting uncommitted operation due to leader change: "
        << round->replicate_msg()->id();
    round->NotifyReplicationFinished(Status::Aborted("Transaction aborted by new leader"));
    // erase the entry from pendings
    pending_txns_.erase(iter++);
  }

  return Status::OK();
}

Status ReplicaState::AddPendingOperation(ConsensusRound* round) {
  DCHECK(update_lock_.is_locked());
  if (PREDICT_FALSE(state_ != kRunning)) {
    // Special case when we're configuring and this is a config change, refuse
    // everything else.
    if (round->replicate_msg()->op_type() != CHANGE_CONFIG_OP) {
      return Status::IllegalState("Cannot trigger prepare. Replica is not in kRunning state.");
    }
  }

  InsertOrDie(&pending_txns_, round->replicate_msg()->id().index(), round);
  return Status::OK();
}

ConsensusRound* ReplicaState::GetPendingOpByIndexOrNullUnlocked(uint64_t index) {
  DCHECK(update_lock_.is_locked());
  return FindPtrOrNull(pending_txns_, index);
}

Status ReplicaState::UpdateMajorityReplicatedUnlocked(const OpId& majority_replicated,
                                                      OpId* committed_index) {
  DCHECK(update_lock_.is_locked());
  DCHECK(majority_replicated.IsInitialized());
  DCHECK(last_committed_index_.IsInitialized());
  if (PREDICT_FALSE(state_ == kShuttingDown || state_ == kShutDown)) {
    return Status::ServiceUnavailable("Cannot trigger apply. Replica is shutting down.");
  }
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Cannot trigger apply. Replica is not in kRunning state.");
  }

  // If the last committed operation was in the current term (the normal case)
  // then 'committed_index' is simply equal to majority replicated.
  if (last_committed_index_.term() == GetCurrentTermUnlocked()) {
    RETURN_NOT_OK(AdvanceCommittedIndexUnlocked(majority_replicated));
    committed_index->CopyFrom(last_committed_index_);
    return Status::OK();
  }

  // If the last committed operation is not in the current term (such as when
  // we change leaders) but 'majority_replicated' is then we can advance the
  // 'committed_index' too.
  if (majority_replicated.term() == GetCurrentTermUnlocked()) {
    OpId previous = last_committed_index_;
    RETURN_NOT_OK(AdvanceCommittedIndexUnlocked(majority_replicated));
    committed_index->CopyFrom(last_committed_index_);
    LOG_WITH_PREFIX(INFO) << "Advanced the committed_index across terms."
        << " Last committed operation was: " << previous.ShortDebugString()
        << " New committed index is: " << last_committed_index_.ShortDebugString();
    return Status::OK();
  }

  committed_index->CopyFrom(last_committed_index_);
  LOG_WITH_PREFIX(WARNING) << "Can't advance the committed index across term boundaries yet."
          << " Last committed operation was: " << last_committed_index_.ShortDebugString()
          << " New majority replicated is: " << majority_replicated.ShortDebugString()
          << ". Current term is: " << GetCurrentTermUnlocked();

  return Status::OK();
}

Status ReplicaState::AdvanceCommittedIndexUnlocked(const OpId& committed_index) {
  // If we already committed up to (or past) 'id' return.
  // This can happen in the case that multiple UpdateConsensus() calls end
  // up in the RPC queue at the same time, and then might get interleaved out
  // of order.
  if (last_committed_index_.index() >= committed_index.index()) {
    VLOG_WITH_PREFIX(1)
      << "Already marked ops through " << last_committed_index_ << " as committed. "
      << "Now trying to mark " << committed_index << " which would be a no-op.";
    return Status::OK();
  }

  if (pending_txns_.empty()) {
    last_committed_index_.CopyFrom(committed_index);
    VLOG_WITH_PREFIX(1) << "No transactions to mark as committed up to: "
        << committed_index.ShortDebugString();
    return Status::OK();
  }

  // Start at the operation after the last committed one.
  IndexToRoundMap::iterator iter = pending_txns_.upper_bound(last_committed_index_.index());
  // Stop at the operation after the last one we must commit.
  IndexToRoundMap::iterator end_iter = pending_txns_.upper_bound(committed_index.index());

  VLOG_WITH_PREFIX(1) << "Last triggered apply was: "
      <<  last_committed_index_.ShortDebugString()
      << " Starting to apply from log index: " << (*iter).first;

  for (; iter != end_iter; iter++) {
    ConsensusRound* round = DCHECK_NOTNULL((*iter).second);
    InsertOrDie(&in_flight_commits_, round->id().index());

    // If we're committing a change config op, persist the new quorum first
    if (PREDICT_FALSE(round->replicate_msg()->op_type() == CHANGE_CONFIG_OP)) {
      DCHECK(round->replicate_msg()->change_config_request().has_new_config());
      CHECK_OK(SetCommittedQuorumUnlocked(
          round->replicate_msg()->change_config_request().new_config()));
    }

    round->NotifyReplicationFinished(Status::OK());
  }

  last_committed_index_.CopyFrom(committed_index);

  return Status::OK();
}

const OpId& ReplicaState::GetCommittedOpIdUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return last_committed_index_;
}

void ReplicaState::UpdateLastReceivedOpIdUnlocked(const OpId& op_id) {
  DCHECK(update_lock_.is_locked());
  DCHECK_LE(OpIdCompare(received_op_id_, op_id), 0)
    << "Previously received OpId: " << received_op_id_.ShortDebugString()
    << ", updated OpId: " << op_id.ShortDebugString()
    << ", Trace:" << std::endl << Trace::CurrentTrace()->DumpToString(true);
  received_op_id_ = op_id;
  next_index_ = op_id.index() + 1;
}

const OpId& ReplicaState::GetLastReceivedOpIdUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return received_op_id_;
}

void ReplicaState::UpdateCommittedOpIdUnlocked(const OpId& committed_op_id) {
  DCHECK(update_lock_.is_locked());
  CHECK_EQ(in_flight_commits_.erase(committed_op_id.index()), 1)
    << "Trying to mark " << committed_op_id.ShortDebugString() << " as committed, but not "
    << "in the in-flight set";
  CHECK(EraseKeyReturnValuePtr(&pending_txns_, committed_op_id.index()))
    << "Couldn't remove " << committed_op_id.ShortDebugString() << " from the pending set";
}

void ReplicaState::NewIdUnlocked(OpId* id) {
  DCHECK(update_lock_.is_locked());
  id->set_term(GetCurrentTermUnlocked());
  id->set_index(next_index_++);
}

void ReplicaState::CancelPendingOperation(const OpId& id) {
  OpId previous = id;
  previous.set_index(previous.index() - 1);
  DCHECK(update_lock_.is_locked());
  CHECK_EQ(GetCurrentTermUnlocked(), id.term());
  CHECK_EQ(next_index_, id.index() + 1);
  next_index_ = id.index();

  // We don't use UpdateLastReceivedOpIdUnlocked because we're actually
  // updating it back to a lower value and we need to avoid the checks
  // that method has.

  // This is only ok if we do _not_ release the lock after calling
  // NewIdUnlocked() (which we don't in RaftConsensus::Replicate()).
  received_op_id_ = previous;
  ignore_result(DCHECK_NOTNULL(EraseKeyReturnValuePtr(&pending_txns_, id.index())));
}

string ReplicaState::LogPrefix() {
  ReplicaState::UniqueLock lock;
  CHECK_OK(LockForRead(&lock));
  return LogPrefixUnlocked();
}

string ReplicaState::LogPrefixUnlocked() const {
  return Substitute("T $0 P $1 [$2]: ",
                    options_.tablet_id,
                    GetPeerUuid(),
                    QuorumPeerPB::Role_Name(active_quorum_state_->role));
}

ReplicaState::State ReplicaState::state() const {
  DCHECK(update_lock_.is_locked());
  return state_;
}

string ReplicaState::ToString() const {
  ReplicaState::UniqueLock lock(&update_lock_);
  return ToStringUnlocked();
}

string ReplicaState::ToStringUnlocked() const {
  DCHECK(update_lock_.is_locked());
  QuorumPeerPB::Role role = QuorumPeerPB::NON_PARTICIPANT;
  if (active_quorum_state_) {
    role = active_quorum_state_->role;
  }
  string ret;
  SubstituteAndAppend(&ret, "Replica: $0, State: $1, Role: $2\n",
                      peer_uuid_, state_,
                      QuorumPeerPB::Role_Name(role));

  SubstituteAndAppend(&ret, "Watermarks: {Received: $0 Committed: $1}\n",
                      received_op_id_.ShortDebugString(),
                      last_committed_index_.ShortDebugString());

  SubstituteAndAppend(&ret, "Num. outstanding commits: $0 IsLocked: $1",
                      in_flight_commits_.size(), update_lock_.is_locked());
  return ret;
}

void ReplicaState::ResetActiveQuorumStateUnlocked(const metadata::QuorumPB& quorum) {
  DCHECK(update_lock_.is_locked());
  active_quorum_state_ = QuorumState::Build(quorum, peer_uuid_).Pass();
}

}  // namespace consensus
}  // namespace kudu

