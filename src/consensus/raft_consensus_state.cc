// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>
#include <boost/foreach.hpp>

#include "consensus/raft_consensus_state.h"
#include "consensus/log_util.h"
#include "gutil/map-util.h"
#include "gutil/strings/strcat.h"
#include "gutil/strings/substitute.h"
#include "util/status.h"

namespace kudu {
namespace consensus {

using metadata::QuorumPeerPB;
using std::string;
using std::tr1::shared_ptr;
using std::tr1::unordered_set;
using strings::Substitute;

ReplicaState::ReplicaState(ThreadPool* callback_exec_pool,
                           const std::string& peer_uuid,
                           ReplicaTransactionFactory* txn_factory,
                           uint64_t current_term,
                           uint64_t current_index)
  : peer_uuid_(peer_uuid),
    current_role_(metadata::QuorumPeerPB::NON_PARTICIPANT),
    current_majority_(-1),
    current_term_(current_term),
    next_index_(current_index + 1),
    txn_factory_(txn_factory),
    in_flight_commits_latch_(0),
    replicate_watchers_(callback_exec_pool),
    commit_watchers_(callback_exec_pool),
    state_(kNotInitialized) {
  OpId initial;
  initial.set_term(current_term);
  initial.set_index(current_index);
  all_committed_before_id_.CopyFrom(initial);
  replicated_op_id_.CopyFrom(initial);
  received_op_id_.CopyFrom(initial);
}

// TODO check that the role change is legal.
Status ReplicaState::ChangeConfigUnlocked(const metadata::QuorumPB& new_quorum) {
  DCHECK(update_lock_.is_locked());
  // set this peer's role as non-participant
  current_role_ = QuorumPeerPB::NON_PARTICIPANT;

  voting_peers_.clear();

  // try to find the role set in the quorum_config
  BOOST_FOREACH(const QuorumPeerPB& peer_pb, new_quorum.peers()) {
    if (peer_pb.permanent_uuid() == peer_uuid_) {
      current_role_ = peer_pb.role();
    }
    if (peer_pb.role() == QuorumPeerPB::LEADER ||
        peer_pb.role() == QuorumPeerPB::FOLLOWER) {
      voting_peers_.insert(peer_pb.permanent_uuid());
    }
    if (peer_pb.role() == QuorumPeerPB::LEADER) {
      leader_uuid_ = peer_pb.permanent_uuid();
    }
  }

  current_quorum_ = new_quorum;
  current_majority_ = (voting_peers_.size() / 2) + 1;
  return Status::OK();
}

Status ReplicaState::LockForRead(UniqueLock* lock) {
  *lock = UniqueLock(update_lock_);
  return Status::OK();
}

Status ReplicaState::LockForReplicate(UniqueLock* lock) {
  UniqueLock l(update_lock_);
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Replica not in running state");
  }
  switch (current_role_) {
    case QuorumPeerPB::LEADER:
      lock->swap(l);
      return Status::OK();
    default:
      return Status::IllegalState("Replica is not leader of this quorum.");
  }
}

Status ReplicaState::LockForCommit(UniqueLock* lock) {
  UniqueLock l(update_lock_);
  if (PREDICT_FALSE(state_ != kRunning && state_ != kShuttingDown)) {
    return Status::IllegalState("Replica not in running state");
  }
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::LockForConfigChange(UniqueLock* lock) {
  UniqueLock l(update_lock_);
  // Can only change the config on non-initialized replicas for
  // now, later kRunning state will also be a valid state for
  // config changes.
  CHECK_EQ(state_, kNotInitialized);
  lock->swap(l);
  state_ = kChangingConfig;
  return Status::OK();
}

Status ReplicaState::LockForUpdate(UniqueLock* lock) {
  UniqueLock l(update_lock_);
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Replica not in running state");
  }
  switch (current_role_) {
    case QuorumPeerPB::LEADER:
      return Status::IllegalState("Replica is leader of the quorum.");
    case QuorumPeerPB::NON_PARTICIPANT:
      return Status::IllegalState("Replica is not a participant of this quorum.");
    default:
      lock->swap(l);
      return Status::OK();
  }
}

Status ReplicaState::LockForShutdown(UniqueLock* lock) {
  UniqueLock l(update_lock_);
  if (state_ != kShuttingDown) {
    state_ = kShuttingDown;
    current_role_ = QuorumPeerPB::NON_PARTICIPANT;
    in_flight_commits_latch_.Reset(in_flight_commits_.size());
  }
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::SetChangeConfigSuccessfulUnlocked() {
  DCHECK(update_lock_.is_locked());
  CHECK_EQ(state_, kChangingConfig);
  state_ = kRunning;
  return Status::OK();
}

metadata::QuorumPeerPB::Role ReplicaState::GetCurrentRoleUnlocked() {
  DCHECK(update_lock_.is_locked());
  return current_role_;
}

const metadata::QuorumPB& ReplicaState::GetCurrentConfigUnlocked() {
  DCHECK(update_lock_.is_locked());
  return current_quorum_;
}

void ReplicaState::IncrementConfigSeqNoUnlocked() {
  DCHECK(update_lock_.is_locked());
  current_quorum_.set_seqno(current_quorum_.seqno() + 1);
}

int ReplicaState::GetCurrentMajorityUnlocked() {
  DCHECK(update_lock_.is_locked());
  return current_majority_;
}

const unordered_set<string>& ReplicaState::GetCurrentVotingPeersUnlocked() {
  DCHECK(update_lock_.is_locked());
  return voting_peers_;
}

int ReplicaState::GetAllPeersCountUnlocked() {
  DCHECK(update_lock_.is_locked());
  return current_quorum_.peers_size();
}

const string& ReplicaState::GetPeerUuid() {
  return peer_uuid_;
}

const string& ReplicaState::GetLeaderUuidUnlocked() {
  DCHECK(update_lock_.is_locked());
  return leader_uuid_;
}

Status ReplicaState::WaitForOustandingApplies() {
  {
    UniqueLock lock(update_lock_);
    if (state_ != kShuttingDown) {
      return Status::IllegalState("Can only wait for pending commits on kShuttingDown state.");
    }
    LOG(INFO) << "Replica " << peer_uuid_ << " waiting on "
      << in_flight_commits_latch_.count() << " outstanding commits.";
  }
  in_flight_commits_latch_.Wait();
  LOG(INFO) << "All local commits completed for replica: " << GetPeerUuid();
  return Status::OK();
}

Status ReplicaState::TriggerPrepareUnlocked(gscoped_ptr<ConsensusRound> context) {
  DCHECK(update_lock_.is_locked());
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Cannot trigger prepare. Replica is not in kRunning state.");
  }
  InsertOrDie(&pending_txns_, context->replicate_op()->id(), context.get());
  CHECK_OK(txn_factory_->StartReplicaTransaction(context.Pass()));
  return Status::OK();
}

Status ReplicaState::TriggerApplyUnlocked(gscoped_ptr<OperationPB> leader_commit_op) {
  DCHECK(update_lock_.is_locked());
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Cannot trigger apply. Replica is not in kRunning state.");
  }
  ConsensusRound* context = FindPtrOrNull(pending_txns_,
                                            leader_commit_op->commit().commited_op_id());
  if (in_flight_commits_.empty()) {
    all_committed_before_id_.CopyFrom(leader_commit_op->id());
  }
  InsertOrDie(&in_flight_commits_, leader_commit_op->id());
  RETURN_NOT_OK(DCHECK_NOTNULL(context)->GetReplicaCommitContinuation()->LeaderCommitted(
      leader_commit_op.Pass()));
  return Status::OK();
}

void ReplicaState::UpdateLastReplicatedOpIdUnlocked(const OpId& op_id) {
  DCHECK(update_lock_.is_locked());
  replicated_op_id_.CopyFrom(op_id);
  replicate_watchers_.MarkFinished(op_id, OpIdWaiterSet::MARK_ALL_OPS_BEFORE);
}

const OpId& ReplicaState::GetLastReplicatedOpIdUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return replicated_op_id_;
}

void ReplicaState::UpdateLastReceivedOpIdUnlocked(const OpId& op_id) {
  DCHECK(update_lock_.is_locked());
  DCHECK_LE(log::OpIdCompare(received_op_id_, op_id), 0);
  received_op_id_ = op_id;
}

const OpId& ReplicaState::GetLastReceivedOpIdUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return received_op_id_;
}

const OpId& ReplicaState::GetSafeCommitOpIdUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return all_committed_before_id_;
}

void ReplicaState::UpdateLeaderCommittedOpIdUnlocked(const OpId& committed_op_id) {
  DCHECK(update_lock_.is_locked());
  commit_watchers_.MarkFinished(committed_op_id, OpIdWaiterSet::MARK_ONLY_THIS_OP);
}


void ReplicaState::UpdateReplicaCommittedOpIdUnlocked(const OpId& commit_op_id,
                                                      const OpId& committed_op_id) {
  DCHECK(update_lock_.is_locked());
  CHECK_EQ(in_flight_commits_.erase(commit_op_id), 1) << commit_op_id.ShortDebugString();
  if (log::OpIdEquals(commit_op_id, all_committed_before_id_)) {
    if (!in_flight_commits_.empty()) {
       all_committed_before_id_ = *std::min_element(in_flight_commits_.begin(),
                                                    in_flight_commits_.end(),
                                                    log::OpIdLessThan);
     } else {
       all_committed_before_id_ = commit_op_id;
     }
  }
  CHECK_NOTNULL(EraseKeyReturnValuePtr(&pending_txns_, committed_op_id));
  commit_watchers_.MarkFinished(committed_op_id, OpIdWaiterSet::MARK_ONLY_THIS_OP);
}

void ReplicaState::CountDownOutstandingCommitsIfShuttingDown() {
  if (PREDICT_FALSE(state_ == kShuttingDown)) {
    in_flight_commits_latch_.CountDown();
  }
}

Status ReplicaState::RegisterOnReplicateCallback(
    const OpId& replicate_op_id,
    const shared_ptr<FutureCallback>& repl_callback) {
  UniqueLock lock(update_lock_);
  if (PREDICT_TRUE(log::OpIdCompare(replicate_op_id, replicated_op_id_) > 0)) {
    replicate_watchers_.RegisterCallback(replicate_op_id, repl_callback);
    return Status::OK();
  }
  return Status::AlreadyPresent("The operation has already been replicated.");
}

Status ReplicaState::RegisterOnCommitCallback(const OpId& op_id,
                                              const shared_ptr<FutureCallback>& commit_callback) {
  UniqueLock lock(update_lock_);
  if (PREDICT_TRUE(log::OpIdCompare(op_id, replicated_op_id_) > 0)) {
    commit_watchers_.RegisterCallback(op_id, commit_callback);
    return Status::OK();
  }
  if (FindOrNull(pending_txns_, op_id) != NULL) {
    commit_watchers_.RegisterCallback(op_id, commit_callback);
    return Status::OK();
  }
  return Status::AlreadyPresent("The operation has already been committed.");
}

void ReplicaState::NewIdUnlocked(OpId* id) {
  DCHECK(update_lock_.is_locked());
  id->set_term(current_term_);
  id->set_index(next_index_++);
}

void ReplicaState::RollbackIdGenUnlocked(const OpId& id) {
  DCHECK(update_lock_.is_locked());
  CHECK_EQ(current_term_, id.term());
  CHECK_EQ(next_index_, id.index() + 1);
  current_term_ = id.term();
  next_index_ = id.index();
}

string ReplicaState::ToString() {
  string ret;
  StrAppend(&ret, Substitute("Replica: $0, State: $1, Role: $2\n",
                             peer_uuid_, state_,
                             QuorumPeerPB::Role_Name(current_role_)));
  StrAppend(&ret, "Watermarks: {Received: ", received_op_id_.ShortDebugString(),
            " Replicated: ", replicated_op_id_.ShortDebugString(),
            " Committed: ", all_committed_before_id_.ShortDebugString(), "}\n");
  StrAppend(&ret, "Num. outstanding commits: ", in_flight_commits_.size(),
            " IsLocked: ", update_lock_.is_locked());
  return ret;
}


OperationCallbackRunnable::OperationCallbackRunnable(const shared_ptr<FutureCallback>& callback)
  : callback_(callback) {}

void OperationCallbackRunnable::set_error(const Status& error) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  error_ = error;
}

void OperationCallbackRunnable::Run() {
  boost::lock_guard<simple_spinlock> lock(lock_);
  if (PREDICT_TRUE(error_.ok())) {
    callback_->OnSuccess();
    return;
  }
  callback_->OnFailure(error_);
}


MajorityOperationStatus::MajorityOperationStatus(const OpId* id,
                                                 const unordered_set<string>& voting_peers,
                                                 int majority,
                                                 int total_peers_count)
    : id_(id),
      majority_(majority),
      voting_peers_(voting_peers),
      total_peers_count_(total_peers_count),
      replicated_count_(0),
      completion_latch_(majority),
      callback_pool_(NULL),
      runnable_(NULL) {
}

MajorityOperationStatus::MajorityOperationStatus(const OpId* id,
                                                 const unordered_set<string>& voting_peers,
                                                 int majority,
                                                 int total_peers_count,
                                                 ThreadPool* callback_pool,
                                                 const shared_ptr<FutureCallback>& callback)
    : id_(id),
      majority_(majority),
      voting_peers_(voting_peers),
      total_peers_count_(total_peers_count),
      replicated_count_(0),
      completion_latch_(majority),
      callback_pool_(callback_pool),
      runnable_(new OperationCallbackRunnable(callback)) {
  DCHECK_GT(majority, 0);
  DCHECK_LE(majority, voting_peers.size());
  DCHECK_LE(voting_peers.size(), total_peers_count_);
}

void MajorityOperationStatus::AckPeer(const string& uuid) {
  boost::lock_guard<simple_spinlock> lock(lock_);
  if (voting_peers_.count(uuid) != 0) {
    completion_latch_.CountDown();
    if (IsDone() && runnable_.get() != NULL) {
      // TODO handle failed operations by setting an error on the OperationCallbackRunnable.
      // The pool should always be alive when we do this, so we CHECK_OK()
      CHECK_OK(callback_pool_->Submit(shared_ptr<Runnable>(runnable_.release())));
    }
  }
  replicated_count_++;
  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG(2) << "Peer: " << uuid << " ACK'd " << ToStringUnlocked();
  }
  DCHECK_LE(replicated_count_, total_peers_count_)
    << "More replicates than expected. " << ToStringUnlocked();
}

bool MajorityOperationStatus::IsDone() const {
  return completion_latch_.count() == 0;
}

bool MajorityOperationStatus::IsAllDone() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return replicated_count_ >= total_peers_count_;
}

void MajorityOperationStatus::Wait() {
  completion_latch_.Wait();
}

MajorityOperationStatus::~MajorityOperationStatus() {
  // TODO support some of the peers not getting back, but for now
  // check that the operation IsDone()
  DCHECK(IsDone());
}

std::string MajorityOperationStatus::ToString() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return ToStringUnlocked();
}

std::string MajorityOperationStatus::ToStringUnlocked() const {
  return Substitute("MajorityOS. Id: $0 IsDone: $1 All Peers: $2, Voting Peers: $3, "
                    "ACK'd Peers: $4, Majority: $5",
                    id_->ShortDebugString(),
                    IsDone(),
                    total_peers_count_,
                    voting_peers_.size(),
                    replicated_count_,
                    majority_);
}

}  // namespace consensus
}  // namespace kudu

