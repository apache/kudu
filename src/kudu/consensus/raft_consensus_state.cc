// Copyright (c) 2013, Cloudera, inc.

#include <algorithm>
#include <boost/foreach.hpp>

#include "kudu/consensus/raft_consensus_state.h"
#include "kudu/consensus/log_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

// Convenience macros to prefix log messages with the id of the tablet and peer.
// Do not obtain the state lock and should be used when holding the state_ lock
#define LOG_WITH_PREFIX(severity) LOG(severity) << LogPrefixUnlocked()
#define VLOG_WITH_PREFIX(verboselevel) LOG_IF(INFO, VLOG_IS_ON(verboselevel)) \
  << LogPrefixUnlocked()
// Same as the above, but obtain the lock
#define LOG_WITH_PREFIX_LK(severity) LOG(severity) << LogPrefix()
#define VLOG_WITH_PREFIX_LK(verboselevel) LOG_IF(INFO, VLOG_IS_ON(verboselevel)) \
  << LogPrefix()

namespace kudu {
namespace consensus {

using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using std::string;
using std::tr1::shared_ptr;
using std::tr1::unordered_set;
using strings::Substitute;

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
                           ThreadPool* callback_exec_pool,
                           const string& peer_uuid,
                           gscoped_ptr<ConsensusMetadata> cmeta,
                           ReplicaTransactionFactory* txn_factory)
  : options_(options),
    peer_uuid_(peer_uuid),
    cmeta_(cmeta.Pass()),
    current_term_(0),
    next_index_(0),
    txn_factory_(txn_factory),
    in_flight_commits_latch_(0),
    replicate_watchers_(callback_exec_pool),
    commit_watchers_(callback_exec_pool),
    state_(kInitialized) {
  CHECK(cmeta_) << "ConsensusMeta passed as NULL";
  UniqueLock l(update_lock_);
  // Now that we know the peer UUID, refresh acting state from persistent state.
  ResetActiveQuorumStateUnlocked(GetCommittedQuorumUnlocked());
}

Status ReplicaState::StartUnlocked(const OpId& initial_id) {
  DCHECK(update_lock_.is_locked());
  current_term_ = initial_id.term();
  next_index_ = initial_id.index() + 1;
  all_committed_before_id_.CopyFrom(initial_id);
  replicated_op_id_.CopyFrom(initial_id);
  received_op_id_.CopyFrom(initial_id);
  return Status::OK();
}

void ReplicaState::IncrementTermUnlocked() {
  DCHECK(update_lock_.is_locked());
  current_term_++;
}

Status ReplicaState::SetCurrentTermUnlocked(uint64_t new_term) {
  DCHECK(update_lock_.is_locked());
  if (new_term < current_term_) {
    return Status::IllegalState(
        Substitute("Cannot change term to a term that is lower than the current one. "
            "Current: $0, Proposed: $1", current_term_, new_term));
  }
  current_term_ = new_term;
  return Status::OK();
}


Status ReplicaState::LockForStart(UniqueLock* lock) {
  UniqueLock l(update_lock_);
  CHECK_EQ(state_, kInitialized) << "Illegal state for Start()."
      << " Replica is not in kInitialized state";
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::LockForRead(UniqueLock* lock) {
  *lock = UniqueLock(update_lock_);
  return Status::OK();
}

Status ReplicaState::LockForReplicate(UniqueLock* lock, const OperationPB& op) {
  CHECK(op.has_replicate()) << "Only replicate operations are allowed. Op: "
      << op.ShortDebugString();
  UniqueLock l(update_lock_);
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Replica not in running state");
  }
  switch (active_quorum_state_->role) {
    case QuorumPeerPB::LEADER:
      lock->swap(l);
      return Status::OK();
    case QuorumPeerPB::CANDIDATE:
      if (op.replicate().op_type() != CHANGE_CONFIG_OP) {
        return Status::IllegalState("Only a change config round can be pushed while CANDIDATE.");
      }
      // TODO support true config change. Right now we only allow
      // replicate calls while CANDIDATE if our term is 0, meaning
      // we're the first CANDIDATE/LEADER of the quorum.
      CHECK_EQ(current_term_, 0);
      lock->swap(l);
      return Status::OK();
    default:
      return Status::IllegalState(Substitute("Replica $0 is not leader of this quorum. Role: $1",
                                             peer_uuid_,
                                             QuorumPeerPB::Role_Name(active_quorum_state_->role)));
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
  CHECK(state_ == kInitialized || state_ == kRunning) << "Unexpected state: " << state_;
  lock->swap(l);
  state_ = kChangingConfig;
  return Status::OK();
}

Status ReplicaState::LockForUpdate(UniqueLock* lock) {
  UniqueLock l(update_lock_);
  if (PREDICT_FALSE(state_ != kRunning)) {
    return Status::IllegalState("Replica not in running state");
  }
  switch (active_quorum_state_->role) {
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
  if (state_ == kShutDown) {
    return Status::IllegalState("Replica is already shutdown");
  }
  if (state_ != kShuttingDown) {
    state_ = kShuttingDown;
    in_flight_commits_latch_.Reset(in_flight_commits_.size());
  }
  lock->swap(l);
  return Status::OK();
}

Status ReplicaState::Shutdown() {
  CHECK_EQ(state_, kShuttingDown);
  UniqueLock l(update_lock_);
  state_ = kShutDown;
  return Status::OK();
}

Status ReplicaState::SetConfigDoneUnlocked() {
  DCHECK(update_lock_.is_locked());
  CHECK_EQ(state_, kChangingConfig);
  state_ = kRunning;
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
  CHECK(!IsQuorumChangePendingUnlocked()) // TODO: Allow rollback of failed config chg txn?
      << "Attempting to make pending quorum change while another is already pending: "
      << "Pending quorum: " << pending_quorum_->ShortDebugString() << "; "
      << "New quorum: " << new_quorum.ShortDebugString();
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

  // TODO: check that the role change is legal.

  // Check that if pending quorum is set, new_quorum is equivalent.
  if (IsQuorumChangePendingUnlocked()) {
    // TODO: Prevent this from being possible once we have proper config change.
    // See KUDU-513 for more details.
    CHECK(pending_quorum_->SerializeAsString() == new_quorum.SerializeAsString())
      << "Attempting to persist quorum change while a different one is pending: "
      << "Pending quorum: " << pending_quorum_->ShortDebugString() << "; "
      << "New quorum: " << new_quorum.ShortDebugString();
  } else {
    // Only update acting quorum members if this is a net-new transaction.
    ResetActiveQuorumStateUnlocked(new_quorum);
  }

  cmeta_->mutable_pb()->mutable_committed_quorum()->CopyFrom(new_quorum);
  RETURN_NOT_OK(cmeta_->Flush());
  pending_quorum_.reset();

  return Status::OK();
}

const metadata::QuorumPB& ReplicaState::GetCommittedQuorumUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return cmeta_->pb().committed_quorum();
}

ReplicaTransactionFactory* ReplicaState::GetReplicaTransactionFactoryUnlocked() const {
  return txn_factory_;
}

Status ReplicaState::IncrementConfigSeqNoUnlocked() {
  DCHECK(update_lock_.is_locked());
  cmeta_->mutable_pb()->mutable_committed_quorum()->set_seqno(
      cmeta_->pb().committed_quorum().seqno() + 1);
  RETURN_NOT_OK(cmeta_->Flush());
  return Status::OK();
}

const string& ReplicaState::GetPeerUuid() const {
  return peer_uuid_;
}

const ConsensusOptions& ReplicaState::GetOptions() const {
  return options_;
}

const uint64_t ReplicaState::GetCurrentTermUnlocked() const {
  DCHECK(update_lock_.is_locked());
  // TODO: Hook into the persisted term later.
  return current_term_;
}

int ReplicaState::GetNumPendingTxnsUnlocked() const {
  DCHECK(update_lock_.is_locked());
  return pending_txns_.size();
}

Status ReplicaState::CancelPendingTransactions() {
  {
    UniqueLock lock(update_lock_);
    if (state_ != kShuttingDown) {
      return Status::IllegalState("Can only wait for pending commits on kShuttingDown state.");
    }
    LOG_WITH_PREFIX(INFO) << "Aborting " << pending_txns_.size() << " pending transactions.";
    for (OpIdToRoundMap::iterator iter = pending_txns_.begin();
         iter != pending_txns_.end(); iter++) {
      ConsensusRound* round = (*iter).second;
      if (round->leader_commit_op() == NULL) {
        round->GetReplicaCommitContinuation()->Abort();
      }
    }
  }
  return Status::OK();
}

Status ReplicaState::WaitForOustandingApplies() {
  {
    UniqueLock lock(update_lock_);
    if (state_ != kShuttingDown) {
      return Status::IllegalState("Can only wait for pending commits on kShuttingDown state.");
    }
    LOG_WITH_PREFIX(INFO) << "Waiting on " << in_flight_commits_latch_.count()
        << " outstanding commits.";
  }
  in_flight_commits_latch_.Wait();
  LOG_WITH_PREFIX_LK(INFO) << "All local commits completed.";
  return Status::OK();
}

Status ReplicaState::EnqueuePrepareUnlocked(gscoped_ptr<ConsensusRound> round) {
  DCHECK(update_lock_.is_locked());
  if (PREDICT_FALSE(state_ != kRunning)) {
    // Special case when we're configuring and this is a config change, refuse
    // everything else.
    if (round->replicate_op()->replicate().op_type() != CHANGE_CONFIG_OP) {
      return Status::IllegalState("Cannot trigger prepare. Replica is not in kRunning state.");
    }
  }
  ConsensusRound* round_ptr = round.get();
  RETURN_NOT_OK(txn_factory_->StartReplicaTransaction(round.Pass()));
  InsertOrDie(&pending_txns_, round_ptr->replicate_op()->id(), round_ptr);
  return Status::OK();
}

Status ReplicaState::MarkConsensusCommittedUnlocked(gscoped_ptr<OperationPB> leader_commit_op) {
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
  DCHECK_LE(OpIdCompare(received_op_id_, op_id), 0);
  received_op_id_ = op_id;
  next_index_ = op_id.index() + 1;
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
  if (OpIdEquals(commit_op_id, all_committed_before_id_)) {
    if (!in_flight_commits_.empty()) {
       all_committed_before_id_ = *std::min_element(in_flight_commits_.begin(),
                                                    in_flight_commits_.end(),
                                                    OpIdLessThan);
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
  if (PREDICT_TRUE(OpIdCompare(replicate_op_id, replicated_op_id_) > 0)) {
    replicate_watchers_.RegisterCallback(replicate_op_id, repl_callback);
    return Status::OK();
  }
  return Status::AlreadyPresent("The operation has already been replicated.");
}

Status ReplicaState::RegisterOnCommitCallback(const OpId& op_id,
                                              const shared_ptr<FutureCallback>& commit_callback) {
  UniqueLock lock(update_lock_);
  if (PREDICT_TRUE(OpIdCompare(op_id, replicated_op_id_) > 0)) {
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


string ReplicaState::ToString() const {
  ReplicaState::UniqueLock lock(update_lock_);
  return ToStringUnlocked();
}

string ReplicaState::ToStringUnlocked() const {
  DCHECK(update_lock_.is_locked());
  QuorumPeerPB::Role role = QuorumPeerPB::NON_PARTICIPANT;
  if (active_quorum_state_) {
    role = active_quorum_state_->role;
  }
  string ret;
  StrAppend(&ret, Substitute("Replica: $0, State: $1, Role: $2\n",
                             peer_uuid_, state_, role));
  StrAppend(&ret, "Watermarks: {Received: ", received_op_id_.ShortDebugString(),
            " Replicated: ", replicated_op_id_.ShortDebugString(),
            " Committed: ", all_committed_before_id_.ShortDebugString(), "}\n");
  StrAppend(&ret, "Num. outstanding commits: ", in_flight_commits_.size(),
            " IsLocked: ", update_lock_.is_locked());
  return ret;
}

void ReplicaState::ResetActiveQuorumStateUnlocked(const metadata::QuorumPB& quorum) {
  DCHECK(update_lock_.is_locked());
  active_quorum_state_ = QuorumState::Build(quorum, peer_uuid_).Pass();
}

//////////////////////////////////////////////////
// OperationCallbackRunnable
//////////////////////////////////////////////////

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


MajorityOpStatusTracker::MajorityOpStatusTracker(gscoped_ptr<OperationPB> operation,
                                                 const unordered_set<string>& voting_peers,
                                                 int majority,
                                                 int total_peers_count)
    : OperationStatusTracker(operation.Pass()),
      majority_(majority),
      voting_peers_(voting_peers),
      total_peers_count_(total_peers_count),
      replicated_count_(0),
      completion_latch_(majority),
      callback_pool_(NULL),
      runnable_(NULL) {
}

MajorityOpStatusTracker::MajorityOpStatusTracker(gscoped_ptr<OperationPB> operation,
                                                 const unordered_set<string>& voting_peers,
                                                 int majority,
                                                 int total_peers_count,
                                                 ThreadPool* callback_pool,
                                                 const shared_ptr<FutureCallback>& callback)
    : OperationStatusTracker(operation.Pass()),
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
  DCHECK_NOTNULL(runnable_->callback_.get());
}

void MajorityOpStatusTracker::AckPeer(const string& uuid) {
  CHECK(!uuid.empty()) << "Peer acked with empty uuid";
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
    << "More replicates than expected. " << ToStringUnlocked()
    << "; Quorum: " << JoinStringsIterator(voting_peers_.begin(), voting_peers_.end(), ", ");
}

bool MajorityOpStatusTracker::IsDone() const {
  return completion_latch_.count() == 0;
}

bool MajorityOpStatusTracker::IsAllDone() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return replicated_count_ >= total_peers_count_;
}

void MajorityOpStatusTracker::Wait() {
  completion_latch_.Wait();
}

MajorityOpStatusTracker::~MajorityOpStatusTracker() {
  if (!IsDone()) {
    LOG(WARNING) << "Deleting incomplete Operation: " << ToString();
  }
}

std::string MajorityOpStatusTracker::ToString() const {
  boost::lock_guard<simple_spinlock> lock(lock_);
  return ToStringUnlocked();
}

std::string MajorityOpStatusTracker::ToStringUnlocked() const {
  return Substitute("MajorityOpStatusTracker: Id: $0 IsDone: $1 All Peers: $2, Voting Peers: $3, "
                    "ACK'd Peers: $4, Majority: $5",
                    (operation_->has_id() ? operation_->id().ShortDebugString() : "NULL"),
                    IsDone(),
                    total_peers_count_,
                    voting_peers_.size(),
                    replicated_count_,
                    majority_);
}

}  // namespace consensus
}  // namespace kudu

