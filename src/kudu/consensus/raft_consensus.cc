// Copyright (c) 2013, Cloudera, inc.

#include "kudu/consensus/raft_consensus.h"

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <iostream>
#include <string>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/raft_consensus_state.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/server/clock.h"
#include "kudu/server/metadata.h"
#include "kudu/util/trace.h"
#include "kudu/util/url-coding.h"

DEFINE_int32(leader_heartbeat_interval_ms, 500,
             "The LEADER's heartbeat interval to the replicas.");

// Convenience macros to prefix log messages with the id of the tablet and peer.
// Do not obtain the state lock and should be used when holding the state_ lock
#define LOG_WITH_PREFIX(severity) LOG(severity) << state_->LogPrefixUnlocked()
#define VLOG_WITH_PREFIX(verboselevel) LOG_IF(INFO, VLOG_IS_ON(verboselevel)) \
  << state_->LogPrefixUnlocked()
// Same as the above, but obtain the lock
#define LOG_WITH_PREFIX_LK(severity) LOG(severity) << state_->LogPrefix()
#define VLOG_WITH_PREFIX_LK(verboselevel) LOG_IF(INFO, VLOG_IS_ON(verboselevel)) \
  << state_->LogPrefix()

namespace kudu {
namespace consensus {

using log::LogEntryBatch;
using metadata::QuorumPB;
using metadata::QuorumPeerPB;
using std::tr1::shared_ptr;
using std::tr1::unordered_set;
using strings::Substitute;

Status RaftConsensus::MakePeerLeaderInQuorum(const string& peer_uuid,
                                             const QuorumPB& old_quorum,
                                             QuorumPB* new_quorum) {
  new_quorum->Clear();
  new_quorum->CopyFrom(old_quorum);
  new_quorum->clear_peers();
  bool found_peer = false;
  BOOST_FOREACH(const QuorumPeerPB& old_peer, old_quorum.peers()) {
    QuorumPeerPB* new_peer = new_quorum->add_peers();
    new_peer->CopyFrom(old_peer);
    if (new_peer->permanent_uuid() == peer_uuid) {
      new_peer->set_role(QuorumPeerPB::LEADER);
      found_peer = true;
      continue;
    }
    if (new_peer->role() == QuorumPeerPB::LEADER ||
        new_peer->role() == QuorumPeerPB::CANDIDATE) {
      new_peer->set_role(QuorumPeerPB::FOLLOWER);
    }
  }
  if (!found_peer) {
    return Status::IllegalState(Substitute("Cannot find peer: $0 in quorum: $1",
                                           peer_uuid, old_quorum.ShortDebugString()));
  }
  return Status::OK();
}

RaftConsensus::RaftConsensus(const ConsensusOptions& options,
                             gscoped_ptr<ConsensusMetadata> cmeta,
                             gscoped_ptr<PeerProxyFactory> proxy_factory,
                             const MetricContext& metric_ctx,
                             const std::string& peer_uuid,
                             const scoped_refptr<server::Clock>& clock,
                             ReplicaTransactionFactory* txn_factory,
                             log::Log* log)
    : log_(DCHECK_NOTNULL(log)),
      clock_(clock),
      peer_proxy_factory_(proxy_factory.Pass()),
      queue_(metric_ctx) {
  CHECK_OK(ThreadPoolBuilder("raft-op-cb").set_max_threads(1).Build(&callback_pool_));
  state_.reset(new ReplicaState(options,
                                callback_pool_.get(),
                                peer_uuid,
                                cmeta.Pass(),
                                DCHECK_NOTNULL(txn_factory)));
}

Status RaftConsensus::VerifyQuorumAndCheckThatNoChangeIsPendingUnlocked(const QuorumPB& quorum) {
  // Sanity checks.
  if (state_->IsQuorumChangePendingUnlocked()) {
    return Status::IllegalState(Substitute("Attempting to become leader "
        "during a pending quorum change. Pending quorum: $0, Persisted quorum: "
        "$1", state_->GetPendingQuorumUnlocked().ShortDebugString(),
        quorum.ShortDebugString()));
  }
  return VerifyQuorum(quorum);
}


Status RaftConsensus::Start(const ConsensusBootstrapInfo& info) {
  RETURN_NOT_OK(ExecuteHook(PRE_START));

  QuorumPB initial_quorum;
  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForStart(&lock));
    queue_.Init(info.last_committed_id);
    RETURN_NOT_OK_PREPEND(state_->StartUnlocked(info.last_id),
                          "Unable to start RAFT ReplicaState");

    RETURN_NOT_OK_PREPEND(VerifyQuorumAndCheckThatNoChangeIsPendingUnlocked(
                              state_->GetCommittedQuorumUnlocked()),
                          "Invalid state on RaftConsensus::Start()");
    initial_quorum.CopyFrom(state_->GetCommittedQuorumUnlocked());
  }

  // If we're marked as candidate emulate a leader election.
  // Temporary while we don't have the real thing.
  QuorumPeerPB::Role my_role = GetRoleInQuorum(state_->GetPeerUuid(), initial_quorum);
  switch (my_role) {
    case QuorumPeerPB::CANDIDATE:
    case QuorumPeerPB::LEADER:
      RETURN_NOT_OK(EmulateElection());
      break;
    default:
      RETURN_NOT_OK(ChangeConfig());
  }

  RETURN_NOT_OK(ExecuteHook(POST_START));
  return Status::OK();
}

Status RaftConsensus::EmulateElection() {

  QuorumPB new_quorum;
  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForConfigChange(&lock));

    // Right now we only tolerate changes when there isn't stuff
    // in flight, so make sure those assumptions hold.
    // The last thing we received should be a commit and it should also match
    // the safe commit op id.
    if (!OpIdEquals(state_->GetLastReceivedOpIdUnlocked(), state_->GetCommittedOpIdUnlocked())) {
      return Status::IllegalState(
          Substitute("Replica is not ready to be leader. "
                     "Last received OpId: $0, committed OpId: $1",
                     state_->GetLastReceivedOpIdUnlocked().ShortDebugString(),
                     state_->GetCommittedOpIdUnlocked().DebugString()));
    }

    QuorumPB new_quorum;
    RETURN_NOT_OK(MakePeerLeaderInQuorum(state_->GetPeerUuid(),
                                         state_->GetCommittedQuorumUnlocked(),
                                         &new_quorum));
    new_quorum.set_seqno(state_->GetCommittedQuorumUnlocked().seqno() + 1);
    // Increment the term.
    state_->IncrementTermUnlocked();
    RETURN_NOT_OK_PREPEND(VerifyQuorumAndCheckThatNoChangeIsPendingUnlocked(new_quorum),
                          "Invalid state on RaftConsensus::EmulateElection()");
    RETURN_NOT_OK(state_->SetPendingQuorumUnlocked(new_quorum));
    return ChangeConfigUnlocked();
  }
}

Status RaftConsensus::ChangeConfig() {
  ReplicaState::UniqueLock lock;
  RETURN_NOT_OK(state_->LockForConfigChange(&lock));
  RETURN_NOT_OK(ChangeConfigUnlocked());
  return Status::OK();
}

Status RaftConsensus::ChangeConfigUnlocked() {
  switch (state_->GetActiveQuorumStateUnlocked().role) {
    case QuorumPeerPB::LEADER:
      RETURN_NOT_OK(BecomeLeaderUnlocked());
      break;
    case QuorumPeerPB::LEARNER:
    case QuorumPeerPB::FOLLOWER:
      RETURN_NOT_OK(BecomeReplicaUnlocked());
      break;
    default:
      LOG(FATAL) << "Unexpected role: "
          << QuorumPeerPB::Role_Name(state_->GetActiveQuorumStateUnlocked().role);
  }

  return Status::OK();
}

Status RaftConsensus::BecomeLeaderUnlocked() {
  LOG_WITH_PREFIX(INFO) << "Becoming Leader";
  // Create the peers so that we're able to replicate messages remotely and locally
  RETURN_NOT_OK(CreateOrUpdatePeersUnlocked());

  // Initiate a leader side config change transaction. We need it to be leader
  // side so that it acquires a timestamp and generally behaves like a leader
  // transaction, e.g. it places the request in the replication queue and commits
  // by itself, vs we having to manage all of that were we to use a replica side
  // transaction.
  RETURN_NOT_OK(state_->GetReplicaTransactionFactoryUnlocked()->SubmitConsensusChangeConfig(
      gscoped_ptr<QuorumPB>(new QuorumPB(state_->GetPendingQuorumUnlocked())).Pass(),
      Bind(&RaftConsensus::BecomeLeaderResult, Unretained(this))));

  // After the change config transaction is queued, independently of whether it was successful
  // we can become leader and start accepting writes, that is the term can officially start.
  // This because any future writes will only ever commit if the change config transaction
  // commits. This is advantageous in the sense that we don't have to block here waiting for
  // something to be replicated to be correct, even if this peer starts accepting writes
  // from this moment forward those will only ever be reported as committed (or applied for
  // that matter) if the initial change config transaction succeeds.

  // TODO we need to make sure that this transaction is the first transaction accepted, right
  // now that is being assured by the fact that the prepare queue is single threaded, but if
  // we ever move to multi-threaded prepare we need to make sure we call replicate on this first.
  RETURN_NOT_OK(state_->SetConfigDoneUnlocked());
  return Status::OK();
}

void RaftConsensus::BecomeLeaderResult(const Status& status) {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  CHECK(status.ok()) << state_->LogPrefixUnlocked()
      << "Change config transaction failure unsupported. Status: " << status.ToString();
  LOG_WITH_PREFIX(INFO)
    << "Quorum has accepted the change config transaction. New Effective quorum: "
    << state_->GetCommittedQuorumUnlocked().ShortDebugString();
}

Status RaftConsensus::BecomeReplicaUnlocked() {
  // TODO start the failure detector.
  LOG_WITH_PREFIX(INFO) << "Becoming Follower/Learner";
  RETURN_NOT_OK(state_->SetConfigDoneUnlocked());
  // clear the consensus replication queue evicting all state. We can reuse the
  // queue should we become leader.
  queue_.Clear();
  return Status::OK();
}

Status RaftConsensus::CreateOrUpdatePeersUnlocked() {
  unordered_set<string> new_peers;

  metadata::QuorumPB quorum;
  if (state_->IsQuorumChangePendingUnlocked()) {
    quorum = state_->GetPendingQuorumUnlocked();
  } else {
    quorum = state_->GetCommittedQuorumUnlocked();
  }

  VLOG(1) << "Updating peers from new quorum: " << quorum.ShortDebugString();

  // Create new peers
  BOOST_FOREACH(const QuorumPeerPB& peer_pb, quorum.peers()) {
    new_peers.insert(peer_pb.permanent_uuid());
    Peer* peer = FindPtrOrNull(peers_, peer_pb.permanent_uuid());
    if (peer != NULL) {
      continue;
    }
    if (peer_pb.permanent_uuid() == state_->GetPeerUuid()) {
      VLOG_WITH_PREFIX(1) << "Adding local peer. Peer: " << peer_pb.ShortDebugString();
      gscoped_ptr<Peer> local_peer;
      OpId initial = GetLastOpIdFromLog();
      RETURN_NOT_OK(Peer::NewLocalPeer(peer_pb,
                                       state_->GetOptions().tablet_id,
                                       state_->GetPeerUuid(),
                                       &queue_,
                                       log_,
                                       initial,
                                       &local_peer));
      InsertOrDie(&peers_, peer_pb.permanent_uuid(), local_peer.release());
    } else {
      VLOG_WITH_PREFIX(1) << "Adding remote peer. Peer: " << peer_pb.ShortDebugString();
      gscoped_ptr<PeerProxy> peer_proxy;
      RETURN_NOT_OK_PREPEND(peer_proxy_factory_->NewProxy(peer_pb, &peer_proxy),
                            "Could not obtain a remote proxy to the peer.");

      gscoped_ptr<Peer> remote_peer;
      RETURN_NOT_OK(Peer::NewRemotePeer(peer_pb,
                                        state_->GetOptions().tablet_id,
                                        state_->GetPeerUuid(),
                                        &queue_,
                                        peer_proxy.Pass(),
                                        &remote_peer));
      InsertOrDie(&peers_, peer_pb.permanent_uuid(), remote_peer.release());
    }
  }
  // Delete old peers
  PeersMap::iterator iter = peers_.begin();
  for (; iter != peers_.end(); iter++) {
    if (new_peers.count((*iter).first) == 0) {
      peers_.erase(iter);
    }
  }

  return Status::OK();
}

Status RaftConsensus::Replicate(ConsensusRound* round) {

  RETURN_NOT_OK(ExecuteHook(PRE_REPLICATE));

  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForReplicate(&lock, *round->replicate_op()));

    state_->NewIdUnlocked(round->replicate_op()->mutable_id());

    // the original instance of the replicate msg is owned by the consensus context
    // so we create a copy for the queue.
    gscoped_ptr<OperationPB> queue_op(new OperationPB(*round->replicate_op()));

    const QuorumState& quorum_state = state_->GetActiveQuorumStateUnlocked();
    scoped_refptr<OperationStatusTracker> status(
        new MajorityOpStatusTracker(queue_op.Pass(),
                                    quorum_state.voting_peers,
                                    quorum_state.majority_size,
                                    quorum_state.quorum_size,
                                    callback_pool_.get(),
                                    round->replicate_callback()));

    Status s = queue_.AppendOperation(status);
    // Handle Status::ServiceUnavailable(), which means the queue is full.
    if (PREDICT_FALSE(s.IsServiceUnavailable())) {
      gscoped_ptr<OpId> id(status->operation()->release_id());
      // Rollback the id gen. so that we reuse this id later, when we can
      // actually append to the state machine, i.e. this makes the state
      // machine have continuous ids, for the same term, even if the queue
      // refused to add any more operations.
      state_->RollbackIdGenUnlocked(*id);
      LOG_WITH_PREFIX(WARNING) << ": Could not append replicate request "
                   << "to the queue. Queue is Full. "
                   << "Queue metrics: " << queue_.ToString();

      // TODO Possibly evict a dangling peer from the quorum here.
      // TODO count of number of ops failed due to consensus queue overflow.
    }
    RETURN_NOT_OK_PREPEND(s, "Unable to append operation to consensus queue");
  }

  SignalRequestToPeers();

  RETURN_NOT_OK(ExecuteHook(POST_REPLICATE));
  return Status::OK();
}

// TODO (perf) we need to acquire the lock for the critical section of the leader's
// commit (since it is generating an id and appending to the log) but we prob
// could go lockless for the replica version, since the id is already assigned
// and replicas are supposed to execute the commits in the same order as the
// leader anyway.
Status RaftConsensus::Commit(ConsensusRound* context) {
  RETURN_NOT_OK(ExecuteHook(PRE_COMMIT));
  DCHECK_NOTNULL(context->commit_op());

  ReplicaState::UniqueLock lock;
  RETURN_NOT_OK(state_->LockForCommit(&lock));
  QuorumPeerPB::Role role = state_->GetActiveQuorumStateUnlocked().role;
  if (role == QuorumPeerPB::LEADER || role == QuorumPeerPB::CANDIDATE) {
    RETURN_NOT_OK(LeaderCommitUnlocked(context, context->commit_op()));
  } else {
    DCHECK(role == QuorumPeerPB::FOLLOWER || role == QuorumPeerPB::LEARNER)
        << "Unexpected role: " << QuorumPeerPB::Role_Name(role);
    RETURN_NOT_OK(ReplicaCommitUnlocked(context, context->commit_op()));
  }
  RETURN_NOT_OK(ExecuteHook(POST_COMMIT));

  // NOTE: RaftConsensus instance might be destroyed after this call.
  state_->CountDownOutstandingCommitsIfShuttingDown();
  return Status::OK();
}

Status RaftConsensus::PersistQuorum(const QuorumPB& quorum) {
  RETURN_NOT_OK_PREPEND(VerifyQuorum(quorum),
                        "Invalid quorum passed to RaftConsensus::PersistQuorum()");
  ReplicaState::UniqueLock lock;
  RETURN_NOT_OK(state_->LockForConfigChange(&lock));
  RETURN_NOT_OK(state_->SetCommittedQuorumUnlocked(quorum));
  // TODO: Update consensus peers if becoming leader.
  RETURN_NOT_OK(state_->SetConfigDoneUnlocked());
  return Status::OK();
}

Status RaftConsensus::LeaderCommitUnlocked(ConsensusRound* round,
                                           OperationPB* commit_op) {

  // Sanity checks
  // TODO remove this when we move commits out of operations.
  DCHECK(commit_op->has_commit());
  DCHECK(!commit_op->has_id());
  DCHECK(commit_op->commit().has_commited_op_id());

  // TODO right now we're just using the replica path to store the commits in the log
  // that should work ok most of the time, but in some cases, if local disk appends/fsyncs
  // are being slow, this might cause commits to come before the replicate messages
  // they refer to in the log. This should be easy enough to handle but isn't yet.

  if (VLOG_IS_ON(1)) {
    VLOG_WITH_PREFIX(1) << "Leader appended commit. Leader: "
        << state_->ToStringUnlocked() << " Commit: " << commit_op->ShortDebugString();
  }

  // Copy the ids to update later as we can't be sure they will be alive
  // after the log append.
  OpId committed_op_id = commit_op->commit().commited_op_id();
  RETURN_NOT_OK(AppendCommitToLogUnlocked(round, commit_op));
  state_->UpdateLeaderCommittedOpIdUnlocked(committed_op_id);

  SignalRequestToPeers();
  return Status::OK();
}
Status RaftConsensus::ReplicaCommitUnlocked(ConsensusRound* round,
                                            OperationPB* commit_op) {

  // Sanity checks
  // TODO remove this when we move commits out of operations.
  DCHECK(commit_op->has_commit());
  DCHECK(!commit_op->has_id());
  DCHECK(commit_op->commit().has_commited_op_id());

  if (VLOG_IS_ON(1)) {
    VLOG_WITH_PREFIX(1) << "Replica appending commit. Replica: "
        << state_->ToStringUnlocked() << " Commit: " << commit_op->ShortDebugString();
  }

  // Copy the ids to update later as we can't be sure they will be alive
  // after the log append.
  OpId committed_op_id = commit_op->commit().commited_op_id();

  RETURN_NOT_OK(AppendCommitToLogUnlocked(round, commit_op));

  state_->UpdateReplicaCommittedOpIdUnlocked(committed_op_id);
  return Status::OK();
}

Status RaftConsensus::AppendCommitToLogUnlocked(ConsensusRound* round,
                                                OperationPB* commit_op) {

  gscoped_ptr<log::LogEntryBatchPB> entry_batch;
  log::CreateBatchFromAllocatedOperations(&commit_op, 1, &entry_batch);

  LogEntryBatch* reserved_entry_batch;
  CHECK_OK(log_->Reserve(entry_batch.Pass(), &reserved_entry_batch));

  // TODO: replace the FutureCallbacks in ConsensusContext with StatusCallbacks
  //
  // AsyncAppend takes ownership of 'ftscb'.
  CHECK_OK(log_->AsyncAppend(reserved_entry_batch,
                             round->commit_callback()->AsStatusCallback()));

  return Status::OK();
}

Status RaftConsensus::Update(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response) {
  RETURN_NOT_OK(ExecuteHook(PRE_UPDATE));
  ConsensusStatusPB* status = response->mutable_status();
  response->set_responder_uuid(state_->GetPeerUuid());

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    VLOG_WITH_PREFIX_LK(1) << "Replica received request: " << request->ShortDebugString();
  }

  // see var declaration
  boost::lock_guard<simple_spinlock> lock(update_lock_);
  RETURN_NOT_OK(UpdateReplica(request));

  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForRead(&lock));
    TRACE("Updating watermarks");
    response->set_responder_term(state_->GetCurrentTermUnlocked());
    status->mutable_last_received()->CopyFrom(state_->GetLastReceivedOpIdUnlocked());
  }

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    if (request->ops_size() == 0) {
      VLOG(1) << state_->LogPrefix() << "Replica replied to status only request. Replica: "
              << state_->ToString() << " Status: " << status->ShortDebugString();
    }
  }
  RETURN_NOT_OK(ExecuteHook(POST_UPDATE));
  return Status::OK();
}

Status RaftConsensus::UpdateReplica(const ConsensusRequestPB* request) {
  Synchronizer log_synchronizer;
  vector<const OperationPB*> replicate_ops;


  // The ordering of the following operations is crucial, read on for details.
  //
  // The main requirements explained in more detail below are:
  //
  //   1) We must enqueue the prepares before we write to our local log.
  //   2) If we were able to enqueue a prepare then we must be able to log it.
  //   3) If we fail to enqueue a prepare, we must not attempt to enqueue any
  //      later-indexed prepare or apply.
  //
  // See below for detailed rationale.
  //
  // The steps are:
  //
  // 0 - Split/Dedup
  //
  // We split the operations into replicates and commits and make sure that we don't
  // don't do anything on operations we've already received in a previous call.
  // This essentially makes this method idempotent.
  //
  // 1 - We enqueue the Prepare of the transactions.
  //
  // The actual prepares are enqueued in order but happen asynchronously so we don't
  // have decoding/acquiring locks on the critical path.
  //
  // We need to do this first for a number of reasons:
  // - Prepares, by themselves, are inconsequential, i.e. they do not mutate the
  //   state machine so, were we to crash afterwards, having the prepares in-flight
  //   won't hurt.
  // - Prepares depend on factors external to consensus (the transaction drivers and
  //   the tablet peer) so if for some reason they cannot be enqueued we must know
  //   before we try write them to the WAL. Once enqueued, prepares that fail for
  //   some reason (like an error while decoding a write) will have a corresponding
  //   OP_ABORT commit message, so we are not concerned about having written them to
  //   the wal.
  // - The prepares corresponding to every operation that was logged must be in-flight
  //   first. This because should we need to abort certain transactions (say a new leader
  //   says they are not committed) we need to have those prepares in-flight so that
  //   the transactions can be continued (in the abort path).
  // - Failure to enqueue prepares is OK, we can continue and let the leader know that
  //   we only went so far. The leader will re-send the remaining messages.
  //
  // 2 - We enqueue the writes to the WAL.
  //
  // We enqueue writes to the WAL, but only the operations that were successfully
  // enqueued for prepare (for the reasons introduced above). This means that even
  // if a prepare fails to enqueue, if any of the previous prepares were successfully
  // submitted they must be written to the WAL.
  // If writing to the WAL fails, we're in an inconsistent state and we crash. In this
  // case, no one will ever know of the transactions we previously prepared so those are
  // inconsequential.
  //
  // 3 - We mark the transactions as committed
  //
  // For each transaction which has been committed by the leader, we update the
  // transaction state to reflect that. If the logging has already succeeded for that
  // transaction, this will trigger the Apply phase. Otherwise, Apply will be triggered
  // when the logging completes. In both cases the Apply phase executes asynchronously.
  // This must, of course, happen after the prepares have been triggered as the same batch
  // can both replicate/prepare and commit/apply an operation.
  //
  // Currently, if a prepare failed to enqueue we still trigger all applies for operations
  // with an id lower than it (if we have them). This is important now as the leader will
  // not re-send those commit messages. This will be moot when we move to the commit
  // commitIndex way of doing things as we can simply ignore the applies as we know
  // they will be triggered with the next successful batch.
  //
  // 4 - We wait for the writes to be durable.
  //
  // Before replying to the leader we wait for the writes to be durable. We then
  // just update the last replicated watermark and respond.
  //
  // TODO - These failure scenarios need to be exercised in an unit
  //        test. Moreover we need to add more fault injection spots (well that
  //        and actually use the) for each of these steps.
  //        This will be done in a follow up patch.
  OpId last_enqueued_prepare;
  int successfully_triggered_prepares = 0;
  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForUpdate(&lock));

    // Do term checks first:
    if (PREDICT_FALSE(request->caller_term() != state_->GetCurrentTermUnlocked())) {
      // Eventually we'll respond error here but right now just check.
      CHECK_OK(state_->SetCurrentTermUnlocked(request->caller_term()));
      // - Since we don't handle in-flights when the terms change yet
      //   make sure that we don't have any.
      CHECK_EQ(state_->GetNumPendingTxnsUnlocked(), 0);
    }

    // TODO check for the log matching property in a follow-up patch.

    TRACE("Updating replica for $0 ops", request->ops_size());

    // 0 - Split/Dedup

    // Split the operations into two lists, one for REPLICATE
    // and one for COMMIT. Also filter out any ops which have already
    // been handled by a previous call to UpdateReplica()
    BOOST_FOREACH(const OperationPB& op, request->ops()) {

      if (OpIdCompare(op.id(), state_->GetLastReceivedOpIdUnlocked()) <= 0) {
        VLOG_WITH_PREFIX(2) << "Skipping op id " << op.id().ShortDebugString()
            << " (already replicated/committed)";
        continue;
      }
      if (PREDICT_TRUE(op.has_replicate())) {
        replicate_ops.push_back(&const_cast<OperationPB&>(op));
      } else {
        LOG_WITH_PREFIX(FATAL)<< "Unexpected op: " << op.ShortDebugString();
      }
    }

    // Only accept requests from a sender with a terms that is equal to or higher
    // than our own.
    if (PREDICT_FALSE(request->caller_term() < state_->GetCurrentTermUnlocked())) {
      return Status::IllegalState(
          Substitute("Sender peer's term: $0 is lower than this replica's term: $1.",
                     request->caller_term(),
                     state_->GetCurrentTermUnlocked()));
    }

    // 1 - Enqueue the prepares

    TRACE("Triggering prepare for $0 ops", replicate_ops.size());
    Status prepare_status;
    for (int i = 0; i < replicate_ops.size(); i++) {
      gscoped_ptr<OperationPB> op_copy(new OperationPB(*replicate_ops[i]));
      gscoped_ptr<ConsensusRound> context(new ConsensusRound(this, op_copy.Pass()));
      prepare_status = state_->EnqueuePrepareUnlocked(context.Pass());
      if (PREDICT_FALSE(!prepare_status.ok())) {
        LOG_WITH_PREFIX(WARNING) << "Failed to prepare operation: "
            << replicate_ops[i]->id().ShortDebugString() << " Status: "
            << prepare_status.ToString();
        break;
      }
      successfully_triggered_prepares++;
    }

    // If we failed all prepares, issue a warning.
    // TODO: this seems to always happen for the first couple round trips
    // in a new term, since we can't prepare any WRITE_OPs while we're still
    // in CONFIGURING mode.
    if (PREDICT_FALSE(!prepare_status.ok() && successfully_triggered_prepares == 0)) {
      LOG_WITH_PREFIX(WARNING) << "Could not trigger prepares. Status: "
                               << prepare_status.ToString();
    }

    last_enqueued_prepare.CopyFrom(state_->GetLastReplicatedOpIdUnlocked());
    if (successfully_triggered_prepares > 0) {
      last_enqueued_prepare = replicate_ops[successfully_triggered_prepares - 1]->id();
    }

    // 2 - Enqueue the writes.
    // Now that we've triggered the prepares enqueue the operations to be written
    // to the WAL.
    LogEntryBatch* reserved_entry_batch;
    if (PREDICT_TRUE(successfully_triggered_prepares > 0)) {
      // Trigger the log append asap, if fsync() is on this might take a while
      // and we can't reply until this is done.
      gscoped_ptr<log::LogEntryBatchPB> entry_batch;
      log::CreateBatchFromAllocatedOperations(&replicate_ops[0],
                                              successfully_triggered_prepares,
                                              &entry_batch);

      // Since we've prepared we need to be able to append (or we risk trying to apply
      // later something that wasn't logged). We crash if we can't.
      CHECK_OK(log_->Reserve(entry_batch.Pass(), &reserved_entry_batch));
      CHECK_OK(log_->AsyncAppend(reserved_entry_batch, log_synchronizer.AsStatusCallback()));
    }

    // 3 - Mark transactions as committed

    // Choose the last operation to be applied. This will either be 'committed_index', if
    // no prepare enqueuing failed, or the minimum between 'committed_index' and the id of
    // the last successfully enqueued prepare, if some prepare failed to enqueue.
    OpId dont_apply_after = request->committed_index();
    if (CopyIfOpIdLessThan(last_enqueued_prepare, &dont_apply_after)) {
      VLOG_WITH_PREFIX(2) << "Received commit index "
                          << request->committed_index().ShortDebugString()
                          << " from the leader but only marked up to "
                          << last_enqueued_prepare.ShortDebugString()
                          << " as committed due to failed prepares.";
    }

    VLOG_WITH_PREFIX(2) << "Marking committed up to " << dont_apply_after.ShortDebugString();
    TRACE(Substitute("Marking committed up to $0", dont_apply_after.ShortDebugString()));
    CHECK_OK(state_->MarkConsensusCommittedUpToUnlocked(dont_apply_after));

    // We can now update the last received watermark.
    //
    // We do it here (and before we actually hear back from the wal whether things
    // are durable) so that, if we receive another, possible duplicate, message
    // that exercises this path we don't handle these messages twice.
    //
    // If all prepares were successful we just set the last received to the last
    // message in the batch. If any prepare failed we set last received to the
    // last successful prepare (we're sure we didn't prepare or apply anything after
    // that).
    TRACE(Substitute("Updating last received op as $0", last_enqueued_prepare.ShortDebugString()));
    state_->UpdateLastReceivedOpIdUnlocked(last_enqueued_prepare);
  }
  // Release the lock while we wait for the log append to finish so that commits can go through.
  // We'll re-acquire it before we update the state again.

  // Update the last replicated op id
  if (successfully_triggered_prepares > 0) {

    // 4 - We wait for the writes to be durable.

    // Note that this is safe because dist consensus now only supports a single outstanding
    // request at a time and this way we can allow commits to proceed while we wait.
    TRACE("Waiting on the replicates to finish logging");
    RETURN_NOT_OK(log_synchronizer.Wait());
    TRACE("finished");

    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForUpdate(&lock));
    state_->UpdateLastReplicatedOpIdUnlocked(last_enqueued_prepare);
  }

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    VLOG_WITH_PREFIX_LK(1) << "Replica updated. Replica: "
        << state_->ToString() << " Request: " << request->ShortDebugString();
  }

  TRACE("UpdateReplicas() finished");
  return Status::OK();
}

Status RaftConsensus::RequestVote(const VoteRequestPB* request,
                                  VoteResponsePB* response) {
  return Status::NotSupported("Not Implemented yet.");
}

void RaftConsensus::SignalRequestToPeers(bool force_if_queue_empty) {
  PeersMap::iterator iter = peers_.begin();
  for (; iter != peers_.end(); iter++) {
    Status s = (*iter).second->SignalRequest(force_if_queue_empty);
    if (PREDICT_FALSE(!s.ok())) {
      LOG_WITH_PREFIX_LK(WARNING) << "Peer was closed, removing from peers. Peer: "
          << (*iter).second->peer_pb().ShortDebugString();
      peers_.erase(iter);
    }
  }
}

void RaftConsensus::ClosePeers() {
  PeersMap::iterator iter = peers_.begin();
  for (; iter != peers_.end(); iter++) {
    (*iter).second->Close();
  }
}

void RaftConsensus::Shutdown() {
  CHECK_OK(ExecuteHook(PRE_SHUTDOWN));
  LOG_WITH_PREFIX_LK(INFO) << "Raft consensus shutting down.";
  {
    ReplicaState::UniqueLock lock;
    Status s = state_->LockForShutdown(&lock);
    if (s.IsIllegalState()) return;
    ClosePeers();
    queue_.Close();
  }
  CHECK_OK(state_->CancelPendingTransactions());
  CHECK_OK(state_->WaitForOustandingApplies());
  LOG_WITH_PREFIX_LK(INFO) << "Raft consensus Shutdown!";
  CHECK_OK(state_->Shutdown());
  CHECK_OK(ExecuteHook(POST_SHUTDOWN));
}

Status RaftConsensus::RegisterOnReplicateCallback(
    const OpId& op_id,
    const std::tr1::shared_ptr<FutureCallback>& repl_callback) {
  return state_->RegisterOnReplicateCallback(op_id, repl_callback);
}

Status RaftConsensus::RegisterOnCommitCallback(
    const OpId& op_id,
    const std::tr1::shared_ptr<FutureCallback>& repl_callback) {
  return state_->RegisterOnCommitCallback(op_id, repl_callback);
}

OpId RaftConsensus::GetLastOpIdFromLog() {
  OpId id;
  Status s = log_->GetLastEntryOpId(&id);
  if (s.ok()) {
  } else if (s.IsNotFound()) {
    id = MinimumOpId();
  } else {
    LOG_WITH_PREFIX(FATAL) << "Unexpected status from Log::GetLastEntryOpId(): " << s.ToString();
  }
  return id;
}

QuorumPeerPB::Role RaftConsensus::role() const {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  return GetRoleInQuorum(state_->GetPeerUuid(), state_->GetCommittedQuorumUnlocked());
}

string RaftConsensus::peer_uuid() const {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  return state_->GetPeerUuid();
}

QuorumPB RaftConsensus::Quorum() const {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  return state_->GetCommittedQuorumUnlocked();
}

void RaftConsensus::DumpStatusHtml(std::ostream& out) const {
  out << "<h1>Raft Consensus State</h1>" << std::endl;

  out << "<h2>State</h2>" << std::endl;
  out << "<pre>" << EscapeForHtmlToString(queue_.ToString()) << "</pre>" << std::endl;

  // Dump the queues on a leader.
  QuorumPeerPB::Role role;
  {
    ReplicaState::UniqueLock lock;
    CHECK_OK(state_->LockForRead(&lock));
    role = state_->GetActiveQuorumStateUnlocked().role;
  }
  if (role == QuorumPeerPB::LEADER) {
    out << "<h2>Queue overview</h2>" << std::endl;
    out << "<pre>" << EscapeForHtmlToString(queue_.ToString()) << "</pre>" << std::endl;
    out << "<hr/>" << std::endl;
    out << "<h2>Queue details</h2>" << std::endl;
    queue_.DumpToHtml(out);
  }
}

ReplicaState* RaftConsensus::GetReplicaStateForTests() {
  return state_.get();
}

RaftConsensus::~RaftConsensus() {
  Shutdown();
  STLDeleteValues(&peers_);
}

}  // namespace consensus
}  // namespace kudu
