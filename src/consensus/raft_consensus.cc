// Copyright (c) 2013, Cloudera, inc.

#include "consensus/raft_consensus.h"

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <iostream>
#include <string>

#include "consensus/log.h"
#include "consensus/consensus_peers.h"
#include "consensus/raft_consensus_state.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "server/clock.h"
#include "server/metadata.h"
#include "util/trace.h"
#include "util/url-coding.h"

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
using strings::Substitute;
using tserver::ChangeConfigRequestPB;
using tserver::ChangeConfigResponsePB;
using tserver::TabletServerServiceProxy;

RaftConsensus::RaftConsensus(const ConsensusOptions& options,
                             gscoped_ptr<PeerProxyFactory> proxy_factory,
                             const MetricContext& metric_ctx)
    : log_(NULL),
      peer_proxy_factory_(proxy_factory.Pass()),
      queue_(metric_ctx) {
  CHECK_OK(ThreadPoolBuilder("raft-op-cb").set_max_threads(1).Build(&callback_pool_));
  state_.reset(new ReplicaState(options, callback_pool_.get()));
}

Status RaftConsensus::Init(const metadata::QuorumPeerPB& peer,
                           const scoped_refptr<server::Clock>& clock,
                           ReplicaTransactionFactory* txn_factory,
                           log::Log* log) {
  log_ = log;
  clock_ = clock;
  OpId initial = GetLastOpIdFromLog();
  RETURN_NOT_OK(state_->Init(peer.permanent_uuid(),
                             txn_factory,
                             initial.term(),
                             initial.index()));
  LOG_WITH_PREFIX_LK(INFO) << "Created Raft consensus for peer " << state_->ToString();
  return Status::OK();
}

Status RaftConsensus::Start(const metadata::QuorumPB& initial_quorum,
                            const ConsensusBootstrapInfo& bootstrap_info,
                            gscoped_ptr<metadata::QuorumPB>* running_quorum) {
  RETURN_NOT_OK(ExecuteHook(PRE_START));

  RETURN_NOT_OK_PREPEND(ChangeConfig(initial_quorum),
                        "Failed to change to initial quorum config");

  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  LOG_WITH_PREFIX(INFO) << "Started. Quorum: "
      << state_->GetCurrentConfigUnlocked().ShortDebugString();
  running_quorum->reset(new QuorumPB(state_->GetCurrentConfigUnlocked()));
  RETURN_NOT_OK(ExecuteHook(POST_START));
  return Status::OK();
}

Status RaftConsensus::ChangeConfig(QuorumPB new_config) {
  RETURN_NOT_OK(ExecuteHook(PRE_CONFIG_CHANGE));

  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForConfigChange(&lock));
  QuorumPB old_config = state_->GetCurrentConfigUnlocked();
  RETURN_NOT_OK(state_->ChangeConfigUnlocked(new_config));
  if (state_->GetCurrentRoleUnlocked() == QuorumPeerPB::LEADER) {
    state_->IncrementConfigSeqNoUnlocked();
    RETURN_NOT_OK(CreateOrUpdatePeersUnlocked());
    RETURN_NOT_OK(PushConfigurationToPeersUnlocked());
  }
  state_->SetChangeConfigSuccessfulUnlocked();
  RETURN_NOT_OK(ExecuteHook(POST_CONFIG_CHANGE));
  return Status::OK();
}

Status RaftConsensus::CreateOrUpdatePeersUnlocked() {
  unordered_set<string> new_peers;
  // Create new peers
  BOOST_FOREACH(const QuorumPeerPB& peer_pb, state_->GetCurrentConfigUnlocked().peers()) {
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
                                       &local_peer))
      peers_.insert(pair<string, Peer*>(peer_pb.permanent_uuid(), local_peer.release()));
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
                                        &remote_peer))
      peers_.insert(pair<string, Peer*>(peer_pb.permanent_uuid(), remote_peer.release()));
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

Status RaftConsensus::PushConfigurationToPeersUnlocked() {

  OpId replicate_op_id;
  state_->NewIdUnlocked(&replicate_op_id);
  gscoped_ptr<OperationPB> replicate_op(new OperationPB());
  replicate_op->mutable_id()->CopyFrom(replicate_op_id);
  ReplicateMsg* replicate = replicate_op->mutable_replicate();
  replicate->set_op_type(CHANGE_CONFIG_OP);
  ChangeConfigRequestPB* cc_request = replicate->mutable_change_config_request();
  cc_request->set_tablet_id(state_->GetOptions().tablet_id);
  QuorumPB* new_config = cc_request->mutable_new_config();
  new_config->CopyFrom(state_->GetCurrentConfigUnlocked());

  const string log_prefix = Substitute("$0ChangeConfiguration(op=$1, seqno=$2)",
                                       state_->LogPrefixUnlocked(),
                                       replicate_op_id.ShortDebugString(),
                                       new_config->seqno());
  LOG(INFO) << log_prefix << ": replicating to peers...";
  scoped_refptr<OperationStatusTracker> repl_status(
      new MajorityOpStatusTracker(replicate_op.Pass(),
                                  state_->GetCurrentVotingPeersUnlocked(),
                                  state_->GetCurrentMajorityUnlocked(),
                                  state_->GetAllPeersCountUnlocked()));
  RETURN_NOT_OK_PREPEND(queue_.AppendOperation(repl_status),
                        "Could not append change config replication req. to the queue");

  SignalRequestToPeers();
  repl_status->Wait();

  LOG(INFO) << log_prefix << ": committing...";

  gscoped_ptr<OperationPB> commit_op(new OperationPB);
  state_->NewIdUnlocked(commit_op->mutable_id());
  CommitMsg* commit_msg = commit_op->mutable_commit();
  commit_msg->set_op_type(CHANGE_CONFIG_OP);
  commit_msg->mutable_commited_op_id()->CopyFrom(replicate_op_id);
  commit_msg->mutable_change_config_response();
  commit_msg->set_timestamp(clock_->Now().ToUint64());

  scoped_refptr<OperationStatusTracker> commit_status(
      new MajorityOpStatusTracker(commit_op.Pass(),
                                  state_->GetCurrentVotingPeersUnlocked(),
                                  state_->GetCurrentMajorityUnlocked(),
                                  state_->GetAllPeersCountUnlocked()));

  RETURN_NOT_OK_PREPEND(queue_.AppendOperation(commit_status),
                        "Could not append change config commit req. to the queue");

  SignalRequestToPeers();
  // Wait for the commit to complete
  commit_status->Wait();

  LOG(INFO) << log_prefix << ": a majority of peers have accepted the new configuration. "
            << "Proceeding.";
  return Status::OK();
}

Status RaftConsensus::Replicate(ConsensusRound* context) {

  RETURN_NOT_OK(ExecuteHook(PRE_REPLICATE));

  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForReplicate(&lock));

    state_->NewIdUnlocked(context->replicate_op()->mutable_id());

    // the original instance of the replicate msg is owned by the consensus context
    // so we create a copy for the queue.
    gscoped_ptr<OperationPB> queue_op(new OperationPB(*context->replicate_op()));

    scoped_refptr<OperationStatusTracker> status(
        new MajorityOpStatusTracker(queue_op.Pass(),
                                    state_->GetCurrentVotingPeersUnlocked(),
                                    state_->GetCurrentMajorityUnlocked(),
                                    state_->GetAllPeersCountUnlocked(),
                                    callback_pool_.get(),
                                    context->replicate_callback()));

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
    RETURN_NOT_OK(s);
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
  QuorumPeerPB::Role role = state_->GetCurrentRoleUnlocked();
  if (role == QuorumPeerPB::LEADER) {
    RETURN_NOT_OK(LeaderCommitUnlocked(context, context->commit_op()));
  } else {
    RETURN_NOT_OK(ReplicaCommitUnlocked(context, context->commit_op()));
  }
  RETURN_NOT_OK(ExecuteHook(POST_COMMIT));

  // NOTE: RaftConsensus instance might be destroyed after this call.
  state_->CountDownOutstandingCommitsIfShuttingDown();
  return Status::OK();
}

OperationStatusTracker* RaftConsensus::CreateLeaderOnlyOperationStatusUnlocked(
    gscoped_ptr<OperationPB> operation,
    const shared_ptr<FutureCallback>& commit_callback) {
  unordered_set<string> leader_uuid_set(1);
  InsertOrDie(&leader_uuid_set, state_->GetPeerUuid());
  return new MajorityOpStatusTracker(operation.Pass(),
                                     leader_uuid_set,
                                     1,
                                     state_->GetAllPeersCountUnlocked(),
                                     callback_pool_.get(),
                                     commit_callback);
}

OperationStatusTracker* RaftConsensus::CreateLeaderOnlyOperationStatusUnlocked(
    gscoped_ptr<OperationPB> operation) {
  unordered_set<string> leader_uuid_set(1);
  InsertOrDie(&leader_uuid_set, state_->GetPeerUuid());
  return new MajorityOpStatusTracker(operation.Pass(), leader_uuid_set,
                                     1, state_->GetAllPeersCountUnlocked());
}

Status RaftConsensus::LeaderCommitUnlocked(ConsensusRound* context,
                                           OperationPB* commit_op) {
  // entry for the CommitMsg
  state_->NewIdUnlocked(commit_op->mutable_id());

  // take ownership of the commit_op as this will go in the queue
  gscoped_ptr<OperationPB> owned_op(context->release_commit_op());

  // the commit callback is the very last thing to execute in a transaction
  // so it needs to free all resources. We need release it from the
  // ConsensusRound or we'd get a cycle. (callback would free the
  // TransactionState which would free the ConsensusRound, which in turn
  // would try to free the callback).
  shared_ptr<FutureCallback> commit_clbk;
  context->release_commit_callback(&commit_clbk);

  scoped_refptr<OperationStatusTracker> status;
  // If the context included a callback set it in the tracker, so that it gets
  // called when the tracker reports IsDone(), if not just create a status tracker
  // without a callback meaning the caller doesn't want to be notified when the
  // operation completes.
  if (PREDICT_TRUE(commit_clbk.get() != NULL)) {
    status.reset(CreateLeaderOnlyOperationStatusUnlocked(owned_op.Pass(), commit_clbk));
  } else {
    status.reset(CreateLeaderOnlyOperationStatusUnlocked(owned_op.Pass()));
  }

  RETURN_NOT_OK_PREPEND(queue_.AppendOperation(status),
                        "Could not append commit request to the queue");

  if (VLOG_IS_ON(1)) {
    VLOG_WITH_PREFIX(1) << "Leader appended commit. Leader: "
        << state_->ToString() << " Commit: " << commit_op->ShortDebugString();
  }

  state_->UpdateLeaderCommittedOpIdUnlocked(commit_op->commit().commited_op_id());

  SignalRequestToPeers();
  return Status::OK();
}
Status RaftConsensus::ReplicaCommitUnlocked(ConsensusRound* context,
                                            OperationPB* commit_op) {

  if (VLOG_IS_ON(1)) {
    VLOG_WITH_PREFIX(1) << "Replica appending commit. Replica: "
        << state_->ToString() << " Commit: " << commit_op->ShortDebugString();
  }

  // Copy the ids to update later as we can't be sure they will be alive
  // after the log append.
  OpId commit_op_id = commit_op->id();
  OpId committed_op_id = commit_op->commit().commited_op_id();

  LogEntryBatch* reserved_entry_batch;
  RETURN_NOT_OK(log_->Reserve(&commit_op, 1, &reserved_entry_batch));
  // TODO: replace the FutureCallbacks in ConsensusContext with StatusCallbacks
  //
  // AsyncAppend takes ownership of 'ftscb'.
  RETURN_NOT_OK(log_->AsyncAppend(reserved_entry_batch,
                                  context->commit_callback()->AsStatusCallback()));

  state_->UpdateReplicaCommittedOpIdUnlocked(commit_op_id, committed_op_id);
  return Status::OK();
}

Status RaftConsensus::Update(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response) {
  RETURN_NOT_OK(ExecuteHook(PRE_UPDATE));
  ConsensusStatusPB* status = response->mutable_status();

  // see var declaration
  boost::lock_guard<simple_spinlock> lock(update_lock_);

  // If there are any operations we update our state machine, otherwise just send the status.
  if (PREDICT_TRUE(request->ops_size() > 0)) {
    RETURN_NOT_OK(UpdateReplica(request, status));
  }

  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForRead(&lock));
    TRACE("Updating watermarks");
    status->mutable_replicated_watermark()->CopyFrom(state_->GetLastReplicatedOpIdUnlocked());
    status->mutable_received_watermark()->CopyFrom(state_->GetLastReceivedOpIdUnlocked());
    status->mutable_safe_commit_watermark()->CopyFrom(state_->GetSafeCommitOpIdUnlocked());
  }

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    if (request->ops_size() == 0) {
      VLOG_WITH_PREFIX(1) <<  "Replica replied to status only request. Replica: "
          << state_->ToString() << " Status: " << status->ShortDebugString();
    }
  }
  RETURN_NOT_OK(ExecuteHook(POST_UPDATE));
  return Status::OK();
}

Status RaftConsensus::UpdateReplica(const ConsensusRequestPB* request,
                                    ConsensusStatusPB* status) {
  Synchronizer log_synchronizer;
  vector<const OperationPB*> replicate_ops;
  vector<const OperationPB*> commit_ops;

  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForUpdate(&lock));
    TRACE("Updating replica for $0 ops", request->ops_size());

    // Split the operations into two lists, one for REPLICATE
    // and one for COMMIT. Also filter out any ops which have already
    // been handled by a previous call to UpdateReplica()
    BOOST_FOREACH(const OperationPB& op, request->ops()) {
      if (log::OpIdCompare(op.id(), state_->GetLastReceivedOpIdUnlocked()) <= 0) {
        VLOG_WITH_PREFIX(2) << "Skipping op id " << op.id().ShortDebugString()
            << " (already replicated/committed)";
        continue;
      }
      if (op.has_replicate()) {
        replicate_ops.push_back(&const_cast<OperationPB&>(op));
      } else if (op.has_commit()) {
        commit_ops.push_back(&op);
      } else {
        LOG_WITH_PREFIX(FATAL)<< "Unexpected op: " << op.ShortDebugString();
      }
    }

    // no operations were received or all operations were duplicated, return.
    if (PREDICT_FALSE(replicate_ops.empty() && commit_ops.empty())) return Status::OK();

    // Only accept requests from the leader set in the last change config
    if (PREDICT_FALSE(request->sender_uuid() != state_->GetLeaderUuidUnlocked())) {
      return Status::IllegalState("Sender peer is not the leader of this quorum.");
    }

    LogEntryBatch* reserved_entry_batch;
    if (!replicate_ops.empty()) {
      // Trigger the log append asap, if fsync() is on this might take a while
      // and we can't reply until this is done.
      CHECK_OK(log_->Reserve(&replicate_ops[0], replicate_ops.size(), &reserved_entry_batch));
      CHECK_OK(log_->AsyncAppend(reserved_entry_batch, log_synchronizer.AsStatusCallback()));
    }

    // Trigger the replica Prepares() and Apply()s.
    // NOTE: This doesn't actually wait for the transactions to complete.
    BOOST_FOREACH(const OperationPB* op, replicate_ops) {
      gscoped_ptr<OperationPB> op_copy(new OperationPB(*op));

      // For replicas we build a consensus context with the op in the request
      // and two latch callbacks.
      gscoped_ptr<ConsensusRound> context(new ConsensusRound(this, op_copy.Pass()));
      CHECK_OK(state_->TriggerPrepareUnlocked(context.Pass()));
    }

    BOOST_FOREACH(const OperationPB* op, commit_ops) {
      gscoped_ptr<OperationPB> op_copy(new OperationPB(*op));

      CHECK_OK(state_->TriggerApplyUnlocked(op_copy.Pass()));
    }
    state_->UpdateLastReceivedOpIdUnlocked(request->ops(request->ops_size() - 1).id());
  }

  // Update the last replicated op id
  if (!replicate_ops.empty()) {
    // Release the lock while we wait for the log append to finish, and re-acquire it to update
    // the replica state.
    // Note that this is safe because dist consensus now only supports a single outstanding
    // request at a time and this way we can allow commits to proceed while we wait.
    TRACE("Waiting on the replicates to finish logging");
    RETURN_NOT_OK(log_synchronizer.Wait());
    TRACE("finished");

    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForUpdate(&lock));
    state_->UpdateLastReplicatedOpIdUnlocked(replicate_ops[replicate_ops.size() - 1]->id());
  }

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    VLOG_WITH_PREFIX_LK(1) << "Replica updated. Replica: "
        << state_->ToString() << " Request: " << request->ShortDebugString();
  }

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
      LOG_WITH_PREFIX(WARNING) << "Peer was closed, removing from peers. Peer: "
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
  STLDeleteValues(&peers_);
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
    id = log::MinimumOpId();
  } else {
    LOG_WITH_PREFIX(FATAL) << "Unexpected status from Log::GetLastEntryOpId(): " << s.ToString();
  }
  return id;
}

QuorumPeerPB::Role RaftConsensus::role() const {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  return state_->GetCurrentRoleUnlocked();
}

string RaftConsensus::peer_uuid() const {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  return state_->GetPeerUuid();
}


QuorumPB RaftConsensus::Quorum() const {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  return state_->GetCurrentConfigUnlocked();
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
    role = state_->GetCurrentRoleUnlocked();
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
}

}  // namespace consensus
}  // namespace kudu

