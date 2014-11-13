// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/raft_consensus.h"

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <iostream>
#include <string>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/leader_election.h"
#include "kudu/consensus/peer_manager.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus_state.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/server/clock.h"
#include "kudu/server/metadata.h"
#include "kudu/util/failure_detector.h"
#include "kudu/util/random_util.h"
#include "kudu/util/trace.h"
#include "kudu/util/url-coding.h"

DEFINE_int32(leader_heartbeat_interval_ms, 500,
             "The LEADER's heartbeat interval to the replicas.");

// Defaults to be the same value as the leader heartbeat interval.
DEFINE_int32(leader_failure_monitor_check_mean_ms, 500,
             "The mean failure-checking interval of the randomized failure monitor.");

// Defaults to half of the mean (above).
DEFINE_int32(leader_failure_monitor_check_stddev_ms, 250,
             "The standard deviation of the failure-checking interval of the randomized "
             "failure monitor.");

DEFINE_double(leader_failure_max_missed_heartbeat_periods, 3.0,
             "Maximum heartbeat periods that the leader can fail to heartbeat in before we "
             "consider the leader to be failed. The total failure timeout in milliseconds is "
             "leader_heartbeat_interval_ms times leader_failure_max_missed_heartbeat_periods. "
             "The value passed to this flag may be fractional.");

DEFINE_bool(enable_leader_failure_detection, true,
            "Whether to enable failure detection of tablet leaders. If enabled, attempts will be "
            "made to fail over to a follower when the leader is detected to have failed.");

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

// Special string that represents any known leader to the failure detector.
static const char* const kTimerId = "election-timer";

scoped_refptr<RaftConsensus> RaftConsensus::Create(
    const ConsensusOptions& options,
    gscoped_ptr<ConsensusMetadata> cmeta,
    const std::string& peer_uuid,
    const MetricContext& metric_ctx,
    const scoped_refptr<server::Clock>& clock,
    ReplicaTransactionFactory* txn_factory,
    const std::tr1::shared_ptr<rpc::Messenger>& messenger,
    log::Log* log) {
  gscoped_ptr<consensus::PeerProxyFactory> rpc_factory(
    new RpcPeerProxyFactory(messenger));

  // The message queue that keeps track of which operations need to be replicated
  // where.
  gscoped_ptr<PeerMessageQueue> queue(new PeerMessageQueue(metric_ctx));

  // A manager for the set of peers that actually send the operations both remotely
  // and to the local wal.
  gscoped_ptr<PeerManager> peer_manager(
    new PeerManager(options.tablet_id,
                    peer_uuid,
                    rpc_factory.get(),
                    queue.get(),
                    log));

  return make_scoped_refptr(new RaftConsensus(
                              options,
                              cmeta.Pass(),
                              rpc_factory.Pass(),
                              queue.Pass(),
                              peer_manager.Pass(),
                              metric_ctx,
                              peer_uuid,
                              clock,
                              txn_factory,
                              log));
}

RaftConsensus::RaftConsensus(const ConsensusOptions& options,
                             gscoped_ptr<ConsensusMetadata> cmeta,
                             gscoped_ptr<PeerProxyFactory> proxy_factory,
                             gscoped_ptr<PeerMessageQueue> queue,
                             gscoped_ptr<PeerManager> peer_manager,
                             const MetricContext& metric_ctx,
                             const std::string& peer_uuid,
                             const scoped_refptr<server::Clock>& clock,
                             ReplicaTransactionFactory* txn_factory,
                             log::Log* log)
    : log_(DCHECK_NOTNULL(log)),
      clock_(clock),
      peer_proxy_factory_(proxy_factory.Pass()),
      queue_(queue.Pass()),
      peer_manager_(peer_manager.Pass()),
      failure_monitor_(GetRandomSeed32(),
                       FLAGS_leader_failure_monitor_check_mean_ms,
                       FLAGS_leader_failure_monitor_check_stddev_ms),
      failure_detector_(new TimedFailureDetector(
          MonoDelta::FromMilliseconds(
              FLAGS_leader_heartbeat_interval_ms *
              FLAGS_leader_failure_max_missed_heartbeat_periods))) {
  state_.reset(new ReplicaState(options,
                                peer_uuid,
                                cmeta.Pass(),
                                DCHECK_NOTNULL(txn_factory)));
}

RaftConsensus::~RaftConsensus() {
  Shutdown();
  STLDeleteValues(&election_proxies_);
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

  // This just starts the monitor thread -- no failure detector is registered yet.
  if (FLAGS_enable_leader_failure_detection) {
    RETURN_NOT_OK(failure_monitor_.Start());
  }

  // Register the failure detector instance with the monitor.
  // We still have not enabled failure detection for the leader election timer.
  // That happens separately via the helper functions
  // EnsureFailureDetector(Enabled/Disabled)Unlocked();
  RETURN_NOT_OK(failure_monitor_.MonitorFailureDetector(state_->GetOptions().tablet_id,
                                                        failure_detector_));

  QuorumPB initial_quorum;
  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForStart(&lock));

    initial_quorum.CopyFrom(state_->GetCommittedQuorumUnlocked());
    RETURN_NOT_OK_PREPEND(VerifyQuorumAndCheckThatNoChangeIsPendingUnlocked(initial_quorum),
                          "Invalid state on RaftConsensus::Start()");

    RETURN_NOT_OK_PREPEND(state_->StartUnlocked(info.last_id),
                          "Unable to start RAFT ReplicaState");

    LOG_WITH_PREFIX(INFO) << "Replica starting. Triggering " << info.orphaned_replicates.size()
        << " pending transactions.";
    BOOST_FOREACH(ReplicateMsg* replicate, info.orphaned_replicates) {
      RETURN_NOT_OK(StartReplicaTransactionUnlocked(
          make_gscoped_ptr(new ReplicateMsg(*replicate))));
    }
  }

  state_->AdvanceCommittedIndex(info.last_committed_id);

  // TODO: Revisit the PeerProxy ownership model once we need to support
  // quorum membership changes and after we refactor the Peer tests.
  string local_uuid = peer_uuid();
  STLDeleteValues(&election_proxies_);
  BOOST_FOREACH(const QuorumPeerPB& peer, initial_quorum.peers()) {
    if (peer.permanent_uuid() == local_uuid) continue;
    gscoped_ptr<PeerProxy> proxy;
    RETURN_NOT_OK(peer_proxy_factory_->NewProxy(peer, &proxy));
    InsertOrDie(&election_proxies_, peer.permanent_uuid(), proxy.release());
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

    QuorumPB new_quorum;
    RETURN_NOT_OK(GivePeerRoleInQuorum(state_->GetPeerUuid(),
                                       QuorumPeerPB::LEADER,
                                       state_->GetCommittedQuorumUnlocked(),
                                       &new_quorum));
    new_quorum.set_seqno(state_->GetCommittedQuorumUnlocked().seqno() + 1);
    // Increment the term.
    RETURN_NOT_OK(state_->IncrementTermUnlocked());
    RETURN_NOT_OK_PREPEND(VerifyQuorumAndCheckThatNoChangeIsPendingUnlocked(new_quorum),
                          "Invalid state on RaftConsensus::EmulateElection()");
    RETURN_NOT_OK(state_->SetPendingQuorumUnlocked(new_quorum));
    return ChangeConfigUnlocked();
  }
}

Status RaftConsensus::StartElection() {

  scoped_refptr<LeaderElection> election;
  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForConfigChange(&lock));

    QuorumPB new_quorum;
    RETURN_NOT_OK(GivePeerRoleInQuorum(state_->GetPeerUuid(),
                                       QuorumPeerPB::CANDIDATE,
                                       state_->GetCommittedQuorumUnlocked(),
                                       &new_quorum));
    new_quorum.set_seqno(state_->GetCommittedQuorumUnlocked().seqno() + 1);
    // Increment the term.
    RETURN_NOT_OK(state_->IncrementTermUnlocked());
    RETURN_NOT_OK(VerifyQuorum(new_quorum));

    RETURN_NOT_OK(state_->SetPendingQuorumUnlocked(new_quorum));

    // Snooze to avoid the election timer firing again as much as possible.
    // We do not disable the election timer while running an election.
    RETURN_NOT_OK(EnsureFailureDetectorEnabledUnlocked());
    RETURN_NOT_OK(SnoozeFailureDetectorUnlocked());

    LOG_WITH_PREFIX(INFO) << "Starting election with quorum: " << new_quorum.ShortDebugString();

    // Initialize the VoteCounter.
    QuorumState quorum_state = state_->GetActiveQuorumStateUnlocked();
    gscoped_ptr<VoteCounter> counter(new VoteCounter(quorum_state.quorum_size,
                                                     quorum_state.majority_size));
    // Vote for ourselves.
    RETURN_NOT_OK(state_->SetVotedForCurrentTermUnlocked(state_->GetPeerUuid()));
    bool duplicate;
    RETURN_NOT_OK(counter->RegisterVote(state_->GetPeerUuid(), VOTE_GRANTED, &duplicate));
    CHECK(!duplicate) << state_->LogPrefixUnlocked()
                      << "Inexplicable duplicate self-vote for term "
                      << state_->GetCurrentTermUnlocked();

    VoteRequestPB request;
    request.set_candidate_uuid(state_->GetPeerUuid());
    request.set_candidate_term(state_->GetCurrentTermUnlocked());
    request.set_tablet_id(state_->GetOptions().tablet_id);
    request.mutable_candidate_status()->mutable_last_received()->CopyFrom(GetLastOpIdFromLog());

    election.reset(new LeaderElection(election_proxies_, request, counter.Pass(),
                                      Bind(&RaftConsensus::ElectionCallback, this)));
  }

  // Start the election outside the lock.
  election->Run();

  return Status::OK();
}

void RaftConsensus::ReportFailureDetected(const std::string& name, const Status& msg) {
  LOG_WITH_PREFIX_LK(INFO) << "Failure of peer " << name << " detected. Triggering leader election";

  // Start an election.
  Status s = StartElection();
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX_LK(WARNING) << "Failed to trigger leader election: " << s.ToString();
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
  LOG_WITH_PREFIX(INFO) << "Becoming Leader for term " << state_->GetCurrentTermUnlocked();

  // Disable FD while we are leader.
  RETURN_NOT_OK(EnsureFailureDetectorDisabledUnlocked());

  queue_->Init(this,
               state_->GetCommittedOpIdUnlocked(),
               state_->GetCurrentTermUnlocked(),
               state_->GetActiveQuorumStateUnlocked().majority_size);

  CHECK(state_->IsQuorumChangePendingUnlocked());

  // Create the peers so that we're able to replicate messages remotely and locally
  RETURN_NOT_OK(peer_manager_->UpdateQuorum(state_->GetPendingQuorumUnlocked()));

  // If we're becoming leader we need to add the pending operations that are not
  // yet committed to the queue, so that the peers can replicate them, or acknowledge
  // them as replicated.
  vector<ConsensusRound*> rounds;
  CHECK_OK(state_->GetUncommittedPendingOperationsUnlocked(&rounds));
  BOOST_FOREACH(ConsensusRound* round, rounds) {
    CHECK_OK(queue_->AppendOperation(gscoped_ptr<ReplicateMsg>(
        new ReplicateMsg(*DCHECK_NOTNULL(round->replicate_msg())))));
  }

  // Initiate a config change transaction. This is mostly acting as the NO_OP
  // transaction that is sent at the beginning of every term change in raft.
  // TODO implement NO_OP, consensus only, rounds and use those instead.
  gscoped_ptr<ReplicateMsg> replicate(new ReplicateMsg);
  replicate->set_op_type(CHANGE_CONFIG_OP);
  ChangeConfigRequestPB* cc_req = replicate->mutable_change_config_request();
  cc_req->set_tablet_id(state_->GetOptions().tablet_id);
  cc_req->mutable_old_config()->CopyFrom(state_->GetCommittedQuorumUnlocked());
  cc_req->mutable_new_config()->CopyFrom(state_->GetPendingQuorumUnlocked());

  gscoped_ptr<ConsensusRound> round(new ConsensusRound(this, replicate.Pass()));
  RETURN_NOT_OK(AppendNewRoundToQueueUnlocked(round.get()));
  RETURN_NOT_OK(state_->GetReplicaTransactionFactoryUnlocked()->StartReplicaTransaction(
      round.Pass()));

  // TODO we need to make sure that this transaction is the first transaction accepted, right
  // now that is being assured by the fact that the prepare queue is single threaded, but if
  // we ever move to multi-threaded prepare we need to make sure we call replicate on this first.

  return Status::OK();
}

Status RaftConsensus::BecomeReplicaUnlocked() {
  // TODO start the failure detector.
  LOG_WITH_PREFIX(INFO) << "Becoming Follower/Learner";

  // FD should be running while we are a follower.
  RETURN_NOT_OK(EnsureFailureDetectorEnabledUnlocked());

  // Clear the consensus replication queue, evicting all state. We can reuse the
  // queue should we become leader.
  queue_->Clear();

  // TODO: pretty certain there is a race here if the queue currently has an
  // outstanding async log reader request -- the callback would fire on the
  // emptied queue and crash. Perhaps instead we should create a new queue in
  // this case instead of clearing the existing one.
  return Status::OK();
}

Status RaftConsensus::Replicate(ConsensusRound* round) {

  RETURN_NOT_OK(ExecuteHook(PRE_REPLICATE));

  boost::lock_guard<simple_spinlock> lock(update_lock_);
  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForReplicate(&lock, *round->replicate_msg()));
    RETURN_NOT_OK(AppendNewRoundToQueueUnlocked(round));
  }

  peer_manager_->SignalRequest();

  RETURN_NOT_OK(ExecuteHook(POST_REPLICATE));
  return Status::OK();
}

Status RaftConsensus::AppendNewRoundToQueueUnlocked(ConsensusRound* round) {
  state_->NewIdUnlocked(round->replicate_msg()->mutable_id());
  RETURN_NOT_OK(state_->AddPendingOperation(round));

  // the original instance of the replicate msg is owned by the consensus context
  // so we create a copy for the queue.
  gscoped_ptr<ReplicateMsg> queue_msg(new ReplicateMsg(*round->replicate_msg()));
  Status s = queue_->AppendOperation(queue_msg.Pass());

  // Handle Status::ServiceUnavailable(), which means the queue is full.
  if (PREDICT_FALSE(s.IsServiceUnavailable())) {
    gscoped_ptr<OpId> id(round->replicate_msg()->release_id());
    // Rollback the id gen. so that we reuse this id later, when we can
    // actually append to the state machine, i.e. this makes the state
    // machine have continuous ids, for the same term, even if the queue
    // refused to add any more operations.
    state_->CancelPendingOperation(*id);
    LOG_WITH_PREFIX(WARNING) << ": Could not append replicate request "
                 << "to the queue. Queue is Full. "
                 << "Queue metrics: " << queue_->ToString();

    // TODO Possibly evict a dangling peer from the quorum here.
    // TODO count of number of ops failed due to consensus queue overflow.
  }
  RETURN_NOT_OK_PREPEND(s, "Unable to append operation to consensus queue");
  state_->UpdateLastReceivedOpIdUnlocked(round->id());
  return Status::OK();
}

void RaftConsensus::UpdateMajorityReplicated(const OpId& majority_replicated,
                                             OpId* committed_index) {
  ReplicaState::UniqueLock lock;
  Status s = state_->LockForCommit(&lock);
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX_LK(WARNING)
        << "Unable to take state lock to update committed index: "
        << s.ToString();
    return;
  }
  UpdateMajorityReplicatedUnlocked(majority_replicated, committed_index);
}

void RaftConsensus::UpdateMajorityReplicatedUnlocked(const OpId& majority_replicated,
                                                     OpId* committed_index) {
  VLOG_WITH_PREFIX(1) << "Marking majority replicated up to "
      << majority_replicated.ShortDebugString();
  TRACE("Marking majority replicated up to $0", majority_replicated.ShortDebugString());
  Status s = state_->UpdateMajorityReplicated(majority_replicated, committed_index);
  if (PREDICT_FALSE(!s.ok())) {
    string msg = Substitute("Unable to mark committed up to $0: $1",
                            majority_replicated.ShortDebugString(),
                            s.ToString());
    TRACE(msg);
    LOG_WITH_PREFIX(WARNING) << msg;
    return;
  }

  QuorumPeerPB::Role role = state_->GetActiveQuorumStateUnlocked().role;
  if (role == QuorumPeerPB::LEADER || role == QuorumPeerPB::CANDIDATE) {
    // TODO Enable the below to make the leader pro-actively send the commit
    // index to followers. Right now we're keeping this disabled as it causes
    // certain errors to be more probable.
    // SignalRequestToPeers(false);
  }
}

void RaftConsensus::NotifyTermChange(uint64_t term) {
  ReplicaState::UniqueLock lock;
  Status s = state_->LockForCommit(&lock);
  if (PREDICT_FALSE(!s.ok())) {
    // We either have not started running or we have already shut down.
    LOG_WITH_PREFIX_LK(INFO) << "Unable to respond to notification of term change to term " << term
                             << ": " << s.ToString();
    return;
  }
  if (state_->GetCurrentTermUnlocked() < term) {
    CHECK_OK(HandleTermAdvanceUnlocked(term));
  }
}

Status RaftConsensus::Commit(gscoped_ptr<CommitMsg> commit,
                             const StatusCallback& cb) {
  DCHECK(commit->has_commited_op_id());
  OpId committed_op_id = commit->commited_op_id();

  VLOG_WITH_PREFIX_LK(1) << "Appending COMMIT " << committed_op_id;

  RETURN_NOT_OK(ExecuteHook(PRE_COMMIT));
  RETURN_NOT_OK(log_->AsyncAppendCommit(commit.Pass(), cb));

  ReplicaState::UniqueLock lock;
  RETURN_NOT_OK(state_->LockForCommit(&lock));
  state_->UpdateCommittedOpIdUnlocked(committed_op_id);
  RETURN_NOT_OK(ExecuteHook(POST_COMMIT));
  return Status::OK();
}

Status RaftConsensus::Update(const ConsensusRequestPB* request,
                             ConsensusResponsePB* response) {
  RETURN_NOT_OK(ExecuteHook(PRE_UPDATE));
  ConsensusStatusPB* status = response->mutable_status();
  response->set_responder_uuid(state_->GetPeerUuid());

  VLOG_WITH_PREFIX_LK(2) << "Replica received request: " << request->ShortDebugString();

  // see var declaration
  boost::lock_guard<simple_spinlock> lock(update_lock_);
  RETURN_NOT_OK(UpdateReplica(request, response));

  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForRead(&lock));
    TRACE("Updating watermarks");
    response->set_responder_term(state_->GetCurrentTermUnlocked());
    status->mutable_last_received()->CopyFrom(state_->GetLastReceivedOpIdUnlocked());
    status->set_last_committed_idx(state_->GetCommittedOpIdUnlocked().index());
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


Status RaftConsensus::StartReplicaTransactionUnlocked(gscoped_ptr<ReplicateMsg> msg) {
  VLOG_WITH_PREFIX(1) << "Starting transaction: " << msg->id().ShortDebugString();
  gscoped_ptr<ConsensusRound> round(new ConsensusRound(this, msg.Pass()));
  ConsensusRound* round_ptr = round.get();
  RETURN_NOT_OK(state_->GetReplicaTransactionFactoryUnlocked()->
      StartReplicaTransaction(round.Pass()));
  return state_->AddPendingOperation(round_ptr);
}

Status RaftConsensus::SanityCheckAndDedupUpdateRequestUnlocked(
    const ConsensusRequestPB* request,
    ConsensusResponsePB* response,
    vector<const ReplicateMsg*>* replicate_msgs) {

  // Do term checks first:
  if (PREDICT_FALSE(request->caller_term() != state_->GetCurrentTermUnlocked())) {

    // If less, reject.
    if (request->caller_term() < state_->GetCurrentTermUnlocked()) {
      string msg = Substitute("Rejecting Update request from peer $0 for earlier term $1. "
                              "Current term is $2.",
                              request->caller_uuid(),
                              request->caller_term(),
                              state_->GetCurrentTermUnlocked());
      LOG_WITH_PREFIX(INFO) << msg;
      FillConsensusResponseError(response,
                                 ConsensusErrorPB::INVALID_TERM,
                                 Status::IllegalState(msg));
      return Status::OK();
    } else {
      Status s = HandleTermAdvanceUnlocked(request->caller_term());
      if (PREDICT_FALSE(!s.ok())) {
        LOG_WITH_PREFIX(FATAL) << "Unable to set current term: " << s.ToString();
      }
    }
  }

  TRACE("Updating replica for $0 ops", request->ops_size());

  // The id of the operation immediately before the first one we'll prepare/replicate.
  OpId preceding_id(request->preceding_id());

  // filter out any ops which have already been handled by a previous call
  // to UpdateReplica()
  BOOST_FOREACH(const ReplicateMsg& msg, request->ops()) {
    if (OpIdCompare(msg.id(), state_->GetLastReceivedOpIdUnlocked()) <= 0) {
      VLOG_WITH_PREFIX(2) << "Skipping op id " << msg.id().ShortDebugString()
          << " (already replicated/committed)";
      preceding_id.CopyFrom(msg.id());
      continue;
    }
    replicate_msgs->push_back(&const_cast<ReplicateMsg&>(msg));
  }

  // Enforce the log matching property.
  if (!OpIdEquals(state_->GetLastReplicatedOpIdUnlocked(), preceding_id)) {
    string error_msg = Substitute(
        "Log matching property violated."
        " Last replicated by replica: $0. Preceding OpId from leader: $1.",
        state_->GetLastReplicatedOpIdUnlocked().ShortDebugString(),
        preceding_id.ShortDebugString());
    FillConsensusResponseError(response,
                               ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH,
                               Status::IllegalState(error_msg));
    LOG_WITH_PREFIX(INFO) << "Refusing update from remote peer "
                          << request->caller_uuid() << ": " << error_msg;
    return Status::OK();
  }
  return Status::OK();
}

Status RaftConsensus::UpdateReplica(const ConsensusRequestPB* request,
                                    ConsensusResponsePB* response) {
  Synchronizer log_synchronizer;
  vector<const ReplicateMsg*> replicate_msgs;


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

    // 0 - Split/Check/Dedup
    RETURN_NOT_OK(SanityCheckAndDedupUpdateRequestUnlocked(request,
                                                           response,
                                                           &replicate_msgs));

    if (response->status().has_error()) {
      return Status::OK();
    }

    // Snooze the failure detector as soon as we decide to accept the message.
    // We are guaranteed to be acting as a FOLLOWER at this point by the above
    // sanity check.
    RETURN_NOT_OK(SnoozeFailureDetectorUnlocked());

    // 1 - Enqueue the prepares

    TRACE("Triggering prepare for $0 ops", replicate_msgs.size());
    Status prepare_status;
    for (int i = 0; i < replicate_msgs.size(); i++) {
      gscoped_ptr<ReplicateMsg> msg_copy(new ReplicateMsg(*replicate_msgs[i]));
      prepare_status = StartReplicaTransactionUnlocked(msg_copy.Pass());
      if (PREDICT_FALSE(!prepare_status.ok())) {
        LOG_WITH_PREFIX(WARNING) << "Failed to prepare operation: "
            << replicate_msgs[i]->id().ShortDebugString() << " Status: "
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
      last_enqueued_prepare = replicate_msgs[successfully_triggered_prepares - 1]->id();
    }

    // 2 - Enqueue the writes.
    // Now that we've triggered the prepares enqueue the operations to be written
    // to the WAL.
    if (PREDICT_TRUE(successfully_triggered_prepares > 0)) {
      // Trigger the log append asap, if fsync() is on this might take a while
      // and we can't reply until this is done.
      //
      // Since we've prepared, we need to be able to append (or we risk trying to apply
      // later something that wasn't logged). We crash if we can't.
      CHECK_OK(log_->AsyncAppendReplicates(&replicate_msgs[0],
                                           successfully_triggered_prepares,
                                           log_synchronizer.AsStatusCallback()));
    }

    // 3 - Mark transactions as committed

    // Choose the last operation to be applied. This will either be 'committed_index', if
    // no prepare enqueuing failed, or the minimum between 'committed_index' and the id of
    // the last successfully enqueued prepare, if some prepare failed to enqueue.
    OpId dont_apply_after = request->committed_index();
    if (CopyIfOpIdLessThan(last_enqueued_prepare, &dont_apply_after)) {
      VLOG_WITH_PREFIX(2) << "Received commit index "
          << request->committed_index().ShortDebugString() << " from the leader but only"
          << " marked up to " << last_enqueued_prepare.ShortDebugString() << " as committed.";
    }

    VLOG_WITH_PREFIX(1) << "Marking committed up to " << dont_apply_after.ShortDebugString();
    TRACE(Substitute("Marking committed up to $0", dont_apply_after.ShortDebugString()));
    CHECK_OK(state_->AdvanceCommittedIndex(dont_apply_after));

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
    VLOG_WITH_PREFIX_LK(1) << "Replica updated."
        << state_->ToString() << " Request: " << request->ShortDebugString();
  }

  TRACE("UpdateReplicas() finished");
  return Status::OK();
}

void RaftConsensus::FillConsensusResponseError(ConsensusResponsePB* response,
                                               ConsensusErrorPB::Code error_code,
                                               const Status& status) {
  ConsensusErrorPB* error = response->mutable_status()->mutable_error();
  error->set_code(error_code);
  StatusToPB(status, error->mutable_status());
}

Status RaftConsensus::RequestVote(const VoteRequestPB* request, VoteResponsePB* response) {
  // We must acquire the update lock in order to ensure that this vote action
  // takes place between requests.
  // Lock ordering: The update lock must be acquired before the ReplicaState lock.
  lock_guard<simple_spinlock> update_guard(&update_lock_);

  // Acquire the replica state lock so we can read / modify the consensus state.
  ReplicaState::UniqueLock state_guard;
  RETURN_NOT_OK(state_->LockForConfigChange(&state_guard));

  response->set_responder_uuid(state_->GetPeerUuid());

  QuorumPeerPB::Role role = GetRoleInQuorum(request->candidate_uuid(),
                                            state_->GetCommittedQuorumUnlocked());
  // Candidate is not a member of the quorum.
  if (role == QuorumPeerPB::NON_PARTICIPANT) {
    return RequestVoteRespondNotInQuorum(request, response);
  }

  // Candidate is running behind.
  if (request->candidate_term() < state_->GetCurrentTermUnlocked()) {
    return RequestVoteRespondInvalidTerm(request, response);
  }

  // We already voted this term.
  if (request->candidate_term() == state_->GetCurrentTermUnlocked() &&
      state_->HasVotedCurrentTermUnlocked()) {

    // Already voted for the same candidate in the current term.
    if (state_->GetVotedForCurrentTermUnlocked() == request->candidate_uuid()) {
      return RequestVoteRespondVoteAlreadyGranted(request, response);
    }

    // Voted for someone else in current term.
    return RequestVoteRespondAlreadyVotedForOther(request, response);
  }

  // The term advanced.
  if (request->candidate_term() > state_->GetCurrentTermUnlocked()) {
    RETURN_NOT_OK(HandleTermAdvanceUnlocked(request->candidate_term()));
  }

  // Candidate must have last-logged OpId at least as large as our own to get
  // our vote.
  OpId local_last_logged_opid = GetLastOpIdFromLog();
  if (OpIdLessThan(request->candidate_status().last_received(), local_last_logged_opid)) {
    return RequestVoteRespondLastOpIdTooOld(local_last_logged_opid, request, response);
  }

  // Passed all our checks. Vote granted.
  return RequestVoteRespondVoteGranted(request, response);
}

void RaftConsensus::Shutdown() {
  CHECK_OK(ExecuteHook(PRE_SHUTDOWN));

  {
    ReplicaState::UniqueLock lock;
    CHECK_OK(state_->LockForShutdown(&lock));
    if (state_->state() == ReplicaState::kShutDown) {
      // We have already shut down.
      return;
    }
    LOG_WITH_PREFIX(INFO) << "Raft consensus shutting down.";
  }

  // Close the peer manager.
  peer_manager_->Close();

  // We must close the queue after we close the peers.
  {
    ReplicaState::UniqueLock lock;
    CHECK_OK(state_->LockForShutdown(&lock));
    queue_->Close();
  }

  CHECK_OK(state_->CancelPendingTransactions());
  CHECK_OK(state_->WaitForOustandingApplies());

  {
    ReplicaState::UniqueLock lock;
    CHECK_OK(state_->LockForShutdown(&lock));
    if (state_->state() != ReplicaState::kShutDown) {
      LOG_WITH_PREFIX(INFO) << "Raft consensus Shutdown!";
      CHECK_OK(state_->ShutdownUnlocked());
    }
  }
  CHECK_OK(ExecuteHook(POST_SHUTDOWN));
}

metadata::QuorumPeerPB::Role RaftConsensus::GetActiveRole() const {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  const QuorumState& state = state_->GetActiveQuorumStateUnlocked();
  return state.role;
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

std::string RaftConsensus::GetRequestVoteLogPrefixUnlocked() const {
  return state_->LogPrefixUnlocked() + "Leader election vote request";
}

void RaftConsensus::FillVoteResponseVoteGranted(VoteResponsePB* response) {
  response->set_responder_term(state_->GetCurrentTermUnlocked());
  response->set_vote_granted(true);
}

void RaftConsensus::FillVoteResponseVoteDenied(ConsensusErrorPB::Code error_code,
                                               VoteResponsePB* response) {
  response->set_responder_term(state_->GetCurrentTermUnlocked());
  response->set_vote_granted(false);
  response->mutable_consensus_error()->set_code(error_code);
}

Status RaftConsensus::RequestVoteRespondNotInQuorum(const VoteRequestPB* request,
                                                    VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::NOT_IN_QUORUM, response);
  string msg = Substitute("$0: Not considering vote for candidate $1 in term $2 due to "
                          "candidate not being a member of the quorum in current term $3. "
                          "Quorum: $4",
                          GetRequestVoteLogPrefixUnlocked(),
                          request->candidate_uuid(),
                          request->candidate_term(),
                          state_->GetCurrentTermUnlocked(),
                          state_->GetCommittedQuorumUnlocked().ShortDebugString());
  LOG(INFO) << msg;
  StatusToPB(Status::InvalidArgument(msg), response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondInvalidTerm(const VoteRequestPB* request,
                                                    VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::INVALID_TERM, response);
  string msg = Substitute("$0: Denying vote to candidate $1 for earlier term $2. "
                          "Current term is $3.",
                          GetRequestVoteLogPrefixUnlocked(),
                          request->candidate_uuid(),
                          request->candidate_term(),
                          state_->GetCurrentTermUnlocked());
  LOG(INFO) << msg;
  StatusToPB(Status::InvalidArgument(msg), response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondVoteAlreadyGranted(const VoteRequestPB* request,
                                                           VoteResponsePB* response) {
  FillVoteResponseVoteGranted(response);
  LOG(INFO) << Substitute("$0: Already granted yes vote for candidate $1 in term $2. "
                          "Re-sending same reply.",
                          GetRequestVoteLogPrefixUnlocked(),
                          request->candidate_uuid(),
                          request->candidate_term());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondAlreadyVotedForOther(const VoteRequestPB* request,
                                                             VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::ALREADY_VOTED, response);
  string msg = Substitute("$0: Denying vote to candidate $1 in current term $2: "
                          "Already voted for candidate $3 in this term.",
                          GetRequestVoteLogPrefixUnlocked(),
                          request->candidate_uuid(),
                          state_->GetCurrentTermUnlocked(),
                          state_->GetVotedForCurrentTermUnlocked());
  LOG(INFO) << msg;
  StatusToPB(Status::InvalidArgument(msg), response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondLastOpIdTooOld(const OpId& local_last_logged_opid,
                                                       const VoteRequestPB* request,
                                                       VoteResponsePB* response) {
  FillVoteResponseVoteDenied(ConsensusErrorPB::LAST_OPID_TOO_OLD, response);
  string msg = Substitute("$0: Denying vote to candidate $1 for term $2 because "
                          "replica has last-logged OpId of $3, which is greater than that of the "
                          "candidate, which has last-logged OpId of $4.",
                          GetRequestVoteLogPrefixUnlocked(),
                          request->candidate_uuid(),
                          request->candidate_term(),
                          local_last_logged_opid.ShortDebugString(),
                          request->candidate_status().last_received().ShortDebugString());
  LOG(INFO) << msg;
  StatusToPB(Status::InvalidArgument(msg), response->mutable_consensus_error()->mutable_status());
  return Status::OK();
}

Status RaftConsensus::RequestVoteRespondVoteGranted(const VoteRequestPB* request,
                                                    VoteResponsePB* response) {
  // Give peer time to become leader if we vote for them.
  RETURN_NOT_OK(SnoozeFailureDetectorUnlocked());

  FillVoteResponseVoteGranted(response);

  // Persist our vote.
  RETURN_NOT_OK(state_->SetVotedForCurrentTermUnlocked(request->candidate_uuid()));

  LOG(INFO) << Substitute("$0: Granting yes vote for candidate $1 in term $2.",
                          GetRequestVoteLogPrefixUnlocked(),
                          request->candidate_uuid(),
                          state_->GetCurrentTermUnlocked());
  return Status::OK();
}

QuorumPeerPB::Role RaftConsensus::role() const {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  return GetRoleInQuorum(state_->GetPeerUuid(), state_->GetCommittedQuorumUnlocked());
}

string RaftConsensus::peer_uuid() const {
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
  out << "<pre>" << EscapeForHtmlToString(queue_->ToString()) << "</pre>" << std::endl;

  // Dump the queues on a leader.
  QuorumPeerPB::Role role;
  {
    ReplicaState::UniqueLock lock;
    CHECK_OK(state_->LockForRead(&lock));
    role = state_->GetActiveQuorumStateUnlocked().role;
  }
  if (role == QuorumPeerPB::LEADER) {
    out << "<h2>Queue overview</h2>" << std::endl;
    out << "<pre>" << EscapeForHtmlToString(queue_->ToString()) << "</pre>" << std::endl;
    out << "<hr/>" << std::endl;
    out << "<h2>Queue details</h2>" << std::endl;
    queue_->DumpToHtml(out);
  }
}

ReplicaState* RaftConsensus::GetReplicaStateForTests() {
  return state_.get();
}

void RaftConsensus::ElectionCallback(const ElectionResult& result) {
  if (result.decision == VOTE_DENIED) {
    LOG_WITH_PREFIX(INFO) << "Leader election lost for term " << result.election_term
                          << ". Reason: "
                          << (!result.message.empty() ? result.message : "None given");
    return;
  }

  ReplicaState::UniqueLock lock;
  Status s = state_->LockForConfigChange(&lock);
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX(INFO) << "Received election callback for term "
                          << result.election_term << " while not running: "
                          << s.ToString();
    return;
  }

  if (result.election_term != state_->GetCurrentTermUnlocked()
      || state_->GetActiveQuorumStateUnlocked().role != QuorumPeerPB::CANDIDATE) {
    LOG_WITH_PREFIX(INFO) << "Leader election decision for defunct term "
                          << result.election_term << ": "
                          << (result.decision == VOTE_GRANTED ? "won" : "lost");
    return;
  }

  // Snooze to avoid the election timer firing again as much as possible.
  CHECK_OK(SnoozeFailureDetectorUnlocked());

  LOG_WITH_PREFIX(INFO) << "Leader election won for term " << result.election_term;

  // Sanity check.
  QuorumState quorum_state = state_->GetActiveQuorumStateUnlocked();
  CHECK_EQ(QuorumPeerPB::CANDIDATE, quorum_state.role);
  CHECK(state_->IsQuorumChangePendingUnlocked());

  // Convert role to LEADER.
  QuorumPB new_quorum;
  CHECK_OK(GivePeerRoleInQuorum(state_->GetPeerUuid(),
                                QuorumPeerPB::LEADER,
                                state_->GetPendingQuorumUnlocked(),
                                &new_quorum));
  new_quorum.set_seqno(state_->GetCommittedQuorumUnlocked().seqno() + 1);
  CHECK_OK(VerifyQuorum(new_quorum));
  CHECK_OK(state_->SetPendingQuorumUnlocked(new_quorum));

  CHECK_OK(BecomeLeaderUnlocked());
}

Status RaftConsensus::GetLastReceivedOpId(OpId* id) {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForRead(&lock));
  DCHECK_NOTNULL(id)->CopyFrom(state_->GetLastReceivedOpIdUnlocked());
  return Status::OK();
}

Status RaftConsensus::EnsureFailureDetectorEnabledUnlocked() {
  if (failure_detector_->IsTracking(kTimerId)) {
    return Status::OK();
  }
  return failure_detector_->Track(kTimerId,
                                  MonoTime::Now(MonoTime::FINE),
                                  // Unretained to avoid a circular ref.
                                  Bind(&RaftConsensus::ReportFailureDetected, Unretained(this)));
}

Status RaftConsensus::EnsureFailureDetectorDisabledUnlocked() {
  if (!failure_detector_->IsTracking(kTimerId)) {
    return Status::OK();
  }
  return failure_detector_->UnTrack(kTimerId);
}

Status RaftConsensus::SnoozeFailureDetectorUnlocked() {
  if (PREDICT_FALSE(!FLAGS_enable_leader_failure_detection)) {
    return Status::OK();
  }
  return failure_detector_->MessageFrom(kTimerId, MonoTime::Now(MonoTime::FINE));
}

Status RaftConsensus::HandleTermAdvanceUnlocked(ConsensusTerm new_term) {
  DCHECK_GT(new_term, state_->GetCurrentTermUnlocked());
  const QuorumState& state = state_->GetActiveQuorumStateUnlocked();
  if (state.role == QuorumPeerPB::LEADER) {
    LOG_WITH_PREFIX(INFO) << "Stepping down as leader of term " << state_->GetCurrentTermUnlocked();
    RETURN_NOT_OK(BecomeReplicaUnlocked());
  }

  // Demote leader in quorum.
  if (!state.leader_uuid.empty()) {
    QuorumPB quorum;
    RETURN_NOT_OK(GivePeerRoleInQuorum(state.leader_uuid,
                                       QuorumPeerPB::FOLLOWER,
                                       state_->GetCommittedQuorumUnlocked(),
                                       &quorum));
    state_->SetPendingQuorumUnlocked(quorum);
  }

  LOG_WITH_PREFIX(INFO) << "Advancing to term " << new_term;
  RETURN_NOT_OK(state_->SetCurrentTermUnlocked(new_term));
  return Status::OK();
}

}  // namespace consensus
}  // namespace kudu
