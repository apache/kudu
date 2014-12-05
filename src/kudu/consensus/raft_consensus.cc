// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/raft_consensus.h"

#include <algorithm>
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
#include "kudu/util/logging.h"
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

DEFINE_int32(leader_failure_exp_backoff_max_delta_ms, 20 * 1000,
             "Maximum time to sleep in between leader election retries, in addition to the "
             "regular timeout. When leader election fails the interval in between retries "
             "increases exponentially, up to this value.");

// TODO enable this by default once consensus is ready.
DEFINE_bool(enable_leader_failure_detection, false,
            "Whether to enable failure detection of tablet leaders. If enabled, attempts will be "
            "made to fail over to a follower when the leader is detected to have failed.");

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
  gscoped_ptr<PeerMessageQueue> queue(new PeerMessageQueue(metric_ctx,
                                                           log,
                                                           peer_uuid,
                                                           options.tablet_id));

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
      peer_manager_(peer_manager.Pass()),
      queue_(queue.Pass()),
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

    if (PREDICT_TRUE(FLAGS_enable_leader_failure_detection)) {
      // Clean up quorum state from previous runs, so everyone is a follower.
      // We do this here so that the logging seems sane while reloading the
      // set of pending transactions.
      SetAllQuorumVotersToFollower(state_->GetCommittedQuorumUnlocked(), &initial_quorum);
      state_->SetPendingQuorumUnlocked(initial_quorum);
      RETURN_NOT_OK_PREPEND(VerifyQuorum(initial_quorum),
                            "Invalid quorum state on RaftConsensus::Start()");
    } else {
      initial_quorum.CopyFrom(state_->GetCommittedQuorumUnlocked());
      RETURN_NOT_OK_PREPEND(VerifyQuorumAndCheckThatNoChangeIsPendingUnlocked(initial_quorum),
                            "Invalid quorum state on RaftConsensus::Start()");
    }

    RETURN_NOT_OK_PREPEND(state_->StartUnlocked(info.last_id),
                          "Unable to start RAFT ReplicaState");

    LOG_WITH_PREFIX(INFO) << "Replica starting. Triggering " << info.orphaned_replicates.size()
        << " pending transactions.";
    BOOST_FOREACH(ReplicateMsg* replicate, info.orphaned_replicates) {
      RETURN_NOT_OK(StartReplicaTransactionUnlocked(
          make_scoped_refptr_replicate(new ReplicateMsg(*replicate))));
    }

    state_->AdvanceCommittedIndexUnlocked(info.last_committed_id);

    queue_->Init(state_->GetLastReceivedOpIdUnlocked());
  }

  if (PREDICT_TRUE(FLAGS_enable_leader_failure_detection)) {
    // Leader election path.
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForConfigChange(&lock));

    LOG_WITH_PREFIX(INFO) << "Initializing failure detector for fast election at startup";
    RETURN_NOT_OK(EnsureFailureDetectorEnabledUnlocked());

    // If this is the first term expire the FD immediately so that we have a fast first
    // election, otherwise we just let the timer expire normally.
    if (state_->GetCurrentTermUnlocked() == 1) {
      // Initialize the failure detector timeout to some time in the past so that
      // the next time the failure detector monitor runs it triggers an election
      // (unless someone else requested a vote from us first, which resets the
      // election timer). We do it this way instead of immediately running an
      // election to get a higher likelihood of enough servers being available
      // when the first one attempts an election to avoid multiple election
      // cycles on startup, while keeping that "waiting period" random.
      RETURN_NOT_OK(ExpireFailureDetectorUnlocked());
    }

    // Now assume "follower" duties.
    RETURN_NOT_OK(BecomeReplicaUnlocked());
  } else {
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
    RETURN_NOT_OK(SnoozeFailureDetectorUnlocked(LeaderElectionExpBackoffDeltaUnlocked()));

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

    election.reset(new LeaderElection(state_->GetActiveQuorumUnlocked(),
                                      peer_proxy_factory_.get(),
                                      request, counter.Pass(),
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
  LOG_WITH_PREFIX(INFO) << "Becoming Leader. State: " << state_->ToStringUnlocked();

  // Disable FD while we are leader.
  RETURN_NOT_OK(EnsureFailureDetectorDisabledUnlocked());

  CHECK(state_->IsQuorumChangePendingUnlocked());

  queue_->RegisterObserver(this);
  queue_->SetLeaderMode(state_->GetCommittedOpIdUnlocked(),
                        state_->GetCurrentTermUnlocked(),
                        state_->GetActiveQuorumStateUnlocked().majority_size);

  // Create the peers so that we're able to replicate messages remotely and locally
  RETURN_NOT_OK(peer_manager_->UpdateQuorum(state_->GetPendingQuorumUnlocked()));

  // Initiate a config change transaction. This is mostly acting as the NO_OP
  // transaction that is sent at the beginning of every term change in raft.
  // TODO implement NO_OP, consensus only, rounds and use those instead.
  ReplicateMsg* replicate = new ReplicateMsg;
  replicate->set_op_type(CHANGE_CONFIG_OP);
  ChangeConfigRequestPB* cc_req = replicate->mutable_change_config_request();
  cc_req->set_tablet_id(state_->GetOptions().tablet_id);
  cc_req->mutable_old_config()->CopyFrom(state_->GetCommittedQuorumUnlocked());
  cc_req->mutable_new_config()->CopyFrom(state_->GetPendingQuorumUnlocked());

  gscoped_ptr<ConsensusRound> round(
      new ConsensusRound(this,
                         make_scoped_refptr(new RefCountedReplicate(replicate))));
  RETURN_NOT_OK(AppendNewRoundToQueueUnlocked(round.get()));
  RETURN_NOT_OK(state_->GetReplicaTransactionFactoryUnlocked()->StartReplicaTransaction(
      round.Pass()));

  return Status::OK();
}

Status RaftConsensus::BecomeReplicaUnlocked() {
  LOG_WITH_PREFIX(INFO) << "Becoming Follower/Learner. State: " << state_->ToStringUnlocked();

  // FD should be running while we are a follower.
  RETURN_NOT_OK(EnsureFailureDetectorEnabledUnlocked());

  queue_->UnRegisterObserver(this);
  // Deregister ourselves from the queue. We don't care what get's replicated, since
  // we're stepping down.
  queue_->SetNonLeaderMode();

  peer_manager_->Close();
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

  Status s = queue_->AppendOperation(round->replicate_scoped_refptr());

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
  Status s = state_->LockForMajorityReplicatedIndexUpdate(&lock);
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
  Status s = state_->UpdateMajorityReplicatedUnlocked(majority_replicated, committed_index);
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
  CHECK_OK(state_->LockForConfigChange(&lock));
  WARN_NOT_OK(HandleTermAdvanceUnlocked(term), "Couldn't advance consensus term.");
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

  if (PREDICT_FALSE(VLOG_IS_ON(1))) {
    if (request->ops_size() == 0) {
      VLOG(1) << state_->LogPrefix() << "Replica replied to status only request. Replica: "
              << state_->ToString() << " Status: " << status->ShortDebugString();
    }
  }
  RETURN_NOT_OK(ExecuteHook(POST_UPDATE));
  return Status::OK();
}


Status RaftConsensus::StartReplicaTransactionUnlocked(const ReplicateRefPtr& msg) {
  VLOG_WITH_PREFIX(1) << "Starting transaction: " << msg->get()->id().ShortDebugString();
  gscoped_ptr<ConsensusRound> round(new ConsensusRound(this, msg));
  ConsensusRound* round_ptr = round.get();
  RETURN_NOT_OK(state_->GetReplicaTransactionFactoryUnlocked()->
      StartReplicaTransaction(round.Pass()));
  return state_->AddPendingOperation(round_ptr);
}

void RaftConsensus::DeduplicateLeaderRequestUnlocked(const ConsensusRequestPB* rpc_req,
                                                     LeaderRequest* deduplicated_req) {
  const OpId& last_committed = state_->GetCommittedOpIdUnlocked();

  // The leader's preceding id.
  deduplicated_req->preceding_opid = &rpc_req->preceding_id();

  int64_t dedup_up_to_index = state_->GetLastReceivedOpIdUnlocked().index();

  ConsensusRequestPB* mutable_req = const_cast<ConsensusRequestPB*>(rpc_req);

  int first_idx_kept = -1;
  int last_idx_kept = -1;

  // In this loop we discard duplicates and advance the leader's preceding id
  // accordingly.
  for (int i = 0; i < rpc_req->ops_size(); i++) {
    ReplicateMsg* leader_msg = mutable_req->mutable_ops(i);

    if (leader_msg->id().index() <= last_committed.index()) {
      VLOG_WITH_PREFIX(2) << "Skipping op id " << leader_msg->id().ShortDebugString()
          << " (already committed)";
      deduplicated_req->preceding_opid = &leader_msg->id();
      continue;
    }

    if (leader_msg->id().index() <= dedup_up_to_index) {
      // If the index is uncommitted and below our match index, then it must be in the
      // pendings set.
      ConsensusRound* round = DCHECK_NOTNULL(state_->GetPendingOpByIndexOrNullUnlocked(
          leader_msg->id().index()));

      // If the OpIds match, i.e. if they have the same term and id, then this is just
      // duplicate, we skip...
      if (OpIdEquals(round->replicate_msg()->id(), leader_msg->id())) {
        VLOG_WITH_PREFIX(2) << "Skipping op id " << leader_msg->id().ShortDebugString()
            << " (already replicated)";
        deduplicated_req->preceding_opid = &leader_msg->id();
        continue;
      }

      // ... otherwise we must adjust our match index, i.e. all messages from now on
      // are "new"
      dedup_up_to_index = leader_msg->id().index();
    }

    if (first_idx_kept == - 1) {
      first_idx_kept = i;
    }
    deduplicated_req->messages.push_back(make_scoped_refptr_replicate(leader_msg));
    last_idx_kept = i;
  }

  // We take ownership of the deduped ops.
  if (!deduplicated_req->messages.empty()) {
    mutable_req->mutable_ops()->ExtractSubrange(first_idx_kept,
                                                last_idx_kept - first_idx_kept + 1,
                                                NULL);
  }
}

Status RaftConsensus::HandleLeaderRequestTermUnlocked(const ConsensusRequestPB* request,
                                                      ConsensusResponsePB* response) {
  // Do term checks first:
  if (PREDICT_FALSE(request->caller_term() != state_->GetCurrentTermUnlocked())) {

    // If less, reject.
    if (request->caller_term() < state_->GetCurrentTermUnlocked()) {
      string msg = Substitute("Rejecting Update request from peer $0 for earlier term $1. "
                              "Current term is $2. Ops: $3",
                              request->caller_uuid(),
                              request->caller_term(),
                              state_->GetCurrentTermUnlocked(),
                              OpsRangeString(*request));
      LOG_WITH_PREFIX(INFO) << msg;
      FillConsensusResponseError(response,
                                 ConsensusErrorPB::INVALID_TERM,
                                 Status::IllegalState(msg));
      return Status::OK();
    } else {
      RETURN_NOT_OK(HandleTermAdvanceUnlocked(request->caller_term()));
    }
  }
  return Status::OK();
}

Status RaftConsensus::EnforceLogMatchingPropertyMatchesUnlocked(const LeaderRequest& req,
                                                                ConsensusResponsePB* response) {

  bool term_mismatch;
  if (state_->IsOpCommittedOrPending(*req.preceding_opid, &term_mismatch)) {
    return Status::OK();
  }

  string error_msg = Substitute("Log matching property violated."
      " Preceding OpId in replica: $0. Preceding OpId from leader: $1.",
      state_->GetLastReceivedOpIdUnlocked().ShortDebugString(),
      req.preceding_opid->ShortDebugString());

  FillConsensusResponseError(response,
                             ConsensusErrorPB::PRECEDING_ENTRY_DIDNT_MATCH,
                             Status::IllegalState(error_msg));

  LOG_WITH_PREFIX(INFO) << "Refusing update from remote peer "
                        << req.leader_uuid << ": " << error_msg;

  // If the terms mismatch we abort down to the index before the leader's preceding,
  // since we know that is the last opid that has a chance of not being overwritten.
  // Aborting preemptively here avoids us reporting a last received index that is
  // possibly higher than the leader's causing an avoidable cache miss on the leader's
  // queue.
  if (term_mismatch) {
    return state_->AbortOpsAfterUnlocked(req.preceding_opid->index() - 1);
  }

  return Status::OK();
}

Status RaftConsensus::CheckLeaderRequestUnlocked(const ConsensusRequestPB* request,
                                                 ConsensusResponsePB* response,
                                                 LeaderRequest* deduped_req) {

  RETURN_NOT_OK(HandleLeaderRequestTermUnlocked(request, response));

  if (response->status().has_error()) {
    return Status::OK();
  }

  DeduplicateLeaderRequestUnlocked(request, deduped_req);

  RETURN_NOT_OK(EnforceLogMatchingPropertyMatchesUnlocked(*deduped_req, response));

  if (response->status().has_error()) {
    return Status::OK();
  }

  // If the first of the messages to apply is not in our log, either it follows the last
  // received message or it replaces some in-flight.
  if (!deduped_req->messages.empty()) {

    bool term_mismatch;
    CHECK(!state_->IsOpCommittedOrPending(deduped_req->messages[0]->get()->id(), &term_mismatch));

    // If the index is in our log but the terms are not the same abort down to the leader's
    // preceding id.
    if (term_mismatch) {
      RETURN_NOT_OK(state_->AbortOpsAfterUnlocked(deduped_req->preceding_opid->index()));
    }
  }
  return Status::OK();
}

Status RaftConsensus::UpdateReplica(const ConsensusRequestPB* request,
                                    ConsensusResponsePB* response) {
  Synchronizer log_synchronizer;
  StatusCallback sync_status_cb = log_synchronizer.AsStatusCallback();


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
  TRACE("Updating replica for $0 ops", request->ops_size());

  OpId last_enqueued_prepare;
  // The deduplicated request.
  LeaderRequest deduped_req;

  {
    ReplicaState::UniqueLock lock;
    RETURN_NOT_OK(state_->LockForUpdate(&lock));

    deduped_req.leader_uuid = request->caller_uuid();

    RETURN_NOT_OK(CheckLeaderRequestUnlocked(request, response, &deduped_req));

    if (response->status().has_error()) {
      // We had an error, like an invalid term, we still fill the response.
      FillConsensusResponseOKUnlocked(response);
      return Status::OK();
    }

    // Snooze the failure detector as soon as we decide to accept the message.
    // We are guaranteed to be acting as a FOLLOWER at this point by the above
    // sanity check.
    RETURN_NOT_OK(SnoozeFailureDetectorUnlocked());

    // 1 - Enqueue the prepares

    TRACE("Triggering prepare for $0 ops", deduped_req.messages.size());

    Status prepare_status;
    std::vector<ReplicateRefPtr>::iterator iter = deduped_req.messages.begin();

    while (iter != deduped_req.messages.end()) {
      prepare_status = StartReplicaTransactionUnlocked(*iter);
      if (PREDICT_FALSE(!prepare_status.ok())) {
        break;
      }
      ++iter;
    }

    // If we stopped before reaching the end we failed to prepare some message(s) and need
    // to perform cleanup, namely trimming deduped_req.messages to only contain the messages
    // that were actually prepared, and deleting the other ones since we've taken ownership
    // when we first deduped.
    if (iter != deduped_req.messages.end()) {
      while (iter != deduped_req.messages.end()) {
        ReplicateRefPtr msg = (*iter);
        iter = deduped_req.messages.erase(iter);
        LOG_WITH_PREFIX(WARNING) << "Could not prepare transaction for op: " << msg->get()->id()
            << ". Status: " << prepare_status.ToString();
      }
    }

    last_enqueued_prepare.CopyFrom(state_->GetLastReceivedOpIdUnlocked());

    // 2 - Enqueue the writes.
    // Now that we've triggered the prepares enqueue the operations to be written
    // to the WAL.
    if (PREDICT_TRUE(deduped_req.messages.size() > 0)) {
      last_enqueued_prepare = deduped_req.messages.back()->get()->id();

      // Trigger the log append asap, if fsync() is on this might take a while
      // and we can't reply until this is done.
      //
      // Since we've prepared, we need to be able to append (or we risk trying to apply
      // later something that wasn't logged). We crash if we can't.
      CHECK_OK(queue_->AppendOperations(deduped_req.messages, sync_status_cb));
    }

    // 3 - Mark transactions as committed

    // Choose the last operation to be applied. This will either be 'committed_index', if
    // no prepare enqueuing failed, or the minimum between 'committed_index' and the id of
    // the last successfully enqueued prepare, if some prepare failed to enqueue.
    OpId apply_up_to = request->committed_index();
    if (last_enqueued_prepare.index() < apply_up_to.index()) {
      apply_up_to = last_enqueued_prepare;
      VLOG_WITH_PREFIX(2) << "Received commit index "
          << request->committed_index().ShortDebugString() << " from the leader but only"
          << " marked up to " << last_enqueued_prepare.ShortDebugString() << " as committed.";
    }

    VLOG_WITH_PREFIX(1) << "Marking committed up to " << apply_up_to.ShortDebugString();
    TRACE(Substitute("Marking committed up to $0", apply_up_to.ShortDebugString()));
    CHECK_OK(state_->AdvanceCommittedIndexUnlocked(apply_up_to));

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

    // Fill the response with the current state. We will not mutate anymore state until
    // we actually reply to the leader, we'll just wait for the messages to be durable.
    FillConsensusResponseOKUnlocked(response);
  }
  // Release the lock while we wait for the log append to finish so that commits can go through.
  // We'll re-acquire it before we update the state again.

  // Update the last replicated op id
  if (deduped_req.messages.size() > 0) {

    // 4 - We wait for the writes to be durable.

    // Note that this is safe because dist consensus now only supports a single outstanding
    // request at a time and this way we can allow commits to proceed while we wait.
    TRACE("Waiting on the replicates to finish logging");
    RETURN_NOT_OK(log_synchronizer.Wait());
    TRACE("finished");
  }

  if (PREDICT_FALSE(VLOG_IS_ON(2))) {
    VLOG_WITH_PREFIX_LK(2) << "Replica updated."
        << state_->ToString() << " Request: " << request->ShortDebugString();
  }

  TRACE("UpdateReplicas() finished");
  return Status::OK();
}

void RaftConsensus::FillConsensusResponseOKUnlocked(ConsensusResponsePB* response) {
  TRACE("Filling leader response.");
  response->set_responder_term(state_->GetCurrentTermUnlocked());
  response->mutable_status()->mutable_last_received()->CopyFrom(
      state_->GetLastReceivedOpIdUnlocked());
  response->mutable_status()->set_last_committed_idx(
      state_->GetCommittedOpIdUnlocked().index());
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
    RETURN_NOT_OK_PREPEND(HandleTermAdvanceUnlocked(request->candidate_term()),
        Substitute("Could not step down in RequestVote. Current term: $0, candidate term: $1",
                   state_->GetCurrentTermUnlocked(), request->candidate_term()));
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
  queue_->Close();

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

Status RaftConsensus::AdvanceTermForTests(int64_t new_term) {
  ReplicaState::UniqueLock lock;
  CHECK_OK(state_->LockForConfigChange(&lock));
  return HandleTermAdvanceUnlocked(new_term);
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
  RETURN_NOT_OK(SnoozeFailureDetectorUnlocked(LeaderElectionExpBackoffDeltaUnlocked()));

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

std::string RaftConsensus::LogPrefixUnlocked() {
  return state_->LogPrefixUnlocked();
}

std::string RaftConsensus::LogPrefix() {
  return state_->LogPrefix();
}

string RaftConsensus::peer_uuid() const {
  return state_->GetPeerUuid();
}

string RaftConsensus::tablet_id() const {
  return state_->GetOptions().tablet_id;
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

  // Snooze to avoid the election timer firing again as much as possible.
  {
    ReplicaState::UniqueLock lock;
    CHECK_OK(state_->LockForRead(&lock));
    // We need to snooze when we win and when we lose:
    // - When we win because we're about to disable the timer and become leader.
    // - When we loose or otherwise we can fall into a cycle, where everyone keeps
    //   triggering elections but no election ever completes because by the time they
    //   finish another one is triggered already.
    // We ignore the status as we don't want to fail if we the timer is
    // disabled.
    ignore_result(SnoozeFailureDetectorUnlocked(LeaderElectionExpBackoffDeltaUnlocked()));
  }

  if (result.decision == VOTE_DENIED) {
    LOG_WITH_PREFIX_LK(INFO) << "Leader election lost for term " << result.election_term
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

Status RaftConsensus::ExpireFailureDetectorUnlocked() {
  if (PREDICT_FALSE(!FLAGS_enable_leader_failure_detection)) {
    return Status::OK();
  }

  return failure_detector_->MessageFrom(kTimerId, MonoTime::Min());
}

Status RaftConsensus::SnoozeFailureDetectorUnlocked() {
  return SnoozeFailureDetectorUnlocked(MonoDelta::FromMicroseconds(0));
}

Status RaftConsensus::SnoozeFailureDetectorUnlocked(const MonoDelta& additional_delta) {
  if (PREDICT_FALSE(!FLAGS_enable_leader_failure_detection)) {
    return Status::OK();
  }
  MonoTime time = MonoTime::Now(MonoTime::FINE);
  time.AddDelta(additional_delta);
  if (additional_delta.ToNanoseconds() > 0) {
    LOG(INFO) << "Snoozing failure detection for an additional: " << additional_delta.ToString();
  }
  return failure_detector_->MessageFrom(kTimerId, time);
}

MonoDelta RaftConsensus::LeaderElectionExpBackoffDeltaUnlocked() {
  int32_t failure_timeout = FLAGS_leader_failure_max_missed_heartbeat_periods *
      FLAGS_leader_heartbeat_interval_ms;
  int32_t exp_backoff_delta = pow(1.1, state_->GetCurrentTermUnlocked() -
                                  state_->GetCommittedOpIdUnlocked().term());
  exp_backoff_delta = std::min(failure_timeout * exp_backoff_delta,
                               FLAGS_leader_failure_exp_backoff_max_delta_ms);
  return MonoDelta::FromMilliseconds(exp_backoff_delta);
}

Status RaftConsensus::HandleTermAdvanceUnlocked(ConsensusTerm new_term) {
  if (new_term <= state_->GetCurrentTermUnlocked()) {
    return Status::IllegalState(Substitute("Can't advance term to: $0 current term: $1 is higher.",
                                           new_term, state_->GetCurrentTermUnlocked()));
  }
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
