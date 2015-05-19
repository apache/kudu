// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_CONSENSUS_RAFT_CONSENSUS_H_
#define KUDU_CONSENSUS_RAFT_CONSENSUS_H_

#include <boost/thread/locks.hpp>
#include <string>
#include <utility>
#include <vector>
#include <tr1/memory>

#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/util/atomic.h"
#include "kudu/util/failure_detector.h"

namespace kudu {

typedef boost::lock_guard<simple_spinlock> Lock;
typedef gscoped_ptr<Lock> ScopedLock;

class FailureDetector;
class FutureCallback;
class HostPort;
class ThreadPool;

namespace server {
class Clock;
}

namespace rpc {
class Messenger;
}

namespace consensus {
class ConsensusMetadata;
class Peer;
class PeerProxyFactory;
class PeerManager;
class ReplicaState;
struct ElectionResult;

class RaftConsensus : public Consensus,
                      public PeerMessageQueueObserver {
 public:
  class ConsensusFaultHooks;

  static scoped_refptr<RaftConsensus> Create(
    const ConsensusOptions& options,
    gscoped_ptr<ConsensusMetadata> cmeta,
    const std::string& peer_uuid,
    const scoped_refptr<MetricEntity>& metric_entity,
    const scoped_refptr<server::Clock>& clock,
    ReplicaTransactionFactory* txn_factory,
    const std::tr1::shared_ptr<rpc::Messenger>& messenger,
    const scoped_refptr<log::Log>& log,
    const std::tr1::shared_ptr<MemTracker>& parent_mem_tracker,
    const Closure& mark_dirty_clbk);

  RaftConsensus(const ConsensusOptions& options,
                gscoped_ptr<ConsensusMetadata> cmeta,
                gscoped_ptr<PeerProxyFactory> peer_proxy_factory,
                gscoped_ptr<PeerMessageQueue> queue,
                gscoped_ptr<PeerManager> peer_manager,
                gscoped_ptr<ThreadPool> thread_pool,
                const scoped_refptr<MetricEntity>& metric_entity,
                const std::string& peer_uuid,
                const scoped_refptr<server::Clock>& clock,
                ReplicaTransactionFactory* txn_factory,
                const scoped_refptr<log::Log>& log,
                const Closure& mark_dirty_clbk);

  virtual ~RaftConsensus();

  virtual Status Start(const ConsensusBootstrapInfo& info) OVERRIDE;

  virtual bool IsRunning() const OVERRIDE;

  // Emulates an election by increasing the term number and asserting leadership
  // in the quorum by sending a NO_OP to other members of the quorum.
  // This is NOT safe to use in a distributed quorum with failure detection
  // enabled, as it could result in a split-brain scenario.
  virtual Status EmulateElection() OVERRIDE;

  virtual Status StartElection(ElectionMode mode) OVERRIDE;

  // Call StartElection(), log a warning if the call fails (usually due to
  // being shut down).
  void ReportFailureDetected(const std::string& name, const Status& msg);

  virtual Status Replicate(const scoped_refptr<ConsensusRound>& round) OVERRIDE;

  virtual Status CheckLeadershipAndBindTerm(const scoped_refptr<ConsensusRound>& round) OVERRIDE;

  virtual Status Update(const ConsensusRequestPB* request,
                        ConsensusResponsePB* response) OVERRIDE;

  virtual Status RequestVote(const VoteRequestPB* request,
                             VoteResponsePB* response) OVERRIDE;

  virtual QuorumPeerPB::Role role() const OVERRIDE;

  virtual std::string peer_uuid() const OVERRIDE;

  virtual std::string tablet_id() const OVERRIDE;

  virtual ConsensusStatePB CommittedConsensusState() const OVERRIDE;

  virtual QuorumPB CommittedQuorum() const OVERRIDE;

  virtual void DumpStatusHtml(std::ostream& out) const OVERRIDE;

  virtual void Shutdown() OVERRIDE;

  // Makes this peer advance it's term (and step down if leader), for tests.
  virtual Status AdvanceTermForTests(int64_t new_term);

  // Return the active (as opposed to committed) role.
  QuorumPeerPB::Role GetActiveRole() const;

  // Returns the replica state for tests. This should never be used outside of
  // tests, in particular calling the LockFor* methods on the returned object
  // can cause consensus to deadlock.
  ReplicaState* GetReplicaStateForTests();

  // Updates the committed_index and triggers the Apply()s for whatever
  // transactions were pending.
  // This is idempotent.
  void UpdateMajorityReplicated(const OpId& majority_replicated,
                                OpId* committed_index) OVERRIDE;

  virtual void NotifyTermChange(uint64_t term) OVERRIDE;

  virtual Status GetLastReceivedOpId(OpId* id) OVERRIDE;

  virtual void MarkDirty() OVERRIDE;

 protected:
  // Trigger that a non-Transaction ConsensusRound has finished replication.
  // If the replication was successful, an status will be OK. Otherwise, it
  // may be Aborted or some other error status.
  // If the status is OK, write a Commit message to the local WAL based on the
  // type of message it is.
  virtual void NonTxRoundReplicationFinished(ConsensusRound* round, const Status& status);

  // As a leader, append a new ConsensusRond to the queue.
  // Only virtual and protected for mocking purposes.
  virtual Status AppendNewRoundToQueueUnlocked(const scoped_refptr<ConsensusRound>& round);

  // As a follower, start a consensus round not associated with a Transaction.
  // Only virtual and protected for mocking purposes.
  virtual Status StartConsensusOnlyRoundUnlocked(const ReplicateRefPtr& msg);

 private:
  friend class ReplicaState;
  friend class RaftConsensusQuorumTest;

  // Helper struct that contains the messages from the leader that we need to
  // append to our log, after they've been deduplicated.
  struct LeaderRequest {
    std::string leader_uuid;
    const OpId* preceding_opid;
    std::vector<ReplicateRefPtr> messages;
    // The positional index of the first message selected to be appended, in the
    // original leader's request message sequence.
    int64_t first_message_idx;

    std::string OpsRangeString() const;
  };

  std::string LogPrefixUnlocked();

  std::string LogPrefix();

  // Set the leader UUID of the quorum and mark the tablet config dirty for
  // reporting to the master.
  void SetLeaderUuidUnlocked(const std::string& uuid);

  // Makes the peer become leader.
  // Returns OK once the change config transaction that has this peer as leader
  // has been enqueued, the transaction will complete asynchronously.
  //
  // The ReplicaState must be locked for quorum change before calling.
  Status BecomeLeaderUnlocked();

  // Makes the peer become a replica, i.e. a FOLLOWER or a LEARNER.
  //
  // The ReplicaState must be locked for quorum change before calling.
  Status BecomeReplicaUnlocked();

  // Updates the state in a replica by storing the received operations in the log
  // and triggering the required transactions. This method won't return until all
  // operations have been stored in the log and all Prepares() have been completed,
  // and a replica cannot accept any more Update() requests until this is done.
  Status UpdateReplica(const ConsensusRequestPB* request,
                       ConsensusResponsePB* response);

  // Deduplicates an RPC request making sure that we get only messages that we
  // haven't appended to our log yet.
  // On return 'deduplicated_req' is instantiated with only the new messages
  // and the correct preceding id.
  void DeduplicateLeaderRequestUnlocked(ConsensusRequestPB* rpc_req,
                                        LeaderRequest* deduplicated_req);

  // Handles a request from a leader, refusing the request if the term is lower than
  // ours or stepping down if it's higher.
  Status HandleLeaderRequestTermUnlocked(const ConsensusRequestPB* request,
                                         ConsensusResponsePB* response);

  // Checks that the preceding op in 'req' is locally committed or pending and sets an
  // appropriate error message in 'response' if not.
  // If there is term mismatch between the preceding op id in 'req' and the local log's
  // pending operations, we proactively abort those pending operations after and including
  // the preceding op in 'req' to avoid a pointless cache miss in the leader's log cache.
  Status EnforceLogMatchingPropertyMatchesUnlocked(const LeaderRequest& req,
                                                   ConsensusResponsePB* response);

  // Check a request received from a leader, making sure:
  // - The request is in the right term
  // - The log matching property holds
  // - Messages are de-duplicated so that we only process previously unprocessed requests.
  // - We abort transactions if the leader sends transactions that have the same index as
  //   transactions currently on the pendings set, but different terms.
  // If this returns ok and the response has no errors, 'deduped_req' is set with only
  // the messages to add to our state machine.
  Status CheckLeaderRequestUnlocked(const ConsensusRequestPB* request,
                                    ConsensusResponsePB* response,
                                    LeaderRequest* deduped_req);

  // Pushes a new quorum configuration to a majority of peers. Contrary to write operations,
  // this actually waits for the commit round to reach a majority of peers, so that we know
  // we can proceed. If this returns Status::OK(), a majority of peers have accepted the new
  // configuration. The peer cannot perform any additional operations until this succeeds.
  Status PushConfigurationToPeersUnlocked(const QuorumPB& new_config);

  OpId GetLastOpIdFromLog();

  // Begin a replica transaction. If the type of message in 'msg' is not a type
  // that uses transactions, delegates to StartConsensusOnlyRoundUnlocked().
  Status StartReplicaTransactionUnlocked(const ReplicateRefPtr& msg);

  // Return header string for RequestVote log messages. The ReplicaState lock must be held.
  std::string GetRequestVoteLogPrefixUnlocked() const;

  // Fills the response with the current status, if an update was successful.
  void FillConsensusResponseOKUnlocked(ConsensusResponsePB* response);

  // Fills the response with an error code and error message.
  void FillConsensusResponseError(ConsensusResponsePB* response,
                                  ConsensusErrorPB::Code error_code,
                                  const Status& status);

  // Fill VoteResponsePB with the following information:
  // - Update responder_term to current local term.
  // - Set vote_granted to true.
  void FillVoteResponseVoteGranted(VoteResponsePB* response);

  // Fill VoteResponsePB with the following information:
  // - Update responder_term to current local term.
  // - Set vote_granted to false.
  // - Set consensus_error.code to the given code.
  void FillVoteResponseVoteDenied(ConsensusErrorPB::Code error_code, VoteResponsePB* response);

  // Respond to VoteRequest that the candidate is not in the quorum.
  Status RequestVoteRespondNotInQuorum(const VoteRequestPB* request, VoteResponsePB* response);

  // Respond to VoteRequest that the candidate has an old term.
  Status RequestVoteRespondInvalidTerm(const VoteRequestPB* request, VoteResponsePB* response);

  // Respond to VoteRequest that we already granted our vote to the candidate.
  Status RequestVoteRespondVoteAlreadyGranted(const VoteRequestPB* request,
                                              VoteResponsePB* response);

  // Respond to VoteRequest that we already granted our vote to someone else.
  Status RequestVoteRespondAlreadyVotedForOther(const VoteRequestPB* request,
                                                VoteResponsePB* response);

  // Respond to VoteRequest that the candidate's last-logged OpId is too old.
  Status RequestVoteRespondLastOpIdTooOld(const OpId& local_last_opid,
                                          const VoteRequestPB* request,
                                          VoteResponsePB* response);

  // Respond to VoteRequest that the vote was not granted because we believe
  // the leader to be alive.
  Status RequestVoteRespondLeaderIsAlive(const VoteRequestPB* request,
                                         VoteResponsePB* response);

  // Respond to VoteRequest that the vote is granted for candidate.
  Status RequestVoteRespondVoteGranted(const VoteRequestPB* request,
                                       VoteResponsePB* response);

  void UpdateMajorityReplicatedUnlocked(const OpId& majority_replicated,
                                        OpId* committed_index);

  // Callback for leader election driver. ElectionCallback is run on the
  // reactor thread, so it simply defers its work to DoElectionCallback.
  void ElectionCallback(const ElectionResult& result);
  void DoElectionCallback(const ElectionResult& result);

  // Start tracking the leader for failures. This typically occurs at startup
  // and when the local peer steps down as leader.
  // If the failure detector is already registered, has no effect.
  Status EnsureFailureDetectorEnabledUnlocked();

  // Untrack the current leader from failure detector.
  // This typically happens when the local peer becomes leader.
  // If the failure detector is already unregistered, has no effect.
  Status EnsureFailureDetectorDisabledUnlocked();

  // Set the failure detector to an "expired" state, so that the next time
  // the failure monitor runs it triggers an election.
  // This is primarily intended to be used at startup time.
  Status ExpireFailureDetectorUnlocked();

  // "Reset" the failure detector to indicate leader activity.
  // The failure detector must currently be enabled.
  // When this is called a failure is guaranteed not to be detected
  // before 'FLAGS_leader_failure_max_missed_heartbeat_periods' *
  // 'FLAGS_leader_heartbeat_interval_ms' has elapsed.
  Status SnoozeFailureDetectorUnlocked();

  // Like the above but adds 'additional_delta' to the default timeout
  // period.
  Status SnoozeFailureDetectorUnlocked(const MonoDelta& additional_delta);

  // Return the minimum election timeout. Due to backoff and random
  // jitter, election timeouts may be longer than this.
  MonoDelta MinimumElectionTimeout() const;

  // Calculates an additional snooze delta for leader election.
  // The additional delta increases exponentially with the difference
  // between the current term and the term of the last committed
  // operation.
  // The maximum delta is capped by 'FLAGS_leader_failure_exp_backoff_max_delta_ms'.
  MonoDelta LeaderElectionExpBackoffDeltaUnlocked();

  // Handle when the term has advanced beyond the current term.
  Status HandleTermAdvanceUnlocked(ConsensusTerm new_term);

  // Threadpool for constructing requests to peers, handling RPC callbacks,
  // etc.
  gscoped_ptr<ThreadPool> thread_pool_;

  scoped_refptr<log::Log> log_;
  scoped_refptr<server::Clock> clock_;
  gscoped_ptr<PeerProxyFactory> peer_proxy_factory_;

  gscoped_ptr<PeerManager> peer_manager_;

  // The queue of messages that must be sent to peers.
  gscoped_ptr<PeerMessageQueue> queue_;

  gscoped_ptr<ReplicaState> state_;

  // TODO: Plumb this from ServerBase.
  RandomizedFailureMonitor failure_monitor_;

  scoped_refptr<FailureDetector> failure_detector_;

  // If any RequestVote() RPC arrives before this timestamp,
  // the request will be ignored. This prevents abandoned or partitioned
  // nodes from disturbing the healthy leader.
  MonoTime withhold_votes_until_;

  // TODO hack to serialize updates due to repeated/out-of-order messages
  // should probably be refactored out.
  //
  // Lock ordering note: If both this lock and the ReplicaState lock are to be
  // taken, this lock must be taken first.
  mutable simple_spinlock update_lock_;

  const Closure mark_dirty_clbk_;

  AtomicBool shutdown_;

  DISALLOW_COPY_AND_ASSIGN(RaftConsensus);
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_RAFT_CONSENSUS_H_ */
