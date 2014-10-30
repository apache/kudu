// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_CONSENSUS_RAFT_CONSENSUS_H_
#define KUDU_CONSENSUS_RAFT_CONSENSUS_H_

#include <boost/thread/locks.hpp>
#include <string>
#include <utility>
#include <vector>

#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/leader_election.h"

namespace kudu {

typedef boost::lock_guard<simple_spinlock> Lock;
typedef gscoped_ptr<Lock> ScopedLock;

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
class Peer;
class PeerProxyFactory;
class PeerManager;
class ReplicaState;

// The interface between RaftConsensus and the PeerMessageQueue.
class RaftConsensusQueueIface {
 public:
  // Called by the queue each time the response for a peer is handled with
  // the resulting commit index, triggering the apply for pending transactions.
  // The implementation is idempotent, i.e. independently of the ordering of
  // calls to this method only non-triggered applys will be started.
  virtual void UpdateCommittedIndex(const OpId& committed_index) = 0;

  // Notify the Consensus implementation that a follower replied with a term
  // higher than that established in the queue.
  virtual void NotifyTermChange(uint64_t term) = 0;

  // Returns a pointer to the Log so that the queue can load messages, if
  // needed.
  virtual log::Log* log() const = 0;

  virtual ~RaftConsensusQueueIface() {}
};

class RaftConsensus : public Consensus,
                      public RaftConsensusQueueIface {
 public:
  class ConsensusFaultHooks;

  RaftConsensus(const ConsensusOptions& options,
                gscoped_ptr<ConsensusMetadata> cmeta,
                gscoped_ptr<PeerProxyFactory> peer_proxy_factory,
                gscoped_ptr<PeerMessageQueue> queue,
                gscoped_ptr<PeerManager> peer_manager,
                const MetricContext& metric_ctx,
                const std::string& peer_uuid,
                const scoped_refptr<server::Clock>& clock,
                ReplicaTransactionFactory* txn_factory,
                log::Log* log);

  virtual ~RaftConsensus();

  virtual Status Start(const ConsensusBootstrapInfo& info) OVERRIDE;

  // Emulates an election by increasing the term number, marking
  // this peer as leader, marking the previous leader as follower
  // and calling ChangeConfig() with the resulting quorum.
  virtual Status EmulateElection() OVERRIDE;

  virtual Status StartElection() OVERRIDE;

  virtual Status Replicate(ConsensusRound* context) OVERRIDE;

  virtual Status Update(const ConsensusRequestPB* request,
                        ConsensusResponsePB* response) OVERRIDE;

  virtual Status RequestVote(const VoteRequestPB* request,
                             VoteResponsePB* response) OVERRIDE;

  virtual log::Log* log() const OVERRIDE { return log_; }

  virtual metadata::QuorumPeerPB::Role role() const OVERRIDE;

  virtual std::string peer_uuid() const OVERRIDE;

  virtual metadata::QuorumPB Quorum() const OVERRIDE;

  virtual void DumpStatusHtml(std::ostream& out) const OVERRIDE;

  virtual void Shutdown() OVERRIDE;

  // Return the active (as opposed to committed) role.
  metadata::QuorumPeerPB::Role GetActiveRole() const;

  // Returns the replica state for tests. This should never be used outside of
  // tests, in particular calling the LockFor* methods on the returned object
  // can cause consensus to deadlock.
  ReplicaState* GetReplicaStateForTests();

  // Registers a callback that will be triggered when the operation with 'op_id'
  // is replicated.
  Status RegisterOnReplicateCallback(
      const OpId& op_id,
      const std::tr1::shared_ptr<FutureCallback>& repl_callback);

  // Registers a callback that will be triggered when the operation with 'op_id'
  // is considered committed.
  // NOTE: 'op_id' is the id of the operation to commit, not the id of the commit
  // itself.
  Status RegisterOnCommitCallback(
      const OpId& op_id,
      const std::tr1::shared_ptr<FutureCallback>& commit_callback);

  // Updates the committed_index and triggers the Apply()s for whatever
  // transactions were pending.
  // This is idempotent.
  virtual void UpdateCommittedIndex(const OpId& commit_index) OVERRIDE;

  virtual void NotifyTermChange(uint64_t term) OVERRIDE;

  virtual Status GetLastReceivedOpId(OpId* id);

 protected:
  virtual Status Commit(gscoped_ptr<CommitMsg> commit,
                        const StatusCallback& cb) OVERRIDE;

 private:
  friend class ReplicaState;
  friend class RaftConsensusQuorumTest;
  FRIEND_TEST(RaftConsensusTest, TestReplicasHandleCommunicationErrors);
  FRIEND_TEST(RaftConsensusTest, DISABLED_TestLeaderPromotionWithQuiescedQuorum);

  // Verifies that 'quorum' is well formed and that no config change is in-flight.
  Status VerifyQuorumAndCheckThatNoChangeIsPendingUnlocked(const metadata::QuorumPB& quorum);

  // Same as below but acquires the lock through LockForChangeConfig first.
  Status ChangeConfig();

  // Changes this peer's configuration. Calls BecomeLeader(),
  // BecomeFollower() if appropriate.
  Status ChangeConfigUnlocked();

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

  // Called as a callback with the result of the config change transaction
  // that establishes this peer as leader.
  void BecomeLeaderResult(const Status& status);

  Status AppendNewRoundToQueueUnlocked(ConsensusRound* round);

  // Updates the state in a replica by storing the received operations in the log
  // and triggering the required transactions. This method won't return until all
  // operations have been stored in the log and all Prepares() have been completed,
  // and a replica cannot accept any more Update() requests until this is done.
  Status UpdateReplica(const ConsensusRequestPB* request,
                       ConsensusResponsePB* response);

  // Pushes a new quorum configuration to a majority of peers. Contrary to write operations,
  // this actually waits for the commit round to reach a majority of peers, so that we know
  // we can proceed. If this returns Status::OK(), a majority of peers have accepted the new
  // configuration. The peer cannot perform any additional operations until this succeeds.
  Status PushConfigurationToPeersUnlocked(const metadata::QuorumPB& new_config);

  OpId GetLastOpIdFromLog();

  // Step down as leader. Stop accepting write requests, etc.
  // Must hold both 'update_lock_' and the ReplicaState lock.
  Status StepDownIfLeaderUnlocked();

  // Return header string for RequestVote log messages.
  std::string GetRequestVoteLogHeader() const;

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

  // Respond to VoteRequest that the vote is granted for candidate.
  Status RequestVoteRespondVoteGranted(const VoteRequestPB* request,
                                       VoteResponsePB* response);

  void UpdateCommittedIndexUnlocked(const OpId& committed_index);

  // Callback for leader election driver.
  void ElectionCallback(const ElectionResult& result);

  log::Log* log_;
  scoped_refptr<server::Clock> clock_;
  gscoped_ptr<PeerProxyFactory> peer_proxy_factory_;
  // The queue of messages that must be sent to peers.
  gscoped_ptr<PeerMessageQueue> queue_;
  // PeerManager that manages a set of consensus Peers.
  gscoped_ptr<PeerManager> peer_manager_;
  gscoped_ptr<ThreadPool> callback_pool_;

  gscoped_ptr<ReplicaState> state_;

  // Proxies used during leader election. Initialized at startup.
  LeaderElection::ProxyMap election_proxies_;

  // TODO hack to serialize updates due to repeated/out-of-order messages
  // should probably be refactored out.
  //
  // Lock ordering note: If both this lock and the ReplicaState lock are to be
  // taken, this lock must be taken first.
  mutable simple_spinlock update_lock_;

  DISALLOW_COPY_AND_ASSIGN(RaftConsensus);
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_RAFT_CONSENSUS_H_ */
