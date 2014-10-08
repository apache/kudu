// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CONSENSUS_RAFT_CONSENSUS_UTIL_H_
#define KUDU_CONSENSUS_RAFT_CONSENSUS_UTIL_H_

#include <map>
#include <set>
#include <string>
#include <tr1/unordered_set>
#include <tr1/unordered_map>
#include <utility>

#include "kudu/consensus/consensus.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_queue.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_waiter_set.h"
#include "kudu/gutil/port.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {

class FutureCallback;
class HostPort;
class ReplicaState;
class TaskExecutor;
class ThreadPool;

namespace rpc {
class Messenger;
}

namespace consensus {

// Immutable cached state of a quorum, including lists of voting peers,
// current role, and other information.
// Instantiate using the static Build() method below.
struct QuorumState {
  // Build an immutable QuorumState object, given a quorum and the UUID of the
  // current node. The object does not retain a reference to either quorum or
  // self_uuid.
  static gscoped_ptr<QuorumState> Build(const metadata::QuorumPB& quorum,
                                        const std::string& self_uuid);

  // The acting role of the current replica in the quorum.
  const metadata::QuorumPeerPB::Role role;

  // The UUID of the leader. This may be the same as the local peer, and
  // changes over time (but not for the lifetime of this object).
  const std::string leader_uuid;

  // The UUIDs for the current set of peers whose votes/acks count towards
  // majority.
  const std::tr1::unordered_set<std::string> voting_peers;

  // Current size of the quorum majority.
  // Number of "yes" votes it takes to get elected.
  const int majority_size;

  // Cache of the total size of the active quorum.
  const int quorum_size;

  // Sequence number of the active quorum.
  const int64_t config_seqno;

 private:
  // Private constructor called by static Build method.
  QuorumState(metadata::QuorumPeerPB::Role role,
              const std::string& leader_uuid,
              const std::tr1::unordered_set<std::string>& voting_peers,
              int majority_size,
              int quorum_size,
              int64_t config_seqno);
};

// Class that coordinates access to the replica state (independently of Role).
// This has a 1-1 relationship with RaftConsensus and is essentially responsible for
// keeping state and checking if state changes are viable.
//
// Note that, in the case of a LEADER role, there are two quorum states that
// that are tracked: a pending and a committed quorum. The "active" state is
// considered to be the pending quorum if it is non-null, otherwise the
// committed quorum is the active quorum.
//
// When a replica becomes a leader of a quorum, it sets the pending quorum to
// a new quorum declaring itself as leader and sets its "active" role to LEADER.
// It then starts up ConsensusPeers for each member of the pending quorum and
// tries to push a new configuration to the quorum. Once that configuration is
// pushed to a majority of the cluster, it is considered committed and the
// replica flushes that quorum to disk as the committed quorum.
//
// Each time an operation is to be performed on the replica the appropriate LockFor*()
// method should be called. The LockFor*() methods check that the replica is in the
// appropriate state to perform the requested operation and returns the lock or return
// Status::IllegalState if that is not the case.
//
// All state reading/writing methods acquire the lock, unless suffixed by "Unlocked", in
// which case a lock should be obtained prior to calling them.
class ReplicaState {
 public:
  typedef unique_lock<simple_spinlock> UniqueLock;

  typedef std::map<consensus::OpId,
                   ConsensusRound*,
                   OpIdCompareFunctor> OpIdToRoundMap;

  typedef std::set<consensus::OpId,
                   OpIdCompareFunctor> OutstandingCommits;

  typedef OpIdToRoundMap::value_type OpToRoundEntry;

  typedef std::multimap<consensus::OpId,
                        std::tr1::shared_ptr<FutureCallback>,
                        OpIdBiggerThanFunctor > CallbackMap;

  ReplicaState(const ConsensusOptions& options,
               ThreadPool* callback_exec_pool,
               const std::string& peer_uuid,
               gscoped_ptr<ConsensusMetadata> cmeta,
               ReplicaTransactionFactory* txn_factory);

  Status StartUnlocked(const OpId& initial_id);

  // Locks a replica in preparation for StartUnlocked(). Makes
  // sure the replica is in kInitialized state.
  Status LockForStart(UniqueLock* lock);

  // Locks a replica down until the critical section of an append completes,
  // i.e. until the replicate message has been assigned an id and placed in
  // the log queue.
  // This also checks that the replica is in the appropriate
  // state (role) to replicate the provided operation, that the operation
  // contains a replicate message and is of the appropriate type, and returns
  // Status::IllegalState if that is not the case.
  Status LockForReplicate(UniqueLock* lock, const OperationPB& op) WARN_UNUSED_RESULT;

  // Locks a replica down until the critical section of a commit completes.
  // This succeeds for all states since a replica which has initiated
  // a Prepare()/Replicate() must eventually commit even if it's state
  // has changed after the initial Append()/Update().
  Status LockForCommit(UniqueLock* lock) WARN_UNUSED_RESULT;

  // Locks a replica down until an the critical section of an update completes.
  // Further updates from the same or some other leader will be blocked until
  // this completes. This also checks that the replica is in the appropriate
  // state (role) to be updated and returns Status::IllegalState if that
  // is not the case.
  Status LockForUpdate(UniqueLock* lock) WARN_UNUSED_RESULT;

  // Changes the role to non-participant and returns a lock that can be
  // used to make sure no state updates come in until Shutdown() is
  // completed.
  Status LockForShutdown(UniqueLock* lock) WARN_UNUSED_RESULT;

  Status LockForConfigChange(UniqueLock* lock) WARN_UNUSED_RESULT;

  // Lock for use during leader election while running.
  Status LockForElection(UniqueLock* lock) WARN_UNUSED_RESULT;

  // Obtains the lock for a state read, does not check state.
  Status LockForRead(UniqueLock* lock) WARN_UNUSED_RESULT;

  // Completes the Shutdown() of this replica. No more operations, local
  // or otherwise can happen after this point.
  // Called after the quiescing phase (started with LockForShutdown())
  // finishes.
  Status Shutdown() WARN_UNUSED_RESULT;

  Status SetConfigDoneUnlocked();

  // Returns a const reference to the currently active quorum state, which may
  // correspond to a quorum pending commit in the case of a leader currently
  // pushing a config change round.
  // The returned reference is only valid while the lock is held.
  const QuorumState& GetActiveQuorumStateUnlocked() const;

  // Returns true if pending_quorum_ is non-null, indicating that there is a
  // quorum change currently in-flight but not yet committed.
  bool IsQuorumChangePendingUnlocked() const;

  // Sets the given quorum as pending commit. Does not persist into the quorum
  // metadata. In order to be persisted, SetCommittedQuorumUnlocked() must be called.
  //
  // This is only currently used by the Leader, in order to set up the quorum
  // state used to initialize the consensus queues and commit tracking before
  // the ChangeConfigTransaction is started and subsequently committed.
  Status SetPendingQuorumUnlocked(const metadata::QuorumPB& new_quorum);

  // Return the pending quorum, or crash if one is not set.
  const metadata::QuorumPB& GetPendingQuorumUnlocked() const;

  // Changes the config for this replica. Checks that the role change
  // is legal. Also checks that if the pending quorum is set, the persistent
  // quorum matches it, and resets the pending quorum to null.
  Status SetCommittedQuorumUnlocked(const metadata::QuorumPB& new_quorum);

  // Return the persisted quorum.
  const metadata::QuorumPB& GetCommittedQuorumUnlocked() const;

  // Increments this peer's term and sets 'has voted' to no for the current
  // term.
  Status IncrementTermUnlocked() WARN_UNUSED_RESULT;

  // Checks if the term change is legal. If so, sets 'current_term'
  // to 'new_term' and sets 'has voted' to no for the current term.
  Status SetCurrentTermUnlocked(uint64_t new_term) WARN_UNUSED_RESULT;

  // Returns the term set in the last config change round.
  const uint64_t GetCurrentTermUnlocked() const;

  // Return whether this peer has voted in the current term.
  const bool HasVotedCurrentTermUnlocked() const;

  // Record replica's vote for the current term, then flush the consensus
  // metadata to disk.
  Status SetVotedForCurrentTermUnlocked(const std::string& uuid) WARN_UNUSED_RESULT;

  // Return replica's vote for the current term.
  // The vote must be set; use HasVotedCurrentTermUnlocked() to check.
  const std::string& GetVotedForCurrentTermUnlocked() const;

  ReplicaTransactionFactory* GetReplicaTransactionFactoryUnlocked() const;

  Status IncrementConfigSeqNoUnlocked();

  // Returns the uuid of the peer to which this replica state belongs.
  const std::string& GetPeerUuid() const;

  const ConsensusOptions& GetOptions() const;

  // Enqueues a Prepare() in the ReplicaTransactionFactory.
  Status EnqueuePrepareUnlocked(gscoped_ptr<ConsensusRound> context);

  // Add 'round' to the set of rounds waiting to be committed.
  Status AddPendingOperation(ConsensusRound* round);

  // Marks ReplicaTransactions up to 'id' as committed by the leader, meaning the
  // transaction may Apply() (immediately if Prepare() has completed or when Prepare()
  // completes, if not).
  Status MarkConsensusCommittedUpToUnlocked(const OpId& id);

  // Returns the watermark below which all operations are known to
  // be committed according to consensus.
  //
  // This must be called under a lock.
  const OpId& GetCommittedOpIdUnlocked() const;

  // Updates the last replicated operation.
  // This must be called under a lock and triggers the replication callbacks
  // registered for all ops whose id is less than or equal to 'op_id'.
  void UpdateLastReplicatedOpIdUnlocked(const OpId& op_id);

  // Returns the last replicated op id. This must be called under the lock.
  const OpId& GetLastReplicatedOpIdUnlocked() const;

  // Updates the last received operation.
  // This must be called under a lock.
  void UpdateLastReceivedOpIdUnlocked(const OpId& op_id);

  // Returns the last received op id. This must be called under the lock.
  const OpId& GetLastReceivedOpIdUnlocked() const;

  void UpdateCommittedOpIdUnlocked(const OpId& committed_op_id);

  // Updates the last committed operation including removing it from the pending commits
  // map and triggering any related callback registered for 'committed_op_id'.
  //
  // 'commit_op_id' refers to the OpId of the actual commit operation, whereas
  // 'committed_op_id' refers to the OpId of the original REPLICATE message which was
  // committed.
  //
  // This must be called under a lock.
  void UpdateReplicaCommittedOpIdUnlocked(const OpId& committed_op_id);

  // Called at the very end of a commit to count down the latch.
  // After this method is called both the RaftConsensus instance and the ReplicaState
  // might be destroyed so it is no longer safe to query their state.
  void CountDownOutstandingCommitsIfShuttingDown();

  // Waits for already triggered Apply()s to commit.
  Status WaitForOustandingApplies();

  // Used by replicas to cancel pending transactions. Pending transaction are those
  // that have completed prepare/replicate but are waiting on the LEADER's commit
  // to complete. This does not cancel transactions being applied.
  Status CancelPendingTransactions();

  // Obtains the lock and registers a callback that will be triggered when
  // an operation with id equal to or greater than 'op_id' is replicated.
  // Once the callback is fired, it is removed from the registered callbacks.
  // Returns Status::OK() if the callback was triggered, or Status::AlreadyPresent
  // if the requested operation was already replicated.
  Status RegisterOnReplicateCallback(
      const OpId& replicate_op_id,
      const std::tr1::shared_ptr<FutureCallback>& repl_callback);

  // Similar to the RegisterOnReplicateCallback() but for commit messages,
  // more specifically for the commit of the operation whose id was
  // 'replicate_op_id'.
  // Note that the callback is only triggered after the Apply(). Callbacks
  // registered through this method will only be triggered after the
  // *replica's* commit message is appended to the local log.
  // With regard to how the registered id compares to previous/future operations
  // one of several things might happen:
  //
  // - replicate_op_id is beyond any replicate message received for
  //   this replica: The callback is registered and will fire when
  //   the corresponding operation is committed.
  //   TODO: fire callback.OnFailure() if the term changes and we
  //   never see a replicate with the provided OpId or if such a message
  //   arrives but is not a replicate.
  //
  // - replicate_op_id is in currently in the pending commits, i.e. we've
  //   received a replicate message with 'replicate_op_id' from the leader
  //   but have yet to receive the corresponding commit message, or we've
  //   received the commit message but it hasn't finished applying yet:
  //   the callback will be registered and fired when it happens.
  //   TODO take care when a new leader is elected as he might choose not
  //   to commit the operation. Fire an callback.OnFailure() in that instance.
  //
  // - replicate_op_id is before the most recent replicate message received
  //   and is not on the pending_commits set: the operation was already committed
  //   and this call returns Status::AlreadyPresent().
  Status RegisterOnCommitCallback(
      const OpId& replicate_op_id,
      const std::tr1::shared_ptr<FutureCallback>& commit_callback);

  void NewIdUnlocked(OpId* id);

  // Used when, for some reason, an operation that failed before it could be considered
  // a part of the state machine. Basically restores the id gen to the state it was before
  // generating 'id'.
  void CancelPendingOperation(const OpId& id);

  // Returns the number of transactions that are currently in the pending state
  // i.e. transactions for which Prepare() is done or under way.
  int GetNumPendingTxnsUnlocked() const;

  std::string ToString() const;
  std::string ToStringUnlocked() const;

  // A common prefix that should be in any log messages emitted,
  // identifying the tablet and peer.
  std::string LogPrefix();
  std::string LogPrefixUnlocked() const;

 private:
  // Helper method to update the active quorum state for peers, etc.
  void ResetActiveQuorumStateUnlocked(const metadata::QuorumPB& quorum);

  const ConsensusOptions options_;

  // The UUID of the local peer.
  const std::string peer_uuid_;

  ThreadPool* callback_pool_;

  // Cache of the current active quorum state. May refer to either the pending
  // or committed quorum. This can be tested by checking whether pending_quorum_
  // is NULL.
  gscoped_ptr<QuorumState> active_quorum_state_;

  // Quorum used by the peers when there is a pending config change operation.
  gscoped_ptr<metadata::QuorumPB> pending_quorum_;

  // Consensus metadata persistence object.
  gscoped_ptr<ConsensusMetadata> cmeta_;

  // Used by the LEADER. This is the index of the next operation generated
  // by this LEADER.
  uint64_t next_index_;

  // OpId=>Context map that manages pending txns, i.e. operations for which we've
  // received a replicate message from the leader but have yet to receive the commit
  // message.
  // The key is the id of the replicate operation for which we're expecting a commit
  // message.
  // Used when Role = FOLLOWER/CANDIDATE/LEARNER.
  OpIdToRoundMap pending_txns_;

  // Set that tracks the outstanding applies that are being executed asynchronously.
  //
  // These operations have been replicated and committed by consensus, but not yet
  // completed on the local replica.
  //
  // The key is the OpId of the operation being applied.
  // Used when Role = FOLLOWER/CANDIDATE/LEARNER.
  OutstandingCommits in_flight_commits_;

  // When we receive a message from a remote peer telling us to start a transaction, we use
  // this factory to start it.
  // Used when Role = FOLLOWER/CANDIDATE/LEARNER.
  ReplicaTransactionFactory* txn_factory_;

  // The id if the last replicated operation. All replicate operations whose id is lower
  // than or equal to this one were already replicated.
  //  Used when Role = FOLLOWER/CANDIDATE/LEARNER.
  OpId replicated_op_id_;

  // The id of the last received operation. Operations whose id is lower than or equal
  // to this id do not need to be resent by the leader.
  //  Used when Role = FOLLOWER/CANDIDATE/LEARNER.
  OpId received_op_id_;

  // The id of the Apply that was last triggered when the last message from the leader
  // was received. Initialized to MinimumOpId().
  // Used when Role = FOLLOWER/CANDIDATE/LEARNER.
  OpId last_triggered_apply_;

  // Latch that allows to wait on the outstanding applies to have completed..
  // Used when Role = FOLLOWER/CANDIDATE/LEARNER.
  CountDownLatch in_flight_applies_latch_;

  // Multimap that stores the replicate callbacks in decreasing order.
  OpIdWaiterSet replicate_watchers_;
  // Multimap that stores the commit callbacks in increasing order.
  OpIdWaiterSet commit_watchers_;

  // lock protecting state machine updates
  mutable simple_spinlock update_lock_;

  enum State {
    // State after the replica is built.
    kInitialized,
    // State signaling the replica is changing configs. Replicas
    // need both the replicate and the commit message for the config
    // change before proceeding.
    kChangingConfig,
    // State signaling the replica accepts requests (from clients
    // if leader, from leader if follower)
    kRunning,
    // State signaling the replica is shutting down and only accepts commits
    // from previously started transactions.
    kShuttingDown,
    // State signaling the replica is shutdown and does not accept
    // any more requests.
    kShutDown
  };

  State state_;
};

// Callbacks are shared_ptrs that need to be kept alive until after they have executed.
// As often callbacks are the very last thing to run on a transaction, they may in some
// cases loose all the references to them before they actually get executed.
// To address this we create a wrapper runnable that makes sure the reference
// count for the operation callback is kept above 0 until the callback actually
// runs.
class OperationCallbackRunnable : public Runnable {
 public:
  explicit OperationCallbackRunnable(const std::tr1::shared_ptr<FutureCallback>& callback);

  void set_error(const Status& error);

  virtual void Run() OVERRIDE;

 private:
   friend class MajorityOpStatusTracker;
   std::tr1::shared_ptr<FutureCallback> callback_;
   Status error_;
   mutable simple_spinlock lock_;
};

// Status for operations that need to be ack's by a majority to be considered
// committed.
// IsDone becomes true when a 'majority' of peers have ACK'd the message
// IsAllDone becomes true when all peers have ACK'd the message.
class MajorityOpStatusTracker : public OperationStatusTracker {
 public:

  MajorityOpStatusTracker(gscoped_ptr<OperationPB> operation,
                          const std::tr1::unordered_set<std::string>& voting_peers,
                          int majority,
                          int total_peers_count);

  MajorityOpStatusTracker(gscoped_ptr<OperationPB> operation,
                          const std::tr1::unordered_set<std::string>& voting_peers,
                          int majority,
                          int total_peers_count,
                          ThreadPool* callback_pool,
                          const std::tr1::shared_ptr<FutureCallback>& callback);


  virtual void AckPeer(const std::string& uuid) OVERRIDE;

  virtual bool IsDone() const OVERRIDE;

  virtual bool IsAllDone() const OVERRIDE;

  virtual void Wait() OVERRIDE;

  virtual std::string ToString() const OVERRIDE;

  virtual ~MajorityOpStatusTracker();

 private:
  std::string ToStringUnlocked() const;

  // The total number of peers in a majority.
  const int majority_;
  // The peer's whose acks count towards majority.
  const std::tr1::unordered_set<std::string> voting_peers_;
  // The total number of peers expected to replicate the operation.
  const int total_peers_count_;

  // How many peers have replicated this operation so far.
  int replicated_count_;

  // Latch that allows to wait for the operation to become
  // IsDone().
  CountDownLatch completion_latch_;

  ThreadPool* callback_pool_;
  gscoped_ptr<OperationCallbackRunnable> runnable_;

  // Lock that protects access to the state variables.
  mutable simple_spinlock lock_;
};

}  // namespace consensus
}  // namespace kudu

#endif /* KUDU_CONSENSUS_RAFT_CONSENSUS_UTIL_H_ */
