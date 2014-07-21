// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDO_QUORUM_CONSENSUS_H_
#define KUDO_QUORUM_CONSENSUS_H_

#include <iosfwd>
#include <string>
#include <vector>

#include "consensus/consensus.pb.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/ref_counted.h"
#include "util/status.h"
#include "util/task_executor.h"

namespace kudu {

namespace server {
class Clock;
}

namespace log {
class Log;
}

namespace metadata {
class QuorumPB;
class QuorumPeerPB;
}

namespace consensus {

// forward declarations
class ConsensusRound;
class ReplicaTransactionFactory;

struct ConsensusOptions {
  std::string tablet_id;
};

// After completing bootstrap, some of the results need to be plumbed through
// into the consensus implementation.
struct ConsensusBootstrapInfo {
  ConsensusBootstrapInfo();
  ~ConsensusBootstrapInfo();

  // The id of the last COMMIT operation in the log
  consensus::OpId last_commit_id;

  // The id of the last REPLICATE operation in the log
  consensus::OpId last_replicate_id;

  // The id of the last operation in the log
  consensus::OpId last_id;

  // REPLICATE messages which were in the log with no accompanying
  // COMMIT. These need to be passed along to consensus init in order
  // to potentially commit them.
  std::vector<consensus::OperationPB*> orphaned_replicates;

 private:
  DISALLOW_COPY_AND_ASSIGN(ConsensusBootstrapInfo);
};

// The external interface for a consensus peer.
//
// Note: Even though Consensus points to Log, it needs to be destroyed
// after it. See Log class header comment for the reason why. On the other
// hand Consensus must be quiesced before closing the log, otherwise it
// will try to write to a destroyed/closed log.
//
// The order of these operations on shutdown must therefore be:
// 1 - quiesce Consensus
// 2 - close/destroy Log
// 3 - destroy Consensus
class Consensus {
 public:
  class ConsensusFaultHooks;
  Consensus() {}

  // Initializes Consensus.
  // Note: Consensus does not own the Log and must be provided a fully built
  // one on startup.
  virtual Status Init(const metadata::QuorumPeerPB& peer,
                      const scoped_refptr<server::Clock>& clock,
                      ReplicaTransactionFactory* txn_factory,
                      log::Log* log) = 0;

  // Starts running the consensus algorithm.
  // The provided configuration is taken as a hint and may not be the
  // final configuration of the quorum. Specifically peer roles may
  // vary (e.g. if leader election was triggered) and even membership
  // may vary if the last known quorum configuration had different
  // members from the provided one.
  //
  // The results of the tablet bootstrap process are passed in as
  // 'bootstrap_results'. This includes any operations which were REPLICATEd
  // without being COMMITted before a crash.
  //
  // The actual configuration used after Start() completes is returned
  // in 'running_quorum'.
  virtual Status Start(const metadata::QuorumPB& initial_quorum,
                       const ConsensusBootstrapInfo& bootstrap_info,
                       gscoped_ptr<metadata::QuorumPB>* running_quorum) = 0;

  // Creates a new ConsensusRound, the entity that owns all the data
  // structures required for a consensus round, such as the ReplicateMsg
  // (and later on the CommitMsg). ConsensusRound will also point to and
  // increase the reference count for the provided callbacks.
  ConsensusRound* NewRound(
      gscoped_ptr<ReplicateMsg> entry,
      const std::tr1::shared_ptr<FutureCallback>& repl_callback,
      const std::tr1::shared_ptr<FutureCallback>& commit_callback);

  // Called by a quorum client to replicate an entry to the state machine.
  //
  // From the leader instance perspective execution proceeds as follows:
  //
  //           Leader                               Quorum
  //             +                                     +
  //     1) Req->| Append()                            |
  //             |                                     |
  //     2)      +-------------replicate-------------->|
  //             |<---------------ACK------------------+
  //             |                                     |
  //     3)      +--+                                  |
  //           <----+ round.replicate_callback()         |
  //             |                                     |
  //     4)   -->| Commit()                            |
  //             |                                     |
  //     5)      +--+                                  |
  //             |<-+ commit                           |
  //             |                                     |
  //     6)      +--+ round.commit_callback()            |
  //        Res<----+                                  |
  //             |                                     |
  //             |                                     |
  //     7)      +--------------commit---------------->|
  //
  // 1) Caller calls Append(), method returns immediately to the caller and
  // runs asynchronously.
  // 2) Leader replicates the entry to the quorum using the consensus
  // algorithm, proceeds as soon as a majority of the quorum acknowledges the
  // entry.
  // 3) Leader defers to the caller through ReplicateCallback.onReplicate()
  // call, performs no additional action.
  // 4) Caller calls ReplicateCallback.Commit(), triggering the Leader to
  // proceed.
  // 5) Leader executes commit locally.
  // 6) CommitCallback.onCommit() gets called completing execution from the
  // client's perspective.
  // 7) Leader eventually sends commit message to the quorum.
  //
  // This method can only be called on the leader, i.e. role() == LEADER
  virtual Status Replicate(ConsensusRound* context) = 0;

  // Messages sent from LEADER to FOLLOWERS and LEARNERS to update their
  // state machines.
  // ConsensusRequestPB contains a sequence of 0 or more operations to apply
  // on the replica. If there are 0 operations the request is considered
  // 'status-only' i.e. the leader is just asking how far the replica has
  // received/replicated/committed the operations and the replica replies
  // as such.
  // If the sequence contains 1 or more operations they will be applied
  // in the same order as the leader.
  // In particular, replicates are stored in the log in the same order as
  // the leader and Prepare()s are triggered in the same order as the
  // replicates.
  // Commit operations have two ids, the "commit_id' which is monotonically
  // increasing and the 'committed_op_id' which might not be monotonically
  // increasing (as the leader may commit out-of-order).
  // Replicas Apply() the commits as soon as the corresponding Prepare()s
  // are done *and* the CommitMsg has been received from the LEADER.
  virtual Status Update(const ConsensusRequestPB* request,
                        ConsensusResponsePB* response) = 0;

  // Messages sent from CANDIDATEs to voting peers to request their vote
  // in leader election.
  virtual Status RequestVote(const VoteRequestPB* request,
                             VoteResponsePB* response) = 0;


  // Returns the current quorum role of this instance.
  virtual metadata::QuorumPeerPB::Role role() const = 0;

  // Returns the uuid of this peer.
  virtual std::string peer_uuid() const = 0;

  // Returns the current configuration of the quorum.
  // NOTE: Returns a copy, thus should not be used in a tight loop.
  virtual metadata::QuorumPB Quorum() const = 0;

  virtual void DumpStatusHtml(std::ostream& out) const = 0;

  virtual ~Consensus() {}

  void SetFaultHooks(const std::tr1::shared_ptr<ConsensusFaultHooks>& hooks);

  const std::tr1::shared_ptr<ConsensusFaultHooks>& GetFaultHooks() const;

  // Stops running the consensus algorithm.
  virtual void Shutdown() = 0;
 protected:
  friend class ConsensusRound;

  // Called by Consensus context to complete the commit of a consensus
  // round.
  virtual Status Commit(ConsensusRound* round) = 0;

  // Fault hooks for tests. In production code this will always be null.
  std::tr1::shared_ptr<ConsensusFaultHooks> fault_hooks_;

  enum HookPoint {
    PRE_START,
    POST_START,
    PRE_CONFIG_CHANGE,
    POST_CONFIG_CHANGE,
    PRE_REPLICATE,
    POST_REPLICATE,
    PRE_COMMIT,
    POST_COMMIT,
    PRE_UPDATE,
    POST_UPDATE,
    PRE_SHUTDOWN,
    POST_SHUTDOWN
  };

  Status ExecuteHook(HookPoint point);

  enum State {
    kNotInitialized,
    kInitializing,
    kConfiguring,
    kRunning,
  };
 private:
  DISALLOW_COPY_AND_ASSIGN(Consensus);
};

// A commit continuation for replicas.
//
// When a replica transaction is started with ReplicaTransactionFactory::StartTransaction()
// the context is set with a commit continuation. Once the leader's commit message for this
// transaction arrives consensus calls ReplicaCommitContinuation::LeaderCommitted() which
// triggers the replica to apply/abort based on the leader's message.
//
// Commit continuations should execute in their own executor, but must execute in order,
// i.e. the caller should not block until LeaderCommitted() completes but two subsequent
// calls must complete in the order they were called. This because replicas must enforce
// that operations are performed in the same order as the leader to keep monotonically
// increasing timestamps.
class ReplicaCommitContinuation {
 public:
  virtual Status LeaderCommitted(gscoped_ptr<OperationPB> leader_commit_op) = 0;

  // Aborts the replica transaction, making the transaction release its
  // resources.
  // Note that this is not equivalent to an OP_ABORT commit, which is issued by
  // the LEADER, instead this is used when the replica is shutting down and needs
  // to cancel pending transactions. This doesn't cause a commit message to be stored
  // in the WAL.
  virtual void Abort() = 0;
  virtual ~ReplicaCommitContinuation() {}
};

// Factory for replica transactions.
// An implementation of this factory must be registered prior to consensus
// start, and is used to create transactions when the consensus implementation receives
// messages from the leader.
//
// Replica transactions execute the following way:
//
// - When a ReplicateMsg is first received from the leader, the Consensus
//   instance creates the ConsensusRound and calls StartReplicaTransaction().
//   This will trigger the Prepare(). At the same time replica consensus
//   instance immediately stores the ReplicateMsg in the Log. Once the replicate
//   message is stored in stable storage an ACK is sent to the leader (i.e. the
//   replica Consensus instance does not wait for Prepare() to finish).
//
// - When the CommitMsg for a replicate is first received from the leader
//   the replica waits for the corresponding Prepare() to finish (if it has
//   not completed yet) and then proceeds to trigger the Apply().
//
// - Once Apply() completes the ReplicaTransactionFactory will call Commit()
//   on 'context' triggering the replica side commit message to be stored
//   on stable storage and the replica transaction to finish.
class ReplicaTransactionFactory {
 public:
  virtual Status StartReplicaTransaction(gscoped_ptr<ConsensusRound> context) = 0;
  virtual ~ReplicaTransactionFactory() {}
};

// Context for a consensus round on the LEADER side, typically created as an
// out-parameter of Consensus::Append.
class ConsensusRound {
 public:
  // Ctor used for leader transactions. Leader transactions can and must specify the
  // callbacks prior to initiating the consensus round.
  ConsensusRound(Consensus* consensus,
                 gscoped_ptr<OperationPB> replicate_op,
                 const std::tr1::shared_ptr<FutureCallback>& replicate_callback,
                 const std::tr1::shared_ptr<FutureCallback>& commit_callback);

  // Ctor used for follower/learner transactions. These transactions do not use the
  // replicate callback and the commit callback is set later, after the transaction
  // is actually started.
  ConsensusRound(Consensus* consensus,
                 gscoped_ptr<OperationPB> replicate_op);

  OperationPB* replicate_op() {
    return replicate_op_.get();
  }

  // Returns the id of the (replicate) operation this context
  // refers to. This is only set _after_ Consensus::Replicate(context).
  OpId id() const {
    return DCHECK_NOTNULL(replicate_op_.get())->id();
  }

  Status Commit(gscoped_ptr<CommitMsg> commit);

  void SetCommitCallback(const std::tr1::shared_ptr<FutureCallback>& commit_clbk) {
    commit_callback_ = commit_clbk;
  }

  const std::tr1::shared_ptr<FutureCallback>& commit_callback() {
    return commit_callback_;
  }

  const std::tr1::shared_ptr<FutureCallback>& replicate_callback() {
    return replicate_callback_;
  }

  void release_commit_callback(std::tr1::shared_ptr<FutureCallback>* ret) {
    ret->swap(commit_callback_);
    commit_callback_.reset();
  }

  OperationPB* commit_op() {
    return commit_op_.get();
  }

  OperationPB* leader_commit_op() {
    return leader_commit_op_.get();
  }

  OperationPB* release_commit_op() {
    return commit_op_.release();
  }

  void SetLeaderCommitOp(gscoped_ptr<OperationPB> leader_commit_op) {
    leader_commit_op_.reset(leader_commit_op.release());
  }

  void SetReplicaCommitContinuation(ReplicaCommitContinuation* continuation) {
    continuation_ = continuation;
  }

  ReplicaCommitContinuation* GetReplicaCommitContinuation() {
    return continuation_;
  }

 private:
  Consensus* consensus_;
  // This round's replicate operation.
  gscoped_ptr<OperationPB> replicate_op_;
  // This round's commit operation.
  gscoped_ptr<OperationPB> commit_op_;
  // This rounds leader commit operation. This is only present in non-leader
  // replicas and is required for the creation of the replica's commit
  // message as it will share some information with the leader.
  gscoped_ptr<OperationPB> leader_commit_op_;
  // The callback that is called once the replicate phase of this consensus
  // round is finished.
  std::tr1::shared_ptr<FutureCallback> replicate_callback_;
  // The callback that is called once the commit phase of this consensus
  // round is finished.
  std::tr1::shared_ptr<FutureCallback> commit_callback_;
  // The commit continuation for replicas. This is only set in non-leader
  // replicas and is called once the leader's commit message is received
  // and the replica can proceed with it's own Apply() and Commit().
  ReplicaCommitContinuation* continuation_;
};

class Consensus::ConsensusFaultHooks {
 public:
  virtual Status PreStart() { return Status::OK(); }
  virtual Status PostStart() { return Status::OK(); }
  virtual Status PreConfigChange() { return Status::OK(); }
  virtual Status PostConfigChange() { return Status::OK(); }
  virtual Status PreReplicate() { return Status::OK(); }
  virtual Status PostReplicate() { return Status::OK(); }
  virtual Status PreCommit() { return Status::OK(); }
  virtual Status PostCommit() { return Status::OK(); }
  virtual Status PreUpdate() { return Status::OK(); }
  virtual Status PostUpdate() { return Status::OK(); }
  virtual Status PreShutdown() { return Status::OK(); }
  virtual Status PostShutdown() { return Status::OK(); }
  virtual ~ConsensusFaultHooks() {}
};

} // namespace consensus
} // namespace kudu

#endif /* CONSENSUS_H_ */
