// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDO_QUORUM_CONSENSUS_H_
#define KUDO_QUORUM_CONSENSUS_H_

#include <iosfwd>
#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"
#include "kudu/util/task_executor.h"

namespace kudu {

namespace server {
class Clock;
}

namespace log {
class Log;
}

namespace master {
class SysTable;
}

namespace metadata {
class QuorumPB;
class QuorumPeerPB;
}

namespace tablet {
class ChangeConfigTransaction;
class TabletPeer;
}

namespace consensus {

// forward declarations
class ConsensusRound;
class ReplicaTransactionFactory;
class ConsensusCommitContinuation;

typedef uint64_t ConsensusTerm;

struct ConsensusOptions {
  std::string tablet_id;
};

// After completing bootstrap, some of the results need to be plumbed through
// into the consensus implementation.
struct ConsensusBootstrapInfo {
  ConsensusBootstrapInfo();
  ~ConsensusBootstrapInfo();

  // The id of the last operation in the log
  consensus::OpId last_id;

  // The id of the last committed operation in the log.
  consensus::OpId last_committed_id;

  // REPLICATE messages which were in the log with no accompanying
  // COMMIT. These need to be passed along to consensus init in order
  // to potentially commit them.
  std::vector<consensus::ReplicateMsg*> orphaned_replicates;

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
class Consensus : public RefCountedThreadSafe<Consensus> {
 public:
  class ConsensusFaultHooks;

  // Verifies that the provided quorum is well formed.
  static Status VerifyQuorum(const metadata::QuorumPB& quorum);

  Consensus() {}

  // Starts running the consensus algorithm.
  virtual Status Start(const ConsensusBootstrapInfo& info) = 0;

  // Emulates a leader election by simply making this peer leader.
  virtual Status EmulateElection() = 0;

  // Triggers a leader election.
  virtual Status StartElection() = 0;

  // Creates a new ConsensusRound, the entity that owns all the data
  // structures required for a consensus round, such as the ReplicateMsg
  // (and later on the CommitMsg). ConsensusRound will also point to and
  // increase the reference count for the provided callbacks.
  ConsensusRound* NewRound(
      gscoped_ptr<ReplicateMsg> replicate_msg,
      ConsensusCommitContinuation* commit_continuation,
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
  // state machines. This is equivalent to "AppendEntries()" in Raft
  // terminology.
  //
  // ConsensusRequestPB contains a sequence of 0 or more operations to apply
  // on the replica. If there are 0 operations the request is considered
  // 'status-only' i.e. the leader is communicating with the follower only
  // in order to pass back and forth information on watermarks (eg committed
  // operation ID, replicated op id, etc).
  //
  // If the sequence contains 1 or more operations they will be replicated
  // in the same order as the leader, and submitted for asynchronous Prepare
  // in the same order.
  //
  // The leader also provides information on the index of the latest
  // operation considered committed by consensus. The replica uses this
  // information to update the state of any pending (previously replicated/prepared)
  // transactions.
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

  void SetFaultHooks(const std::tr1::shared_ptr<ConsensusFaultHooks>& hooks);

  const std::tr1::shared_ptr<ConsensusFaultHooks>& GetFaultHooks() const;

  // Stops running the consensus algorithm.
  virtual void Shutdown() = 0;

  // TEMPORARY: Allows to get the last received OpId by this replica
  // TODO Remove once we have solid election.
  virtual Status GetLastReceivedOpId(OpId* id) { return Status::NotFound("Not implemented."); }

 protected:
  friend class ConsensusRound;
  friend class RefCountedThreadSafe<Consensus>;
  friend class tablet::ChangeConfigTransaction;
  friend class tablet::TabletPeer;
  friend class master::SysTable;

  // This class is refcounted.
  virtual ~Consensus() {}

  // Called by Consensus context to complete the commit of a consensus
  // round.
  virtual Status Commit(gscoped_ptr<CommitMsg> commit_msg,
                        const StatusCallback& cb) = 0;

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

// A commit continuation for consensus peers, called when consensus deems a transaction
// committed or aborted.
class ConsensusCommitContinuation {
 public:

  // Called by consensus to notify that the operation has been considered either replicated,
  // if 'status' is OK(), or that it has permanently failed to replicate if 'status' is anything
  // else. If 'status' is OK() then the operation can be applied to the state machine, otherwise
  // the operation should be aborted.
  //
  // TODO The only place where this is called right now is when consensus is
  // shutting down and a replica needs to cancel transactions that are in flight.
  // In this case those will be left as pending transactions that will be reprised
  // on startup. That is ok for now, but once we have true operation abort, i.e.
  // when consensus needs to cancel a transaction that already landed on disk,
  // we will need to distinguish between a the current case and 'true' abort, as
  // the latter requires an abort commit message.
  virtual void ReplicationFinished(const Status& status) = 0;

  StatusCallback AsStatusCallback() {
    return Bind(&ConsensusCommitContinuation::ReplicationFinished, Unretained(this));
  }

  virtual ~ConsensusCommitContinuation() {}
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
                 gscoped_ptr<ReplicateMsg> replicate_msg,
                 ConsensusCommitContinuation* commit_continuation,
                 const std::tr1::shared_ptr<FutureCallback>& commit_callback);

  // Ctor used for follower/learner transactions. These transactions do not use the
  // replicate callback and the commit callback is set later, after the transaction
  // is actually started.
  ConsensusRound(Consensus* consensus,
                 gscoped_ptr<ReplicateMsg> replicate_msg);

  ReplicateMsg* replicate_msg() {
    return replicate_msg_.get();
  }

  // Returns the id of the (replicate) operation this context
  // refers to. This is only set _after_ Consensus::Replicate(context).
  OpId id() const {
    return DCHECK_NOTNULL(replicate_msg_.get())->id();
  }

  Status Commit(gscoped_ptr<CommitMsg> commit);

  void SetCommitCallback(const std::tr1::shared_ptr<FutureCallback>& commit_clbk) {
    commit_callback_ = commit_clbk;
  }

  const std::tr1::shared_ptr<FutureCallback>& commit_callback() {
    return commit_callback_;
  }

  void release_commit_callback(std::tr1::shared_ptr<FutureCallback>* ret) {
    ret->swap(commit_callback_);
    commit_callback_.reset();
  }

  void SetReplicaCommitContinuation(ConsensusCommitContinuation* continuation) {
    continuation_ = continuation;
  }

  // If a continuation was set, notifies it that the round has been replicated.
  void NotifyReplicationFinished(const Status& status);

 private:
  friend class RaftConsensusQuorumTest;
  Consensus* consensus_;
  // This round's replicate message.
  gscoped_ptr<ReplicateMsg> replicate_msg_;

  // The continuation that will be called once the transaction is
  // deemed committed/aborted by consensus.
  ConsensusCommitContinuation* continuation_;
  // The callback that is called once the commit phase of this consensus
  // round is finished.
  std::tr1::shared_ptr<FutureCallback> commit_callback_;
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
