// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDO_QUORUM_CONSENSUS_H_
#define KUDO_QUORUM_CONSENSUS_H_

#include <string>
#include <vector>

#include "gutil/ref_counted.h"
#include "consensus/consensus.pb.h"
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
class ConsensusContext;
class ReplicaUpdateContext;

struct ConsensusOptions {
  string tablet_id;
};

// The external interface for a consensus peer.
//
// Note: Even though Consensus points to Log, it needs to be destroyed
// after it. See Log class header comment for the reason why. On the other
// hand Consensus must be quiesced before closing the log, otherwise it
// will try to write to a destroyed/closed log.
// The order of these operations on shutdown must therefore be:
// 1 - quiesce Consensus
// 2 - close/destroy Log
// 3 - destroy Consensus
class Consensus {
 public:
  Consensus() : state_(kNotInitialized) {}

  // Initializes Consensus.
  // Note: Consensus does not own the Log and must be provided a fully built
  // one on startup.
  virtual Status Init(const metadata::QuorumPeerPB& peer,
                      const scoped_refptr<server::Clock>& clock,
                      log::Log* log) = 0;

  // Starts running the consensus algorithm.
  // The provided configuration is taken as a hint and may not be the
  // final configuration of the quorum. Specifically peer roles may
  // vary (e.g. if leader election was triggered) and even membership
  // may vary if the last known quorum configuration had different
  // members from the provided one.
  // The actual configuration used after Start() completes is returned
  // in 'running_quorum'.
  virtual Status Start(const metadata::QuorumPB& initial_quorum,
                       gscoped_ptr<metadata::QuorumPB>* running_quorum) = 0;

  // Stops running the consensus algorithm.
  virtual Status Shutdown() = 0;

  // Called by a quorum client to append an entry to the state machine.
  // Callbacks are provided so that ad-hoc actions can be taken on the
  // different phases of the two-phase commit process.
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
  //           <----+ ReplicateCallback.onReplicate()  |
  //             |                                     |
  //     4)   -->| Commit()                            |
  //             |                                     |
  //     5)      +--+                                  |
  //             |<-+ commit                           |
  //             |                                     |
  //     6)      +--+ CommitCallback.onCommit()        |
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
  // This method can only be called on the leader (i.e. is_leader() returns
  // true).
  // A successful call to Append yields a ConsensusContext that can be used
  // to call Commit.
  // The ConsensusContext out parameter will own all the data structures
  // required for a consensus round, such as the ReplicateMsg (and later on
  // the CommitMsg). ConsensusContext will also point to and increase the
  // reference count for the provided callbacks.
  virtual Status Append(
      gscoped_ptr<ReplicateMsg> entry,
      const std::tr1::shared_ptr<FutureCallback>& repl_callback,
      const std::tr1::shared_ptr<FutureCallback>& commit_callback,
      OpId* op_id, // TODO: Consider passing the whole TransactionContext.
      gscoped_ptr<ConsensusContext>* context) = 0;

  // Messages sent from LEADER to FOLLOWERS and LEARNERS to update their
  // state machines. This method can only be called on LEARNERs and
  // FOLLOWERs, i.e. is_leader() returns false.
  virtual Status Update(ReplicaUpdateContext* context) = 0;

  // Returns the number of participants that constitutes a majority for this
  // quorum, e.g. 1 for a quorum of 1 participant, 2 for a quorum of 2,3
  // participants, 3 for a quorum of 4,5 participants etc..
  //
  // The quorum will stop answering requests if num_participants() becomes
  // less than n_majority(). To maintain service the quorum can be Reconfigured
  // to add participants (to maintain/increase n_majority) or to remove
  // participants (decrease n_majority)
  virtual uint8_t n_majority() const = 0;

  // Returns the current number of participants of the quorum.
  virtual uint8_t num_participants() const = 0;

  // Whether this instance of a quorum participant is the current leader of
  // quorum.
  virtual bool is_leader() const = 0;

  // Returns the current quorum role of this instance.
  virtual metadata::QuorumPeerPB::Role role() const = 0;

  // Returns the current leader of the quorum.
  // NOTE: Returns a copy, thus should not be used in a tight loop.
  virtual metadata::QuorumPeerPB CurrentLeader() const = 0;

  // Returns the current configuration of the quorum.
  // NOTE: Returns a copy, thus should not be used in a tight loop.
  virtual metadata::QuorumPB CurrentQuorum() const = 0;

  // Returns the last opid assigned.
  virtual void GetLastOpId(consensus::OpId* op_id) const = 0;

  virtual ~Consensus() {}

 protected:

  friend class ConsensusContext;

  // Called by Consensus context to complete the commit of a consensus
  // round. CommitMsg pointer ownership will remain with ConsensusContext.
  virtual Status Commit(ConsensusContext* ctx, OperationPB* msg) = 0;

  enum State {
    kNotInitialized,
    kInitializing,
    kConfiguring,
    kRunning,
  };
  State state_;

 private:
  DISALLOW_COPY_AND_ASSIGN(Consensus);
};

// Context for a consensus round on the LEADER side, typically created as an
// out-parameter of Consensus::Append.
class ConsensusContext {
 public:
  ConsensusContext(Consensus* consensus,
                   gscoped_ptr<OperationPB> replicate_op,
                   const std::tr1::shared_ptr<FutureCallback>& replicate_callback,
                   const std::tr1::shared_ptr<FutureCallback>& commit_callback);

  OperationPB* replicate_op() {
    return replicate_op_.get();
  }

  Status Commit(gscoped_ptr<CommitMsg> commit);

  const std::tr1::shared_ptr<FutureCallback>& commit_callback() {
    return commit_callback_;
  }

  void release_commit_callback() {
    commit_callback_.reset();
  }

  OperationPB* release_commit_op() {
    return commit_op_.release();
  }

 private:
  Consensus* consensus_;
  gscoped_ptr<OperationPB> replicate_op_;
  gscoped_ptr<OperationPB> commit_op_;
  std::tr1::shared_ptr<FutureCallback> replicate_callback_;
  std::tr1::shared_ptr<FutureCallback> commit_callback_;
};

// A context for a consensus update request, i.e. a batch of ReplicateMsg and
// and CommitMsgs received from the LEADER. Usually wraps an RpcContext
// that gets called by the Respond*() methods but not by default so that
// consensus is not tied to RPC.
//
// Contrarily to a ConsensusContext, which refers to a single consensus round
// and lives LEADER side, a ConsensusUpdateContext may refer a series of
// of replicate/commit operations (as many as were batched together by the
// LEADER) and lives FOLLOWER/LEARNER side.
class ReplicaUpdateContext {
 public:
  ReplicaUpdateContext(const ConsensusRequestPB* request,
                         ConsensusResponsePB* response)
    : request_(request),
      response_(response) {
  }

  const ConsensusRequestPB* request() { return request_; }

  ConsensusResponsePB* response() { return response_; }

  // Sets the future that was assigned to the replica log task.
  void SetLogFuture(const std::tr1::shared_ptr<Future>& log_future) {
    log_future_ = log_future;
  }

  void AddPrepareFuture(const std::tr1::shared_ptr<Future>& prepare_future) {
    prepare_futures_.push_back(prepare_future);
  }

  void AddCommitFuture(const std::tr1::shared_ptr<Future>& commit_future) {
    commit_futures_.push_back(commit_future);
  }

  const std::tr1::shared_ptr<Future>& log_future() {
    return log_future_;
  }

  const vector<std::tr1::shared_ptr<Future> >& prepare_futures() {
    return prepare_futures_;
  }

  const vector<std::tr1::shared_ptr<Future> >& commit_futures() {
    return commit_futures_;
  }

  virtual void RespondSuccess() = 0;

  virtual void RespondFailure(const Status &status) = 0;

  virtual ~ReplicaUpdateContext() {}

 private:
  const ConsensusRequestPB* request_;
  ConsensusResponsePB* response_;
  std::tr1::shared_ptr<Future> log_future_;
  vector<std::tr1::shared_ptr<Future> > prepare_futures_;
  vector<std::tr1::shared_ptr<Future> > commit_futures_;


  DISALLOW_COPY_AND_ASSIGN(ReplicaUpdateContext);
};

} // namespace consensus
} // namespace kudu

#endif /* CONSENSUS_H_ */
