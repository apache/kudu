// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDO_QUORUM_CONSENSUS_H_
#define KUDO_QUORUM_CONSENSUS_H_

#include "google/protobuf/message_lite.h"

#include "consensus/consensus.pb.h"
#include "util/status.h"
#include "util/task_executor.h"

namespace kudu {
namespace consensus {

// forward declarations
class ConsensusContext;

struct ConsensusOptions {};

// The external interface for consensus.
class Consensus {
 public:

  Consensus() {}

  // Starts running the consensus algorithm.
  virtual Status Start() = 0;

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
      gscoped_ptr<ConsensusContext>* context) = 0;

  // Appends a local commit to the state machine.
  // This function is required as different nodes have independent physical
  // layers and therefore need to sometimes issue "local" commit messages that
  // change only the local state and not the coordinated state machine.
  //
  // A successful call will yield a Future which can be used to Wait() until
  // the commit is done or to add callbacks. This is important as LocalCommit()
  // does not take ownership of the passed 'commit_msg'.
  virtual Status LocalCommit(CommitMsg* commit_msg,
                             std::tr1::shared_ptr<kudu::Future>* commit_future) = 0;

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

  // Returns the current leader of the quorum.
  virtual const QuorumPeerPB &current_leader() const = 0;

  virtual ~Consensus() {}

 protected:
  DISALLOW_COPY_AND_ASSIGN(Consensus);

  friend class ConsensusContext;

  // Called by Consensus context to complete the commit of a consensus
  // round. CommitMsg pointer ownership will remain with ConsensusContext.
  virtual Status Commit(ConsensusContext* ctx, CommitMsg *msg) = 0;
};

// Context for a consensus round, typically created as an out-parameter of
// Consensus::Append.
class ConsensusContext {
 public:
  ConsensusContext(Consensus* consensus,
                   gscoped_ptr<ReplicateMsg> replicate_msg,
                   const std::tr1::shared_ptr<FutureCallback>& replicate_callback,
                   const std::tr1::shared_ptr<FutureCallback>& commit_callback);

  ReplicateMsg* replicate_msg() {
    return replicate_msg_.get();
  }

  void Commit(gscoped_ptr<CommitMsg> commit);

  const std::tr1::shared_ptr<FutureCallback>& commit_callback() {
    return commit_callback_;
  }

  void release_commit_callback() {
    commit_callback_.reset();
  }

 private:
  Consensus* consensus_;
  gscoped_ptr<ReplicateMsg> replicate_msg_;
  gscoped_ptr<CommitMsg> commit_msg_;
  std::tr1::shared_ptr<FutureCallback> replicate_callback_;
  std::tr1::shared_ptr<FutureCallback> commit_callback_;
};

} // namespace consensus
} // namespace kudu

#endif /* CONSENSUS_H_ */
