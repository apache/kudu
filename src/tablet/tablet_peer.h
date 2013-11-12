// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TABLET_PEER_H_
#define KUDU_TABLET_TABLET_PEER_H_

#include "tablet/tablet.h"
#include "consensus/log.h"
#include "consensus/consensus.h"
#include "util/metrics.h"

namespace kudu {
namespace tablet {

// A peer in a tablet quorum, which coordinates writes to tablets.
// Each time Write() is called this class appends a new entry to a replicated
// state machine through a consensus algorithm, which makes sure that other
// peers see the same updates in the same order. In addition to this, this
// class also splits the work and coordinates multi-threaded execution.
class TabletPeer {
 public:

  TabletPeer(const std::tr1::shared_ptr<tablet::Tablet>& tablet,
             gscoped_ptr<log::Log> log);

  // Initializes the TabletPeer, namely creating the Log and initializing
  // Consensus.
  Status Init();

  // Starts the TabletPeer, making it available for Write()s. If this
  // TabletPeer is part of a quorum this will connect it to other peers
  // in the quorum.
  Status Start();

  // Shutdown this tablet peer.
  Status Shutdown();

  // Called by the tablet service to start a transaction.
  //
  // The caller is expected to build and pass a TrasactionContext that points
  // to the RPC WriteRequest, WriteResponse, RpcContext and to the tablet's
  // MvccManager.
  //
  // Transaction execution is illustrated in the next diagram. (Further
  // illustration of the inner workings of the consensus system can be found
  // in consensus/consensus.h).
  //
  //                                 + TabletPeer::Write()
  //                                 |
  //                                 |
  //                                 |
  //                         +-------+-------+
  //                         |       |       |
  // 1) Consensus::Append()  |       v       | 2) PrepareTask::Run()
  //                         |   (returns)   |
  //                         v               v
  //                  +------------------------------+
  //               3) | ApplyOnReplicateAndPrepareCB |
  //                  |------------------------------|
  //                  | - continues once both        |
  //                  |   phases have finished       |
  //                  +--------------+---------------+
  //                                 |
  //                                 | Submits ApplyTask()
  //                                 v
  //                     +-------------------------+
  //                  4) |  ApplyTask              |
  //                     |-------------------------|
  //                     | - applies transaction   |
  //                     +-------------------------+
  //                                 |
  //                                 | 5) Calls Consensus::Commit()
  //                                 v
  //                                 + Send Response to client.
  //
  // 1) TabletPeer creates the ReplicateMsg, the Apply/Prepare callback,
  //    and calls Append() to Consensus.
  //
  // 2) At the same time TabletPeer submits the PrepareTask which will
  //    parse the request and acquire the row locks. Both Append() and Prepare()
  //    are submitted under a mutex so that we are sure they are executed in
  //    the same order, i.e. that if a TabletPeer receives transactions A, B
  //    it executes Append(A) before Append(B) *and* Prepare(A) before Prepare(B).
  //
  // 3) When Append() completes, having replicated and persisted the client's
  //    request, the Append/Prepare callback is called. AppendCallback and
  //    PrepareCallback are actually one and the same i.e. the same callback
  //    instance is called when both actions complete. When the callback's
  //    OnSuccess() method is called twice (independently of who finishes first)
  //    the apply task is triggered.
  //
  // 4) When ApplyTask starts execution the TransactionContext was
  //    passed all the PreparedRowWrites (each containing a row lock) and the
  //    'component_lock'. The ApplyTask starts the mvcc transaction and calls
  //    Tablet::InsertUnlocked/Tablet::MutateUnlocked with each of the
  //    PreparedRowWrites. The ApplyTask keeps track of any single row errors
  //    that might have occurred while inserting/mutating and sets those in
  //    WriteResponse. TransactionContext is passed with each insert/mutate
  //    to keep track of which in-memory stores were mutated.
  //    After all the inserts/mutates are performed ApplyTask releases all
  //    locks (see 'Implementation Techniques for Main Memory Database Systems',
  //    DeWitt et. al.). It then readies the CommitMsg with the TXResultPB in
  //    transaction context and calls ConsensusContext::Commit() which will
  //    in turn trigger a commit of the consensus system.
  //
  // 5) After the consensus system deems the CommitMsg committed (which might
  //    have different requirements depending on the consensus algorithm) the
  //    Commit callback is called and the transaction is considered completed,
  //    the mvcc transaction committed (making the updates visible to other
  //    transactions) and the transaction's resources released.
  //
  Status Write(WriteTransactionContext *tx_ctx);

  consensus::Consensus* consensus() { return consensus_.get(); }

  Tablet* tablet() {
    return tablet_.get();
  }

  const std::tr1::shared_ptr<Tablet>& shared_tablet() {
    return tablet_;
  }

 private:
  Status ExecuteTransaction(TransactionContext *tx_ctx,
                            gscoped_ptr<consensus::ReplicateMsg> replicate_msg,
                            const shared_ptr<Task>& prepare_task,
                            const shared_ptr<FutureCallback>& apply_clbk,
                            const shared_ptr<FutureCallback>& commit_clbk);

  std::tr1::shared_ptr<Tablet> tablet_;
  gscoped_ptr<log::Log> log_;
  gscoped_ptr<consensus::Consensus> consensus_;
  typedef simple_spinlock LockType;
  LockType lock_;

  // TODO move these executors to TabletServer when we support multiple tablets
  // IMPORTANT: correct execution of PrepareTask assumes that 'prepare_executor_'
  // is single-threaded, moving to a multi-tablet setup where multiple TabletPeers
  // use the same 'prepare_executor_' needs to enforce that, for a single
  // TabletPeer, PrepareTasks are executed *serially*.
  gscoped_ptr<TaskExecutor> prepare_executor_;
  gscoped_ptr<TaskExecutor> apply_executor_;

  DISALLOW_COPY_AND_ASSIGN(TabletPeer);
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TABLET_PEER_H_ */
