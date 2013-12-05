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

  // Submits a write to a tablet and executes it asynchronously.
  // The caller is expected to build and pass a TrasactionContext that points
  // to the RPC WriteRequest, WriteResponse, RpcContext and to the tablet's
  // MvccManager.
  Status SubmitWrite(WriteTransactionContext *tx_ctx);

  // Called by the tablet service to start an alter schema transaction.
  //
  // The transaction contains all the information required to execute the
  // AlterSchema operation and send the response back.
  //
  // If the returned Status is OK, the response to the client will be sent
  // asynchronously. Otherwise the tablet service will have to send the response directly.
  //
  // The AlterSchema operation is taking the tablet component lock in exclusive mode
  // meaning that no other operation on the tablet can be executed while the
  // AlterSchema is in progress.
  Status SubmitAlterSchema(AlterSchemaTransactionContext *tx_ctx);

  consensus::Consensus* consensus() { return consensus_.get(); }

  Tablet* tablet() {
    return tablet_.get();
  }

  const std::tr1::shared_ptr<Tablet>& shared_tablet() {
    return tablet_;
  }

 private:
  std::tr1::shared_ptr<Tablet> tablet_;
  gscoped_ptr<log::Log> log_;
  gscoped_ptr<consensus::Consensus> consensus_;
  simple_spinlock prepare_replicate_lock_;

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
