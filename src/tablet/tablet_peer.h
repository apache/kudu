// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TABLET_PEER_H_
#define KUDU_TABLET_TABLET_PEER_H_

#include <string>

#include "consensus/consensus.h"
#include "consensus/log.h"
#include "consensus/opid_anchor_registry.h"
#include "tablet/tablet.h"
#include "tablet/transactions/transaction_tracker.h"
#include "util/countdown_latch.h"
#include "util/metrics.h"
#include "util/semaphore.h"

namespace kudu {

namespace metadata {
class TabletMetadata;
}

namespace rpc {
class Messenger;
}

namespace tablet {

class ChangeConfigTransactionState;
class TabletStatusPB;
class TabletStatusListener;

// A peer in a tablet quorum, which coordinates writes to tablets.
// Each time Write() is called this class appends a new entry to a replicated
// state machine through a consensus algorithm, which makes sure that other
// peers see the same updates in the same order. In addition to this, this
// class also splits the work and coordinates multi-threaded execution.
class TabletPeer : public consensus::ReplicaTransactionFactory {
 public:

  explicit TabletPeer(const metadata::TabletMetadata& meta);

  ~TabletPeer();

  // Initializes the TabletPeer, namely creating the Log and initializing
  // Consensus. 'local_peer' indicates whether this should serve the tablet
  // locally or if it collaborates with other replicas through consensus.
  // Takes a reference to 'opid_anchor_registry'
  Status Init(const std::tr1::shared_ptr<tablet::Tablet>& tablet,
              const scoped_refptr<server::Clock>& clock,
              const std::tr1::shared_ptr<rpc::Messenger>& messenger,
              const metadata::QuorumPeerPB& quorum_peer,
              gscoped_ptr<log::Log> log,
              log::OpIdAnchorRegistry* opid_anchor_registry,
              bool local_peer);

  // Starts the TabletPeer, making it available for Write()s. If this
  // TabletPeer is part of a quorum this will connect it to other peers
  // in the quorum.
  Status Start(const metadata::QuorumPB& quorum);

  // Shutdown this tablet peer.
  void Shutdown();

  // Check that the tablet is in a RUNNING state.
  Status CheckRunning() const;

  // Submits a write to a tablet and executes it asynchronously.
  // The caller is expected to build and pass a TrasactionContext that points
  // to the RPC WriteRequest, WriteResponse, RpcContext and to the tablet's
  // MvccManager.
  Status SubmitWrite(WriteTransactionState *tx_state);

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
  Status SubmitAlterSchema(AlterSchemaTransactionState *tx_state);

  // Called by the tablet service to start a change config transaction.
  //
  // The transaction contains all the information required to execute the
  // change config operation and send the response back.
  //
  // If the returned Status is OK, the response to the master will be sent
  // asynchronously.
  Status SubmitChangeConfig(ChangeConfigTransactionState* tx_state);

  void GetTabletStatusPB(TabletStatusPB* status_pb_out) const;

  // Used by consensus to create and start a new ReplicaTransaction.
  virtual Status StartReplicaTransaction(
      gscoped_ptr<consensus::ConsensusRound> context) OVERRIDE;

  consensus::Consensus* consensus() { return consensus_.get(); }

  Tablet* tablet() const {
    return tablet_.get();
  }

  const std::tr1::shared_ptr<Tablet>& shared_tablet() {
    return tablet_;
  }

  const metadata::TabletStatePB state() const {
    boost::lock_guard<simple_spinlock> lock(internal_state_lock_);
    return state_;
  }

  // Returns the current role of this peer as accepted by the last configuration
  // round, that is the role which is set in the tablet metadata's quorum.
  // If this peer hasn't yet finished the configuration round this will return
  // NON_PARTICIPANT.
  const metadata::QuorumPeerPB::Role role() const;

  TabletStatusListener* status_listener() const {
    return status_listener_.get();
  }

  // sets the tablet state to FAILED additionally setting the error to the provided
  // one.
  void SetFailed(const Status& error) {
    boost::lock_guard<simple_spinlock> lock(internal_state_lock_);
    state_ = metadata::FAILED;
    error_ = error;
  }

  // Returns the error that occurred, when state is FAILED.
  Status error() const {
    boost::lock_guard<simple_spinlock> lock(internal_state_lock_);
    return error_;
  }

  // Returns the minimum known OpId that is in-memory or in-flight.
  // Used for selection of log segments to delete during Log GC.
  void GetEarliestNeededOpId(consensus::OpId* op_id) const;

  server::Clock* clock() {
    return clock_.get();
  }

 private:
  friend class TabletPeerTest;
  FRIEND_TEST(TabletPeerTest, TestMRSAnchorPreventsLogGC);
  FRIEND_TEST(TabletPeerTest, TestDMSAnchorPreventsLogGC);
  FRIEND_TEST(TabletPeerTest, TestActiveTransactionPreventsLogGC);

  // Schedule the Log GC task to run in the executor.
  Status StartLogGCTask();

  // Task that runs Log GC on a periodic basis.
  Status RunLogGC();

  // Helper method for log messages. Returns empty string if tablet not assigned.
  std::string tablet_id() const;

  metadata::TabletStatePB state_;
  Status error_;
  scoped_refptr<log::OpIdAnchorRegistry> opid_anchor_registry_;
  TransactionTracker txn_tracker_;
  gscoped_ptr<log::Log> log_;
  std::tr1::shared_ptr<Tablet> tablet_;
  metadata::QuorumPeerPB quorum_peer_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;
  gscoped_ptr<consensus::Consensus> consensus_;
  gscoped_ptr<TabletStatusListener> status_listener_;
  simple_spinlock prepare_replicate_lock_;

  // lock protecting internal (usually rare) state changes.
  mutable simple_spinlock internal_state_lock_;

  // TODO move these executors to TabletServer when we support multiple tablets
  // IMPORTANT: correct execution of PrepareTask assumes that 'prepare_executor_'
  // is single-threaded, moving to a multi-tablet setup where multiple TabletPeers
  // use the same 'prepare_executor_' needs to enforce that, for a single
  // TabletPeer, PrepareTasks are executed *serially*.
  gscoped_ptr<TaskExecutor> prepare_executor_;
  gscoped_ptr<TaskExecutor> apply_executor_;
  gscoped_ptr<TaskExecutor> log_gc_executor_;
  CountDownLatch log_gc_shutdown_latch_;

  scoped_refptr<server::Clock> clock_;

  // Lock protecting updates to the configuration, stored in the tablet's
  // metadata.
  // ChangeConfigTransactions obtain this lock on prepare and release it on
  // apply.
  mutable Semaphore config_sem_;

  DISALLOW_COPY_AND_ASSIGN(TabletPeer);
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TABLET_PEER_H_ */
