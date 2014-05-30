// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_TABLET_PEER_H_
#define KUDU_TABLET_TABLET_PEER_H_

#include <string>
#include <vector>

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
class LeaderTransactionDriver;
class ReplicaTransactionDriver;
class TabletPeer;
class TabletStatusPB;
class TabletStatusListener;
class TransactionDriver;

// A function def. for the callback that allows TabletPeer to notify
// that something has changed internally, e.g. a consensus role change.
typedef boost::function<void(TabletPeer*)> MarkDirtyCallback;

// A peer in a tablet quorum, which coordinates writes to tablets.
// Each time Write() is called this class appends a new entry to a replicated
// state machine through a consensus algorithm, which makes sure that other
// peers see the same updates in the same order. In addition to this, this
// class also splits the work and coordinates multi-threaded execution.
class TabletPeer : public consensus::ReplicaTransactionFactory {
 public:

  explicit TabletPeer(const metadata::TabletMetadata& meta,
                      MarkDirtyCallback mark_dirty_func);

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
              log::OpIdAnchorRegistry* opid_anchor_registry);

  // Starts the TabletPeer, making it available for Write()s. If this
  // TabletPeer is part of a quorum this will connect it to other peers
  // in the quorum.
  Status Start();

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
      gscoped_ptr<consensus::ConsensusRound> round) OVERRIDE;

  consensus::Consensus* consensus() { return consensus_.get(); }

  Tablet* tablet() const {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return tablet_.get();
  }

  const std::tr1::shared_ptr<Tablet>& shared_tablet() {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return tablet_;
  }

  const metadata::TabletStatePB state() const {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return state_;
  }

  // Returns the current role of this peer as accepted by the last configuration
  // round, that is the role which is set in the tablet metadata's quorum.
  // If a configuration has not yet been committed or if this peer is no longer
  // part of the quorum this will return NON_PARTICIPANT.
  const metadata::QuorumPeerPB::Role role() const;

  // Notifies the TabletPeer that the consensus state has changed.
  // Currently this is called to active the TsTabletManager callback that allows to
  // mark the tablet report as dirty, so that the master will eventually become
  // aware that the consensus role has changed for this peer.
  void ConsensusStateChanged();

  TabletStatusListener* status_listener() const {
    return status_listener_.get();
  }

  // sets the tablet state to FAILED additionally setting the error to the provided
  // one.
  void SetFailed(const Status& error) {
    boost::lock_guard<simple_spinlock> lock(lock_);
    state_ = metadata::FAILED;
    error_ = error;
  }

  // Returns the error that occurred, when state is FAILED.
  Status error() const {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return error_;
  }

  // Adds list of transactions in-flight at the time of the call to
  // 'out'. TransactionStatusPB objects are used to allow this method
  // to be used by both the web-UI and ts-cli.
  void GetInFlightTransactions(Transaction::TraceType trace_type,
                               std::vector<consensus::TransactionStatusPB>* out) const;

  // Returns the minimum known OpId that is in-memory or in-flight.
  // Used for selection of log segments to delete during Log GC.
  void GetEarliestNeededOpId(consensus::OpId* op_id) const;

  const scoped_refptr<log::OpIdAnchorRegistry>& opid_anchor_registry() const {
    return opid_anchor_registry_;
  }

  // Return a pointer to the Log.
  // The Log is owned by TabletPeer and will be destroyed with TabletPeer.
  log::Log* log() const {
    return log_.get();
  }

  server::Clock* clock() {
    return clock_.get();
  }

  // Returns the tablet_id of the tablet managed by this TabletPeer.
  // Returns the correct tablet_id even if the underlying tablet is not available
  // yet.
  std::string tablet_id() const { return tablet_id_; }

  void NewLeaderTransactionDriver(scoped_refptr<LeaderTransactionDriver>* driver);

  void NewReplicaTransactionDriver(scoped_refptr<ReplicaTransactionDriver>* driver);

 private:
  friend class TabletPeerTest;
  FRIEND_TEST(TabletPeerTest, TestMRSAnchorPreventsLogGC);
  FRIEND_TEST(TabletPeerTest, TestDMSAnchorPreventsLogGC);
  FRIEND_TEST(TabletPeerTest, TestActiveTransactionPreventsLogGC);

  // Schedule the Log GC task to run in the executor.
  Status StartLogGCTask();

  // Task that runs Log GC on a periodic basis.
  Status RunLogGC();

  const std::string tablet_id_;

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

  // lock protecting internal (usually rare) state changes as well as access to
  // smart pointers to data structures such as tablet_ and consensus_.
  mutable simple_spinlock lock_;

  // TODO move these executors to TabletServer when we support multiple tablets
  // IMPORTANT: correct execution of PrepareTask assumes that 'prepare_executor_'
  // is single-threaded, moving to a multi-tablet setup where multiple TabletPeers
  // use the same 'prepare_executor_' needs to enforce that, for a single
  // TabletPeer, PrepareTasks are executed *serially*.
  gscoped_ptr<TaskExecutor> prepare_executor_;

  // Different executors for leader/replica transactions. The leader apply executor
  // is multi-threaded while the replica one is single threaded.
  // TODO There is no reason for the replica not being able to also have a
  // multi-threaded executor. Even though that means that it's own logs will
  // differ from the leader's in terms of the ordering of commit messages
  // there is a guarantee that operations that touch the same rows will be
  // in the same order. This said, and although consensus has been designed
  // with multi-threaded replica applies in mind, this needs further testing
  // so we leave the replica apply executor as single-threaded, for now.
  gscoped_ptr<TaskExecutor> leader_apply_executor_;
  gscoped_ptr<TaskExecutor> replica_apply_executor_;


  gscoped_ptr<TaskExecutor> log_gc_executor_;
  CountDownLatch log_gc_shutdown_latch_;

  scoped_refptr<server::Clock> clock_;

  // Function to mark this TabletPeer's tablet as dirty in the TSTabletManager.
  MarkDirtyCallback mark_dirty_clbk_;

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
