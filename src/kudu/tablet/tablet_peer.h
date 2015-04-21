// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#ifndef KUDU_TABLET_TABLET_PEER_H_
#define KUDU_TABLET_TABLET_PEER_H_

#include <map>
#include <string>
#include <vector>

#include "kudu/consensus/consensus.h"
#include "kudu/consensus/log.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/transaction_order_verifier.h"
#include "kudu/tablet/transactions/transaction_tracker.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/semaphore.h"

namespace kudu {

namespace log {
class LogAnchorRegistry;
}

namespace rpc {
class Messenger;
}

namespace tserver {
class CatchUpServiceTest;
}

class MaintenanceManager;
class MaintenanceOp;

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
class TabletPeer : public RefCountedThreadSafe<TabletPeer>,
                   public consensus::ReplicaTransactionFactory {
 public:
  typedef std::map<int64_t, int64_t> MaxIdxToSegmentSizeMap;

  TabletPeer(const scoped_refptr<TabletMetadata>& meta,
             ThreadPool* leader_apply_pool,
             ThreadPool* replica_apply_pool,
             MarkDirtyCallback mark_dirty_clbk);

  // Initializes the TabletPeer, namely creating the Log and initializing
  // Consensus. 'local_peer' indicates whether this should serve the tablet
  // locally or if it collaborates with other replicas through consensus.
  Status Init(const std::tr1::shared_ptr<tablet::Tablet>& tablet,
              const scoped_refptr<server::Clock>& clock,
              const std::tr1::shared_ptr<rpc::Messenger>& messenger,
              const scoped_refptr<log::Log>& log,
              const scoped_refptr<MetricEntity>& metric_entity);

  // Starts the TabletPeer, making it available for Write()s. If this
  // TabletPeer is part of a quorum this will connect it to other peers
  // in the quorum.
  Status Start(const consensus::ConsensusBootstrapInfo& info);

  // Shutdown this tablet peer.
  // Returns the previous state value.
  // This function is a no-op and makes no state change if the previous state
  // was QUIESCING or SHUTDOWN.
  TabletStatePB Shutdown();

  // Check that the tablet is in a RUNNING state.
  Status CheckRunning() const;

  // Wait until the tablet is in a RUNNING state or if there's a timeout.
  // TODO have a way to wait for any state?
  Status WaitUntilConsensusRunning(const MonoDelta& delta);

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

  const TabletStatePB state() const {
    boost::lock_guard<simple_spinlock> lock(lock_);
    return state_;
  }

  // Returns the current quorum configuration.
  const consensus::QuorumPB Quorum() const;

  // If any peers in the quorum lack permanent uuids, get them via an
  // RPC call and update.
  // TODO: move this to raft_consensus.h.
  Status UpdatePermanentUuids();

  // Notifies the TabletPeer that the consensus state has changed.
  // Currently this is called to activate the TsTabletManager callback that allows to
  // mark the tablet report as dirty, so that the master will eventually become
  // aware that the consensus role has changed for this peer.
  void ConsensusStateChanged(consensus::QuorumPeerPB::Role new_role);

  TabletStatusListener* status_listener() const {
    return status_listener_.get();
  }

  // sets the tablet state to FAILED additionally setting the error to the provided
  // one.
  void SetFailed(const Status& error) {
    boost::lock_guard<simple_spinlock> lock(lock_);
    state_ = FAILED;
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

  // Returns the minimum known log index that is in-memory or in-flight.
  // Used for selection of log segments to delete during Log GC.
  void GetEarliestNeededLogIndex(int64_t* log_index) const;

  // Returns a map of log index -> segment size, of all the segments that currently cannot be GCed
  // because in-memory structures have anchors in them.
  //
  // Returns a non-ok status if the tablet isn't running.
  Status GetMaxIndexesToSegmentSizeMap(MaxIdxToSegmentSizeMap* idx_size_map) const;

  // Returns the amount of bytes that would be GC'd if RunLogGC() was called.
  //
  // Returns a non-ok status if the tablet isn't running.
  Status GetGCableDataSize(int64_t* retention_size) const;

  // Return a pointer to the Log.
  // The Log is owned by TabletPeer and will be destroyed with TabletPeer.
  log::Log* log() const {
    return log_.get();
  }

  server::Clock* clock() {
    return clock_.get();
  }

  const scoped_refptr<log::LogAnchorRegistry>& log_anchor_registry() const {
    return log_anchor_registry_;
  }

  // Returns the tablet_id of the tablet managed by this TabletPeer.
  // Returns the correct tablet_id even if the underlying tablet is not available
  // yet.
  std::string tablet_id() const { return tablet_id_; }

  // Convenience method to return the permanent_uuid of this peer.
  std::string permanent_uuid() const { return tablet_->metadata()->fs_manager()->uuid(); }

  void NewLeaderTransactionDriver(gscoped_ptr<Transaction> transaction,
                                  scoped_refptr<TransactionDriver>* driver);

  void NewReplicaTransactionDriver(gscoped_ptr<Transaction> transaction,
                                   scoped_refptr<TransactionDriver>* driver);

  // Tells the tablet's log to garbage collect.
  Status RunLogGC();

  // Register the maintenance ops associated with this peer's tablet, also invokes
  // Tablet::RegisterMaintenanceOps().
  void RegisterMaintenanceOps(MaintenanceManager* maintenance_manager);

  // Unregister the maintenance ops associated with this peer's tablet.
  // This method is not thread safe.
  void UnregisterMaintenanceOps();

 private:
  friend class RefCountedThreadSafe<TabletPeer>;
  friend class TabletPeerTest;
  FRIEND_TEST(TabletPeerTest, TestMRSAnchorPreventsLogGC);
  FRIEND_TEST(TabletPeerTest, TestDMSAnchorPreventsLogGC);
  FRIEND_TEST(TabletPeerTest, TestActiveTransactionPreventsLogGC);

  ~TabletPeer();

  // After bootstrap is complete and consensus is setup this initiates the transactions
  // that were not complete on bootstrap.
  // Not implemented yet. See .cc file.
  Status StartPendingTransactions(consensus::QuorumPeerPB::Role my_role,
                                  const consensus::ConsensusBootstrapInfo& bootstrap_info);

  scoped_refptr<TabletMetadata> meta_;

  const std::string tablet_id_;

  TabletStatePB state_;
  Status error_;
  TransactionTracker txn_tracker_;
  TransactionOrderVerifier txn_order_verifier_;
  scoped_refptr<log::Log> log_;
  std::tr1::shared_ptr<Tablet> tablet_;
  std::tr1::shared_ptr<rpc::Messenger> messenger_;
  scoped_refptr<consensus::Consensus> consensus_;
  gscoped_ptr<TabletStatusListener> status_listener_;
  simple_spinlock prepare_replicate_lock_;

  // lock protecting internal (usually rare) state changes as well as access to
  // smart pointers to data structures such as tablet_ and consensus_.
  mutable simple_spinlock lock_;

  // IMPORTANT: correct execution of PrepareTask assumes that 'prepare_pool_'
  // is single-threaded, moving to a multi-tablet setup where multiple TabletPeers
  // use the same 'prepare_pool_' needs to enforce that, for a single
  // TabletPeer, PrepareTasks are executed *serially*.
  // TODO move the prepare pool to TabletServer.
  gscoped_ptr<ThreadPool> prepare_pool_;

  // Pool for apply tasks for leader and replica
  // transactions. These are multi-threaded pools,
  // constructor-injected by either the Master (for system tables) or
  // the Tablet server.
  ThreadPool* leader_apply_pool_;
  ThreadPool* replica_apply_pool_;

  // Latch that goes down to 0 when the tablet is in RUNNING state.
  CountDownLatch consensus_ready_latch_;

  scoped_refptr<server::Clock> clock_;

  scoped_refptr<log::LogAnchorRegistry> log_anchor_registry_;

  // Function to mark this TabletPeer's tablet as dirty in the TSTabletManager.
  MarkDirtyCallback mark_dirty_clbk_;

  // Lock protecting updates to the configuration, stored in the tablet's
  // metadata.
  // ChangeConfigTransactions obtain this lock on prepare and release it on
  // apply.
  mutable Semaphore config_sem_;

  // List of maintenance operations for the tablet that need information that only the peer
  // can provide.
  std::vector<MaintenanceOp*> maintenance_ops_;

  DISALLOW_COPY_AND_ASSIGN(TabletPeer);
};

// A callback to wait for the in-flight transactions to complete and to flush
// the Log when they do.
// Tablet is passed as a raw pointer as this callback is set in TabletMetadata and
// were we to keep the tablet as a shared_ptr a circular dependency would occur:
// callback->tablet->metadata->callback. Since the tablet indirectly owns this
// callback we know that is must still be alive when it fires.
class FlushInflightsToLogCallback : public RefCountedThreadSafe<FlushInflightsToLogCallback> {
 public:
  FlushInflightsToLogCallback(Tablet* tablet,
                              const scoped_refptr<log::Log>& log)
   : tablet_(tablet),
     log_(log) {}

  Status WaitForInflightsAndFlushLog();

 private:
  Tablet* tablet_;
  scoped_refptr<log::Log> log_;
};


}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_TABLET_PEER_H_ */
