// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef KUDU_TABLET_TABLET_REPLICA_H_
#define KUDU_TABLET_TABLET_REPLICA_H_

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "kudu/consensus/log.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/transaction_order_verifier.h"
#include "kudu/tablet/transactions/transaction_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/semaphore.h"

namespace kudu {
class MaintenanceManager;
class MaintenanceOp;
class ThreadPool;

namespace log {
class LogAnchorRegistry;
} // namespace log

namespace rpc {
class Messenger;
class ResultTracker;
} // namespace rpc

namespace tserver {
class CatchUpServiceTest;
} // namespace tserver

namespace tablet {
class LeaderTransactionDriver;
class ReplicaTransactionDriver;
class TabletReplica;
class TabletStatusPB;
class TransactionDriver;

// A replica in a tablet consensus configuration, which coordinates writes to tablets.
// Each time Write() is called this class appends a new entry to a replicated
// state machine through a consensus algorithm, which makes sure that other
// peers see the same updates in the same order. In addition to this, this
// class also splits the work and coordinates multi-threaded execution.
class TabletReplica : public RefCountedThreadSafe<TabletReplica>,
                      public consensus::ReplicaTransactionFactory {
 public:
  TabletReplica(const scoped_refptr<TabletMetadata>& meta,
                consensus::RaftPeerPB local_peer_pb, ThreadPool* apply_pool,
                Callback<void(const std::string& reason)> mark_dirty_clbk);

  // Initializes the TabletReplica, namely creating the Log and initializing
  // RaftConsensus.
  Status Init(const std::shared_ptr<tablet::Tablet>& tablet,
              const scoped_refptr<server::Clock>& clock,
              const std::shared_ptr<rpc::Messenger>& messenger,
              const scoped_refptr<rpc::ResultTracker>& result_tracker,
              const scoped_refptr<log::Log>& log,
              const scoped_refptr<MetricEntity>& metric_entity,
              ThreadPool* raft_pool);

  // Starts the TabletReplica, making it available for Write()s. If this
  // TabletReplica is part of a consensus configuration this will connect it to other replicas
  // in the consensus configuration.
  Status Start(const consensus::ConsensusBootstrapInfo& info);

  // Shutdown this tablet replica.
  // If a shutdown is already in progress, blocks until that shutdown is complete.
  void Shutdown();

  // Check that the tablet is in a RUNNING state.
  Status CheckRunning() const;

  // Wait until the tablet is in a RUNNING state or if there's a timeout.
  // TODO have a way to wait for any state?
  Status WaitUntilConsensusRunning(const MonoDelta& timeout);

  // Submits a write to a tablet and executes it asynchronously.
  // The caller is expected to build and pass a TransactionContext that points
  // to the RPC WriteRequest, WriteResponse, RpcContext and to the tablet's
  // MvccManager.
  Status SubmitWrite(std::unique_ptr<WriteTransactionState> tx_state);

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
  Status SubmitAlterSchema(std::unique_ptr<AlterSchemaTransactionState> tx_state);

  void GetTabletStatusPB(TabletStatusPB* status_pb_out) const;

  // Used by consensus to create and start a new ReplicaTransaction.
  virtual Status StartReplicaTransaction(
      const scoped_refptr<consensus::ConsensusRound>& round) OVERRIDE;

  consensus::RaftConsensus* consensus() {
    std::lock_guard<simple_spinlock> lock(lock_);
    return consensus_.get();
  }

  scoped_refptr<consensus::RaftConsensus> shared_consensus() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return consensus_;
  }

  Tablet* tablet() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return tablet_.get();
  }

  scoped_refptr<consensus::TimeManager> time_manager() const {
    return consensus_->time_manager();
  }

  std::shared_ptr<Tablet> shared_tablet() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return tablet_;
  }

  const TabletStatePB state() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return state_;
  }

  // Returns the current Raft configuration.
  const consensus::RaftConfigPB RaftConfig() const;

  // If any peers in the consensus configuration lack permanent uuids, get them via an
  // RPC call and update.
  // TODO: move this to raft_consensus.h.
  Status UpdatePermanentUuids();

  // Sets the tablet to a BOOTSTRAPPING state, indicating it is starting up.
  void SetBootstrapping() {
    std::lock_guard<simple_spinlock> lock(lock_);
    CHECK_EQ(NOT_STARTED, state_);
    state_ = BOOTSTRAPPING;
  }

  // Set a user-readable status message about the tablet. This may appear on
  // the Web UI, for example.
  void SetStatusMessage(const std::string& status);

  // Retrieve the last human-readable status of this tablet replica.
  std::string last_status() const;

  // Sets the tablet state to FAILED additionally setting the error to the provided
  // one.
  void SetFailed(const Status& error);

  // Returns the error that occurred, when state is FAILED.
  Status error() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return error_;
  }

  // Returns a human-readable string indicating the state of the tablet.
  // Typically this looks like "NOT_STARTED", "TABLET_DATA_COPYING",
  // etc. For use in places like the Web UI.
  std::string HumanReadableState() const;

  // Adds list of transactions in-flight at the time of the call to 'out'.
  void GetInFlightTransactions(Transaction::TraceType trace_type,
                               std::vector<consensus::TransactionStatusPB>* out) const;

  // Returns the log indexes to be retained for durability and to catch up peers.
  // Used for selection of log segments to delete during Log GC.
  log::RetentionIndexes GetRetentionIndexes() const;

  // See Log::GetReplaySizeMap(...).
  //
  // Returns a non-ok status if the tablet isn't running.
  Status GetReplaySizeMap(std::map<int64_t, int64_t>* replay_size_map) const;

  // Returns the amount of bytes that would be GC'd if RunLogGC() was called.
  //
  // Returns a non-ok status if the tablet isn't running.
  Status GetGCableDataSize(int64_t* retention_size) const;

  // Return a pointer to the Log.
  // TabletReplica keeps a reference to Log after Init().
  log::Log* log() const {
    return log_.get();
  }

  server::Clock* clock() {
    return clock_.get();
  }

  const scoped_refptr<log::LogAnchorRegistry>& log_anchor_registry() const {
    return log_anchor_registry_;
  }

  // Returns the tablet_id of the tablet managed by this TabletReplica.
  // Returns the correct tablet_id even if the underlying tablet is not available
  // yet.
  const std::string& tablet_id() const { return tablet_id_; }

  // Convenience method to return the permanent_uuid of this peer.
  std::string permanent_uuid() const { return tablet_->metadata()->fs_manager()->uuid(); }

  Status NewLeaderTransactionDriver(gscoped_ptr<Transaction> transaction,
                                    scoped_refptr<TransactionDriver>* driver);

  Status NewReplicaTransactionDriver(gscoped_ptr<Transaction> transaction,
                                     scoped_refptr<TransactionDriver>* driver);

  // Tells the tablet's log to garbage collect.
  Status RunLogGC();

  // Register the maintenance ops associated with this peer's tablet, also invokes
  // Tablet::RegisterMaintenanceOps().
  void RegisterMaintenanceOps(MaintenanceManager* maintenance_manager);

  // Unregister the maintenance ops associated with this peer's tablet.
  // This method is not thread safe.
  void UnregisterMaintenanceOps();

  // Return pointer to the transaction tracker for this peer.
  const TransactionTracker* transaction_tracker() const { return &txn_tracker_; }

  const scoped_refptr<TabletMetadata>& tablet_metadata() const {
    return meta_;
  }

  // Marks the tablet as dirty so that it's included in the next heartbeat.
  void MarkTabletDirty(const std::string& reason) {
    mark_dirty_clbk_.Run(reason);
  }

 private:
  friend class RefCountedThreadSafe<TabletReplica>;
  friend class TabletReplicaTest;
  FRIEND_TEST(TabletReplicaTest, TestMRSAnchorPreventsLogGC);
  FRIEND_TEST(TabletReplicaTest, TestDMSAnchorPreventsLogGC);
  FRIEND_TEST(TabletReplicaTest, TestActiveTransactionPreventsLogGC);

  ~TabletReplica();

  // Wait until the TabletReplica is fully in SHUTDOWN state.
  void WaitUntilShutdown();

  // After bootstrap is complete and consensus is setup this initiates the transactions
  // that were not complete on bootstrap.
  // Not implemented yet. See .cc file.
  Status StartPendingTransactions(consensus::RaftPeerPB::Role my_role,
                                  const consensus::ConsensusBootstrapInfo& bootstrap_info);

  const scoped_refptr<TabletMetadata> meta_;

  const std::string tablet_id_;

  const consensus::RaftPeerPB local_peer_pb_;

  TabletStatePB state_;
  Status error_;
  TransactionTracker txn_tracker_;
  TransactionOrderVerifier txn_order_verifier_;
  scoped_refptr<log::Log> log_;
  std::shared_ptr<Tablet> tablet_;
  std::shared_ptr<rpc::Messenger> messenger_;
  scoped_refptr<consensus::RaftConsensus> consensus_;
  simple_spinlock prepare_replicate_lock_;

  // Lock protecting state_, last_status_, as well as smart pointers to collaborating
  // classes such as tablet_ and consensus_.
  mutable simple_spinlock lock_;

  // The human-readable last status of the tablet, displayed on the web page, command line
  // tools, etc.
  std::string last_status_;

  // Lock taken during Init/Shutdown which ensures that only a single thread
  // attempts to perform major lifecycle operations (Init/Shutdown) at once.
  // This must be acquired before acquiring lock_ if they are acquired together.
  // We don't just use lock_ since the lifecycle operations may take a while
  // and we'd like other threads to be able to quickly poll the state_ variable
  // during them in order to reject RPCs, etc.
  mutable simple_spinlock state_change_lock_;

  // IMPORTANT: correct execution of PrepareTask assumes that 'prepare_pool_'
  // is single-threaded, moving to a multi-tablet setup where multiple TabletReplicas
  // use the same 'prepare_pool_' needs to enforce that, for a single
  // TabletReplica, PrepareTasks are executed *serially*.
  // TODO move the prepare pool to TabletServer.
  gscoped_ptr<ThreadPool> prepare_pool_;

  // Pool that executes apply tasks for transactions. This is a multi-threaded
  // pool, constructor-injected by either the Master (for system tables) or
  // the Tablet server.
  ThreadPool* apply_pool_;

  scoped_refptr<server::Clock> clock_;

  scoped_refptr<log::LogAnchorRegistry> log_anchor_registry_;

  // Function to mark this TabletReplica's tablet as dirty in the TSTabletManager.
  //
  // Must be called whenever cluster membership or leadership changes, or when
  // the tablet's schema changes.
  Callback<void(const std::string& reason)> mark_dirty_clbk_;

  // List of maintenance operations for the tablet that need information that only the peer
  // can provide.
  std::vector<MaintenanceOp*> maintenance_ops_;

  // The result tracker for writes.
  scoped_refptr<rpc::ResultTracker> result_tracker_;

  DISALLOW_COPY_AND_ASSIGN(TabletReplica);
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

#endif /* KUDU_TABLET_TABLET_REPLICA_H_ */
