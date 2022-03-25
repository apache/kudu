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
#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/consensus/log.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/op_order_verifier.h"
#include "kudu/tablet/ops/op.h"
#include "kudu/tablet/ops/op_tracker.h"
#include "kudu/tablet/ops/write_op.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"

namespace kudu {
class AlterTableTest;
class DnsResolver;
class MaintenanceManager;
class MaintenanceOp;
class MonoDelta;
class ThreadPool;
class ThreadPoolToken;
class TxnOpDispatcherITest;
class TxnOpDispatcherITest_DispatcherLifecycleMultipleReplicas_Test;
class TxnOpDispatcherITest_DuplicateTxnParticipantRegistration_Test;
class TxnOpDispatcherITest_ErrorInProcessingWriteOp_Test;
class TxnOpDispatcherITest_LifecycleBasic_Test;
class TxnOpDispatcherITest_NoPendingWriteOps_Test;
class TxnOpDispatcherITest_PreliminaryTasksTimeout_Test;

namespace consensus {
class ConsensusMetadataManager;
class OpStatusPB;
class TimeManager;
}

namespace clock {
class Clock;
}

namespace log {
class LogAnchorRegistry;
} // namespace log

namespace rpc {
class Messenger;
class ResultTracker;
} // namespace rpc

namespace tools {
struct RunnerContext;
Status CopyFromLocal(const RunnerContext& context);
} // namespace tools

namespace tablet {
class AlterSchemaOpState;
class OpDriver;
class ParticipantOpState;
class TabletStatusPB;
class TxnCoordinator;
class TxnCoordinatorFactory;

// Callback to run once the work to register a participant and start a
// transaction on the participant has completed (whether successful or not).
typedef std::function<void(const Status& status, tserver::TabletServerErrorPB::Code code)>
    RegisteredTxnCallback;

// A replica in a tablet consensus configuration, which coordinates writes to tablets.
// Each time Write() is called this class appends a new entry to a replicated
// state machine through a consensus algorithm, which makes sure that other
// peers see the same updates in the same order. In addition to this, this
// class also splits the work and coordinates multi-threaded execution.
class TabletReplica : public RefCountedThreadSafe<TabletReplica>,
                      public consensus::ConsensusRoundHandler {
 public:
  TabletReplica(scoped_refptr<TabletMetadata> meta,
                scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager,
                consensus::RaftPeerPB local_peer_pb,
                ThreadPool* apply_pool,
                ThreadPool* reload_txn_status_tablet_pool,
                TxnCoordinatorFactory* txn_coordinator_factory,
                consensus::MarkDirtyCallback cb);

  // Initializes RaftConsensus.
  // This must be called before publishing the instance to other threads.
  // If this fails, the TabletReplica instance remains in a NOT_INITIALIZED
  // state.
  Status Init(consensus::ServerContext server_ctx);

  // Starts the TabletReplica, making it available for Write()s. If this
  // TabletReplica is part of a consensus configuration this will connect it to other replicas
  // in the consensus configuration.
  Status Start(const consensus::ConsensusBootstrapInfo& bootstrap_info,
               std::shared_ptr<tablet::Tablet> tablet,
               clock::Clock* clock,
               std::shared_ptr<rpc::Messenger> messenger,
               scoped_refptr<rpc::ResultTracker> result_tracker,
               scoped_refptr<log::Log> log,
               ThreadPool* prepare_pool,
               DnsResolver* resolver);

  // Synchronously transition this replica to STOPPED state from any other
  // state. This also stops RaftConsensus. If a Stop() operation is already in
  // progress, blocks until that operation is complete.
  // See tablet/metadata.proto for a description of legal state transitions.
  void Stop();

  // Synchronously transition this replica to SHUTDOWN state from any other state.
  // See tablet/metadata.proto for a description of legal state transitions.
  //
  // If 'error_' has been set to a non-OK status, the final state will be
  // FAILED instead of SHUTDOWN.
  void Shutdown();

  // Check that the tablet is in a RUNNING state.
  Status CheckRunning() const;

  // Whether the tablet is already shutting down or shutdown.
  bool IsShuttingDown() const;

  // Wait until the tablet is in a RUNNING state or if there's a timeout.
  // TODO(jdcryans): have a way to wait for any state?
  Status WaitUntilConsensusRunning(const MonoDelta& timeout);

  // Submit a write operation 'op_state' attributed to a multi-row transaction.
  // This is similar to SubmitWrite(), but it's more complicated because it is
  // stateful. The necessary preliminary steps to complete before submitting the
  // operation via SubmitWrite() are: (1) register the tablet as a participant
  // in the corresponding transaction (2) issue BEGIN_TXN operation to the
  // replica. The 'scheduler' functor is used to schedule the activities
  // mentioned above.
  Status SubmitTxnWrite(
      std::unique_ptr<WriteOpState> op_state,
      const std::function<Status(int64_t txn_id, RegisteredTxnCallback cb)>& scheduler);

  // Unregister TxnWriteOpDispacher for the specified transaction identifier
  // 'txn_id'. If no pending write requests are accumulated by the dispatcher,
  // the dispatcher is unregistered immediately and this method returns
  // Status::OK. If any write request is pending, the dispatcher is marked to be
  // unregistered and this method returns Status::ServiceUnavailable(),
  // prompting the caller to try again later, unless 'abort_pending_ops' is set
  // to 'true'. If 'abort_pending_ops' is set to true, all pending requests are
  // responsed with Status::Aborted() status and the entry is removed.
  Status UnregisterTxnOpDispatcher(int64_t txn_id, bool abort_pending_ops);

  // Submits a write to a tablet and executes it asynchronously.
  // The caller is expected to build and pass a WriteOpState that points to the
  // RPC's WriteRequest, and WriteResponse.
  Status SubmitWrite(std::unique_ptr<WriteOpState> op_state);

  // Submits an op to update transaction participant state, executing it
  // asynchonously.
  Status SubmitTxnParticipantOp(std::unique_ptr<ParticipantOpState> op_state);

  // Called by the tablet service to start an alter schema op.
  //
  // The op contains all the information required to execute the
  // AlterSchema operation and send the response back.
  //
  // If the returned Status is OK, the response to the client will be sent
  // asynchronously. Otherwise the tablet service will have to send the response directly.
  //
  // The AlterSchema operation is taking the tablet component lock in exclusive mode
  // meaning that no other operation on the tablet can be executed while the
  // AlterSchema is in progress.
  Status SubmitAlterSchema(std::unique_ptr<AlterSchemaOpState> op_state);

  void GetTabletStatusPB(TabletStatusPB* status_pb_out) const;

  // Used by consensus to create and start a new ReplicaOp.
  virtual Status StartFollowerOp(
      const scoped_refptr<consensus::ConsensusRound>& round) override;

  // Used by consensus to notify the tablet replica that a consensus-only round
  // has finished, advancing MVCC safe time as appropriate.
  virtual void FinishConsensusOnlyRound(consensus::ConsensusRound* round) override;

  consensus::RaftConsensus* consensus() {
    std::lock_guard<simple_spinlock> lock(lock_);
    return consensus_.get();
  }

  std::shared_ptr<consensus::RaftConsensus> shared_consensus() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return consensus_;
  }

  Tablet* tablet() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return tablet_.get();
  }

  consensus::TimeManager* time_manager() const {
    return consensus_->time_manager();
  }

  std::shared_ptr<Tablet> shared_tablet() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return tablet_;
  }

  TabletStatePB state() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return state_;
  }

  const std::string& StateName() const;

  TabletDataState data_state() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return meta_->tablet_data_state();
  }

  // Returns the current Raft configuration.
  const consensus::RaftConfigPB RaftConfig() const;

  // Sets the tablet to a BOOTSTRAPPING state, indicating it is starting up.
  void SetBootstrapping();

  // Set a user-readable status message about the tablet. This may appear on
  // the Web UI, for example.
  void SetStatusMessage(std::string status);

  // Retrieve the last human-readable status of this tablet replica.
  std::string last_status() const;

  // Sets the error to the provided one.
  void SetError(const Status& error);

  // Returns the error that occurred, when state is FAILED.
  Status error() const {
    std::lock_guard<simple_spinlock> lock(lock_);
    return error_;
  }

  // Returns a human-readable string indicating the state of the tablet.
  // Typically this looks like "NOT_STARTED", "TABLET_DATA_COPYING",
  // etc. For use in places like the Web UI.
  std::string HumanReadableState() const;

  // Adds list of ops in-flight at the time of the call to 'out'.
  void GetInFlightOps(Op::TraceType trace_type,
                      std::vector<consensus::OpStatusPB>* out) const;

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

  clock::Clock* clock() const { return clock_; }

  const scoped_refptr<log::LogAnchorRegistry>& log_anchor_registry() const {
    return log_anchor_registry_;
  }

  const std::string& tablet_id() const { return meta_->tablet_id(); }

  // Convenience method to return the permanent_uuid of this peer.
  std::string permanent_uuid() const { return tablet_->metadata()->fs_manager()->uuid(); }

  Status NewLeaderOpDriver(std::unique_ptr<Op> op,
                           scoped_refptr<OpDriver>* driver);

  Status NewReplicaOpDriver(std::unique_ptr<Op> op,
                            scoped_refptr<OpDriver>* driver);

  // Tells the tablet's log to garbage collect.
  Status RunLogGC();

  // Register the maintenance ops associated with this peer's tablet, also invokes
  // Tablet::RegisterMaintenanceOps().
  void RegisterMaintenanceOps(MaintenanceManager* maint_mgr);

  // Unregister the maintenance ops associated with this replica's tablet.
  void UnregisterMaintenanceOps();

  // Cancels the maintenance ops associated with this replica's tablet.
  // Only to be used in tests.
  void CancelMaintenanceOpsForTests();

  // Stops further I/O on the replica.
  void MakeUnavailable(const Status& error);

  // Return pointer to the op tracker for this peer.
  const OpTracker* op_tracker() const { return &op_tracker_; }

  const scoped_refptr<TabletMetadata>& tablet_metadata() const {
    return meta_;
  }

  // Marks the tablet as dirty so that it's included in the next heartbeat.
  void MarkTabletDirty(const std::string& reason) {
    mark_dirty_clbk_(reason);
  }

  // Return the total on-disk size of this tablet replica, in bytes.
  size_t OnDiskSize() const;

  // Counts the number of live rows in this tablet replica.
  //
  // Returns a bad Status on failure.
  Status CountLiveRows(uint64_t* live_row_count) const;

  // Like CountLiveRows but returns 0 on failure.
  uint64_t CountLiveRowsNoFail() const;

  // Update the tablet stats.
  // When the replica's stats change and it's the LEADER, it is added to
  // the 'dirty_tablets'.
  void UpdateTabletStats(std::vector<std::string>* dirty_tablets);

  // Return the tablet stats.
  ReportedTabletStatsPB GetTabletStats() const;

  TxnCoordinator* txn_coordinator() const {
    return txn_coordinator_.get();
  }

  // Whether or not to run a new staleness transactions aborting task.
  // If the tablet is part of a transaction status table and is in
  // RUNNING state, register the task by increasing the transaction status
  // manager task counter. Once registered, the tablet will wait the task to
  // be executed before shutting down. More concretely, if this returns
  // 'true', callers must call DecreaseTxnCoordinatorTaskCounter().
  //
  // Return true if the caller should run the staleness task. Otherwise
  // return false.
  bool ShouldRunTxnCoordinatorStalenessTask();

  // Whether or not to run a new transaction status table reloading
  // metadata task. If the tablet is part of a transaction status table
  // and is not shutting down, register the task by increasing the
  // transaction status manager task counter if the tablet is not shutting
  // down. Similar to 'ShouldRunTxnCoordinatorStalenessTask' above. if
  // this returns 'true', callers must call DecreaseTxnCoordinatorTaskCounter().
  //
  // Return true if the caller should run the metadata reloading task
  // Otherwise return false.
  bool ShouldRunTxnCoordinatorStateChangedTask();

  // Decrease the task counter of the transaction status manager.
  void DecreaseTxnCoordinatorTaskCounter();

  // Submit ParticipantOpPB::BEGIN_TXN operation for the specified transaction.
  void BeginTxnParticipantOp(int64_t txn_id, RegisteredTxnCallback began_txn_cb);

 private:
  friend Status kudu::tools::CopyFromLocal(const kudu::tools::RunnerContext& context);
  friend class kudu::AlterTableTest;
  friend class RefCountedThreadSafe<TabletReplica>;
  friend class TabletReplicaTest;
  friend class TabletReplicaTestBase;
  friend class kudu::TxnOpDispatcherITest;
  FRIEND_TEST(TabletReplicaTest, TestActiveOpPreventsLogGC);
  FRIEND_TEST(TabletReplicaTest, TestDMSAnchorPreventsLogGC);
  FRIEND_TEST(TabletReplicaTest, TestMRSAnchorPreventsLogGC);
  FRIEND_TEST(kudu::TxnOpDispatcherITest, LifecycleBasic);

  // Only for CLI tools and tests.
  TabletReplica();

  // A class to properly dispatch transactional write operations arriving
  // with TabletServerService::Write() RPC for the specified tablet replica.
  // Before submitting the operations via TabletReplica::SubmitWrite(), it's
  // necessary to register the tablet as a participant in the transaction and
  // issue BEGIN_TXN operation for the target tablet. This class implements
  // the logic to schedule those preliminary tasks while accumulating incoming
  // write operations for the interval between the time when the very first
  // write operation for a transaction arrives and the time when the preliminary
  // tasks finish.
  class TxnOpDispatcher: public std::enable_shared_from_this<TxnOpDispatcher> {
   public:
    // The 'max_queue_size' parameter sets the limit of how many operations
    // this TxnOpDispatcher instance is allowed to buffer before starting to
    // reject new ones with Status::ServiceUnavailable error.
    TxnOpDispatcher(TabletReplica* replica,
                    size_t max_queue_size)
        : replica_(replica),
          max_queue_size_(max_queue_size),
          preliminary_tasks_completed_(false),
          unregistered_(false),
          inflight_status_(Status::OK()) {
    }

    // Dispatch specified write operation: either put it into the queue,
    // or submit it immediately via TabletReplica::SubmitWrite(), or reject the
    // operation. In the two former cases, returns Status::OK(); in the latter
    // case returns non-OK status correspondingly. The 'scheduler' function is
    // invoked to schedule preliminary tasks, if necessary.
    Status Dispatch(std::unique_ptr<WriteOpState> op,
                    const std::function<Status(int64_t txn_id,
                                               RegisteredTxnCallback cb)>& scheduler);

    // Submit all pending operations. Returns OK if all operations have been
    // submitted successfully, or 'inflight_status_' if any of those failed.
    Status Submit();

    // Invoke callbacks for every buffered operation with the 'status';
    // the 'status' must be a non-OK one.
    void Cancel(const Status& status, tserver::TabletServerErrorPB::Code code);

    // Mark the dispatcher as not accepting any write operations: this is to
    // eventually unregister the dispatcher for the corresponding transaction
    // (i.e. remove the element from the map of available dispatchers). In the
    // unlikely event of the presence of pending write operations, this method
    // returns Status::ServiceUnavailable().
    Status MarkUnregistered();

   private:
    FRIEND_TEST(kudu::TxnOpDispatcherITest, DispatcherLifecycleMultipleReplicas);
    FRIEND_TEST(kudu::TxnOpDispatcherITest, DuplicateTxnParticipantRegistration);
    FRIEND_TEST(kudu::TxnOpDispatcherITest, ErrorInProcessingWriteOp);
    FRIEND_TEST(kudu::TxnOpDispatcherITest, LifecycleBasic);
    FRIEND_TEST(kudu::TxnOpDispatcherITest, NoPendingWriteOps);
    FRIEND_TEST(kudu::TxnOpDispatcherITest, PreliminaryTasksTimeout);

    // Add the specified operation into the queue.
    Status EnqueueUnlocked(std::unique_ptr<WriteOpState> op);

    // Respond to the given write operations with the specified status.
    static Status RespondWithStatus(
        const Status& status,
        tserver::TabletServerErrorPB::Code code,
        std::deque<std::unique_ptr<WriteOpState>> ops);

    // Pointer to the parent TabletReplica instance which keeps this
    // TxnOpDispatcher instance in its 'txn_op_dispatchers_' map.
    TabletReplica* const replica_;

    // Maximum number of transactional write operation to buffer in the
    // 'ops_queue_' before completing all the preliminary tasks which are
    // required to start processing transactional write operations for the
    // tablet.
    const size_t max_queue_size_;

    // Protects the members below: preliminary_tasks_completed_, unregistered_,
    // inflight_status_, ops_queue_.
    mutable simple_spinlock lock_;

    // Whether the preliminary tasks are completed and this instance is
    // ready to submit incoming txn write operations directly via
    // TabletReplica::SubmitWrite().
    bool preliminary_tasks_completed_;

    // Whether this instance has been marked as unregistered and pending
    // destruction.
    bool unregistered_;

    // This field stores the first non-OK status (if any) returned by
    // TabletReplica::SubmitWrite() when submitting pending operations from
    // 'ops_queue_' upon completion of preliminary tasks.
    Status inflight_status_;

    // Queue to buffer txn write operations while the preliminary work of
    // registering the tablet as a participant in the transaction, etc. are
    // in progress.
    std::deque<std::unique_ptr<WriteOpState>> ops_queue_;
  };

  ~TabletReplica();

  // Wait until the TabletReplica is fully in STOPPED, SHUTDOWN, or FAILED
  // state.
  void WaitUntilStopped();

  // Wait until all on-going tasks for transaction status manager, if any,
  // to finish.
  void WaitUntilTxnCoordinatorTasksFinished();

  // Handle the state change accordingly if this tablet is a part of the
  // transaction status table.
  void TxnStatusReplicaStateChanged(const std::string& tablet_id,
                                    const std::string& reason);

  const std::string& LogPrefix() const {
    return meta_->LogPrefix();
  }

  // Transition to another state. Requires that the caller hold 'lock_' if the
  // object has already published to other threads. See tablet/metadata.proto
  // for state descriptions and legal state transitions.
  void set_state(TabletStatePB new_state);

  const scoped_refptr<TabletMetadata> meta_;
  const scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager_;

  const consensus::RaftPeerPB local_peer_pb_;
  scoped_refptr<log::LogAnchorRegistry> log_anchor_registry_; // Assigned in tablet_replica-test

  // Pool that executes apply tasks for ops. This is a multi-threaded pool,
  // constructor-injected by either the Master (for system tables) or the
  // Tablet server.
  ThreadPool* const apply_pool_;

  // Pool that executes txn status tablet in memory state reloading. This is
  // a multi-threaded pool, constructor-injected by the tablet server.
  ThreadPool* const reload_txn_status_tablet_pool_;

  // If this tablet is a part of the transaction status table, this is the
  // entity responsible for accepting and managing requests to coordinate
  // transactions.
  const std::unique_ptr<TxnCoordinator> txn_coordinator_;

  // Track the number of on-going tasks of the transaction status manager.
  int txn_coordinator_task_counter_;

  // Maps txn_id --> txn write op dispatcher. This map stores ref-counted
  // pointers instead of instances/references because an element can be removed
  // from the map (unregistered) while concurrently still receiving requests
  // (the latter will be rejected with Status::IllegalState). An alternative to
  // this scheme with ref-counted pointers would be
  //   (a) process _all_ transactional requests under the same giant lock
  //       (even for different transactions)
  //   (b) change the lifecycle of an entry in the txn_op_dispatcher_ map,
  //       introducing an alternative approach to clean-up obsolete elements
  //       from the map
  std::unordered_map<int64_t, std::shared_ptr<TxnOpDispatcher>> txn_op_dispatchers_;
  simple_spinlock txn_op_dispatchers_lock_; // protects 'txn_op_dispatchers_'

  // Function to mark this TabletReplica's tablet as dirty in the TSTabletManager.
  //
  // Must be called whenever cluster membership or leadership changes, or when
  // the tablet's schema changes.
  const consensus::MarkDirtyCallback mark_dirty_clbk_;

  TabletStatePB state_;
  Status error_;
  OpTracker op_tracker_;
  OpOrderVerifier op_order_verifier_;
  scoped_refptr<log::Log> log_;
  std::shared_ptr<Tablet> tablet_;
  std::shared_ptr<rpc::Messenger> messenger_;
  std::shared_ptr<consensus::RaftConsensus> consensus_;

  // Lock protecting state_, last_status_, as well as pointers to collaborating
  // classes such as tablet_, consensus_, and maintenance_ops_.
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

  // Token for serial task submission to the server-wide op prepare pool.
  std::unique_ptr<ThreadPoolToken> prepare_pool_token_;

  clock::Clock* clock_;

  // List of maintenance operations for the tablet that need information that only the peer
  // can provide.
  std::vector<MaintenanceOp*> maintenance_ops_;

  // The result tracker for writes.
  scoped_refptr<rpc::ResultTracker> result_tracker_;

  // Cached stats for the tablet replica.
  ReportedTabletStatsPB stats_pb_;

  // NOTE: it's important that this is the first member to be destructed. This
  // ensures we do not attempt to collect metrics while calling the destructor.
  FunctionGaugeDetacher metric_detacher_;

  DISALLOW_COPY_AND_ASSIGN(TabletReplica);
};

// A callback to wait for the in-flight ops to complete and to flush
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
