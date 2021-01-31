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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gtest/gtest_prod.h>

#include "kudu/common/timestamp.h"
#include "kudu/common/txn_id.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/txn_coordinator.h"
#include "kudu/transactions/txn_status_entry.h"
#include "kudu/transactions/txn_status_tablet.h"
#include "kudu/util/locks.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace kudu {
class ThreadPool;

class MonoDelta;
namespace rpc {
class RpcContext;
}  // namespace rpc

namespace tablet {
class TabletReplica;
} // namespace tablet

namespace tserver {
class TabletServerErrorPB;
} // namespace tserver

namespace transactions {
class TxnStatusEntryPB;
class TxnStatusManager;
class TxnSystemClient;
class TxnSystemClientInitializer;

// A handle for background tasks associated with a single transaction.
class CommitTasks : public RefCountedThreadSafe<CommitTasks> {
 public:
  CommitTasks(TxnId txn_id,
              std::vector<std::string> participant_ids,
              TxnSystemClient* txn_client,
              ThreadPool* commit_pool,
              TxnStatusManager* txn_status_manager)
     : txn_id_(std::move(txn_id)),
       participant_ids_(std::move(participant_ids)),
       txn_client_(txn_client),
       commit_pool_(commit_pool),
       txn_status_manager_(txn_status_manager),
       ops_in_flight_(participant_ids_.size()),
       begin_commit_timestamps_(participant_ids_.size(), Timestamp::kInvalidTimestamp),
       stop_task_(false) {
  }

  // Asynchronously sends a BEGIN_COMMIT participant op to each participant,
  // and on completion, schedules FINALIZE_COMMIT ops to be sent.
  //
  // If there are no participants for the given transaction, a commit record is
  // written to the tablet.
  void BeginCommitAsync();

  // Asynchronously sends a BEGIN_COMMIT participant op to the participant at
  // the given index. If this was the last one to complete, schedules
  // FINALIZE_COMMIT ops to be sent.
  void BeginCommitAsyncTask(int participant_idx);

  // Asynchronously sends a FINALIZE_COMMIT participant op to the each
  // participant in the transaction, and upon completion, schedules a commit
  // record to be written to the tablet.
  void FinalizeCommitAsyncTask(int participant_idx, const Timestamp& commit_timestamp);

  // Asynchronously sends a FINALIZE_COMMIT participant op to the participant
  // at the given index. If this was the last one to complete, schedules a
  // commit record with the given timestamp to be written to the tablet.
  void FinalizeCommitAsync(Timestamp commit_timestamp);

  // Asynchronously sends a ABORT_TXN participant op to each participant in the
  // transaction, and upon completion, schedules an abort record to be written
  // to the tablet.
  void AbortTxnAsync();

  // Asynchronously sends an ABORT_TXN participant op to the participant at the
  // given index. If this was the last one to complete, schedules an abort
  // record to be written to the tablet.
  void AbortTxnAsyncTask(int participant_idx);

  // Schedule calls to the TxnStatusManager to be made on the commit pool.
  // NOTE: these may be called on reactor threads and thus must not
  // synchronously do any IO.
  void ScheduleFinalizeCommitWrite(Timestamp commit_timestamp);
  void ScheduleAbortTxnWrite();

  // Stops further tasks from being run. Once called calls to the above methods
  // should effectively no-op.
  void stop() {
    stop_task_ = true;
  }

 private:
  friend class RefCountedThreadSafe<CommitTasks>;
  ~CommitTasks() = default;

  // Returns true if the task has been stopped or if the TxnStatusManager is
  // being shut down. Cleans up the task state if the caller was the last op in
  // flight.
  //
  // This is useful in participant op callbacks to determine whether the
  // callback can return immediately without doing work.
  bool IsShuttingDownCleanupIfLastOp();

  // Returns true if the task has been stopped or if the TxnStatusManager is
  // being shut down. Cleans up the task state, expecting that the caller was
  // the last op in flight.
  //
  // This is useful when there are no ops in flight and the caller is about to
  // schedule work, and needs to determine if it should opt out because of the
  // shutdown.
  bool IsShuttingDownCleanup() const;

  // The ID of the transaction being committed.
  const TxnId txn_id_;

  // Tablet IDs of the participants of this transaction.
  const std::vector<std::string> participant_ids_;

  // Client with which to send requests.
  TxnSystemClient* txn_client_;

  // Threadpool on which to schedule tasks.
  ThreadPool* commit_pool_;

  // The TxnStatusManager that created these tasks.
  TxnStatusManager* txn_status_manager_;

  // The number of participant op RPCs in flight.
  std::atomic<int> ops_in_flight_;

  // The timestamps used to replica each participant's BEGIN_COMMIT ops.
  std::vector<Timestamp> begin_commit_timestamps_;

  // The commit timestamp for this transaction.
  Timestamp commit_timestamp_;

  // Whether the task should stop executing, e.g. since an IllegalState error
  // was observed on a participant, or because the TxnStatusManager changed
  // leadership.
  std::atomic<bool> stop_task_;
};

// Maps the transaction ID to the corresponding TransactionEntry.
typedef std::unordered_map<int64_t, scoped_refptr<TransactionEntry>> TransactionsMap;

// Visitor used to iterate over and load into memory the existing state from a
// status tablet.
class TxnStatusManagerBuildingVisitor : public TransactionsVisitor {
 public:
  TxnStatusManagerBuildingVisitor();
  ~TxnStatusManagerBuildingVisitor() = default;
  // Builds a TransactionEntry for the given metadata and keeps track of it in
  // txns_by_id_. This is not thread-safe -- callers should ensure only a
  // single thread calls it at once.
  void VisitTransactionEntries(int64_t txn_id, TxnStatusEntryPB status_entry_pb,
                               std::vector<ParticipantIdAndPB> participants) override;

  // Releases the transactions map to the caller. Should only be called once
  // per call to VisitTransactionEntries().
  void Release(int64_t* highest_txn_id, TransactionsMap* txns_by_id);
 private:
  int64_t highest_txn_id_;
  TransactionsMap txns_by_id_;
};

// Manages ongoing transactions and participants therein, backed by an
// underlying tablet.
class TxnStatusManager final : public tablet::TxnCoordinator {
 public:
  TxnStatusManager(tablet::TabletReplica* tablet_replica,
                   TxnSystemClientInitializer* client_initializer,
                   ThreadPool* commit_pool);
  ~TxnStatusManager();

  // Scoped "shared lock" to serialize replica leader elections.
  //
  // While in scope, blocks the transaction status manager in the event that
  // it becomes the leader of its Raft configuration and needs to reload its
  // persistent metadata. Once destroyed, the transaction status manager is
  // unblocked.
  class ScopedLeaderSharedLock {
   public:
    // Creates a new shared lock, trying to acquire the transaction status
    // manager's leader_lock_ for reading in the process. If acquired, the
    // lock is released when this object is destroyed.
    //
    // The object pointed by the 'txn_coordinator' parameter must outlive this
    // object. 'txn_coordinator' must be a 'TxnStatusManager*' because of the
    // downcast in the initialization.
    explicit ScopedLeaderSharedLock(TxnCoordinator* txn_coordinator);

    // First non-OK status of the transaction status manager, adhering to
    // the checking order of replica_status_ first and then leader_status_.
    const Status& first_failed_status() const {
      RETURN_NOT_OK(replica_status_);
      return leader_status_;
    }

    // Check that the transaction status manager is initialized and that it is
    // the leader of its Raft configuration. Initialization status takes precedence
    // over leadership status.
    //
    // If not initialized or if not the leader, writes the corresponding error
    // to 'resp', responds to 'rpc', and returns false.
    template<typename RespClass>
    bool CheckIsInitializedAndIsLeaderOrRespond(RespClass* resp, rpc::RpcContext* rpc);

   private:
    TxnStatusManager* txn_status_manager_;
    shared_lock<RWMutex> leader_shared_lock_;

    // General status of the transaction status manager. If not OK (e.g. the tablet
    // replica is still being initialized), all operations are illegal.
    Status replica_status_;

    // Leadership status of the transaction status manager. If not OK, the
    // transaction status manager is not the leader.
    Status leader_status_;
    int64_t initial_term_;

    DISALLOW_COPY_AND_ASSIGN(ScopedLeaderSharedLock);
  };

  void Shutdown() override;
  void PrepareLeadershipTask() override;

  // Writes an entry to the status tablet and creates a transaction in memory.
  // Returns an error if a higher transaction ID has already been attempted
  // (even if that attempt failed), which helps ensure that at most one call to
  // this method will succeed for a given transaction ID. The
  // 'highest_seen_txn_id' output parameter, if not null, is populated in both
  // success and failure cases, except for the case when returning
  // Status::ServiceUnavailable() due to not-yet-loaded data from the
  // backing transaction status tablet.
  //
  // TODO(awong): consider computing the next available transaction ID in this
  // partition and using it in case this transaction is already used, or having
  // callers forward a request for the next-highest transaction ID.
  Status BeginTransaction(int64_t txn_id,
                          const std::string& user,
                          int64_t* highest_seen_txn_id,
                          tserver::TabletServerErrorPB* ts_error) override;

  // Begins committing the given transaction, returning an error if the
  // transaction doesn't exist, isn't open, or isn't owned by the given user.
  Status BeginCommitTransaction(int64_t txn_id, const std::string& user,
                                tserver::TabletServerErrorPB* ts_error) override;

  // Finalizes the commit of the transaction, returning an error if the
  // transaction isn't in an appropraite state.
  //
  // Unlike the other transaction life-cycle calls, this isn't user-initiated,
  // so it doesn't take a user.
  Status FinalizeCommitTransaction(int64_t txn_id, Timestamp commit_timestamp,
                                   tserver::TabletServerErrorPB* ts_error) override;

  // Begins aborting the given transaction, returning an error if the
  // transaction doesn't exist, is committed or not yet opened, or isn't owned
  // by the given user.
  Status AbortTransaction(int64_t txn_id, const std::string& user,
                          tserver::TabletServerErrorPB* ts_error) override;

  // Writes a record to the TxnStatusManager indicating the given transaction
  // has been successfully aborted.
  Status FinalizeAbortTransaction(int64_t txn_id);

  // Retrieves the status of the specified transaction, returning an error if
  // the transaction doesn't exist or isn't owned by the specified user.
  Status GetTransactionStatus(int64_t txn_id,
                              const std::string& user,
                              transactions::TxnStatusEntryPB* txn_status,
                              tserver::TabletServerErrorPB* ts_error) override;

  // Processes keep-alive heartbeat for the specified transaction.
  Status KeepTransactionAlive(int64_t txn_id,
                              const std::string& user,
                              tserver::TabletServerErrorPB* ts_error) override;

  // Creates an in-memory participant, writes an entry to the status table, and
  // attaches the in-memory participant to the transaction.
  //
  // If the transaction is open, it is ensured to be active for the duration of
  // this call. Returns an error if the given transaction isn't open.
  Status RegisterParticipant(int64_t txn_id, const std::string& tablet_id,
                             const std::string& user,
                             tserver::TabletServerErrorPB* ts_error) override;

  // Abort transactions which are still in non-terminal state but haven't
  // received keep-alive updates (see KeepTransactionAlive()) for a long time.
  void AbortStaleTransactions() override;

  int64_t highest_txn_id() const override {
    std::lock_guard<simple_spinlock> l(lock_);
    return highest_txn_id_;
  }

  // Populates a map from transaction ID to the sorted list of participants
  // associated with that transaction ID.
  tablet::ParticipantIdsByTxnId GetParticipantsByTxnIdForTests() const override;

  void RemoveCommitTask(int64_t txn_id, const CommitTasks* tasks) {
    std::lock_guard<simple_spinlock> l(lock_);
    const auto& iter = commits_in_flight_.find(txn_id);
    if (iter != commits_in_flight_.end() && iter->second.get() == tasks) {
      commits_in_flight_.erase(iter);
    }
  }

  bool shutting_down() const {
    return shutting_down_;
  }

 private:
  // This test class calls LoadFromTablet() directly.
  FRIEND_TEST(TxnStatusManagerTest, TestStartTransactions);
  FRIEND_TEST(TxnStatusManagerTest, GetTransactionStatus);
  friend class TxnStatusManagerTest;

  // Loads the contents of the transaction status tablet into memory, starting
  // up any background tasks (e.g. commits) that need to be driven by the new
  // leader.
  Status LoadFromTabletUnlocked();

  // This is called by tests only.
  Status LoadFromTablet();

  // Verifies that the transaction status data has already been loaded from the
  // underlying tablet and the replica is a leader. Returns Status::OK() if the
  // data is loaded and the replica is a leader. Otherwise, if the data hasn't
  // been loaded yet, return Status::ServiceUnavailable().  If the data has
  // been loaded, but the replica isn't a leader, returns
  // Status::ServiceUnavailable() and sets the code in 'ts_error'
  // to TabletServerErrorPB::NOT_THE_LEADER.
  Status CheckTxnStatusDataLoadedUnlocked(
      tserver::TabletServerErrorPB* ts_error) const;

  // Loops and sleeps until one of the following conditions occurs:
  // 1. The current node is the leader in the current term
  //    and at least one op from the current term is committed. Returns OK.
  // 2. The current node is not the leader. Returns IllegalState.
  // 3. The provided timeout expires. Returns TimedOut.
  //
  // This method is intended to ensure that all operations replicated by
  // previous leader are committed and visible to the local node before
  // reading the data, to ensure consistency across failovers.
  Status WaitUntilCaughtUpAsLeader(const MonoDelta& timeout);

  // Returns the transaction entry, returning an error if the transaction ID
  // doesn't exist or if 'user' is specified but isn't the owner of the
  // transaction. In addition, if the underlying replica isn't a leader,
  // sets the code in 'ts_error' to TabletServerErrorPB::NOT_THE_LEADER
  // correspondingly.
  Status GetTransaction(int64_t txn_id, const boost::optional<std::string>& user,
                        scoped_refptr<TransactionEntry>* txn,
                        tserver::TabletServerErrorPB* ts_error) const;

  TxnSystemClientInitializer* client_initializer_;
  ThreadPool* commit_pool_;

  std::atomic<bool> shutting_down_;

  // Protects 'highest_txn_id_', 'txns_by_id_', and insertions or removal from
  // 'commits_in_flight_'.
  mutable simple_spinlock lock_;

  // The highest transaction ID seen by this status manager so far. Requests to
  // create a new transaction must provide an ID higher than this ID.
  int64_t highest_txn_id_;

  // Tracks the currently on-going transactions.
  TransactionsMap txns_by_id_;

  // Can only be inserted to while the appropriate transaction lock is being
  // held.
  std::unordered_map<int64_t, scoped_refptr<CommitTasks>> commits_in_flight_;

  // The access to underlying storage.
  TxnStatusTablet status_tablet_;

  // This field is updated when a node becomes the leader, waits for all outstanding
  // uncommitted metadata in the transaction status manager to commit, and then
  // reads that metadata into in-memory data structures. This is used to "fence"
  // requests that depend on the in-memory state until the node can respond
  // correctly.
  int64_t leader_ready_term_;

  // Lock protecting 'leader_ready_term_'.
  mutable simple_spinlock leader_term_lock_;

  // Lock used to fence operations and leader elections. All logical operations
  // (i.e. begin transaction, get transaction, etc.) should acquire this lock for
  // reading. Following an election where this replica is elected leader, it
  // should acquire this lock for writing before reloading the metadata.
  //
  // Readers should not acquire this lock directly; use ScopedLeaderSharedLock
  // instead.
  //
  // Always acquire this lock before 'leader_term_lock_'.
  RWMutex leader_lock_;
};

class TxnStatusManagerFactory : public tablet::TxnCoordinatorFactory {
 public:
  TxnStatusManagerFactory(TxnSystemClientInitializer* client_initializer,
                          ThreadPool* commit_pool)
      : txn_client_initializer_(client_initializer),
        commit_pool_(commit_pool) {}

  std::unique_ptr<tablet::TxnCoordinator> Create(tablet::TabletReplica* replica) override {
    return std::unique_ptr<tablet::TxnCoordinator>(
        new TxnStatusManager(replica, txn_client_initializer_, commit_pool_));
  }

 private:
  TxnSystemClientInitializer* txn_client_initializer_;
  ThreadPool* commit_pool_;
};

} // namespace transactions
} // namespace kudu
