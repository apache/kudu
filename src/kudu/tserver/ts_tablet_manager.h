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
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_copy_client.h"
#include "kudu/tserver/tablet_replica_lookup.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace boost {
template <class T>
class optional;
}

namespace kudu {

class FsManager;
class NodeInstancePB;
class Partition;
class PartitionSchema;
class ThreadPool;
class Timer;

namespace transactions {
class TxnSystemClient;
}  // namespace transactions

namespace consensus {
class ConsensusMetadataManager;
class OpId;
class StartTabletCopyRequestPB;
} // namespace consensus

namespace master {
class ReportedTabletPB;
class TabletReportPB;
} // namespace master

namespace tablet {
class TabletMetadata;
}

namespace tserver {
class TabletServer;

// Map of tablet id -> transition reason string.
typedef std::unordered_map<std::string, std::string> TransitionInProgressMap;

// Map of dimension -> tablets number.
typedef std::unordered_map<std::string, int32_t> TabletNumByDimensionMap;

class TransitionInProgressDeleter;

// Keeps track of the tablets hosted on the tablet server side.
//
// TODO(todd): will also be responsible for keeping the local metadata about which
// tablets are hosted on this server persistent on disk, as well as re-opening all
// the tablets at startup, etc.
class TSTabletManager : public tserver::TabletReplicaLookupIf {
 public:
  // Construct the tablet manager.
  explicit TSTabletManager(TabletServer* server);

  virtual ~TSTabletManager();

  // Load all tablet metadata blocks from disk, and open their respective tablets.
  // Starts the timer, 'start_tablets' and populates the 'tablets_total'. The subsequent
  // function call to OpenTablet() updates 'tablets_processed' and stops the timer.
  // Upon return of this method all existing tablets are registered, but
  // the bootstrap is performed asynchronously.
  Status Init(Timer* start_tablets,
              std::atomic<int>* tablets_processed,
              std::atomic<int>* tablets_total);

  // Waits for all the bootstraps to complete.
  // Returns Status::OK if all tablets bootstrapped successfully. If
  // the bootstrap of any tablet failed returns the failure reason for
  // the first tablet whose bootstrap failed.
  Status WaitForAllBootstrapsToFinish();

  // Shut down all of the tablets, gracefully flushing before shutdown.
  void Shutdown();

  // Create a new tablet and register it with the tablet manager. The new tablet
  // is persisted on disk and opened before this method returns.
  //
  // If 'replica' is non-NULL, the newly created tablet will be returned.
  //
  // If another tablet already exists with this ID, logs a DFATAL
  // and returns a bad Status.
  Status CreateNewTablet(const std::string& table_id,
                         const std::string& tablet_id,
                         const Partition& partition,
                         const std::string& table_name,
                         SchemaPtr& schema,
                         const PartitionSchema& partition_schema,
                         consensus::RaftConfigPB config,
                         boost::optional<TableExtraConfigPB> extra_config,
                         boost::optional<std::string> dimension_label,
                         boost::optional<TableTypePB> table_type,
                         scoped_refptr<tablet::TabletReplica>* replica);

  // Delete the specified tablet asynchronously with callback 'cb'.
  // - If the async task cannot be started, 'cb' will be called with
  //   Status::ServiceUnavailable and TabletServerErrorPB::THROTTLED.
  // - 'delete_type' must be one of TABLET_DATA_DELETED or TABLET_DATA_TOMBSTONED.
  // - 'cas_config_index' is optionally specified to enable an
  //   atomic DeleteTablet operation that only occurs if the latest committed
  //   Raft config change op has an opid_index equal to or less than the specified
  //   value. If not, the callback is called with a non-OK Status and error code
  //   CAS_FAILED.
  void DeleteTabletAsync(const std::string& tablet_id,
                         tablet::TabletDataState delete_type,
                         const boost::optional<int64_t>& cas_config_index,
                         const std::function<void(const Status&, TabletServerErrorPB::Code)>& cb);

  // Delete the specified tablet synchronously.
  // See DeleteTabletAsync() for more information.
  Status DeleteTablet(const std::string& tablet_id,
                      tablet::TabletDataState delete_type,
                      const boost::optional<int64_t>& cas_config_index,
                      TabletServerErrorPB::Code* error_code = nullptr);

  // Lookup the given tablet replica by its ID.
  // Returns true if the tablet is found successfully.
  bool LookupTablet(const std::string& tablet_id,
                    scoped_refptr<tablet::TabletReplica>* replica) const;

  // Same as LookupTablet but doesn't acquired the shared lock.
  bool LookupTabletUnlocked(const std::string& tablet_id,
                            scoped_refptr<tablet::TabletReplica>* replica) const;

  virtual Status GetTabletReplica(const std::string& tablet_id,
                                  scoped_refptr<tablet::TabletReplica>* replica) const
                                  override;

  virtual const NodeInstancePB& NodeInstance() const override;

  // Initiate tablet copy of the specified tablet on the tablet_copy_pool_.
  // See the StartTabletCopy() RPC declaration in consensus.proto for details.
  // 'cb' is guaranteed to be invoked as a callback.
  virtual void StartTabletCopy(
      const consensus::StartTabletCopyRequestPB* req,
      std::function<void(const Status&, TabletServerErrorPB::Code)> cb) override;

  // Synchronously run the tablet copy procedure.
  void RunTabletCopy(
      const consensus::StartTabletCopyRequestPB* req,
      std::function<void(const Status&, TabletServerErrorPB::Code)> cb);

  // Adds updated tablet information to 'report'.
  void PopulateFullTabletReport(master::TabletReportPB* report) const;

  // Adds updated tablet information to 'report'. Only tablets in 'tablet_ids'
  // are included.
  void PopulateIncrementalTabletReport(master::TabletReportPB* report,
                                       const std::vector<std::string>& tablet_ids) const;

  // Get all of the tablets currently hosted on this server.
  virtual void GetTabletReplicas(
      std::vector<scoped_refptr<tablet::TabletReplica> >* replicas) const override;

  // Marks tablet with 'tablet_id' as dirty so that it'll be included in the
  // next round of master heartbeats.
  //
  // Dirtying events typically include state changes outside of the control of
  // TsTabletManager, such as consensus role changes.
  void MarkTabletDirty(const std::string& tablet_id, const std::string& reason);

  // Marks tablets as dirty in batch.
  void MarkTabletsDirty(const std::vector<std::string>& tablet_ids, const std::string& reason);

  // Return the number of tablets in RUNNING or BOOTSTRAPPING state.
  int GetNumLiveTablets() const;

  // Get the number of tablets in RUNNING or BOOTSTRAPPING state in each dimension.
  TabletNumByDimensionMap GetNumLiveTabletsByDimension() const;

  Status RunAllLogGC();

  // Delete the tablet using the specified delete_type as the final metadata
  // state. Deletes the on-disk data, metadata, as well as all WAL segments.
  //
  // If set, 'last_logged_opid' will be persisted in the
  // 'tombstone_last_logged_opid' field in the tablet metadata. Otherwise, if
  // 'last_logged_opid' is equal to boost::none, the tablet metadata will
  // retain its previous value of 'tombstone_last_logged_opid', if any.
  static Status DeleteTabletData(
      const scoped_refptr<tablet::TabletMetadata>& meta,
      const scoped_refptr<consensus::ConsensusMetadataManager>& cmeta_manager,
      tablet::TabletDataState delete_type,
      boost::optional<consensus::OpId> last_logged_opid);

  // Synchronously makes the specified tablet unavailable for further I/O and
  // schedules its asynchronous shutdown.
  void FailTabletAndScheduleShutdown(const std::string& tablet_id);

  // Forces shutdown of the tablet replicas in the data dir corresponding to 'uuid'.
  void FailTabletsInDataDir(const std::string& uuid);

  // Refresh the cached counts of tablet states, if the cache is old enough,
  // and return the count for tablet state 'st'.
  int RefreshTabletStateCacheAndReturnCount(tablet::TabletStatePB st);

  // Wait a period up to 'timeout' for there to be no tablet state transitions
  // registered with the tablet manager.
  // This method is for use in tests only. See KUDU-2444.
  Status WaitForNoTransitionsForTests(const MonoDelta& timeout) const;

  // Update the tablet statistics if necessary.
  void UpdateTabletStatsIfNecessary();

  // Schedule preliminary tasks to begin transaction 'txn_id' started by 'user'
  // with 'replica' as a participant, with the given deadline. Calls 'cb' if
  // any of the tasks fail.
  Status SchedulePreliminaryTasksForTxnWrite(
      scoped_refptr<tablet::TabletReplica> replica,
      int64_t txn_id,
      const std::string& user,
      MonoTime deadline,
      tablet::RegisteredTxnCallback cb);

  // Schedule the rollback of the given transaction as the given user.
  Status ScheduleAbortTxn(int64_t txn_id, const std::string& user);

 private:
  FRIEND_TEST(LeadershipChangeReportingTest, TestReportStatsDuringLeadershipChange);
  FRIEND_TEST(TsTabletManagerTest, TestPersistBlocks);
  FRIEND_TEST(TsTabletManagerTest, TestTabletStatsReports);
  FRIEND_TEST(TsTabletManagerITest, TestTableStats);

  // Flag specified when registering a TabletReplica.
  enum RegisterTabletReplicaMode {
    NEW_REPLICA,
    REPLACEMENT_REPLICA
  };

  // Standard log prefix, given a tablet id.
  static std::string LogPrefix(const std::string& tablet_id, FsManager *fs_manager);
  std::string LogPrefix(const std::string& tablet_id) const {
    return LogPrefix(tablet_id, fs_manager_);
  }

  static void RegisterAndBeginParticipantTxnTask(
      transactions::TxnSystemClient* txn_system_client,
      scoped_refptr<tablet::TabletReplica> replica,
      int64_t txn_id,
      const std::string& user,
      MonoTime deadline,
      tablet::RegisteredTxnCallback began_txn_cb);

  // Returns Status::OK() iff state_ == MANAGER_RUNNING.
  Status CheckRunningUnlocked(TabletServerErrorPB::Code* error_code) const;

  // Registers the start of a tablet state transition by inserting the tablet
  // id and reason string into the transition_in_progress_ map.
  // 'reason' is a string included in the Status return when there is
  // contention indicating why the tablet is currently already transitioning.
  // Returns IllegalState if the tablet is already "locked" for a state
  // transition by some other operation.
  // On success, returns OK and populates 'deleter' with an object that removes
  // the map entry on destruction.
  Status StartTabletStateTransitionUnlocked(const std::string& tablet_id,
                                            const std::string& reason,
                                            scoped_refptr<TransitionInProgressDeleter>* deleter);

  // Marks the replica indicated by 'tablet_id' as being in a transitional
  // state. Returns an error status if the replica is already in a transitional
  // state.
  Status BeginReplicaStateTransition(const std::string& tablet_id,
                                     const std::string& reason,
                                     scoped_refptr<tablet::TabletReplica>* replica,
                                     scoped_refptr<TransitionInProgressDeleter>* deleter,
                                     TabletServerErrorPB::Code* error_code);

  // Open a tablet meta from the local file system by loading its superblock.
  Status OpenTabletMeta(const std::string& tablet_id,
                        scoped_refptr<tablet::TabletMetadata>* metadata);

  // Open a tablet whose metadata has already been loaded/created.
  // This method does not return anything as it can be run asynchronously.
  // Upon completion of this method the tablet should be initialized and running.
  // If something wrong happened on bootstrap/initialization the relevant error
  // will be set on TabletReplica along with the state set to FAILED.
  //
  // The tablet must be registered and an entry corresponding to this tablet
  // must be put into the transition_in_progress_ map before calling this
  // method. A TransitionInProgressDeleter must be passed as 'deleter' into
  // this method in order to remove that transition-in-progress entry when
  // opening the tablet is complete (in either a success or a failure case).
  //
  // In the subsequent call made to UpdateStartupProgress, 'tablets_processed'
  // will be updated and the timer is stopped once all the tablets are processed.
  void OpenTablet(const scoped_refptr<tablet::TabletReplica> &replica,
                  const scoped_refptr<TransitionInProgressDeleter> &deleter,
                  std::atomic<int>* tablets_processed = nullptr,
                  std::atomic<int>* tablets_total = nullptr,
                  Timer* bootstrap_tablets = nullptr);

  // Open a tablet whose metadata has already been loaded.
  void BootstrapAndInitTablet(const scoped_refptr<tablet::TabletMetadata>& meta,
                              scoped_refptr<tablet::TabletReplica>* replica);

  // Add the tablet to the tablet map.
  // 'mode' specifies whether to expect an existing tablet to exist in the map.
  // If mode == NEW_REPLICA but a tablet with the same name is already registered,
  // or if mode == REPLACEMENT_REPLICA but a tablet with the same name is not
  // registered, a FATAL message is logged, causing a process crash.
  // Calls to this method are expected to be externally synchronized, typically
  // using the transition_in_progress_ map.
  void RegisterTablet(const std::string& tablet_id,
                      const scoped_refptr<tablet::TabletReplica>& replica,
                      RegisterTabletReplicaMode mode);

  // Create and register a new TabletReplica, given tablet metadata.
  // Calls RegisterTablet() with the given 'mode' parameter after constructing
  // the TabletReplica object. See RegisterTablet() for details about the
  // semantics of 'mode' and the locking requirements.
  Status CreateAndRegisterTabletReplica(scoped_refptr<tablet::TabletMetadata> meta,
                                        RegisterTabletReplicaMode mode,
                                        scoped_refptr<tablet::TabletReplica>* replica_out);

  // Helper to generate the report for a single tablet.
  void CreateReportedTabletPB(const scoped_refptr<tablet::TabletReplica>& replica,
                              master::ReportedTabletPB* reported_tablet) const;

  // Handle the case on startup where we find a tablet that is not in
  // TABLET_DATA_READY state. Generally, we tombstone the replica.
  Status HandleNonReadyTabletOnStartup(const scoped_refptr<tablet::TabletMetadata>& meta);

  // Return Status::IllegalState if leader_term < last_logged_term.
  // Helper function for use with tablet copy.
  Status CheckLeaderTermNotLower(const std::string& tablet_id,
                                 int64_t leader_term,
                                 int64_t last_logged_term);

  TSTabletManagerStatePB state() const {
    shared_lock<RWMutex> l(lock_);
    return state_;
  }

  // Initializes the RaftPeerPB for the local peer.
  // Guaranteed to include both uuid and last_seen_addr fields.
  // Crashes with an invariant check if the RPC server is not currently in a
  // running state.
  void InitLocalRaftPeerPB();

  // A task to check for the staleness of transactions registered with
  // corresponding transaction status tablets (if any).
  void TxnStalenessTrackerTask();

  // Just for tests.
  void SetNextUpdateTimeForTests();

  // If 'tablets_processed' is not nullptr, 'tablets_processed' will be incremented
  // after every tablet is attempted to be opened and the timer is stopped once all
  // the tablets are processed.
  void IncrementTabletsProcessed(int tablets_total, std::atomic<int>* tablets_processed,
                             Timer* start_tablets);

  FsManager* const fs_manager_;

  const scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager_;

  TabletServer* const server_;

  consensus::RaftPeerPB local_peer_pb_;

  typedef std::unordered_map<std::string, scoped_refptr<tablet::TabletReplica> > TabletMap;

  // Lock protecting tablet_map_, dirty_tablets_, state_,
  // transition_in_progress_, perm_deleted_tablet_ids_,
  // tablet_state_counts_, and last_walked_.
  mutable RWMutex lock_;

  // A latch to notify the task running on the txn_status_manager_pool_ on
  // shutdown.
  //
  // TODO(aserbin): instead of using CountDownLatch, extend ConditionVariable
  //                to be able to work with RWMutex and use lock_ from above
  //                to create one to notify the task on shutdown
  CountDownLatch shutdown_latch_;

  // Map from tablet ID to tablet
  TabletMap tablet_map_;

  // Permanently deleted tablet ids. If a tablet is removed with status
  // TABLET_DATA_DELETED then it is added to this map (until the next process
  // restart).
  std::unordered_set<std::string> perm_deleted_tablet_ids_;

  // Map of tablet ids -> reason strings where the keys are tablets whose
  // bootstrap, creation, or deletion is in-progress
  TransitionInProgressMap transition_in_progress_;

  MetricRegistry* metric_registry_;

  TabletCopyClientMetrics tablet_copy_metrics_;

  // Timestamp indicating the last time tablet_map_ was walked to count
  // tablet states.
  MonoTime last_walked_ = MonoTime::Min();

  // Holds cached tablet states from tablet_map_.
  std::map<tablet::TabletStatePB, int> tablet_state_counts_;

  // Set of transactions that have a pending call to abort, indicating that
  // further attempts to schedule such a call can be ignored.
  simple_spinlock txn_aborts_lock_;
  std::unordered_set<int64_t> txn_aborts_in_progress_;

  TSTabletManagerStatePB state_;

  // Thread pool used to run tablet copy operations.
  std::unique_ptr<ThreadPool> tablet_copy_pool_;

  // Thread pool used to open the tablets async, whether bootstrap is required or not.
  std::unique_ptr<ThreadPool> open_tablet_pool_;

  // Thread pool used to delete tablets asynchronously.
  std::unique_ptr<ThreadPool> delete_tablet_pool_;

  // Thread pool used to reload transaction status tablets asynchronously.
  std::unique_ptr<ThreadPool> reload_txn_status_tablet_pool_;

  // Thread pool used to perform background tasks on transactions, e.g. to commit.
  std::unique_ptr<ThreadPool> txn_commit_pool_;

  // Thread pool to perform preliminary tasks when processing write operations
  // in the context of a multi-row transaction. Such tasks include registering
  // tablet as a participant in the corresponding transaction, etc.
  std::unique_ptr<ThreadPool> txn_participant_registration_pool_;

  // Thread pool to run TxnStatusManager tasks. As of now, this pool is
  // to run a long-running single periodic task to abort stale transactions
  // registered with corresponding transaction status tablets.
  std::unique_ptr<ThreadPool> txn_status_manager_pool_;

  // Ensures that we only update stats from a single thread at a time.
  mutable rw_spinlock lock_update_;
  MonoTime next_update_time_;

  // Keep track of number of tablets opened/attempted to be opened
  // during server startup
  scoped_refptr<AtomicGauge<uint32_t>> tablets_num_opened_startup_;

  // NOTE: it's important that this is the first member to be destructed. This
  // ensures we do not attempt to collect metrics while calling the destructor.
  FunctionGaugeDetacher metric_detacher_;

  DISALLOW_COPY_AND_ASSIGN(TSTabletManager);
};

// Helper to delete the transition-in-progress entry from the corresponding set
// when tablet bootstrap, create, and delete operations complete.
class TransitionInProgressDeleter : public RefCountedThreadSafe<TransitionInProgressDeleter> {
 public:
  TransitionInProgressDeleter(TransitionInProgressMap* map, RWMutex* lock,
                              std::string entry);
  void Destroy();

 private:
  friend class RefCountedThreadSafe<TransitionInProgressDeleter>;
  ~TransitionInProgressDeleter();

  TransitionInProgressMap* const in_progress_;
  RWMutex* const lock_;
  const std::string entry_;
  bool is_destroyed_;
};

} // namespace tserver
} // namespace kudu
