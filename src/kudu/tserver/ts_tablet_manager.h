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

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tablet_copy_client.h"
#include "kudu/tserver/tablet_replica_lookup.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
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
class Schema;
class ThreadPool;

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
class TabletReplica;
}

namespace tserver {
class TabletServer;

// Map of tablet id -> transition reason string.
typedef std::unordered_map<std::string, std::string> TransitionInProgressMap;

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
  // Upon return of this method all existing tablets are registered, but
  // the bootstrap is performed asynchronously.
  Status Init();

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
                         const Schema& schema,
                         const PartitionSchema& partition_schema,
                         consensus::RaftConfigPB config,
                         scoped_refptr<tablet::TabletReplica>* replica);

  // Delete the specified tablet.
  // 'delete_type' must be one of TABLET_DATA_DELETED or TABLET_DATA_TOMBSTONED
  // or else returns Status::IllegalArgument.
  // 'cas_config_index' is optionally specified to enable an
  // atomic DeleteTablet operation that only occurs if the latest committed
  // Raft config change op has an opid_index equal to or less than the specified
  // value. If not, 'error_code' is set to CAS_FAILED and a non-OK Status is
  // returned.
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

  // Return the number of tablets in RUNNING or BOOTSTRAPPING state.
  int GetNumLiveTablets() const;

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

 private:
  FRIEND_TEST(TsTabletManagerTest, TestPersistBlocks);

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
  void OpenTablet(const scoped_refptr<tablet::TabletReplica>& replica,
                  const scoped_refptr<TransitionInProgressDeleter>& deleter);

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
  // the TablerPeer object. See RegisterTablet() for details about the
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

  FsManager* const fs_manager_;

  const scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager_;

  TabletServer* server_;

  consensus::RaftPeerPB local_peer_pb_;

  typedef std::unordered_map<std::string, scoped_refptr<tablet::TabletReplica> > TabletMap;

  // Lock protecting tablet_map_, dirty_tablets_, state_,
  // transition_in_progress_, perm_deleted_tablet_ids_,
  // tablet_state_counts_, and last_walked_.
  mutable RWMutex lock_;

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

  TSTabletManagerStatePB state_;

  // Thread pool used to run tablet copy operations.
  gscoped_ptr<ThreadPool> tablet_copy_pool_;

  // Thread pool used to open the tablets async, whether bootstrap is required or not.
  gscoped_ptr<ThreadPool> open_tablet_pool_;

  FunctionGaugeDetacher metric_detacher_;

  DISALLOW_COPY_AND_ASSIGN(TSTabletManager);
};

// Helper to delete the transition-in-progress entry from the corresponding set
// when tablet bootstrap, create, and delete operations complete.
class TransitionInProgressDeleter : public RefCountedThreadSafe<TransitionInProgressDeleter> {
 public:
  TransitionInProgressDeleter(TransitionInProgressMap* map, RWMutex* lock,
                              std::string entry);

 private:
  friend class RefCountedThreadSafe<TransitionInProgressDeleter>;
  ~TransitionInProgressDeleter();

  TransitionInProgressMap* const in_progress_;
  RWMutex* const lock_;
  const std::string entry_;
};

} // namespace tserver
} // namespace kudu
