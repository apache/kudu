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
#ifndef KUDU_MASTER_CATALOG_MANAGER_H
#define KUDU_MASTER_CATALOG_MANAGER_H

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional_fwd.hpp>

#include "kudu/common/partition.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/ts_manager.h"
#include "kudu/server/monitored_task.h"
#include "kudu/tserver/tablet_peer_lookup.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/promise.h"
#include "kudu/util/random.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

namespace kudu {

class Schema;
class ThreadPool;
class CreateTableStressTest_TestConcurrentCreateTableAndReloadMetadata_Test;

namespace rpc {
class RpcContext;
} // namespace rpc

namespace master {

class CatalogManagerBgTasks;
class Master;
class SysCatalogTable;
class TableInfo;
class TSDescriptor;

struct DeferredAssignmentActions;

// The data related to a tablet which is persisted on disk.
// This portion of TableInfo is managed via CowObject.
// It wraps the underlying protobuf to add useful accessors.
struct PersistentTabletInfo {
  bool is_running() const {
    return pb.state() == SysTabletsEntryPB::RUNNING;
  }

  bool is_deleted() const {
    return pb.state() == SysTabletsEntryPB::REPLACED ||
           pb.state() == SysTabletsEntryPB::DELETED;
  }

  // Helper to set the state of the tablet with a custom message.
  // Requires that the caller has prepared this object for write.
  // The change will only be visible after Commit().
  void set_state(SysTabletsEntryPB::State state, const std::string& msg);

  SysTabletsEntryPB pb;
};

// The information about a single tablet which exists in the cluster,
//
// This object uses copy-on-write for the portions of data which are persisted
// on disk. This allows the mutated data to be staged and written to disk
// while readers continue to access the previous version. These portions
// of data are in PersistentTableInfo above, and typically accessed using
// TabletMetadataLock. For example:
//
//   TabletInfo* table = ...;
//   TabletMetadataLock l(tablet, TableMetadataLock::READ);
//   if (l.data().is_running()) { ... }
//
// The non-persistent information about the tablet is protected by an internal
// spin-lock.
//
// The object is owned/managed by the CatalogManager, and exposed for testing.
class TabletInfo : public RefCountedThreadSafe<TabletInfo> {
 public:
  typedef PersistentTabletInfo cow_state;

  TabletInfo(const scoped_refptr<TableInfo>& table, std::string tablet_id);

  const std::string& tablet_id() const { return tablet_id_; }
  const scoped_refptr<TableInfo>& table() const { return table_; }

  // Access the persistent metadata. Typically you should use
  // TabletMetadataLock to gain access to this data.
  const CowObject<PersistentTabletInfo>& metadata() const { return metadata_; }
  CowObject<PersistentTabletInfo>* mutable_metadata() { return &metadata_; }

  // Accessors for the last time create tablet RPCs were sent for this tablet.
  void set_last_create_tablet_time(const MonoTime& ts);
  MonoTime last_create_tablet_time() const;

  // Accessors for the last reported schema version
  bool set_reported_schema_version(uint32_t version);
  uint32_t reported_schema_version() const;

  // No synchronization needed.
  std::string ToString() const;

 private:
  friend class RefCountedThreadSafe<TabletInfo>;
  ~TabletInfo();

  const std::string tablet_id_;
  const scoped_refptr<TableInfo> table_;

  CowObject<PersistentTabletInfo> metadata_;

  // Lock protecting the below mutable fields.
  // This doesn't protect metadata_ (the on-disk portion).
  mutable simple_spinlock lock_;

  // The last time the master sent create tablet RPCs for the tablet.
  MonoTime last_create_tablet_time_;

  // Reported schema version (in-memory only).
  uint32_t reported_schema_version_;

  DISALLOW_COPY_AND_ASSIGN(TabletInfo);
};

// The data related to a table which is persisted on disk.
// This portion of TableInfo is managed via CowObject.
// It wraps the underlying protobuf to add useful accessors.
struct PersistentTableInfo {
  bool is_deleted() const {
    return pb.state() == SysTablesEntryPB::REMOVED;
  }

  bool is_running() const {
    return pb.state() == SysTablesEntryPB::RUNNING ||
           pb.state() == SysTablesEntryPB::ALTERING;
  }

  // Return the table's name.
  const std::string& name() const {
    return pb.name();
  }

  // Helper to set the state of the tablet with a custom message.
  void set_state(SysTablesEntryPB::State state, const std::string& msg);

  SysTablesEntryPB pb;
};

// The information about a table, including its state and tablets.
//
// This object uses copy-on-write techniques similarly to TabletInfo.
// Please see the TabletInfo class doc above for more information.
//
// The non-persistent information about the table is protected by an internal
// spin-lock.
class TableInfo : public RefCountedThreadSafe<TableInfo> {
 public:
  typedef PersistentTableInfo cow_state;
  typedef std::map<std::string, TabletInfo*> TabletInfoMap;

  explicit TableInfo(std::string table_id);

  std::string ToString() const;

  // Return the table's ID. Does not require synchronization.
  const std::string& id() const { return table_id_; }

  // Add a tablet to this table.
  void AddTablet(TabletInfo *tablet);
  // Add multiple tablets to this table.
  void AddTablets(const std::vector<TabletInfo*>& tablets);

  // Atomically add and remove multiple tablets from this table.
  void AddRemoveTablets(const vector<scoped_refptr<TabletInfo>>& tablets_to_add,
                        const vector<scoped_refptr<TabletInfo>>& tablets_to_drop);

  // Return true if tablet with 'partition_key_start' has been
  // removed from 'tablet_map_' below.
  bool RemoveTablet(const std::string& partition_key_start);

  // This only returns tablets which are in RUNNING state.
  void GetTabletsInRange(const GetTableLocationsRequestPB* req,
                         std::vector<scoped_refptr<TabletInfo> > *ret) const;

  // Adds all tablets to the vector in partition key sorted order.
  void GetAllTablets(std::vector<scoped_refptr<TabletInfo> > *ret) const;

  // Access the persistent metadata. Typically you should use
  // TableMetadataLock to gain access to this data.
  const CowObject<PersistentTableInfo>& metadata() const { return metadata_; }
  CowObject<PersistentTableInfo>* mutable_metadata() { return &metadata_; }

  // Returns true if the table creation is in-progress
  bool IsCreateInProgress() const;

  // Returns true if an "Alter" operation is in-progress
  bool IsAlterInProgress(uint32_t version) const;

  void AddTask(MonitoredTask *task);
  void RemoveTask(MonitoredTask *task);
  void AbortTasks();
  void WaitTasksCompletion();

  // Allow for showing outstanding tasks in the master UI.
  void GetTaskList(std::vector<scoped_refptr<MonitoredTask> > *tasks);

  // Returns a snapshot copy of the table info's tablet map.
  TabletInfoMap tablet_map() const {
    shared_lock<rw_spinlock> l(lock_);
    return tablet_map_;
  }

  // Returns the number of tablets.
  int num_tablets() const {
    shared_lock<rw_spinlock> l(lock_);
    return tablet_map_.size();
  }

 private:
  friend class RefCountedThreadSafe<TableInfo>;
  ~TableInfo();

  void AddTabletUnlocked(TabletInfo* tablet);

  const std::string table_id_;

  // Sorted index of tablet start partition-keys to TabletInfo.
  // The TabletInfo objects are owned by the CatalogManager.
  TabletInfoMap tablet_map_;

  // Protects tablet_map_ and pending_tasks_
  mutable rw_spinlock lock_;

  CowObject<PersistentTableInfo> metadata_;

  // List of pending tasks (e.g. create/alter tablet requests)
  std::unordered_set<MonitoredTask*> pending_tasks_;

  DISALLOW_COPY_AND_ASSIGN(TableInfo);
};

// Helper to manage locking on the persistent metadata of TabletInfo or TableInfo.
template<class MetadataClass>
class MetadataLock : public CowLock<typename MetadataClass::cow_state> {
 public:
  typedef CowLock<typename MetadataClass::cow_state> super;
  MetadataLock(MetadataClass* info, typename super::LockMode mode)
    : super(DCHECK_NOTNULL(info)->mutable_metadata(), mode) {
  }
  MetadataLock(const MetadataClass* info, typename super::LockMode mode)
    : super(&(DCHECK_NOTNULL(info))->metadata(), mode) {
  }
};

typedef MetadataLock<TabletInfo> TabletMetadataLock;
typedef MetadataLock<TableInfo> TableMetadataLock;

// The component of the master which tracks the state and location
// of tables/tablets in the cluster.
//
// This is the master-side counterpart of TSTabletManager, which tracks
// the state of each tablet on a given tablet-server.
//
// Thread-safe.
class CatalogManager : public tserver::TabletPeerLookupIf {
 public:

  // Scoped "shared lock" to serialize master leader elections.
  //
  // While in scope, blocks the catalog manager in the event that it becomes
  // the leader of its Raft configuration and needs to reload its persistent
  // metadata. Once destroyed, the catalog manager is unblocked.
  //
  // Usage:
  //
  // void MasterServiceImpl::CreateTable(const CreateTableRequestPB* req,
  //                                     CreateTableResponsePB* resp,
  //                                     rpc::RpcContext* rpc) {
  //   CatalogManager::ScopedLeaderSharedLock l(server_->catalog_manager());
  //   if (!l.CheckIsInitializedAndIsLeaderOrRespond(resp, rpc)) {
  //     return;
  //   }
  //
  //   Status s = server_->catalog_manager()->CreateTable(req, resp, rpc);
  //   CheckRespErrorOrSetUnknown(s, resp);
  //   rpc->RespondSuccess();
  // }
  //
  class ScopedLeaderSharedLock {
   public:
    // Creates a new shared lock, acquiring the catalog manager's leader_lock_
    // for reading in the process. The lock is released when this object is
    // destroyed.
    //
    // 'catalog' must outlive this object.
    explicit ScopedLeaderSharedLock(CatalogManager* catalog);

    // General status of the catalog manager. If not OK (e.g. the catalog
    // manager is still being initialized), all operations are illegal and
    // leader_status() should not be trusted.
    const Status& catalog_status() const { return catalog_status_; }

    // Leadership status of the catalog manager. If not OK, the catalog
    // manager is not the leader, but some operations may still be legal.
    const Status& leader_status() const {
      DCHECK(catalog_status_.ok());
      return leader_status_;
    }

    // First non-OK status of the catalog manager, adhering to the checking
    // order specified above.
    const Status& first_failed_status() const {
      if (!catalog_status_.ok()) {
        return catalog_status_;
      }
      return leader_status_;
    }

    // Check that the catalog manager is initialized. It may or may not be the
    // leader of its Raft configuration.
    //
    // If not initialized, writes the corresponding error to 'resp',
    // responds to 'rpc', and returns false.
    template<typename RespClass>
    bool CheckIsInitializedOrRespond(RespClass* resp, rpc::RpcContext* rpc);

    // Check that the catalog manager is initialized and that it is the leader
    // of its Raft configuration. Initialization status takes precedence over
    // leadership status.
    //
    // If not initialized or if not the leader, writes the corresponding error
    // to 'resp', responds to 'rpc', and returns false.
    template<typename RespClass>
    bool CheckIsInitializedAndIsLeaderOrRespond(RespClass* resp, rpc::RpcContext* rpc);

   private:
    CatalogManager* catalog_;
    shared_lock<RWMutex> leader_shared_lock_;
    Status catalog_status_;
    Status leader_status_;

    DISALLOW_COPY_AND_ASSIGN(ScopedLeaderSharedLock);
  };

  // Temporarily forces the catalog manager to be a follower. Only for tests!
  class ScopedLeaderDisablerForTests {
   public:

    explicit ScopedLeaderDisablerForTests(CatalogManager* catalog)
        : catalog_(catalog),
        old_leader_ready_term_(catalog->leader_ready_term_) {
      catalog_->leader_ready_term_ = -1;
    }

    ~ScopedLeaderDisablerForTests() {
      catalog_->leader_ready_term_ = old_leader_ready_term_;
    }

   private:
    CatalogManager* catalog_;
    int64_t old_leader_ready_term_;

    DISALLOW_COPY_AND_ASSIGN(ScopedLeaderDisablerForTests);
  };

  explicit CatalogManager(Master *master);
  virtual ~CatalogManager();

  Status Init(bool is_first_run);

  void Shutdown();
  Status CheckOnline() const;

  // Create a new Table with the specified attributes
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status CreateTable(const CreateTableRequestPB* req,
                     CreateTableResponsePB* resp,
                     rpc::RpcContext* rpc);

  // Get the information about an in-progress create operation
  Status IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                           IsCreateTableDoneResponsePB* resp);

  // Delete the specified table
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status DeleteTable(const DeleteTableRequestPB* req,
                     DeleteTableResponsePB* resp,
                     rpc::RpcContext* rpc);

  // Alter the specified table
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status AlterTable(const AlterTableRequestPB* req,
                    AlterTableResponsePB* resp,
                    rpc::RpcContext* rpc);

  // Get the information about an in-progress alter operation
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status IsAlterTableDone(const IsAlterTableDoneRequestPB* req,
                          IsAlterTableDoneResponsePB* resp,
                          rpc::RpcContext* rpc);

  // Get the information about the specified table
  Status GetTableSchema(const GetTableSchemaRequestPB* req,
                        GetTableSchemaResponsePB* resp);

  // List all the running tables
  Status ListTables(const ListTablesRequestPB* req,
                    ListTablesResponsePB* resp);

  // Lookup the tablets contained in the partition range of the request.
  // Returns an error if any of the tablets are not running.
  Status GetTableLocations(const GetTableLocationsRequestPB* req,
                           GetTableLocationsResponsePB* resp);

  // Look up the locations of the given tablet. The locations
  // vector is overwritten (not appended to).
  // If the tablet is not found, returns Status::NotFound.
  // If the tablet is not running, returns Status::ServiceUnavailable.
  // Otherwise, returns Status::OK and puts the result in 'locs_pb'.
  // This only returns tablets which are in RUNNING state.
  Status GetTabletLocations(const std::string& tablet_id,
                            TabletLocationsPB* locs_pb);

  // Handle a tablet report from the given tablet server.
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status ProcessTabletReport(TSDescriptor* ts_desc,
                             const TabletReportPB& report,
                             TabletReportUpdatesPB *report_update,
                             rpc::RpcContext* rpc);

  SysCatalogTable* sys_catalog() { return sys_catalog_.get(); }

  // Dump all of the current state about tables and tablets to the
  // given output stream. This is verbose, meant for debugging.
  void DumpState(std::ostream* out) const;

  // Retrieve a table by ID, or null if no such table exists. May fail if the
  // catalog manager is not yet running. Caller must hold leader_lock_.
  //
  // NOTE: This should only be used by tests or web-ui
  Status GetTableInfo(const std::string& table_id, scoped_refptr<TableInfo> *table);

  // Retrieve all known tables, even those that are not running. May fail if
  // the catalog manager is not yet running. Caller must hold leader_lock_.
  //
  // NOTE: This should only be used by tests or web-ui
  Status GetAllTables(std::vector<scoped_refptr<TableInfo>>* tables);

  // Check if a table exists by name, setting 'exist' appropriately. May fail
  // if the catalog manager is not yet running. Caller must hold leader_lock_.
  //
  // NOTE: This should only be used by tests
  Status TableNameExists(const std::string& table_name, bool* exists);

  // Let the catalog manager know that the the given tablet server successfully
  // deleted the specified tablet.
  void NotifyTabletDeleteSuccess(const std::string& permanent_uuid, const std::string& tablet_id);

  // Used by ConsensusService to retrieve the TabletPeer for a system
  // table specified by 'tablet_id'.
  //
  // See also: TabletPeerLookupIf, ConsensusServiceImpl.
  virtual Status GetTabletPeer(const std::string& tablet_id,
                               scoped_refptr<tablet::TabletPeer>* tablet_peer) const override;

  virtual const NodeInstancePB& NodeInstance() const override;

  bool IsInitialized() const;

  virtual void StartTabletCopy(
      const consensus::StartTabletCopyRequestPB* req,
      std::function<void(const Status&, tserver::TabletServerErrorPB::Code)> cb) override;

  // Returns this CatalogManager's role in a consensus configuration. CatalogManager
  // must be initialized before calling this method.
  consensus::RaftPeerPB::Role Role() const;

 private:
  // These tests call ElectedAsLeaderCb() directly.
  FRIEND_TEST(MasterTest, TestShutdownDuringTableVisit);
  FRIEND_TEST(MasterTest, TestGetTableLocationsDuringRepeatedTableVisit);

  // This test calls VisitTablesAndTablets() directly.
  FRIEND_TEST(kudu::CreateTableStressTest, TestConcurrentCreateTableAndReloadMetadata);

  friend class TableLoader;
  friend class TabletLoader;

  typedef std::unordered_map<std::string, scoped_refptr<TableInfo>> TableInfoMap;
  typedef std::unordered_map<std::string, scoped_refptr<TabletInfo>> TabletInfoMap;

  // Called by SysCatalog::SysCatalogStateChanged when this node
  // becomes the leader of a consensus configuration. Executes VisitTablesAndTabletsTask
  // via 'worker_pool_'.
  Status ElectedAsLeaderCb();

  // Loops and sleeps until one of the following conditions occurs:
  // 1. The current node is the leader master in the current term
  //    and at least one op from the current term is committed. Returns OK.
  // 2. The current node is not the leader master.
  //    Returns IllegalState.
  // 3. The provided timeout expires. Returns TimedOut.
  //
  // This method is intended to ensure that all operations replicated by
  // previous masters are committed and visible to the local node before
  // reading that data, to ensure consistency across failovers.
  Status WaitUntilCaughtUpAsLeader(const MonoDelta& timeout);

  // Performs several checks before calling VisitTablesAndTablets to actually
  // reload table/tablet metadata into memory.
  void VisitTablesAndTabletsTask();

  // Clears out the existing metadata ('table_names_map_', 'table_ids_map_',
  // and 'tablet_map_'), loads tables metadata into memory and if successful
  // loads the tablets metadata.
  Status VisitTablesAndTabletsUnlocked();
  // This is called by tests only.
  Status VisitTablesAndTablets();

  // Helper for initializing 'sys_catalog_'. After calling this
  // method, the caller should call WaitUntilRunning() on sys_catalog_
  // WITHOUT holding 'lock_' to wait for consensus to start for
  // sys_catalog_.
  //
  // This method is thread-safe.
  Status InitSysCatalogAsync(bool is_first_run);

  // Load existing root CA information from the system table, if present.
  // If not, generate new ones, store them into the system table.
  // After that, use the CA information to initialize the certificate authority.
  Status CheckInitCertAuthority();

  // Helper for creating the initial TableInfo state
  // Leaves the table "write locked" with the new info in the
  // "dirty" state field.
  TableInfo* CreateTableInfo(const CreateTableRequestPB& req,
                             const Schema& schema,
                             const PartitionSchema& partition_schema);

  // Helper for creating the initial TabletInfo state.
  // Leaves the tablet "write locked" with the new info in the
  // "dirty" state field.
  scoped_refptr<TabletInfo> CreateTabletInfo(TableInfo* table,
                                             const PartitionPB& partition);

  // Builds the TabletLocationsPB for a tablet based on the provided TabletInfo.
  // Populates locs_pb and returns true on success.
  // Returns Status::ServiceUnavailable if tablet is not running.
  Status BuildLocationsForTablet(const scoped_refptr<TabletInfo>& tablet,
                                 TabletLocationsPB* locs_pb);

  Status FindTable(const TableIdentifierPB& table_identifier,
                   scoped_refptr<TableInfo>* table_info);

  // Handle one of the tablets in a tablet reported.
  // Requires that the lock is already held.
  Status HandleReportedTablet(TSDescriptor* ts_desc,
                              const ReportedTabletPB& report,
                              ReportedTabletUpdatesPB *report_updates);

  Status HandleRaftConfigChanged(const ReportedTabletPB& report,
                                 const scoped_refptr<TabletInfo>& tablet,
                                 TabletMetadataLock* tablet_lock,
                                 TableMetadataLock* table_lock);

  // Extract the set of tablets that must be processed because not running yet.
  void ExtractTabletsToProcess(std::vector<scoped_refptr<TabletInfo>>* tablets_to_process);

  Status ApplyAlterSchemaSteps(const SysTablesEntryPB& current_pb,
                               std::vector<AlterTableRequestPB::Step> steps,
                               Schema* new_schema,
                               ColumnId* next_col_id);

  Status ApplyAlterPartitioningSteps(const TableMetadataLock& l,
                                     TableInfo* table,
                                     const Schema& client_schema,
                                     std::vector<AlterTableRequestPB::Step> steps,
                                     std::vector<scoped_refptr<TabletInfo>>* tablets_to_add,
                                     std::vector<scoped_refptr<TabletInfo>>* tablets_to_drop);

  // Task that takes care of the tablet assignments/creations.
  // Loops through the "not created" tablets and sends a CreateTablet() request.
  Status ProcessPendingAssignments(const std::vector<scoped_refptr<TabletInfo> >& tablets);

  // Given 'two_choices', which should be a vector of exactly two elements, select which
  // one is the better choice for a new replica.
  std::shared_ptr<TSDescriptor> PickBetterReplicaLocation(const TSDescriptorVector& two_choices);

  // Select a tablet server from 'ts_descs' on which to place a new replica.
  // Any tablet servers in 'excluded' are not considered.
  // REQUIRES: 'ts_descs' must include at least one non-excluded server.
  std::shared_ptr<TSDescriptor> SelectReplica(
      const TSDescriptorVector& ts_descs,
      const std::set<std::shared_ptr<TSDescriptor>>& excluded);

  // Select N Replicas from online tablet servers (as specified by
  // 'ts_descs') for the specified tablet and populate the consensus configuration
  // object. If 'ts_descs' does not specify enough online tablet
  // servers to select the N replicas, return Status::InvalidArgument.
  //
  // This method is called by "ProcessPendingAssignments()".
  Status SelectReplicasForTablet(const TSDescriptorVector& ts_descs, TabletInfo* tablet);

  // Select N Replicas from the online tablet servers
  // and populate the consensus configuration object.
  //
  // This method is called by "SelectReplicasForTablet".
  void SelectReplicas(const TSDescriptorVector& ts_descs,
                      int nreplicas,
                      consensus::RaftConfigPB *config);

  void HandleAssignPreparingTablet(TabletInfo* tablet,
                                   DeferredAssignmentActions* deferred);

  // Assign tablets and send CreateTablet RPCs to tablet servers.
  // The out param 'new_tablets' should have any newly-created TabletInfo
  // objects appended to it.
  void HandleAssignCreatingTablet(TabletInfo* tablet,
                                  DeferredAssignmentActions* deferred,
                                  std::vector<scoped_refptr<TabletInfo> >* new_tablets);

  Status HandleTabletSchemaVersionReport(TabletInfo *tablet,
                                         uint32_t version);

  // Send the "create tablet request" to all peers of a particular tablet.
  //.
  // The creation is async, and at the moment there is no error checking on the
  // caller side. We rely on the assignment timeout. If we don't see the tablet
  // after the timeout, we regenerate a new one and proceed with a new
  // assignment/creation.
  //
  // This method is part of the "ProcessPendingAssignments()"
  //
  // This must be called after persisting the tablet state as
  // CREATING to ensure coherent state after Master failover.
  //
  // The tablet lock must be acquired for reading before making this call.
  void SendCreateTabletRequest(const scoped_refptr<TabletInfo>& tablet,
                               const TabletMetadataLock& tablet_lock);

  // Send the "alter table request" to all tablets of the specified table.
  void SendAlterTableRequest(const scoped_refptr<TableInfo>& table);

  // Start the background task to send the AlterTable() RPC to the leader for this
  // tablet.
  void SendAlterTabletRequest(const scoped_refptr<TabletInfo>& tablet);

  // Send the "delete tablet request" to all replicas of all tablets of the
  // specified table.
  void SendDeleteTableRequest(const scoped_refptr<TableInfo>& table,
                              const std::string& deletion_msg);

  // Send the "delete tablet request" to all replicas of the specified tablet.
  //
  // The tablet lock must be acquired for reading before making this call.
  void SendDeleteTabletRequest(const scoped_refptr<TabletInfo>& tablet,
                               const TabletMetadataLock& tablet_lock,
                               const std::string& deletion_msg);

  // Send the "delete tablet request" to a particular replica (i.e. TS and
  // tablet combination). The specified 'reason' will be logged on the TS.
  void SendDeleteReplicaRequest(const std::string& tablet_id,
                                tablet::TabletDataState delete_type,
                                const boost::optional<int64_t>& cas_config_opid_index_less_or_equal,
                                const scoped_refptr<TableInfo>& table,
                                const std::string& ts_uuid,
                                const std::string& reason);

  // Start a task to change the config to add an additional voter because the
  // specified tablet is under-replicated.
  void SendAddServerRequest(const scoped_refptr<TabletInfo>& tablet,
                            const consensus::ConsensusStatePB& cstate);

  std::string GenerateId() { return oid_generator_.Next(); }

  // Conventional "T xxx P yyy: " prefix for logging.
  std::string LogPrefix() const;

  // Aborts all tasks belonging to 'tables' and waits for them to finish.
  void AbortAndWaitForAllTasks(const std::vector<scoped_refptr<TableInfo>>& tables);

  // TODO: the maps are a little wasteful of RAM, since the TableInfo/TabletInfo
  // objects have a copy of the string key. But STL doesn't make it
  // easy to make a "gettable set".

  // Lock protecting the various maps and sets below.
  typedef rw_spinlock LockType;
  mutable LockType lock_;

  // Table maps: table-id -> TableInfo and table-name -> TableInfo
  TableInfoMap table_ids_map_;
  TableInfoMap table_names_map_;

  // Tablet maps: tablet-id -> TabletInfo
  TabletInfoMap tablet_map_;

  // Names of tables that are currently reserved by CreateTable() or
  // AlterTable().
  //
  // As a rule, operations that add new table names should do so as follows:
  // 1. Acquire lock_.
  // 2. Ensure table_names_map_ does not contain the new name.
  // 3. Ensure reserved_table_names_ does not contain the new name.
  // 4. Add the new name to reserved_table_names_.
  // 5. Release lock_.
  // 6. Perform the operation.
  // 7. If it succeeded, add the name to table_names_map_ with lock_ held.
  // 8. Remove the new name from reserved_table_names_ with lock_ held.
  std::unordered_set<std::string> reserved_table_names_;

  Master *master_;
  ObjectIdGenerator oid_generator_;

  // Random number generator used for selecting replica locations.
  ThreadSafeRandom rng_;

  gscoped_ptr<SysCatalogTable> sys_catalog_;

  // Background thread, used to execute the catalog manager tasks
  // like the assignment and cleaner
  friend class CatalogManagerBgTasks;
  gscoped_ptr<CatalogManagerBgTasks> background_tasks_;

  enum State {
    kConstructed,
    kStarting,
    kRunning,
    kClosing
  };

  // Lock protecting state_, leader_ready_term_
  mutable simple_spinlock state_lock_;
  State state_;

  // Singleton pool that serializes invocations of ElectedAsLeaderCb().
  gscoped_ptr<ThreadPool> leader_election_pool_;

  // This field is updated when a node becomes leader master,
  // waits for all outstanding uncommitted metadata (table and tablet metadata)
  // in the sys catalog to commit, and then reads that metadata into in-memory
  // data structures. This is used to "fence" client and tablet server requests
  // that depend on the in-memory state until this master can respond
  // correctly.
  int64_t leader_ready_term_;

  // Lock used to fence operations and leader elections. All logical operations
  // (i.e. create table, alter table, etc.) should acquire this lock for
  // reading. Following an election where this master is elected leader, it
  // should acquire this lock for writing before reloading the metadata.
  //
  // Readers should not acquire this lock directly; use ScopedLeadershipLock
  // instead.
  //
  // Always acquire this lock before state_lock_.
  RWMutex leader_lock_;

  // Async operations are accessing some private methods
  // (TODO: this stuff should be deferred and done in the background thread)
  friend class AsyncAlterTable;

  DISALLOW_COPY_AND_ASSIGN(CatalogManager);
};

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_CATALOG_MANAGER_H */
