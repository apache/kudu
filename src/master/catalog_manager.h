// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_CATALOG_MANAGER_H
#define KUDU_MASTER_CATALOG_MANAGER_H

#include <boost/thread/mutex.hpp>
#include <map>
#include <string>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include <vector>

#include "gutil/macros.h"
#include "gutil/ref_counted.h"
#include "util/monotime.h"
#include "master/master.pb.h"
#include "master/ts_manager.h"
#include "server/oid_generator.h"
#include "server/monitored_task.h"
#include "util/cow_object.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {

class Schema;

namespace rpc {
class RpcContext;
} // namespace rpc

namespace master {

class CatalogManagerBgTasks;
class Master;
class SysTablesTable;
class SysTabletsTable;
class TableInfo;
class TSDescriptor;

struct DeferredAssignmentActions;

// The data related to a tablet which is persisted on disk.
// This portion of TableInfo is managed via CowObject.
// It wraps the underlying protobuf to add useful accessors.
struct PersistentTabletInfo {
  bool is_running() const {
    return pb.state() == SysTabletsEntryPB::kTabletStateRunning;
  }

  bool is_deleted() const {
    return pb.state() == SysTabletsEntryPB::kTabletStateReplaced;
  }

  // Returns true if the specified 'ts_desc' is the leader of the quorum
  bool IsQuorumLeader(const TSDescriptor* ts_desc) const;

  // Helper to set the state of the tablet with a custom message.
  // Requires that the caller has prepared this object for write.
  // The change will only be visible after Commit().
  void set_state(SysTabletsEntryPB::State state, const string& msg);

  SysTabletsEntryPB pb;
};

// Information on a current replica of a tablet.
// This is copyable so that no locking is needed.
struct TabletReplica {
  TSDescriptor* ts_desc;
  metadata::TabletStatePB state;
  metadata::QuorumPeerPB::Role role;
};

// The information about a single tablet which exists in the cluster,
// including its state and locations.
//
// This object uses copy-on-write for the portions of data which are persisted
// on disk. This allows the mutated data to be staged and written to disk
// while readers continue to access the previous version. These portions
// of data are in PersistentTableInfo above, and typically accessed using
// TableMetadataLock. For example:
//
//   TableInfo* table = ...;
//   TableMetadataLock l(table, TableMetadataLock::READ);
//   if (l.data().is_running()) { ... }
//
// The non-persistent information about the table is protected by an internal
// spin-lock.
//
// The object is owned/managed by the CatalogManager, and exposed for testing.
class TabletInfo : public base::RefCountedThreadSafe<TabletInfo> {
 public:
  typedef PersistentTabletInfo cow_state;

  TabletInfo(const scoped_refptr<TableInfo>& table, const std::string& tablet_id);

  // Add a replica reported on the given server
  void AddReplica(TSDescriptor* ts_desc,
                  metadata::TabletStatePB state,
                  metadata::QuorumPeerPB::Role role);

  // Remove any replicas which were on this server.
  void ClearReplicasOnTS(const TSDescriptor* ts_desc);

  std::string ToString() const;

  TableInfo *table() { return table_.get(); }
  const TableInfo *table() const { return table_.get(); }

  // Does not require synchronization.
  const std::string& tablet_id() const { return tablet_id_; }

  void GetLocations(std::vector<TabletReplica>* locations) const;

  MonoDelta TimeSinceLastUpdate(const MonoTime& now) const {
    return now.GetDeltaSince(last_update_ts_);
  }

  void set_last_update_ts(const MonoTime& ts) {
    last_update_ts_ = ts;
  }

  // Update the Reported schema version
  bool set_reported_schema_version(uint32_t version);
  uint32_t reported_schema_version() const {
    return reported_schema_version_;
  }

  // Access the persistent metadata. Typically you should use
  // TabletMetadataLock to gain access to this data.
  const CowObject<PersistentTabletInfo>& metadata() const { return metadata_; }
  CowObject<PersistentTabletInfo>* mutable_metadata() { return &metadata_; }

 private:
  friend class base::RefCountedThreadSafe<TabletInfo>;
  ~TabletInfo();

  const std::string tablet_id_;
  scoped_refptr<TableInfo> table_;
  MonoTime last_update_ts_;

  // The locations where this tablet has been reported.
  // TODO: this probably will turn into a struct which also includes
  // some state information at some point.
  std::vector<TabletReplica> locations_;

  // Reported schema version (in-memory only)
  uint32_t reported_schema_version_;

  // Lock protecting locations_ and last_update_ts_.
  // This doesn't protect metadata_ (the on-disk portion).
  mutable simple_spinlock lock_;

  CowObject<PersistentTabletInfo> metadata_;

  DISALLOW_COPY_AND_ASSIGN(TabletInfo);
};

// The data related to a table which is persisted on disk.
// This portion of TableInfo is managed via CowObject.
// It wraps the underlying protobuf to add useful accessors.
struct PersistentTableInfo {
  bool is_deleted() const {
    return pb.state() == SysTablesEntryPB::kTableStateRemoved;
  }

  bool is_running() const {
    return pb.state() == SysTablesEntryPB::kTableStateRunning ||
           pb.state() == SysTablesEntryPB::kTableStateAltering;
  }

  // Return the table's name.
  const std::string& name() const {
    return pb.name();
  }

  // Helper to set the state of the tablet with a custom message.
  void set_state(SysTablesEntryPB::State state, const string& msg);

  SysTablesEntryPB pb;
};

// The information about a table, including its state and tablets.
//
// This object uses copy-on-write techniques similarly to TabletInfo.
// Please see the TabletInfo class doc above for more information.
//
// The non-persistent information about the table is protected by an internal
// spin-lock.
class TableInfo : public base::RefCountedThreadSafe<TableInfo> {
 public:
  typedef PersistentTableInfo cow_state;

  explicit TableInfo(const std::string& table_id);

  std::string ToString() const;

  // Return the table's ID. Does not require synchronization.
  const std::string& id() const { return table_id_; }

  // Add a tablet to this table.
  void AddTablet(TabletInfo *tablet);
  // Add multiple tablets to this table.
  void AddTablets(const vector<TabletInfo*>& tablets);

  // This only returns tablets which are in RUNNING state.
  void GetTabletsInRange(const GetTableLocationsRequestPB* req,
                         vector<scoped_refptr<TabletInfo> > *ret) const;

  void GetAllTablets(vector<scoped_refptr<TabletInfo> > *ret) const;

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
  void GetTaskList(std::vector<scoped_refptr<MonitoredTask> > *tasks);

 private:
  friend class base::RefCountedThreadSafe<TableInfo>;
  ~TableInfo();

  void AddTabletUnlocked(TabletInfo* tablet);

  const std::string table_id_;

  // Tablet map start-key/info.
  // The TabletInfo objects are owned by the CatalogManager.
  typedef std::map<std::string, TabletInfo *> TabletInfoMap;
  TabletInfoMap tablet_map_;

  // Protects tablet_map_ and pending_tasks_
  mutable simple_spinlock lock_;

  CowObject<PersistentTableInfo> metadata_;

  // List of pending tasks (e.g. create/alter tablet requests)
  std::tr1::unordered_set<MonitoredTask*> pending_tasks_;

  DISALLOW_COPY_AND_ASSIGN(TableInfo);
};

// Helper to manage locking on the persistent metadata of TabletInfo or TableInfo.
template<class MetadataClass>
class MetadataLock : public CowLock<typename MetadataClass::cow_state> {
 public:
  typedef CowLock<typename MetadataClass::cow_state> super;
  MetadataLock(MetadataClass* info,
               typename super::LockMode mode)
    : super(info->mutable_metadata(), mode) {}
  MetadataLock(const MetadataClass* info,
               typename super::LockMode mode)
    : super(&info->metadata(), mode) {}
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
class CatalogManager {
 public:
  explicit CatalogManager(Master *master);
  ~CatalogManager();

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

  Status GetTableLocations(const GetTableLocationsRequestPB* req,
                           GetTableLocationsResponsePB* resp);

  // Look up the locations of the given tablet. The locations
  // vector is overwritten (not appended to).
  // If the tablet is not found, return false and true otherwise.
  // This only returns tablets which are in RUNNING state.
  bool GetTabletLocations(const std::string& tablet_id,
                          TabletLocationsPB* locs_pb);

  // Handle a tablet report from the given tablet server.
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status ProcessTabletReport(TSDescriptor* ts_desc,
                             const TabletReportPB& report,
                             TabletReportUpdatesPB *report_update,
                             rpc::RpcContext* rpc);

  SysTablesTable *sys_tables() { return sys_tables_.get(); }
  SysTabletsTable *sys_tablets() { return sys_tablets_.get(); }

  // Dump all of the current state about tables and tablets to the
  // given output stream. This is verbose, meant for debugging.
  void DumpState(std::ostream* out) const;

  // Return true if the table with the specified ID exists,
  // and set the table pointer to the TableInfo object
  // NOTE: This should only be used by tests or web-ui
  bool GetTableInfo(const string& table_id, scoped_refptr<TableInfo> *table);

  // Return all the available TableInfo, which also may include not running tables
  // NOTE: This should only be used by tests or web-ui
  void GetAllTables(std::vector<scoped_refptr<TableInfo> > *tables);

  // Return true if the specified table name exists
  // NOTE: This should only be used by tests
  bool TableNameExists(const string& table_name);

  // Let the catalog manager know that the the given tablet server successfully
  // deleted the specified tablet.
  void NotifyTabletDeleteSuccess(const std::string& permanent_uuid, const std::string& tablet_id);

 private:
  friend class TableLoader;
  friend class TabletLoader;

  // Helper for creating the inital Tablets of the table
  // based on the split-keys field in the request.
  // Leaves the tablets "write locked" with the new info in the
  // "dirty" state field.
  void CreateTablets(const CreateTableRequestPB* req,
                     TableInfo *table,
                     vector<TabletInfo*> *tablets);

  // Helper for creating the initial TableInfo state
  // Leaves the table "write locked" with the new info in the
  // "dirty" state field.
  TableInfo *CreateTableInfo(const CreateTableRequestPB* req,
                             const Schema& schema);

  // Helper for creating the initial TabletInfo state.
  // Leaves the tablet "write locked" with the new info in the
  // "dirty" state field.
  TabletInfo *CreateTabletInfo(TableInfo *table,
                               const string& start_key,
                               const string& end_key);

  // Builds the TabletLocationsPB for based on the provided TabletInfo.
  bool BuildLocationsForTablet(const scoped_refptr<TabletInfo>& tablet,
                               TabletLocationsPB* locs_pb);

  Status FindTable(const TableIdentifierPB& table_identifier,
                   scoped_refptr<TableInfo>* table_info);

  // Handle one of the tablets in a tablet reported.
  // Requires that the lock is already held.
  Status HandleReportedTablet(TSDescriptor* ts_desc,
                              const ReportedTabletPB& report,
                              ReportedTabletUpdatesPB *report_updates);

  void ClearAllReplicasOnTS(TSDescriptor* ts_desc);

  // Extract the set of tablets that can be deleted and the set of tablets
  // that must be processed because not running yet.
  void ExtractTabletsToProcess(std::vector<scoped_refptr<TabletInfo> > *tablets_to_delete,
                               std::vector<scoped_refptr<TabletInfo> > *tablets_to_process);

  // Task that takes care of the tablet assignments/creations.
  // loops through the "not created" tablets and send a create request.
  void ProcessPendingAssignments(
    const std::vector<scoped_refptr<TabletInfo> >& tablets,
    int *next_timeout_ms);

  void SelectReplicasForTablets(const vector<TabletInfo*>& tablets,
                                const TSDescriptorVector& ts_descs);

  // Select N Replicas from the online tablet servers
  // and populate the quorum object.
  //
  // This method is part of the "ProcessPendingAssignments()"
  void SelectReplicas(metadata::QuorumPB *quorum,
                      const TSDescriptorVector& ts_descs,
                      int nreplicas);

  void HandleAssignPreparingTablet(TabletInfo* tablet,
                                   DeferredAssignmentActions* deferred);
  void HandleAssignCreatingTablet(TabletInfo* tablet,
                                  DeferredAssignmentActions* deferred);
  void HandleTabletSchemaVersionReport(TabletInfo *tablet,
                                       uint32_t version);

  // Send the create tablet requests to the selected peers of the quorums.
  // The creation is async, and at the moment there is no error checking on the
  // caller side. We rely on the assignment timeout. If we don't see the tablet
  // after the timeout, we regenerate a new one and proceed with a new
  // assignment/creation.
  //
  // This method is part of the "ProcessPendingAssignments()"
  void SendCreateTabletRequests(const vector<TabletInfo*>& tablets);

  // Send the "alter table request" to all TS that have tablet of the specified table
  void SendAlterTableRequest(const scoped_refptr<TableInfo>& table);

  // Send the "alter table request" to all the TS running the specified tablet
  void SendAlterTabletRequest(const scoped_refptr<TabletInfo>& tablet);

  // Send the "alter table request" to the specified TS/tablet
  void SendAlterTabletRequest(const scoped_refptr<TabletInfo>& tablet,
                              TSDescriptor* ts_desc);

  // Send the "delete tablet request" to all TS that have tablet of the specified table
  void SendDeleteTableRequest(const scoped_refptr<TableInfo>& table);

  // Send the "delete tablet request" to the specified TS/tablet.
  // The specified 'reason' will be logged on the TS.
  void SendDeleteTabletRequest(const std::string& tablet_id,
                               const scoped_refptr<TableInfo>& table,
                               TSDescriptor* ts_desc,
                               const std::string& reason);

  string GenerateId() { return oid_generator_.Next(); }


  // TODO: the maps are a little wasteful of RAM, since the TableInfo/TabletInfo
  // objects have a copy of the string key. But STL doesn't make it
  // easy to make a "gettable set".

  // Table maps: table-id -> TableInfo and table-name -> TableInfo
  typedef std::tr1::unordered_map<std::string, scoped_refptr<TableInfo> > TableInfoMap;
  TableInfoMap table_ids_map_;
  TableInfoMap table_names_map_;

  // Tablet maps: tablet-id -> TabletInfo
  typedef std::tr1::unordered_map<std::string, scoped_refptr<TabletInfo> > TabletInfoMap;
  TabletInfoMap tablet_map_;

  // Lock protecting the various maps above.
  typedef rw_spinlock LockType;
  mutable LockType lock_;

  Master *master_;
  Atomic32 closing_;
  ObjectIdGenerator oid_generator_;
  gscoped_ptr<SysTablesTable> sys_tables_;
  gscoped_ptr<SysTabletsTable> sys_tablets_;

  // Background thread, used to execute the catalog manager tasks
  // like the assignment and cleaner
  friend class CatalogManagerBgTasks;
  gscoped_ptr<CatalogManagerBgTasks> background_tasks_;

  // Async operations are accessing some private methods
  // (TODO: this stuff should be deferred and done in the background thread)
  friend class AsyncAlterTable;

  DISALLOW_COPY_AND_ASSIGN(CatalogManager);
};

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_CATALOG_MANAGER_H */
