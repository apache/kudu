// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_MASTER_CATALOG_MANAGER_H
#define KUDU_MASTER_CATALOG_MANAGER_H

#include <map>
#include <string>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#include <vector>

#include "gutil/macros.h"
#include "gutil/ref_counted.h"
#include "util/monotime.h"
#include "master/master.pb.h"
#include "server/oid_generator.h"
#include "util/locks.h"
#include "util/status.h"

namespace kudu {

class Schema;

namespace rpc {
class RpcContext;
} // namespace rpc

namespace master {

class Master;
class SysTablesTable;
class SysTabletsTable;
class TableInfo;
class TSDescriptor;

// The information about a single tablet which exists in the cluster,
// including its state and locations.
//
// Requires external synchronization.
// The object is owned/managed by the CatalogManager, and exposed for testing.
class TabletInfo {
 public:
  TabletInfo(TableInfo *table, const std::string& tablet_id);
  ~TabletInfo();

  // Add a replica reported on the given server
  void AddReplica(TSDescriptor* ts_desc);

  // Remove any replicas which were on this server.
  void ClearReplicasOnTS(const TSDescriptor* ts_desc);

  TableInfo *table() { return table_; }
  const TableInfo *table() const { return table_; }

  const std::string& tablet_id() const { return tablet_id_; }
  const std::vector<TSDescriptor*> locations() const { return locations_; }

  bool is_running() const {
    return current_metadata_->state() == SysTabletsEntryPB::kTabletStateRunning;
  }

  bool is_deleted() const {
    return current_metadata_->state() == SysTabletsEntryPB::kTabletStateReplaced;
  }

  // Helper to set the state of the tablet with a custom message.
  // This change will be visible only in staging_metadata() until Commit()
  void set_state(SysTabletsEntryPB::State state, const string& msg);

  // Returns the SysTabletsEntryPB for the current committed metadata
  const SysTabletsEntryPB& metadata() const { return *current_metadata_; }

  // Returns the SysTabletsEntryPB for the latest metadata, including pending mutations
  const SysTabletsEntryPB& staging_metadata() const { return metadata_; }

  // Returns a mutable pointer to the staging_metadata().
  // A new SysTabletsEntryPB will be created with a copy of the current metadata.
  // The new entry will be used as current metadata(), and it will be released on Commit().
  // Multiple calls to mutable_metadata() will not trigger a new entry creation.
  SysTabletsEntryPB *mutable_metadata();

  // Returns true if there are no pending mutations.
  bool is_committed() const;

  // Makes the changes applied to the disk-metadata visible to the metadata() user.
  // This operation will release the SysTabletsEntryPB allocated by mutable_metadata().
  void Commit();

 private:
  friend class CatalogManager;

  const std::string tablet_id_;
  TableInfo *table_;
  MonoTime last_update_ts_;

  // The locations where this tablet has been reported.
  // TODO: this probably will turn into a struct which also includes
  // some state information at some point.
  std::vector<TSDescriptor*> locations_;

  // The current metadata:
  //  - if there are no mutation pending (is_committed() == true)
  //    current_metadata_ will be equals to &metadata_
  //  - if there are mutations pending (is_committed() == false)
  //    current_metadata_ will point to the SysTabletsEntryPB allocated on
  //    mutate_metadata(). The entry will be released on Commit().
  SysTabletsEntryPB *current_metadata_;
  SysTabletsEntryPB metadata_;

  DISALLOW_COPY_AND_ASSIGN(TabletInfo);
};

// The information about a table, including its state and tablets.
//
// Requires external synchronization.
// The object is owned/managed by the CatalogManager, and exposed for testing.
class TableInfo {
 public:
  explicit TableInfo(const std::string& table_id);
  ~TableInfo();

  const std::string& id() const { return table_id_; }
  const std::string& name() const { return current_metadata_->name(); }

  bool is_deleted() const {
    return current_metadata_->state() == SysTablesEntryPB::kTableStateRemoved;
  }

  bool is_running() const {
    return current_metadata_->state() == SysTablesEntryPB::kTableStateRunning;
  }

  // Returns the SysTablesEntryPB for the current committed metadata
  const SysTablesEntryPB& metadata() const { return *current_metadata_; }

  // Returns the SysTablesEntryPB for the latest metadata, including pending mutations
  const SysTablesEntryPB& staging_metadata() const { return metadata_; }

  void AddTablet(TabletInfo *tablet);
  void AddTablets(const vector<TabletInfo*>& tablets);

  // Helper to set the state of the tablet with a custom message.
  // This change will be visible only in staging_metadata() until Commit()
  void set_state(SysTablesEntryPB::State state, const string& msg);

  // Returns a mutable pointer to the staging_metadata().
  // A new SysTablesEntryPB will be created with a copy of the current metadata.
  // The new entry will be used as current metadata(), and it will be released on Commit().
  // Multiple calls to mutable_metadata() will not trigger a new entry creation.
  SysTablesEntryPB *mutable_metadata();

  // Returns true if there are no pending mutations.
  bool is_committed() const;

  // Makes the changes applied to the disk-metadata visible to the metadata() user.
  // This operation will release the SysTablesEntryPB allocated by mutable_metadata().
  void Commit();

 private:
  friend class CatalogManager;

  const std::string table_id_;

  // The current metadata:
  //  - if there are no mutation pending (is_committed() == true)
  //    current_metadata_ will be equals to &metadata_
  //  - if there are mutations pending (is_committed() == false)
  //    current_metadata_ will point to the SysTablesEntryPB allocated on
  //    mutate_metadata(). The entry will be released on Commit().
  SysTablesEntryPB *current_metadata_;
  SysTablesEntryPB metadata_;

  // Tablet map start-key/info
  typedef std::map<std::string, TabletInfo *> TabletInfoMap;
  TabletInfoMap tablet_map_;

  DISALLOW_COPY_AND_ASSIGN(TableInfo);
};


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

  // Create a new Table with the specified attributes
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status CreateTable(const CreateTableRequestPB* req,
                     CreateTableResponsePB* resp,
                     rpc::RpcContext* rpc);

  // Delete the specified table
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status DeleteTable(const DeleteTableRequestPB* req,
                     DeleteTableResponsePB* resp,
                     rpc::RpcContext* rpc);

  // List all the running tables
  Status ListTables(const ListTablesRequestPB* req,
                    ListTablesResponsePB* resp);

  // Look up the locations of the given tablet. The locations
  // vector is overwritten (not appended to).
  // If the tablet is not found, clears the result vector.
  void GetTabletLocations(const std::string& tablet_id,
                          std::vector<TSDescriptor*>* locations);

  // Handle a tablet report from the given tablet server.
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status ProcessTabletReport(TSDescriptor* ts_desc,
                             const TabletReportPB& report,
                             rpc::RpcContext* rpc);

  SysTablesTable *sys_tables() { return sys_tables_.get(); }
  SysTabletsTable *sys_tablets() { return sys_tablets_.get(); }

 private:
  friend class TableLoader;
  friend class TabletLoader;

  // Helper for creating the inital Tablets of the table
  // based on the split-keys field in the request.
  void CreateTablets(const CreateTableRequestPB* req,
                     TableInfo *table,
                     vector<TabletInfo *> *tablets);

  // Helper for creating the initial TableInfo state
  TableInfo *CreateTableInfo(const string& name,
                             const Schema& schema);

  // Helper for creating the initial TabletInfo state
  TabletInfo *CreateTabletInfo(TableInfo *table,
                               const string& start_key,
                               const string& end_key);

  Status FindTable(const TableIdentifierPB& table_identifier,
                   TableInfo **table_info);

  // Handle one of the tablets in a tablet reported.
  // Requires that the lock is already held.
  Status HandleReportedTablet(TSDescriptor* ts_desc,
                              const ReportedTabletPB& report);

  void ClearAllReplicasOnTS(TSDescriptor* ts_desc);


  string GenerateId() { return oid_generator_.Next(); }

  typedef rw_spinlock LockType;
  LockType lock_;

  // TODO: the maps are a little wasteful of RAM, since the TableInfo/TabletInfo
  // objects have a copy of the string key. But STL doesn't make it
  // easy to make a "gettable set".

  // Table maps: table-id -> TableInfo and table-name -> TableInfo
  // The CatalogManager owns all the TableInfo objects
  typedef std::tr1::unordered_map<std::string, TableInfo *> TableInfoMap;
  TableInfoMap table_ids_map_;
  TableInfoMap table_names_map_;

  // Tablet maps: tablet-id -> TabletInfo
  // The CatalogManager owns all the TabletInfo objects
  typedef std::tr1::unordered_map<std::string, TabletInfo*> TabletInfoMap;
  TabletInfoMap tablet_map_;

  Master *master_;
  ObjectIdGenerator oid_generator_;
  gscoped_ptr<SysTablesTable> sys_tables_;
  gscoped_ptr<SysTabletsTable> sys_tablets_;

  DISALLOW_COPY_AND_ASSIGN(CatalogManager);
};

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_CATALOG_MANAGER_H */
