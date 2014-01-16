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
#include "server/oid_generator.h"
#include "util/cow_object.h"
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

  // Helper to set the state of the tablet with a custom message.
  // Requires that the caller has prepared this object for write.
  // The change will only be visible after Commit().
  void set_state(SysTabletsEntryPB::State state, const string& msg);

  SysTabletsEntryPB pb;
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

  TabletInfo(TableInfo *table, const std::string& tablet_id);

  // Add a replica reported on the given server
  // TODO: figure out locking for this.
  void AddReplica(TSDescriptor* ts_desc);

  // Remove any replicas which were on this server.
  void ClearReplicasOnTS(const TSDescriptor* ts_desc);

  TableInfo *table() { return table_; }
  const TableInfo *table() const { return table_; }

  // Does not require synchronization.
  const std::string& tablet_id() const { return tablet_id_; }

  void GetLocations(std::vector<TSDescriptor*>* locations) const;

  // Access the persistent metadata. Typically you should use
  // TabletMetadataLock to gain access to this data.
  const CowObject<PersistentTabletInfo>& metadata() const {return metadata_; }
  CowObject<PersistentTabletInfo>& metadata() {return metadata_; }

 private:
  friend class base::RefCountedThreadSafe<TabletInfo>;
  ~TabletInfo();

  const std::string tablet_id_;
  TableInfo *table_;
  MonoTime last_update_ts_;

  // The locations where this tablet has been reported.
  // TODO: this probably will turn into a struct which also includes
  // some state information at some point.
  std::vector<TSDescriptor*> locations_;

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
    return pb.state() == SysTablesEntryPB::kTableStateRunning;
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

  // Return the table's ID. Does not require synchronization.
  const std::string& id() const { return table_id_; }

  // Add a tablet to this table.
  void AddTablet(TabletInfo *tablet);
  // Add multiple tablets to this table.
  void AddTablets(const vector<TabletInfo*>& tablets);

  // Access the persistent metadata. Typically you should use
  // TableMetadataLock to gain access to this data.
  const CowObject<PersistentTableInfo>& metadata() const { return metadata_; }
  CowObject<PersistentTableInfo>& metadata() { return metadata_; }

 private:
  friend class base::RefCountedThreadSafe<TableInfo>;
  ~TableInfo();

  const std::string table_id_;

  // Tablet map start-key/info
  typedef std::map<std::string, TabletInfo *> TabletInfoMap;
  TabletInfoMap tablet_map_;

  // Protects tablet_map_
  mutable simple_spinlock lock_;

  CowObject<PersistentTableInfo> metadata_;

  DISALLOW_COPY_AND_ASSIGN(TableInfo);
};

// Helper to manage locking on the persistent metadata of TabletInfo or TableInfo.
template<class MetadataClass>
class MetadataLock : public CowLock<typename MetadataClass::cow_state> {
 public:
  typedef CowLock<typename MetadataClass::cow_state> super;
  MetadataLock(MetadataClass* info,
               typename super::LockMode mode)
    : super(&info->metadata(), mode) {}
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
                   scoped_refptr<TableInfo>* table_info);

  // Handle one of the tablets in a tablet reported.
  // Requires that the lock is already held.
  Status HandleReportedTablet(TSDescriptor* ts_desc,
                              const ReportedTabletPB& report);

  void ClearAllReplicasOnTS(TSDescriptor* ts_desc);


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
  LockType lock_;

  Master *master_;
  ObjectIdGenerator oid_generator_;
  gscoped_ptr<SysTablesTable> sys_tables_;
  gscoped_ptr<SysTabletsTable> sys_tablets_;

  DISALLOW_COPY_AND_ASSIGN(CatalogManager);
};

} // namespace master
} // namespace kudu
#endif /* KUDU_MASTER_CATALOG_MANAGER_H */
