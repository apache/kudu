// Copyright (c) 2013, Cloudera, inc.
//
// The catalog manager handles the current list of tables
// and tablets in the cluster, as well as their current locations.
// Since most operations in the master go through these data
// structures, locking is carefully managed here to prevent unnecessary
// contention and deadlocks:
//
// - each structure has an internal spinlock used for operations that
//   are purely in-memory (eg the current status of replicas)
// - data that is persisted on disk is stored in separate PersistentTable(t)Info
//   structs. These are managed using copy-on-write so that writers may block
//   writing them back to disk while not impacting concurrent readers.
//
// Usage rules:
// - You may obtain READ locks in any order. READ locks should never block,
//   since they only conflict with COMMIT which is a purely in-memory operation.
//   Thus they are deadlock-free.
// - If you need a WRITE lock on both a table and one or more of its tablets,
//   acquire the lock on the table first. This strict ordering prevents deadlocks.

#include "master/catalog_manager.h"

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>

#include <algorithm>
#include <string>
#include <vector>

#include "common/wire_protocol.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "gutil/walltime.h"
#include "master/master.h"
#include "master/master.pb.h"
#include "master/sys_tables.h"
#include "master/ts_descriptor.h"
#include "master/ts_manager.h"
#include "tserver/tserver_service.proxy.h"
#include "rpc/rpc_context.h"

namespace kudu {
namespace master {

using strings::Substitute;
using rpc::RpcContext;

////////////////////////////////////////////////////////////
// Table Loader
////////////////////////////////////////////////////////////

class TableLoader : public SysTablesTable::Visitor {
 public:
  explicit TableLoader(CatalogManager *table_manager)
    : table_manager_(table_manager) {
  }

  virtual Status VisitTable(const std::string& table_id,
                            const SysTablesEntryPB& metadata) {
    CHECK(!ContainsKey(table_manager_->table_ids_map_, table_id))
          << "Table already exists: " << table_id;

    // Setup the table info
    TableInfo *table = new TableInfo(table_id);
    TableMetadataLock l(table, TableMetadataLock::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);

    // Add the tablet to the IDs map and to the name map (if the table is not deleted)
    table_manager_->table_ids_map_[table->id()] = table;
    if (!l.data().is_deleted()) {
      table_manager_->table_names_map_[l.data().name()] = table;
    }

    LOG(INFO) << "Loaded table " << l.data().name() << " [id=" << table_id << "]";
    if (VLOG_IS_ON(1)) {
      VLOG(1) << "Metadata: " << metadata.DebugString();
    }
    l.Commit();
    return Status::OK();
  }

 private:
  CatalogManager *table_manager_;

  DISALLOW_COPY_AND_ASSIGN(TableLoader);
};

////////////////////////////////////////////////////////////
// Tablet Loader
////////////////////////////////////////////////////////////

class TabletLoader : public SysTabletsTable::Visitor {
 public:
  explicit TabletLoader(CatalogManager *table_manager)
    : table_manager_(table_manager) {
  }

  virtual Status VisitTablet(const std::string& table_id,
                             const std::string& tablet_id,
                             const SysTabletsEntryPB& metadata) {
    // Lookup the table
    scoped_refptr<TableInfo> table(FindPtrOrNull(
                                     table_manager_->table_ids_map_, table_id));

    // Setup the tablet info
    TabletInfo* tablet = new TabletInfo(table.get(), tablet_id);
    TabletMetadataLock l(tablet, TabletMetadataLock::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);

    // Add the tablet to the tablet manager
    table_manager_->tablet_map_[tablet->tablet_id()] = tablet;

    if (table == NULL) {
      // if the table is missing and the tablet is in "preparing" state
      // may mean that the table was not created (maybe due to a failed write
      // for the sys-tablets). The cleaner will remove
      if (l.data().pb.state() == SysTabletsEntryPB::kTabletStatePreparing) {
        LOG(WARNING) << "Missing Table " << table_id << " required by tablet " << tablet_id
                     << " (probably a failed table creation: the tablet was not assigned)";
        return Status::OK();
      }

      // if the tablet is not in a "preparing" state, something is wrong...
      LOG(ERROR) << "Missing Table " << table_id << " required by tablet " << tablet_id;
      LOG(ERROR) << "Metadata: " << metadata.DebugString();
      return Status::Corruption("Missing table for tablet: ", tablet_id);
    }

    // Add the tablet to the Table
    table->AddTablet(tablet);

    TableMetadataLock table_lock(table.get(), TableMetadataLock::READ);

    LOG(INFO) << "Loaded tablet " << tablet_id << " for table "
              << table_lock.data().pb.name() << " [id=" << table_id << "]";
    if (VLOG_IS_ON(2)) {
      VLOG(2) << "Metadata: " << metadata.DebugString();
    }

    l.Commit();
    return Status::OK();
  }

 private:
  CatalogManager *table_manager_;

  DISALLOW_COPY_AND_ASSIGN(TabletLoader);
};

////////////////////////////////////////////////////////////
// CatalogManager
////////////////////////////////////////////////////////////

CatalogManager::CatalogManager(Master *master)
  : master_(master) {
}

CatalogManager::~CatalogManager() {
}

Status CatalogManager::Init(bool is_first_run) {
  boost::lock_guard<LockType> l(lock_);

  sys_tables_.reset(new SysTablesTable(master_, master_->metric_registry()));
  sys_tablets_.reset(new SysTabletsTable(master_, master_->metric_registry()));

  if (is_first_run) {
    RETURN_NOT_OK(sys_tables_->CreateNew(master_->fs_manager()));
    RETURN_NOT_OK(sys_tablets_->CreateNew(master_->fs_manager()));
  } else {
    RETURN_NOT_OK(sys_tables_->Load(master_->fs_manager()));
    RETURN_NOT_OK(sys_tablets_->Load(master_->fs_manager()));

    TableLoader table_loader(this);
    RETURN_NOT_OK(sys_tables_->VisitTables(&table_loader));

    TabletLoader tablet_loader(this);
    RETURN_NOT_OK(sys_tablets_->VisitTablets(&tablet_loader));
  }

  // TODO: Run the cleaner

  return Status::OK();
}

static void SetupError(MasterErrorPB* error,
                       MasterErrorPB::Code code,
                       const Status& s) {
  StatusToPB(s, error->mutable_status());
  error->set_code(code);
}

// Create a new table
// - Add the new table to the in-memory map in "preparing" state
// - Create the tablets metadata, based on the split-keys
// - Write the tablets metadata to sys-tablets
// - Add the tablets to the "pending assignments" list
// - Write the table metadata to the sys-tables as running (PONR)
// - Update the table metadata to "running" state
Status CatalogManager::CreateTable(const CreateTableRequestPB* req,
                                   CreateTableResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  // 0. Verify the request
  Schema schema;
  RETURN_NOT_OK(SchemaFromPB(req->schema(), &schema));
  if (schema.has_column_ids()) {
    Status s = Status::InvalidArgument("User requests should not have Column IDs");
    SetupError(resp->mutable_error(), MasterErrorPB::INVALID_SCHEMA, s);
    return s;
  }
  schema = SchemaBuilder(schema).Build();

  scoped_refptr<TableInfo> table;
  vector<TabletInfo *> tablets;
  {
    boost::lock_guard<LockType> l(lock_);

    // 1. Verify that the table does not exist
    table = FindPtrOrNull(table_names_map_, req->name());
    if (table != NULL) {
      Status s = Status::AlreadyPresent("Table already exists", table->id());
      SetupError(resp->mutable_error(), MasterErrorPB::TABLE_ALREADY_PRESENT, s);
      return s;
    }

    // 2. Add the new table in "preparing" state
    table = CreateTableInfo(req->name(), schema);
    table_ids_map_[table->id()] = table;
    table_names_map_[req->name()] = table;

    // 3. Create the Tablet Infos (state is kTabletStatePreparing)
    CreateTablets(req, table.get(), &tablets);

    // 4. Add the table/tablets to the in-memory map for the assignment
    //    NOTE: even if we have the metadata table->is_running() will return
    //          false until the syncing is completed.
    resp->set_table_id(table->id());
    table->AddTablets(tablets);
    BOOST_FOREACH(TabletInfo *tablet, tablets) {
      resp->add_tablet_ids(tablet->tablet_id());
      InsertOrDie(&tablet_map_, tablet->tablet_id(), tablet);
    }
  }

  // Lock the table and tablets for write and COW its metadata.
  table->metadata().StartMutation();
  DCHECK_EQ(SysTablesEntryPB::kTableStatePreparing,
            table->metadata().dirty().pb.state());
  table->metadata().mutable_dirty()->pb.set_state(SysTablesEntryPB::kTableStateRunning);

  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    // Though we aren't actually going to mutate the Tablet, when we write it back
    // to disk, it requires that it be locked for write. Otherwise, someone else
    // could come along and mutate it underneath the SysTable write. Even though
    // in this particular case no one else would touch the tablet, there are
    // assertions that would fire if we didn't lock for write.
    tablet->metadata().StartMutation();
    DCHECK_EQ(SysTabletsEntryPB::kTabletStatePreparing,
              tablet->metadata().dirty().pb.state());
  }


  // 5. Write Tablets to sys-tablets
  Status s = sys_tablets_->AddTablets(tablets);
  if (!s.ok()) {
    // panic-mode: abort the master
    LOG(ERROR) << "requestor: " << rpc->requestor_string();
    LOG(ERROR) << "request: " << req->DebugString();
    LOG(FATAL) << "An error occurred while inserting to sys-tablets: " << s.ToString();
  }

  // 6. Update the on-disk table state (PONR)
  s = sys_tables_->AddTable(table.get());
  if (!s.ok()) {
    // panic-mode: abort the master
    LOG(ERROR) << "requestor: " << rpc->requestor_string();
    LOG(ERROR) << "request: " << req->DebugString();
    LOG(FATAL) << "An error occurred while inserting to sys-tables: " << s.ToString();
  }

  // 7. Update the in-memory state
  table->metadata().CommitMutation();

  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    tablet->metadata().CommitMutation();
  }
  return Status::OK();
}

void CatalogManager::CreateTablets(const CreateTableRequestPB* req,
                                   TableInfo *table,
                                   vector<TabletInfo *> *tablets) {
  const char *kTabletEmptyKey = "";
  if (req->pre_split_keys_size() > 0) {
    int i = 0;

    // Sort the pre-split keys from the request
    vector<string> split_keys(req->pre_split_keys_size());
    std::copy(req->pre_split_keys().begin(), req->pre_split_keys().end(), split_keys.begin());
    std::sort(split_keys.begin(), split_keys.end());

    // First region with empty start key
    tablets->push_back(CreateTabletInfo(table, kTabletEmptyKey, split_keys[0]));
    // Mid regions with non-empty start/end key
    while (++i < split_keys.size()) {
      tablets->push_back(CreateTabletInfo(table, split_keys[i - 1], split_keys[i]));
    }
    // Last region with empty end key
    tablets->push_back(CreateTabletInfo(table, split_keys[i - 1], kTabletEmptyKey));
  } else {
    // Single region with empty start/end key
    tablets->push_back(CreateTabletInfo(table, kTabletEmptyKey, kTabletEmptyKey));
  }
}

TableInfo *CatalogManager::CreateTableInfo(const string& name, const Schema& schema) {
  TableInfo* table = new TableInfo(GenerateId());
  TableMetadataLock l(table, TableMetadataLock::WRITE);
  SysTablesEntryPB *metadata = &l.mutable_data()->pb;
  metadata->set_state(SysTablesEntryPB::kTableStatePreparing);
  metadata->set_name(name);
  metadata->set_version(0);
  CHECK_OK(SchemaToPB(schema, metadata->mutable_schema()));
  l.Commit();
  return table;
}

TabletInfo *CatalogManager::CreateTabletInfo(TableInfo *table,
                                             const string& start_key,
                                             const string& end_key) {
  TabletInfo* tablet = new TabletInfo(table, GenerateId());
  TabletMetadataLock l(tablet, TabletMetadataLock::WRITE);
  SysTabletsEntryPB *metadata = &l.mutable_data()->pb;
  metadata->set_state(SysTabletsEntryPB::kTabletStatePreparing);
  metadata->set_start_key(start_key);
  metadata->set_end_key(end_key);
  l.Commit();
  return tablet;
}

Status CatalogManager::FindTable(const TableIdentifierPB& table_identifier,
                                 scoped_refptr<TableInfo> *table_info) {
  boost::shared_lock<LockType> l(lock_);

  if (table_identifier.has_table_id()) {
    *table_info = FindPtrOrNull(table_ids_map_, table_identifier.table_id());
  } else if (table_identifier.has_table_name()) {
    *table_info = FindPtrOrNull(table_names_map_, table_identifier.table_name());
  } else {
    return Status::InvalidArgument("Missing Table ID or Table Name");
  }
  return Status::OK();
}

// Delete a Table
//  - Update the table state to "removed"
//  - Write the updated table metadata to sys-table
//
// we are lazy about deletions...
// the cleaner will remove tables and tablets marked as "removed"
Status CatalogManager::DeleteTable(const DeleteTableRequestPB* req,
                                   DeleteTableResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  scoped_refptr<TableInfo> table;

  // 1. Lookup the table and verify if it exists
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == NULL) {
    Status s = Status::NotFound("The table does not exist", req->table().DebugString());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TableMetadataLock l(table.get(), TableMetadataLock::WRITE);
  if (l.data().is_deleted()) {
    Status s = Status::NotFound("The table was deleted", l.data().pb.state_msg());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  // 2. Update the metadata for the on-disk state
  l.mutable_data()->set_state(SysTablesEntryPB::kTableStateRemoved,
                              Substitute("Deleted at ts=$0", GetCurrentTimeMicros()));

  // 3. Update sys-tablets with the removed the table state (PONR)
  Status s = sys_tables_->UpdateTable(table.get());
  if (!s.ok()) {
    LOG(ERROR) << "requestor: " << rpc->requestor_string();
    LOG(ERROR) << "request: " << req->DebugString();
    LOG(FATAL) << "An error occurred while updating sys-tables: " << s.ToString();
  }

  // 4. Update the in-memory state
  l.Commit();
  return Status::OK();
}

Status CatalogManager::ListTables(const ListTablesRequestPB* req,
                                  ListTablesResponsePB* resp) {
  boost::shared_lock<LockType> l(lock_);

  BOOST_FOREACH(const TableInfoMap::value_type& entry, table_names_map_) {
    TableMetadataLock ltm(entry.second.get(), TableMetadataLock::READ);
    if (!ltm.data().is_running()) continue;

    // TODO: Add a name filter?
    //if (!re_match(req->name_filter(), entry.second->name())) continue;

    ListTablesResponsePB::TableInfo *table = resp->add_tables();
    table->set_id(entry.second->id());
    table->set_name(ltm.data().name());
  }

  return Status::OK();
}

void CatalogManager::GetTabletLocations(const std::string& tablet_id,
                                        std::vector<TSDescriptor*>* locations) {
  scoped_refptr<TabletInfo> info;
  {
    boost::shared_lock<LockType> l(lock_);
    info = FindPtrOrNull(tablet_map_, tablet_id);
    if (info == NULL) {
      locations->clear();
      return;
    }
  }

  info->GetLocations(locations);
}

Status CatalogManager::ProcessTabletReport(TSDescriptor* ts_desc,
                                           const TabletReportPB& report,
                                           RpcContext* rpc) {
  if (VLOG_IS_ON(1)) {
    VLOG(1) << "Received tablet report from " <<
      rpc->requestor_string() << ": " << report.DebugString();
  }
  if (!ts_desc->has_tablet_report() && report.is_incremental()) {
    string msg = "Received an incremental tablet report when a full one was needed";
    LOG(WARNING) << "Invalid tablet report from " << rpc->requestor_string() << ": "
                 << msg;
    return Status::IllegalState(msg);
  }

  // If it's non-incremental, we need to clear all tablets which we previously
  // thought were on this server.
  // TODO: should we also have a map of server->tablet, not just tablet->server,
  // so this is O(tablets on server) instead of O(total replicas)? Optimization
  // for later, unless we find some functional reason to add it, I guess.
  if (!report.is_incremental()) {
    ClearAllReplicasOnTS(ts_desc);
  }

  BOOST_FOREACH(const ReportedTabletPB& reported, report.updated_tablets()) {
    RETURN_NOT_OK_PREPEND(HandleReportedTablet(ts_desc, reported),
                          Substitute("Error handling $0", reported.ShortDebugString()));
  }

  CHECK_EQ(report.removed_tablet_ids().size(), 0) << "TODO: implement tablet removal";

  ts_desc->set_has_tablet_report(true);
  return Status::OK();
}

void CatalogManager::ClearAllReplicasOnTS(TSDescriptor* ts_desc) {
  boost::shared_lock<LockType> l(lock_);
  BOOST_FOREACH(TabletInfoMap::value_type& e, tablet_map_) {
    e.second->ClearReplicasOnTS(ts_desc);
  }
}

Status CatalogManager::HandleReportedTablet(TSDescriptor* ts_desc,
                                            const ReportedTabletPB& report) {
  scoped_refptr<TabletInfo> tablet;
  {
    boost::shared_lock<LockType> l(lock_);
    tablet = FindPtrOrNull(tablet_map_, report.tablet_id());
  }
  if (tablet == NULL) {
    // TODO: The tablet server should trash the tablet
    return Status::NotFound("Tablet " + report.tablet_id() + " does not exist");
  }

  // TODO: we don't actually need to do the COW here until we see we're going
  // to change the state. Can we change CowedObject to lazily do the copy?
  TableMetadataLock table_lock(tablet->table(), TableMetadataLock::READ);
  TabletMetadataLock tablet_lock(tablet.get(), TabletMetadataLock::WRITE);
  if (tablet_lock.data().is_deleted()) {
    return Status::NotFound("The tablet was removed",
                            tablet_lock.data().pb.state_msg());
  }

  if (!table_lock.data().is_running()) {
    return Status::NotFound(
      Substitute("The tablet $0 of table $1 does not exist",
                 report.tablet_id(),
                 table_lock.data().name()),
      table_lock.data().pb.state_msg());
  }

  // TODO: Check table state
  tablet->AddReplica(ts_desc);

  // TODO if is the leader
  if (!tablet_lock.data().is_running()) {
    // Mark the tablet as running
    tablet_lock.mutable_data()->set_state(SysTabletsEntryPB::kTabletStateRunning,
                                          "Tablet reported");
    // TODO: the CommitMutation() will be done by the "assignment-loop"
    tablet_lock.Commit();
  }

  return Status::OK();
}

////////////////////////////////////////////////////////////
// TabletInfo
////////////////////////////////////////////////////////////

TabletInfo::TabletInfo(TableInfo *table, const std::string& tablet_id)
  : tablet_id_(tablet_id), table_(table),
    last_update_ts_(MonoTime::Now(MonoTime::FINE)) {
}

TabletInfo::~TabletInfo() {
}

void TabletInfo::AddReplica(TSDescriptor* ts_desc) {
  boost::lock_guard<simple_spinlock> l(lock_);

  last_update_ts_ = MonoTime::Now(MonoTime::FINE);
  BOOST_FOREACH(const TSDescriptor* l, locations_) {
    if (l == ts_desc) return;
  }
  VLOG(2) << tablet_id_ << " reported on " << ts_desc->permanent_uuid();
  locations_.push_back(ts_desc);
}

void TabletInfo::ClearReplicasOnTS(const TSDescriptor* ts) {
  boost::lock_guard<simple_spinlock> l(lock_);

  std::vector<TSDescriptor*>::iterator it = locations_.begin();
  while (it != locations_.end()) {
    if (*it == ts) {
      it = locations_.erase(it);
    } else {
      ++it;
    }
  }
}

void TabletInfo::GetLocations(std::vector<TSDescriptor*>* locations) const {
  boost::lock_guard<simple_spinlock> l(lock_);
  *locations = locations_;
}

void PersistentTabletInfo::set_state(SysTabletsEntryPB::State state, const string& msg) {
  pb.set_state(state);
  pb.set_state_msg(msg);
}

////////////////////////////////////////////////////////////
// TableInfo
////////////////////////////////////////////////////////////

TableInfo::TableInfo(const std::string& table_id)
  : table_id_(table_id) {
}

TableInfo::~TableInfo() {
}

void TableInfo::AddTablet(TabletInfo *tablet) {
  boost::lock_guard<simple_spinlock> l(lock_);
  tablet_map_[tablet->metadata().state().pb.start_key()] = tablet;
}

void TableInfo::AddTablets(const vector<TabletInfo*>& tablets) {
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    tablet_map_[tablet->metadata().state().pb.start_key()] = tablet;
  }
}

void PersistentTableInfo::set_state(SysTablesEntryPB::State state, const string& msg) {
  pb.set_state(state);
  pb.set_state_msg(msg);
}

} // namespace master
} // namespace kudu
