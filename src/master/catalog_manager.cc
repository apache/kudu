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
#include <boost/assign/list_of.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <glog/logging.h>

#include <algorithm>
#include <string>
#include <vector>

#include "common/wire_protocol.h"
#include "gutil/macros.h"
#include "gutil/map-util.h"
#include "gutil/mathlimits.h"
#include "gutil/stl_util.h"
#include "gutil/strings/escaping.h"
#include "gutil/strings/substitute.h"
#include "gutil/walltime.h"
#include "master/master.h"
#include "master/master.pb.h"
#include "master/sys_tables.h"
#include "master/ts_descriptor.h"
#include "master/ts_manager.h"
#include "tserver/tserver_service.proxy.h"
#include "rpc/rpc_context.h"
#include "util/thread_util.h"
#include "util/trace.h"

DEFINE_int32(assignment_timeout_ms, 10 * 1000, // 10 sec
             "Timeout used for the Master->TS assignment timeout. "
             "(Advanced option)");
DEFINE_int32(alter_timeout_ms, 10 * 1000, // 10 sec
             "Timeout used for the Master->TS alter request timeout. "
             "(Advanced option)");
DEFINE_int32(default_num_replicas, 1, // TODO switch to 3 and fix SelectReplicas()
             "Default number of replicas for tables that do have the num_replicas set. "
             "(Advanced option)");

namespace kudu {
namespace master {

using strings::Substitute;
using rpc::RpcContext;
using tserver::TabletServerErrorPB;
using metadata::QuorumPeerPB;

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

    LOG(INFO) << "Loaded table " << table->ToString();
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
    TabletInfo* tablet = new TabletInfo(table, tablet_id);
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
    if (!l.mutable_data()->is_deleted()) {
      table->AddTablet(tablet);
    }
    l.Commit();

    TableMetadataLock table_lock(table.get(), TableMetadataLock::READ);

    LOG(INFO) << "Loaded tablet " << tablet_id << " for table " << table->ToString();
    if (VLOG_IS_ON(2)) {
      VLOG(2) << "Metadata: " << metadata.DebugString();
    }

    return Status::OK();
  }

 private:
  CatalogManager *table_manager_;

  DISALLOW_COPY_AND_ASSIGN(TabletLoader);
};

////////////////////////////////////////////////////////////
// Background Tasks
////////////////////////////////////////////////////////////

class CatalogManagerBgTasks {
 public:
  explicit CatalogManagerBgTasks(CatalogManager *catalog_manager)
    : closing_(false), pending_updates_(false),
      thread_(NULL), catalog_manager_(catalog_manager) {
  }

  ~CatalogManagerBgTasks() {}

  Status Init();
  void Shutdown();

  void Wake() {
    boost::lock_guard<boost::mutex> lock(lock_);
    cond_.notify_all();
  }

  void Wait(int msec) {
    boost::unique_lock<boost::mutex> lock(lock_);
    if (closing_) return;
    boost::system_time wtime = boost::get_system_time() + boost::posix_time::milliseconds(msec);
    cond_.timed_wait(lock, wtime);
    pending_updates_ = false;
  }

  void WakeIfHasPendingUpdates() {
    boost::lock_guard<boost::mutex> lock(lock_);
    if (pending_updates_) {
      cond_.notify_all();
    }
  }

  void NotifyForUpdate() {
    boost::lock_guard<boost::mutex> lock(lock_);
    pending_updates_ = true;
  }

 private:
  void Run();

 private:
  Atomic32 closing_;
  bool pending_updates_;
  mutable boost::mutex lock_;
  boost::condition_variable cond_;
  gscoped_ptr<boost::thread> thread_;
  CatalogManager *catalog_manager_;
};

Status CatalogManagerBgTasks::Init() {
  RETURN_NOT_OK(StartThread(boost::bind(&CatalogManagerBgTasks::Run, this), &thread_));
  return Status::OK();
}

void CatalogManagerBgTasks::Shutdown() {
  if (Acquire_CompareAndSwap(&closing_, false, true) != false) {
    VLOG(2) << "CatalogManagerBgTasks already shut down";
    return;
  }

  Wake();
  if (thread_ != NULL) {
    CHECK_OK(ThreadJoiner(thread_.get(), "catalog manager thread").Join());
  }
}

void CatalogManagerBgTasks::Run() {
  SetThreadName("cat_mgr bgtasks");

  const int kMaxWaitMs = 60000;
  const int kMinWaitMs = 1000;
  while (!NoBarrier_Load(&closing_)) {
    std::vector<scoped_refptr<TabletInfo> > to_delete;
    std::vector<scoped_refptr<TabletInfo> > to_process;
    catalog_manager_->ExtractTabletsToProcess(&to_delete, &to_process);

    // Process the pending assignments
    int next_timeout_ms = kMaxWaitMs;
    if (!to_process.empty()) {
      catalog_manager_->ProcessPendingAssignments(to_process, &next_timeout_ms);
    }

    //if (!to_delete.empty()) {
      // TODO: Run the cleaner
    //}

    // Wait for a notification or a timeout expiration.
    //  - CreateTable will call Wake() to notify about the tablets to add
    //  - HandleReportedTablet/ProcessPendingAssignments will call WakeIfHasPendingUpdates()
    //    to notify about tablets creation.
    Wait(std::max(next_timeout_ms, kMinWaitMs));
  }
  VLOG(1) << "Catalog manager background task thread shutting down";
}

////////////////////////////////////////////////////////////
// CatalogManager
////////////////////////////////////////////////////////////

namespace {

string RequestorString(RpcContext* rpc) {
  if (rpc) {
    return rpc->requestor_string();
  } else {
    return "internal request";
  }
}

} // anonymous namespace

CatalogManager::CatalogManager(Master *master)
  : master_(master), closing_(false) {
}

CatalogManager::~CatalogManager() {
  Shutdown();
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

  background_tasks_.reset(new CatalogManagerBgTasks(this));
  RETURN_NOT_OK(background_tasks_->Init());

  return Status::OK();
}

void CatalogManager::Shutdown() {
  if (Acquire_CompareAndSwap(&closing_, false, true) != false) {
    VLOG(2) << "CatalogManager already shut down";
    return;
  }

  // Shutdown the Catalog Manager backgroud thread
  background_tasks_->Shutdown();
}

static void SetupError(MasterErrorPB* error,
                       MasterErrorPB::Code code,
                       const Status& s) {
  StatusToPB(s, error->mutable_status());
  error->set_code(code);
}

// Create a new table.
// See 'README' in this directory for description of the design.
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

  LOG(INFO) << "CreateTable from " << RequestorString(rpc)
            << ":\n" << req->DebugString();

  scoped_refptr<TableInfo> table;
  vector<TabletInfo*> tablets;
  {
    boost::lock_guard<LockType> l(lock_);
    TRACE("Acquired catalog manager lock");

    // 1. Verify that the table does not exist
    table = FindPtrOrNull(table_names_map_, req->name());
    if (table != NULL) {
      Status s = Status::AlreadyPresent("Table already exists", table->id());
      SetupError(resp->mutable_error(), MasterErrorPB::TABLE_ALREADY_PRESENT, s);
      return s;
    }

    // 2. Add the new table in "preparing" state
    table = CreateTableInfo(req, schema);
    table_ids_map_[table->id()] = table;
    table_names_map_[req->name()] = table;

    // 3. Create the Tablet Infos (state is kTabletStatePreparing)
    CreateTablets(req, table.get(), &tablets);

    // 4. Add the table/tablets to the in-memory map for the assignment.
    resp->set_table_id(table->id());
    table->AddTablets(tablets);
    BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
      InsertOrDie(&tablet_map_, tablet->tablet_id(), tablet);
    }
  }
  TRACE("Inserted table and tablets into CM maps");

  // NOTE: the table and tablets are already locked for write at this point,
  // since the CreateTableInfo/CreateTabletInfo functions leave them in that state.
  // They will get committed at the end of this function.
  // Sanity check: the tables and tablets should all be in "preparing" state.
  CHECK_EQ(SysTablesEntryPB::kTableStatePreparing, table->metadata().dirty().pb.state());
  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    CHECK_EQ(SysTabletsEntryPB::kTabletStatePreparing,
             tablet->metadata().dirty().pb.state());
  }

  // 5. Write Tablets to sys-tablets (in "preparing" state)
  Status s = sys_tablets_->AddTablets(tablets);
  if (!s.ok()) {
    // TODO: we could potentially handle this error case by returning an error,
    // since even if we mistakenly believe this failed, when it actually made it
    // to disk, we'll end up removing the orphaned tablets later.
    PANIC_RPC(rpc, Substitute("An error occurred while inserting to sys-tablets: $0",
                              s.ToString()));
  }
  TRACE("Wrote tablets to system table");

  // 6. Update the on-disk table state to "running" (PONR)
  table->metadata().mutable_dirty()->pb.set_state(SysTablesEntryPB::kTableStateRunning);
  s = sys_tables_->AddTable(table.get());
  if (!s.ok()) {
    PANIC_RPC(rpc, Substitute("An error occurred while inserting to sys-tablets: $0",
                              s.ToString()));
  }
  TRACE("Wrote table to system table");

  // 7. Update the in-memory state
  table->metadata().CommitMutation();

  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    tablet->metadata().CommitMutation();
  }

  VLOG(1) << "Created table " << table->ToString();
  background_tasks_->Wake();
  return Status::OK();
}

void CatalogManager::CreateTablets(const CreateTableRequestPB* req,
                                   TableInfo *table,
                                   vector<TabletInfo* > *tablets) {
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

TableInfo *CatalogManager::CreateTableInfo(const CreateTableRequestPB* req,
                                           const Schema& schema) {
  DCHECK(schema.has_column_ids());
  TableInfo* table = new TableInfo(GenerateId());
  table->metadata().StartMutation();
  SysTablesEntryPB *metadata = &table->metadata().mutable_dirty()->pb;
  metadata->set_state(SysTablesEntryPB::kTableStatePreparing);
  metadata->set_name(req->name());
  metadata->set_version(0);
  if (req->has_num_replicas()) {
    metadata->set_num_replicas(req->num_replicas());
  }
  // Use the Schema object passed in, since it has the column IDs already assigned,
  // whereas the user request PB does not.
  CHECK_OK(SchemaToPB(schema, metadata->mutable_schema()));
  return table;
}

TabletInfo *CatalogManager::CreateTabletInfo(TableInfo *table,
                                             const string& start_key,
                                             const string& end_key) {
  TabletInfo* tablet = new TabletInfo(table, GenerateId());
  tablet->metadata().StartMutation();
  SysTabletsEntryPB *metadata = &tablet->metadata().mutable_dirty()->pb;
  metadata->set_state(SysTabletsEntryPB::kTabletStatePreparing);
  metadata->set_start_key(start_key);
  metadata->set_end_key(end_key);
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
  LOG(INFO) << "Servicing DeleteTable request from " << RequestorString(rpc)
            << ": " << req->ShortDebugString();

  scoped_refptr<TableInfo> table;

  // 1. Lookup the table and verify if it exists
  TRACE("Looking up table");
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == NULL) {
    Status s = Status::NotFound("The table does not exist", req->table().DebugString());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TRACE("Locking table");
  TableMetadataLock l(table.get(), TableMetadataLock::WRITE);
  if (l.data().is_deleted()) {
    Status s = Status::NotFound("The table was deleted", l.data().pb.state_msg());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TRACE("Updating metadata on disk");
  // 2. Update the metadata for the on-disk state
  l.mutable_data()->set_state(SysTablesEntryPB::kTableStateRemoved,
                              Substitute("Deleted at ts=$0", GetCurrentTimeMicros()));

  // 3. Update sys-tablets with the removed the table state (PONR)
  Status s = sys_tables_->UpdateTable(table.get());
  if (!s.ok()) {
    PANIC_RPC(rpc, Substitute("An error occurred while updating sys tables: $0",
                              s.ToString()));
  }

  // 4. Remove it from the by-name map
  {
    TRACE("Removing from by-name map");
    boost::lock_guard<LockType> l_map(lock_);
    if (table_names_map_.erase(l.data().name()) != 1) {
      PANIC_RPC(rpc, "Could not remove table from map, name=" + l.data().name());
    }
  }

  table->AbortTasks();

  // 5. Update the in-memory state
  TRACE("Committing in-memory state");
  l.Commit();

  // Send the 'delete tablet' request to all the TSs
  SendDeleteTableRequest(table);

  VLOG(1) << "Deleted table " << table->ToString();
  background_tasks_->Wake();
  return Status::OK();
}

static Status ApplyAlterSteps(const SchemaPB& current_schema_pb,
                              const AlterTableRequestPB* req,
                              Schema* new_schema) {
  Schema cur_schema;
  RETURN_NOT_OK(SchemaFromPB(current_schema_pb, &cur_schema));

  SchemaBuilder builder(cur_schema);
  BOOST_FOREACH(const AlterTableRequestPB::Step& step, req->alter_schema_steps()) {
    switch (step.type()) {
      case AlterTableRequestPB::ADD_COLUMN: {
        if (!step.has_add_column()) {
          return Status::InvalidArgument("ADD_COLUMN missing column info");
        }

        ColumnSchema new_col = ColumnSchemaFromPB(step.add_column().schema());

        // can't accept a NOT NULL column without read default
        if (!new_col.is_nullable() && !new_col.has_read_default()) {
          return Status::InvalidArgument(
              Substitute("$0 is NOT NULL but does not have a default", new_col.name()));
        }

        RETURN_NOT_OK(builder.AddColumn(new_col, false));
        break;
      }

      case AlterTableRequestPB::DROP_COLUMN: {
        if (!step.has_drop_column()) {
          return Status::InvalidArgument("DROP_COLUMN missing column info");
        }

        if (cur_schema.is_key_column(step.drop_column().name())) {
          return Status::InvalidArgument("cannot remove a key column");
        }

        RETURN_NOT_OK(builder.RemoveColumn(step.drop_column().name()));
        break;
      }

      case AlterTableRequestPB::RENAME_COLUMN: {
        if (!step.has_rename_column()) {
          return Status::InvalidArgument("RENAME_COLUMN missing column info");
        }

        // TODO: In theory we can rename a key
        if (cur_schema.is_key_column(step.rename_column().old_name())) {
          return Status::InvalidArgument("cannot rename a key column");
        }

        RETURN_NOT_OK(builder.RenameColumn(
                        step.rename_column().old_name(),
                        step.rename_column().new_name()));
        break;
      }

      // TODO: EDIT_COLUMN

      default: {
        return Status::InvalidArgument(
          Substitute("Invalid alter step type: $0", step.type()));
      }
    }
  }
  *new_schema = builder.Build();
  return Status::OK();
}

Status CatalogManager::AlterTable(const AlterTableRequestPB* req,
                                  AlterTableResponsePB* resp,
                                  rpc::RpcContext* rpc) {
  LOG(INFO) << "Servicing AlterTable request from " << RequestorString(rpc)
            << ": " << req->ShortDebugString();

  scoped_refptr<TableInfo> table;

  // 1. Lookup the table and verify if it exists
  TRACE("Looking up table");
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == NULL) {
    Status s = Status::NotFound("The table does not exist", req->table().DebugString());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TRACE("Locking table");
  TableMetadataLock l(table.get(), TableMetadataLock::WRITE);
  if (l.data().is_deleted()) {
    Status s = Status::NotFound("The table was deleted", l.data().pb.state_msg());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  bool has_changes = false;
  string table_name = l.data().name();

  // 2. Calculate new schema for the on-disk state, not persisted yet
  Schema new_schema;
  if (req->alter_schema_steps_size()) {
    TRACE("Apply alter schema");
    Status s = ApplyAlterSteps(l.data().pb.schema(), req, &new_schema);
    if (!s.ok()) {
      SetupError(resp->mutable_error(), MasterErrorPB::INVALID_SCHEMA, s);
      return s;
    }

    has_changes = true;
  }

  // 3. Try to acquire the new table name
  if (req->has_new_table_name()) {
    boost::lock_guard<LockType> catalog_lock(lock_);

    TRACE("Acquired catalog manager lock");

    // Verify that the table does not exist
    scoped_refptr<TableInfo> other_table = FindPtrOrNull(table_names_map_, req->new_table_name());
    if (other_table != NULL) {
      Status s = Status::AlreadyPresent("Table already exists", other_table->id());
      SetupError(resp->mutable_error(), MasterErrorPB::TABLE_ALREADY_PRESENT, s);
      return s;
    }

    // Acquire the new table name (now we have 2 name for the same table)
    table_names_map_[req->new_table_name()] = table;
    l.mutable_data()->pb.set_name(req->new_table_name());

    has_changes = true;
  }

  // Skip empty requests...
  if (!has_changes) {
    return Status::OK();
  }

  // 4. Serialize the schema Increment the version number
  if (new_schema.initialized()) {
    if (!l.data().pb.has_fully_applied_schema()) {
      l.mutable_data()->pb.mutable_fully_applied_schema()->CopyFrom(l.data().pb.schema());
    }
    CHECK_OK(SchemaToPB(new_schema, l.mutable_data()->pb.mutable_schema()));
  }
  l.mutable_data()->pb.set_version(l.mutable_data()->pb.version() + 1);
  l.mutable_data()->set_state(SysTablesEntryPB::kTableStateAltering,
                              Substitute("Alter Table version=$0 ts=$1",
                              l.mutable_data()->pb.version(),
                              GetCurrentTimeMicros()));

  // 5. Update sys-tables with the new table schema (point of no return!)
  TRACE("Updating metadata on disk");
  Status s = sys_tables_->UpdateTable(table.get());
  if (!s.ok()) {
    PANIC_RPC(rpc, Substitute("An error occurred while updating sys tables: $0",
                              s.ToString()));
  }

  // 6. Remove the old name
  if (req->has_new_table_name()) {
    TRACE("Removing old-name $0 from by-name map", table_name);
    boost::lock_guard<LockType> l_map(lock_);
    if (table_names_map_.erase(table_name) != 1) {
      PANIC_RPC(rpc, "Could not remove table from map, name=" + l.data().name());
    }
  }

  // 7. Update the in-memory state
  TRACE("Committing in-memory state");
  l.Commit();

  SendAlterTableRequest(table);
  return Status::OK();
}

Status CatalogManager::IsAlterTableDone(const IsAlterTableDoneRequestPB* req,
                                        IsAlterTableDoneResponsePB* resp,
                                        rpc::RpcContext* rpc) {
  scoped_refptr<TableInfo> table;

  // 1. Lookup the table and verify if it exists
  TRACE("Looking up table");
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == NULL) {
    Status s = Status::NotFound("The table does not exist", req->table().DebugString());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TRACE("Locking table");
  TableMetadataLock l(table.get(), TableMetadataLock::READ);
  if (l.data().is_deleted()) {
    Status s = Status::NotFound("The table was deleted", l.data().pb.state_msg());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  // 2. Verify if the alter is in-progress
  TRACE("Verify if there is an alter operation in progress for $0", table->ToString());
  resp->set_schema_version(l.data().pb.version());
  resp->set_done(l.data().pb.state() != SysTablesEntryPB::kTableStateAltering);

  return Status::OK();
}

Status CatalogManager::GetTableSchema(const GetTableSchemaRequestPB* req,
                                      GetTableSchemaResponsePB* resp) {
  scoped_refptr<TableInfo> table;

  // 1. Lookup the table and verify if it exists
  TRACE("Looking up table");
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == NULL) {
    Status s = Status::NotFound("The table does not exist", req->table().DebugString());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TRACE("Locking table");
  TableMetadataLock l(table.get(), TableMetadataLock::READ);
  if (l.data().is_deleted()) {
    Status s = Status::NotFound("The table was deleted", l.data().pb.state_msg());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  if (l.data().pb.has_fully_applied_schema()) {
    CHECK(l.data().pb.state() == SysTablesEntryPB::kTableStateAltering);
    resp->mutable_schema()->CopyFrom(l.data().pb.fully_applied_schema());
  } else {
    resp->mutable_schema()->CopyFrom(l.data().pb.schema());
  }

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

bool CatalogManager::GetTableInfo(const string& table_id, scoped_refptr<TableInfo> *table) {
  boost::shared_lock<LockType> l(lock_);
  *table = FindPtrOrNull(table_ids_map_, table_id);
  return *table != NULL;
}

void CatalogManager::GetAllTables(std::vector<scoped_refptr<TableInfo> > *tables) {
  tables->clear();
  boost::shared_lock<LockType> l(lock_);
  BOOST_FOREACH(const TableInfoMap::value_type& e, table_ids_map_) {
    tables->push_back(e.second);
  }
}

bool CatalogManager::TableNameExists(const string& table_name) {
  boost::shared_lock<LockType> l(lock_);
  return table_names_map_.find(table_name) != table_names_map_.end();
}

Status CatalogManager::ProcessTabletReport(TSDescriptor* ts_desc,
                                           const TabletReportPB& report,
                                           TabletReportUpdatesPB *report_update,
                                           RpcContext* rpc) {
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Received tablet report from " <<
      RequestorString(rpc) << ": " << report.DebugString();
  }
  if (!ts_desc->has_tablet_report() && report.is_incremental()) {
    string msg = "Received an incremental tablet report when a full one was needed";
    LOG(WARNING) << "Invalid tablet report from " << RequestorString(rpc) << ": "
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
    ReportedTabletUpdatesPB *tablet_report = report_update->add_tablets();
    tablet_report->set_tablet_id(reported.tablet_id());
    RETURN_NOT_OK_PREPEND(HandleReportedTablet(ts_desc, reported, tablet_report),
                          Substitute("Error handling $0", reported.ShortDebugString()));
  }

  // TODO: Remove this, since we have the report-updates with the extra status-msg
  CHECK_EQ(report.removed_tablet_ids().size(), 0) << "TODO: implement tablet removal";

  ts_desc->set_has_tablet_report(true);

  if (report.updated_tablets_size() > 0) {
    background_tasks_->WakeIfHasPendingUpdates();
  }

  return Status::OK();
}

void CatalogManager::ClearAllReplicasOnTS(TSDescriptor* ts_desc) {
  boost::shared_lock<LockType> l(lock_);
  BOOST_FOREACH(TabletInfoMap::value_type& e, tablet_map_) {
    e.second->ClearReplicasOnTS(ts_desc);
  }
}

Status CatalogManager::HandleReportedTablet(TSDescriptor* ts_desc,
                                            const ReportedTabletPB& report,
                                            ReportedTabletUpdatesPB *report_updates) {
  scoped_refptr<TabletInfo> tablet;
  {
    boost::shared_lock<LockType> l(lock_);
    tablet = FindPtrOrNull(tablet_map_, report.tablet_id());
  }
  if (tablet == NULL) {
    VLOG(1) << "Got report from missing tablet " << report.tablet_id();
    SendDeleteTabletRequest(report.tablet_id(), NULL, ts_desc);
    return Status::OK();
  }

  // TODO: we don't actually need to do the COW here until we see we're going
  // to change the state. Can we change CowedObject to lazily do the copy?
  TableMetadataLock table_lock(tablet->table(), TableMetadataLock::READ);
  TabletMetadataLock tablet_lock(tablet.get(), TabletMetadataLock::WRITE);

  if (tablet_lock.data().is_deleted()) {
    report_updates->set_state_msg(tablet_lock.data().pb.state_msg());
    VLOG(1) << "Got report from deleted tablet " << tablet->ToString()
            << ": " << tablet_lock.data().pb.state_msg();
    SendDeleteTabletRequest(tablet->tablet_id(), tablet->table(), ts_desc);
    return Status::OK();
  }

  if (!table_lock.data().is_running()) {
    VLOG(1) << "Got report for a non-running table. " << tablet->ToString()
            << ": " << tablet_lock.data().pb.state_msg();
    report_updates->set_state_msg(tablet_lock.data().pb.state_msg());
    return Status::OK();
  }

  // Check if the tablet requires an "alter table" call
  if (report.has_schema_version() &&
      table_lock.data().pb.version() != report.schema_version()) {
    if (report.schema_version() > table_lock.data().pb.version()) {
      LOG(ERROR) << "TS " << ts_desc->permanent_uuid()
                 << " has reported a schema version greater than the current one "
                 << " for tablet " << tablet->ToString()
                 << ". Expected version " << table_lock.data().pb.version()
                 << " got " << report.schema_version()
                 << " (corruption)";
    } else {
      VLOG(1) << "TS " << ts_desc->permanent_uuid()
            << " does not have the latest schema for tablet " << tablet->ToString()
            << ". Expected version " << table_lock.data().pb.version()
            << " got " << report.schema_version();
    }
    SendAlterTabletRequest(tablet, ts_desc);
  }

  table_lock.Unlock();

  tablet->AddReplica(ts_desc, report.state(), report.role());

  if (report.has_error()) {
    Status s = StatusFromPB(report.error());
    DCHECK(!s.ok());
    DCHECK_EQ(report.state(), metadata::FAILED);
    LOG(WARNING) << "Tablet " << tablet->ToString() << " has failed on TS "
                 << ts_desc->permanent_uuid() << ": " << s.ToString();
    return Status::OK();
  }

  if (tablet_lock.data().IsQuorumLeader(ts_desc)) {
    if (report.has_schema_version()) {
      tablet->set_reported_schema_version(report.schema_version());
    }

    if (!tablet_lock.data().is_running() && report.state() == metadata::RUNNING) {
      DCHECK(tablet_lock.data().pb.state() == SysTabletsEntryPB::kTabletStateCreating);
      // Mark the tablet as running
      // TODO: we could batch the IO onto a background thread, or at least
      // across multiple tablets in the same report.
      tablet_lock.mutable_data()->set_state(SysTabletsEntryPB::kTabletStateRunning,
                                            "Tablet reported by leader");
      Status s = sys_tablets_->UpdateTablets(boost::assign::list_of(tablet.get()));
      if (!s.ok()) {
        // panic-mode: abort the master
        LOG(FATAL) << "An error occurred while updating sys-tablets: " << s.ToString();
      }

      tablet_lock.Commit();
    }
  }

  return Status::OK();
}

// Send the "Alter Table" with latest table schema
// keeps retrying until we get an "ok" response.
//  - Alter completed
//  - TS has already a newer version
//    (which may happen in case of 2 alter since we can't abort an in-progress operation)
class AsyncAlterTable : public MonitoredTask {
 public:
  AsyncAlterTable(CatalogManager *catalog_manager,
                  const std::tr1::shared_ptr<tserver::TabletServerServiceProxy>& ts_proxy,
                  const scoped_refptr<TabletInfo>& tablet,
                  const std::string& permanent_uuid)
    : state_(kStatePreparing),
      permanent_uuid_(permanent_uuid),
      catalog_manager_(catalog_manager),
      tablet_(tablet),
      ts_proxy_(ts_proxy) {
    }

  virtual Status Run() {
    state_ = kStateRunning;
    start_ts_ = MonoTime::Now(MonoTime::FINE);
    SendRequest();
    return Status::OK();
  }

  virtual bool Abort() {
    state_ = kStateAborted;
    return true;
  }

  virtual State state() const { return state_; }

  virtual string type_name() const { return "Alter Table"; }

  virtual string description() const {
    return tablet_->ToString() + " Alter Table RPC for TS=" + permanent_uuid_;
  }

  virtual MonoTime start_timestamp() const { return start_ts_; }

  virtual MonoTime completion_timestamp() const { return end_ts_; }

 private:
  void Callback() {
    if (!rpc_.status().ok()) {
      LOG(WARNING) << permanent_uuid_ << " Alter RPC failed for tablet " << tablet_->ToString()
                   << ": " << rpc_.status().ToString();
    } else if (resp_.has_error()) {
      Status status = StatusFromPB(resp_.error().status());

      // Do not retry on a fatal error
      switch (resp_.error().code()) {
        case TabletServerErrorPB::TABLET_NOT_FOUND:
        case TabletServerErrorPB::MISMATCHED_SCHEMA:
        case TabletServerErrorPB::TABLET_HAS_A_NEWER_SCHEMA:
          LOG(WARNING) << permanent_uuid_ << " alter failed for tablet " << tablet_->ToString()
                       << " no further retry: " << status.ToString();
          state_ = kStateComplete;
          break;
        default:
          LOG(WARNING) << permanent_uuid_ << " alter failed for tablet " << tablet_->ToString()
                       << ": " << status.ToString();
          break;
      }
    } else {
      state_ = kStateComplete;
      VLOG(1) << permanent_uuid_ << " alter complete on tablet " << tablet_->ToString();
    }

    if (state_ != kStateComplete && state_ != kStateAborted) {
      SendRequest();
    } else {
      end_ts_ = MonoTime::Now(MonoTime::FINE);

      if (state_ != kStateAborted &&
          tablet_->set_reported_schema_version(schema_version_) &&
          !tablet_->table()->IsAlterInProgress(schema_version_)) {
        MarkAlterTableAsComplete();
      }

      tablet_->table()->RemoveTask(this);
    }
  }

  void SendRequest() {
    TableMetadataLock l(tablet_->table(), TableMetadataLock::READ);
    if (l.data().is_deleted()) {
      return;
    }

    tserver::AlterSchemaRequestPB req;
    req.set_tablet_id(tablet_->tablet_id());
    req.set_new_table_name(l.data().pb.name());
    req.set_schema_version(l.data().pb.version());
    req.mutable_schema()->CopyFrom(l.data().pb.schema());
    schema_version_ = l.data().pb.version();

    l.Unlock();

    rpc_.Reset();
    rpc_.set_timeout(MonoDelta::FromMilliseconds(FLAGS_alter_timeout_ms));
    ts_proxy_->AlterSchemaAsync(req, &resp_, &rpc_,
                                boost::bind(&AsyncAlterTable::Callback, this));
    VLOG(1) << "Send alter table request to " << permanent_uuid_ << ":\n"
            << req.DebugString();
  }

  // Mark that every tablet has the latest schema.
  // TODO: we could batch the IO onto a background thread.
  //       but this is following the current HandleReportedTablet()
  void MarkAlterTableAsComplete() {
    TableInfo *table = tablet_->table();
    TableMetadataLock l(table, TableMetadataLock::WRITE);
    if (l.data().is_deleted() || l.data().pb.state() != SysTablesEntryPB::kTableStateAltering) {
      return;
    }

    uint32_t current_version = l.data().pb.version();
    if (schema_version_ != current_version && !table->IsAlterInProgress(current_version)) {
      return;
    }

    // Update the state from altering to running and remove the last schema
    DCHECK(l.mutable_data()->pb.has_fully_applied_schema());
    l.mutable_data()->pb.mutable_fully_applied_schema()->Clear();
    l.mutable_data()->set_state(SysTablesEntryPB::kTableStateRunning,
                                Substitute("Current schema version=$0", current_version));

    Status s = catalog_manager_->sys_tables()->UpdateTable(table);
    if (!s.ok()) {
      // panic-mode: abort the master
      LOG(FATAL) << "An error occurred while updating sys-tables: " << s.ToString();
    }

    l.Commit();
    LOG(INFO) << table->ToString() << " - Alter table completed version=" << current_version;
  }

  MonoTime start_ts_;
  MonoTime end_ts_;
  volatile State state_;
  rpc::RpcController rpc_;
  uint32_t schema_version_;
  std::string permanent_uuid_;
  CatalogManager *catalog_manager_;
  scoped_refptr<TabletInfo> tablet_;
  tserver::AlterSchemaResponsePB resp_;
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> ts_proxy_;
};

void CatalogManager::SendAlterTableRequest(const scoped_refptr<TableInfo>& table) {
  vector<scoped_refptr<TabletInfo> > tablets;
  table->GetAllTablets(&tablets);

  BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
    SendAlterTabletRequest(tablet);
  }
}

void CatalogManager::SendAlterTabletRequest(const scoped_refptr<TabletInfo>& tablet) {
  std::vector<TabletReplica> locations;
  tablet->GetLocations(&locations);
  BOOST_FOREACH(const TabletReplica& replica, locations) {
    SendAlterTabletRequest(tablet, replica.ts_desc);
  }
}

void CatalogManager::SendAlterTabletRequest(const scoped_refptr<TabletInfo>& tablet,
                                            TSDescriptor* ts_desc) {
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> ts_proxy;
  CHECK_OK(ts_desc->GetProxy(master_->messenger(), &ts_proxy));

  AsyncAlterTable *call = new AsyncAlterTable(this, ts_proxy, tablet, ts_desc->permanent_uuid());
  tablet->table()->AddTask(call);
  call->Run();
}

class AsyncCreateTablet : public MonitoredTask {
 public:
  explicit AsyncCreateTablet(Master *master,
                             const QuorumPeerPB& peer,
                             TabletInfo* tablet)
    : master_(master),
      peer_(peer),
      tablet_(tablet),
      state_(kStatePreparing) {
  }

  // Fire off the async create table.
  // This requires that the new tablet info is locked for write, and the
  // quorum information has been filled into the 'dirty' data.
  Status Run() {
    state_ = kStateRunning;
    start_ts_ = MonoTime::Now(MonoTime::FINE);

    std::tr1::shared_ptr<TSDescriptor> ts_desc;
    RETURN_NOT_OK(master_->ts_manager()->LookupTSByUUID(peer_.permanent_uuid(), &ts_desc));
    RETURN_NOT_OK(ts_desc->GetProxy(master_->messenger(), &ts_proxy_));

    tserver::CreateTabletRequestPB req;
    {
      TableMetadataLock table_lock(tablet_->table(), TableMetadataLock::READ);

      const SysTabletsEntryPB& tablet_pb = tablet_->metadata().dirty().pb;

      req.set_table_id(tablet_->table()->id());
      req.set_tablet_id(tablet_->tablet_id());
      req.set_start_key(tablet_pb.start_key());
      req.set_end_key(tablet_pb.end_key());
      req.set_table_name(table_lock.data().pb.name());
      req.mutable_schema()->CopyFrom(table_lock.data().pb.schema());
      req.mutable_quorum()->CopyFrom(tablet_pb.quorum());
    }

    rpc_.set_timeout(MonoDelta::FromMilliseconds(FLAGS_assignment_timeout_ms));
    ts_proxy_->CreateTabletAsync(req, &resp_, &rpc_,
                                 boost::bind(&AsyncCreateTablet::Callback, this));
    VLOG(1) << "Send create tablet request to " << peer_.permanent_uuid() << ":\n"
            << req.DebugString();
    return Status::OK();
  }

  virtual bool Abort() {
    state_ = kStateAborted;
    return true;
  }

  virtual State state() const { return state_; }

  virtual string type_name() const { return "Create Table"; }

  virtual string description() const {
    return tablet_->ToString() + " Create Tablet RPC for TS=" + peer_.permanent_uuid();
  }

  virtual MonoTime start_timestamp() const { return start_ts_; }

  virtual MonoTime completion_timestamp() const { return end_ts_; }

 private:
  void Callback() {
    Status s;
    if (!rpc_.status().ok()) {
      s = rpc_.status();
    } else if (resp_.has_error()) {
      s = StatusFromPB(resp_.error().status());
    }
    if (!s.ok()) {
      state_ = kStateComplete;
      LOG(WARNING) << "CreateTablet RPC for tablet " << tablet_->tablet_id()
                   << " on TS " << peer_.permanent_uuid() << " failed: " << s.ToString();
    }

    end_ts_ = MonoTime::Now(MonoTime::FINE);
    tablet_->table()->RemoveTask(this);
  }

 private:
  Master * const master_;
  const QuorumPeerPB peer_;
  const scoped_refptr<TabletInfo> tablet_;

  MonoTime start_ts_;
  MonoTime end_ts_;
  volatile State state_;
  rpc::RpcController rpc_;
  tserver::CreateTabletResponsePB resp_;
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> ts_proxy_;
};

// Send the "Delete Tablet"
// keeps retrying until we get an "ok" response.
//  - Delete completed
//  - TS has already deleted the tablet
class AsyncDeleteTablet : public MonitoredTask {
 public:
  AsyncDeleteTablet(const std::tr1::shared_ptr<tserver::TabletServerServiceProxy>& ts_proxy,
                    const scoped_refptr<TableInfo>& table,
                    const std::string& tablet_id,
                    const std::string& permanent_uuid)
    : state_(kStatePreparing),
      permanent_uuid_(permanent_uuid),
      tablet_id_(tablet_id),
      table_(table),
      ts_proxy_(ts_proxy) {
    }

  virtual Status Run() {
    state_ = kStateRunning;
    start_ts_ = MonoTime::Now(MonoTime::FINE);
    SendRequest();
    return Status::OK();
  }

  virtual bool Abort() {
    return false;
  }

  virtual State state() const { return state_; }

  virtual string type_name() const { return "Delete Tablet"; }

  virtual string description() const {
    return tablet_id_ + " Delete Tablet RPC for TS=" + permanent_uuid_;
  }

  virtual MonoTime start_timestamp() const { return start_ts_; }

  virtual MonoTime completion_timestamp() const { return end_ts_; }

 private:
  void Callback() {
    if (!rpc_.status().ok()) {
      LOG(WARNING) << permanent_uuid_ << " Delete RPC failed for tablet " << tablet_id_
                   << ": " << rpc_.status().ToString();
    } else if (resp_.has_error()) {
      Status status = StatusFromPB(resp_.error().status());

      // Do not retry on a fatal error
      switch (resp_.error().code()) {
        case TabletServerErrorPB::TABLET_NOT_FOUND:
          LOG(WARNING) << permanent_uuid_ << " delete failed for tablet " << tablet_id_
                       << " no further retry: " << status.ToString();
          state_ = kStateComplete;
          break;
        default:
          LOG(WARNING) << permanent_uuid_ << " delete failed for tablet " << tablet_id_
                       << ": " << status.ToString();
          break;
      }
    } else {
      state_ = kStateComplete;
      VLOG(1) << permanent_uuid_ << " delete complete on tablet " << tablet_id_;
    }

    if (state_ != kStateComplete) {
      SendRequest();
    } else if (table_ != NULL) {
      end_ts_ = MonoTime::Now(MonoTime::FINE);
      table_->RemoveTask(this);
    } else {
      // This is a floating task (since the tablet does not exist)
      // created as response to a tablet report.
      Release();
    }
  }

  void SendRequest() {
    tserver::DeleteTabletRequestPB req;
    req.set_tablet_id(tablet_id_);

    rpc_.Reset();
    rpc_.set_timeout(MonoDelta::FromMilliseconds(FLAGS_alter_timeout_ms));
    ts_proxy_->DeleteTabletAsync(req, &resp_, &rpc_,
                                 boost::bind(&AsyncDeleteTablet::Callback, this));
    VLOG(1) << "Send delete tablet request to " << permanent_uuid_ << ":\n"
            << req.DebugString();
  }

  MonoTime start_ts_;
  MonoTime end_ts_;
  volatile State state_;
  rpc::RpcController rpc_;
  std::string permanent_uuid_;
  const std::string tablet_id_;
  const scoped_refptr<TableInfo> table_;
  tserver::DeleteTabletResponsePB resp_;
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> ts_proxy_;
};

void CatalogManager::SendDeleteTableRequest(const scoped_refptr<TableInfo>& table) {
  vector<scoped_refptr<TabletInfo> > tablets;
  table->GetAllTablets(&tablets);

  BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
    std::vector<TabletReplica> locations;
    tablet->GetLocations(&locations);
    BOOST_FOREACH(const TabletReplica& replica, locations) {
      SendDeleteTabletRequest(tablet->tablet_id(), table, replica.ts_desc);
    }
  }
}

void CatalogManager::SendDeleteTabletRequest(const std::string& tablet_id,
                                             const scoped_refptr<TableInfo>& table,
                                             TSDescriptor* ts_desc) {
  std::tr1::shared_ptr<tserver::TabletServerServiceProxy> ts_proxy;
  CHECK_OK(ts_desc->GetProxy(master_->messenger(), &ts_proxy));

  AsyncDeleteTablet *call = new AsyncDeleteTablet(ts_proxy, table,
                                                  tablet_id, ts_desc->permanent_uuid());
  if (table != NULL) {
    table->AddTask(call);
  } else {
    // This is a floating task (since the tablet does not exist)
    // created as response to a tablet report.
    call->AddRef();
  }
  call->Run();
}

void CatalogManager::ExtractTabletsToProcess(
    std::vector<scoped_refptr<TabletInfo> > *tablets_to_delete,
    std::vector<scoped_refptr<TabletInfo> > *tablets_to_process) {

  boost::shared_lock<LockType> l(lock_);

  // TODO: At the moment we loop through all the tablets
  //       we can keep a set of tablets waiting for "assignment"
  //       or just a counter to avoid to take the lock and loop through the tablets
  //       if everything is "stable".

  BOOST_FOREACH(TabletInfoMap::value_type& entry, tablet_map_) {
    scoped_refptr<TabletInfo> tablet = entry.second;
    TabletMetadataLock tablet_lock(tablet.get(), TabletMetadataLock::READ);
    TableMetadataLock table_lock(tablet->table(), TableMetadataLock::READ);

    // if the table is deleted or the tablet was replaced
    if (tablet_lock.data().is_deleted() || table_lock.data().is_deleted()) {
      tablets_to_delete->push_back(tablet);
      continue;
    }

    // Nothing to do with the running tablets
    if (tablet_lock.data().is_running()) {
      // TODO: handle last update > not responding timeout?
      //tablets_not_reporting->push_back(tablet);
      continue;
    }

    // Tablets not yet assigned or with a report just received
    tablets_to_process->push_back(tablet);
  }
}

struct DeferredAssignmentActions {
  DeferredAssignmentActions()
    : next_timeout_ms(MathLimits<int>::kMax) {}
  vector<TabletInfo*> tablets_to_add;
  vector<TabletInfo*> tablets_to_update;
  vector<TabletInfo*> needs_create_rpc;
  int next_timeout_ms;
};

void CatalogManager::HandleAssignPreparingTablet(TabletInfo* tablet,
                                                 DeferredAssignmentActions* deferred) {
  // The tablet was just created (probably by a CreateTable)
  // update the state to "creating" to be reading for the creation request.
  tablet->metadata().mutable_dirty()->set_state(
    SysTabletsEntryPB::kTabletStateCreating, "Sending initial creation of tablet");
  deferred->tablets_to_update.push_back(tablet);
  deferred->needs_create_rpc.push_back(tablet);
  VLOG(1) << "Assign new tablet " << tablet->ToString();
}

void CatalogManager::HandleAssignCreatingTablet(TabletInfo* tablet,
                                                DeferredAssignmentActions* deferred) {
  MicrosecondsInt64 current_time = GetCurrentTimeMicros();
  MonoTime now = MonoTime::Now(MonoTime::FINE);

  int remaining_timeout = FLAGS_assignment_timeout_ms -
    tablet->TimeSinceLastUpdate(now).ToMilliseconds();

  // Skip the tablet if the assignment timeout is not yet expired
  if (remaining_timeout > 0) {
    tablet->metadata().AbortMutation();
    VLOG(2) << "Tablet " << tablet->ToString() << " still being created. "
            << remaining_timeout << "ms remain until timeout.";

    deferred->next_timeout_ms = std::min(deferred->next_timeout_ms, remaining_timeout);
    return;
  }

  const PersistentTabletInfo& old_info = tablet->metadata().state();

  // The "tablet creation" was already sent, but we didn't receive an answer
  // within the timeout. so the tablet will be replaced by a new one.
  TabletInfo *replacement = CreateTabletInfo(tablet->table(),
                                             old_info.pb.start_key(),
                                             old_info.pb.end_key());
  LOG(WARNING) << "Tablet " << tablet->ToString() << " was not created within "
               << "the allowed timeout. Replacing with a new tablet "
               << replacement->tablet_id();

  tablet->table()->AddTablet(replacement);
  {
    boost::lock_guard<LockType> l_maps(lock_);
    tablet_map_[replacement->tablet_id()] = replacement;
  }

  tablet->metadata().mutable_dirty()->set_state(
    SysTabletsEntryPB::kTabletStateReplaced,
    Substitute("Replaced by $0 at ts=$1",
               replacement->tablet_id(), current_time));

  replacement->metadata().mutable_dirty()->set_state(
    SysTabletsEntryPB::kTabletStateCreating,
    Substitute("Replacement for $0", tablet->tablet_id()));

  deferred->tablets_to_update.push_back(tablet);
  deferred->tablets_to_add.push_back(replacement);
  deferred->needs_create_rpc.push_back(replacement);
  VLOG(1) << "Replaced tablet " << tablet->tablet_id()
          << " with " << replacement->tablet_id()
          << " (Table " << tablet->table()->ToString() << ")";
}

void CatalogManager::ProcessPendingAssignments(
    const std::vector<scoped_refptr<TabletInfo> >& tablets,
    int *next_timeout_ms) {
  VLOG(1) << "Processing pending assignments";

  *next_timeout_ms = FLAGS_assignment_timeout_ms;

  DeferredAssignmentActions deferred;

  // Iterate over each of the tablets and handle it, whatever state
  // it may be in. The actions required for the tablet are collected
  // into 'deferred'.
  BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
    tablet->metadata().StartMutation();
    SysTabletsEntryPB::State t_state = tablet->metadata().state().pb.state();

    switch (t_state) {
      case SysTabletsEntryPB::kTabletStatePreparing:
        HandleAssignPreparingTablet(tablet.get(), &deferred);
        break;

      case SysTabletsEntryPB::kTabletStateCreating:
        HandleAssignCreatingTablet(tablet.get(), &deferred);
        break;

      default:
        VLOG(2) << "Nothing to do for tablet " << tablet->tablet_id() << ": state = "
                << SysTabletsEntryPB_State_Name(t_state);
        break;
    }
  }

  // Nothing to do
  if (deferred.tablets_to_add.empty() &&
      deferred.tablets_to_update.empty() &&
      deferred.needs_create_rpc.empty()) {
    return;
  }

  // For those tablets which need to be created in this round, assign replicas.
  TSDescriptorVector ts_descs;
  master_->ts_manager()->GetAllDescriptors(&ts_descs);
  SelectReplicasForTablets(deferred.needs_create_rpc, ts_descs);

  // Update the sys-tablets with the new set of tablets/metadata.
  Status s = sys_tablets_->AddAndUpdateTablets(
    deferred.tablets_to_add, deferred.tablets_to_update);
  if (!s.ok()) {
    // panic-mode: abort the master
    LOG(FATAL) << "An error occurred while updating sys-tablets: " << s.ToString();
  }

  // Send the "create tablet" requests to the servers. This is asynchronous / non-blocking.
  SendCreateTabletRequests(deferred.needs_create_rpc);

  // Commit the changes in memory. This comes after the above sending of the RPCs,
  // to ensure that no one else tries to mutate the TabletInfos while we're
  // doing this.
  BOOST_FOREACH(TabletInfo* t, deferred.tablets_to_add) {
    t->metadata().CommitMutation();
  }
  BOOST_FOREACH(TabletInfo* t, deferred.tablets_to_update) {
    t->metadata().CommitMutation();
  }
}

void CatalogManager::SelectReplicasForTablets(const vector<TabletInfo*>& tablets,
                                              const TSDescriptorVector& ts_descs) {
  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    TableInfo *table = tablet->table();

    TableMetadataLock l(table, TableMetadataLock::READ);

    int nreplicas = FLAGS_default_num_replicas;
    if (l.data().pb.has_num_replicas()) {
      nreplicas = l.data().pb.num_replicas();
    }

    if (ts_descs.size() < nreplicas) {
      LOG(WARNING) << "Not enough Tablet Servers are online for " << l.data().name()
                   << " expected at least " << nreplicas
                   << " but " << ts_descs.size() << " are available";
      continue;
    }

    // Select the set of replicas
    metadata::QuorumPB *quorum = tablet->metadata().mutable_dirty()->pb.mutable_quorum();
    quorum->set_seqno(0);
    SelectReplicas(quorum, ts_descs, nreplicas);
  }
}

void CatalogManager::SendCreateTabletRequests(const vector<TabletInfo*>& tablets) {
  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    const metadata::QuorumPB& quorum = tablet->metadata().dirty().pb.quorum();
    tablet->set_last_update_ts(MonoTime::Now(MonoTime::FINE));
    BOOST_FOREACH(const QuorumPeerPB& peer, quorum.peers()) {
      AsyncCreateTablet *task = new AsyncCreateTablet(master_, peer, tablet);
      tablet->table()->AddTask(task);
      WARN_NOT_OK(task->Run(), "Failed to send new tablet request");
    }
  }
}

void CatalogManager::SelectReplicas(metadata::QuorumPB *quorum,
                                    const TSDescriptorVector& ts_descs,
                                    int nreplicas) {
  // TODO: Select N Replicas
  // at the moment we have to scan all the tablets to build a map TS -> tablets
  // to know how many tablets a TS has... so, let's do a dumb assignment for now.
  int index = rand();
  for (int i = 0; i < nreplicas; ++i) {
    const TSDescriptor *ts = ts_descs[index++ % ts_descs.size()].get();

    TSRegistrationPB reg;
    ts->GetRegistration(&reg);

    QuorumPeerPB *peer = quorum->add_peers();
    peer->set_role(QuorumPeerPB::CANDIDATE);
    peer->set_permanent_uuid(ts->permanent_uuid());

    // TODO: This is temporary, we will use only UUIDs
    BOOST_FOREACH(const HostPortPB& addr, reg.rpc_addresses()) {
      peer->mutable_last_known_addr()->CopyFrom(addr);
    }
  }

  // TODO: Select the leader
  QuorumPeerPB *leader = quorum->mutable_peers(rand() % nreplicas);
  leader->set_role(QuorumPeerPB::LEADER);
}

bool CatalogManager::BuildLocationsForTablet(const scoped_refptr<TabletInfo>& tablet,
                                             TabletLocationsPB* locs_pb) {
  TSRegistrationPB reg;
  vector<TabletReplica> locs;
  {
    TabletMetadataLock l_tablet(tablet.get(), TabletMetadataLock::READ);

    if (!l_tablet.data().is_running()) return false;

    locs.clear();
    tablet->GetLocations(&locs);
  }

  locs_pb->set_tablet_id(tablet->tablet_id());

  BOOST_FOREACH(const TabletReplica& replica, locs) {
    TabletLocationsPB_ReplicaPB* replica_pb = locs_pb->add_replicas();
    replica_pb->set_role(replica.role);

    TSInfoPB* tsinfo_pb = replica_pb->mutable_ts_info();
    tsinfo_pb->set_permanent_uuid(replica.ts_desc->permanent_uuid());

    replica.ts_desc->GetRegistration(&reg);
    tsinfo_pb->mutable_rpc_addresses()->Swap(reg.mutable_rpc_addresses());
  }
  return true;
}

bool CatalogManager::GetTabletLocations(const std::string& tablet_id,
                                        TabletLocationsPB* locs_pb) {
  locs_pb->mutable_replicas()->Clear();
  scoped_refptr<TabletInfo> info;
  {
    boost::shared_lock<LockType> l(lock_);
    info = FindPtrOrNull(tablet_map_, tablet_id);
    if (info == NULL) {
      return false;
    }
  }

  return BuildLocationsForTablet(info, locs_pb);
}

Status CatalogManager::GetTableLocations(const GetTableLocationsRequestPB* req,
                                         GetTableLocationsResponsePB* resp) {
  // If start-key is > end-key report an error instead of swap the two
  // since probably there is something wrong app-side.
  if (req->has_start_key() && req->has_end_key() && req->start_key() > req->end_key()) {
    return Status::InvalidArgument("start-key is greater than end_key");
  }

  if (req->max_returned_locations() <= 0) {
    return Status::InvalidArgument("max_returned_locations must be greater than 0");
  }

  scoped_refptr<TableInfo> table;
  RETURN_NOT_OK(FindTable(req->table(), &table));

  if (table == NULL) {
    Status s = Status::NotFound("The table does not exist");
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TableMetadataLock l(table.get(), TableMetadataLock::READ);
  if (l.data().is_deleted()) {
    Status s = Status::NotFound("The table was deleted",
                                l.data().pb.state_msg());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  if (!l.data().is_running()) {
    Status s = Status::ServiceUnavailable("The table is not running");
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  vector<scoped_refptr<TabletInfo> > tablets_in_range;
  table->GetTabletsInRange(req, &tablets_in_range);

  TSRegistrationPB reg;
  vector<TabletReplica> locs;
  BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets_in_range) {
    if (!BuildLocationsForTablet(tablet, resp->add_tablet_locations())) {
      resp->mutable_tablet_locations()->RemoveLast();
    }
  }
  return Status::OK();
}

void CatalogManager::DumpState(std::ostream* out) const {
  TableInfoMap ids_copy, names_copy;
  TabletInfoMap tablets_copy;

  // Copy the internal state so that, if the output stream blocks,
  // we don't end up holding the lock for a long time.
  {
    boost::shared_lock<LockType> l(lock_);
    ids_copy = table_ids_map_;
    names_copy = table_names_map_;
    tablets_copy = tablet_map_;
  }

  *out << "Tables:\n";
  BOOST_FOREACH(const TableInfoMap::value_type& e, ids_copy) {
    TableInfo* t = e.second.get();
    TableMetadataLock l(t, TableMetadataLock::READ);
    const string& name = l.data().name();

    *out << t->id() << ":\n";
    *out << "  name: \"" << strings::CHexEscape(name) << "\"\n";
    // Erase from the map, so later we can check that we don't have
    // any orphaned tables in the by-name map that aren't in the
    // by-id map.
    if (names_copy.erase(name) != 1) {
      *out << "  [not present in by-name map]\n";
    }
    *out << "  metadata: " << l.data().pb.ShortDebugString() << "\n";

    *out << "  tablets:\n";

    vector<scoped_refptr<TabletInfo> > table_tablets;
    t->GetAllTablets(&table_tablets);
    BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, table_tablets) {
      TabletMetadataLock l_tablet(tablet.get(), TabletMetadataLock::READ);
      *out << "    " << tablet->tablet_id() << ": "
           << l_tablet.data().pb.ShortDebugString() << "\n";

      if (tablets_copy.erase(tablet->tablet_id()) != 1) {
        *out << "  [ERROR: not present in CM tablet map!]\n";
      }
    }
  }

  if (!tablets_copy.empty()) {
    *out << "Orphaned tablets (not referenced by any table):\n";
    BOOST_FOREACH(const TabletInfoMap::value_type& entry, tablets_copy) {
      const scoped_refptr<TabletInfo>& tablet = entry.second;
      TabletMetadataLock l_tablet(tablet.get(), TabletMetadataLock::READ);
      *out << "    " << tablet->tablet_id() << ": "
           << l_tablet.data().pb.ShortDebugString() << "\n";
    }
  }

  if (!names_copy.empty()) {
    *out << "Orphaned tables (in by-name map, but not id map):\n";
    BOOST_FOREACH(const TableInfoMap::value_type& e, names_copy) {
      *out << e.second->id() << ":\n";
      *out << "  name: \"" << CHexEscape(e.first) << "\"\n";
    }
  }
}

////////////////////////////////////////////////////////////
// TabletInfo
////////////////////////////////////////////////////////////

TabletInfo::TabletInfo(const scoped_refptr<TableInfo>& table,
                       const std::string& tablet_id)
  : tablet_id_(tablet_id), table_(table),
    last_update_ts_(MonoTime::Now(MonoTime::FINE)),
    reported_schema_version_(0) {
}

TabletInfo::~TabletInfo() {
}

std::string TabletInfo::ToString() const {
  return Substitute("$0 (Table $1)", tablet_id_,
                    (table_ != NULL ? table_->ToString() : "MISSING"));
}

void TabletInfo::AddReplica(TSDescriptor* ts_desc,
                            metadata::TabletStatePB state,
                            QuorumPeerPB::Role role) {
  boost::lock_guard<simple_spinlock> l(lock_);

  last_update_ts_ = MonoTime::Now(MonoTime::FINE);
  BOOST_FOREACH(TabletReplica& replica, locations_) {
    if (replica.ts_desc == ts_desc) {
      // Just update the existing replica
      VLOG(2) << tablet_id_ << " on " << ts_desc->permanent_uuid()
              << " changed state from "
              << TabletStatePB_Name(replica.state) << "->"
              << TabletStatePB_Name(state);
      replica.state = state;
      return;
    }
  }
  VLOG(2) << tablet_id_ << " reported on " << ts_desc->permanent_uuid()
          << " in state " << TabletStatePB_Name(state);

  TabletReplica r;
  r.ts_desc = ts_desc;
  r.state = state;
  r.role = role;
  locations_.push_back(r);
}

void TabletInfo::ClearReplicasOnTS(const TSDescriptor* ts) {
  boost::lock_guard<simple_spinlock> l(lock_);

  std::vector<TabletReplica>::iterator it = locations_.begin();
  while (it != locations_.end()) {
    if (it->ts_desc == ts) {
      it = locations_.erase(it);
    } else {
      ++it;
    }
  }
}

void TabletInfo::GetLocations(std::vector<TabletReplica>* locations) const {
  boost::lock_guard<simple_spinlock> l(lock_);
  *locations = locations_;
}

bool TabletInfo::set_reported_schema_version(uint32_t version) {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (version > reported_schema_version_) {
    reported_schema_version_ = version;
    return true;
  }
  return false;
}

bool PersistentTabletInfo::IsQuorumLeader(const TSDescriptor* ts_desc) const {
  BOOST_FOREACH(const QuorumPeerPB& peer, pb.quorum().peers()) {
    if (peer.role() == QuorumPeerPB::LEADER &&
        peer.permanent_uuid() == ts_desc->permanent_uuid()) {
      return true;
    }
  }
  return false;
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

std::string TableInfo::ToString() const {
  TableMetadataLock l(this, TableMetadataLock::READ);
  return Substitute("$0 [id=$1]", l.data().pb.name(), table_id_);
}

void TableInfo::AddTablet(TabletInfo *tablet) {
  boost::lock_guard<simple_spinlock> l(lock_);
  AddTabletUnlocked(tablet);
}

void TableInfo::AddTablets(const vector<TabletInfo*>& tablets) {
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    AddTabletUnlocked(tablet);
  }
}

void TableInfo::AddTabletUnlocked(TabletInfo* tablet) {
  TabletInfo* old = NULL;
  if (UpdateReturnCopy(&tablet_map_, tablet->metadata().dirty().pb.start_key(), tablet, &old)) {
    VLOG(1) << "Replaced tablet " << old->tablet_id() << " with " << tablet->tablet_id();
    // TODO: can we assert that the replaced tablet is not in Running state?
    // May be a little tricky since we don't know whether to look at its committed or
    // uncommitted state.
  }
}

void TableInfo::GetTabletsInRange(const GetTableLocationsRequestPB* req,
                                  vector<scoped_refptr<TabletInfo> > *ret) const {
  boost::lock_guard<simple_spinlock> l(lock_);
  int max_returned_locations = req->max_returned_locations();

  TableInfo::TabletInfoMap::const_iterator it, it_end;
  if (req->has_start_key()) {
    it = tablet_map_.upper_bound(req->start_key());
    --it;
  } else {
    it = tablet_map_.begin();
  }

  if (req->has_end_key()) {
    it_end = tablet_map_.upper_bound(req->end_key());
  } else {
    it_end = tablet_map_.end();
  }

  int count = 0;
  for (; it != it_end && count < max_returned_locations; ++it) {
    ret->push_back(make_scoped_refptr(it->second));
    count++;
  }
}

bool TableInfo::IsAlterInProgress(uint32_t version) const {
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const TableInfo::TabletInfoMap::value_type& e, tablet_map_) {
    if (e.second->reported_schema_version() < version) {
      return true;
    }
  }
  return false;
}

void TableInfo::AddTask(MonitoredTask* task) {
  boost::lock_guard<simple_spinlock> l(lock_);
  task->AddRef();
  pending_tasks_.insert(task);
}

void TableInfo::RemoveTask(MonitoredTask* task) {
  boost::lock_guard<simple_spinlock> l(lock_);
  pending_tasks_.erase(task);
  task->Release();
}

void TableInfo::AbortTasks() {
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(MonitoredTask* task, pending_tasks_) {
    task->Abort();
  }
}

void TableInfo::GetTaskList(std::vector<scoped_refptr<MonitoredTask> > *ret) {
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(MonitoredTask* task, pending_tasks_) {
    ret->push_back(make_scoped_refptr(task));
  }
}

void TableInfo::GetAllTablets(vector<scoped_refptr<TabletInfo> > *ret) const {
  ret->clear();
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const TableInfo::TabletInfoMap::value_type& e, tablet_map_) {
    ret->push_back(make_scoped_refptr(e.second));
  }
}

void PersistentTableInfo::set_state(SysTablesEntryPB::State state, const string& msg) {
  pb.set_state(state);
  pb.set_state_msg(msg);
}

} // namespace master
} // namespace kudu
