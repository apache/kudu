// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
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

#include "kudu/master/catalog_manager.h"

#include <boost/foreach.hpp>
#include <boost/assign/list_of.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>

#include <algorithm>
#include <string>
#include <vector>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/monotime.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"
#include "kudu/cfile/type_encodings.h"

DEFINE_int32(async_rpc_timeout_ms, 10 * 1000, // 10 sec
             "Timeout used for the Master->TS async rpc calls. "
             "(Advanced option)");
DEFINE_int32(assignment_timeout_ms, 10 * 1000, // 10 sec
             "Timeout used for Master->TS assignment requests. "
             "(Advanced option)");
DEFINE_int32(unresponsive_ts_rpc_timeout_ms, 30 * 1000, // 30 sec
             "(Advanced option)");
DEFINE_int32(default_num_replicas, 1, // TODO switch to 3 and fix SelectReplicas()
             "Default number of replicas for tables that do have the num_replicas set. "
             "(Advanced option)");
DEFINE_int32(catalog_manager_bg_task_wait_ms, 1000,
             "Amount of time the catalog manager background task thread waits "
             "between runs");
DEFINE_int32(max_create_tablets_per_ts, 20,
             "The number of tablets per TS that can be requested for a new table. "
             "(Advanced option)");

namespace kudu {
namespace master {

using base::subtle::NoBarrier_Load;
using base::subtle::NoBarrier_CompareAndSwap;
using cfile::TypeEncodingInfo;
using metadata::QuorumPeerPB;
using rpc::RpcContext;
using std::string;
using std::vector;
using strings::Substitute;
using tablet::TabletPeer;
using tablet::TabletStatePB;
using tserver::TabletServerErrorPB;

////////////////////////////////////////////////////////////
// Table Loader
////////////////////////////////////////////////////////////

class TableLoader : public TableVisitor {
 public:
  explicit TableLoader(CatalogManager *catalog_manager)
    : catalog_manager_(catalog_manager) {
  }

  virtual Status VisitTable(const std::string& table_id,
                            const SysTablesEntryPB& metadata) OVERRIDE {
    CHECK(!ContainsKey(catalog_manager_->table_ids_map_, table_id))
          << "Table already exists: " << table_id;

    // Setup the table info
    TableInfo *table = new TableInfo(table_id);
    TableMetadataLock l(table, TableMetadataLock::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);

    // Add the tablet to the IDs map and to the name map (if the table is not deleted)
    catalog_manager_->table_ids_map_[table->id()] = table;
    if (!l.data().is_deleted()) {
      catalog_manager_->table_names_map_[l.data().name()] = table;
    }

    LOG(INFO) << "Loaded metadata for table " << table->ToString();
    VLOG(1) << "Metadata for table " << table->ToString() << ": " << metadata.ShortDebugString();
    l.Commit();
    return Status::OK();
  }

 private:
  CatalogManager *catalog_manager_;

  DISALLOW_COPY_AND_ASSIGN(TableLoader);
};

////////////////////////////////////////////////////////////
// Tablet Loader
////////////////////////////////////////////////////////////

class TabletLoader : public TabletVisitor {
 public:
  explicit TabletLoader(CatalogManager *catalog_manager)
    : catalog_manager_(catalog_manager) {
  }

  virtual Status VisitTablet(const std::string& table_id,
                             const std::string& tablet_id,
                             const SysTabletsEntryPB& metadata) OVERRIDE {
    // Lookup the table
    scoped_refptr<TableInfo> table(FindPtrOrNull(
                                     catalog_manager_->table_ids_map_, table_id));

    // Setup the tablet info
    TabletInfo* tablet = new TabletInfo(table, tablet_id);
    TabletMetadataLock l(tablet, TabletMetadataLock::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);

    // Add the tablet to the tablet manager
    catalog_manager_->tablet_map_[tablet->tablet_id()] = tablet;

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

    LOG(INFO) << "Loaded metadata for tablet " << tablet_id
              << " (table " << table->ToString() << ")";
    VLOG(2) << "Metadata for tablet " << tablet_id << ": " << metadata.ShortDebugString();

    return Status::OK();
  }

 private:
  CatalogManager *catalog_manager_;

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
    pending_updates_ = true;
    cond_.notify_all();
  }

  void Wait(int msec) {
    boost::unique_lock<boost::mutex> lock(lock_);
    if (closing_) return;
    if (!pending_updates_) {
      boost::system_time wtime = boost::get_system_time() + boost::posix_time::milliseconds(msec);
      cond_.timed_wait(lock, wtime);
    }
    pending_updates_ = false;
  }

  void WakeIfHasPendingUpdates() {
    boost::lock_guard<boost::mutex> lock(lock_);
    if (pending_updates_) {
      cond_.notify_all();
    }
  }

 private:
  void Run();

 private:
  Atomic32 closing_;
  bool pending_updates_;
  mutable boost::mutex lock_;
  boost::condition_variable cond_;
  scoped_refptr<kudu::Thread> thread_;
  CatalogManager *catalog_manager_;
};

Status CatalogManagerBgTasks::Init() {
  RETURN_NOT_OK(kudu::Thread::Create("catalog manager", "bgtasks",
      &CatalogManagerBgTasks::Run, this, &thread_));
  return Status::OK();
}

void CatalogManagerBgTasks::Shutdown() {
  if (Acquire_CompareAndSwap(&closing_, false, true) != false) {
    VLOG(2) << "CatalogManagerBgTasks already shut down";
    return;
  }

  Wake();
  if (thread_ != NULL) {
    CHECK_OK(ThreadJoiner(thread_.get()).Join());
  }
}

void CatalogManagerBgTasks::Run() {
  while (!NoBarrier_Load(&closing_)) {
    std::vector<scoped_refptr<TabletInfo> > to_delete;
    std::vector<scoped_refptr<TabletInfo> > to_process;

    // Get list of tablets not yet running or already replaced.
    catalog_manager_->ExtractTabletsToProcess(&to_delete, &to_process);

    if (!to_process.empty()) {
      // Transition tablet assignment state from preparing to creating, send
      // and schedule creation / deletion RPC messages, etc.
      catalog_manager_->ProcessPendingAssignments(to_process);
    }

    //if (!to_delete.empty()) {
      // TODO: Run the cleaner
    //}

    // Wait for a notification or a timeout expiration.
    //  - CreateTable will call Wake() to notify about the tablets to add
    //  - HandleReportedTablet/ProcessPendingAssignments will call WakeIfHasPendingUpdates()
    //    to notify about tablets creation.
    Wait(FLAGS_catalog_manager_bg_task_wait_ms);
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
  : master_(master),
    state_(kConstructed) {
}

CatalogManager::~CatalogManager() {
  Shutdown();
}

Status CatalogManager::Init(bool is_first_run) {
  {
    boost::lock_guard<simple_spinlock> l(state_lock_);
    CHECK_EQ(kConstructed, state_);
    state_ = kStarting;
  }

  RETURN_NOT_OK_PREPEND(InitSysCatalogAsync(is_first_run),
                        "Failed to initialize sys tables async");

  // WaitUntilRunning() must run outside of the lock as to prevent
  // deadlock. This is safe as WaitUntilRunning waits for another
  // thread to finish its work and doesn't itself depend on any state
  // within CatalogManager.

  RETURN_NOT_OK_PREPEND(sys_catalog_->WaitUntilRunning(),
                        "Failed waiting for the catalog tablet to run");

  boost::lock_guard<LockType> l(lock_);

  if (!is_first_run) {
    TableLoader table_loader(this);
    RETURN_NOT_OK_PREPEND(sys_catalog_->VisitTables(&table_loader),
                          "Failed while visiting tables in sys catalog");

    TabletLoader tablet_loader(this);
    RETURN_NOT_OK_PREPEND(sys_catalog_->VisitTablets(&tablet_loader),
                          "Failed while visiting tablets in sys catalog");
  }

  background_tasks_.reset(new CatalogManagerBgTasks(this));
  RETURN_NOT_OK_PREPEND(background_tasks_->Init(),
                        "Failed to initialize catalog manager background tasks");

  {
    boost::lock_guard<simple_spinlock> l(state_lock_);
    CHECK_EQ(kStarting, state_);
    state_ = kRunning;
  }

  return Status::OK();
}

Status CatalogManager::InitSysCatalogAsync(bool is_first_run) {
  boost::lock_guard<LockType> l(lock_);
  sys_catalog_.reset(new SysCatalogTable(master_, master_->metric_registry()));
  if (is_first_run) {
    RETURN_NOT_OK(sys_catalog_->CreateNew(master_->fs_manager()));
  } else {
    RETURN_NOT_OK(sys_catalog_->Load(master_->fs_manager()));
  }
  return Status::OK();
}

bool CatalogManager::IsInitialized() const {
  boost::lock_guard<simple_spinlock> l(state_lock_);
  return state_ == kRunning;
}

QuorumPeerPB::Role CatalogManager::Role() const {
  CHECK(IsInitialized());
  return sys_catalog_->tablet_peer_->consensus()->role();
}

void CatalogManager::Shutdown() {
  {
    boost::shared_lock<LockType> l(lock_);
    if (state_ == kClosing) {
      VLOG(2) << "CatalogManager already shut down";
      return;
    }
    state_ = kClosing;
  }

  // Shutdown the Catalog Manager background thread
  if (background_tasks_) {
    background_tasks_->Shutdown();
  }

  // Abort and Wait tables task completion
  BOOST_FOREACH(const TableInfoMap::value_type& e, table_ids_map_) {
    e.second->AbortTasks();
    e.second->WaitTasksCompletion();
  }

  // Shut down the underlying storage for tables and tablets.
  if (sys_catalog_) {
    sys_catalog_->Shutdown();
  }
}

static void SetupError(MasterErrorPB* error,
                       MasterErrorPB::Code code,
                       const Status& s) {
  StatusToPB(s, error->mutable_status());
  error->set_code(code);
}

Status CatalogManager::CheckOnline() const {
  if (PREDICT_FALSE(!IsInitialized())) {
    return Status::ServiceUnavailable("CatalogManager is not running");
  }
  return Status::OK();
}

// Create a new table.
// See README file in this directory for a description of the design.
Status CatalogManager::CreateTable(const CreateTableRequestPB* req,
                                   CreateTableResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  RETURN_NOT_OK(CheckOnline());

  // a. Validate the user request.
  Schema schema;
  RETURN_NOT_OK(SchemaFromPB(req->schema(), &schema));
  if (schema.has_column_ids()) {
    Status s = Status::InvalidArgument("User requests should not have Column IDs");
    SetupError(resp->mutable_error(), MasterErrorPB::INVALID_SCHEMA, s);
    return s;
  }
  schema = schema.CopyWithColumnIds();

  int max_tablets = FLAGS_max_create_tablets_per_ts * master_->ts_manager()->GetCount();
  if (req->num_replicas() > 1 && max_tablets > 0 && req->pre_split_keys_size() > max_tablets) {
    Status s = Status::InvalidArgument(Substitute("The number of tablets requested is over the "
                                                  "permitted maximum ($0)", max_tablets));
    SetupError(resp->mutable_error(), MasterErrorPB::TOO_MANY_TABLETS, s);
    return s;
  }

  LOG(INFO) << "CreateTable from " << RequestorString(rpc)
            << ":\n" << req->DebugString();

  scoped_refptr<TableInfo> table;
  vector<TabletInfo*> tablets;
  {
    boost::lock_guard<LockType> l(lock_);
    TRACE("Acquired catalog manager lock");

    // b. Verify that the table does not exist.
    table = FindPtrOrNull(table_names_map_, req->name());
    if (table != NULL) {
      Status s = Status::AlreadyPresent("Table already exists", table->id());
      SetupError(resp->mutable_error(), MasterErrorPB::TABLE_ALREADY_PRESENT, s);
      return s;
    }

    // c. Add the new table in "preparing" state.
    table = CreateTableInfo(req, schema);
    table_ids_map_[table->id()] = table;
    table_names_map_[req->name()] = table;

    // d. Create the TabletInfo objects in state kTabletStatePreparing.
    CreateTablets(req, table.get(), &tablets);

    // Add the table/tablets to the in-memory map for the assignment.
    resp->set_table_id(table->id());
    table->AddTablets(tablets);
    BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
      InsertOrDie(&tablet_map_, tablet->tablet_id(), tablet);
    }
  }
  TRACE("Inserted new table and tablet info into CatalogManager maps");

  // NOTE: the table and tablets are already locked for write at this point,
  // since the CreateTableInfo/CreateTabletInfo functions leave them in that state.
  // They will get committed at the end of this function.
  // Sanity check: the tables and tablets should all be in "preparing" state.
  CHECK_EQ(SysTablesEntryPB::kTableStatePreparing, table->metadata().dirty().pb.state());
  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    CHECK_EQ(SysTabletsEntryPB::kTabletStatePreparing,
             tablet->metadata().dirty().pb.state());
  }

  // e. Write Tablets to sys-tablets (in "preparing" state)
  Status s = sys_catalog_->AddTablets(tablets);
  if (!s.ok()) {
    // TODO: we could potentially handle this error case by returning an error,
    // since even if we mistakenly believe this failed, when it actually made it
    // to disk, we'll end up removing the orphaned tablets later.
    PANIC_RPC(rpc, Substitute("An error occurred while inserting to sys-tablets: $0",
                              s.ToString()));
  }
  TRACE("Wrote tablets to system table");

  // f. Update the on-disk table state to "running" (point of no return).
  table->mutable_metadata()->mutable_dirty()->pb.set_state(SysTablesEntryPB::kTableStateRunning);
  s = sys_catalog_->AddTable(table.get());
  if (!s.ok()) {
    PANIC_RPC(rpc, Substitute("An error occurred while inserting to sys-tablets: $0",
                              s.ToString()));
  }
  TRACE("Wrote table to system table");

  // g. Commit the in-memory state.
  table->mutable_metadata()->CommitMutation();

  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    tablet->mutable_metadata()->CommitMutation();
  }

  VLOG(1) << "Created table " << table->ToString();
  background_tasks_->Wake();
  return Status::OK();
}

Status CatalogManager::IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                                         IsCreateTableDoneResponsePB* resp) {
  RETURN_NOT_OK(CheckOnline());

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

  // 2. Verify if the create is in-progress
  TRACE("Verify if the table creation is in progress for $0", table->ToString());
  resp->set_done(!table->IsCreateInProgress());

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
  table->mutable_metadata()->StartMutation();
  SysTablesEntryPB *metadata = &table->mutable_metadata()->mutable_dirty()->pb;
  metadata->set_state(SysTablesEntryPB::kTableStatePreparing);
  metadata->set_name(req->name());
  metadata->set_version(0);
  if (req->has_num_replicas()) {
    metadata->set_num_replicas(req->num_replicas());
  } else {
    metadata->set_num_replicas(FLAGS_default_num_replicas);
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
  tablet->mutable_metadata()->StartMutation();
  SysTabletsEntryPB *metadata = &tablet->mutable_metadata()->mutable_dirty()->pb;
  metadata->set_state(SysTabletsEntryPB::kTabletStatePreparing);
  metadata->set_start_key(start_key);
  metadata->set_end_key(end_key);
  metadata->set_table_id(table->id());
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

  RETURN_NOT_OK(CheckOnline());

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

  // 3. Update sys-catalog with the removed table state (point of no return).
  Status s = sys_catalog_->UpdateTable(table.get());
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

  // Send a DeleteTablet() request to each tablet replica in the table.
  SendDeleteTabletRequestsForTable(table);

  LOG(INFO) << "Successfully deleted table " << table->ToString()
            << " per request from " << RequestorString(rpc);
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

        // Verify that encoding is appropriate for the new column's
        // type
        ColumnSchema new_col = ColumnSchemaFromPB(step.add_column().schema());
        const TypeEncodingInfo *dummy;
        RETURN_NOT_OK(TypeEncodingInfo::Get(new_col.type_info()->type(),
                                            new_col.attributes().encoding(),
                                            &dummy));

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

  RETURN_NOT_OK(CheckOnline());

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

  // 5. Update sys-catalog with the new table schema (point of no return!)
  TRACE("Updating metadata on disk");
  Status s = sys_catalog_->UpdateTable(table.get());
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
  RETURN_NOT_OK(CheckOnline());

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
  RETURN_NOT_OK(CheckOnline());

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
    // An AlterTable is in progress; fully_applied_schema is the last
    // schema that has reached every TS.
    CHECK(l.data().pb.state() == SysTablesEntryPB::kTableStateAltering);
    resp->mutable_schema()->CopyFrom(l.data().pb.fully_applied_schema());
  } else {
    // There's no AlterTable, the regular schema is "fully applied".
    resp->mutable_schema()->CopyFrom(l.data().pb.schema());
  }
  resp->set_num_replicas(l.data().pb.num_replicas());

  return Status::OK();
}

Status CatalogManager::ListTables(const ListTablesRequestPB* req,
                                  ListTablesResponsePB* resp) {
  RETURN_NOT_OK(CheckOnline());

  boost::shared_lock<LockType> l(lock_);

  BOOST_FOREACH(const TableInfoMap::value_type& entry, table_names_map_) {
    TableMetadataLock ltm(entry.second.get(), TableMetadataLock::READ);
    if (!ltm.data().is_running()) continue;

    if (req->has_name_filter()) {
      size_t found = ltm.data().name().find(req->name_filter());
      if (found == string::npos) {
        continue;
      }
    }

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

void CatalogManager::NotifyTabletDeleteSuccess(const string& permanent_uuid,
                                               const string& tablet_id) {
  // TODO: Clean up the stale deleted tablet data once all relevant tablet
  // servers have responded that they have removed the remnants of the deleted
  // tablet.
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
    ignore_result(FindCopy(tablet_map_, report.tablet_id(), &tablet));
  }
  if (!tablet) {
    LOG(INFO) << "Got report from unknown tablet " << report.tablet_id()
              << ": Sending delete request for this orphan tablet";
    SendDeleteTabletRequest(report.tablet_id(), NULL, ts_desc,
                            "Report from unknown tablet");
    return Status::OK();
  }

  // TODO: we don't actually need to do the COW here until we see we're going
  // to change the state. Can we change CowedObject to lazily do the copy?
  TableMetadataLock table_lock(tablet->table(), TableMetadataLock::READ);
  TabletMetadataLock tablet_lock(tablet.get(), TabletMetadataLock::WRITE);

  if (tablet_lock.data().is_deleted()) {
    report_updates->set_state_msg(tablet_lock.data().pb.state_msg());
    const string msg = tablet_lock.data().pb.state_msg();
    LOG(INFO) << "Got report from replaced tablet " << tablet->ToString()
              << " (" << msg << "): Sending delete request for this tablet";
    // TODO: Cancel tablet creation, instead of deleting, in cases where
    // that might be possible (tablet creation timeout & replacement).
    SendDeleteTabletRequest(tablet->tablet_id(), tablet->table(), ts_desc,
                            Substitute("Tablet deleted: $0", msg));
    return Status::OK();
  }

  if (!table_lock.data().is_running()) {
    LOG(INFO) << "Got report from tablet " << tablet->tablet_id()
              << " for non-running table " << tablet->table()->ToString() << ": "
              << tablet_lock.data().pb.state_msg();
    report_updates->set_state_msg(tablet_lock.data().pb.state_msg());
    return Status::OK();
  }

  // Check if the tablet requires an "alter table" call
  bool alter_requested = false;
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
    alter_requested = true;
  }


  table_lock.Unlock();

  if (report.has_error()) {
    Status s = StatusFromPB(report.error());
    DCHECK(!s.ok());
    DCHECK_EQ(report.state(), tablet::FAILED);
    LOG(WARNING) << "Tablet " << tablet->ToString() << " has failed on TS "
                 << ts_desc->permanent_uuid() << ": " << s.ToString();
    return Status::OK();
  }

  if (report.has_schema_version() && !alter_requested) {
    HandleTabletSchemaVersionReport(tablet.get(), report.schema_version());
  }

  int64 current_seqno = tablet_lock.data().pb.quorum().seqno();

  if (report.has_quorum() && report.quorum().seqno() >= current_seqno) {
    // If the tablet was not RUNNING mark it as such
    if (!tablet_lock.data().is_running() && report.state() == tablet::RUNNING) {
      DCHECK(tablet_lock.data().pb.state() == SysTabletsEntryPB::kTabletStateCreating);
      // Mark the tablet as running
      // TODO: we could batch the IO onto a background thread, or at least
      // across multiple tablets in the same report.
      VLOG(1) << "Tablet " << tablet->ToString() << " is now online";
      tablet_lock.mutable_data()->set_state(SysTabletsEntryPB::kTabletStateRunning,
                                            "Tablet reported by leader");
    }

    // If a replica is reporting a new quorum, reset the tablet's replicas. Note that
    // we leave out replicas who live in tablet servers who have not heartbeated to
    // master yet.
    if (report.quorum().seqno() > current_seqno) {
      LOG(INFO) << "Tablet: " << tablet->tablet_id() << " reported quorum change."
          " New quorum: " << report.quorum().ShortDebugString();
      ResetTabletReplicasFromReportedQuorum(ts_desc, report, tablet, &tablet_lock);
    // If some replica is reporting the same quorum we already know about and hasn't
    // been added as replica, add it.
    } else if (report.quorum().seqno() == current_seqno) {
      AddReplicaToTabletIfNotFound(ts_desc, report, tablet);
    }
  }

  // We update the tablets each time the someone reports it.
  // This shouldn't be very frequent and should only happen when something in fact changed.
  Status s = sys_catalog_->UpdateTablets(boost::assign::list_of(tablet.get()));
  if (!s.ok()) {
    // panic-mode: abort the master
    LOG(FATAL) << "An error occurred while updating sys-tablets: " << s.ToString();
  }
  tablet_lock.Commit();
  return Status::OK();
}

void CatalogManager::ResetTabletReplicasFromReportedQuorum(TSDescriptor* ts_desc,
                                                           const ReportedTabletPB& report,
                                                           const scoped_refptr<TabletInfo>& tablet,
                                                           TabletMetadataLock* tablet_lock) {
  tablet_lock->mutable_data()->pb.mutable_quorum()->CopyFrom(report.quorum());
  vector<TabletReplica> replicas;
  BOOST_FOREACH(const metadata::QuorumPeerPB& peer, report.quorum().peers()) {
    std::tr1::shared_ptr<TSDescriptor> ts_desc;
    Status status = master_->ts_manager()->LookupTSByUUID(peer.permanent_uuid(), &ts_desc);
    if (status.IsNotFound()) {
      LOG(WARNING) << "Cannot find TabletServer descriptor for tablet replica. Tablet: "
          << tablet->tablet_id() << " Peer: " << peer.ShortDebugString();
      continue;
    }
    CHECK_OK(status);
    TabletReplica replica;
    replica.state = report.state();
    replica.role = peer.role();
    replica.ts_desc = ts_desc.get();
    replicas.push_back(replica);
  }
  tablet->ResetReplicas(replicas);
}

void CatalogManager::AddReplicaToTabletIfNotFound(TSDescriptor* ts_desc,
                                                  const ReportedTabletPB& report,
                                                  const scoped_refptr<TabletInfo>& tablet) {
  vector<TabletReplica> locations;
  tablet->GetLocations(&locations);
  bool found_replica = false;
  BOOST_FOREACH(const TabletReplica& replica, locations) {
    if (replica.ts_desc->permanent_uuid() == ts_desc->permanent_uuid()) {
      found_replica = true;
      break;
    }
  }
  if (!found_replica) {
    TabletReplica replica;
    replica.state = report.state();
    replica.role = report.role();
    replica.ts_desc = ts_desc;
    locations.push_back(replica);
    tablet->ResetReplicas(locations);
  }
}

Status CatalogManager::GetTabletPeer(const string& tablet_id,
                                     scoped_refptr<TabletPeer>* tablet_peer) const {
  // Note: CatalogManager has only one table, 'sys_catalog', with only
  // one tablet.
  boost::shared_lock<LockType> l(lock_);
  CHECK(sys_catalog_.get() != NULL) << "sys_catalog_ must be initialized!";
  if (sys_catalog_->tablet_id() == tablet_id) {
    *tablet_peer = sys_catalog_->tablet_peer();
  } else {
    return Status::NotFound(Substitute("no SysTable exists with tablet_id $0 in CatalogManager",
                                       tablet_id));
  }
  return Status::OK();
}

const NodeInstancePB& CatalogManager::NodeInstance() const {
  return master_->instance_pb();
}

class AsyncTabletRequestTask : public MonitoredTask {
 public:
  AsyncTabletRequestTask(Master *master,
                         const string& permanent_uuid,
                         const scoped_refptr<TableInfo>& table)
    : master_(master),
      permanent_uuid_(permanent_uuid),
      table_(table),
      start_ts_(MonoTime::Now(MonoTime::FINE)),
      attempt_(0),
      state_(kStateRunning) {
    deadline_ = start_ts_;
    deadline_.AddDelta(MonoDelta::FromMilliseconds(FLAGS_unresponsive_ts_rpc_timeout_ms));
  }

  // Send the subclass RPC request.
  Status Run() {
    Status s = ResetTSProxy();
    if (!s.ok()) {
      LOG(WARNING) << "Unable to reset TS proxy: " << s.ToString();
      MarkFailed();
      UnregisterAsyncTask(); // May delete this.
      return s.CloneAndPrepend("Failed to reset TS proxy");
    }

    // Calculate and set the timeout deadline.
    MonoTime timeout = MonoTime::Now(MonoTime::FINE);
    timeout.AddDelta(MonoDelta::FromMilliseconds(FLAGS_async_rpc_timeout_ms));
    const MonoTime& deadline = MonoTime::Earliest(timeout, deadline_);
    rpc_.set_deadline(deadline);

    SendRequest(++attempt_);
    return Status::OK();
  }

  // Abort this task.
  virtual void Abort() OVERRIDE {
    MarkAborted();
  }

  virtual State state() const OVERRIDE {
    return static_cast<State>(NoBarrier_Load(&state_));
  }

  virtual MonoTime start_timestamp() const OVERRIDE { return start_ts_; }
  virtual MonoTime completion_timestamp() const OVERRIDE { return end_ts_; }

 protected:
  // Send an RPC request and register a callback.
  virtual void SendRequest(int attempt) = 0;

  // Handle the response from the RPC request. On success, MarkSuccess() must
  // be called to mutate the state_ variable. If retry is desired, then
  // no state change is made. Retries will automatically be attempted as long
  // as the state is kStateRunning and deadline_ has not yet passed.
  virtual void HandleResponse(int attempt) = 0;

  // Return the id of the tablet that is the subject of the async request.
  virtual string tablet_id() const = 0;

  // Transition from running -> complete.
  void MarkComplete() {
    NoBarrier_CompareAndSwap(&state_, kStateRunning, kStateComplete);
  }

  // Transition from running -> aborted.
  void MarkAborted() {
    NoBarrier_CompareAndSwap(&state_, kStateRunning, kStateAborted);
  }

  // Transition from running -> failed.
  void MarkFailed() {
    NoBarrier_CompareAndSwap(&state_, kStateRunning, kStateFailed);
  }

  // Callback meant to be invoked from asynchronous RPC service proxy calls.
  void RpcCallback() {
    if (!rpc_.status().ok()) {
      LOG(WARNING) << "TS " << permanent_uuid_ << ": " << type_name() << " RPC failed for tablet "
                   << tablet_id() << ": " << rpc_.status().ToString();
    } else if (state() != kStateAborted) {
      HandleResponse(attempt_); // Modifies state_.
    }

    // Schedule a retry if the RPC call was not successful.
    if (state() == kStateRunning) {
      MonoTime now = MonoTime::Now(MonoTime::FINE);
      // We assume it might take 10ms to process the request in the best case,
      // fail if we have less than that amount of time remaining.
      int64_t millis_remaining = deadline_.GetDeltaSince(now).ToMilliseconds() - 10;
      // Exponential backoff with jitter.
      int64_t base_delay_ms = 1 << (attempt_ + 3);  // 1st retry delayed 2^4 ms, 2nd 2^5, etc.
      int64_t jitter_ms = rand() % 50;              // Add up to 50ms of additional random delay.
      int64_t delay_millis = std::min<int64_t>(base_delay_ms + jitter_ms, millis_remaining);

      if (delay_millis <= 0) {
        LOG(WARNING) << "Request timed out: " << description();
        MarkFailed();
      } else {
        MonoTime new_start_time = now;
        new_start_time.AddDelta(MonoDelta::FromMilliseconds(delay_millis));
        LOG(INFO) << "Scheduling retry of " << description() << " with a delay"
                  << " of " << delay_millis << "ms (attempt = " << attempt_ << ")...";
        master_->messenger()->ScheduleOnReactor(
            boost::bind(&AsyncTabletRequestTask::RunDelayedTask, this, _1),
            MonoDelta::FromMilliseconds(delay_millis));
        return;
      }
    }

    UnregisterAsyncTask();  // May call 'delete this'.
  }

  Master * const master_;
  const string permanent_uuid_;
  const scoped_refptr<TableInfo> table_;

  MonoTime start_ts_;
  MonoTime end_ts_;
  MonoTime deadline_;

  int attempt_;
  rpc::RpcController rpc_;
  std::tr1::shared_ptr<tserver::TabletServerAdminServiceProxy> ts_proxy_;

 private:
  // Callback for Reactor delayed task mechanism. Called either when it is time
  // to execute the delayed task (with status == OK) or when the task
  // is cancelled, i.e. when the scheduling timer is shut down (status != OK).
  void RunDelayedTask(const Status& status) {
    if (!status.ok()) {
      LOG(WARNING) << "Async tablet task " << description() << " failed or was cancelled: "
                   << status.ToString();
      UnregisterAsyncTask();   // May delete this.
      return;
    }

    string desc = description();  // Save in case we need to log after deletion.
    Status s = Run();             // May delete this.
    if (!s.ok()) {
      LOG(WARNING) << "Async tablet task " << desc << " failed: " << s.ToString();
    }
  }

  // Clean up request and release resources.
  void UnregisterAsyncTask() {
    end_ts_ = MonoTime::Now(MonoTime::FINE);
    if (table_ != NULL) {
      table_->RemoveTask(this);
    } else {
      // This is a floating task (since the table does not exist)
      // created as response to a tablet report.
      Release();  // May call "delete this";
    }
  }

  Status ResetTSProxy() {
    std::tr1::shared_ptr<TSDescriptor> ts_desc;
    std::tr1::shared_ptr<tserver::TabletServerAdminServiceProxy> ts_proxy;
    RETURN_NOT_OK(master_->ts_manager()->LookupTSByUUID(permanent_uuid_, &ts_desc));
    RETURN_NOT_OK(ts_desc->GetProxy(master_->messenger(), &ts_proxy));
    ts_proxy_.swap(ts_proxy);
    rpc_.Reset();
    return Status::OK();
  }

  // Use state() and MarkX() accessors.
  AtomicWord state_;
};

// Fire off the async create tablet.
// This requires that the new tablet info is locked for write, and the
// quorum information has been filled into the 'dirty' data.
class AsyncCreateTablet : public AsyncTabletRequestTask {
 public:
  AsyncCreateTablet(Master *master,
                    const string& permanent_uuid,
                    const scoped_refptr<TabletInfo>& tablet)
    : AsyncTabletRequestTask(master, permanent_uuid, tablet->table()),
      tablet_(tablet) {
    deadline_ = start_ts_;
    deadline_.AddDelta(MonoDelta::FromMilliseconds(FLAGS_assignment_timeout_ms));

    TableMetadataLock table_lock(tablet_->table(), TableMetadataLock::READ);
    const SysTabletsEntryPB& tablet_pb = tablet_->metadata().dirty().pb;

    req_.set_table_id(tablet_->table()->id());
    req_.set_tablet_id(tablet_->tablet_id());
    req_.set_start_key(tablet_pb.start_key());
    req_.set_end_key(tablet_pb.end_key());
    req_.set_table_name(table_lock.data().pb.name());
    req_.mutable_schema()->CopyFrom(table_lock.data().pb.schema());
    req_.mutable_quorum()->CopyFrom(tablet_pb.quorum());
  }

  virtual string type_name() const OVERRIDE { return "Create Tablet"; }

  virtual string description() const OVERRIDE {
    return "CreateTablet RPC for tablet " + tablet_->ToString() + " on TS " + permanent_uuid_;
  }

 protected:
  virtual string tablet_id() const OVERRIDE { return tablet_->tablet_id(); }

  virtual void HandleResponse(int attempt) OVERRIDE {
    if (!resp_.has_error()) {
      MarkComplete();
    } else {
      Status s = StatusFromPB(resp_.error().status());
      if (s.IsAlreadyPresent()) {
        LOG(INFO) << "CreateTablet RPC for tablet " << tablet_->tablet_id()
                  << " on TS " << permanent_uuid_ << " returned already present: "
                  << s.ToString();
        MarkComplete();
      } else {
        LOG(WARNING) << "CreateTablet RPC for tablet " << tablet_->tablet_id()
                     << " on TS " << permanent_uuid_ << " failed: " << s.ToString();
      }
    }
  }

  virtual void SendRequest(int attempt) OVERRIDE {
    ts_proxy_->CreateTabletAsync(req_, &resp_, &rpc_,
                                 boost::bind(&AsyncCreateTablet::RpcCallback, this));
    VLOG(1) << "Send create tablet request to " << permanent_uuid_ << ":\n"
            << " (attempt " << attempt << "):\n"
            << req_.DebugString();
  }

 private:
  const scoped_refptr<TabletInfo> tablet_;
  tserver::CreateTabletRequestPB req_;
  tserver::CreateTabletResponsePB resp_;
};

// Send a DeleteTablet() RPC request.
class AsyncDeleteTablet : public AsyncTabletRequestTask {
 public:
  AsyncDeleteTablet(Master *master,
                    const string& permanent_uuid,
                    const scoped_refptr<TableInfo>& table,
                    const std::string& tablet_id,
                    const string& reason)
    : AsyncTabletRequestTask(master, permanent_uuid, table),
      tablet_id_(tablet_id),
      reason_(reason) {
    }

  virtual string type_name() const OVERRIDE { return "Delete Tablet"; }

  virtual string description() const OVERRIDE {
    return tablet_id_ + " Delete Tablet RPC for TS=" + permanent_uuid_;
  }

 protected:
  virtual string tablet_id() const OVERRIDE { return tablet_id_; }

  virtual void HandleResponse(int attempt) OVERRIDE {
    if (resp_.has_error()) {
      Status status = StatusFromPB(resp_.error().status());

      // Do not retry on a fatal error
      switch (resp_.error().code()) {
        case TabletServerErrorPB::TABLET_NOT_FOUND:
          LOG(WARNING) << "TS " << permanent_uuid_ << ": delete failed for tablet " << tablet_id_
                       << " no further retry: " << status.ToString();
          MarkComplete();
          break;
        default:
          LOG(WARNING) << "TS " << permanent_uuid_ << ": delete failed for tablet " << tablet_id_
                       << ": " << status.ToString();
          break;
      }
    } else {
      master_->catalog_manager()->NotifyTabletDeleteSuccess(permanent_uuid_, tablet_id_);
      if (table_) {
        LOG(INFO) << "Tablet " << tablet_id_ << " (table " << table_->ToString() << ") "
                  << "successfully deleted";
      } else {
        LOG(WARNING) << "Tablet " << tablet_id_ << " did not belong to a known table, but was "
                        "successfully deleted";
      }
      MarkComplete();
      VLOG(1) << "TS " << permanent_uuid_ << ": delete complete on tablet " << tablet_id_;
    }
  }

  virtual void SendRequest(int attempt) OVERRIDE {
    tserver::DeleteTabletRequestPB req;
    req.set_tablet_id(tablet_id_);
    req.set_reason(reason_);

    ts_proxy_->DeleteTabletAsync(req, &resp_, &rpc_,
                                 boost::bind(&AsyncDeleteTablet::RpcCallback, this));
    VLOG(1) << "Send delete tablet request to " << permanent_uuid_
            << " (attempt " << attempt << "):\n"
            << req.DebugString();
  }

  const std::string tablet_id_;
  const std::string reason_;
  tserver::DeleteTabletResponsePB resp_;
};

// Send the "Alter Table" with latest table schema
// keeps retrying until we get an "ok" response.
//  - Alter completed
//  - TS has already a newer version
//    (which may happen in case of 2 alter since we can't abort an in-progress operation)
class AsyncAlterTable : public AsyncTabletRequestTask {
 public:
  AsyncAlterTable(Master *master,
                  const string& permanent_uuid,
                  const scoped_refptr<TabletInfo>& tablet)
    : AsyncTabletRequestTask(master, permanent_uuid, tablet->table()),
      tablet_(tablet) {
  }

  virtual string type_name() const OVERRIDE { return "Alter Table"; }

  virtual string description() const OVERRIDE {
    return tablet_->ToString() + " Alter Table RPC for TS=" + permanent_uuid_;
  }

 private:
  virtual string tablet_id() const OVERRIDE { return tablet_->tablet_id(); }

  virtual void HandleResponse(int attempt) OVERRIDE {
    if (resp_.has_error()) {
      Status status = StatusFromPB(resp_.error().status());

      // Do not retry on a fatal error
      switch (resp_.error().code()) {
        case TabletServerErrorPB::TABLET_NOT_FOUND:
        case TabletServerErrorPB::MISMATCHED_SCHEMA:
        case TabletServerErrorPB::TABLET_HAS_A_NEWER_SCHEMA:
          LOG(WARNING) << "TS " << permanent_uuid_ << ": alter failed for tablet "
                       << tablet_->ToString() << " no further retry: " << status.ToString();
          MarkComplete();
          break;
        default:
          LOG(WARNING) << "TS " << permanent_uuid_ << ": alter failed for tablet "
                       << tablet_->ToString() << ": " << status.ToString();
          break;
      }
    } else {
      MarkComplete();
      VLOG(1) << "TS " << permanent_uuid_ << ": alter complete on tablet " << tablet_->ToString();
    }

    if (state() == kStateComplete) {
      master_->catalog_manager()->HandleTabletSchemaVersionReport(tablet_.get(), schema_version_);
    } else {
      VLOG(1) << "Still waiting for other tablets to finish ALTER";
    }
  }

  virtual void SendRequest(int attempt) OVERRIDE {
    TableMetadataLock l(tablet_->table(), TableMetadataLock::READ);

    tserver::AlterSchemaRequestPB req;
    req.set_tablet_id(tablet_->tablet_id());
    req.set_new_table_name(l.data().pb.name());
    req.set_schema_version(l.data().pb.version());
    req.mutable_schema()->CopyFrom(l.data().pb.schema());
    schema_version_ = l.data().pb.version();

    l.Unlock();

    ts_proxy_->AlterSchemaAsync(req, &resp_, &rpc_,
                                boost::bind(&AsyncAlterTable::RpcCallback, this));
    VLOG(1) << "Send alter table request to " << permanent_uuid_
            << " (attempt " << attempt << "):\n"
            << req.DebugString();
  }

  uint32_t schema_version_;
  scoped_refptr<TabletInfo> tablet_;
  tserver::AlterSchemaResponsePB resp_;
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
  AsyncAlterTable *call = new AsyncAlterTable(master_, ts_desc->permanent_uuid(), tablet);
  tablet->table()->AddTask(call);
  WARN_NOT_OK(call->Run(), "Failed to send alter table request");
}

void CatalogManager::SendDeleteTabletRequestsForTable(const scoped_refptr<TableInfo>& table) {
  vector<scoped_refptr<TabletInfo> > tablets;
  table->GetAllTablets(&tablets);

  BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
    std::vector<TabletReplica> locations;
    tablet->GetLocations(&locations);
    BOOST_FOREACH(const TabletReplica& replica, locations) {
      SendDeleteTabletRequest(tablet->tablet_id(), table, replica.ts_desc,
                              "Table being deleted");
    }
  }
}

void CatalogManager::SendDeleteTabletRequest(const std::string& tablet_id,
                                             const scoped_refptr<TableInfo>& table,
                                             TSDescriptor* ts_desc,
                                             const string& reason) {
  AsyncDeleteTablet *call = new AsyncDeleteTablet(master_, ts_desc->permanent_uuid(),
                                                  table, tablet_id, reason);
  if (table != NULL) {
    table->AddTask(call);
  } else {
    // This is a floating task (since the table does not exist)
    // created as response to a tablet report.
    call->AddRef();
  }
  WARN_NOT_OK(call->Run(), "Failed to send delete tablet request");
}

void CatalogManager::ExtractTabletsToProcess(
    std::vector<scoped_refptr<TabletInfo> > *tablets_to_delete,
    std::vector<scoped_refptr<TabletInfo> > *tablets_to_process) {

  boost::shared_lock<LockType> l(lock_);

  // TODO: At the moment we loop through all the tablets
  //       we can keep a set of tablets waiting for "assignment"
  //       or just a counter to avoid to take the lock and loop through the tablets
  //       if everything is "stable".

  BOOST_FOREACH(const TabletInfoMap::value_type& entry, tablet_map_) {
    scoped_refptr<TabletInfo> tablet = entry.second;
    TabletMetadataLock tablet_lock(tablet.get(), TabletMetadataLock::READ);
    TableMetadataLock table_lock(tablet->table(), TableMetadataLock::READ);

    // If the table is deleted or the tablet was replaced.
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
  vector<TabletInfo*> tablets_to_add;
  vector<TabletInfo*> tablets_to_update;
  vector<TabletInfo*> needs_create_rpc;
};

void CatalogManager::HandleAssignPreparingTablet(TabletInfo* tablet,
                                                 DeferredAssignmentActions* deferred) {
  // The tablet was just created (probably by a CreateTable RPC).
  // Update the state to "creating" to be ready for the creation request.
  tablet->mutable_metadata()->mutable_dirty()->set_state(
    SysTabletsEntryPB::kTabletStateCreating, "Sending initial creation of tablet");
  deferred->tablets_to_update.push_back(tablet);
  deferred->needs_create_rpc.push_back(tablet);
  VLOG(1) << "Assign new tablet " << tablet->ToString();
}

void CatalogManager::HandleAssignCreatingTablet(TabletInfo* tablet,
                                                DeferredAssignmentActions* deferred,
                                                vector<scoped_refptr<TabletInfo> >* new_tablets) {
  MonoTime now = MonoTime::Now(MonoTime::FINE);
  int64_t remaining_timeout_ms = FLAGS_assignment_timeout_ms -
    tablet->TimeSinceLastUpdate(now).ToMilliseconds();

  // Skip the tablet if the assignment timeout is not yet expired
  if (remaining_timeout_ms > 0) {
    VLOG(2) << "Tablet " << tablet->ToString() << " still being created. "
            << remaining_timeout_ms << "ms remain until timeout.";
    return;
  }

  const PersistentTabletInfo& old_info = tablet->metadata().state();

  // The "tablet creation" was already sent, but we didn't receive an answer
  // within the timeout. So the tablet will be replaced by a new one.
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

  // Mark old tablet as replaced.
  tablet->mutable_metadata()->mutable_dirty()->set_state(
    SysTabletsEntryPB::kTabletStateReplaced,
    Substitute("Replaced by $0 at ts=$1",
               replacement->tablet_id(), GetCurrentTimeMicros()));

  // Mark new tablet as being created.
  replacement->mutable_metadata()->mutable_dirty()->set_state(
    SysTabletsEntryPB::kTabletStateCreating,
    Substitute("Replacement for $0", tablet->tablet_id()));

  deferred->tablets_to_update.push_back(tablet);
  deferred->tablets_to_add.push_back(replacement);
  deferred->needs_create_rpc.push_back(replacement);
  VLOG(1) << "Replaced tablet " << tablet->tablet_id()
          << " with " << replacement->tablet_id()
          << " (Table " << tablet->table()->ToString() << ")";

  new_tablets->push_back(replacement);
}

// TODO: we could batch the IO onto a background thread.
//       but this is following the current HandleReportedTablet()
void CatalogManager::HandleTabletSchemaVersionReport(TabletInfo *tablet, uint32_t version) {
  // Update the schema version if it's the latest
  tablet->set_reported_schema_version(version);

  // Verify if it's the last tablet report, and the alter completed.
  TableInfo *table = tablet->table();
  TableMetadataLock l(table, TableMetadataLock::WRITE);
  if (l.data().is_deleted() || l.data().pb.state() != SysTablesEntryPB::kTableStateAltering) {
    return;
  }

  uint32_t current_version = l.data().pb.version();
  if (table->IsAlterInProgress(current_version)) {
    return;
  }

  // Update the state from altering to running and remove the last fully
  // applied schema (if it exists).
  l.mutable_data()->pb.clear_fully_applied_schema();
  l.mutable_data()->set_state(SysTablesEntryPB::kTableStateRunning,
                              Substitute("Current schema version=$0", current_version));

  Status s = sys_catalog_->UpdateTable(table);
  if (!s.ok()) {
    // panic-mode: abort the master
    LOG(FATAL) << "An error occurred while updating sys-tables: " << s.ToString();
  }

  l.Commit();
  LOG(INFO) << table->ToString() << " - Alter table completed version=" << current_version;
}

// Helper class to commit TabletInfo mutations at the end of a scope.
namespace {

class ScopedTabletInfoCommitter {
 public:
  explicit ScopedTabletInfoCommitter(const std::vector<scoped_refptr<TabletInfo> >* tablets)
    : tablets_(DCHECK_NOTNULL(tablets)) {
  }

  // Commit the transactions.
  ~ScopedTabletInfoCommitter() {
    BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, *tablets_) {
      tablet->mutable_metadata()->CommitMutation();
    }
  }

 private:
  const std::vector<scoped_refptr<TabletInfo> >* tablets_;
};
} // anonymous namespace

void CatalogManager::ProcessPendingAssignments(
    const std::vector<scoped_refptr<TabletInfo> >& tablets) {
  VLOG(1) << "Processing pending assignments";

  // Take write locks on all tablets to be processed, and ensure that they are
  // unlocked at the end of this scope.
  BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
    tablet->mutable_metadata()->StartMutation();
  }
  ScopedTabletInfoCommitter unlocker_in(&tablets);

  // Any tablets created by the helper functions will also be created in a
  // locked state, so we must ensure they are unlocked before we return to
  // avoid deadlocks.
  std::vector<scoped_refptr<TabletInfo> > new_tablets;
  ScopedTabletInfoCommitter unlocker_out(&new_tablets);

  DeferredAssignmentActions deferred;

  // Iterate over each of the tablets and handle it, whatever state
  // it may be in. The actions required for the tablet are collected
  // into 'deferred'.
  BOOST_FOREACH(const scoped_refptr<TabletInfo>& tablet, tablets) {
    SysTabletsEntryPB::State t_state = tablet->metadata().state().pb.state();

    switch (t_state) {
      case SysTabletsEntryPB::kTabletStatePreparing:
        HandleAssignPreparingTablet(tablet.get(), &deferred);
        break;

      case SysTabletsEntryPB::kTabletStateCreating:
        HandleAssignCreatingTablet(tablet.get(), &deferred, &new_tablets);
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

  BOOST_FOREACH(TabletInfo *tablet, deferred.needs_create_rpc) {
    SelectReplicasForTablet(ts_descs, tablet);
  }

  // Update the sys catalog with the new set of tablets/metadata.
  Status s = sys_catalog_->AddAndUpdateTablets(
    deferred.tablets_to_add, deferred.tablets_to_update);
  if (!s.ok()) {
    // panic-mode: abort the master
    LOG(FATAL) << "An error occurred while updating sys-tablets: " << s.ToString();
  }

  // Send the CreateTablet() requests to the servers. This is asynchronous / non-blocking.
  SendCreateTabletRequests(deferred.needs_create_rpc);
}

void CatalogManager::SelectReplicasForTablet(const TSDescriptorVector& ts_descs,
                                             TabletInfo* tablet) {
  TableMetadataLock table_guard(tablet->table(), TableMetadataLock::READ);

  int nreplicas = table_guard.data().pb.num_replicas();

  if (ts_descs.size() < nreplicas) {
    LOG(WARNING) << "Not enough Tablet Servers are online for table '" << table_guard.data().name()
                  << "'. Need at least " << nreplicas
                  << " servers, but only " << ts_descs.size() << " are available";
    return;
  }

  // Select the set of replicas
  metadata::QuorumPB *quorum = tablet->mutable_metadata()->mutable_dirty()->pb.mutable_quorum();
  quorum->set_seqno(-1);
  // TODO allow the user to choose num replicas per table and
  // and allow to choose local/dist quorum. See: KUDU-96
  quorum->set_local(nreplicas == 1);
  SelectReplicas(ts_descs, nreplicas, quorum);
}

void CatalogManager::SendCreateTabletRequests(const vector<TabletInfo*>& tablets) {
  BOOST_FOREACH(TabletInfo *tablet, tablets) {
    const metadata::QuorumPB& quorum = tablet->metadata().dirty().pb.quorum();
    tablet->set_last_update_ts(MonoTime::Now(MonoTime::FINE));
    BOOST_FOREACH(const QuorumPeerPB& peer, quorum.peers()) {
      AsyncCreateTablet *task = new AsyncCreateTablet(master_, peer.permanent_uuid(), tablet);
      tablet->table()->AddTask(task);
      WARN_NOT_OK(task->Run(), "Failed to send new tablet request");
    }
  }
}

void CatalogManager::SelectReplicas(const TSDescriptorVector& ts_descs,
                                    int nreplicas,
                                    metadata::QuorumPB *quorum) {
  // TODO: Select N Replicas
  // at the moment we have to scan all the tablets to build a map TS -> tablets
  // to know how many tablets a TS has... so, let's do a dumb assignment for now.
  //
  // Using a static variable here ensures that we round-robin our assignments.
  // TODO: In the future we should do something smarter based on number of tablets currently
  // running on each server, since round-robin may get unbalanced after moves/deletes.

  DCHECK_EQ(0, quorum->peers_size()) << "Quorum not empty: " << quorum->ShortDebugString();

  static int index = rand();
  for (int i = 0; i < nreplicas; ++i) {
    const TSDescriptor *ts = ts_descs[index++ % ts_descs.size()].get();

    TSRegistrationPB reg;
    ts->GetRegistration(&reg);

    QuorumPeerPB *peer = quorum->add_peers();
    peer->set_role(QuorumPeerPB::FOLLOWER);
    peer->set_permanent_uuid(ts->permanent_uuid());

    // TODO: This is temporary, we will use only UUIDs
    BOOST_FOREACH(const HostPortPB& addr, reg.rpc_addresses()) {
      peer->mutable_last_known_addr()->CopyFrom(addr);
    }
  }

  // TODO: Select the leader
  // Super hack for clusters where ts_descs.size() is a multiple of nreplicas, gives
  // us perfect distribution for the demo.
  QuorumPeerPB *leader = quorum->mutable_peers((index / ts_descs.size()) % nreplicas);
  // The master hints the node it wants as leader by making it start as a candidate.
  // This makes the quorum skip leader election (the initial candidate has implicit votes)
  // but the candidate still needs to successfully complete a config change to effectively
  // become leader.
  // TODO reconsider this when we have leader election. Todd suggests that we might
  // want to run leader election all the time.
  leader->set_role(QuorumPeerPB::CANDIDATE);
}

bool CatalogManager::BuildLocationsForTablet(const scoped_refptr<TabletInfo>& tablet,
                                             TabletLocationsPB* locs_pb) {
  metadata::QuorumPB stale_quorum;
  TSRegistrationPB reg;

  vector<TabletReplica> locs;
  {
    TabletMetadataLock l_tablet(tablet.get(), TabletMetadataLock::READ);
    if (!l_tablet.data().is_running()) return false;

    locs.clear();
    tablet->GetLocations(&locs);
    if (locs.empty() && l_tablet.data().pb.has_quorum()) {
      stale_quorum.CopyFrom(l_tablet.data().pb.quorum());
    }

    locs_pb->set_start_key(tablet->metadata().state().pb.start_key());
    locs_pb->set_end_key(tablet->metadata().state().pb.end_key());
  }

  locs_pb->set_tablet_id(tablet->tablet_id());
  locs_pb->set_stale(locs.empty());

  BOOST_FOREACH(const TabletReplica& replica, locs) {
    TabletLocationsPB_ReplicaPB* replica_pb = locs_pb->add_replicas();
    replica_pb->set_role(replica.role);

    TSInfoPB* tsinfo_pb = replica_pb->mutable_ts_info();
    tsinfo_pb->set_permanent_uuid(replica.ts_desc->permanent_uuid());

    replica.ts_desc->GetRegistration(&reg);
    tsinfo_pb->mutable_rpc_addresses()->Swap(reg.mutable_rpc_addresses());
  }

  BOOST_FOREACH(const metadata::QuorumPeerPB& peer, stale_quorum.peers()) {
    TabletLocationsPB_ReplicaPB* replica_pb = locs_pb->add_replicas();
    replica_pb->set_role(peer.role());

    TSInfoPB* tsinfo_pb = replica_pb->mutable_ts_info();
    tsinfo_pb->set_permanent_uuid(peer.permanent_uuid());
    tsinfo_pb->add_rpc_addresses()->CopyFrom(peer.last_known_addr());
  }

  return true;
}

bool CatalogManager::GetTabletLocations(const std::string& tablet_id,
                                        TabletLocationsPB* locs_pb) {
  locs_pb->mutable_replicas()->Clear();
  scoped_refptr<TabletInfo> tablet_info;
  {
    boost::shared_lock<LockType> l(lock_);
    if (!FindCopy(tablet_map_, tablet_id, &tablet_info)) {
      return false;
    }
  }

  return BuildLocationsForTablet(tablet_info, locs_pb);
}

Status CatalogManager::GetTableLocations(const GetTableLocationsRequestPB* req,
                                         GetTableLocationsResponsePB* resp) {
  RETURN_NOT_OK(CheckOnline());

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
  return Substitute("$0 (table $1)", tablet_id_,
                    (table_ != NULL ? table_->ToString() : "MISSING"));
}

void TabletInfo::ResetReplicas(const std::vector<TabletReplica>& replicas) {
  boost::lock_guard<simple_spinlock> l(lock_);

  last_update_ts_ = MonoTime::Now(MonoTime::FINE);
  locations_.assign(replicas.begin(), replicas.end());
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
      VLOG(3) << "Table " << table_id_ << " ALTER in progress due to tablet "
              << e.second->ToString() << " because reported schema "
              << e.second->reported_schema_version() << " < expected " << version;
      return true;
    }
  }
  return false;
}

bool TableInfo::IsCreateInProgress() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const TableInfo::TabletInfoMap::value_type& e, tablet_map_) {
    TabletMetadataLock tablet_lock(e.second, TabletMetadataLock::READ);
    if (!e.second->metadata().state().is_running()) {
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

void TableInfo::WaitTasksCompletion() {
  int wait_time = 250;
  while (1) {
    {
      boost::lock_guard<simple_spinlock> l(lock_);
      if (pending_tasks_.empty()) {
        break;
      }
    }
    usleep(wait_time);
    wait_time = std::min(wait_time * 5 / 4, 1000000);
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
