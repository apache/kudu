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
//   acquire the lock on the table first, and acquire the locks on the tablets
//   in tablet ID order, or let ScopedTabletInfoCommitter do the locking. This
//   strict ordering prevents deadlocks. Along the same lines, COMMIT must
//   happen in reverse (i.e. the tablet lock must be committed before the table
//   lock). The only exceptions to this are when there's only one thread in
//   operation, such as during master failover.

#include "kudu/master/catalog_manager.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cfile/type_encodings.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/move.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/gutil/utf/utf.h"
#include "kudu/gutil/walltime.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master_cert_authority.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/server/monitored_task.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/transactions/transaction_tracker.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

DEFINE_int32(master_ts_rpc_timeout_ms, 30 * 1000, // 30 sec
             "Timeout used for the master->TS async rpc calls.");
TAG_FLAG(master_ts_rpc_timeout_ms, advanced);

DEFINE_int32(tablet_creation_timeout_ms, 30 * 1000, // 30 sec
             "Timeout used by the master when attempting to create tablet "
             "replicas during table creation.");
TAG_FLAG(tablet_creation_timeout_ms, advanced);

DEFINE_bool(catalog_manager_wait_for_new_tablets_to_elect_leader, true,
            "Whether the catalog manager should wait for a newly created tablet to "
            "elect a leader before considering it successfully created. "
            "This is disabled in some tests where we explicitly manage leader "
            "election.");
TAG_FLAG(catalog_manager_wait_for_new_tablets_to_elect_leader, hidden);

DEFINE_int32(unresponsive_ts_rpc_timeout_ms, 60 * 60 * 1000, // 1 hour
             "After this amount of time, the master will stop attempting to contact "
             "a tablet server in order to perform operations such as deleting a tablet.");
TAG_FLAG(unresponsive_ts_rpc_timeout_ms, advanced);

DEFINE_int32(default_num_replicas, 3,
             "Default number of replicas for tables that do not have the num_replicas set.");
TAG_FLAG(default_num_replicas, advanced);

DEFINE_int32(max_num_replicas, 7,
             "Maximum number of replicas that may be specified for a table.");
// Tag as unsafe since we have done very limited testing of higher than 5 replicas.
TAG_FLAG(max_num_replicas, unsafe);

DEFINE_int32(max_num_columns, 300,
             "Maximum number of columns that may be in a table.");
// Tag as unsafe since we have done very limited testing of higher than 300 columns.
TAG_FLAG(max_num_columns, unsafe);


DEFINE_int32(max_identifier_length, 256,
             "Maximum length of the name of a column or table.");
// Tag as unsafe because we end up writing schemas in every WAL entry, etc,
// and having very long column names would enter untested territory and affect
// performance.
TAG_FLAG(max_identifier_length, unsafe);


DEFINE_bool(allow_unsafe_replication_factor, false,
            "Allow creating tables with even replication factor.");
TAG_FLAG(allow_unsafe_replication_factor, unsafe);

DEFINE_int32(catalog_manager_bg_task_wait_ms, 1000,
             "Amount of time the catalog manager background task thread waits "
             "between runs");
TAG_FLAG(catalog_manager_bg_task_wait_ms, hidden);

DEFINE_int32(max_create_tablets_per_ts, 20,
             "The number of tablets per TS that can be requested for a new table.");
TAG_FLAG(max_create_tablets_per_ts, advanced);

DEFINE_int32(master_failover_catchup_timeout_ms, 30 * 1000, // 30 sec
             "Amount of time to give a newly-elected leader master to load"
             " the previous master's metadata and become active. If this time"
             " is exceeded, the node crashes.");
TAG_FLAG(master_failover_catchup_timeout_ms, advanced);
TAG_FLAG(master_failover_catchup_timeout_ms, experimental);

DEFINE_bool(master_tombstone_evicted_tablet_replicas, true,
            "Whether the master should tombstone (delete) tablet replicas that "
            "are no longer part of the latest reported raft config.");
TAG_FLAG(master_tombstone_evicted_tablet_replicas, hidden);

DEFINE_bool(master_add_server_when_underreplicated, true,
            "Whether the master should attempt to add a new server to a tablet "
            "config when it detects that the tablet is under-replicated.");
TAG_FLAG(master_add_server_when_underreplicated, hidden);

DEFINE_bool(catalog_manager_check_ts_count_for_create_table, true,
            "Whether the master should ensure that there are enough live tablet "
            "servers to satisfy the provided replication count before allowing "
            "a table to be created.");
TAG_FLAG(catalog_manager_check_ts_count_for_create_table, hidden);

DEFINE_int32(table_locations_ttl_ms, 5 * 60 * 1000, // 5 minutes
             "Maximum time in milliseconds which clients may cache table locations. "
             "New range partitions may not be visible to existing client instances "
             "until after waiting for the ttl period.");
TAG_FLAG(table_locations_ttl_ms, advanced);

DEFINE_bool(catalog_manager_fail_ts_rpcs, false,
            "Whether all master->TS async calls should fail. Only for testing!");
TAG_FLAG(catalog_manager_fail_ts_rpcs, hidden);
TAG_FLAG(catalog_manager_fail_ts_rpcs, runtime);

DEFINE_bool(catalog_manager_delete_orphaned_tablets, false,
            "Whether the master should delete tablets reported by tablet "
            "servers for which there are no corresponding records in the "
            "master's metadata. Use this option with care; it may cause "
            "permanent tablet data loss under specific (and rare) cases of "
            "master failures!");
TAG_FLAG(catalog_manager_delete_orphaned_tablets, advanced);

DEFINE_int32(catalog_manager_inject_latency_prior_tsk_write_ms, 0,
             "Injects a random sleep between 0 and this many milliseconds "
             "prior to writing newly generated TSK into the system table. "
             "This is a test-only flag, do not use in production.");
TAG_FLAG(catalog_manager_inject_latency_prior_tsk_write_ms, hidden);
TAG_FLAG(catalog_manager_inject_latency_prior_tsk_write_ms, unsafe);

using std::map;
using std::pair;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;

namespace kudu {
namespace master {

using base::subtle::NoBarrier_CompareAndSwap;
using base::subtle::NoBarrier_Load;
using cfile::TypeEncodingInfo;
using consensus::ConsensusServiceProxy;
using consensus::ConsensusStatePB;
using consensus::GetConsensusRole;
using consensus::IsRaftConfigMember;
using consensus::RaftConsensus;
using consensus::RaftPeerPB;
using consensus::StartTabletCopyRequestPB;
using consensus::kMinimumTerm;
using pb_util::SecureDebugString;
using pb_util::SecureShortDebugString;
using rpc::RpcContext;
using security::Cert;
using security::DataFormat;
using security::PrivateKey;
using security::TokenSigner;
using security::TokenSigningPrivateKey;
using security::TokenSigningPrivateKeyPB;
using strings::Substitute;
using tablet::TABLET_DATA_DELETED;
using tablet::TABLET_DATA_TOMBSTONED;
using tablet::TabletDataState;
using tablet::TabletReplica;
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

    // Set up the table info.
    TableInfo *table = new TableInfo(table_id);
    TableMetadataLock l(table, TableMetadataLock::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);

    // Add the tablet to the IDs map and to the name map (if the table is not deleted).
    catalog_manager_->table_ids_map_[table->id()] = table;
    if (!l.data().is_deleted()) {
      catalog_manager_->table_names_map_[l.data().name()] = table;
    }
    l.Commit();

    LOG(INFO) << "Loaded metadata for table " << table->ToString();
    VLOG(1) << "Metadata for table " << table->ToString()
            << ": " << SecureShortDebugString(metadata);
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
    // Lookup the table.
    scoped_refptr<TableInfo> table(FindPtrOrNull(
        catalog_manager_->table_ids_map_, table_id));
    if (table == nullptr) {
      // Tables and tablets are always created/deleted in one operation, so
      // this shouldn't be possible.
      LOG(ERROR) << "Missing Table " << table_id << " required by tablet " << tablet_id;
      LOG(ERROR) << "Metadata: " << SecureDebugString(metadata);
      return Status::Corruption("Missing table for tablet: ", tablet_id);
    }

    // Set up the tablet info.
    TabletInfo* tablet = new TabletInfo(table, tablet_id);
    TabletMetadataLock l(tablet, TabletMetadataLock::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);

    // Add the tablet to the tablet manager.
    catalog_manager_->tablet_map_[tablet->tablet_id()] = tablet;

    // Add the tablet to the Tablet.
    bool is_deleted = l.mutable_data()->is_deleted();
    l.Commit();
    if (!is_deleted) {
      table->AddTablet(tablet);
    }

    LOG(INFO) << "Loaded metadata for tablet " << tablet_id
              << " (table " << table->ToString() << ")";
    VLOG(2) << "Metadata for tablet " << tablet_id << ": " << SecureShortDebugString(metadata);
    return Status::OK();
  }

 private:
  CatalogManager *catalog_manager_;

  DISALLOW_COPY_AND_ASSIGN(TabletLoader);
};

////////////////////////////////////////////////////////////
// TSK (Token Signing Key) Entry Loader
////////////////////////////////////////////////////////////

class TskEntryLoader : public TskEntryVisitor {
 public:
  TskEntryLoader()
      : entry_expiration_seconds_(WallTime_Now()) {
  }

  Status Visit(const string& entry_id,
               const SysTskEntryPB& metadata) override {
    TokenSigningPrivateKeyPB tsk(metadata.tsk());
    CHECK(tsk.has_key_seq_num());
    CHECK(tsk.has_expire_unix_epoch_seconds());
    CHECK(tsk.has_rsa_key_der());

    if (tsk.expire_unix_epoch_seconds() <= entry_expiration_seconds_) {
      expired_entry_ids_.insert(entry_id);
    }

    // Expired entries are useful as well: they are needed for correct tracking
    // of TSK sequence numbers.
    entries_.emplace_back(std::move(tsk));
    return Status::OK();
  }

  const vector<TokenSigningPrivateKeyPB>& entries() const {
    return entries_;
  }

  const set<string>& expired_entry_ids() const {
    return expired_entry_ids_;
  }

 private:
  const int64_t entry_expiration_seconds_;
  vector<TokenSigningPrivateKeyPB> entries_;
  set<string> expired_entry_ids_;

  DISALLOW_COPY_AND_ASSIGN(TskEntryLoader);
};

////////////////////////////////////////////////////////////
// Background Tasks
////////////////////////////////////////////////////////////

class CatalogManagerBgTasks {
 public:
  explicit CatalogManagerBgTasks(CatalogManager *catalog_manager)
    : closing_(false),
      pending_updates_(false),
      cond_(&lock_),
      thread_(nullptr),
      catalog_manager_(catalog_manager) {
  }

  ~CatalogManagerBgTasks() {}

  Status Init();
  void Shutdown();

  void Wake() {
    MutexLock lock(lock_);
    pending_updates_ = true;
    cond_.Broadcast();
  }

  void Wait(int msec) {
    MutexLock lock(lock_);
    if (closing_) return;
    if (!pending_updates_) {
      cond_.TimedWait(MonoDelta::FromMilliseconds(msec));
    }
    pending_updates_ = false;
  }

  void WakeIfHasPendingUpdates() {
    MutexLock lock(lock_);
    if (pending_updates_) {
      cond_.Broadcast();
    }
  }

 private:
  void Run();

 private:
  Atomic32 closing_;
  bool pending_updates_;
  mutable Mutex lock_;
  ConditionVariable cond_;
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
  if (thread_ != nullptr) {
    CHECK_OK(ThreadJoiner(thread_.get()).Join());
  }
}

void CatalogManagerBgTasks::Run() {
  while (!NoBarrier_Load(&closing_)) {
    {
      CatalogManager::ScopedLeaderSharedLock l(catalog_manager_);
      if (!l.catalog_status().ok()) {
        if (l.catalog_status().IsServiceUnavailable()) {
          LOG(INFO) << "Waiting for catalog manager background task thread to start: "
                    << l.catalog_status().ToString();
        } else {
          LOG(WARNING) << "Catalog manager background task thread going to sleep: "
                       << l.catalog_status().ToString();
        }
      } else if (l.leader_status().ok()) {
        // Get list of tablets not yet running.
        std::vector<scoped_refptr<TabletInfo>> to_process;
        catalog_manager_->ExtractTabletsToProcess(&to_process);

        if (!to_process.empty()) {
          // Transition tablet assignment state from preparing to creating, send
          // and schedule creation / deletion RPC messages, etc.
          Status s = catalog_manager_->ProcessPendingAssignments(to_process);
          if (!s.ok()) {
            // If there is an error (e.g., we are not the leader) abort this task
            // and wait until we're woken up again.
            //
            // TODO(unknown): Add tests for this in the revision that makes
            // create/alter fault tolerant.
            LOG(ERROR) << "Error processing pending assignments: " << s.ToString();
          }
        }

        // If this is the leader master, check if it's time to generate
        // and store a new TSK (Token Signing Key).
        Status s = catalog_manager_->TryGenerateNewTskUnlocked();
        if (!s.ok()) {
          const TokenSigner* signer = catalog_manager_->master_->token_signer();
          const string err_msg = "failed to refresh TSK: " + s.ToString() + ": ";
          if (l.has_term_changed()) {
            LOG(INFO) << err_msg
                      << "ignoring the error since not the leader anymore";
          } else if (signer->IsCurrentKeyValid()) {
            LOG(WARNING) << err_msg << "will try again next cycle";
          } else {
            // The TokenSigner ended up with no valid key to use. If the catalog
            // manager is still the leader, it would not be able to create valid
            // authn token signatures. It's not clear how to properly resolve
            // this situation and keep the process running. To avoid possible
            // inconsistency, let's crash the process.
            //
            // NOTE: This can only happen in a multi-master Kudu cluster. In
            //       that case, after this particular master crashes, another
            //       master will take over as leader.
            LOG(FATAL) << err_msg;
          }
        }
      }
    }
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

// Tracks, or aborts commits TabletInfo mutations.
//
// Can be used in one of two ways:
// 1. To track already-locked TabletInfos and commit them on end of scope:
//      {
//        ScopedTabletInfoCommitter c(ScopedTabletInfoCommitter::LOCKED);
//        c.addTablets({ one, two, three });
//        <Perform mutations>
//      } // Mutations are committed
//
// 2. To aggregate unlocked TabletInfos, lock them safely, and commit them on end of scope:
//      {
//        ScopedTabletInfoCommitter c(ScopedTabletInfoCommitter::UNLOCKED);
//        c.addTablets({ five, two, three });
//        c.addTablets({ four, one });
//        c.LockTabletsForWriting();
//        <Perform mutations>
//      } // Mutations are committed
//
// The acquisition or release of multiple tablet locks is done in tablet ID
// order, as required by the locking rules (see the top of the file).
class ScopedTabletInfoCommitter {
 private:
  // Compares TabletInfos using their underlying tablet IDs.
  struct TabletInfoCompare {
    bool operator() (const scoped_refptr<TabletInfo>& left,
                     const scoped_refptr<TabletInfo>& right) const {
      return left->tablet_id() < right->tablet_id();
    }
  };

  // Must be defined before begin()/end() below.
  typedef set<scoped_refptr<TabletInfo>, TabletInfoCompare> TabletSet;

 public:
  // Whether tablets added to this committer have been locked already or
  // should be locked by the committer itself.
  enum State {
    LOCKED,
    UNLOCKED,
  };

  explicit ScopedTabletInfoCommitter(State state)
    : state_(state),
      aborted_(false) {
  }

  // Acquire write locks for all of the tablets previously added.
  void LockTabletsForWriting() {
    DCHECK_EQ(UNLOCKED, state_);
    for (const auto& t : tablets_) {
      t->mutable_metadata()->StartMutation();
    }
    state_ = LOCKED;
  }

  // Release all write locks, discarding any mutated tablet data.
  void Abort() {
    DCHECK(!aborted_);
    if (state_ == LOCKED) {
      for (const auto & t : tablets_) {
        t->mutable_metadata()->AbortMutation();
      }
    }
    aborted_ = true;
  }

  ~ScopedTabletInfoCommitter() {
    Commit();
  }

  // Release all write locks, committing any mutated tablet data.
  void Commit() {
    if (PREDICT_TRUE(!aborted_ && state_ == LOCKED)) {
      for (const auto& t : tablets_) {
        t->mutable_metadata()->CommitMutation();
      }
      state_ = UNLOCKED;
    }
  }

  // Add new tablets to be tracked.
  void AddTablets(const vector<scoped_refptr<TabletInfo>>& new_tablets) {
    DCHECK(!aborted_);
    tablets_.insert(new_tablets.begin(), new_tablets.end());
  }

  // These methods allow the class to be used in range-based for loops.
  const TabletSet::iterator begin() const {
    return tablets_.begin();
  }
  const TabletSet::iterator end() const {
    return tablets_.end();
  }

 private:
  TabletSet tablets_;
  State state_;
  bool aborted_;
};

string RequestorString(RpcContext* rpc) {
  if (rpc) {
    return rpc->requestor_string();
  } else {
    return "internal request";
  }
}

// If 's' indicates that the node is no longer the leader, setup
// Service::UnavailableError as the error, set NOT_THE_LEADER as the
// error code and return true.
template<class RespClass>
void CheckIfNoLongerLeaderAndSetupError(Status s, RespClass* resp) {
  // TODO (KUDU-591): This is a bit of a hack, as right now
  // there's no way to propagate why a write to a consensus configuration has
  // failed. However, since we use Status::IllegalState()/IsAborted() to
  // indicate the situation where a write was issued on a node
  // that is no longer the leader, this suffices until we
  // distinguish this cause of write failure more explicitly.
  if (s.IsIllegalState() || s.IsAborted()) {
    Status new_status = Status::ServiceUnavailable(
        "operation requested can only be executed on a leader master, but this"
        " master is no longer the leader", s.ToString());
    SetupError(resp->mutable_error(), MasterErrorPB::NOT_THE_LEADER, new_status);
  }
}

template<class RespClass>
Status CheckIfTableDeletedOrNotRunning(TableMetadataLock* lock, RespClass* resp) {
  if (lock->data().is_deleted()) {
    Status s = Status::NotFound("The table was deleted", lock->data().pb.state_msg());
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }
  if (!lock->data().is_running()) {
    Status s = Status::ServiceUnavailable("The table is not running");
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }
  return Status::OK();
}

// Propagate the 'read_default' to the 'write_default' in 'col',
// and check that the client didn't specify an invalid combination of the two fields.
Status ProcessColumnPBDefaults(ColumnSchemaPB* col) {
  if (col->has_read_default_value() && !col->has_write_default_value()) {
    // We expect clients to send just the 'read_default_value' field.
    col->set_write_default_value(col->read_default_value());
  } else if (col->has_read_default_value() && col->has_write_default_value()) {
    // C++ client 1.0 and earlier sends the default in both PB fields.
    // Check that the defaults match (we never provided an API that would
    // let them be set to different values)
    if (col->read_default_value() != col->write_default_value()) {
      return Status::InvalidArgument(Substitute(
          "column '$0' has mismatched read/write defaults", col->name()));
    }
  } else if (!col->has_read_default_value() && col->has_write_default_value()) {
    // We don't expect any client to send us this, but better cover our
    // bases.
    return Status::InvalidArgument(Substitute(
        "column '$0' has write_default field set but no read_default", col->name()));
  }
  return Status::OK();
}

} // anonymous namespace

CatalogManager::CatalogManager(Master *master)
  : master_(master),
    rng_(GetRandomSeed32()),
    state_(kConstructed),
    leader_ready_term_(-1),
    leader_lock_(RWMutex::Priority::PREFER_WRITING) {
  CHECK_OK(ThreadPoolBuilder("leader-initialization")
           // Presently, this thread pool must contain only a single thread
           // (to correctly serialize invocations of ElectedAsLeaderCb upon
           // closely timed consecutive elections).
           .set_max_threads(1)
           .Build(&leader_election_pool_));
}

CatalogManager::~CatalogManager() {
  Shutdown();
}

Status CatalogManager::Init(bool is_first_run) {
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
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

  std::lock_guard<LockType> l(lock_);
  background_tasks_.reset(new CatalogManagerBgTasks(this));
  RETURN_NOT_OK_PREPEND(background_tasks_->Init(),
                        "Failed to initialize catalog manager background tasks");

  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    CHECK_EQ(kStarting, state_);
    state_ = kRunning;
  }

  return Status::OK();
}

Status CatalogManager::ElectedAsLeaderCb() {
  return leader_election_pool_->SubmitClosure(
      Bind(&CatalogManager::PrepareForLeadershipTask, Unretained(this)));
}

Status CatalogManager::WaitUntilCaughtUpAsLeader(const MonoDelta& timeout) {
  ConsensusStatePB cstate = sys_catalog_->tablet_replica()->consensus()->ConsensusState();
  const string& uuid = master_->fs_manager()->uuid();
  if (!cstate.has_leader_uuid() || cstate.leader_uuid() != uuid) {
    return Status::IllegalState(
        Substitute("Node $0 not leader. Raft Consensus state: $1",
                    uuid, SecureShortDebugString(cstate)));
  }

  // Wait for all transactions to be committed.
  RETURN_NOT_OK(sys_catalog_->tablet_replica()->transaction_tracker()->WaitForAllToFinish(timeout));
  return Status::OK();
}

Status CatalogManager::InitCertAuthority() {
  leader_lock_.AssertAcquiredForWriting();

  unique_ptr<PrivateKey> key;
  unique_ptr<Cert> cert;
  const Status s = LoadCertAuthorityInfo(&key, &cert);
  if (s.ok()) {
    return InitCertAuthorityWith(std::move(key), std::move(cert));
  }
  if (s.IsNotFound()) {
    // Status::NotFound is returned if no IPKI certificate authority record is
    // found in the system catalog table. It can happen on the very first run
    // of a secured Kudu cluster. If so, it's necessary to create and persist
    // a new CA record which, if persisted, will be used for this and next runs.
    //
    // The subtlety here is that first it's necessary to store the newly
    // generated IPKI CA information (the private key and the certificate) into
    // the system table and only after that initialize the master certificate
    // authority. This protects against a leadership change between the
    // generation and the usage of the newly generated IPKI CA information
    // by the master.
    //
    // An example of such 'leadership change in the middle' scenario:
    //
    // 1. The catalog manager starts generating Kudu  IPKI CA private key and
    //    corresponding certificate. This takes some time since generating
    //    a cryptographically strong private key requires many CPU cycles.
    //
    // 2. While the catalog manager is busy with generating the CA info, a new
    //    election happens in the background and the catalog manager loses its
    //    leadership role.
    //
    // 3. The catalog manager tries to write the newly generated information
    //    into the system table. There are two possible cases at the time when
    //    applying the write operation:
    //
    //      a. The catalog manager is not the system tablet's leader.
    //
    //      b. The catalog manager is the system tablet's leader.
    //         It regained its leadership role by the time the write operation
    //         is applied. That can happen if another election occurs before
    //         the write operation is applied.
    //
    // 4. Essentially, the following responses are possible for the write
    //    operation, enumerated in accordance with 3.{a,b} items above:
    //
    //      a. A failure happens and corresponding error message is logged;
    //         the failure is ignored.
    //
    //      b. In the case when the catalog manager becomes the leader again,
    //         there are two possible outcomes for the write operation:
    //
    //           i.  Success. The master completes the initialization process
    //               and proceeds to serve client requests.
    //
    //           ii. Failure. This is when the former in-the-middle leader has
    //               succeeded in writing its CA info into the system table.
    //               That could happen if the former in-the-middle leader was
    //               very fast because there were plenty of CPU resources
    //               available for CA info generation. Since the CA info record
    //               has pre-defined identifier, it's impossible to have more
    //               than one CA info record in the system table. This is due to
    //               the {record_id, record_type} uniqueness constraint.
    //
    // In case of the write operation's success (4.b.i), it's safe to proceed
    // with loading the persisted CA information into the CertAuthority run-time
    // object.
    //
    // In case of the write operation's failure (4.a, 4.b.ii), the generated
    // CA information is no longer relevant and can be safely discarded. The
    // crucial point is to not initialize the CertAuthority with non-persisted
    // information. Otherwise that information could get into the run-time
    // structures of some system components, cutting them off from communicating
    // with the rest of the system which uses the genuine CA information.
    //
    // Once the CA information is persisted in the system table, a catalog
    // manager reads and loads it into the CertAuthority every time it becomes
    // an elected leader.
    unique_ptr<PrivateKey> key(new PrivateKey);
    unique_ptr<Cert> cert(new Cert);

    // Generate new private key and corresponding CA certificate.
    RETURN_NOT_OK(master_->cert_authority()->Generate(key.get(), cert.get()));
    // If the leadership was lost, writing into the system table fails.
    RETURN_NOT_OK(StoreCertAuthorityInfo(*key, *cert));
    // Once the CA information is persisted, it's necessary to initialize
    // the certificate authority sub-component with it. The leader master
    // should not run without a CA certificate.
    return InitCertAuthorityWith(std::move(key), std::move(cert));
  }

  return s;
}

// Initialize the master's certificate authority component with the specified
// private key and certificate.
Status CatalogManager::InitCertAuthorityWith(
    unique_ptr<PrivateKey> key, unique_ptr<Cert> cert) {
  leader_lock_.AssertAcquiredForWriting();
  auto* ca = master_->cert_authority();
  RETURN_NOT_OK_PREPEND(ca->Init(std::move(key), std::move(cert)),
                        "could not init master CA");

  auto* tls = master_->mutable_tls_context();
  RETURN_NOT_OK_PREPEND(tls->AddTrustedCertificate(ca->ca_cert()),
                        "could not trust master CA cert");
  // If we haven't signed our own server cert yet, do so.
  boost::optional<security::CertSignRequest> csr =
      tls->GetCsrIfNecessary();
  if (csr) {
    Cert cert;
    RETURN_NOT_OK_PREPEND(ca->SignServerCSR(*csr, &cert),
                          "couldn't sign master cert with CA cert");
    RETURN_NOT_OK_PREPEND(tls->AdoptSignedCert(cert),
                          "couldn't adopt signed master cert");
  }
  return Status::OK();
}

Status CatalogManager::LoadCertAuthorityInfo(unique_ptr<PrivateKey>* key,
                                             unique_ptr<Cert>* cert) {
  leader_lock_.AssertAcquiredForWriting();

  SysCertAuthorityEntryPB info;
  RETURN_NOT_OK(sys_catalog_->GetCertAuthorityEntry(&info));

  unique_ptr<PrivateKey> ca_private_key(new PrivateKey);
  unique_ptr<Cert> ca_cert(new Cert);
  RETURN_NOT_OK(ca_private_key->FromString(
      info.private_key(), DataFormat::DER));
  RETURN_NOT_OK(ca_cert->FromString(
      info.certificate(), DataFormat::DER));
  // Extra sanity check.
  RETURN_NOT_OK(ca_cert->CheckKeyMatch(*ca_private_key));

  key->swap(ca_private_key);
  cert->swap(ca_cert);

  return Status::OK();
}

// Store internal Kudu CA cert authority information into the system table.
Status CatalogManager::StoreCertAuthorityInfo(const PrivateKey& key,
                                              const Cert& cert) {
  leader_lock_.AssertAcquiredForWriting();

  SysCertAuthorityEntryPB info;
  RETURN_NOT_OK(key.ToString(info.mutable_private_key(), DataFormat::DER));
  RETURN_NOT_OK(cert.ToString(info.mutable_certificate(), DataFormat::DER));
  RETURN_NOT_OK(sys_catalog_->AddCertAuthorityEntry(info));
  LOG(INFO) << "Generated new certificate authority record";

  return Status::OK();
}

Status CatalogManager::InitTokenSigner() {
  leader_lock_.AssertAcquiredForWriting();

  set<string> expired_tsk_entry_ids;
  RETURN_NOT_OK(LoadTskEntries(&expired_tsk_entry_ids));
  RETURN_NOT_OK(TryGenerateNewTskUnlocked());
  return DeleteTskEntries(expired_tsk_entry_ids);
}

void CatalogManager::PrepareForLeadershipTask() {
  {
    // Hack to block this function until InitSysCatalogAsync() is finished.
    shared_lock<LockType> l(lock_);
  }
  const RaftConsensus* consensus = sys_catalog_->tablet_replica()->consensus();
  const int64_t term_before_wait = consensus->ConsensusState().current_term();
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    if (leader_ready_term_ == term_before_wait) {
      // The term hasn't changed since the last time this master was the
      // leader. It's not possible for another master to be leader for the same
      // term, so there hasn't been any actual leadership change and thus
      // there's no reason to reload the on-disk metadata.
      VLOG(2) << Substitute("Term $0 hasn't changed, ignoring dirty callback",
                            term_before_wait);
      return;
    }
  }
  Status s = WaitUntilCaughtUpAsLeader(
      MonoDelta::FromMilliseconds(FLAGS_master_failover_catchup_timeout_ms));
  if (!s.ok()) {
    WARN_NOT_OK(s, "Failed waiting for node to catch up after master election");
    // TODO: Abdicate on timeout instead of crashing.
    if (s.IsTimedOut()) {
      LOG(FATAL) << "Shutting down due to unavailability of other masters after"
                 << " election. TODO: Abdicate instead.";
    }
    return;
  }

  const int64_t term = consensus->ConsensusState().current_term();
  if (term_before_wait != term) {
    // If we got elected leader again while waiting to catch up then we will
    // get another callback to visit the tables and tablets, so bail.
    LOG(INFO) << "Term changed from " << term_before_wait << " to " << term
        << " while waiting for master leader catchup. Not loading sys catalog metadata";
    return;
  }

  {
    // This lambda returns the result of calling the 'func', checking whether
    // the error, if any, is fatal for the leader catalog. If the returned
    // status is non-OK, the caller should bail on the leadership preparation
    // task. If the error is considered fatal, LOG(FATAL) is called.
    const auto check = [this](
        std::function<Status()> func,
        const RaftConsensus& consensus,
        int64_t start_term,
        const char* op_description) {

      leader_lock_.AssertAcquiredForWriting();
      const Status s = func();
      if (s.ok()) {
        // Not an error at all.
        return s;
      }

      {
        std::lock_guard<simple_spinlock> l(state_lock_);
        if (state_ == kClosing) {
          // Errors on shutdown are not considered fatal.
          LOG(INFO) << op_description
                    << " failed due to the shutdown of the catalog: "
                    << s.ToString();
          return s;
        }
      }

      const int64_t term = consensus.ConsensusState().current_term();
      if (term != start_term) {
        // If the term has changed we assume the new leader catalog is about
        // to do the necessary work in its leadership preparation task.
        LOG(INFO) << op_description << " failed; "
                  << Substitute("change in term detected: $0 vs $1: ",
                                start_term, term)
                  << s.ToString();
        return s;
      }

      // In all other cases non-OK status is considered fatal.
      LOG(FATAL) << op_description << " failed: " << s.ToString();
      return s; // unreachable
    };

    // Block new catalog operations, and wait for existing operations to finish.
    std::lock_guard<RWMutex> leader_lock_guard(leader_lock_);

    static const char* const kLoadMetaOpDescription =
        "Loading table and tablet metadata into memory";
    LOG(INFO) << kLoadMetaOpDescription << "...";
    LOG_SLOW_EXECUTION(WARNING, 1000, LogPrefix() + kLoadMetaOpDescription) {
      if (!check(std::bind(&CatalogManager::VisitTablesAndTabletsUnlocked, this),
                 *consensus, term, kLoadMetaOpDescription).ok()) {
        return;
      }
    }

    // TODO(KUDU-1920): update this once "BYO PKI" feature is supported.
    static const char* const kCaInitOpDescription =
        "Initializing Kudu internal certificate authority";
    LOG(INFO) << kCaInitOpDescription << "...";
    LOG_SLOW_EXECUTION(WARNING, 1000, LogPrefix() + kCaInitOpDescription) {
      if (!check(std::bind(&CatalogManager::InitCertAuthority, this),
                 *consensus, term, kCaInitOpDescription).ok()) {
        return;
      }
    }

    static const char* const kTskOpDescription = "Loading token signing keys";
    LOG(INFO) << kTskOpDescription << "...";
    LOG_SLOW_EXECUTION(WARNING, 1000, LogPrefix() + kTskOpDescription) {
      if (!check(std::bind(&CatalogManager::InitTokenSigner, this),
                 *consensus, term, kTskOpDescription).ok()) {
        return;
      }
    }
  }

  std::lock_guard<simple_spinlock> l(state_lock_);
  leader_ready_term_ = term;
}

Status CatalogManager::VisitTablesAndTabletsUnlocked() {
  leader_lock_.AssertAcquiredForWriting();

  // This lock is held for the entirety of the function because the calls to
  // VisitTables and VisitTablets mutate global maps.
  std::lock_guard<LockType> lock(lock_);

  // Abort any outstanding tasks. All TableInfos are orphaned below, so
  // it's important to end their tasks now; otherwise Shutdown() will
  // destroy master state used by these tasks.
  vector<scoped_refptr<TableInfo>> tables;
  AppendValuesFromMap(table_ids_map_, &tables);
  AbortAndWaitForAllTasks(tables);

  // Clear the existing state.
  table_names_map_.clear();
  table_ids_map_.clear();
  tablet_map_.clear();

  // Visit tables and tablets, load them into memory.
  TableLoader table_loader(this);
  RETURN_NOT_OK_PREPEND(sys_catalog_->VisitTables(&table_loader),
                        "Failed while visiting tables in sys catalog");
  TabletLoader tablet_loader(this);
  RETURN_NOT_OK_PREPEND(sys_catalog_->VisitTablets(&tablet_loader),
                        "Failed while visiting tablets in sys catalog");
  return Status::OK();
}

// This method is called by tests only.
Status CatalogManager::VisitTablesAndTablets() {
  // Block new catalog operations, and wait for existing operations to finish.
  std::lock_guard<RWMutex> leader_lock_guard(leader_lock_);
  return VisitTablesAndTabletsUnlocked();
}

Status CatalogManager::InitSysCatalogAsync(bool is_first_run) {
  std::lock_guard<LockType> l(lock_);
  unique_ptr<SysCatalogTable> new_catalog(
      new SysCatalogTable(master_,
                          Bind(&CatalogManager::ElectedAsLeaderCb,
                               Unretained(this))));
  if (is_first_run) {
    RETURN_NOT_OK(new_catalog->CreateNew(master_->fs_manager()));
  } else {
    RETURN_NOT_OK(new_catalog->Load(master_->fs_manager()));
  }
  sys_catalog_.reset(new_catalog.release());
  return Status::OK();
}

bool CatalogManager::IsInitialized() const {
  std::lock_guard<simple_spinlock> l(state_lock_);
  return state_ == kRunning;
}

RaftPeerPB::Role CatalogManager::Role() const {
  shared_ptr<consensus::RaftConsensus> consensus;
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
    if (state_ == kRunning) {
      consensus = sys_catalog_->tablet_replica()->shared_consensus();
    }
  }
  return consensus ? consensus->role() : RaftPeerPB::UNKNOWN_ROLE;
}

void CatalogManager::Shutdown() {
  {
    std::lock_guard<simple_spinlock> l(state_lock_);
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

  // Mark all outstanding table tasks as aborted and wait for them to fail.
  //
  // There may be an outstanding table visitor thread modifying the table map,
  // so we must make a copy of it before we iterate. It's OK if the visitor
  // adds more entries to the map even after we finish; it won't start any new
  // tasks for those entries.
  vector<scoped_refptr<TableInfo>> copy;
  {
    shared_lock<LockType> l(lock_);
    AppendValuesFromMap(table_ids_map_, &copy);
  }
  AbortAndWaitForAllTasks(copy);

  // Shutdown the underlying consensus implementation. This aborts all pending
  // operations on the system table. In case of a multi-master Kudu cluster,
  // a deadlock might happen if the consensus implementation were active during
  // further phases: shutting down the leader election pool and the system
  // catalog.
  //
  // The mechanics behind the deadlock are as follows:
  //   * The majority of the system table's peers goes down (e.g. all non-leader
  //     masters shut down).
  //   * The ElectedAsLeaderCb task issues an operation to the system
  //     table (e.g. write newly generated TSK).
  //   * The code below calls Shutdown() on the leader election pool. That
  //     call does not return because the underlying Raft indefinitely
  //     retries to get the response for the submitted operations.
  if (sys_catalog_) {
    sys_catalog_->tablet_replica()->consensus()->Shutdown();
  }

  // Wait for any outstanding ElectedAsLeaderCb tasks to finish.
  //
  // Must be done before shutting down the catalog, otherwise its TabletReplica
  // may be destroyed while still in use by the ElectedAsLeaderCb task.
  leader_election_pool_->Shutdown();

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

namespace {

// Validate a table or column name to ensure that it is a valid identifier.
Status ValidateIdentifier(const string& id) {
  if (id.empty()) {
    return Status::InvalidArgument("empty string not a valid identifier");
  }

  if (id.length() > FLAGS_max_identifier_length) {
    return Status::InvalidArgument(Substitute(
        "identifier '$0' longer than maximum permitted length $1",
        id, FLAGS_max_identifier_length));
  }

  // Identifiers should be valid UTF8.
  const char* p = id.data();
  int rem = id.size();
  while (rem > 0) {
    Rune rune = Runeerror;
    int rune_len = charntorune(&rune, p, rem);
    if (rune == Runeerror) {
      return Status::InvalidArgument("invalid UTF8 sequence");
    }
    if (rune == 0) {
      return Status::InvalidArgument("identifier must not contain null bytes");
    }
    rem -= rune_len;
    p += rune_len;
  }

  return Status::OK();
}

// Validate the client-provided schema and name.
Status ValidateClientSchema(const boost::optional<string>& name,
                            const Schema& schema) {
  if (name != boost::none) {
    RETURN_NOT_OK_PREPEND(ValidateIdentifier(name.get()), "invalid table name");
  }
  for (int i = 0; i < schema.num_columns(); i++) {
    RETURN_NOT_OK_PREPEND(ValidateIdentifier(schema.column(i).name()),
                          "invalid column name");
  }
  if (schema.num_key_columns() <= 0) {
    return Status::InvalidArgument("must specify at least one key column");
  }
  if (schema.num_columns() > FLAGS_max_num_columns) {
    return Status::InvalidArgument(Substitute(
        "number of columns $0 is greater than the permitted maximum $1",
        schema.num_columns(), FLAGS_max_num_columns));
  }
  for (int i = 0; i < schema.num_key_columns(); i++) {
    if (!IsTypeAllowableInKey(schema.column(i).type_info())) {
      return Status::InvalidArgument(
          "key column may not have type of BOOL, FLOAT, or DOUBLE");
    }
  }

  // Check that the encodings are valid for the specified types.
  for (int i = 0; i < schema.num_columns(); i++) {
    const auto& col = schema.column(i);
    const TypeEncodingInfo *dummy;
    Status s = TypeEncodingInfo::Get(col.type_info(),
                                     col.attributes().encoding,
                                     &dummy);
    if (!s.ok()) {
      return s.CloneAndPrepend(
          Substitute("invalid encoding for column '$0'", col.name()));
    }
  }
  return Status::OK();
}

} // anonymous namespace

// Create a new table.
// See README file in this directory for a description of the design.
Status CatalogManager::CreateTable(const CreateTableRequestPB* orig_req,
                                   CreateTableResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  auto SetError = [&](MasterErrorPB::Code code, const Status& s) {
    SetupError(resp->mutable_error(), code, s);
    return s;
  };

  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());
  Status s;

  // Copy the request, so we can fill in some defaults.
  CreateTableRequestPB req = *orig_req;
  LOG(INFO) << "CreateTable from " << RequestorString(rpc)
            << ":\n" << SecureDebugString(req);

  // Do some fix-up of any defaults specified on columns.
  // Clients are only expected to pass the default value in the 'read_default'
  // field, but we need to write the schema to disk including the default
  // as both the 'read' and 'write' default. It's easier to do this fix-up
  // on the protobuf here.
  for (int i = 0; i < req.schema().columns_size(); i++) {
    auto* col = req.mutable_schema()->mutable_columns(i);
    Status s = ProcessColumnPBDefaults(col);
    if (!s.ok()) {
      return SetError(MasterErrorPB::INVALID_SCHEMA, s);
    }
  }

  // a. Validate the user request.
  Schema client_schema;
  RETURN_NOT_OK(SchemaFromPB(req.schema(), &client_schema));
  s = ValidateClientSchema(req.name(), client_schema);
  if (s.ok() && client_schema.has_column_ids()) {
    s = Status::InvalidArgument("User requests should not have Column IDs");
  }
  if (!s.ok()) {
    return SetError(MasterErrorPB::INVALID_SCHEMA, s);
  }
  Schema schema = client_schema.CopyWithColumnIds();

  // If the client did not set a partition schema in the create table request,
  // the default partition schema (no hash bucket components and a range
  // partitioned on the primary key columns) will be used.
  PartitionSchema partition_schema;
  s = PartitionSchema::FromPB(req.partition_schema(), schema, &partition_schema);
  if (!s.ok()) {
    return SetError(MasterErrorPB::INVALID_SCHEMA, s);
  }

  // Decode split rows.
  vector<KuduPartialRow> split_rows;
  vector<pair<KuduPartialRow, KuduPartialRow>> range_bounds;

  RowOperationsPBDecoder decoder(req.mutable_split_rows_range_bounds(),
                                 &client_schema, &schema, nullptr);
  vector<DecodedRowOperation> ops;
  RETURN_NOT_OK(decoder.DecodeOperations(&ops));

  for (int i = 0; i < ops.size(); i++) {
    const DecodedRowOperation& op = ops[i];
    switch (op.type) {
      case RowOperationsPB::SPLIT_ROW: {
        split_rows.push_back(*op.split_row);
        break;
      }
      case RowOperationsPB::RANGE_LOWER_BOUND:
      case RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND: {
        i += 1;
        if (i >= ops.size() ||
            (ops[i].type != RowOperationsPB::RANGE_UPPER_BOUND &&
             ops[i].type != RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND)) {
          return SetError(MasterErrorPB::UNKNOWN_ERROR,
                          Status::InvalidArgument(
                              "Missing upper range bound in create table request"));
        }

        if (op.type == RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND) {
          RETURN_NOT_OK(partition_schema.MakeLowerBoundRangePartitionKeyInclusive(
                op.split_row.get()));
        }
        if (ops[i].type == RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND) {
          RETURN_NOT_OK(partition_schema.MakeUpperBoundRangePartitionKeyExclusive(
                ops[i].split_row.get()));
        }

        range_bounds.emplace_back(*op.split_row, *ops[i].split_row);
        break;
      }
      default: return Status::InvalidArgument(
                   Substitute("Illegal row operation type in create table request: $0", op.type));
    }
  }

  // Create partitions based on specified partition schema and split rows.
  vector<Partition> partitions;
  RETURN_NOT_OK(partition_schema.CreatePartitions(split_rows, range_bounds, schema, &partitions));

  // If they didn't specify a num_replicas, set it based on the default.
  if (!req.has_num_replicas()) {
    req.set_num_replicas(FLAGS_default_num_replicas);
  }

  // Reject create table with even replication factors, unless master flag
  // allow_unsafe_replication_factor is on.
  if (req.num_replicas() % 2 == 0 && !FLAGS_allow_unsafe_replication_factor) {
    s = Status::InvalidArgument(Substitute("illegal replication factor $0 (replication "
                                           "factor must be odd)", req.num_replicas()));
    return SetError(MasterErrorPB::EVEN_REPLICATION_FACTOR, s);
  }

  if (req.num_replicas() > FLAGS_max_num_replicas) {
    s = Status::InvalidArgument(Substitute("illegal replication factor $0 (max replication "
                                           "factor is $1)",
                                           req.num_replicas(),
                                           FLAGS_max_num_replicas));
    return SetError(MasterErrorPB::REPLICATION_FACTOR_TOO_HIGH, s);

  }
  if (req.num_replicas() <= 0) {
    s = Status::InvalidArgument(Substitute("illegal replication factor $0 (replication factor "
                                           "must be positive)",
                                           req.num_replicas(),
                                           FLAGS_max_num_replicas));
    return SetError(MasterErrorPB::ILLEGAL_REPLICATION_FACTOR, s);
  }

  // Verify that the total number of tablets is reasonable, relative to the number
  // of live tablet servers.
  TSDescriptorVector ts_descs;
  master_->ts_manager()->GetAllLiveDescriptors(&ts_descs);
  int num_live_tservers = ts_descs.size();
  int max_tablets = FLAGS_max_create_tablets_per_ts * num_live_tservers;
  if (req.num_replicas() > 1 && max_tablets > 0 && partitions.size() > max_tablets) {
    s = Status::InvalidArgument(Substitute("The requested number of tablets is over the "
                                           "maximum permitted at creation time ($0). Additional "
                                           "tablets may be added by adding range partitions to the "
                                           "table post-creation.", max_tablets));
    return SetError(MasterErrorPB::TOO_MANY_TABLETS, s);
  }

  // Verify that the number of replicas isn't larger than the number of live tablet
  // servers.
  if (FLAGS_catalog_manager_check_ts_count_for_create_table &&
      req.num_replicas() > num_live_tservers) {
    s = Status::InvalidArgument(Substitute(
        "Not enough live tablet servers to create a table with the requested replication "
        "factor $0. $1 tablet servers are alive.", req.num_replicas(), num_live_tservers));
    return SetError(MasterErrorPB::REPLICATION_FACTOR_TOO_HIGH, s);
  }

  scoped_refptr<TableInfo> table;
  {
    std::lock_guard<LockType> l(lock_);
    TRACE("Acquired catalog manager lock");

    // b. Verify that the table does not exist.
    table = FindPtrOrNull(table_names_map_, req.name());
    if (table != nullptr) {
      s = Status::AlreadyPresent(Substitute("Table $0 already exists with id $1",
                                 req.name(), table->id()));
      return SetError(MasterErrorPB::TABLE_ALREADY_PRESENT, s);
    }

    // c. Reserve the table name if possible.
    if (!InsertIfNotPresent(&reserved_table_names_, req.name())) {
      s = Status::ServiceUnavailable(Substitute(
          "New table name $0 is already reserved", req.name()));
      return SetError(MasterErrorPB::TABLE_NOT_FOUND, s);
    }
  }

  // Ensure that we drop the name reservation upon return.
  auto cleanup = MakeScopedCleanup([&] () {
    std::lock_guard<LockType> l(lock_);
    CHECK_EQ(1, reserved_table_names_.erase(req.name()));
  });

  // d. Create the in-memory representation of the new table and its tablets.
  //    It's not yet in any global maps; that will happen in step g below.
  table = CreateTableInfo(req, schema, partition_schema);
  vector<TabletInfo*> tablets;
  vector<scoped_refptr<TabletInfo>> tablet_refs;
  for (const Partition& partition : partitions) {
    PartitionPB partition_pb;
    partition.ToPB(&partition_pb);
    scoped_refptr<TabletInfo> t = CreateTabletInfo(table.get(), partition_pb);
    tablets.push_back(t.get());
    tablet_refs.emplace_back(std::move(t));
  }
  TRACE("Created new table and tablet info");

  // NOTE: the table and tablets are already locked for write at this point,
  // since the CreateTableInfo/CreateTabletInfo functions leave them in that state.
  // They will get committed at the end of this function.
  // Sanity check: the tables and tablets should all be in "preparing" state.
  CHECK_EQ(SysTablesEntryPB::PREPARING, table->metadata().dirty().pb.state());
  for (const TabletInfo *tablet : tablets) {
    CHECK_EQ(SysTabletsEntryPB::PREPARING, tablet->metadata().dirty().pb.state());
  }
  table->mutable_metadata()->mutable_dirty()->pb.set_state(SysTablesEntryPB::RUNNING);

  // e. Write table and tablets to sys-catalog.
  SysCatalogTable::Actions actions;
  actions.table_to_add = table.get();
  actions.tablets_to_add = tablets;
  s = sys_catalog_->Write(actions);
  if (!s.ok()) {
    s = s.CloneAndPrepend(Substitute("An error occurred while writing to sys-catalog: $0",
                                     s.ToString()));
    LOG(WARNING) << s.ToString();
    CheckIfNoLongerLeaderAndSetupError(s, resp);
    return s;
  }
  TRACE("Wrote table and tablets to system table");

  // f. Commit the in-memory state.
  table->mutable_metadata()->CommitMutation();

  for (TabletInfo *tablet : tablets) {
    tablet->mutable_metadata()->CommitMutation();
  }
  table->AddTablets(tablets);

  // g. Make the new table and tablets visible in the catalog.
  {
    std::lock_guard<LockType> l(lock_);

    table_ids_map_[table->id()] = table;
    table_names_map_[req.name()] = table;
    for (const auto& tablet : tablet_refs) {
      InsertOrDie(&tablet_map_, tablet->tablet_id(), tablet);
    }
  }
  TRACE("Inserted table and tablets into CatalogManager maps");

  resp->set_table_id(table->id());
  VLOG(1) << "Created table " << table->ToString();
  background_tasks_->Wake();
  return Status::OK();
}

Status CatalogManager::IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                                         IsCreateTableDoneResponsePB* resp) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  scoped_refptr<TableInfo> table;

  // 1. Lookup the table and verify if it exists
  TRACE("Looking up table");
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == nullptr) {
    Status s = Status::NotFound("The table does not exist", SecureShortDebugString(req->table()));
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TRACE("Locking table");
  TableMetadataLock l(table.get(), TableMetadataLock::READ);
  RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(&l, resp));

  // 2. Verify if the create is in-progress
  TRACE("Verify if the table creation is in progress for $0", table->ToString());
  resp->set_done(!table->IsCreateInProgress());

  return Status::OK();
}

TableInfo *CatalogManager::CreateTableInfo(const CreateTableRequestPB& req,
                                           const Schema& schema,
                                           const PartitionSchema& partition_schema) {
  DCHECK(schema.has_column_ids());
  TableInfo* table = new TableInfo(GenerateId());
  table->mutable_metadata()->StartMutation();
  SysTablesEntryPB *metadata = &table->mutable_metadata()->mutable_dirty()->pb;
  metadata->set_state(SysTablesEntryPB::PREPARING);
  metadata->set_name(req.name());
  metadata->set_version(0);
  metadata->set_next_column_id(ColumnId(schema.max_col_id() + 1));
  metadata->set_num_replicas(req.num_replicas());
  // Use the Schema object passed in, since it has the column IDs already assigned,
  // whereas the user request PB does not.
  CHECK_OK(SchemaToPB(schema, metadata->mutable_schema()));
  partition_schema.ToPB(metadata->mutable_partition_schema());
  return table;
}

scoped_refptr<TabletInfo> CatalogManager::CreateTabletInfo(TableInfo* table,
                                                           const PartitionPB& partition) {
  scoped_refptr<TabletInfo> tablet(new TabletInfo(table, GenerateId()));
  tablet->mutable_metadata()->StartMutation();
  SysTabletsEntryPB *metadata = &tablet->mutable_metadata()->mutable_dirty()->pb;
  metadata->set_state(SysTabletsEntryPB::PREPARING);
  metadata->mutable_partition()->CopyFrom(partition);
  metadata->set_table_id(table->id());
  return tablet;
}

Status CatalogManager::FindTable(const TableIdentifierPB& table_identifier,
                                 scoped_refptr<TableInfo> *table_info) {
  shared_lock<LockType> l(lock_);

  if (table_identifier.has_table_id()) {
    *table_info = FindPtrOrNull(table_ids_map_, table_identifier.table_id());
  } else if (table_identifier.has_table_name()) {
    *table_info = FindPtrOrNull(table_names_map_, table_identifier.table_name());
  } else {
    return Status::InvalidArgument("Missing Table ID or Table Name");
  }
  return Status::OK();
}

Status CatalogManager::DeleteTable(const DeleteTableRequestPB* req,
                                   DeleteTableResponsePB* resp,
                                   rpc::RpcContext* rpc) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  LOG(INFO) << "Servicing DeleteTable request from " << RequestorString(rpc)
            << ": " << SecureShortDebugString(*req);

  // 1. Look up the table, lock it, and mark it as removed.
  TRACE("Looking up table");
  scoped_refptr<TableInfo> table;
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == nullptr) {
    Status s = Status::NotFound("The table does not exist", SecureShortDebugString(req->table()));
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

  TRACE("Modifying in-memory table state")
  string deletion_msg = "Table deleted at " + LocalTimeAsString();
  l.mutable_data()->set_state(SysTablesEntryPB::REMOVED, deletion_msg);

  // 2. Look up the tablets, lock them, and mark them as deleted.
  {
    ScopedTabletInfoCommitter committer(ScopedTabletInfoCommitter::UNLOCKED);
    TRACE("Locking tablets");
    vector<scoped_refptr<TabletInfo>> tablets;
    table->GetAllTablets(&tablets);
    committer.AddTablets(tablets);
    committer.LockTabletsForWriting();

    vector<TabletInfo*> tablets_raw;
    for (const auto& t : committer) {
      t->mutable_metadata()->mutable_dirty()->set_state(
          SysTabletsEntryPB::DELETED, deletion_msg);
      tablets_raw.push_back(t.get());
    }

    // 3. Update sys-catalog with the removed table and tablet state.
    TRACE("Removing table and tablets from system table");
    SysCatalogTable::Actions actions;
    actions.table_to_update = table.get();
    actions.tablets_to_update = tablets_raw;
    Status s = sys_catalog_->Write(actions);
    if (!s.ok()) {
      s = s.CloneAndPrepend(Substitute("An error occurred while updating sys tables: $0",
                                       s.ToString()));
      LOG(WARNING) << s.ToString();
      CheckIfNoLongerLeaderAndSetupError(s, resp);
      committer.Abort();
      return s;
    }

    // The operation has been written to sys-catalog; now it must succeed.

    // 4. Remove the table from the by-name map.
    {
      TRACE("Removing table from by-name map");
      std::lock_guard<LockType> l_map(lock_);
      if (table_names_map_.erase(l.data().name()) != 1) {
        PANIC_RPC(rpc, "Could not remove table from map, name=" + l.data().name());
      }
    }

    // 5. Commit the dirty tablet state (on end of scope).
  }

  // 6. Commit the dirty table state.
  TRACE("Committing in-memory state");
  l.Commit();

  // 7. Abort any extant tasks belonging to the table.
  TRACE("Aborting table tasks");
  table->AbortTasks();

  // 8. Send a DeleteTablet() request to each tablet replica in the table.
  SendDeleteTableRequest(table, deletion_msg);

  LOG(INFO) << "Successfully deleted table " << table->ToString()
            << " per request from " << RequestorString(rpc);
  return Status::OK();
}

Status CatalogManager::ApplyAlterSchemaSteps(const SysTablesEntryPB& current_pb,
                                             vector<AlterTableRequestPB::Step> steps,
                                             Schema* new_schema,
                                             ColumnId* next_col_id) {
  const SchemaPB& current_schema_pb = current_pb.schema();
  Schema cur_schema;
  RETURN_NOT_OK(SchemaFromPB(current_schema_pb, &cur_schema));

  SchemaBuilder builder(cur_schema);
  if (current_pb.has_next_column_id()) {
    builder.set_next_column_id(ColumnId(current_pb.next_column_id()));
  }

  for (const auto& step : steps) {
    switch (step.type()) {
      case AlterTableRequestPB::ADD_COLUMN: {
        if (!step.has_add_column()) {
          return Status::InvalidArgument("ADD_COLUMN missing column info");
        }

        ColumnSchemaPB new_col_pb = step.add_column().schema();
        if (new_col_pb.has_id()) {
          return Status::InvalidArgument("column $0: client should not specify column ID",
                                         SecureShortDebugString(new_col_pb));
        }
        RETURN_NOT_OK(ProcessColumnPBDefaults(&new_col_pb));

        // Can't accept a NOT NULL column without a default.
        ColumnSchema new_col = ColumnSchemaFromPB(new_col_pb);
        if (!new_col.is_nullable() && !new_col.has_read_default()) {
          return Status::InvalidArgument(
              Substitute("column `$0`: NOT NULL columns must have a default", new_col.name()));
        }

        RETURN_NOT_OK(builder.AddColumn(new_col, false));
        break;
      }

      case AlterTableRequestPB::DROP_COLUMN: {
        if (!step.has_drop_column()) {
          return Status::InvalidArgument("DROP_COLUMN missing column info");
        }

        if (cur_schema.is_key_column(step.drop_column().name())) {
          return Status::InvalidArgument("cannot remove a key column",
                                         step.drop_column().name());
        }

        RETURN_NOT_OK(builder.RemoveColumn(step.drop_column().name()));
        break;
      }
      // Remains for backwards compatibility.
      case AlterTableRequestPB::RENAME_COLUMN: {
        if (!step.has_rename_column()) {
          return Status::InvalidArgument("RENAME_COLUMN missing column info");
        }

        RETURN_NOT_OK(builder.RenameColumn(
                        step.rename_column().old_name(),
                        step.rename_column().new_name()));
        break;
      }
      case AlterTableRequestPB::ALTER_COLUMN: {
        if (!step.has_alter_column()) {
          return Status::InvalidArgument("ALTER_COLUMN missing column info");
        }
        const ColumnSchemaDelta col_delta = ColumnSchemaDeltaFromPB(step.alter_column().delta());
        RETURN_NOT_OK(builder.ApplyColumnSchemaDelta(col_delta));
        break;
      }
      default: {
        return Status::InvalidArgument("Invalid alter schema step type",
                                       SecureShortDebugString(step));
      }
    }
  }
  *new_schema = builder.Build();
  *next_col_id = builder.next_column_id();
  return Status::OK();
}

Status CatalogManager::ApplyAlterPartitioningSteps(
    const TableMetadataLock& l,
    TableInfo* table,
    const Schema& client_schema,
    vector<AlterTableRequestPB::Step> steps,
    vector<scoped_refptr<TabletInfo>>* tablets_to_add,
    vector<scoped_refptr<TabletInfo>>* tablets_to_drop) {

  Schema schema;
  RETURN_NOT_OK(SchemaFromPB(l.data().pb.schema(), &schema));
  PartitionSchema partition_schema;
  RETURN_NOT_OK(PartitionSchema::FromPB(l.data().pb.partition_schema(), schema, &partition_schema));

  map<string, TabletInfo*> existing_tablets = table->tablet_map();
  map<string, scoped_refptr<TabletInfo>> new_tablets;

  for (const auto& step : steps) {
    vector<DecodedRowOperation> ops;
    if (step.type() == AlterTableRequestPB::ADD_RANGE_PARTITION) {
      RowOperationsPBDecoder decoder(&step.add_range_partition().range_bounds(),
                                     &client_schema, &schema, nullptr);
      RETURN_NOT_OK(decoder.DecodeOperations(&ops));
    } else {
      CHECK_EQ(step.type(), AlterTableRequestPB::DROP_RANGE_PARTITION);
      RowOperationsPBDecoder decoder(&step.drop_range_partition().range_bounds(),
                                     &client_schema, &schema, nullptr);
      RETURN_NOT_OK(decoder.DecodeOperations(&ops));
    }

    if (ops.size() != 2) {
      return Status::InvalidArgument("expected two row operations for alter range partition step",
                                     SecureShortDebugString(step));
    }

    if ((ops[0].type != RowOperationsPB::RANGE_LOWER_BOUND &&
         ops[0].type != RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND) ||
        (ops[1].type != RowOperationsPB::RANGE_UPPER_BOUND &&
         ops[1].type != RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND)) {
      return Status::InvalidArgument(
          "expected a lower bound and upper bound row op for alter range partition step",
          strings::Substitute("$0, $1", ops[0].ToString(schema), ops[1].ToString(schema)));
    }

    if (ops[0].type == RowOperationsPB::EXCLUSIVE_RANGE_LOWER_BOUND) {
      RETURN_NOT_OK(partition_schema.MakeLowerBoundRangePartitionKeyInclusive(
            ops[0].split_row.get()));
    }
    if (ops[1].type == RowOperationsPB::INCLUSIVE_RANGE_UPPER_BOUND) {
      RETURN_NOT_OK(partition_schema.MakeUpperBoundRangePartitionKeyExclusive(
            ops[1].split_row.get()));
    }

    vector<Partition> partitions;
    RETURN_NOT_OK(partition_schema.CreatePartitions({}, {{ *ops[0].split_row, *ops[1].split_row }},
                                                    schema, &partitions));

    switch (step.type()) {
      case AlterTableRequestPB::ADD_RANGE_PARTITION: {
        for (const Partition& partition : partitions) {
          const string& lower_bound = partition.partition_key_start();
          const string& upper_bound = partition.partition_key_end();

          // Check that the new tablet doesn't overlap with the existing tablets.
          // Iter points at the tablet directly *after* the lower bound (or to
          // existing_tablets.end(), if such a tablet does not exist).
          auto existing_iter = existing_tablets.upper_bound(lower_bound);
          if (existing_iter != existing_tablets.end()) {
            TabletMetadataLock metadata(existing_iter->second, TabletMetadataLock::READ);
            if (upper_bound.empty() ||
                metadata.data().pb.partition().partition_key_start() < upper_bound) {
              return Status::InvalidArgument(
                  "New range partition conflicts with existing range partition",
                  partition_schema.RangePartitionDebugString(*ops[0].split_row, *ops[1].split_row));
            }
          }
          if (existing_iter != existing_tablets.begin()) {
            TabletMetadataLock metadata(std::prev(existing_iter)->second, TabletMetadataLock::READ);
            if (metadata.data().pb.partition().partition_key_end().empty() ||
                metadata.data().pb.partition().partition_key_end() > lower_bound) {
              return Status::InvalidArgument(
                  "New range partition conflicts with existing range partition",
                  partition_schema.RangePartitionDebugString(*ops[0].split_row, *ops[1].split_row));
            }
          }

          // Check that the new tablet doesn't overlap with any other new tablets.
          auto new_iter = new_tablets.upper_bound(lower_bound);
          if (new_iter != new_tablets.end()) {
            const auto& metadata = new_iter->second->mutable_metadata()->dirty();
            if (upper_bound.empty() ||
                metadata.pb.partition().partition_key_start() < upper_bound) {
              return Status::InvalidArgument(
                  "New range partition conflicts with another new range partition",
                  partition_schema.RangePartitionDebugString(*ops[0].split_row, *ops[1].split_row));
            }
          }
          if (new_iter != new_tablets.begin()) {
            const auto& metadata = std::prev(new_iter)->second->mutable_metadata()->dirty();
            if (metadata.pb.partition().partition_key_end().empty() ||
                metadata.pb.partition().partition_key_end() > lower_bound) {
              return Status::InvalidArgument(
                  "New range partition conflicts with another new range partition",
                  partition_schema.RangePartitionDebugString(*ops[0].split_row, *ops[1].split_row));
            }
          }

          PartitionPB partition_pb;
          partition.ToPB(&partition_pb);
          new_tablets.emplace(lower_bound, CreateTabletInfo(table, partition_pb));
        }
        break;
      }

      case AlterTableRequestPB::DROP_RANGE_PARTITION: {
        for (const Partition& partition : partitions) {
          const string& lower_bound = partition.partition_key_start();
          const string& upper_bound = partition.partition_key_end();

          // Iter points to the tablet if it exists, or the next tablet, or the end.
          auto existing_iter = existing_tablets.lower_bound(lower_bound);
          auto new_iter = new_tablets.lower_bound(lower_bound);

          bool found_existing = false;
          bool found_new = false;

          if (existing_iter != existing_tablets.end()) {
            TabletMetadataLock metadata(existing_iter->second, TabletMetadataLock::READ);
            const auto& partition = metadata.data().pb.partition();
            found_existing = partition.partition_key_start() == lower_bound &&
                             partition.partition_key_end() == upper_bound;
          }
          if (new_iter != new_tablets.end()) {
            const auto& partition = new_iter->second->mutable_metadata()->dirty().pb.partition();
            found_new = partition.partition_key_start() == lower_bound &&
                        partition.partition_key_end() == upper_bound;
          }

          DCHECK(!found_existing || !found_new);
          if (found_existing) {
            tablets_to_drop->emplace_back(existing_iter->second);
            existing_tablets.erase(existing_iter);
          } else if (found_new) {
            new_tablets.erase(new_iter);
          } else {
            return Status::InvalidArgument(
                "No range partition found for drop range partition step",
                partition_schema.RangePartitionDebugString(*ops[0].split_row, *ops[1].split_row));
          }
        }
        break;
      }
      default: {
        return Status::InvalidArgument("Unknown alter table range partitioning step",
                                       SecureShortDebugString(step));
      }
    }
  }

  for (auto& tablet : new_tablets) {
    tablets_to_add->emplace_back(std::move(tablet.second));
  }
  return Status::OK();
}

Status CatalogManager::AlterTable(const AlterTableRequestPB* req,
                                  AlterTableResponsePB* resp,
                                  rpc::RpcContext* rpc) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  LOG(INFO) << "Servicing AlterTable request from " << RequestorString(rpc)
            << ": " << SecureShortDebugString(*req);

  RETURN_NOT_OK(CheckOnline());

  // 1. Group the steps into schema altering steps and partition altering steps.
  vector<AlterTableRequestPB::Step> alter_schema_steps;
  vector<AlterTableRequestPB::Step> alter_partitioning_steps;
  for (const auto& step : req->alter_schema_steps()) {
    switch (step.type()) {
      case AlterTableRequestPB::ADD_COLUMN:
      case AlterTableRequestPB::DROP_COLUMN:
      case AlterTableRequestPB::RENAME_COLUMN:
      case AlterTableRequestPB::ALTER_COLUMN: {
        alter_schema_steps.emplace_back(step);
        break;
      }
      case AlterTableRequestPB::ADD_RANGE_PARTITION:
      case AlterTableRequestPB::DROP_RANGE_PARTITION: {
        alter_partitioning_steps.emplace_back(step);
        break;
      }
      case AlterTableRequestPB::UNKNOWN: {
        return Status::InvalidArgument("Invalid alter step type", SecureShortDebugString(step));
      }
    }
  }

  // 2. Lookup the table, verify if it exists, and lock it for modification.
  TRACE("Looking up table");
  scoped_refptr<TableInfo> table;
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == nullptr) {
    Status s = Status::NotFound("The table does not exist", SecureShortDebugString(req->table()));
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

  // 3. Having locked the table, look it up again, in case we raced with another
  //    AlterTable() that renamed our table.
  {
    scoped_refptr<TableInfo> table_again;
    CHECK_OK(FindTable(req->table(), &table_again));
    if (table_again == nullptr) {
      Status s = Status::NotFound("The table does not exist", SecureShortDebugString(req->table()));
      SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
      return s;
    }
  }

  string table_name = l.data().name();
  *resp->mutable_table_id() = table->id();

  // 4. Calculate and validate new schema for the on-disk state, not persisted yet.
  Schema new_schema;
  ColumnId next_col_id = ColumnId(l.data().pb.next_column_id());
  if (!alter_schema_steps.empty()) {
    TRACE("Apply alter schema");
    Status s = ApplyAlterSchemaSteps(l.data().pb, alter_schema_steps, &new_schema, &next_col_id);
    if (!s.ok()) {
      SetupError(resp->mutable_error(), MasterErrorPB::INVALID_SCHEMA, s);
      return s;
    }
    DCHECK_NE(next_col_id, 0);
    DCHECK_EQ(new_schema.find_column_by_id(next_col_id),
              static_cast<int>(Schema::kColumnNotFound));

    // Just validate the schema, not the name (validated below).
    s = ValidateClientSchema(boost::none, new_schema);
    if (!s.ok()) {
      SetupError(resp->mutable_error(), MasterErrorPB::INVALID_SCHEMA, s);
      return s;
    }
  }

  // 5. Validate and try to acquire the new table name.
  if (req->has_new_table_name()) {
    Status s = ValidateIdentifier(req->new_table_name());
    if (!s.ok()) {
      SetupError(resp->mutable_error(), MasterErrorPB::INVALID_SCHEMA,
                 s.CloneAndPrepend("invalid table name"));
      return s;
    }

    std::lock_guard<LockType> catalog_lock(lock_);
    TRACE("Acquired catalog manager lock");

    // Verify that the table does not exist.
    scoped_refptr<TableInfo> other_table = FindPtrOrNull(table_names_map_, req->new_table_name());
    if (other_table != nullptr) {
      Status s = Status::AlreadyPresent(Substitute("Table $0 already exists with id $1",
                                                   req->new_table_name(), table->id()));
      SetupError(resp->mutable_error(), MasterErrorPB::TABLE_ALREADY_PRESENT, s);
      return s;
    }

    // Reserve the new table name if possible.
    if (!InsertIfNotPresent(&reserved_table_names_, req->new_table_name())) {
      Status s = Status::ServiceUnavailable(Substitute(
          "Table name $0 is already reserved", req->new_table_name()));
      SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
      return s;
    }

    l.mutable_data()->pb.set_name(req->new_table_name());
  }

  // Ensure that we drop our reservation upon return.
  auto cleanup = MakeScopedCleanup([&] () {
    if (req->has_new_table_name()) {
      std::lock_guard<LockType> l(lock_);
      CHECK_EQ(1, reserved_table_names_.erase(req->new_table_name()));
    }
  });

  // 6. Alter table partitioning.
  vector<scoped_refptr<TabletInfo>> tablets_to_add;
  vector<scoped_refptr<TabletInfo>> tablets_to_drop;
  if (!alter_partitioning_steps.empty()) {
    TRACE("Apply alter partitioning");
    Schema client_schema;
    RETURN_NOT_OK(SchemaFromPB(req->schema(), &client_schema));
    Status s = ApplyAlterPartitioningSteps(l, table.get(), client_schema, alter_partitioning_steps,
                                           &tablets_to_add, &tablets_to_drop);
    if (!s.ok()) {
      SetupError(resp->mutable_error(), MasterErrorPB::UNKNOWN_ERROR, s);
      return s;
    }
  }

  // Set to true if columns are altered, added or dropped.
  bool has_schema_changes = !alter_schema_steps.empty();
  // Set to true if there are schema changes, or the table is renamed.
  bool has_metadata_changes = has_schema_changes || req->has_new_table_name();
  // Set to true if there are partitioning changes.
  bool has_partitioning_changes = !alter_partitioning_steps.empty();
  // Set to true if metadata changes need to be applied to existing tablets.
  bool has_metadata_changes_for_existing_tablets =
    has_metadata_changes && table->num_tablets() > tablets_to_drop.size();

  // Skip empty requests...
  if (!has_metadata_changes && !has_partitioning_changes) {
    return Status::OK();
  }

  // 7. Serialize the schema and increment the version number.
  if (has_metadata_changes_for_existing_tablets && !l.data().pb.has_fully_applied_schema()) {
    l.mutable_data()->pb.mutable_fully_applied_schema()->CopyFrom(l.data().pb.schema());
  }
  if (has_schema_changes) {
    CHECK_OK(SchemaToPB(new_schema, l.mutable_data()->pb.mutable_schema()));
  }
  if (has_metadata_changes) {
    l.mutable_data()->pb.set_version(l.mutable_data()->pb.version() + 1);
    l.mutable_data()->pb.set_next_column_id(next_col_id);
  }
  if (!tablets_to_add.empty() || has_metadata_changes_for_existing_tablets) {
    // If some tablet schemas need to be updated or there are any new tablets,
    // set the table state to ALTERING, so that IsAlterTableDone RPCs will wait
    // for the schema updates and tablets to be running.
    l.mutable_data()->set_state(SysTablesEntryPB::ALTERING,
                                Substitute("Alter Table version=$0 ts=$1",
                                           l.mutable_data()->pb.version(),
                                           LocalTimeAsString()));
  }

  // 8. Update sys-catalog with the new table schema and tablets to add/drop.
  TRACE("Updating metadata on disk");
  string deletion_msg = "Partition dropped at " + LocalTimeAsString();
  SysCatalogTable::Actions actions;
  if (!tablets_to_add.empty() || has_metadata_changes) {
    // If anything modified the table's persistent metadata, then sync it to the sys catalog.
    actions.table_to_update = table.get();
  }
  for (const auto& tablet : tablets_to_add) {
    actions.tablets_to_add.push_back(tablet.get());
  }

  ScopedTabletInfoCommitter tablets_to_add_committer(ScopedTabletInfoCommitter::LOCKED);
  ScopedTabletInfoCommitter tablets_to_drop_committer(ScopedTabletInfoCommitter::UNLOCKED);
  tablets_to_add_committer.AddTablets(tablets_to_add);
  tablets_to_drop_committer.AddTablets(tablets_to_drop);
  tablets_to_drop_committer.LockTabletsForWriting();
  for (auto& tablet : tablets_to_drop) {
    tablet->mutable_metadata()->mutable_dirty()->set_state(SysTabletsEntryPB::DELETED,
                                                           deletion_msg);
    actions.tablets_to_update.push_back(tablet.get());
  }

  Status s = sys_catalog_->Write(actions);
  if (!s.ok()) {
    s = s.CloneAndPrepend(
        Substitute("An error occurred while updating sys-catalog tables entry: $0",
                   s.ToString()));
    LOG(WARNING) << s.ToString();
    CheckIfNoLongerLeaderAndSetupError(s, resp);
    tablets_to_add_committer.Abort();
    tablets_to_drop_committer.Abort();
    return s;
  }

  // 9. Commit the in-memory state.
  {
    TRACE("Committing alterations to in-memory state");
    // Commit new tablet in-memory state. This doesn't require taking the global
    // lock since the new tablets are not yet visible, because they haven't been
    // added to the table or tablet index.
    tablets_to_add_committer.Commit();

    // Take the global catalog manager lock in order to modify the global table
    // and tablets indices.
    std::lock_guard<LockType> lock(lock_);
    if (req->has_new_table_name()) {
      if (table_names_map_.erase(table_name) != 1) {
        PANIC_RPC(rpc, Substitute(
            "Could not remove table (name $0) from map", table_name));
      }
      InsertOrDie(&table_names_map_, req->new_table_name(), table);
    }

    // Insert new tablets into the global tablet map. After this, the tablets
    // will be visible in GetTabletLocations RPCs.
    for (const auto& tablet : tablets_to_add) {
      InsertOrDie(&tablet_map_, tablet->tablet_id(), tablet);
    }
  }

  // Add and remove new tablets from the table. This makes the tablets visible
  // to GetTableLocations RPCs. This doesn't need to happen under the global
  // lock, since:
  //  * clients can not know the new tablet IDs, so GetTabletLocations RPCs
  //    are impossible.
  //  * the new tablets can not heartbeat yet, since they don't get created
  //    until further down.
  table->AddRemoveTablets(tablets_to_add, tablets_to_drop);

  // Commit state change for dropped tablets. This comes after removing the
  // tablets from their associated tables so that if a GetTableLocations or
  // GetTabletLocations returns a deleted tablet, the retry will never include
  // the tablet again.
  tablets_to_drop_committer.Commit();

  if (!tablets_to_add.empty() || has_metadata_changes) {
    l.Commit();
  } else {
    l.Unlock();
  }

  SendAlterTableRequest(table);
  for (const auto& tablet : tablets_to_drop) {
    TabletMetadataLock l(tablet.get(), TabletMetadataLock::READ);
    SendDeleteTabletRequest(tablet, l, deletion_msg);
  }

  background_tasks_->Wake();
  return Status::OK();
}

Status CatalogManager::IsAlterTableDone(const IsAlterTableDoneRequestPB* req,
                                        IsAlterTableDoneResponsePB* resp,
                                        rpc::RpcContext* rpc) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  scoped_refptr<TableInfo> table;

  // 1. Lookup the table and verify if it exists
  TRACE("Looking up table");
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == nullptr) {
    Status s = Status::NotFound("The table does not exist", SecureShortDebugString(req->table()));
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TRACE("Locking table");
  TableMetadataLock l(table.get(), TableMetadataLock::READ);
  RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(&l, resp));

  // 2. Verify if the alter is in-progress
  TRACE("Verify if there is an alter operation in progress for $0", table->ToString());
  resp->set_schema_version(l.data().pb.version());
  resp->set_done(l.data().pb.state() != SysTablesEntryPB::ALTERING);

  return Status::OK();
}

Status CatalogManager::GetTableSchema(const GetTableSchemaRequestPB* req,
                                      GetTableSchemaResponsePB* resp) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  scoped_refptr<TableInfo> table;

  // 1. Lookup the table and verify if it exists
  TRACE("Looking up table");
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == nullptr) {
    Status s = Status::NotFound("The table does not exist", SecureShortDebugString(req->table()));
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TRACE("Locking table");
  TableMetadataLock l(table.get(), TableMetadataLock::READ);
  RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(&l, resp));

  if (l.data().pb.has_fully_applied_schema()) {
    // An AlterTable is in progress; fully_applied_schema is the last
    // schema that has reached every TS.
    CHECK_EQ(SysTablesEntryPB::ALTERING, l.data().pb.state());
    resp->mutable_schema()->CopyFrom(l.data().pb.fully_applied_schema());
  } else {
    // There's no AlterTable, the regular schema is "fully applied".
    resp->mutable_schema()->CopyFrom(l.data().pb.schema());
  }
  resp->set_num_replicas(l.data().pb.num_replicas());
  resp->set_table_id(table->id());
  resp->mutable_partition_schema()->CopyFrom(l.data().pb.partition_schema());
  resp->set_create_table_done(!table->IsCreateInProgress());
  resp->set_table_name(l.data().pb.name());

  return Status::OK();
}

Status CatalogManager::ListTables(const ListTablesRequestPB* req,
                                  ListTablesResponsePB* resp) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  shared_lock<LockType> l(lock_);

  for (const TableInfoMap::value_type& entry : table_names_map_) {
    TableMetadataLock ltm(entry.second.get(), TableMetadataLock::READ);
    if (!ltm.data().is_running()) continue; // implies !is_deleted() too

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

Status CatalogManager::GetTableInfo(const string& table_id, scoped_refptr<TableInfo> *table) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  shared_lock<LockType> l(lock_);
  *table = FindPtrOrNull(table_ids_map_, table_id);
  return Status::OK();
}

Status CatalogManager::GetAllTables(std::vector<scoped_refptr<TableInfo>>* tables) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  tables->clear();
  shared_lock<LockType> l(lock_);
  for (const TableInfoMap::value_type& e : table_ids_map_) {
    tables->push_back(e.second);
  }

  return Status::OK();
}

Status CatalogManager::TableNameExists(const string& table_name, bool* exists) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  shared_lock<LockType> l(lock_);
  *exists = ContainsKey(table_names_map_, table_name);
  return Status::OK();
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
  TRACE_EVENT2("master", "ProcessTabletReport",
               "requestor", rpc->requestor_string(),
               "num_tablets", report.updated_tablets_size());

  leader_lock_.AssertAcquiredForReading();

  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Received tablet report from " <<
      RequestorString(rpc) << ": " << SecureDebugString(report);
  }

  // TODO: on a full tablet report, we may want to iterate over the tablets we think
  // the server should have, compare vs the ones being reported, and somehow mark
  // any that have been "lost" (eg somehow the tablet metadata got corrupted or something).

  for (const ReportedTabletPB& reported : report.updated_tablets()) {
    ReportedTabletUpdatesPB *tablet_report = report_update->add_tablets();
    tablet_report->set_tablet_id(reported.tablet_id());
    RETURN_NOT_OK_PREPEND(HandleReportedTablet(ts_desc, reported, tablet_report),
                          Substitute("Error handling $0", SecureShortDebugString(reported)));
  }

  if (report.updated_tablets_size() > 0) {
    background_tasks_->WakeIfHasPendingUpdates();
  }

  return Status::OK();
}

namespace {
// Return true if receiving 'report' for a tablet in CREATING state should
// transition it to the RUNNING state.
bool ShouldTransitionTabletToRunning(const ReportedTabletPB& report) {
  if (report.state() != tablet::RUNNING) return false;

  // In many tests, we disable leader election, so newly created tablets
  // will never elect a leader on their own. In this case, we transition
  // to RUNNING as soon as we get a single report.
  if (!FLAGS_catalog_manager_wait_for_new_tablets_to_elect_leader) {
    return true;
  }

  // Otherwise, we only transition to RUNNING once there is a leader that is a
  // member of the committed configuration.
  const ConsensusStatePB& cstate = report.consensus_state();
  return cstate.has_leader_uuid() &&
      IsRaftConfigMember(cstate.leader_uuid(), cstate.committed_config());
}
} // anonymous namespace

Status CatalogManager::HandleReportedTablet(TSDescriptor* ts_desc,
                                            const ReportedTabletPB& report,
                                            ReportedTabletUpdatesPB *report_updates) {
  TRACE_EVENT1("master", "HandleReportedTablet",
               "tablet_id", report.tablet_id());
  scoped_refptr<TabletInfo> tablet;
  {
    shared_lock<LockType> l(lock_);
    tablet = FindPtrOrNull(tablet_map_, report.tablet_id());
  }
  if (!tablet) {
    // It'd be unsafe to ask the tserver to delete this tablet without first
    // replicating something to our followers (i.e. to guarantee that we're the
    // leader). For example, if we were a rogue master, we might be deleting a
    // tablet created by a new master accidentally. But masters retain metadata
    // for deleted tablets forever, so a tablet can only be truly unknown in
    // the event of a serious misconfiguration, such as a tserver heartbeating
    // to the wrong cluster. Therefore, it should be reasonable to ignore it
    // and wait for an operator fix the situation.
    if (FLAGS_catalog_manager_delete_orphaned_tablets) {
      LOG(INFO) << "Deleting unknown tablet " << report.tablet_id();
      SendDeleteReplicaRequest(report.tablet_id(), TABLET_DATA_DELETED,
                               boost::none, nullptr, ts_desc->permanent_uuid(),
                               "Report from unknown tablet");
    } else {
      LOG(WARNING) << "Ignoring report from unknown tablet: "
                   << report.tablet_id();
    }
    return Status::OK();
  }
  DCHECK(tablet->table()); // guaranteed by TabletLoader

  VLOG(3) << "tablet report: " << SecureShortDebugString(report);

  // TODO: we don't actually need to do the COW here until we see we're going
  // to change the state. Can we change CowedObject to lazily do the copy?
  TableMetadataLock table_lock(tablet->table().get(), TableMetadataLock::READ);
  TabletMetadataLock tablet_lock(tablet.get(), TabletMetadataLock::WRITE);

  // If the TS is reporting a tablet which has been deleted, or a tablet from
  // a table which has been deleted, send it an RPC to delete it.
  if (tablet_lock.data().is_deleted() ||
      table_lock.data().is_deleted()) {
    report_updates->set_state_msg(tablet_lock.data().pb.state_msg());
    const string msg = tablet_lock.data().pb.state_msg();
    LOG(INFO) << "Got report from deleted tablet " << tablet->ToString()
              << " (" << msg << "): Sending delete request for this tablet";
    // TODO: Cancel tablet creation, instead of deleting, in cases where
    // that might be possible (tablet creation timeout & replacement).
    SendDeleteReplicaRequest(tablet->tablet_id(), TABLET_DATA_DELETED,
                             boost::none, tablet->table(),
                             ts_desc->permanent_uuid(), msg);
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
  bool tablet_needs_alter = false;
  if (report.has_schema_version() &&
      table_lock.data().pb.version() != report.schema_version()) {
    if (report.schema_version() > table_lock.data().pb.version()) {
      LOG(ERROR) << "TS " << ts_desc->ToString()
                 << " has reported a schema version greater than the current one "
                 << " for tablet " << tablet->ToString()
                 << ". Expected version " << table_lock.data().pb.version()
                 << " got " << report.schema_version()
                 << " (corruption)";
    } else {
      LOG(INFO) << "TS " << ts_desc->ToString()
            << " does not have the latest schema for tablet " << tablet->ToString()
            << ". Expected version " << table_lock.data().pb.version()
            << " got " << report.schema_version();
    }
    // It's possible that the tablet being reported is a laggy replica, and in fact
    // the leader has already received an AlterTable RPC. That's OK, though --
    // it'll safely ignore it if we send another.
    tablet_needs_alter = true;
  }


  if (report.has_error()) {
    Status s = StatusFromPB(report.error());
    DCHECK(!s.ok());
    DCHECK_EQ(report.state(), tablet::FAILED);
    LOG(WARNING) << "Tablet " << tablet->ToString() << " has failed on TS "
                 << ts_desc->ToString() << ": " << s.ToString();
    return Status::OK();
  }

  // The report will not have a consensus_state if it is in the
  // middle of starting up, such as during tablet bootstrap.
  if (report.has_consensus_state()) {
    const ConsensusStatePB& prev_cstate = tablet_lock.data().pb.consensus_state();
    ConsensusStatePB cstate = report.consensus_state();

    // Check if we got a report from a tablet that is no longer part of the raft
    // config. If so, tombstone it. We only tombstone replicas that include a
    // committed raft config in their report that has an opid_index strictly
    // less than the latest reported committed config, and (obviously) who are
    // not members of the latest config. This prevents us from spuriously
    // deleting replicas that have just been added to a pending config and are
    // in the process of catching up to the log entry where they were added to
    // the config.
    if (FLAGS_master_tombstone_evicted_tablet_replicas &&
        cstate.committed_config().opid_index() < prev_cstate.committed_config().opid_index() &&
        !IsRaftConfigMember(ts_desc->permanent_uuid(), prev_cstate.committed_config())) {
      SendDeleteReplicaRequest(report.tablet_id(), TABLET_DATA_TOMBSTONED,
                               prev_cstate.committed_config().opid_index(),
                               tablet->table(), ts_desc->permanent_uuid(),
                               Substitute("Replica from old config with index $0 (latest is $1)",
                                          cstate.committed_config().opid_index(),
                                          prev_cstate.committed_config().opid_index()));
      return Status::OK();
    }

    // If the reported leader is not a member of the committed config, then we
    // disregard the leader state.
    if (cstate.has_leader_uuid() &&
        !IsRaftConfigMember(cstate.leader_uuid(), cstate.committed_config())) {
      cstate.clear_leader_uuid();
    }

    // If the tablet was not RUNNING, and we have a leader elected, mark it as RUNNING.
    // We need to wait for a leader before marking a tablet as RUNNING, or else we
    // could incorrectly consider a tablet created when only a minority of its replicas
    // were successful. In that case, the tablet would be stuck in this bad state
    // forever.
    if (!tablet_lock.data().is_running() && ShouldTransitionTabletToRunning(report)) {
      DCHECK_EQ(SysTabletsEntryPB::CREATING, tablet_lock.data().pb.state())
          << "Tablet in unexpected state: " << tablet->ToString()
          << ": " << SecureShortDebugString(tablet_lock.data().pb);
      // Mark the tablet as running
      // TODO: we could batch the IO onto a background thread, or at least
      // across multiple tablets in the same report.
      VLOG(1) << "Tablet " << tablet->ToString() << " is now online";
      tablet_lock.mutable_data()->set_state(SysTabletsEntryPB::RUNNING,
                                            "Tablet reported with an active leader");
    }

    // The Master only accepts committed consensus configurations since it needs the committed index
    // to only cache the most up-to-date config.
    if (PREDICT_FALSE(!cstate.committed_config().has_opid_index())) {
      LOG(DFATAL) << "Missing opid_index in reported config:\n" << SecureDebugString(report);
      return Status::InvalidArgument("Missing opid_index in reported config");
    }

    bool modified_cstate = false;
    if (cstate.committed_config().opid_index() > prev_cstate.committed_config().opid_index() ||
        (cstate.has_leader_uuid() &&
         (!prev_cstate.has_leader_uuid() || cstate.current_term() > prev_cstate.current_term()))) {

      // When a config change is reported to the master, it may not include the
      // leader because the follower doing the reporting may not know who the
      // leader is yet (it may have just started up). If the reported config
      // has the same term as the previous config, and the leader was
      // previously known for the current term, then retain knowledge of that
      // leader even if it wasn't reported in the latest config.
      if (cstate.current_term() == prev_cstate.current_term()) {
        if (!cstate.has_leader_uuid() && prev_cstate.has_leader_uuid()) {
          cstate.set_leader_uuid(prev_cstate.leader_uuid());
          modified_cstate = true;
        // Sanity check to detect consensus divergence bugs.
        } else if (cstate.has_leader_uuid() && prev_cstate.has_leader_uuid() &&
                   cstate.leader_uuid() != prev_cstate.leader_uuid()) {
          string msg = Substitute("Previously reported cstate for tablet $0 gave "
                                  "a different leader for term $1 than the current cstate. "
                                  "Previous cstate: $2. Current cstate: $3.",
                                  tablet->ToString(), cstate.current_term(),
                                  SecureShortDebugString(prev_cstate),
                                  SecureShortDebugString(cstate));
          LOG(DFATAL) << msg;
          return Status::InvalidArgument(msg);
        }
      }

      // If a replica is reporting a new consensus configuration, update the
      // master's copy of that configuration.
      LOG(INFO) << "T " << tablet->tablet_id() << " reported consensus state change: "
                << DiffConsensusStates(prev_cstate, cstate)
                << ". New consensus state: " << SecureShortDebugString(cstate);

      // If we need to change the report, copy the whole thing on the stack
      // rather than const-casting.
      const ReportedTabletPB* final_report = &report;
      ReportedTabletPB updated_report;
      if (modified_cstate) {
        updated_report = report;
        *updated_report.mutable_consensus_state() = cstate;
        final_report = &updated_report;
      }

      VLOG(2) << "Updating consensus configuration for tablet "
              << final_report->tablet_id()
              << " from config reported by " << ts_desc->ToString()
              << " to that committed in log index "
              << final_report->consensus_state().committed_config().opid_index()
              << " with leader state from term "
              << final_report->consensus_state().current_term();

      RETURN_NOT_OK(HandleRaftConfigChanged(*final_report, tablet,
                                            &tablet_lock, &table_lock));

    }
  }

  table_lock.Unlock();

  // We update the tablets each time they are reported.
  // SysCatalogTable::Write will short-circuit the case where the data
  // has not in fact changed since the previous version and avoid any
  // unnecessary operations.
  SysCatalogTable::Actions actions;
  actions.tablets_to_update.push_back(tablet.get());
  Status s = sys_catalog_->Write(actions);
  if (!s.ok()) {
    LOG(WARNING) << "Error updating tablets: " << s.ToString() << ". Tablet report was: "
                 << SecureShortDebugString(report);
    return s;
  }
  tablet_lock.Commit();

  // Need to defer the AlterTable command to after we've committed the new tablet data,
  // since the tablet report may also be updating the raft config, and the Alter Table
  // request needs to know who the most recent leader is.
  if (tablet_needs_alter) {
    SendAlterTabletRequest(tablet);
  } else if (report.has_schema_version()) {
    HandleTabletSchemaVersionReport(tablet.get(), report.schema_version());
  }

  return Status::OK();
}

Status CatalogManager::HandleRaftConfigChanged(
    const ReportedTabletPB& report,
    const scoped_refptr<TabletInfo>& tablet,
    TabletMetadataLock* tablet_lock,
    TableMetadataLock* table_lock) {

  DCHECK(tablet_lock->is_write_locked());
  ConsensusStatePB prev_cstate = tablet_lock->mutable_data()->pb.consensus_state();
  const ConsensusStatePB& cstate = report.consensus_state();
  *tablet_lock->mutable_data()->pb.mutable_consensus_state() = cstate;

  if (FLAGS_master_tombstone_evicted_tablet_replicas) {
    std::unordered_set<string> current_member_uuids;
    for (const consensus::RaftPeerPB& peer : cstate.committed_config().peers()) {
      InsertOrDie(&current_member_uuids, peer.permanent_uuid());
    }
    // Send a DeleteTablet() request to peers that are not in the new config.
    for (const consensus::RaftPeerPB& prev_peer : prev_cstate.committed_config().peers()) {
      const string& peer_uuid = prev_peer.permanent_uuid();
      if (!ContainsKey(current_member_uuids, peer_uuid)) {
        SendDeleteReplicaRequest(report.tablet_id(), TABLET_DATA_TOMBSTONED,
                                 prev_cstate.committed_config().opid_index(),
                                 tablet->table(), peer_uuid,
                                 Substitute("TS $0 not found in new config with opid_index $1",
                                            peer_uuid, cstate.committed_config().opid_index()));
      }
    }
  }

  // If the config is under-replicated, add a server to the config.
  if (FLAGS_master_add_server_when_underreplicated &&
      CountVoters(cstate.committed_config()) < table_lock->data().pb.num_replicas()) {
    SendAddServerRequest(tablet, cstate);
  }

  return Status::OK();
}

Status CatalogManager::GetTabletReplica(const string& tablet_id,
                                        scoped_refptr<TabletReplica>* replica) const {
  // Note: CatalogManager has only one table, 'sys_catalog', with only
  // one tablet.
  shared_lock<LockType> l(lock_);
  if (!sys_catalog_) {
    return Status::ServiceUnavailable("Systable not yet initialized");
  }
  if (sys_catalog_->tablet_id() == tablet_id) {
    *replica = sys_catalog_->tablet_replica();
  } else {
    return Status::NotFound(Substitute("no SysTable exists with tablet_id $0 in CatalogManager",
                                       tablet_id));
  }
  return Status::OK();
}

void CatalogManager::GetTabletReplicas(vector<scoped_refptr<TabletReplica>>* replicas) const {
  // Note: CatalogManager has only one table, 'sys_catalog', with only
  // one tablet.
  shared_lock<LockType> l(lock_);
  if (!sys_catalog_) {
    return;
  }
  replicas->push_back(sys_catalog_->tablet_replica());
}

const NodeInstancePB& CatalogManager::NodeInstance() const {
  return master_->instance_pb();
}

void CatalogManager::StartTabletCopy(
    const StartTabletCopyRequestPB* /* req */,
    std::function<void(const Status&, TabletServerErrorPB::Code)> cb) {
  cb(Status::NotSupported("Tablet Copy not yet implemented for the master tablet"),
     TabletServerErrorPB::UNKNOWN_ERROR);
}

// Interface used by RetryingTSRpcTask to pick the tablet server to
// send the next RPC to.
class TSPicker {
 public:
  TSPicker() {}
  virtual ~TSPicker() {}

  // Sets *ts_uuid to the uuid of the tserver to contact for the next RPC.
  virtual Status PickReplica(string* ts_uuid) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(TSPicker);
};

// Implementation of TSPicker which sends to a specific tablet server,
// identified by its UUID.
class PickSpecificUUID : public TSPicker {
 public:
  explicit PickSpecificUUID(string ts_uuid)
      : ts_uuid_(std::move(ts_uuid)) {}

  virtual Status PickReplica(string* ts_uuid) OVERRIDE {
    // Just a straight passthrough.
    *ts_uuid = ts_uuid_;
    return Status::OK();
  }

 private:
  const string ts_uuid_;

  DISALLOW_COPY_AND_ASSIGN(PickSpecificUUID);
};

// Implementation of TSPicker which locates the current leader replica,
// and sends the RPC to that server.
class PickLeaderReplica : public TSPicker {
 public:
  explicit PickLeaderReplica(const scoped_refptr<TabletInfo>& tablet) :
    tablet_(tablet) {
  }

  virtual Status PickReplica(string* ts_uuid) OVERRIDE {
    TabletMetadataLock l(tablet_.get(), TabletMetadataLock::READ);

    string err_msg;
    if (!l.data().pb.has_consensus_state()) {
      // The tablet is still in the PREPARING state and has no replicas.
      err_msg = Substitute("Tablet $0 has no consensus state",
                           tablet_->tablet_id());
    } else if (!l.data().pb.consensus_state().has_leader_uuid()) {
      // The tablet may be in the midst of a leader election.
      err_msg = Substitute("Tablet $0 consensus state has no leader",
                           tablet_->tablet_id());
    } else {
      *ts_uuid = l.data().pb.consensus_state().leader_uuid();
      return Status::OK();
    }
    return Status::NotFound("No leader found", err_msg);
  }

 private:
  const scoped_refptr<TabletInfo> tablet_;
};

// A background task which continuously retries sending an RPC to a tablet server.
//
// The target tablet server is refreshed before each RPC by consulting the provided
// TSPicker implementation.
class RetryingTSRpcTask : public MonitoredTask {
 public:
  RetryingTSRpcTask(Master *master,
                    gscoped_ptr<TSPicker> replica_picker,
                    const scoped_refptr<TableInfo>& table)
    : master_(master),
      replica_picker_(std::move(replica_picker)),
      table_(table),
      start_ts_(MonoTime::Now()),
      deadline_(start_ts_ + MonoDelta::FromMilliseconds(FLAGS_unresponsive_ts_rpc_timeout_ms)),
      attempt_(0),
      state_(kStateRunning) {
  }

  // Send the subclass RPC request.
  Status Run();

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
  // The implementation must return true if the callback was registered, and
  // false if an error occurred and no callback will occur.
  virtual bool SendRequest(int attempt) = 0;

  // Handle the response from the RPC request. On success, MarkSuccess() must
  // be called to mutate the state_ variable. If retry is desired, then
  // no state change is made. Retries will automatically be attempted as long
  // as the state is kStateRunning and deadline_ has not yet passed.
  //
  // Runs on the reactor thread, so must not block or perform any IO.
  virtual void HandleResponse(int attempt) = 0;

  // Return the id of the tablet that is the subject of the async request.
  virtual string tablet_id() const = 0;

  // Overridable log prefix with reasonable default.
  virtual string LogPrefix() const {
    return Substitute("$0: ", description());
  }

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
  //
  // Runs on a reactor thread, so should not block or do any IO.
  void RpcCallback();

  Master * const master_;
  const gscoped_ptr<TSPicker> replica_picker_;
  const scoped_refptr<TableInfo> table_;

  MonoTime start_ts_;
  MonoTime end_ts_;
  MonoTime deadline_;

  int attempt_;
  rpc::RpcController rpc_;
  TSDescriptor* target_ts_desc_;
  shared_ptr<tserver::TabletServerAdminServiceProxy> ts_proxy_;
  shared_ptr<consensus::ConsensusServiceProxy> consensus_proxy_;

 private:
  // Reschedules the current task after a backoff delay.
  // Returns false if the task was not rescheduled due to reaching the maximum
  // timeout or because the task is no longer in a running state.
  // Returns true if rescheduling the task was successful.
  bool RescheduleWithBackoffDelay();

  // Callback for Reactor delayed task mechanism. Called either when it is time
  // to execute the delayed task (with status == OK) or when the task
  // is cancelled, i.e. when the scheduling timer is shut down (status != OK).
  void RunDelayedTask(const Status& status);

  // Clean up request and release resources. May call 'delete this'.
  void UnregisterAsyncTask();

  // Find a new replica and construct the RPC proxy.
  Status ResetTSProxy();

  // Use state() and MarkX() accessors.
  AtomicWord state_;
};

Status RetryingTSRpcTask::Run() {
  if (PREDICT_FALSE(FLAGS_catalog_manager_fail_ts_rpcs)) {
    MarkFailed();
    UnregisterAsyncTask(); // May delete this.
    return Status::RuntimeError("Async RPCs configured to fail");
  }

  // Calculate and set the timeout deadline.
  MonoTime timeout = MonoTime::Now() + MonoDelta::FromMilliseconds(FLAGS_master_ts_rpc_timeout_ms);
  const MonoTime& deadline = std::min(timeout, deadline_);
  rpc_.Reset();
  rpc_.set_deadline(deadline);

  Status s = ResetTSProxy();
  if (s.ok()) {
    if (SendRequest(++attempt_)) {
      return Status::OK();
    }
  } else {
    s = s.CloneAndPrepend("Failed to reset TS proxy");
  }

  if (!RescheduleWithBackoffDelay()) {
    MarkFailed();
    UnregisterAsyncTask();  // May call 'delete this'.
  }
  return s;
}

void RetryingTSRpcTask::RpcCallback() {
  if (!rpc_.status().ok()) {
    LOG(WARNING) << "TS " << target_ts_desc_->ToString() << ": "
                  << type_name() << " RPC failed for tablet "
                  << tablet_id() << ": " << rpc_.status().ToString();
  } else if (state() != kStateAborted) {
    HandleResponse(attempt_); // Modifies state_.
  }

  // Schedule a retry if the RPC call was not successful.
  if (RescheduleWithBackoffDelay()) {
    return;
  }

  UnregisterAsyncTask();  // May call 'delete this'.
}

bool RetryingTSRpcTask::RescheduleWithBackoffDelay() {
  if (state() != kStateRunning) return false;
  MonoTime now = MonoTime::Now();
  // We assume it might take 10ms to process the request in the best case,
  // fail if we have less than that amount of time remaining.
  int64_t millis_remaining = (deadline_ - now).ToMilliseconds() - 10;
  // Exponential backoff with jitter.
  int64_t base_delay_ms;
  if (attempt_ <= 12) {
    base_delay_ms = 1 << (attempt_ + 3);  // 1st retry delayed 2^4 ms, 2nd 2^5, etc.
  } else {
    base_delay_ms = 60 * 1000; // cap at 1 minute
  }
  int64_t jitter_ms = rand() % 50;              // Add up to 50ms of additional random delay.
  int64_t delay_millis = std::min<int64_t>(base_delay_ms + jitter_ms, millis_remaining);

  if (delay_millis <= 0) {
    LOG(WARNING) << "Request timed out: " << description();
    MarkFailed();
  } else {
    LOG(INFO) << "Scheduling retry of " << description() << " with a delay"
              << " of " << delay_millis << "ms (attempt = " << attempt_ << ")...";
    master_->messenger()->ScheduleOnReactor(
        boost::bind(&RetryingTSRpcTask::RunDelayedTask, this, _1),
        MonoDelta::FromMilliseconds(delay_millis));
    return true;
  }
  return false;
}

void RetryingTSRpcTask::RunDelayedTask(const Status& status) {
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

void RetryingTSRpcTask::UnregisterAsyncTask() {
  end_ts_ = MonoTime::Now();
  if (table_ != nullptr) {
    table_->RemoveTask(this);
  } else {
    // This is a floating task (since the table does not exist)
    // created as response to a tablet report.
    Release();  // May call "delete this";
  }
}

Status RetryingTSRpcTask::ResetTSProxy() {
  // TODO: if there is no replica available, should we still keep the task running?
  string ts_uuid;
  // TODO: don't pick replica we can't lookup???
  RETURN_NOT_OK(replica_picker_->PickReplica(&ts_uuid));
  shared_ptr<TSDescriptor> ts_desc;
  if (!master_->ts_manager()->LookupTSByUUID(ts_uuid, &ts_desc)) {
    return Status::NotFound(Substitute("Could not find TS for UUID $0",
                                        ts_uuid));
  }

  // This assumes that TSDescriptors are never deleted by the master,
  // so the task need not take ownership of the returned pointer.
  target_ts_desc_ = ts_desc.get();

  shared_ptr<tserver::TabletServerAdminServiceProxy> ts_proxy;
  RETURN_NOT_OK(target_ts_desc_->GetTSAdminProxy(master_->messenger(), &ts_proxy));
  ts_proxy_.swap(ts_proxy);

  shared_ptr<consensus::ConsensusServiceProxy> consensus_proxy;
  RETURN_NOT_OK(target_ts_desc_->GetConsensusProxy(master_->messenger(), &consensus_proxy));
  consensus_proxy_.swap(consensus_proxy);

  rpc_.Reset();
  return Status::OK();
}

// RetryingTSRpcTask subclass which always retries the same tablet server,
// identified by its UUID.
class RetrySpecificTSRpcTask : public RetryingTSRpcTask {
 public:
  RetrySpecificTSRpcTask(Master* master,
                         const string& permanent_uuid,
                         const scoped_refptr<TableInfo>& table)
    : RetryingTSRpcTask(master,
                        gscoped_ptr<TSPicker>(new PickSpecificUUID(permanent_uuid)),
                        table),
      permanent_uuid_(permanent_uuid) {
  }

 protected:
  const string permanent_uuid_;
};

// Fire off the async create tablet.
// This requires that the new tablet info is locked for write, and the
// consensus configuration information has been filled into the 'dirty' data.
class AsyncCreateReplica : public RetrySpecificTSRpcTask {
 public:

  // The tablet lock must be acquired for reading before making this call.
  AsyncCreateReplica(Master *master,
                     const string& permanent_uuid,
                     const scoped_refptr<TabletInfo>& tablet,
                     const TabletMetadataLock& tablet_lock)
    : RetrySpecificTSRpcTask(master, permanent_uuid, tablet->table()),
      tablet_id_(tablet->tablet_id()) {
    deadline_ = start_ts_ + MonoDelta::FromMilliseconds(FLAGS_tablet_creation_timeout_ms);

    TableMetadataLock table_lock(tablet->table().get(), TableMetadataLock::READ);
    req_.set_dest_uuid(permanent_uuid);
    req_.set_table_id(tablet->table()->id());
    req_.set_tablet_id(tablet->tablet_id());
    req_.mutable_partition()->CopyFrom(tablet_lock.data().pb.partition());
    req_.set_table_name(table_lock.data().pb.name());
    req_.mutable_schema()->CopyFrom(table_lock.data().pb.schema());
    req_.mutable_partition_schema()->CopyFrom(
        table_lock.data().pb.partition_schema());
    req_.mutable_config()->CopyFrom(
        tablet_lock.data().pb.consensus_state().committed_config());
  }

  virtual string type_name() const OVERRIDE { return "Create Tablet"; }

  virtual string description() const OVERRIDE {
    return "CreateTablet RPC for tablet " + tablet_id_ + " on TS " + permanent_uuid_;
  }

 protected:
  virtual string tablet_id() const OVERRIDE { return tablet_id_; }

  virtual void HandleResponse(int attempt) OVERRIDE {
    if (!resp_.has_error()) {
      MarkComplete();
    } else {
      Status s = StatusFromPB(resp_.error().status());
      if (s.IsAlreadyPresent()) {
        LOG(INFO) << "CreateTablet RPC for tablet " << tablet_id_
                  << " on TS " << target_ts_desc_->ToString() << " returned already present: "
                  << s.ToString();
        MarkComplete();
      } else {
        LOG(WARNING) << "CreateTablet RPC for tablet " << tablet_id_
                     << " on TS " << target_ts_desc_->ToString() << " failed: " << s.ToString();
      }
    }
  }

  virtual bool SendRequest(int attempt) OVERRIDE {
    VLOG(1) << "Send create tablet request to "
            << target_ts_desc_->ToString() << ":\n"
            << " (attempt " << attempt << "):\n"
            << SecureDebugString(req_);
    ts_proxy_->CreateTabletAsync(req_, &resp_, &rpc_,
                                 boost::bind(&AsyncCreateReplica::RpcCallback, this));
    return true;
  }

 private:
  const string tablet_id_;
  tserver::CreateTabletRequestPB req_;
  tserver::CreateTabletResponsePB resp_;
};

// Send a DeleteTablet() RPC request.
class AsyncDeleteReplica : public RetrySpecificTSRpcTask {
 public:
  AsyncDeleteReplica(
      Master* master, const string& permanent_uuid,
      const scoped_refptr<TableInfo>& table, std::string tablet_id,
      TabletDataState delete_type,
      boost::optional<int64_t> cas_config_opid_index_less_or_equal,
      string reason)
      : RetrySpecificTSRpcTask(master, permanent_uuid, table),
        tablet_id_(std::move(tablet_id)),
        delete_type_(delete_type),
        cas_config_opid_index_less_or_equal_(
            std::move(cas_config_opid_index_less_or_equal)),
        reason_(std::move(reason)) {}

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
      TabletServerErrorPB::Code code = resp_.error().code();
      switch (code) {
        case TabletServerErrorPB::TABLET_NOT_FOUND:
          LOG(WARNING) << "TS " << target_ts_desc_->ToString()
                       << ": delete failed for tablet " << tablet_id_
                       << " because the tablet was not found. No further retry: "
                       << status.ToString();
          MarkComplete();
          break;
        case TabletServerErrorPB::CAS_FAILED:
          LOG(WARNING) << "TS " << target_ts_desc_->ToString()
                       << ": delete failed for tablet " << tablet_id_
                       << " due to a CAS failure. No further retry: " << status.ToString();
          MarkComplete();
          break;
        default:
          LOG(WARNING) << "TS " << target_ts_desc_->ToString()
                       << ": delete failed for tablet " << tablet_id_
                       << " with error code " << TabletServerErrorPB::Code_Name(code)
                       << ": " << status.ToString();
          break;
      }
    } else {
      master_->catalog_manager()->NotifyTabletDeleteSuccess(permanent_uuid_, tablet_id_);
      if (table_) {
        LOG(INFO) << "TS " << target_ts_desc_->ToString()
                  << ": tablet " << tablet_id_
                  << " (table " << table_->ToString() << ") successfully deleted";
      } else {
        LOG(WARNING) << "TS " << target_ts_desc_->ToString()
                     << ": tablet " << tablet_id_
                     << " did not belong to a known table, but was successfully deleted";
      }
      MarkComplete();
      VLOG(1) << "TS " << target_ts_desc_->ToString()
              << ": delete complete on tablet " << tablet_id_;
    }
  }

  virtual bool SendRequest(int attempt) OVERRIDE {
    tserver::DeleteTabletRequestPB req;
    req.set_dest_uuid(permanent_uuid_);
    req.set_tablet_id(tablet_id_);
    req.set_reason(reason_);
    req.set_delete_type(delete_type_);
    if (cas_config_opid_index_less_or_equal_) {
      req.set_cas_config_opid_index_less_or_equal(*cas_config_opid_index_less_or_equal_);
    }

    LOG(INFO) << Substitute("Sending DeleteTablet($0) for tablet $1 on $2 "
                            "($3)",
                            TabletDataState_Name(delete_type_),
                            tablet_id_,
                            target_ts_desc_->ToString(),
                            reason_);
    ts_proxy_->DeleteTabletAsync(req, &resp_, &rpc_,
                                 boost::bind(&AsyncDeleteReplica::RpcCallback, this));
    return true;
  }

  const std::string tablet_id_;
  const TabletDataState delete_type_;
  const boost::optional<int64_t> cas_config_opid_index_less_or_equal_;
  const std::string reason_;
  tserver::DeleteTabletResponsePB resp_;
};

namespace {

// Given exactly two choices in 'two_choices', pick the better tablet server on
// which to place a tablet replica. Ties are broken using 'rng'.
shared_ptr<TSDescriptor> PickBetterReplicaLocation(const TSDescriptorVector& two_choices,
                                                   ThreadSafeRandom* rng) {
  DCHECK_EQ(two_choices.size(), 2);

  const auto& a = two_choices[0];
  const auto& b = two_choices[1];

  // When creating replicas, we consider two aspects of load:
  //   (1) how many tablet replicas are already on the server, and
  //   (2) how often we've chosen this server recently.
  //
  // The first factor will attempt to put more replicas on servers that
  // are under-loaded (eg because they have newly joined an existing cluster, or have
  // been reformatted and re-joined).
  //
  // The second factor will ensure that we take into account the recent selection
  // decisions even if those replicas are still in the process of being created (and thus
  // not yet reported by the server). This is important because, while creating a table,
  // we batch the selection process before sending any creation commands to the
  // servers themselves.
  //
  // TODO(wdberkeley): in the future we may want to factor in other items such
  // as available disk space, actual request load, etc.
  double load_a = a->RecentReplicaCreations() + a->num_live_replicas();
  double load_b = b->RecentReplicaCreations() + b->num_live_replicas();
  if (load_a < load_b) {
    return a;
  }
  if (load_b < load_a) {
    return b;
  }
  // If the load is the same, we can just pick randomly.
  return two_choices[rng->Uniform(2)];
}

// Given the tablet servers in 'ts_descs', use 'rng' to pick a tablet server to
// host a tablet replica, excluding tablet servers in 'excluded'.
// If there are no servers in 'ts_descs' that are not in 'excluded, return nullptr.
shared_ptr<TSDescriptor> SelectReplica(const TSDescriptorVector& ts_descs,
                                       const set<shared_ptr<TSDescriptor>>& excluded,
                                       ThreadSafeRandom* rng) {
  // The replica selection algorithm follows the idea from
  // "Power of Two Choices in Randomized Load Balancing"[1]. For each replica,
  // we randomly select two tablet servers, and then assign the replica to the
  // less-loaded one of the two. This has some nice properties:
  //
  // 1) because the initial selection of two servers is random, we get good
  //    spreading of replicas across the cluster. In contrast if we sorted by
  //    load and always picked under-loaded servers first, we'd end up causing
  //    all tablets of a new table to be placed on an empty server. This wouldn't
  //    give good load balancing of that table.
  //
  // 2) because we pick the less-loaded of two random choices, we do end up with a
  //    weighting towards filling up the underloaded one over time, without
  //    the extreme scenario above.
  //
  // 3) because we don't follow any sequential pattern, every server is equally
  //    likely to replicate its tablets to every other server. In contrast, a
  //    round-robin design would enforce that each server only replicates to its
  //    adjacent nodes in the TS sort order, limiting recovery bandwidth (see
  //    KUDU-1317).
  //
  // [1] http://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf

  // Pick two random servers, excluding those we've already picked.
  // If we've only got one server left, 'two_choices' will actually
  // just contain one element.
  vector<shared_ptr<TSDescriptor>> two_choices;
  rng->ReservoirSample(ts_descs, 2, excluded, &two_choices);

  if (two_choices.size() == 2) {
    // Pick the better of the two.
    return PickBetterReplicaLocation(two_choices, rng);
  }
  if (two_choices.size() == 1) {
    return two_choices[0];
  }
  return nullptr;
}

} // anonymous namespace

// Send the "Alter Table" with the latest table schema to the leader replica
// for the tablet.
// Keeps retrying until we get an "ok" response.
//  - Alter completed
//  - Tablet has already a newer version
//    (which may happen in case of concurrent alters, or in case a previous attempt timed
//     out but was actually applied).
class AsyncAlterTable : public RetryingTSRpcTask {
 public:
  AsyncAlterTable(Master *master,
                  const scoped_refptr<TabletInfo>& tablet)
    : RetryingTSRpcTask(master,
                        gscoped_ptr<TSPicker>(new PickLeaderReplica(tablet)),
                        tablet->table()),
      tablet_(tablet) {
  }

  virtual string type_name() const OVERRIDE { return "Alter Table"; }

  virtual string description() const OVERRIDE {
    return tablet_->ToString() + " Alter Table RPC";
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
          LOG(WARNING) << "TS " << target_ts_desc_->ToString() << ": alter failed for tablet "
                       << tablet_->ToString() << " no further retry: " << status.ToString();
          MarkComplete();
          break;
        default:
          LOG(WARNING) << "TS " << target_ts_desc_->ToString() << ": alter failed for tablet "
                       << tablet_->ToString() << ": " << status.ToString();
          break;
      }
    } else {
      MarkComplete();
      VLOG(1) << "TS " << target_ts_desc_->ToString()
              << ": alter complete on tablet " << tablet_->ToString();
    }

    if (state() != kStateComplete) {
      VLOG(1) << "Still waiting for other tablets to finish ALTER";
    }
  }

  virtual bool SendRequest(int attempt) OVERRIDE {
    TableMetadataLock l(tablet_->table().get(), TableMetadataLock::READ);

    tserver::AlterSchemaRequestPB req;
    req.set_dest_uuid(target_ts_desc_->permanent_uuid());
    req.set_tablet_id(tablet_->tablet_id());
    req.set_new_table_name(l.data().pb.name());
    req.set_schema_version(l.data().pb.version());
    req.mutable_schema()->CopyFrom(l.data().pb.schema());

    l.Unlock();

    VLOG(1) << "Send alter table request to " << target_ts_desc_->ToString()
            << " (attempt " << attempt << "):\n"
            << SecureDebugString(req);
    ts_proxy_->AlterSchemaAsync(req, &resp_, &rpc_,
                                boost::bind(&AsyncAlterTable::RpcCallback, this));
    return true;
  }

  scoped_refptr<TabletInfo> tablet_;
  tserver::AlterSchemaResponsePB resp_;
};

class AsyncAddServerTask : public RetryingTSRpcTask {
 public:
  AsyncAddServerTask(Master *master,
                     const scoped_refptr<TabletInfo>& tablet,
                     ConsensusStatePB cstate,
                     ThreadSafeRandom* rng)
    : RetryingTSRpcTask(master,
                        gscoped_ptr<TSPicker>(new PickLeaderReplica(tablet)),
                        tablet->table()),
      tablet_(tablet),
      cstate_(std::move(cstate)),
      rng_(rng) {
    deadline_ = MonoTime::Max(); // Never time out.
  }

  virtual string type_name() const OVERRIDE { return "AddServer ChangeConfig"; }

  virtual string description() const OVERRIDE;

 protected:
  virtual bool SendRequest(int attempt) OVERRIDE;
  virtual void HandleResponse(int attempt) OVERRIDE;

 private:
  virtual string tablet_id() const OVERRIDE { return tablet_->tablet_id(); }

  const scoped_refptr<TabletInfo> tablet_;
  const ConsensusStatePB cstate_;

  // Used to make random choices in replica selection.
  ThreadSafeRandom* rng_;

  consensus::ChangeConfigRequestPB req_;
  consensus::ChangeConfigResponsePB resp_;
};

string AsyncAddServerTask::description() const {
  return Substitute("AddServer ChangeConfig RPC for tablet $0 "
                    "with cas_config_opid_index $1",
                    tablet_->tablet_id(),
                    cstate_.committed_config().opid_index());
}

bool AsyncAddServerTask::SendRequest(int attempt) {
  LOG(INFO) << "Sending request for AddServer on tablet " << tablet_->tablet_id()
            << " (attempt " << attempt << ")";

  // Bail if we're retrying in vain.
  int64_t latest_index;
  {
    TabletMetadataLock tablet_lock(tablet_.get(), TabletMetadataLock::READ);
    latest_index = tablet_lock.data().pb.consensus_state()
      .committed_config().opid_index();
  }
  if (latest_index > cstate_.committed_config().opid_index()) {
    LOG_WITH_PREFIX(INFO) << "Latest config for has opid_index of " << latest_index
                          << " while this task has opid_index of "
                          << cstate_.committed_config().opid_index() << ". Aborting task.";
    MarkAborted();
    return false;
  }

  // Select the replica we wish to add to the config.
  // Do not include current members of the config.
  TSDescriptorVector ts_descs;
  master_->ts_manager()->GetAllLiveDescriptors(&ts_descs);
  set<std::shared_ptr<TSDescriptor>> excluded;
  for (const auto& ts_desc : ts_descs) {
    if (IsRaftConfigMember(ts_desc->permanent_uuid(), cstate_.committed_config())) {
      InsertOrDie(&excluded, ts_desc);
    }
  }
  auto replacement_replica = SelectReplica(ts_descs, excluded, rng_);
  if (PREDICT_FALSE(!replacement_replica)) {
    KLOG_EVERY_N(WARNING, 100) << LogPrefix() << "No candidate replacement replica found "
                               << "for tablet " << tablet_->ToString();
    return false;
  }

  req_.set_dest_uuid(target_ts_desc_->permanent_uuid());
  req_.set_tablet_id(tablet_->tablet_id());
  req_.set_type(consensus::ADD_SERVER);
  req_.set_cas_config_opid_index(cstate_.committed_config().opid_index());
  RaftPeerPB* peer = req_.mutable_server();
  peer->set_permanent_uuid(replacement_replica->permanent_uuid());
  ServerRegistrationPB peer_reg;
  replacement_replica->GetRegistration(&peer_reg);
  CHECK_GT(peer_reg.rpc_addresses_size(), 0);
  *peer->mutable_last_known_addr() = peer_reg.rpc_addresses(0);
  peer->set_member_type(RaftPeerPB::VOTER);
  VLOG(1) << "Sending AddServer ChangeConfig request to "
          << target_ts_desc_->ToString() << ":\n"
          << SecureDebugString(req_);
  consensus_proxy_->ChangeConfigAsync(req_, &resp_, &rpc_,
                                      boost::bind(&AsyncAddServerTask::RpcCallback, this));
  return true;
}

void AsyncAddServerTask::HandleResponse(int attempt) {
  if (!resp_.has_error()) {
    MarkComplete();
    LOG_WITH_PREFIX(INFO) << "Config change to add server succeeded";
    return;
  }

  Status status = StatusFromPB(resp_.error().status());

  // Do not retry on a CAS error, otherwise retry forever or until cancelled.
  switch (resp_.error().code()) {
    case TabletServerErrorPB::CAS_FAILED:
      LOG_WITH_PREFIX(WARNING) << "ChangeConfig() failed with leader "\
                               << target_ts_desc_->ToString()
                               << " due to CAS failure. No further retry: "
                               << status.ToString();
      MarkFailed();
      break;
    default:
      LOG_WITH_PREFIX(INFO) << "ChangeConfig() failed with leader "
                            << target_ts_desc_->ToString()
                            << " due to error "
                            << TabletServerErrorPB::Code_Name(resp_.error().code())
                            << ". This operation will be retried. Error detail: "
                            << status.ToString();
      break;
  }
}

void CatalogManager::SendAlterTableRequest(const scoped_refptr<TableInfo>& table) {
  vector<scoped_refptr<TabletInfo> > tablets;
  table->GetAllTablets(&tablets);

  for (const scoped_refptr<TabletInfo>& tablet : tablets) {
    SendAlterTabletRequest(tablet);
  }
}

void CatalogManager::SendAlterTabletRequest(const scoped_refptr<TabletInfo>& tablet) {
  auto call = new AsyncAlterTable(master_, tablet);
  tablet->table()->AddTask(call);
  WARN_NOT_OK(call->Run(), "Failed to send alter table request");
}

void CatalogManager::SendDeleteTableRequest(const scoped_refptr<TableInfo>& table,
                                            const string& deletion_msg) {
  vector<scoped_refptr<TabletInfo> > tablets;
  table->GetAllTablets(&tablets);

  for (const scoped_refptr<TabletInfo>& tablet : tablets) {
    TabletMetadataLock l(tablet.get(), TabletMetadataLock::READ);
    SendDeleteTabletRequest(tablet, l, deletion_msg);
  }
}

void CatalogManager::SendDeleteTabletRequest(const scoped_refptr<TabletInfo>& tablet,
                                             const TabletMetadataLock& tablet_lock,
                                             const string& deletion_msg) {
  if (!tablet_lock.data().pb.has_consensus_state()) {
    // We could end up here if we're deleting a tablet that never made it to
    // the CREATING state. That would mean no replicas were ever assigned, so
    // there's nothing to delete.
    LOG(INFO) << "Not sending DeleteTablet requests; no consensus state for tablet "
              << tablet->tablet_id();
    return;
  }
  const ConsensusStatePB& cstate = tablet_lock.data().pb.consensus_state();
  LOG_WITH_PREFIX(INFO)
      << "Sending DeleteTablet for " << cstate.committed_config().peers().size()
      << " replicas of tablet " << tablet->tablet_id();
  for (const auto& peer : cstate.committed_config().peers()) {
    SendDeleteReplicaRequest(tablet->tablet_id(), TABLET_DATA_DELETED,
                             boost::none, tablet->table(),
                             peer.permanent_uuid(), deletion_msg);
  }
}

void CatalogManager::SendDeleteReplicaRequest(
    const std::string& tablet_id,
    TabletDataState delete_type,
    const boost::optional<int64_t>& cas_config_opid_index_less_or_equal,
    const scoped_refptr<TableInfo>& table,
    const string& ts_uuid,
    const string& reason) {
  AsyncDeleteReplica* call =
      new AsyncDeleteReplica(master_, ts_uuid, table,
                             tablet_id, delete_type, cas_config_opid_index_less_or_equal,
                             reason);
  if (table != nullptr) {
    table->AddTask(call);
  } else {
    // This is a floating task (since the table does not exist)
    // created as response to a tablet report.
    call->AddRef();
  }
  WARN_NOT_OK(call->Run(),
              Substitute("Failed to send DeleteReplica request for tablet $0", tablet_id));
}

void CatalogManager::SendAddServerRequest(const scoped_refptr<TabletInfo>& tablet,
                                          const ConsensusStatePB& cstate) {
  auto task = new AsyncAddServerTask(master_, tablet, cstate, &rng_);
  tablet->table()->AddTask(task);
  WARN_NOT_OK(task->Run(),
              Substitute("Failed to send AddServer request for tablet $0", tablet->tablet_id()));
  // We can't access 'task' after calling Run() because it may delete itself
  // inside Run() in the case that the tablet has no known leader.
  LOG_WITH_PREFIX(INFO)
      << "Started AddServer task for tablet " << tablet->tablet_id();
}

void CatalogManager::ExtractTabletsToProcess(
    vector<scoped_refptr<TabletInfo>>* tablets_to_process) {

  shared_lock<LockType> l(lock_);

  // TODO: At the moment we loop through all the tablets
  //       we can keep a set of tablets waiting for "assignment"
  //       or just a counter to avoid to take the lock and loop through the tablets
  //       if everything is "stable".

  // 'tablets_to_process' elements must be partially ordered in the same way as
  // table->GetAllTablets(); see the locking rules at the top of the file.
  for (const auto& table_entry : table_ids_map_) {
    scoped_refptr<TableInfo> table = table_entry.second;
    TableMetadataLock table_lock(table.get(), TableMetadataLock::READ);
    if (table_lock.data().is_deleted()) {
      continue;
    }

    vector<scoped_refptr<TabletInfo>> tablets;
    table->GetAllTablets(&tablets);
    for (const auto& tablet : tablets) {
      TabletMetadataLock tablet_lock(tablet.get(), TabletMetadataLock::READ);
      if (tablet_lock.data().is_deleted() ||
          tablet_lock.data().is_running()) {
        continue;
      }
      tablets_to_process->push_back(tablet);
    }
  }
}

// Check if it's time to roll TokenSigner's key. There's a bit of subtlety here:
// we shouldn't start exporting a key until it is properly persisted.
// So, the protocol is:
//   1) Generate a new TSK.
//   2) Try to write it to the system table.
//   3) Pass it back to the TokenSigner on success.
//   4) Check and switch TokenSigner to the new key if it's time to do so.
Status CatalogManager::TryGenerateNewTskUnlocked() {
  TokenSigner* signer = master_->token_signer();
  unique_ptr<security::TokenSigningPrivateKey> tsk;
  RETURN_NOT_OK(signer->CheckNeedKey(&tsk));
  if (tsk) {
    // First save the new TSK into the system table.
    TokenSigningPrivateKeyPB tsk_pb;
    tsk->ExportPB(&tsk_pb);
    SysTskEntryPB sys_entry;
    sys_entry.mutable_tsk()->Swap(&tsk_pb);
    MAYBE_INJECT_RANDOM_LATENCY(
        FLAGS_catalog_manager_inject_latency_prior_tsk_write_ms);
    RETURN_NOT_OK(sys_catalog_->AddTskEntry(sys_entry));
    LOG_WITH_PREFIX(INFO) << "Generated new TSK " << tsk->key_seq_num();
    // Then add the new TSK into the signer.
    RETURN_NOT_OK(signer->AddKey(std::move(tsk)));
  }
  return signer->TryRotateKey();
}

Status CatalogManager::LoadTskEntries(set<string>* expired_entry_ids) {
  TskEntryLoader loader;
  RETURN_NOT_OK(sys_catalog_->VisitTskEntries(&loader));
  for (const auto& key : loader.entries()) {
    LOG_WITH_PREFIX(INFO) << "Loaded TSK: " << key.key_seq_num();
  }
  if (expired_entry_ids) {
    set<string> ref(loader.expired_entry_ids());
    expired_entry_ids->swap(ref);
  }
  return master_->token_signer()->ImportKeys(loader.entries());
}

Status CatalogManager::DeleteTskEntries(const set<string>& entry_ids) {
  leader_lock_.AssertAcquiredForWriting();
  return sys_catalog_->RemoveTskEntries(entry_ids);
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
    SysTabletsEntryPB::CREATING, "Sending initial creation of tablet");
  deferred->tablets_to_update.push_back(tablet);
  deferred->needs_create_rpc.push_back(tablet);
  VLOG(1) << "Assign new tablet " << tablet->ToString();
}

void CatalogManager::HandleAssignCreatingTablet(TabletInfo* tablet,
                                                DeferredAssignmentActions* deferred,
                                                vector<scoped_refptr<TabletInfo> >* new_tablets) {
  MonoDelta time_since_updated =
      MonoTime::Now() - tablet->last_create_tablet_time();
  int64_t remaining_timeout_ms =
      FLAGS_tablet_creation_timeout_ms - time_since_updated.ToMilliseconds();

  // Skip the tablet if the assignment timeout is not yet expired
  if (remaining_timeout_ms > 0) {
    VLOG(2) << "Tablet " << tablet->ToString() << " still being created. "
            << remaining_timeout_ms << "ms remain until timeout.";
    return;
  }

  const PersistentTabletInfo& old_info = tablet->metadata().state();

  // The "tablet creation" was already sent, but we didn't receive an answer
  // within the timeout. So the tablet will be replaced by a new one.
  scoped_refptr<TabletInfo> replacement = CreateTabletInfo(tablet->table().get(),
                                                           old_info.pb.partition());
  LOG_WITH_PREFIX(WARNING)
      << "Tablet " << tablet->ToString() << " was not created within "
      << "the allowed timeout. Replacing with a new tablet "
      << replacement->tablet_id();

  // Mark old tablet as replaced.
  tablet->mutable_metadata()->mutable_dirty()->set_state(
    SysTabletsEntryPB::REPLACED,
    Substitute("Replaced by $0 at $1",
               replacement->tablet_id(), LocalTimeAsString()));

  // Mark new tablet as being created.
  replacement->mutable_metadata()->mutable_dirty()->set_state(
    SysTabletsEntryPB::CREATING,
    Substitute("Replacement for $0", tablet->tablet_id()));

  deferred->tablets_to_update.push_back(tablet);
  deferred->tablets_to_add.push_back(replacement.get());
  deferred->needs_create_rpc.push_back(replacement.get());
  VLOG(1) << "Replaced tablet " << tablet->tablet_id()
          << " with " << replacement->tablet_id()
          << " (Table " << tablet->table()->ToString() << ")";

  new_tablets->emplace_back(std::move(replacement));
}

// TODO: we could batch the IO onto a background thread.
//       but this is following the current HandleReportedTablet()
Status CatalogManager::HandleTabletSchemaVersionReport(TabletInfo *tablet, uint32_t version) {
  // Update the schema version if it's the latest
  tablet->set_reported_schema_version(version);

  // Verify if it's the last tablet report, and the alter completed.
  TableInfo *table = tablet->table().get();
  TableMetadataLock l(table, TableMetadataLock::WRITE);
  if (l.data().is_deleted() || l.data().pb.state() != SysTablesEntryPB::ALTERING) {
    return Status::OK();
  }

  uint32_t current_version = l.data().pb.version();
  if (table->IsAlterInProgress(current_version)) {
    return Status::OK();
  }

  // Update the state from altering to running and remove the last fully
  // applied schema (if it exists).
  l.mutable_data()->pb.clear_fully_applied_schema();
  l.mutable_data()->set_state(SysTablesEntryPB::RUNNING,
                              Substitute("Current schema version=$0", current_version));

  SysCatalogTable::Actions actions;
  actions.table_to_update = table;
  Status s = sys_catalog_->Write(actions);
  if (!s.ok()) {
    LOG_WITH_PREFIX(WARNING)
        << "An error occurred while updating sys-tables: " << s.ToString();
    return s;
  }

  l.Commit();
  LOG_WITH_PREFIX(INFO)
      << table->ToString() << " - Alter table completed version="
      << current_version;
  return Status::OK();
}

Status CatalogManager::ProcessPendingAssignments(
    const std::vector<scoped_refptr<TabletInfo> >& tablets) {
  VLOG(1) << "Processing pending assignments";

  // Take write locks on all tablets to be processed, and ensure that they are
  // unlocked at the end of this scope.
  ScopedTabletInfoCommitter unlocker_in(ScopedTabletInfoCommitter::UNLOCKED);
  unlocker_in.AddTablets(tablets);
  unlocker_in.LockTabletsForWriting();

  // Any tablets created by the helper functions will also be created in a
  // locked state, so we must ensure they are unlocked before we return to
  // avoid deadlocks.
  ScopedTabletInfoCommitter unlocker_out(ScopedTabletInfoCommitter::LOCKED);

  DeferredAssignmentActions deferred;

  // Iterate over each of the tablets and handle it, whatever state
  // it may be in. The actions required for the tablet are collected
  // into 'deferred'.
  for (const scoped_refptr<TabletInfo>& tablet : tablets) {
    SysTabletsEntryPB::State t_state = tablet->metadata().state().pb.state();

    switch (t_state) {
      case SysTabletsEntryPB::PREPARING:
        HandleAssignPreparingTablet(tablet.get(), &deferred);
        break;

      case SysTabletsEntryPB::CREATING:
      {
        vector<scoped_refptr<TabletInfo>> new_tablets;
        HandleAssignCreatingTablet(tablet.get(), &deferred, &new_tablets);
        unlocker_out.AddTablets(new_tablets);
        break;
      }
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
    return Status::OK();
  }

  // For those tablets which need to be created in this round, assign replicas.
  TSDescriptorVector ts_descs;
  master_->ts_manager()->GetAllLiveDescriptors(&ts_descs);

  Status s;
  for (TabletInfo *tablet : deferred.needs_create_rpc) {
    // NOTE: if we fail to select replicas on the first pass (due to
    // insufficient Tablet Servers being online), we will still try
    // again unless the tablet/table creation is cancelled.
    s = SelectReplicasForTablet(ts_descs, tablet);
    if (!s.ok()) {
      s = s.CloneAndPrepend(Substitute(
          "An error occured while selecting replicas for tablet $0",
          tablet->tablet_id()));
      break;
    }
  }

  // Update the sys catalog with the new set of tablets/metadata.
  if (s.ok()) {
    SysCatalogTable::Actions actions;
    actions.tablets_to_add = deferred.tablets_to_add;
    actions.tablets_to_update = deferred.tablets_to_update;
    s = sys_catalog_->Write(actions);
    if (!s.ok()) {
      s = s.CloneAndPrepend("An error occurred while persisting the updated tablet metadata");
    }
  }

  if (!s.ok()) {
    LOG_WITH_PREFIX(WARNING)
        << "Aborting the current task due to error: " << s.ToString();
    // If there was an error, abort any mutations started by the
    // current task.
    unlocker_out.Abort();
    unlocker_in.Abort();
    return s;
  }

  // Expose tablet metadata changes before the new tablets themselves.
  unlocker_out.Commit();
  unlocker_in.Commit();
  {
    std::lock_guard<LockType> l(lock_);
    for (const auto& new_tablet : unlocker_out) {
      new_tablet->table()->AddTablet(new_tablet.get());
      tablet_map_[new_tablet->tablet_id()] = new_tablet;
    }
  }

  // Send DeleteTablet requests to tablet servers serving deleted tablets.
  // This is asynchronous / non-blocking.
  for (TabletInfo* tablet : deferred.tablets_to_update) {
    TabletMetadataLock l(tablet, TabletMetadataLock::READ);
    if (l.data().is_deleted()) {
      SendDeleteTabletRequest(tablet, l, l.data().pb.state_msg());
    }
  }
  // Send the CreateTablet() requests to the servers. This is asynchronous / non-blocking.
  for (TabletInfo* tablet : deferred.needs_create_rpc) {
    TabletMetadataLock l(tablet, TabletMetadataLock::READ);
    SendCreateTabletRequest(tablet, l);
  }
  return Status::OK();
}

Status CatalogManager::SelectReplicasForTablet(const TSDescriptorVector& ts_descs,
                                               TabletInfo* tablet) {
  TableMetadataLock table_guard(tablet->table().get(), TableMetadataLock::READ);

  if (!table_guard.data().pb.IsInitialized()) {
    return Status::InvalidArgument(
        Substitute("TableInfo for tablet $0 is not initialized (aborted CreateTable attempt?)",
                   tablet->tablet_id()));
  }

  int nreplicas = table_guard.data().pb.num_replicas();

  if (ts_descs.size() < nreplicas) {
    return Status::InvalidArgument(
        Substitute("Not enough tablet servers are online for table '$0'. Need at least $1 "
                   "replicas, but only $2 tablet servers are available",
                   table_guard.data().name(), nreplicas, ts_descs.size()));
  }

  // Select the set of replicas for the tablet.
  ConsensusStatePB* cstate = tablet->mutable_metadata()->mutable_dirty()
          ->pb.mutable_consensus_state();
  cstate->set_current_term(kMinimumTerm);
  consensus::RaftConfigPB *config = cstate->mutable_committed_config();

  // Maintain ability to downgrade Kudu to a version with LocalConsensus.
  if (nreplicas == 1) {
    config->set_obsolete_local(true);
  } else {
    config->set_obsolete_local(false);
  }

  config->set_opid_index(consensus::kInvalidOpIdIndex);
  SelectReplicas(ts_descs, nreplicas, config);
  return Status::OK();
}

void CatalogManager::SendCreateTabletRequest(const scoped_refptr<TabletInfo>& tablet,
                                             const TabletMetadataLock& tablet_lock) {
  const consensus::RaftConfigPB& config =
      tablet_lock.data().pb.consensus_state().committed_config();
  tablet->set_last_create_tablet_time(MonoTime::Now());
  for (const RaftPeerPB& peer : config.peers()) {
    AsyncCreateReplica* task = new AsyncCreateReplica(master_,
                                                      peer.permanent_uuid(),
                                                      tablet, tablet_lock);
    tablet->table()->AddTask(task);
    WARN_NOT_OK(task->Run(), "Failed to send new tablet request");
  }
}

void CatalogManager::SelectReplicas(const TSDescriptorVector& ts_descs,
                                    int nreplicas,
                                    consensus::RaftConfigPB *config) {
  DCHECK_EQ(0, config->peers_size()) << "RaftConfig not empty: " << SecureShortDebugString(*config);
  DCHECK_LE(nreplicas, ts_descs.size());

  // Keep track of servers we've already selected, so that we don't attempt to
  // put two replicas on the same host.
  set<shared_ptr<TSDescriptor> > already_selected;
  for (int i = 0; i < nreplicas; ++i) {
    shared_ptr<TSDescriptor> ts = SelectReplica(ts_descs, already_selected, &rng_);
    // We must be able to find a tablet server for the replica because of
    // checks before this function is called.
    DCHECK(ts) << "ts_descs: " << ts_descs.size()
               << " already_sel: " << already_selected.size();
    InsertOrDie(&already_selected, ts);

    // Increment the number of pending replicas so that we take this selection into
    // account when assigning replicas for other tablets of the same table. This
    // value decays back to 0 over time.
    ts->IncrementRecentReplicaCreations();

    ServerRegistrationPB reg;
    ts->GetRegistration(&reg);

    RaftPeerPB *peer = config->add_peers();
    peer->set_member_type(RaftPeerPB::VOTER);
    peer->set_permanent_uuid(ts->permanent_uuid());

    // TODO: This is temporary, we will use only UUIDs
    for (const HostPortPB& addr : reg.rpc_addresses()) {
      peer->mutable_last_known_addr()->CopyFrom(addr);
    }
  }
}

Status CatalogManager::BuildLocationsForTablet(const scoped_refptr<TabletInfo>& tablet,
                                               TabletLocationsPB* locs_pb) {
  TabletMetadataLock l_tablet(tablet.get(), TabletMetadataLock::READ);
  if (PREDICT_FALSE(l_tablet.data().is_deleted())) {
    return Status::NotFound("Tablet deleted", l_tablet.data().pb.state_msg());
  }

  if (PREDICT_FALSE(!l_tablet.data().is_running())) {
    return Status::ServiceUnavailable("Tablet not running");
  }

  // Guaranteed because the tablet is RUNNING.
  DCHECK(l_tablet.data().pb.has_consensus_state());

  const ConsensusStatePB& cstate = l_tablet.data().pb.consensus_state();
  for (const consensus::RaftPeerPB& peer : cstate.committed_config().peers()) {
    // TODO(adar): GetConsensusRole() iterates over all of the peers, making this an
    // O(n^2) loop. If replication counts get high, it should be optimized.
    TabletLocationsPB_ReplicaPB* replica_pb = locs_pb->add_replicas();
    replica_pb->set_role(GetConsensusRole(peer.permanent_uuid(), cstate));

    TSInfoPB* tsinfo_pb = replica_pb->mutable_ts_info();
    tsinfo_pb->set_permanent_uuid(peer.permanent_uuid());

    shared_ptr<TSDescriptor> ts_desc;
    if (master_->ts_manager()->LookupTSByUUID(peer.permanent_uuid(), &ts_desc)) {
      ServerRegistrationPB reg;
      ts_desc->GetRegistration(&reg);
      tsinfo_pb->mutable_rpc_addresses()->Swap(reg.mutable_rpc_addresses());
    } else {
      // If we've never received a heartbeat from the tserver, we'll fall back
      // to the last known RPC address in the RaftPeerPB.
      //
      // TODO: We should track these RPC addresses in the master table itself.
      tsinfo_pb->add_rpc_addresses()->CopyFrom(peer.last_known_addr());
    }
  }

  locs_pb->mutable_partition()->CopyFrom(tablet->metadata().state().pb.partition());
  locs_pb->set_tablet_id(tablet->tablet_id());

  // No longer used; always set to false.
  locs_pb->set_deprecated_stale(false);

  return Status::OK();
}

Status CatalogManager::GetTabletLocations(const std::string& tablet_id,
                                          TabletLocationsPB* locs_pb) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  locs_pb->mutable_replicas()->Clear();
  scoped_refptr<TabletInfo> tablet_info;
  {
    shared_lock<LockType> l(lock_);
    if (!FindCopy(tablet_map_, tablet_id, &tablet_info)) {
      return Status::NotFound(Substitute("Unknown tablet $0", tablet_id));
    }
  }

  return BuildLocationsForTablet(tablet_info, locs_pb);
}

Status CatalogManager::GetTableLocations(const GetTableLocationsRequestPB* req,
                                         GetTableLocationsResponsePB* resp) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // If start-key is > end-key report an error instead of swap the two
  // since probably there is something wrong app-side.
  if (req->has_partition_key_start() && req->has_partition_key_end()
      && req->partition_key_start() > req->partition_key_end()) {
    return Status::InvalidArgument("start partition key is greater than the end partition key");
  }

  if (req->max_returned_locations() <= 0) {
    return Status::InvalidArgument("max_returned_locations must be greater than 0");
  }

  scoped_refptr<TableInfo> table;
  RETURN_NOT_OK(FindTable(req->table(), &table));
  if (table == nullptr) {
    Status s = Status::NotFound("The table does not exist");
    SetupError(resp->mutable_error(), MasterErrorPB::TABLE_NOT_FOUND, s);
    return s;
  }

  TableMetadataLock l(table.get(), TableMetadataLock::READ);
  RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(&l, resp));

  vector<scoped_refptr<TabletInfo> > tablets_in_range;
  table->GetTabletsInRange(req, &tablets_in_range);

  for (const scoped_refptr<TabletInfo>& tablet : tablets_in_range) {
    Status s = BuildLocationsForTablet(tablet, resp->add_tablet_locations());
    if (s.ok()) {
      continue;
    } else if (s.IsNotFound()) {
      // The tablet has been deleted; force the client to retry. This is a
      // transient state that only happens with a concurrent drop range
      // partition alter table operation.
      resp->Clear();
      resp->mutable_error()->set_code(MasterErrorPB_Code::MasterErrorPB_Code_TABLET_NOT_RUNNING);
      StatusToPB(Status::ServiceUnavailable("Tablet not running"),
                 resp->mutable_error()->mutable_status());
    } else if (s.IsServiceUnavailable()) {
      // The tablet is not yet running; fail the request.
      resp->Clear();
      resp->mutable_error()->set_code(MasterErrorPB_Code::MasterErrorPB_Code_TABLET_NOT_RUNNING);
      StatusToPB(s, resp->mutable_error()->mutable_status());
      break;
    } else {
      LOG_WITH_PREFIX(FATAL)
          << "Unexpected error while building tablet locations: "
          << s.ToString();
    }
  }
  resp->set_ttl_millis(FLAGS_table_locations_ttl_ms);
  return Status::OK();
}

void CatalogManager::DumpState(std::ostream* out) const {
  TableInfoMap ids_copy, names_copy;
  TabletInfoMap tablets_copy;

  // Copy the internal state so that, if the output stream blocks,
  // we don't end up holding the lock for a long time.
  {
    shared_lock<LockType> l(lock_);
    ids_copy = table_ids_map_;
    names_copy = table_names_map_;
    tablets_copy = tablet_map_;
    // TODO(aserbin): add information about root CA certs, if any
  }

  *out << "Tables:\n";
  for (const TableInfoMap::value_type& e : ids_copy) {
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
    *out << "  metadata: " << SecureShortDebugString(l.data().pb) << "\n";

    *out << "  tablets:\n";

    vector<scoped_refptr<TabletInfo> > table_tablets;
    t->GetAllTablets(&table_tablets);
    for (const scoped_refptr<TabletInfo>& tablet : table_tablets) {
      TabletMetadataLock l_tablet(tablet.get(), TabletMetadataLock::READ);
      *out << "    " << tablet->tablet_id() << ": "
           << SecureShortDebugString(l_tablet.data().pb) << "\n";

      if (tablets_copy.erase(tablet->tablet_id()) != 1) {
        *out << "  [ERROR: not present in CM tablet map!]\n";
      }
    }
  }

  if (!tablets_copy.empty()) {
    *out << "Orphaned tablets (not referenced by any table):\n";
    for (const TabletInfoMap::value_type& entry : tablets_copy) {
      const scoped_refptr<TabletInfo>& tablet = entry.second;
      TabletMetadataLock l_tablet(tablet.get(), TabletMetadataLock::READ);
      *out << "    " << tablet->tablet_id() << ": "
           << SecureShortDebugString(l_tablet.data().pb) << "\n";
    }
  }

  if (!names_copy.empty()) {
    *out << "Orphaned tables (in by-name map, but not id map):\n";
    for (const TableInfoMap::value_type& e : names_copy) {
      *out << e.second->id() << ":\n";
      *out << "  name: \"" << strings::CHexEscape(e.first) << "\"\n";
    }
  }
}

std::string CatalogManager::LogPrefix() const {
  return Substitute("T $0 P $1: ",
                    sys_catalog_->tablet_replica()->tablet_id(),
                    sys_catalog_->tablet_replica()->permanent_uuid());
}

void CatalogManager::AbortAndWaitForAllTasks(
    const vector<scoped_refptr<TableInfo>>& tables) {
  for (const auto& t : tables) {
    t->AbortTasks();
  }
  for (const auto& t : tables) {
    t->WaitTasksCompletion();
  }
}
////////////////////////////////////////////////////////////
// CatalogManager::ScopedLeaderSharedLock
////////////////////////////////////////////////////////////

CatalogManager::ScopedLeaderSharedLock::ScopedLeaderSharedLock(
    CatalogManager* catalog)
    : catalog_(DCHECK_NOTNULL(catalog)),
      leader_shared_lock_(catalog->leader_lock_, std::try_to_lock),
      catalog_status_(Status::Uninitialized("")),
      leader_status_(Status::Uninitialized("")),
      initial_term_(-1) {

  // Check if the catalog manager is running.
  std::lock_guard<simple_spinlock> l(catalog_->state_lock_);
  if (PREDICT_FALSE(catalog_->state_ != kRunning)) {
    catalog_status_ = Status::ServiceUnavailable(
        Substitute("Catalog manager is not initialized. State: $0",
                   catalog_->state_));
    return;
  }
  catalog_status_ = Status::OK();

  // Check if the catalog manager is the leader.
  const ConsensusStatePB cstate = catalog_->sys_catalog_->tablet_replica()->
      consensus()->ConsensusState();
  initial_term_ = cstate.current_term();
  const string& uuid = catalog_->master_->fs_manager()->uuid();
  if (PREDICT_FALSE(!cstate.has_leader_uuid() || cstate.leader_uuid() != uuid)) {
    leader_status_ = Status::IllegalState(
        Substitute("Not the leader. Local UUID: $0, Raft Consensus state: $1",
                   uuid, SecureShortDebugString(cstate)));
    return;
  }
  if (PREDICT_FALSE(catalog_->leader_ready_term_ != cstate.current_term() ||
                    !leader_shared_lock_.owns_lock())) {
    leader_status_ = Status::ServiceUnavailable(
        "Leader not yet ready to serve requests");
    return;
  }
  leader_status_ = Status::OK();
}

bool CatalogManager::ScopedLeaderSharedLock::has_term_changed() const {
  DCHECK(leader_status().ok());
  const ConsensusStatePB cstate = catalog_->sys_catalog_->tablet_replica()->
      consensus()->ConsensusState();
  return cstate.current_term() != initial_term_;
}

template<typename RespClass>
bool CatalogManager::ScopedLeaderSharedLock::CheckIsInitializedOrRespond(
    RespClass* resp, RpcContext* rpc) {
  if (PREDICT_FALSE(!catalog_status_.ok())) {
    StatusToPB(catalog_status_, resp->mutable_error()->mutable_status());
    resp->mutable_error()->set_code(
        MasterErrorPB::CATALOG_MANAGER_NOT_INITIALIZED);
    rpc->RespondSuccess();
    return false;
  }
  return true;
}

template<typename RespClass>
bool CatalogManager::ScopedLeaderSharedLock::CheckIsInitializedAndIsLeaderOrRespond(
    RespClass* resp, RpcContext* rpc) {
  const Status& s = first_failed_status();
  if (PREDICT_TRUE(s.ok())) {
    return true;
  }

  StatusToPB(s, resp->mutable_error()->mutable_status());
  resp->mutable_error()->set_code(MasterErrorPB::NOT_THE_LEADER);
  rpc->RespondSuccess();
  return false;
}

// Explicit specialization for callers outside this compilation unit.
#define INITTED_OR_RESPOND(RespClass) \
  template bool \
  CatalogManager::ScopedLeaderSharedLock::CheckIsInitializedOrRespond( \
      RespClass* resp, RpcContext* rpc)
#define INITTED_AND_LEADER_OR_RESPOND(RespClass) \
  template bool \
  CatalogManager::ScopedLeaderSharedLock::CheckIsInitializedAndIsLeaderOrRespond( \
      RespClass* resp, RpcContext* rpc)

INITTED_OR_RESPOND(ConnectToMasterResponsePB);
INITTED_OR_RESPOND(GetMasterRegistrationResponsePB);
INITTED_OR_RESPOND(TSHeartbeatResponsePB);
INITTED_AND_LEADER_OR_RESPOND(AlterTableResponsePB);
INITTED_AND_LEADER_OR_RESPOND(CreateTableResponsePB);
INITTED_AND_LEADER_OR_RESPOND(DeleteTableResponsePB);
INITTED_AND_LEADER_OR_RESPOND(IsAlterTableDoneResponsePB);
INITTED_AND_LEADER_OR_RESPOND(IsCreateTableDoneResponsePB);
INITTED_AND_LEADER_OR_RESPOND(ListTablesResponsePB);
INITTED_AND_LEADER_OR_RESPOND(GetTableLocationsResponsePB);
INITTED_AND_LEADER_OR_RESPOND(GetTableSchemaResponsePB);
INITTED_AND_LEADER_OR_RESPOND(GetTabletLocationsResponsePB);

#undef INITTED_OR_RESPOND
#undef INITTED_AND_LEADER_OR_RESPOND

////////////////////////////////////////////////////////////
// TabletInfo
////////////////////////////////////////////////////////////

TabletInfo::TabletInfo(const scoped_refptr<TableInfo>& table,
                       std::string tablet_id)
    : tablet_id_(std::move(tablet_id)),
      table_(table),
      last_create_tablet_time_(MonoTime::Now()),
      reported_schema_version_(0) {}

TabletInfo::~TabletInfo() {
}

void TabletInfo::set_last_create_tablet_time(const MonoTime& ts) {
  std::lock_guard<simple_spinlock> l(lock_);
  last_create_tablet_time_ = ts;
}

MonoTime TabletInfo::last_create_tablet_time() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return last_create_tablet_time_;
}

bool TabletInfo::set_reported_schema_version(uint32_t version) {
  std::lock_guard<simple_spinlock> l(lock_);
  if (version > reported_schema_version_) {
    reported_schema_version_ = version;
    return true;
  }
  return false;
}

uint32_t TabletInfo::reported_schema_version() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return reported_schema_version_;
}

std::string TabletInfo::ToString() const {
  return Substitute("$0 (table $1)", tablet_id_,
                    (table_ != nullptr ? table_->ToString() : "MISSING"));
}

void PersistentTabletInfo::set_state(SysTabletsEntryPB::State state, const string& msg) {
  pb.set_state(state);
  pb.set_state_msg(msg);
}

////////////////////////////////////////////////////////////
// TableInfo
////////////////////////////////////////////////////////////

TableInfo::TableInfo(std::string table_id) : table_id_(std::move(table_id)) {}

TableInfo::~TableInfo() {
}

std::string TableInfo::ToString() const {
  TableMetadataLock l(this, TableMetadataLock::READ);
  return Substitute("$0 [id=$1]", l.data().pb.name(), table_id_);
}

bool TableInfo::RemoveTablet(const std::string& partition_key_start) {
  std::lock_guard<rw_spinlock> l(lock_);
  return EraseKeyReturnValuePtr(&tablet_map_, partition_key_start) != nullptr;
}

void TableInfo::AddTablet(TabletInfo *tablet) {
  std::lock_guard<rw_spinlock> l(lock_);
  AddTabletUnlocked(tablet);
}

void TableInfo::AddTablets(const vector<TabletInfo*>& tablets) {
  std::lock_guard<rw_spinlock> l(lock_);
  for (TabletInfo *tablet : tablets) {
    AddTabletUnlocked(tablet);
  }
}

void TableInfo::AddRemoveTablets(const vector<scoped_refptr<TabletInfo>>& tablets_to_add,
                                 const vector<scoped_refptr<TabletInfo>>& tablets_to_drop) {
  std::lock_guard<rw_spinlock> l(lock_);
  for (const auto& tablet : tablets_to_drop) {
    const auto& lower_bound = tablet->metadata().state().pb.partition().partition_key_start();
    CHECK(EraseKeyReturnValuePtr(&tablet_map_, lower_bound) != nullptr);
  }
  for (const auto& tablet : tablets_to_add) {
    AddTabletUnlocked(tablet.get());
  }
}

void TableInfo::AddTabletUnlocked(TabletInfo* tablet) {
  TabletInfo* old = nullptr;
  if (UpdateReturnCopy(&tablet_map_,
                       tablet->metadata().state().pb.partition().partition_key_start(),
                       tablet, &old)) {
    VLOG(1) << "Replaced tablet " << old->tablet_id() << " with " << tablet->tablet_id();
    // TODO: can we assert that the replaced tablet is not in Running state?
    // May be a little tricky since we don't know whether to look at its committed or
    // uncommitted state.
  }
}

void TableInfo::GetTabletsInRange(const GetTableLocationsRequestPB* req,
                                  vector<scoped_refptr<TabletInfo> > *ret) const {
  shared_lock<rw_spinlock> l(lock_);
  int max_returned_locations = req->max_returned_locations();

  TableInfo::TabletInfoMap::const_iterator it, it_end;
  if (req->has_partition_key_start()) {
    it = tablet_map_.upper_bound(req->partition_key_start());
    if (it != tablet_map_.begin()) {
      --it;
    }
  } else {
    it = tablet_map_.begin();
  }

  if (req->has_partition_key_end()) {
    it_end = tablet_map_.upper_bound(req->partition_key_end());
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
  shared_lock<rw_spinlock> l(lock_);
  for (const TableInfo::TabletInfoMap::value_type& e : tablet_map_) {
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
  shared_lock<rw_spinlock> l(lock_);
  for (const TableInfo::TabletInfoMap::value_type& e : tablet_map_) {
    TabletMetadataLock tablet_lock(e.second, TabletMetadataLock::READ);
    if (!tablet_lock.data().is_running()) {
      return true;
    }
  }
  return false;
}

void TableInfo::AddTask(MonitoredTask* task) {
  task->AddRef();
  {
    std::lock_guard<rw_spinlock> l(lock_);
    pending_tasks_.insert(task);
  }
}

void TableInfo::RemoveTask(MonitoredTask* task) {
  {
    std::lock_guard<rw_spinlock> l(lock_);
    pending_tasks_.erase(task);
  }

  // Done outside the lock so that if Release() drops the last ref to this
  // TableInfo, RemoveTask() won't unlock a freed lock.
  task->Release();
}

void TableInfo::AbortTasks() {
  shared_lock<rw_spinlock> l(lock_);
  for (MonitoredTask* task : pending_tasks_) {
    task->Abort();
  }
}

void TableInfo::WaitTasksCompletion() {
  int wait_time = 5;
  while (1) {
    {
      shared_lock<rw_spinlock> l(lock_);
      if (pending_tasks_.empty()) {
        break;
      }
    }
    base::SleepForMilliseconds(wait_time);
    wait_time = std::min(wait_time * 5 / 4, 10000);
  }
}

void TableInfo::GetTaskList(std::vector<scoped_refptr<MonitoredTask> > *ret) {
  shared_lock<rw_spinlock> l(lock_);
  for (MonitoredTask* task : pending_tasks_) {
    ret->push_back(make_scoped_refptr(task));
  }
}

void TableInfo::GetAllTablets(vector<scoped_refptr<TabletInfo> > *ret) const {
  ret->clear();
  shared_lock<rw_spinlock> l(lock_);
  for (const auto& e : tablet_map_) {
    ret->push_back(make_scoped_refptr(e.second));
  }
}

void PersistentTableInfo::set_state(SysTablesEntryPB::State state, const string& msg) {
  pb.set_state(state);
  pb.set_state_msg(msg);
}

} // namespace master
} // namespace kudu
