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
#include <ctime>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/common.h>

#include "kudu/cfile/type_encodings.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/consensus.proxy.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/escaping.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/gutil/utf/utf.h"
#include "kudu/gutil/walltime.h"
#include "kudu/hms/hms_catalog.h"
#include "kudu/master/authz_provider.h"
#include "kudu/master/auto_rebalancer.h"
#include "kudu/master/default_authz_provider.h"
#include "kudu/master/hms_notification_log_listener.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master_cert_authority.h"
#include "kudu/master/placement_policy.h"
#include "kudu/master/ranger_authz_provider.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/master/table_metrics.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/master/ts_manager.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/security/cert.h"
#include "kudu/security/crypto.h"
#include "kudu/security/openssl_util.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_signer.h"
#include "kudu/security/token_signing_key.h"
#include "kudu/security/token_verifier.h"
#include "kudu/server/monitored_task.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/transactions/transaction_tracker.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_admin.proxy.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"
#include "kudu/util/thread_restrictions.h"
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

DEFINE_int32(max_column_comment_length, 256,
             "Maximum length of the comment of a column");

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

DEFINE_int32(max_create_tablets_per_ts, 60,
             "The number of tablet replicas per TS that can be requested for a "
             "new table. If 0, no limit is enforced.");
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
TAG_FLAG(table_locations_ttl_ms, runtime);

DEFINE_bool(catalog_manager_fail_ts_rpcs, false,
            "Whether all master->TS async calls should fail. Only for testing!");
TAG_FLAG(catalog_manager_fail_ts_rpcs, hidden);
TAG_FLAG(catalog_manager_fail_ts_rpcs, runtime);

DEFINE_int32(catalog_manager_inject_latency_load_ca_info_ms, 0,
             "Injects a random sleep between 0 and this many milliseconds "
             "while reading CA info from the system table. "
             "This is a test-only flag, do not use in production.");
TAG_FLAG(catalog_manager_inject_latency_load_ca_info_ms, hidden);
TAG_FLAG(catalog_manager_inject_latency_load_ca_info_ms, runtime);
TAG_FLAG(catalog_manager_inject_latency_load_ca_info_ms, unsafe);

DEFINE_int32(catalog_manager_inject_latency_prior_tsk_write_ms, 0,
             "Injects a random sleep between 0 and this many milliseconds "
             "prior to writing newly generated TSK into the system table. "
             "This is a test-only flag, do not use in production.");
TAG_FLAG(catalog_manager_inject_latency_prior_tsk_write_ms, hidden);
TAG_FLAG(catalog_manager_inject_latency_prior_tsk_write_ms, unsafe);

DEFINE_bool(catalog_manager_evict_excess_replicas, true,
            "Whether catalog manager evicts excess replicas from tablet "
            "configuration based on replication factor.");
TAG_FLAG(catalog_manager_evict_excess_replicas, hidden);
TAG_FLAG(catalog_manager_evict_excess_replicas, runtime);

DEFINE_int32(catalog_manager_inject_latency_list_authz_ms, 0,
             "Injects a sleep in milliseconds while authorizing a ListTables "
             "request. This is a test-only flag.");
TAG_FLAG(catalog_manager_inject_latency_list_authz_ms, hidden);
TAG_FLAG(catalog_manager_inject_latency_list_authz_ms, unsafe);

DEFINE_bool(mock_table_metrics_for_testing, false,
            "Whether to enable mock table metrics for testing.");
TAG_FLAG(mock_table_metrics_for_testing, hidden);
TAG_FLAG(mock_table_metrics_for_testing, runtime);

DEFINE_bool(catalog_manager_support_on_disk_size, true,
            "Whether to enable mock on disk size statistic for tables. For testing only.");
TAG_FLAG(catalog_manager_support_on_disk_size, hidden);
TAG_FLAG(catalog_manager_support_on_disk_size, runtime);

DEFINE_bool(catalog_manager_support_live_row_count, true,
            "Whether to enable mock live row count statistic for tables. For testing only.");
TAG_FLAG(catalog_manager_support_live_row_count, hidden);
TAG_FLAG(catalog_manager_support_live_row_count, runtime);

DEFINE_bool(catalog_manager_enable_chunked_tablet_reports, true,
            "Whether to split the tablet report data received from one tablet "
            "server into chunks when persisting it in the system catalog. "
            "The chunking starts at around the maximum allowed RPC size "
            "controlled by the --rpc_max_message_size flag. When the chunking "
            "is disabled, a tablet report sent by a tablet server is rejected "
            "if it would result in an oversized update on the system catalog "
            "tablet. With the default settings for --rpc_max_message_size, "
            "the latter can happen only in case of extremely high number "
            "of tablet replicas per tablet server.");
TAG_FLAG(catalog_manager_enable_chunked_tablet_reports, advanced);
TAG_FLAG(catalog_manager_enable_chunked_tablet_reports, runtime);

DEFINE_int64(on_disk_size_for_testing, 0,
             "Mock the on disk size of metrics for testing.");
TAG_FLAG(on_disk_size_for_testing, hidden);
TAG_FLAG(on_disk_size_for_testing, runtime);

DEFINE_int64(live_row_count_for_testing, 0,
             "Mock the live row count of metrics for testing.");
TAG_FLAG(live_row_count_for_testing, hidden);
TAG_FLAG(live_row_count_for_testing, runtime);

DEFINE_bool(auto_rebalancing_enabled, false,
            "Whether auto-rebalancing is enabled.");
TAG_FLAG(auto_rebalancing_enabled, advanced);
TAG_FLAG(auto_rebalancing_enabled, experimental);

DECLARE_bool(raft_prepare_replacement_before_eviction);
DECLARE_int64(tsk_rotation_seconds);

METRIC_DEFINE_entity(table);

DECLARE_string(ranger_config_path);

// Validates that if auto-rebalancing is enabled, the cluster uses 3-4-3 replication
// (the --raft_prepare_replacement_before_eviction flag must be set to true).
static bool Validate343SchemeEnabledForAutoRebalancing()  {
  if (FLAGS_auto_rebalancing_enabled &&
      !FLAGS_raft_prepare_replacement_before_eviction) {
    LOG(ERROR) << "If enabling auto-rebalancing, Kudu must be configured"
                  " with --raft_prepare_replacement_before_eviction.";
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(auto_rebalancing_flags,
                     Validate343SchemeEnabledForAutoRebalancing);

using base::subtle::NoBarrier_CompareAndSwap;
using base::subtle::NoBarrier_Load;
using boost::make_optional;
using boost::none;
using boost::optional;
using google::protobuf::Map;
using kudu::cfile::TypeEncodingInfo;
using kudu::consensus::ConsensusServiceProxy;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::IsRaftConfigMember;
using kudu::consensus::RaftConfigPB;
using kudu::consensus::RaftConsensus;
using kudu::consensus::RaftPeerPB;
using kudu::consensus::StartTabletCopyRequestPB;
using kudu::consensus::kMinimumTerm;
using kudu::hms::HmsClientVerifyKuduSyncConfig;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::RpcContext;
using kudu::security::Cert;
using kudu::security::DataFormat;
using kudu::security::PrivateKey;
using kudu::security::TablePrivilegePB;
using kudu::security::TokenSigner;
using kudu::security::TokenSigningPrivateKey;
using kudu::security::TokenSigningPrivateKeyPB;
using kudu::security::TokenSigningPublicKeyPB;
using kudu::tablet::ReportedTabletStatsPB;
using kudu::tablet::TABLET_DATA_DELETED;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tablet::TabletDataState;
using kudu::tablet::TabletReplica;
using kudu::tablet::TabletStatePB;
using kudu::tserver::TabletServerErrorPB;
using std::accumulate;
using std::inserter;
using std::map;
using std::pair;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

////////////////////////////////////////////////////////////
// Table Loader
////////////////////////////////////////////////////////////

class TableLoader : public TableVisitor {
 public:
  explicit TableLoader(CatalogManager *catalog_manager)
    : catalog_manager_(catalog_manager) {
  }

  Status VisitTable(const string& table_id,
                    const SysTablesEntryPB& metadata) override {
    CHECK(!ContainsKey(catalog_manager_->table_ids_map_, table_id))
          << "Table already exists: " << table_id;

    // Set up the table info.
    scoped_refptr<TableInfo> table = new TableInfo(table_id);
    TableMetadataLock l(table.get(), LockMode::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);

    // Add the tablet to the IDs map and to the name map (if the table is not deleted).
    bool is_deleted = l.mutable_data()->is_deleted();
    catalog_manager_->table_ids_map_[table->id()] = table;
    if (!is_deleted) {
      auto* existing = InsertOrReturnExisting(&catalog_manager_->normalized_table_names_map_,
                                              CatalogManager::NormalizeTableName(l.data().name()),
                                              table);
      if (existing) {
        // Return an HMS-specific error message, since this error currently only
        // occurs when the HMS is enabled.
        return Status::IllegalState(
            "when the Hive Metastore integration is enabled, Kudu table names must not differ "
            "only by case; restart the master(s) with the Hive Metastore integration disabled and "
            "rename one of the conflicting tables",
            Substitute("$0 or $1 [id=$2]", (*existing)->ToString(), l.data().name(), table_id));
      }
    }
    l.Commit();

    if (!is_deleted) {
      // It's unnecessary to register metrics for the deleted tables.
      table->RegisterMetrics(catalog_manager_->master_->metric_registry(),
          CatalogManager::NormalizeTableName(metadata.name()));
      LOG(INFO) << Substitute("Loaded metadata for table $0", table->ToString());
    }
    VLOG(2) << Substitute("Metadata for table $0: $1",
                          table->ToString(), SecureShortDebugString(metadata));
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

  Status VisitTablet(const string& table_id,
                     const string& tablet_id,
                     const SysTabletsEntryPB& metadata) override {
    // Lookup the table.
    scoped_refptr<TableInfo> table(FindPtrOrNull(
        catalog_manager_->table_ids_map_, table_id));
    if (table == nullptr) {
      // Tables and tablets are always created/deleted in one operation, so
      // this shouldn't be possible.
      string msg = Substitute("Missing table $0 required by tablet $1 (metadata: $2)",
                              table_id, tablet_id, SecureDebugString(metadata));
      LOG(ERROR) << msg;
      return Status::Corruption(msg);
    }

    // Set up the tablet info.
    scoped_refptr<TabletInfo> tablet = new TabletInfo(table, tablet_id);
    TabletMetadataLock l(tablet.get(), LockMode::WRITE);
    l.mutable_data()->pb.CopyFrom(metadata);

    // Add the tablet to the tablet manager.
    catalog_manager_->tablet_map_[tablet->id()] = tablet;

    // Add the tablet to the table.
    bool is_deleted = l.mutable_data()->is_deleted();
    l.Commit();
    if (!is_deleted) {
      // Need to use a new tablet lock here because AddRemoveTablets() reads
      // from clean state, which is uninitialized for these brand new tablets.
      TabletMetadataLock l(tablet.get(), LockMode::READ);
      table->AddRemoveTablets({ tablet }, {});
      LOG(INFO) << Substitute("Loaded metadata for tablet $0 (table $1)",
                              tablet_id, table->ToString());
    }

    VLOG(2) << Substitute("Metadata for tablet $0: $1",
                          tablet_id, SecureShortDebugString(metadata));
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

  Status Init() WARN_UNUSED_RESULT;
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
      cond_.WaitFor(MonoDelta::FromMilliseconds(msec));
    }
    pending_updates_ = false;
  }

 private:
  void Run();

  Atomic32 closing_;
  bool pending_updates_;
  mutable Mutex lock_;
  ConditionVariable cond_;
  scoped_refptr<kudu::Thread> thread_;
  CatalogManager *catalog_manager_;
};

Status CatalogManagerBgTasks::Init() {
  RETURN_NOT_OK(kudu::Thread::Create("catalog manager", "bgtasks",
                                     [this]() { this->Run(); }, &thread_));
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
  MonoTime last_tspk_run;
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
        vector<scoped_refptr<TabletInfo>> to_process;
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
      } else if (l.owns_lock()) {
        // This is the case of a follower catalog manager running as a part
        // of master process. To be able to authenticate connecting clients
        // using their authn tokens, a follower master needs:
        //  * CA-signed server certificate to authenticate itself to a
        //    connecting client (otherwise the client wont try to use its token)
        //  * public parts of active TSK keys to verify token signature
        Status s = catalog_manager_->PrepareFollower(&last_tspk_run);
        if (!s.ok()) {
          LOG(WARNING) << s.ToString()
                       << ": failed to prepare follower catalog manager, will retry";
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

string RequestorString(RpcContext* rpc) {
  if (rpc) {
    return rpc->requestor_string();
  } else {
    return "internal request";
  }
}

// If 's' is not OK, fills in the RPC response with the error and provided code. Returns 's'.
template<typename RespClass>
Status SetupError(Status s, RespClass* resp, MasterErrorPB::Code code) {
  if (PREDICT_FALSE(!s.ok())) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
    resp->mutable_error()->set_code(code);
  }
  return s;
}

// If 's' indicates that the node is no longer the leader, setup
// Service::UnavailableError as the error, set NOT_THE_LEADER as the
// error code and return true.
template<class RespClass>
void CheckIfNoLongerLeaderAndSetupError(const Status& s, RespClass* resp) {
  // TODO (KUDU-591): This is a bit of a hack, as right now
  // there's no way to propagate why a write to a consensus configuration has
  // failed. However, since we use Status::IllegalState()/IsAborted() to
  // indicate the situation where a write was issued on a node
  // that is no longer the leader, this suffices until we
  // distinguish this cause of write failure more explicitly.
  if (s.IsIllegalState() || s.IsAborted()) {
    SetupError(Status::ServiceUnavailable(
          "operation requested can only be executed on a leader master, but this"
          " master is no longer the leader", s.ToString()),
        resp, MasterErrorPB::NOT_THE_LEADER);
  }
}

template<class RespClass>
Status CheckIfTableDeletedOrNotRunning(TableMetadataLock* lock, RespClass* resp) {
  if (lock->data().is_deleted()) {
    return SetupError(Status::NotFound(
          Substitute("table $0 was deleted", lock->data().name()),
          lock->data().pb.state_msg()),
        resp, MasterErrorPB::TABLE_NOT_FOUND);
  }
  if (!lock->data().is_running()) {
    return SetupError(Status::ServiceUnavailable(
          Substitute("table $0 is not running", lock->data().name())),
        resp, MasterErrorPB::TABLE_NOT_FOUND);
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

CatalogManager::CatalogManager(Master* master)
    : master_(master),
      rng_(GetRandomSeed32()),
      state_(kConstructed),
      leader_ready_term_(-1),
      hms_notification_log_event_id_(-1),
      leader_lock_(RWMutex::Priority::PREFER_WRITING) {
  if (RangerAuthzProvider::IsEnabled()) {
    authz_provider_.reset(new RangerAuthzProvider(master_->fs_manager()->env(),
                                                  master_->metric_entity()));
  } else {
    authz_provider_.reset(new DefaultAuthzProvider);
  }
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

  if (FLAGS_auto_rebalancing_enabled) {
    unique_ptr<AutoRebalancerTask> task(
        new AutoRebalancerTask(this, master_->ts_manager()));
    RETURN_NOT_OK_PREPEND(task->Init(), "failed to initialize auto-rebalancing task");
    auto_rebalancer_ = std::move(task);
  }

  RETURN_NOT_OK(master_->GetMasterHostPorts(&master_addresses_));
  if (hms::HmsCatalog::IsEnabled()) {
    string master_addresses = JoinMapped(
      master_addresses_,
      [] (const HostPort& hostport) {
        return Substitute("$0:$1", hostport.host(), hostport.port());
      },
      ",");

    // The leader_lock_ isn't really intended for this (it's for serializing
    // new leadership initialization against regular catalog manager operations)
    // but we need to use something to protect this hms_catalog_ write vis a vis
    // the read in PrepareForLeadershipTask(), and that read is performed while
    // holding leader_lock_, so this is the path of least resistance.
    std::lock_guard<RWMutex> leader_lock_guard(leader_lock_);

    hms_catalog_.reset(new hms::HmsCatalog(std::move(master_addresses)));
    RETURN_NOT_OK_PREPEND(hms_catalog_->Start(HmsClientVerifyKuduSyncConfig::VERIFY),
                          "failed to start Hive Metastore catalog");

    hms_notification_log_listener_.reset(new HmsNotificationLogListenerTask(this));
    RETURN_NOT_OK_PREPEND(hms_notification_log_listener_->Init(),
        "failed to initialize Hive Metastore notification log listener task");
  }

  RETURN_NOT_OK_PREPEND(authz_provider_->Start(), "failed to start Authz Provider");

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
  return leader_election_pool_->Submit([this]() { this->PrepareForLeadershipTask(); });
}

Status CatalogManager::WaitUntilCaughtUpAsLeader(const MonoDelta& timeout) {
  ConsensusStatePB cstate;
  RETURN_NOT_OK(sys_catalog_->tablet_replica()->consensus()->ConsensusState(&cstate));
  const string& uuid = master_->fs_manager()->uuid();
  if (cstate.leader_uuid() != uuid) {
    return Status::IllegalState(
        Substitute("Node $0 not leader. Raft Consensus state: $1",
                    uuid, SecureShortDebugString(cstate)));
  }

  // Wait for all ops to be committed.
  RETURN_NOT_OK(sys_catalog_->tablet_replica()->op_tracker()->WaitForAllToFinish(timeout));
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
    RETURN_NOT_OK(MasterCertAuthority::Generate(key.get(), cert.get()));
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

  leader_lock_.AssertAcquired();

  auto* ca = master_->cert_authority();
  RETURN_NOT_OK_PREPEND(ca->Init(std::move(key), std::move(cert)),
                        "could not init master CA");
  auto* tls = master_->mutable_tls_context();
  RETURN_NOT_OK_PREPEND(tls->AddTrustedCertificate(ca->ca_cert()),
                        "could not trust master CA cert");
  // If we haven't signed our own server cert yet, do so.
  optional<security::CertSignRequest> csr = tls->GetCsrIfNecessary();
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
  leader_lock_.AssertAcquired();

  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_catalog_manager_inject_latency_load_ca_info_ms);

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
  if (!expired_tsk_entry_ids.empty()) {
    return DeleteTskEntries(expired_tsk_entry_ids);
  }
  return Status::OK();
}

void CatalogManager::PrepareForLeadershipTask() {
  {
    // Hack to block this function until InitSysCatalogAsync() is finished.
    shared_lock<LockType> l(lock_);
  }
  const RaftConsensus* consensus = sys_catalog_->tablet_replica()->consensus();
  const int64_t term_before_wait = consensus->CurrentTerm();
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

  const int64_t term = consensus->CurrentTerm();
  if (term_before_wait != term) {
    // If we got elected leader again while waiting to catch up then we will
    // get another callback to visit the tables and tablets, so bail.
    LOG(INFO) << Substitute("Term changed from $0 to $1 while waiting for "
        "master leader catchup. Not loading sys catalog metadata",
        term_before_wait, term);
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
          LOG(INFO) << Substitute("$0 failed due to the shutdown of the catalog: $1",
                                  op_description, s.ToString());
          return s;
        }
      }

      const int64_t term = consensus.CurrentTerm();
      if (term != start_term) {
        // If the term has changed we assume the new leader catalog is about
        // to do the necessary work in its leadership preparation task.
        LOG(INFO) << Substitute("$0 failed; change in term detected: $1 vs $2: $3",
                                op_description, start_term, term, s.ToString());
        return s;
      }

      // In all other cases non-OK status is considered fatal.
      LOG(FATAL) << Substitute("$0 failed: $1", op_description, s.ToString());
      return s; // unreachable
    };

    // Block new catalog operations, and wait for existing operations to finish.
    std::lock_guard<RWMutex> leader_lock_guard(leader_lock_);

    static const char* const kLoadMetaOpDescription =
        "Loading table and tablet metadata into memory";
    LOG(INFO) << kLoadMetaOpDescription << "...";
    LOG_SLOW_EXECUTION(WARNING, 1000, LogPrefix() + kLoadMetaOpDescription) {
      if (!check([this]() { return this->VisitTablesAndTabletsUnlocked(); },
                 *consensus, term, kLoadMetaOpDescription).ok()) {
        return;
      }
    }

    // TODO(KUDU-1920): update this once "BYO PKI" feature is supported.
    static const char* const kCaInitOpDescription =
        "Initializing Kudu internal certificate authority";
    LOG(INFO) << kCaInitOpDescription << "...";
    LOG_SLOW_EXECUTION(WARNING, 1000, LogPrefix() + kCaInitOpDescription) {
      if (!check([this]() { return this->InitCertAuthority(); },
                 *consensus, term, kCaInitOpDescription).ok()) {
        return;
      }
    }

    static const char* const kTskOpDescription = "Loading token signing keys";
    LOG(INFO) << kTskOpDescription << "...";
    LOG_SLOW_EXECUTION(WARNING, 1000, LogPrefix() + kTskOpDescription) {
      if (!check([this]() { return this->InitTokenSigner(); },
                 *consensus, term, kTskOpDescription).ok()) {
        return;
      }
    }

    static const char* const kTServerStatesDescription =
        "Initializing in-progress tserver states";
    LOG(INFO) << kTServerStatesDescription << "...";
    LOG_SLOW_EXECUTION(WARNING, 1000, LogPrefix() + kTServerStatesDescription) {
      if (!check([this]() {
            return this->master_->ts_manager()->ReloadTServerStates(this->sys_catalog_.get());
          },
          *consensus, term, kTServerStatesDescription).ok()) {
        return;
      }
    }

    if (hms_catalog_) {
      static const char* const kNotificationLogEventIdDescription =
          "Loading latest processed Hive Metastore notification log event ID";
      LOG(INFO) << kNotificationLogEventIdDescription << "...";
      LOG_SLOW_EXECUTION(WARNING, 1000, LogPrefix() + kNotificationLogEventIdDescription) {
      if (!check([this]() { return this->InitLatestNotificationLogEventId(); },
                 *consensus, term, kNotificationLogEventIdDescription).ok()) {
          return;
        }
      }
    }
  }

  std::lock_guard<simple_spinlock> l(state_lock_);
  leader_ready_term_ = term;
}

Status CatalogManager::PrepareFollowerCaInfo() {
  static const char* const kDescription =
      "acquiring CA information for follower catalog manager";

  // Load the CA certificate and CA private key.
  unique_ptr<PrivateKey> key;
  unique_ptr<Cert> cert;
  Status s = LoadCertAuthorityInfo(&key, &cert).AndThen([&] {
    return InitCertAuthorityWith(std::move(key), std::move(cert));
  });
  if (s.ok()) {
    LOG_WITH_PREFIX(INFO) << kDescription << ": success";
  } else {
    LOG_WITH_PREFIX(WARNING) << kDescription << ": " << s.ToString();
  }
  return s;
}

Status CatalogManager::PrepareFollowerTokenVerifier() {
  static const char* const kDescription =
      "importing token verification keys for follower catalog manager";

  // Load public parts of the existing TSKs.
  vector<TokenSigningPublicKeyPB> keys;
  const Status s = LoadTspkEntries(&keys).AndThen([&] {
    return master_->messenger()->shared_token_verifier()->ImportKeys(keys);
  });
  if (!s.ok()) {
    LOG_WITH_PREFIX(WARNING) << kDescription << ": " << s.ToString();
    return s;
  }

  if (keys.empty()) {
    // In case if no keys are found in the system table it's necessary to retry.
    // Returning non-OK will lead the upper-level logic to call this method
    // again as soon as possible.
    return Status::NotFound("no TSK found in the system table");
  }

  LOG_WITH_PREFIX(INFO) << kDescription
                        << ": success; most recent TSK sequence number "
                        << keys.back().key_seq_num();
  return Status::OK();
}

Status CatalogManager::PrepareFollower(MonoTime* last_tspk_run) {
  leader_lock_.AssertAcquiredForReading();
  // Load the CA certificate and CA private key.
  if (!master_->tls_context().has_signed_cert()) {
    RETURN_NOT_OK(PrepareFollowerCaInfo());
  }
  // Import keys for authn token verification. A new TSK appear every
  // tsk_rotation_seconds, so using 1/2 of that interval to avoid edge cases.
  const auto tsk_rotation_interval =
      MonoDelta::FromSeconds(FLAGS_tsk_rotation_seconds / 2.0);
  const auto now = MonoTime::Now();
  if (!last_tspk_run->Initialized() || *last_tspk_run + tsk_rotation_interval < now) {
    RETURN_NOT_OK(PrepareFollowerTokenVerifier());
    *last_tspk_run = now;
  }
  return Status::OK();
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
  normalized_table_names_map_.clear();
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
  unique_ptr<SysCatalogTable> new_catalog(new SysCatalogTable(
      master_, [this]() { return this->ElectedAsLeaderCb(); }));
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

  if (authz_provider_) {
    authz_provider_->Stop();
  }

  if (hms_catalog_) {
    hms_notification_log_listener_->Shutdown();
    hms_catalog_->Stop();
  }

  if (auto_rebalancer_) {
    auto_rebalancer_->Shutdown();
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

Status CatalogManager::CheckOnline() const {
  if (PREDICT_FALSE(!IsInitialized())) {
    return Status::ServiceUnavailable("CatalogManager is not running");
  }
  return Status::OK();
}

namespace {

Status ValidateLengthAndUTF8(const string& id, int32_t max_length) {
  // Id should not exceed the maximum allowed length.
  if (id.length() > max_length) {
    return Status::InvalidArgument(Substitute(
        "identifier '$0' longer than maximum permitted length $1",
        id, FLAGS_max_identifier_length));
  }

  // Id should be valid UTF8.
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

// Validate a table or column name to ensure that it is a valid identifier.
Status ValidateIdentifier(const string& id) {
  if (id.empty()) {
    return Status::InvalidArgument("empty string not a valid identifier");
  }

  return ValidateLengthAndUTF8(id, FLAGS_max_identifier_length);
}

// Validate a column comment to ensure that it is a valid identifier.
Status ValidateCommentIdentifier(const string& id) {
  if (id.empty()) {
    return Status::OK();
  }

  return ValidateLengthAndUTF8(id, FLAGS_max_column_comment_length);
}

// Validate the client-provided schema and name.
Status ValidateClientSchema(const optional<string>& name,
                            const Schema& schema) {
  if (name) {
    RETURN_NOT_OK_PREPEND(ValidateIdentifier(name.get()), "invalid table name");
  }
  for (int i = 0; i < schema.num_columns(); i++) {
    RETURN_NOT_OK_PREPEND(ValidateIdentifier(schema.column(i).name()),
                          "invalid column name");
    RETURN_NOT_OK_PREPEND(ValidateCommentIdentifier(schema.column(i).comment()),
                          "invalid column comment");
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

  for (int i = 0; i < schema.num_columns(); i++) {
    const auto& col = schema.column(i);
    const auto* ti = col.type_info();

    // Prohibit the creation of virtual columns.
    if (ti->is_virtual()) {
      return Status::InvalidArgument(Substitute(
          "may not create virtual column of type '$0' (column '$1')",
          ti->name(), col.name()));
    }

    // Check that the encodings are valid for the specified types.
    const TypeEncodingInfo *dummy;
    Status s = TypeEncodingInfo::Get(ti, col.attributes().encoding, &dummy);
    if (!s.ok()) {
      return s.CloneAndPrepend(Substitute("invalid encoding for column '$0'", col.name()));
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
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // Copy the request, so we can fill in some defaults.
  CreateTableRequestPB req = *orig_req;
  LOG(INFO) << Substitute("Servicing CreateTable request from $0:\n$1",
                          RequestorString(rpc), SecureDebugString(req));

  // Do some fix-up of any defaults specified on columns.
  // Clients are only expected to pass the default value in the 'read_default'
  // field, but we need to write the schema to disk including the default
  // as both the 'read' and 'write' default. It's easier to do this fix-up
  // on the protobuf here.
  for (int i = 0; i < req.schema().columns_size(); i++) {
    auto* col = req.mutable_schema()->mutable_columns(i);
    RETURN_NOT_OK(SetupError(ProcessColumnPBDefaults(col), resp, MasterErrorPB::INVALID_SCHEMA));
  }

  // a. Validate the user request.
  const string& normalized_table_name = NormalizeTableName(req.name());
  if (rpc) {
    const string& user = rpc->remote_user().username();
    const string& owner = req.has_owner() ? req.owner() : user;
    RETURN_NOT_OK(SetupError(
        authz_provider_->AuthorizeCreateTable(normalized_table_name, user, owner),
        resp, MasterErrorPB::NOT_AUTHORIZED));
  }

  // If the HMS integration is enabled, wait for the notification log listener
  // to catch up. This reduces the likelihood of attempting to create a table
  // with a name that conflicts with a table that has just been deleted or
  // renamed in the HMS.
  RETURN_NOT_OK(WaitForNotificationLogListenerCatchUp(resp, rpc));

  Schema client_schema;
  RETURN_NOT_OK(SchemaFromPB(req.schema(), &client_schema));

  RETURN_NOT_OK(SetupError(ValidateClientSchema(normalized_table_name, client_schema),
                           resp, MasterErrorPB::INVALID_SCHEMA));
  if (client_schema.has_column_ids()) {
    return SetupError(Status::InvalidArgument("user requests should not have Column IDs"),
                      resp, MasterErrorPB::INVALID_SCHEMA);
  }
  Schema schema = client_schema.CopyWithColumnIds();

  // If the client did not set a partition schema in the create table request,
  // the default partition schema (no hash bucket components and a range
  // partitioned on the primary key columns) will be used.
  PartitionSchema partition_schema;
  RETURN_NOT_OK(SetupError(
        PartitionSchema::FromPB(req.partition_schema(), schema, &partition_schema),
        resp, MasterErrorPB::INVALID_SCHEMA));

  // Decode split rows.
  vector<KuduPartialRow> split_rows;
  vector<pair<KuduPartialRow, KuduPartialRow>> range_bounds;

  RowOperationsPBDecoder decoder(req.mutable_split_rows_range_bounds(),
                                 &client_schema, &schema, nullptr);
  vector<DecodedRowOperation> ops;
  RETURN_NOT_OK(decoder.DecodeOperations<DecoderMode::SPLIT_ROWS>(&ops));

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
          return SetupError(
              Status::InvalidArgument("missing upper range bound in create table request"),
              resp, MasterErrorPB::UNKNOWN_ERROR);
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

  const auto num_replicas = req.num_replicas();
  // Reject create table with even replication factors, unless master flag
  // allow_unsafe_replication_factor is on.
  if (num_replicas % 2 == 0 && !FLAGS_allow_unsafe_replication_factor) {
    return SetupError(Status::InvalidArgument(
        Substitute("illegal replication factor $0 (replication factor must be odd)", num_replicas)),
      resp, MasterErrorPB::EVEN_REPLICATION_FACTOR);
  }

  if (num_replicas > FLAGS_max_num_replicas) {
    return SetupError(Status::InvalidArgument(
          Substitute("illegal replication factor $0 (max replication factor is $1)",
            num_replicas, FLAGS_max_num_replicas)),
        resp, MasterErrorPB::REPLICATION_FACTOR_TOO_HIGH);
  }
  if (num_replicas <= 0) {
    return SetupError(Status::InvalidArgument(
          Substitute("illegal replication factor $0 (replication factor must be positive)",
            num_replicas, FLAGS_max_num_replicas)),
        resp, MasterErrorPB::ILLEGAL_REPLICATION_FACTOR);
  }

  // Verify that the number of replicas isn't larger than the number of live tablet
  // servers.
  TSDescriptorVector ts_descs;
  master_->ts_manager()->GetDescriptorsAvailableForPlacement(&ts_descs);
  const auto num_live_tservers = ts_descs.size();
  if (FLAGS_catalog_manager_check_ts_count_for_create_table && num_replicas > num_live_tservers) {
    // Note: this error message is matched against in master-stress-test.
    return SetupError(Status::InvalidArgument(Substitute(
            "not enough live tablet servers to create a table with the requested replication "
            "factor $0; $1 tablet servers are alive", req.num_replicas(), num_live_tservers)),
        resp, MasterErrorPB::REPLICATION_FACTOR_TOO_HIGH);
  }

  // Verify that the total number of replicas is reasonable.
  //
  // Table creation can generate a fair amount of load, both in the form of RPC
  // traffic (due to Raft leader elections) and disk I/O (due to durably writing
  // several files during both replica creation and leader elections).
  //
  // Ideally we would have more effective ways of mitigating this load (such
  // as more efficient on-disk metadata management), but in lieu of that, we
  // employ this coarse-grained check that prohibits up-front creation of too
  // many replicas.
  //
  // Note: non-replicated tables are exempt because, by not using replication,
  // they do not generate much of the load described above.
  const auto max_replicas_total = FLAGS_max_create_tablets_per_ts * num_live_tservers;
  if (num_replicas > 1 &&
      max_replicas_total > 0 &&
      partitions.size() * num_replicas > max_replicas_total) {
    return SetupError(Status::InvalidArgument(Substitute(
        "the requested number of tablet replicas is over the maximum permitted "
        "at creation time ($0), additional tablets may be added by adding "
        "range partitions to the table post-creation", max_replicas_total)),
                      resp, MasterErrorPB::TOO_MANY_TABLETS);
  }

  // Warn if the number of live tablet servers is not enough to re-replicate
  // a failed replica of the tablet.
  const auto num_ts_needed_for_rereplication =
      num_replicas + (FLAGS_raft_prepare_replacement_before_eviction ? 1 : 0);
  if (num_replicas > 1 && num_ts_needed_for_rereplication > num_live_tservers) {
    LOG(WARNING) << Substitute(
        "The number of live tablet servers is not enough to re-replicate a "
        "tablet replica of the newly created table $0 in case of a server "
        "failure: $1 tablet servers would be needed, $2 are available. "
        "Consider bringing up more tablet servers.",
        normalized_table_name, num_ts_needed_for_rereplication,
        num_live_tservers);
  }

  // Verify the table's extra configuration properties.
  TableExtraConfigPB extra_config_pb;
  RETURN_NOT_OK(ExtraConfigPBFromPBMap(req.extra_configs(), &extra_config_pb));

  scoped_refptr<TableInfo> table;
  {
    std::lock_guard<LockType> l(lock_);
    TRACE("Acquired catalog manager lock");

    // b. Verify that the table does not exist.
    table = FindPtrOrNull(normalized_table_names_map_, normalized_table_name);
    if (table != nullptr) {
      return SetupError(Status::AlreadyPresent(Substitute(
              "table $0 already exists with id $1", normalized_table_name, table->id())),
          resp, MasterErrorPB::TABLE_ALREADY_PRESENT);
    }

    // c. Reserve the table name if possible.
    if (!InsertIfNotPresent(&reserved_normalized_table_names_, normalized_table_name)) {
      // ServiceUnavailable will cause the client to retry the create table
      // request. We don't want to outright fail the request with
      // 'AlreadyPresent', because a table name reservation can be rolled back
      // in the case of an error. Instead, we force the client to retry at a
      // later time.
      return SetupError(Status::ServiceUnavailable(Substitute(
              "new table name $0 is already reserved", normalized_table_name)),
          resp, MasterErrorPB::TABLE_ALREADY_PRESENT);
    }
  }

  // Ensure that we drop the name reservation upon return.
  SCOPED_CLEANUP({
    std::lock_guard<LockType> l(lock_);
    CHECK_EQ(1, reserved_normalized_table_names_.erase(normalized_table_name));
  });

  // d. Create the in-memory representation of the new table and its tablets.
  //    It's not yet in any global maps; that will happen in step g below.
  table = CreateTableInfo(req, schema, partition_schema, std::move(extra_config_pb));
  vector<scoped_refptr<TabletInfo>> tablets;
  auto abort_mutations = MakeScopedCleanup([&table, &tablets]() {
    table->mutable_metadata()->AbortMutation();
    for (const auto& e : tablets) {
      e->mutable_metadata()->AbortMutation();
    }
  });
  optional<string> dimension_label =
      req.has_dimension_label() ? make_optional<string>(req.dimension_label()) : none;
  for (const Partition& partition : partitions) {
    PartitionPB partition_pb;
    partition.ToPB(&partition_pb);
    tablets.emplace_back(CreateTabletInfo(table, partition_pb, dimension_label));
  }
  TRACE("Created new table and tablet info");

  // NOTE: the table and tablets are already locked for write at this point,
  // since the CreateTableInfo/CreateTabletInfo functions leave them in that state.
  // They will get committed at the end of this function.
  // Sanity check: the tables and tablets should all be in "preparing" state.
  CHECK_EQ(SysTablesEntryPB::PREPARING, table->metadata().dirty().pb.state());
  for (const auto& tablet : tablets) {
    CHECK_EQ(SysTabletsEntryPB::PREPARING, tablet->metadata().dirty().pb.state());
  }
  table->mutable_metadata()->mutable_dirty()->pb.set_state(SysTablesEntryPB::RUNNING);

  // e. Create the table in the HMS.
  //
  // It is critical that this step happen before writing the table to the sys catalog,
  // since this step validates that the table name is available in the HMS catalog.
  if (hms_catalog_) {
    CHECK(rpc);
    const string& owner = req.has_owner() ? req.owner() : rpc->remote_user().username();
    Status s = hms_catalog_->CreateTable(table->id(), normalized_table_name, owner, schema);
    if (!s.ok()) {
      s = s.CloneAndPrepend(Substitute("an error occurred while creating table $0 in the HMS",
                                       normalized_table_name));
      LOG(WARNING) << s.ToString();
      return SetupError(std::move(s), resp, MasterErrorPB::HIVE_METASTORE_ERROR);
    }
    TRACE("Created new table in HMS catalog");
  }
  // Delete the new HMS entry if we exit early.
  auto abort_hms = MakeScopedCleanup([&] {
      // TODO(dan): figure out how to test this.
      if (hms_catalog_) {
        TRACE("Rolling back HMS table creation");
        WARN_NOT_OK(hms_catalog_->DropTable(table->id(), normalized_table_name),
                    "an error occurred while attempting to delete orphaned HMS table entry");
      }
  });

  // f. Write table and tablets to sys-catalog.
  {
    SysCatalogTable::Actions actions;
    actions.table_to_add = table;
    actions.tablets_to_add = tablets;
    Status s = sys_catalog_->Write(std::move(actions));
    if (PREDICT_FALSE(!s.ok())) {
      s = s.CloneAndPrepend("an error occurred while writing to the sys-catalog");
      LOG(WARNING) << s.ToString();
      CheckIfNoLongerLeaderAndSetupError(s, resp);
      return s;
    }
  }
  TRACE("Wrote table and tablets to system table");

  // g. Commit the in-memory state.
  abort_hms.cancel();
  table->mutable_metadata()->CommitMutation();

  for (const auto& tablet : tablets) {
    tablet->mutable_metadata()->CommitMutation();
  }
  abort_mutations.cancel();

  // h. Add the tablets to the table.
  //
  // We can't reuse the above WRITE tablet locks for this because
  // AddRemoveTablets() will read from the clean state, which is empty for
  // these brand new tablets.
  for (const auto& tablet : tablets) {
    tablet->metadata().ReadLock();
  }
  table->AddRemoveTablets(tablets, {});
  for (const auto& tablet : tablets) {
    tablet->metadata().ReadUnlock();
  }

  // i. Make the new table and tablets visible in the catalog.
  {
    std::lock_guard<LockType> l(lock_);

    table_ids_map_[table->id()] = table;
    normalized_table_names_map_[normalized_table_name] = table;
    for (const auto& tablet : tablets) {
      InsertOrDie(&tablet_map_, tablet->id(), tablet);
    }
  }
  TRACE("Inserted table and tablets into CatalogManager maps");

  resp->set_table_id(table->id());
  VLOG(1) << "Created table " << table->ToString();
  background_tasks_->Wake();
  return Status::OK();
}

Status CatalogManager::IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                                         IsCreateTableDoneResponsePB* resp,
                                         optional<const string&> user) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // 1. Lookup the table, verify if it exists, and then check that
  //    the user is authorized to operate on the table.
  scoped_refptr<TableInfo> table;
  TableMetadataLock l;
  auto authz_func = [&] (const string& username, const string& table_name) {
    return SetupError(authz_provider_->AuthorizeGetTableMetadata(table_name, username),
                      resp, MasterErrorPB::NOT_AUTHORIZED);
  };
  RETURN_NOT_OK(FindLockAndAuthorizeTable(*req, resp, LockMode::READ, authz_func, user,
                                          &table, &l));
  RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(&l, resp));

  // 2. Verify if the create is in-progress
  TRACE("Verify if the table creation is in progress for $0", table->ToString());
  resp->set_done(!table->IsCreateInProgress());

  return Status::OK();
}

scoped_refptr<TableInfo> CatalogManager::CreateTableInfo(
    const CreateTableRequestPB& req,
    const Schema& schema,
    const PartitionSchema& partition_schema,
    TableExtraConfigPB extra_config_pb) {
  DCHECK(schema.has_column_ids());
  scoped_refptr<TableInfo> table = new TableInfo(GenerateId());
  table->mutable_metadata()->StartMutation();
  SysTablesEntryPB *metadata = &table->mutable_metadata()->mutable_dirty()->pb;
  metadata->set_state(SysTablesEntryPB::PREPARING);
  metadata->set_name(NormalizeTableName(req.name()));
  metadata->set_version(0);
  metadata->set_next_column_id(ColumnId(schema.max_col_id() + 1));
  metadata->set_num_replicas(req.num_replicas());
  // Use the Schema object passed in, since it has the column IDs already assigned,
  // whereas the user request PB does not.
  CHECK_OK(SchemaToPB(schema, metadata->mutable_schema()));
  partition_schema.ToPB(metadata->mutable_partition_schema());
  metadata->set_create_timestamp(time(nullptr));
  (*metadata->mutable_extra_config()) = std::move(extra_config_pb);
  table->RegisterMetrics(master_->metric_registry(), metadata->name());
  return table;
}

scoped_refptr<TabletInfo> CatalogManager::CreateTabletInfo(
    const scoped_refptr<TableInfo>& table,
    const PartitionPB& partition,
    const optional<string>& dimension_label) {
  scoped_refptr<TabletInfo> tablet(new TabletInfo(table, GenerateId()));
  tablet->mutable_metadata()->StartMutation();
  SysTabletsEntryPB* metadata = &tablet->mutable_metadata()->mutable_dirty()->pb;
  metadata->set_state(SysTabletsEntryPB::PREPARING);
  metadata->mutable_partition()->CopyFrom(partition);
  metadata->set_table_id(table->id());
  if (dimension_label) {
    metadata->set_dimension_label(*dimension_label);
  }
  return tablet;
}

template<typename ReqClass, typename RespClass, typename F>
Status CatalogManager::FindLockAndAuthorizeTable(
    const ReqClass& request,
    RespClass* response,
    LockMode lock_mode,
    F authz_func,
    optional<const string&> user,
    scoped_refptr<TableInfo>* table_info,
    TableMetadataLock* table_lock) {
  TRACE("Looking up, locking, and authorizing table");
  const TableIdentifierPB& table_identifier = request.table();

  // For authorization, depends on whether the request contains table ID/name,
  // below is the name of the table to validate against.
  // *----------------*--------------*---------------------*-----------------*
  // | HAS TABLE NAME | HAS TABLE ID | TABLE NAME/ID MATCH | AUTHZ NAME      |
  // *----------------*--------------*---------------------*-----------------*
  // | YES            | YES          | YES                 | TABLE NAME      |
  // *----------------*--------------*---------------------*-----------------*
  // | YES            | YES          | NO                  | TABLE NAME/ID   |
  // *----------------*--------------*---------------------*-----------------*
  // | YES            | NO           | N/A                 | TABLE NAME      |
  // *----------------*--------------*---------------------*-----------------*
  // | NO             | YES          | N/A                 | TABLE ID        |
  // *----------------*--------------*---------------------*-----------------*
  // | NO             | NO           | N/A                 | InvalidArgument |
  // *----------------*--------------*---------------------*-----------------*
  auto authorize = [&] (const string& name) {
    if (user) {
      return authz_func(*user, name);
    }
    return Status::OK();
  };

  auto tnf_error = [&] {
    return SetupError(
        Status::NotFound("the table does not exist", SecureShortDebugString(table_identifier)),
        response, MasterErrorPB::TABLE_NOT_FOUND);
  };

  scoped_refptr<TableInfo> table;
  // Set to true if the client-provided table name and ID refer to different tables.
  bool mismatched_table = false;
  {
    shared_lock<LockType> l(lock_);
    if (table_identifier.has_table_id()) {
      table = FindPtrOrNull(table_ids_map_, table_identifier.table_id());

      // If the request contains both a table ID and table name, ensure that
      // both match the same table.
      if (table_identifier.has_table_name() &&
          table.get() != FindPtrOrNull(normalized_table_names_map_,
                                       NormalizeTableName(table_identifier.table_name())).get()) {
        mismatched_table = true;
      }
    } else if (table_identifier.has_table_name()) {
      table = FindPtrOrNull(normalized_table_names_map_,
                            NormalizeTableName(table_identifier.table_name()));
    } else {
      return SetupError(Status::InvalidArgument("missing table ID or table name"),
                        response, MasterErrorPB::UNKNOWN_ERROR);
    }
  }

  // If the table doesn't exist, don't attempt to lock it.
  //
  // If the request contains table name and the user is authorized to operate
  // on the table, then return TABLE_NOT_FOUND error. Otherwise, return
  // NOT_AUTHORIZED error, to avoid leaking table existence.
  if (!table) {
    if (table_identifier.has_table_name()) {
      RETURN_NOT_OK(authorize(NormalizeTableName(table_identifier.table_name())));
    }
    return tnf_error();
  }

  // Acquire the table lock. And validate if the operation on the table
  // found is authorized.
  TableMetadataLock lock(table.get(), lock_mode);
  string table_name = NormalizeTableName(lock.data().name());
  RETURN_NOT_OK(authorize(table_name));

  // If the table name and table ID refer to different tables, for example,
  //   1. the ID maps to table A.
  //   2. the name maps to table B.
  //
  // Authorize user against both tables, then return TABLE_NOT_FOUND error to
  // avoid leaking table existence.
  if (mismatched_table) {
    RETURN_NOT_OK(authorize(NormalizeTableName(table_identifier.table_name())));
    return SetupError(
        Status::NotFound(
            Substitute("the table ID refers to a different table '$0' than '$1'",
                       table_name, table_identifier.table_name()),
            SecureShortDebugString(table_identifier)),
        response, MasterErrorPB::TABLE_NOT_FOUND);
  }

  if (table_identifier.has_table_name() &&
      NormalizeTableName(table_identifier.table_name()) != table_name) {
    // We've encountered the table while it's in the process of being renamed;
    // pretend it doesn't yet exist.
    return tnf_error();
  }

  *table_info = std::move(table);
  *table_lock = std::move(lock);
  return Status::OK();
}

Status CatalogManager::DeleteTableRpc(const DeleteTableRequestPB& req,
                                      DeleteTableResponsePB* resp,
                                      rpc::RpcContext* rpc) {
  LOG(INFO) << Substitute("Servicing DeleteTable request from $0:\n$1",
                          RequestorString(rpc), SecureShortDebugString(req));

  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  optional<const string&> user = rpc ?
      make_optional<const string&>(rpc->remote_user().username()) : none;

  // If the HMS integration is enabled and the table should be deleted in the HMS,
  // then don't directly remove the table from the Kudu catalog. Instead, delete
  // the table from the HMS and wait for the notification log listener to apply
  // the corresponding event to the catalog. By 'serializing' the drop through
  // the HMS, race conditions are avoided.
  if (hms_catalog_ && req.modify_external_catalogs()) {
    // Wait for the notification log listener to catch up. This reduces the
    // likelihood of attempting to delete a table which has just been deleted or
    // renamed in the HMS.
    RETURN_NOT_OK(WaitForNotificationLogListenerCatchUp(resp, rpc));

    // Look up the table, lock it and then check that the user is authorized
    // to operate on the table.
    scoped_refptr<TableInfo> table;
    TableMetadataLock l;
    auto authz_func = [&](const string& username, const string& table_name) {
      return SetupError(authz_provider_->AuthorizeDropTable(table_name, username),
                        resp, MasterErrorPB::NOT_AUTHORIZED);
    };
    RETURN_NOT_OK(FindLockAndAuthorizeTable(req, resp, LockMode::READ, authz_func, user,
                                            &table, &l));
    if (l.data().is_deleted()) {
      return SetupError(Status::NotFound("the table was deleted", l.data().pb.state_msg()),
          resp, MasterErrorPB::TABLE_NOT_FOUND);
    }

    // Drop the table from the HMS.
    RETURN_NOT_OK(SetupError(
          hms_catalog_->DropTable(table->id(), l.data().name()),
          resp, MasterErrorPB::HIVE_METASTORE_ERROR));

    // Unlock the table, and wait for the notification log listener to handle
    // the delete table event.
    l.Unlock();
    return WaitForNotificationLogListenerCatchUp(resp, rpc);
  }

  // If the HMS integration isn't enabled or the deletion should only happen in Kudu,
  // then delete the table directly from the Kudu catalog.
  return DeleteTable(req, resp, /*hms_notification_log_event_id=*/none, user);
}

Status CatalogManager::DeleteTableHms(const string& table_name,
                                      const string& table_id,
                                      int64_t notification_log_event_id) {
  LOG(INFO) << "Deleting table " << table_name
            << " [id=" << table_id
            << "] in response to Hive Metastore notification log event "
            << notification_log_event_id;

  DeleteTableRequestPB req;
  DeleteTableResponsePB resp;
  req.mutable_table()->set_table_name(table_name);
  req.mutable_table()->set_table_id(table_id);

  // Use empty user to skip the authorization validation since the operation
  // originates from catalog manager. Moreover, this avoids duplicate effort,
  // because we already perform authorization before making any changes to the HMS.
  RETURN_NOT_OK(DeleteTable(req, &resp, notification_log_event_id, /*user=*/none));

  // Update the cached HMS notification log event ID, if it changed.
  DCHECK_GT(notification_log_event_id, hms_notification_log_event_id_);
  hms_notification_log_event_id_ = notification_log_event_id;

  return Status::OK();
}

Status CatalogManager::DeleteTable(const DeleteTableRequestPB& req,
                                   DeleteTableResponsePB* resp,
                                   optional<int64_t> hms_notification_log_event_id,
                                   optional<const string&> user) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // 1. Look up the table, lock it, and then check that the user is authorized
  //    to operate on the table. Last, mark it as removed.
  scoped_refptr<TableInfo> table;
  TableMetadataLock l;
  auto authz_func = [&] (const string& username, const string& table_name) {
    return SetupError(authz_provider_->AuthorizeDropTable(table_name, username),
                      resp, MasterErrorPB::NOT_AUTHORIZED);
  };
  RETURN_NOT_OK(FindLockAndAuthorizeTable(req, resp, LockMode::WRITE, authz_func, user,
                                          &table, &l));
  if (l.data().is_deleted()) {
    return SetupError(Status::NotFound("the table was deleted", l.data().pb.state_msg()),
        resp, MasterErrorPB::TABLE_NOT_FOUND);
  }

  TRACE("Modifying in-memory table state");
  string deletion_msg = "Table deleted at " + LocalTimeAsString();
  l.mutable_data()->set_state(SysTablesEntryPB::REMOVED, deletion_msg);

  // 2. Look up the tablets, lock them, and mark them as deleted.
  {
    TRACE("Locking tablets");
    vector<scoped_refptr<TabletInfo>> tablets;
    TabletMetadataGroupLock lock(LockMode::RELEASED);
    table->GetAllTablets(&tablets);
    lock.AddMutableInfos(tablets);
    lock.Lock(LockMode::WRITE);

    for (const auto& t : tablets) {
      t->mutable_metadata()->mutable_dirty()->set_state(
          SysTabletsEntryPB::DELETED, deletion_msg);
    }

    // 3. Update sys-catalog with the removed table and tablet state.
    TRACE("Removing table and tablets from system table");
    {
      SysCatalogTable::Actions actions;
      actions.hms_notification_log_event_id =
          std::move(hms_notification_log_event_id);
      actions.table_to_update = table;
      actions.tablets_to_update.assign(tablets.begin(), tablets.end());
      Status s = sys_catalog_->Write(std::move(actions));
      if (PREDICT_FALSE(!s.ok())) {
        s = s.CloneAndPrepend("an error occurred while updating the sys-catalog");
        LOG(WARNING) << s.ToString();
        CheckIfNoLongerLeaderAndSetupError(s, resp);
        return s;
      }
    }

    // 4. Remove the table from the by-name map.
    {
      TRACE("Removing table from by-name map");
      std::lock_guard<LockType> l_map(lock_);
      if (normalized_table_names_map_.erase(NormalizeTableName(l.data().name())) != 1) {
        LOG(FATAL) << "Could not remove table " << table->ToString()
                   << " from map in response to DeleteTable request: "
                   << SecureShortDebugString(req);
      }
      table->UnregisterMetrics();
    }

    // 5. Commit the dirty tablet state.
    lock.Commit();
  }

  // 6. Commit the dirty table state.
  TRACE("Committing in-memory state");
  l.Commit();

  // 7. Abort any extant tasks belonging to the table.
  TRACE("Aborting table tasks");
  table->AbortTasks();

  // 8. Send a DeleteTablet() request to each tablet replica in the table.
  SendDeleteTableRequest(table, deletion_msg);

  VLOG(1) << "Deleted table " << table->ToString();
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
        boost::optional<ColumnSchema> new_col;
        RETURN_NOT_OK(ColumnSchemaFromPB(new_col_pb, &new_col));
        if (!new_col->is_nullable() && !new_col->has_read_default()) {
          return Status::InvalidArgument(
              Substitute("column `$0`: NOT NULL columns must have a default", new_col->name()));
        }

        RETURN_NOT_OK(builder.AddColumn(*new_col, false));
        break;
      }

      case AlterTableRequestPB::DROP_COLUMN: {
        if (!step.has_drop_column()) {
          return Status::InvalidArgument("DROP_COLUMN missing column info");
        }

        if (builder.is_key_column(step.drop_column().name())) {
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
    const scoped_refptr<TableInfo>& table,
    const Schema& client_schema,
    vector<AlterTableRequestPB::Step> steps,
    vector<scoped_refptr<TabletInfo>>* tablets_to_add,
    vector<scoped_refptr<TabletInfo>>* tablets_to_drop) {

  Schema schema;
  RETURN_NOT_OK(SchemaFromPB(l.data().pb.schema(), &schema));
  PartitionSchema partition_schema;
  RETURN_NOT_OK(PartitionSchema::FromPB(l.data().pb.partition_schema(), schema, &partition_schema));

  TableInfo::TabletInfoMap existing_tablets = table->tablet_map();
  TableInfo::TabletInfoMap new_tablets;
  auto abort_mutations = MakeScopedCleanup([&new_tablets]() {
    for (const auto& e : new_tablets) {
      e.second->mutable_metadata()->AbortMutation();
    }
  });

  for (const auto& step : steps) {
    vector<DecodedRowOperation> ops;
    if (step.type() == AlterTableRequestPB::ADD_RANGE_PARTITION) {
      RowOperationsPBDecoder decoder(&step.add_range_partition().range_bounds(),
                                     &client_schema, &schema, nullptr);
      RETURN_NOT_OK(decoder.DecodeOperations<DecoderMode::SPLIT_ROWS>(&ops));
    } else {
      CHECK_EQ(step.type(), AlterTableRequestPB::DROP_RANGE_PARTITION);
      RowOperationsPBDecoder decoder(&step.drop_range_partition().range_bounds(),
                                     &client_schema, &schema, nullptr);
      RETURN_NOT_OK(decoder.DecodeOperations<DecoderMode::SPLIT_ROWS>(&ops));
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
            TabletMetadataLock metadata(existing_iter->second.get(), LockMode::READ);
            if (upper_bound.empty() ||
                metadata.data().pb.partition().partition_key_start() < upper_bound) {
              return Status::InvalidArgument(
                  "New range partition conflicts with existing range partition",
                  partition_schema.RangePartitionDebugString(*ops[0].split_row, *ops[1].split_row));
            }
          }
          if (existing_iter != existing_tablets.begin()) {
            TabletMetadataLock metadata(std::prev(existing_iter)->second.get(),
                                        LockMode::READ);
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
          optional<string> dimension_label = step.add_range_partition().has_dimension_label() ?
              make_optional<string>(step.add_range_partition().dimension_label()) : none;
          new_tablets.emplace(lower_bound,
                              CreateTabletInfo(table, partition_pb, dimension_label));
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
            TabletMetadataLock metadata(existing_iter->second.get(), LockMode::READ);
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
            new_iter->second->mutable_metadata()->AbortMutation();
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
  abort_mutations.cancel();
  return Status::OK();
}

Status CatalogManager::AlterTableRpc(const AlterTableRequestPB& req,
                                     AlterTableResponsePB* resp,
                                     rpc::RpcContext* rpc) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // If the HMS integration is enabled, wait for the notification log listener
  // to catch up. This reduces the likelihood of attempting to apply an
  // alteration to a table which has just been renamed or deleted through the HMS.
  RETURN_NOT_OK(WaitForNotificationLogListenerCatchUp(resp, rpc));

  LOG(INFO) << Substitute("Servicing AlterTable request from $0:\n$1",
                          RequestorString(rpc), SecureShortDebugString(req));

  optional<const string&> user = rpc ?
      make_optional<const string&>(rpc->remote_user().username()) : none;

  // If the HMS integration is enabled, the alteration includes a table
  // rename and the table should be altered in the HMS, then don't directly
  // rename the table in the Kudu catalog. Instead, rename the table
  // in the HMS and wait for the notification log listener to apply
  // that event to the catalog. By 'serializing' the rename through the
  // HMS, race conditions are avoided.
  if (hms_catalog_ && req.has_new_table_name() && req.modify_external_catalogs()) {
    // Look up the table, lock it and then check that the user is authorized
    // to operate on the table.
    scoped_refptr<TableInfo> table;
    TableMetadataLock l;
    string normalized_new_table_name = NormalizeTableName(req.new_table_name());
    auto authz_func = [&](const string& username,
                          const string& table_name) {
      return SetupError(authz_provider_->AuthorizeAlterTable(table_name,
                                                             normalized_new_table_name,
                                                             username),
                        resp, MasterErrorPB::NOT_AUTHORIZED);
    };
    RETURN_NOT_OK(FindLockAndAuthorizeTable(req, resp, LockMode::READ, authz_func, user,
                                            &table, &l));
    RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(&l, resp));

    // The HMS allows renaming a table to the same name (ALTER TABLE t RENAME TO t),
    // however Kudu does not, so we must enforce this constraint ourselves before
    // altering the table in the HMS. The comparison is on the non-normalized
    // table names, since we want to allow changing the case of a table name.
    if (l.data().name() == normalized_new_table_name) {
      return SetupError(
          Status::AlreadyPresent(Substitute("table $0 already exists with id $1",
              normalized_new_table_name, table->id())),
          resp, MasterErrorPB::TABLE_ALREADY_PRESENT);
    }

    Schema schema;
    RETURN_NOT_OK(SchemaFromPB(l.data().pb.schema(), &schema));

    // Rename the table in the HMS.
    RETURN_NOT_OK(SetupError(hms_catalog_->AlterTable(
            table->id(), l.data().name(), normalized_new_table_name,
            schema),
        resp, MasterErrorPB::HIVE_METASTORE_ERROR));

    // Unlock the table, and wait for the notification log listener to handle
    // the alter table event.
    l.Unlock();
    RETURN_NOT_OK(WaitForNotificationLogListenerCatchUp(resp, rpc));

    // Finally, apply the remaining schema and partitioning alterations to the
    // local catalog. Since Kudu holds the canonical version of table schemas
    // and partitions the HMS is not updated first.
    //
    // Note that we pass empty user to AlterTable() to skip the authorization
    // validation since we already perform authorization before making any
    // changes to the HMS. Moreover, even though a table renaming could happen
    // before the remaining schema and partitioning alterations taking place,
    // it is ideal from the users' point of view, to not authorize against the
    // new table name arose from other RPCs.
    AlterTableRequestPB r(req);
    r.mutable_table()->clear_table_name();
    r.mutable_table()->set_table_id(table->id());
    r.clear_new_table_name();

    return AlterTable(r, resp,
                      /*hms_notification_log_event_id=*/none,
                      /*user=*/none);
  }

  return AlterTable(req, resp, /*hms_notification_log_event_id=*/ none, user);
}

Status CatalogManager::RenameTableHms(const string& table_id,
                                      const string& table_name,
                                      const string& new_table_name,
                                      int64_t notification_log_event_id) {
  AlterTableRequestPB req;
  AlterTableResponsePB resp;
  req.mutable_table()->set_table_id(table_id);
  req.mutable_table()->set_table_name(table_name);
  req.set_new_table_name(new_table_name);

  // Use empty user to skip the authorization validation since the operation
  // originates from catalog manager. Moreover, this avoids duplicate effort,
  // because we already perform authorization before making any changes to the HMS.
  RETURN_NOT_OK(AlterTable(req, &resp, notification_log_event_id, /*user=*/none));

  // Update the cached HMS notification log event ID.
  DCHECK_GT(notification_log_event_id, hms_notification_log_event_id_);
  hms_notification_log_event_id_ = notification_log_event_id;

  return Status::OK();
}

Status CatalogManager::AlterTable(const AlterTableRequestPB& req,
                                  AlterTableResponsePB* resp,
                                  optional<int64_t> hms_notification_log_event_id,
                                  optional<const string&> user) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // 1. Group the steps into schema altering steps and partition altering steps.
  vector<AlterTableRequestPB::Step> alter_schema_steps;
  vector<AlterTableRequestPB::Step> alter_partitioning_steps;
  for (const auto& step : req.alter_schema_steps()) {
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

  // 2. Lookup the table, verify if it exists, lock it for modification, and then
  //    checks that the user is authorized to operate on the table.
  scoped_refptr<TableInfo> table;
  TableMetadataLock l;
  auto authz_func = [&] (const string& username,
                         const string& table_name) {
    const string new_table = req.has_new_table_name() ?
        NormalizeTableName(req.new_table_name()) : table_name;
    return SetupError(authz_provider_->AuthorizeAlterTable(table_name, new_table, username),
                      resp, MasterErrorPB::NOT_AUTHORIZED);
  };
  RETURN_NOT_OK(FindLockAndAuthorizeTable(req, resp, LockMode::WRITE, authz_func, user,
                                          &table, &l));
  if (l.data().is_deleted()) {
    return SetupError(
        Status::NotFound("the table was deleted", l.data().pb.state_msg()),
        resp, MasterErrorPB::TABLE_NOT_FOUND);
  }
  l.mutable_data()->pb.set_alter_timestamp(time(nullptr));

  string normalized_table_name = NormalizeTableName(l.data().name());
  *resp->mutable_table_id() = table->id();

  // 3. Calculate and validate new schema for the on-disk state, not persisted yet.
  Schema new_schema;
  ColumnId next_col_id = ColumnId(l.data().pb.next_column_id());

  // Apply the alter steps. Note that there may be no steps, in which case this
  // is essentialy a no-op. It's still important to execute because
  // ApplyAlterSchemaSteps populates 'new_schema', which is used below.
  TRACE("Apply alter schema");
  RETURN_NOT_OK(SetupError(
        ApplyAlterSchemaSteps(l.data().pb, alter_schema_steps, &new_schema, &next_col_id),
        resp, MasterErrorPB::INVALID_SCHEMA));

  DCHECK_NE(next_col_id, 0);
  DCHECK_EQ(new_schema.find_column_by_id(next_col_id),
            static_cast<int>(Schema::kColumnNotFound));

  // Just validate the schema, not the name (validated below).
  RETURN_NOT_OK(SetupError(
        ValidateClientSchema(none, new_schema),
        resp, MasterErrorPB::INVALID_SCHEMA));

  // 4. Validate and try to acquire the new table name.
  string normalized_new_table_name = NormalizeTableName(req.new_table_name());
  if (req.has_new_table_name()) {

    // Validate the new table name.
    RETURN_NOT_OK(SetupError(
          ValidateIdentifier(req.new_table_name()).CloneAndPrepend("invalid table name"),
          resp, MasterErrorPB::INVALID_SCHEMA));

    std::lock_guard<LockType> catalog_lock(lock_);
    TRACE("Acquired catalog manager lock");

    // Verify that a table does not already exist with the new name. This
    // also disallows no-op renames (ALTER TABLE a RENAME TO a).
    //
    // Special case: if this is a rename of a table from a non-normalized to
    // normalized name (ALTER TABLE A RENAME to a), then allow it.
    scoped_refptr<TableInfo> other_table = FindPtrOrNull(normalized_table_names_map_,
                                                         normalized_new_table_name);
    if (other_table &&
        !(table.get() == other_table.get() && l.data().name() != normalized_new_table_name)) {
      return SetupError(
          Status::AlreadyPresent(Substitute("table $0 already exists with id $1",
              normalized_new_table_name, other_table->id())),
          resp, MasterErrorPB::TABLE_ALREADY_PRESENT);
    }

    // Reserve the new table name if possible.
    if (!InsertIfNotPresent(&reserved_normalized_table_names_, normalized_new_table_name)) {
      // ServiceUnavailable will cause the client to retry the create table
      // request. We don't want to outright fail the request with
      // 'AlreadyPresent', because a table name reservation can be rolled back
      // in the case of an error. Instead, we force the client to retry at a
      // later time.
      return SetupError(Status::ServiceUnavailable(Substitute(
              "table name $0 is already reserved", normalized_new_table_name)),
          resp, MasterErrorPB::TABLE_ALREADY_PRESENT);
    }

    l.mutable_data()->pb.set_name(normalized_new_table_name);
  }

  // Ensure that we drop our reservation upon return.
  SCOPED_CLEANUP({
    if (req.has_new_table_name()) {
      std::lock_guard<LockType> l(lock_);
      CHECK_EQ(1, reserved_normalized_table_names_.erase(normalized_new_table_name));
    }
  });

  // 5. Alter table partitioning.
  vector<scoped_refptr<TabletInfo>> tablets_to_add;
  vector<scoped_refptr<TabletInfo>> tablets_to_drop;
  if (!alter_partitioning_steps.empty()) {
    TRACE("Apply alter partitioning");
    Schema client_schema;
    RETURN_NOT_OK(SetupError(SchemaFromPB(req.schema(), &client_schema),
          resp, MasterErrorPB::UNKNOWN_ERROR));
    RETURN_NOT_OK(SetupError(
          ApplyAlterPartitioningSteps(l, table, client_schema, alter_partitioning_steps,
            &tablets_to_add, &tablets_to_drop),
          resp, MasterErrorPB::UNKNOWN_ERROR));
  }

  // 6. Alter table's extra configuration properties.
  if (!req.new_extra_configs().empty()) {
    TRACE("Apply alter extra-config");
    Map<string, string> new_extra_configs;
    RETURN_NOT_OK(ExtraConfigPBToPBMap(l.data().pb.extra_config(),
                                       &new_extra_configs));
    // Merge table's extra configuration properties.
    for (auto config : req.new_extra_configs()) {
      new_extra_configs[config.first] = config.second;
    }
    RETURN_NOT_OK(ExtraConfigPBFromPBMap(new_extra_configs,
                                         l.mutable_data()->pb.mutable_extra_config()));
  }

  // Set to true if columns are altered, added or dropped.
  bool has_schema_changes = !alter_schema_steps.empty();
  // Set to true if there are schema changes, or the table is renamed.
  bool has_metadata_changes =
      has_schema_changes || req.has_new_table_name() || !req.new_extra_configs().empty();
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

  const string deletion_msg = "Partition dropped at " + LocalTimeAsString();
  TabletMetadataGroupLock tablets_to_add_lock(LockMode::WRITE);
  TabletMetadataGroupLock tablets_to_drop_lock(LockMode::RELEASED);

  // 8. Update sys-catalog with the new table schema and tablets to add/drop.
  TRACE("Updating metadata on disk");
  {
    SysCatalogTable::Actions actions;
    actions.hms_notification_log_event_id =
        std::move(hms_notification_log_event_id);
    if (!tablets_to_add.empty() || has_metadata_changes) {
      // If anything modified the table's persistent metadata, then sync it to the sys catalog.
      actions.table_to_update = table;
    }
    actions.tablets_to_add = tablets_to_add;

    tablets_to_add_lock.AddMutableInfos(tablets_to_add);
    tablets_to_drop_lock.AddMutableInfos(tablets_to_drop);
    tablets_to_drop_lock.Lock(LockMode::WRITE);
    for (auto& tablet : tablets_to_drop) {
      tablet->mutable_metadata()->mutable_dirty()->set_state(
          SysTabletsEntryPB::DELETED, deletion_msg);
    }
    actions.tablets_to_update = tablets_to_drop;

    Status s = sys_catalog_->Write(std::move(actions));
    if (PREDICT_FALSE(!s.ok())) {
      s = s.CloneAndPrepend("an error occurred while updating the sys-catalog");
      LOG(WARNING) << s.ToString();
      CheckIfNoLongerLeaderAndSetupError(s, resp);
      return s;
    }
  }

  // 9. Commit the in-memory state.
  TRACE("Committing alterations to in-memory state");
  {
    // Commit new tablet in-memory state. This doesn't require taking the global
    // lock since the new tablets are not yet visible, because they haven't been
    // added to the table or tablet index.
    tablets_to_add_lock.Commit();

    // Take the global catalog manager lock in order to modify the global table
    // and tablets indices.
    std::lock_guard<LockType> lock(lock_);
    if (req.has_new_table_name()) {
      if (normalized_table_names_map_.erase(normalized_table_name) != 1) {
        LOG(FATAL) << "Could not remove table " << table->ToString()
                   << " from map in response to AlterTable request: "
                   << SecureShortDebugString(req);
      }
      InsertOrDie(&normalized_table_names_map_, normalized_new_table_name, table);

      // Alter the table name in the attributes of the metrics.
      table->UpdateMetricsAttrs(normalized_new_table_name);
    }

    // Insert new tablets into the global tablet map. After this, the tablets
    // will be visible in GetTabletLocations RPCs.
    for (const auto& tablet : tablets_to_add) {
      InsertOrDie(&tablet_map_, tablet->id(), tablet);
    }
  }

  // Add and remove new tablets from the table. This makes the tablets visible
  // to GetTableLocations RPCs. This doesn't need to happen under the global
  // lock, since:
  //  * clients can not know the new tablet IDs, so GetTabletLocations RPCs
  //    are impossible.
  //  * the new tablets can not heartbeat yet, since they don't get created
  //    until further down.
  //
  // We acquire new READ locks for tablets_to_add because we've already
  // committed our WRITE locks above, and reordering the operations such that
  // the WRITE locks could be reused would open a short window wherein
  // uninitialized tablet state is published to the world.
  for (const auto& tablet : tablets_to_add) {
    tablet->metadata().ReadLock();
  }
  table->AddRemoveTablets(tablets_to_add, tablets_to_drop);
  for (const auto& tablet : tablets_to_add) {
    tablet->metadata().ReadUnlock();
  }

  // Commit state change for dropped tablets. This comes after removing the
  // tablets from their associated tables so that if a GetTableLocations or
  // GetTabletLocations returns a deleted tablet, the retry will never include
  // the tablet again.
  tablets_to_drop_lock.Commit();

  // If there are schema changes, then update the entry in the Hive Metastore.
  // This is done on a best-effort basis, since Kudu is the source of truth for
  // table schema information, and the table has already been altered in the
  // Kudu catalog via the successful sys-table write above.
  if (hms_catalog_ && has_schema_changes) {
    // Sanity check: if there are schema changes then this is necessarily not a
    // table rename, since we split out the rename portion into its own
    // 'transaction' which is serialized through the HMS.
    DCHECK(!req.has_new_table_name());
    WARN_NOT_OK(hms_catalog_->AlterTable(
          table->id(), normalized_table_name, normalized_table_name, new_schema),
        Substitute("failed to alter HiveMetastore schema for table $0, "
                   "HMS schema information will be stale", table->ToString()));
  }

  if (!tablets_to_add.empty() || has_metadata_changes) {
    l.Commit();
  } else {
    l.Unlock();
  }

  SendAlterTableRequest(table);
  for (const auto& tablet : tablets_to_drop) {
    TabletMetadataLock l(tablet.get(), LockMode::READ);
    SendDeleteTabletRequest(tablet, l, deletion_msg);
  }

  background_tasks_->Wake();
  return Status::OK();
}

Status CatalogManager::IsAlterTableDone(const IsAlterTableDoneRequestPB* req,
                                        IsAlterTableDoneResponsePB* resp,
                                        optional<const string&> user) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // 1. Lookup the table, verify if it exists, and then check that
  //    the user is authorized to operate on the table.
  scoped_refptr<TableInfo> table;
  TableMetadataLock l;
  auto authz_func = [&] (const string& username, const string& table_name) {
    return SetupError(authz_provider_->AuthorizeGetTableMetadata(table_name, username),
                      resp, MasterErrorPB::NOT_AUTHORIZED);
  };
  RETURN_NOT_OK(FindLockAndAuthorizeTable(*req, resp, LockMode::READ, authz_func, user,
                                          &table, &l));
  RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(&l, resp));

  // 2. Verify if the alter is in-progress
  TRACE("Verify if there is an alter operation in progress for $0", table->ToString());
  resp->set_schema_version(l.data().pb.version());
  resp->set_done(l.data().pb.state() != SysTablesEntryPB::ALTERING);

  return Status::OK();
}

Status CatalogManager::GetTableSchema(const GetTableSchemaRequestPB* req,
                                      GetTableSchemaResponsePB* resp,
                                      optional<const string&> user,
                                      const TokenSigner* token_signer) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // Lookup the table, verify if it exists, and then check that
  // the user is authorized to operate on the table.
  scoped_refptr<TableInfo> table;
  TableMetadataLock l;

  auto authz_func = [&] (const string& username, const string& table_name) {
    return SetupError(authz_provider_->AuthorizeGetTableMetadata(table_name, username),
                      resp, MasterErrorPB::NOT_AUTHORIZED);
  };
  RETURN_NOT_OK(FindLockAndAuthorizeTable(*req, resp, LockMode::READ, authz_func, user,
                                          &table, &l));
  RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(&l, resp));

  // If fully_applied_schema is set, use it, since an alter is in progress.
  CHECK(!l.data().pb.has_fully_applied_schema() ||
        (l.data().pb.state() == SysTablesEntryPB::ALTERING));
  const SchemaPB& schema_pb = l.data().pb.has_fully_applied_schema() ?
      l.data().pb.fully_applied_schema() : l.data().pb.schema();

  if (token_signer && user) {
    TablePrivilegePB table_privilege;
    table_privilege.set_table_id(table->id());
    RETURN_NOT_OK(
        SetupError(authz_provider_->FillTablePrivilegePB(l.data().name(), *user, schema_pb,
                                                         &table_privilege),
                   resp, MasterErrorPB::UNKNOWN_ERROR));
    security::SignedTokenPB authz_token;
    RETURN_NOT_OK(token_signer->GenerateAuthzToken(
        *user, std::move(table_privilege), &authz_token));
    *resp->mutable_authz_token() = std::move(authz_token);
  }
  resp->mutable_schema()->CopyFrom(schema_pb);
  resp->set_num_replicas(l.data().pb.num_replicas());
  resp->set_table_id(table->id());
  resp->mutable_partition_schema()->CopyFrom(l.data().pb.partition_schema());
  resp->set_table_name(l.data().pb.name());
  RETURN_NOT_OK(ExtraConfigPBToPBMap(l.data().pb.extra_config(), resp->mutable_extra_configs()));

  return Status::OK();
}

Status CatalogManager::ListTables(const ListTablesRequestPB* req,
                                  ListTablesResponsePB* resp,
                                  optional<const string&> user) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  vector<scoped_refptr<TableInfo>> tables_info;
  {
    shared_lock<LockType> l(lock_);
    for (const TableInfoMap::value_type &entry : normalized_table_names_map_) {
      tables_info.emplace_back(entry.second);
    }
  }
  unordered_map<string, scoped_refptr<TableInfo>> table_info_by_name;
  unordered_set<string> table_names;
  for (const auto& table_info : tables_info) {
    TableMetadataLock ltm(table_info.get(), LockMode::READ);
    if (!ltm.data().is_running()) continue; // implies !is_deleted() too

    const string& table_name = ltm.data().name();
    if (req->has_name_filter()) {
      size_t found = table_name.find(req->name_filter());
      if (found == string::npos) {
        continue;
      }
    }
    InsertOrUpdate(&table_info_by_name, table_name, table_info);
    EmplaceIfNotPresent(&table_names, table_name);
  }

  MAYBE_INJECT_FIXED_LATENCY(FLAGS_catalog_manager_inject_latency_list_authz_ms);
  bool checked_table_names = false;
  if (user) {
    RETURN_NOT_OK(authz_provider_->AuthorizeListTables(
        *user, &table_names, &checked_table_names));
  }

  // If we checked privileges, do another pass over the tables to filter out
  // any that may have been altered while authorizing.
  if (checked_table_names) {
    for (const auto& table_name : table_names) {
      const auto& table_info = FindOrDie(table_info_by_name, table_name);
      TableMetadataLock ltm(table_info.get(), LockMode::READ);
      if (!ltm.data().is_running()) continue;

      // If we have a different table name than expected, there was a table
      // rename and we shouldn't show the table.
      if (table_name != ltm.data().name()) {
        continue;
      }
      ListTablesResponsePB::TableInfo* table = resp->add_tables();
      table->set_id(table_info->id());
      table->set_name(table_name);
    }
  } else {
    // Otherwise, pass all tables through.
    for (const auto& name_and_table_info : table_info_by_name) {
      const auto& table_name = name_and_table_info.first;
      const auto& table_info = name_and_table_info.second;
      ListTablesResponsePB::TableInfo* table = resp->add_tables();
      table->set_id(table_info->id());
      table->set_name(table_name);
    }
  }
  return Status::OK();
}

Status CatalogManager::GetTableStatistics(const GetTableStatisticsRequestPB* req,
                                          GetTableStatisticsResponsePB* resp,
                                          optional<const string&> user) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  scoped_refptr<TableInfo> table;
  TableMetadataLock l;
  auto authz_func = [&] (const string& username, const string& table_name) {
      return SetupError(authz_provider_->AuthorizeGetTableStatistics(table_name, username),
                        resp, MasterErrorPB::NOT_AUTHORIZED);
  };
  RETURN_NOT_OK(FindLockAndAuthorizeTable(*req, resp, LockMode::READ, authz_func, user,
                                          &table, &l));

  if (PREDICT_FALSE(FLAGS_mock_table_metrics_for_testing)) {
    if (FLAGS_catalog_manager_support_on_disk_size) {
      resp->set_on_disk_size(FLAGS_on_disk_size_for_testing);
    }
    if (FLAGS_catalog_manager_support_live_row_count) {
      resp->set_live_row_count(FLAGS_live_row_count_for_testing);
    }
  } else {
    if (table->GetMetrics()->TableSupportsOnDiskSize()) {
      resp->set_on_disk_size(table->GetMetrics()->on_disk_size->value());
    }
    if (table->GetMetrics()->TableSupportsLiveRowCount()) {
      resp->set_live_row_count(table->GetMetrics()->live_row_count->value());
    }
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

Status CatalogManager::GetAllTables(vector<scoped_refptr<TableInfo>>* tables) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  tables->clear();
  shared_lock<LockType> l(lock_);
  AppendValuesFromMap(table_ids_map_, tables);

  return Status::OK();
}

Status CatalogManager::TableNameExists(const string& table_name, bool* exists) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  shared_lock<LockType> l(lock_);
  *exists = ContainsKey(normalized_table_names_map_, NormalizeTableName(table_name));
  return Status::OK();
}

namespace {

// Returns true if 'report' for 'tablet' should cause it to transition to RUNNING.
//
// Note: do not use the consensus state in 'report'; use 'cstate' instead.
bool ShouldTransitionTabletToRunning(const scoped_refptr<TabletInfo>& tablet,
                                     const ReportedTabletPB& report,
                                     const ConsensusStatePB& cstate) {
  // Does the master think the tablet is running?
  if (tablet->metadata().state().is_running()) return false;

  // Does the report indicate that the tablet is running?
  if (report.state() != tablet::RUNNING) return false;

  // In many tests, we disable leader election, so newly created tablets
  // will never elect a leader on their own. In this case, we transition
  // to RUNNING as soon as we get a single report.
  if (!FLAGS_catalog_manager_wait_for_new_tablets_to_elect_leader) {
    return true;
  }

  // Otherwise, we only transition to RUNNING once there is a leader that is a
  // member of the committed configuration.
  return !cstate.leader_uuid().empty() &&
      IsRaftConfigMember(cstate.leader_uuid(), cstate.committed_config());
}

} // anonymous namespace

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

  Status PickReplica(string* ts_uuid) override {
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
  explicit PickLeaderReplica(scoped_refptr<TabletInfo> tablet) :
      tablet_(std::move(tablet)) {
  }

  Status PickReplica(string* ts_uuid) override {
    TabletMetadataLock l(tablet_.get(), LockMode::READ);

    string err_msg;
    if (!l.data().pb.has_consensus_state()) {
      // The tablet is still in the PREPARING state and has no replicas.
      err_msg = Substitute("Tablet $0 has no consensus state",
                           tablet_->id());
    } else if (l.data().pb.consensus_state().leader_uuid().empty()) {
      // The tablet may be in the midst of a leader election.
      err_msg = Substitute("Tablet $0 consensus state has no leader",
                           tablet_->id());
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
                    unique_ptr<TSPicker> replica_picker,
                    scoped_refptr<TableInfo> table)
    : master_(master),
      replica_picker_(std::move(replica_picker)),
      table_(std::move(table)),
      start_ts_(MonoTime::Now()),
      deadline_(start_ts_ + MonoDelta::FromMilliseconds(FLAGS_unresponsive_ts_rpc_timeout_ms)),
      attempt_(0),
      state_(kStateRunning) {
  }

  // Send the subclass RPC request.
  Status Run();

  // Abort this task.
  void Abort() override {
    MarkAborted();
  }

  State state() const override {
    return static_cast<State>(NoBarrier_Load(&state_));
  }

  // Return the id of the tablet that is the subject of the async request.
  virtual string tablet_id() const = 0;

  MonoTime start_timestamp() const override { return start_ts_; }
  MonoTime completion_timestamp() const override { return end_ts_; }
  const scoped_refptr<TableInfo>& table() const { return table_ ; }

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
  const unique_ptr<TSPicker> replica_picker_;
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

  // Increment the counter of the attempts to run the task.
  ++attempt_;

  Status s = ResetTSProxy();
  if (s.ok()) {
    if (SendRequest(attempt_)) {
      return Status::OK();
    }
  } else {
    s = s.CloneAndPrepend("failed to reset TS proxy");
  }

  if (!RescheduleWithBackoffDelay()) {
    MarkFailed();
    UnregisterAsyncTask();  // May call 'delete this'.
  }
  return s;
}

void RetryingTSRpcTask::RpcCallback() {
  if (!rpc_.status().ok()) {
    KLOG_EVERY_N_SECS(WARNING, 1) << Substitute("TS $0: $1 RPC failed for tablet $2: $3",
                                                target_ts_desc_->ToString(), type_name(),
                                                tablet_id(), rpc_.status().ToString());
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
    return false;
  }
  VLOG(1) << Substitute("Scheduling retry of $0 with a delay of $1 ms (attempt = $2)",
                        description(), delay_millis, attempt_);
  master_->messenger()->ScheduleOnReactor(
      [this](const Status& s) { this->RunDelayedTask(s); },
      MonoDelta::FromMilliseconds(delay_millis));
  return true;
}

void RetryingTSRpcTask::RunDelayedTask(const Status& status) {
  if (!status.ok()) {
    LOG(WARNING) << Substitute("Async tablet task $0 failed was cancelled: $1",
                               description(), status.ToString());
    UnregisterAsyncTask();   // May delete this.
    return;
  }

  string desc = description();  // Save in case we need to log after deletion.
  Status s = Run();             // May delete this.
  if (!s.ok()) {
    KLOG_EVERY_N_SECS(WARNING, 1) << Substitute("Async tablet task $0 failed: $1",
                                                desc, s.ToString());
  }
}

void RetryingTSRpcTask::UnregisterAsyncTask() {
  end_ts_ = MonoTime::Now();
  table_->RemoveTask(tablet_id(), this);
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

  // We may be called by a reactor thread, and creating proxies may trigger DNS
  // resolution.
  //
  // TODO(adar): make the DNS resolution asynchronous.
  ThreadRestrictions::ScopedAllowWait allow_wait;

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
                        unique_ptr<TSPicker>(new PickSpecificUUID(permanent_uuid)),
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
      tablet_id_(tablet->id()) {
    deadline_ = start_ts_ + MonoDelta::FromMilliseconds(FLAGS_tablet_creation_timeout_ms);

    TableMetadataLock table_lock(tablet->table().get(), LockMode::READ);
    req_.set_dest_uuid(permanent_uuid);
    req_.set_table_id(tablet->table()->id());
    req_.set_tablet_id(tablet->id());
    req_.mutable_partition()->CopyFrom(tablet_lock.data().pb.partition());
    req_.set_table_name(table_lock.data().pb.name());
    req_.mutable_schema()->CopyFrom(table_lock.data().pb.schema());
    req_.mutable_partition_schema()->CopyFrom(
        table_lock.data().pb.partition_schema());
    req_.mutable_config()->CopyFrom(
        tablet_lock.data().pb.consensus_state().committed_config());
    req_.mutable_extra_config()->CopyFrom(
        table_lock.data().pb.extra_config());
    req_.set_dimension_label(tablet_lock.data().pb.dimension_label());
  }

  string type_name() const override { return "CreateTablet"; }

  string description() const override {
    return "CreateTablet RPC for tablet " + tablet_id_ + " on TS " + permanent_uuid_;
  }

 protected:
  string tablet_id() const override { return tablet_id_; }

  void HandleResponse(int attempt) override {
    if (!resp_.has_error()) {
      MarkComplete();
    } else {
      Status s = StatusFromPB(resp_.error().status());
      if (s.IsAlreadyPresent()) {
        LOG(INFO) << Substitute("CreateTablet RPC for tablet $0 on TS $1 "
            "returned already present: $2", tablet_id_,
            target_ts_desc_->ToString(), s.ToString());
        MarkComplete();
      } else {
        KLOG_EVERY_N_SECS(WARNING, 1) <<
            Substitute("CreateTablet RPC for tablet $0 on TS $1 failed: $2",
                       tablet_id_, target_ts_desc_->ToString(), s.ToString());
      }
    }
  }

  bool SendRequest(int attempt) override {
    VLOG(1) << Substitute("Sending $0 request to $1 (attempt $2): $3",
                          type_name(), target_ts_desc_->ToString(), attempt,
                          SecureDebugString(req_));
    ts_proxy_->CreateTabletAsync(req_, &resp_, &rpc_,
                                 [this]() { this->RpcCallback(); });
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
      const scoped_refptr<TableInfo>& table, string tablet_id,
      TabletDataState delete_type,
      optional<int64_t> cas_config_opid_index_less_or_equal,
      string reason)
      : RetrySpecificTSRpcTask(master, permanent_uuid, table),
        tablet_id_(std::move(tablet_id)),
        delete_type_(delete_type),
        cas_config_opid_index_less_or_equal_(
            std::move(cas_config_opid_index_less_or_equal)),
        reason_(std::move(reason)) {}

  string type_name() const override {
    return Substitute("DeleteTablet:$0", TabletDataState_Name(delete_type_));
  }

  string description() const override {
    return "DeleteTablet RPC for tablet " + tablet_id_ + " on TS " + permanent_uuid_;
  }

 protected:
  string tablet_id() const override { return tablet_id_; }

  void HandleResponse(int attempt) override {
    if (resp_.has_error()) {
      Status status = StatusFromPB(resp_.error().status());

      // Do not retry on a fatal error
      TabletServerErrorPB::Code code = resp_.error().code();
      switch (code) {
        case TabletServerErrorPB::TABLET_NOT_FOUND:
          LOG(WARNING) << Substitute("TS $0: delete failed for tablet $1 "
              "because the tablet was not found. No further retry: $2",
              target_ts_desc_->ToString(), tablet_id_, status.ToString());
          MarkComplete();
          break;
        // Do not retry on a CAS error
        case TabletServerErrorPB::CAS_FAILED:
          LOG(WARNING) << Substitute("TS $0: delete failed for tablet $1 "
              "because of a CAS failure. No further retry: $2",
              target_ts_desc_->ToString(), tablet_id_, status.ToString());
          MarkFailed();
          break;
        case TabletServerErrorPB::ALREADY_INPROGRESS:
          LOG(WARNING) << Substitute("TS $0: delete failed for tablet $1 "
            "because tablet deleting was already in progress. No further retry: $2",
            target_ts_desc_->ToString(), tablet_id_, status.ToString());
          MarkComplete();
          break;
        default:
          KLOG_EVERY_N_SECS(WARNING, 1) <<
              Substitute("TS $0: delete failed for tablet $1 with error code $2: $3",
                         target_ts_desc_->ToString(), tablet_id_,
                         TabletServerErrorPB::Code_Name(code), status.ToString());
          break;
      }
    } else {
      if (table_) {
        LOG(INFO) << Substitute("TS $0: tablet $1 (table $2) successfully deleted",
                                target_ts_desc_->ToString(), tablet_id_, table_->ToString());
      } else {
        LOG(WARNING) << Substitute("TS $0: tablet $1 did not belong to a known table, "
            "but was successfully deleted", target_ts_desc_->ToString(), tablet_id_);
      }
      MarkComplete();
      VLOG(1) << Substitute("TS $0: delete complete on tablet $1",
                            target_ts_desc_->ToString(), tablet_id_);
    }
  }

  bool SendRequest(int attempt) override {
    tserver::DeleteTabletRequestPB req;
    req.set_dest_uuid(permanent_uuid_);
    req.set_tablet_id(tablet_id_);
    req.set_reason(reason_);
    req.set_delete_type(delete_type_);
    if (cas_config_opid_index_less_or_equal_) {
      req.set_cas_config_opid_index_less_or_equal(*cas_config_opid_index_less_or_equal_);
    }

    VLOG(1) << Substitute("Sending $0 request to $1 (attempt $2): $3",
                          type_name(), target_ts_desc_->ToString(), attempt,
                          SecureDebugString(req));
    ts_proxy_->DeleteTabletAsync(req, &resp_, &rpc_,
                                 [this]() { this->RpcCallback(); });
    return true;
  }

  const string tablet_id_;
  const TabletDataState delete_type_;
  const optional<int64_t> cas_config_opid_index_less_or_equal_;
  const string reason_;
  tserver::DeleteTabletResponsePB resp_;
};

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
                  scoped_refptr<TabletInfo> tablet)
    : RetryingTSRpcTask(master,
                        unique_ptr<TSPicker>(new PickLeaderReplica(tablet)),
                        tablet->table()),
      tablet_(std::move(tablet)) {
  }

  string type_name() const override { return "AlterTable"; }

  string description() const override {
    return Substitute("AlterTable RPC for tablet $0 (table $1, current schema version=$2)",
                      tablet_->id(), table_->ToString(), table_->schema_version());
  }

 private:
  string tablet_id() const override { return tablet_->id(); }

  void HandleResponse(int /*attempt*/) override {
    if (resp_.has_error()) {
      Status status = StatusFromPB(resp_.error().status());

      // Do not retry on a fatal error
      switch (resp_.error().code()) {
        case TabletServerErrorPB::TABLET_NOT_FOUND:
        case TabletServerErrorPB::MISMATCHED_SCHEMA:
        case TabletServerErrorPB::TABLET_HAS_A_NEWER_SCHEMA:
          LOG(WARNING) << Substitute("TS $0: alter failed for tablet $1,"
              "no further retry: $2", target_ts_desc_->ToString(),
              tablet_->ToString(), status.ToString());
          MarkComplete();
          break;
        default:
          KLOG_EVERY_N_SECS(WARNING, 1) <<
              Substitute("TS $0: alter failed for tablet $1: $2",
                         target_ts_desc_->ToString(), tablet_->ToString(),
                         status.ToString());
          break;
      }
    } else {
      MarkComplete();
      VLOG(1) << Substitute("TS $0: alter complete on tablet $1",
                            target_ts_desc_->ToString(), tablet_->ToString());
    }

    if (state() != kStateComplete) {
      VLOG(1) << "Still waiting for other tablets to finish ALTER";
    }
  }

  bool SendRequest(int attempt) override {
    TableMetadataLock l(tablet_->table().get(), LockMode::READ);

    tserver::AlterSchemaRequestPB req;
    req.set_dest_uuid(target_ts_desc_->permanent_uuid());
    req.set_tablet_id(tablet_->id());
    req.set_new_table_name(l.data().pb.name());
    req.set_schema_version(l.data().pb.version());
    req.mutable_schema()->CopyFrom(l.data().pb.schema());
    req.mutable_new_extra_config()->CopyFrom(l.data().pb.extra_config());

    l.Unlock();

    VLOG(1) << Substitute("Sending $0 request to $1 (attempt $2): $3",
                          type_name(), target_ts_desc_->ToString(), attempt,
                          SecureDebugString(req));
    ts_proxy_->AlterSchemaAsync(req, &resp_, &rpc_,
                                [this]() { this->RpcCallback(); });
    return true;
  }

  scoped_refptr<TabletInfo> tablet_;
  tserver::AlterSchemaResponsePB resp_;
};

class AsyncChangeConfigTask : public RetryingTSRpcTask {
 public:
  AsyncChangeConfigTask(Master* master,
                        scoped_refptr<TabletInfo> tablet,
                        ConsensusStatePB cstate,
                        consensus::ChangeConfigType change_config_type);

  string description() const override;

 protected:
  void HandleResponse(int attempt) override;
  bool CheckOpIdIndex();

  const scoped_refptr<TabletInfo> tablet_;
  const ConsensusStatePB cstate_;
  const consensus::ChangeConfigType change_config_type_;

  consensus::ChangeConfigResponsePB resp_;

 private:
  string tablet_id() const override { return tablet_->id(); }
};

AsyncChangeConfigTask::AsyncChangeConfigTask(Master* master,
                                             scoped_refptr<TabletInfo> tablet,
                                             ConsensusStatePB cstate,
                                             consensus::ChangeConfigType change_config_type)
    : RetryingTSRpcTask(master,
                        unique_ptr<TSPicker>(new PickLeaderReplica(tablet)),
                        tablet->table()),
      tablet_(std::move(tablet)),
      cstate_(std::move(cstate)),
      change_config_type_(change_config_type) {
    deadline_ = MonoTime::Max(); // Never time out.
  }

string AsyncChangeConfigTask::description() const {
  return Substitute("$0 RPC for tablet $1 with cas_config_opid_index $2",
                    type_name(),
                    tablet_->id(),
                    cstate_.committed_config().opid_index());
}

void AsyncChangeConfigTask::HandleResponse(int attempt) {
  if (!resp_.has_error()) {
    MarkComplete();
    LOG_WITH_PREFIX(INFO) << Substitute("$0 succeeded (attempt $1)",
                                        type_name(), attempt);
    return;
  }

  Status status = StatusFromPB(resp_.error().status());

  // Do not retry on a CAS error, otherwise retry forever or until cancelled.
  switch (resp_.error().code()) {
    case TabletServerErrorPB::CAS_FAILED:
      LOG_WITH_PREFIX(WARNING) << Substitute("$0 failed with leader $1 "
          "due to CAS failure; no further retry: $2",
          type_name(), target_ts_desc_->ToString(),
          status.ToString());
      MarkFailed();
      break;
    default:
      KLOG_EVERY_N_SECS(WARNING, 1) << LogPrefix() <<
          Substitute("$0 failed with leader $1 due to error $2; will retry: $3",
                     type_name(), target_ts_desc_->ToString(),
                     TabletServerErrorPB::Code_Name(resp_.error().code()),
                     status.ToString());
      break;
  }
}

bool AsyncChangeConfigTask::CheckOpIdIndex() {
  int64_t latest_index;
  {
    TabletMetadataLock tablet_lock(tablet_.get(), LockMode::READ);
    latest_index = tablet_lock.data().pb.consensus_state()
        .committed_config().opid_index();
  }
  if (latest_index > cstate_.committed_config().opid_index()) {
    LOG_WITH_PREFIX(INFO) << Substitute("aborting the task: "
        "latest config opid_index $0; task opid_index $1",
        latest_index, cstate_.committed_config().opid_index());
    MarkAborted();
    return false;
  }
  return true;
}

class AsyncAddReplicaTask : public AsyncChangeConfigTask {
 public:
  AsyncAddReplicaTask(Master* master,
                      scoped_refptr<TabletInfo> tablet,
                      ConsensusStatePB cstate,
                      RaftPeerPB::MemberType member_type,
                      ThreadSafeRandom* rng);

  string type_name() const override;

 protected:
  bool SendRequest(int attempt) override;

 private:
  const RaftPeerPB::MemberType member_type_;

  // Used to make random choices in replica selection.
  ThreadSafeRandom* rng_;
};

AsyncAddReplicaTask::AsyncAddReplicaTask(Master* master,
                                         scoped_refptr<TabletInfo> tablet,
                                         ConsensusStatePB cstate,
                                         RaftPeerPB::MemberType member_type,
                                         ThreadSafeRandom* rng)
    : AsyncChangeConfigTask(master, std::move(tablet), std::move(cstate),
                            consensus::ADD_PEER),
      member_type_(member_type),
      rng_(rng) {
}

string AsyncAddReplicaTask::type_name() const {
  return Substitute("ChangeConfig:$0:$1",
                    consensus::ChangeConfigType_Name(change_config_type_),
                    RaftPeerPB::MemberType_Name(member_type_));
}

bool AsyncAddReplicaTask::SendRequest(int attempt) {
  // Bail if we're retrying in vain.
  if (!CheckOpIdIndex()) {
    return false;
  }

  Status s;
  shared_ptr<TSDescriptor> extra_replica;
  {
    // Select the replica we wish to add to the config.
    // Do not include current members of the config.
    const auto& config = cstate_.committed_config();
    TSDescriptorVector existing;
    for (auto i = 0; i < config.peers_size(); ++i) {
      shared_ptr<TSDescriptor> desc;
      if (master_->ts_manager()->LookupTSByUUID(config.peers(i).permanent_uuid(),
                                                &desc)) {
        existing.emplace_back(std::move(desc));
      }
    }

    TSDescriptorVector ts_descs;
    master_->ts_manager()->GetDescriptorsAvailableForPlacement(&ts_descs);

    // Get the dimension of the tablet. Otherwise, it will be none.
    optional<string> dimension = none;
    {
      TabletMetadataLock l(tablet_.get(), LockMode::READ);
      if (tablet_->metadata().state().pb.has_dimension_label()) {
        dimension = tablet_->metadata().state().pb.dimension_label();
      }
    }

    // Some of the tablet servers hosting the current members of the config
    // (see the 'existing' populated above) might be presumably dead.
    // Inclusion of a presumably dead tablet server into 'existing' is OK:
    // PlacementPolicy::PlaceExtraTabletReplica() does not require elements of
    // 'existing' to be a subset of 'ts_descs', and 'ts_descs' contains only
    // alive tablet servers. Essentially, the list of candidate tablet servers
    // to host the extra replica is 'ts_descs' after blacklisting all elements
    // common with 'existing'.
    PlacementPolicy policy(std::move(ts_descs), rng_);
    s = policy.PlaceExtraTabletReplica(std::move(existing), dimension, &extra_replica);
  }
  if (PREDICT_FALSE(!s.ok())) {
    auto msg = Substitute("no extra replica candidate found for tablet $0: $1",
                          tablet_->ToString(), s.ToString());
    // Check whether it's a situation when a replacement replica cannot be found
    // due to an inconsistency in cluster configuration. If the tablet has the
    // replication factor of N, and the cluster is using the N->(N+1)->N
    // replica management scheme (see --raft_prepare_replacement_before_eviction
    // flag), at least N+1 tablet servers should be registered to find a place
    // for an extra replica.
    const auto num_tservers_registered = master_->ts_manager()->GetCount();

    auto replication_factor = 0;
    {
      TableMetadataLock l(tablet_->table().get(), LockMode::READ);
      replication_factor = tablet_->table()->metadata().state().pb.num_replicas();
    }
    DCHECK_GE(replication_factor, 1);
    const auto num_tservers_needed =
        FLAGS_raft_prepare_replacement_before_eviction ? replication_factor + 1
                                                       : replication_factor;
    if (num_tservers_registered < num_tservers_needed) {
      msg += Substitute(
          ": the total number of registered tablet servers ($0) does not allow "
          "for adding an extra replica; consider bringing up more "
          "to have at least $1 tablet servers up and running",
          num_tservers_registered, num_tservers_needed);
    }
    KLOG_EVERY_N_SECS(WARNING, 60) << LogPrefix() << msg;
    return false;
  }

  DCHECK(extra_replica);
  consensus::ChangeConfigRequestPB req;
  req.set_dest_uuid(target_ts_desc_->permanent_uuid());
  req.set_tablet_id(tablet_->id());
  req.set_type(consensus::ADD_PEER);
  req.set_cas_config_opid_index(cstate_.committed_config().opid_index());
  RaftPeerPB* peer = req.mutable_server();
  peer->set_permanent_uuid(extra_replica->permanent_uuid());
  if (FLAGS_raft_prepare_replacement_before_eviction &&
      member_type_ == RaftPeerPB::NON_VOTER) {
    peer->mutable_attrs()->set_promote(true);
  }
  ServerRegistrationPB peer_reg;
  extra_replica->GetRegistration(&peer_reg);
  CHECK_GT(peer_reg.rpc_addresses_size(), 0);
  *peer->mutable_last_known_addr() = peer_reg.rpc_addresses(0);
  peer->set_member_type(member_type_);
  VLOG(1) << Substitute("Sending $0 request to $1 (attempt $2): $3",
                        type_name(), target_ts_desc_->ToString(), attempt,
                        SecureDebugString(req));
  consensus_proxy_->ChangeConfigAsync(req, &resp_, &rpc_,
                                      [this]() { this->RpcCallback(); });
  return true;
}

class AsyncEvictReplicaTask : public AsyncChangeConfigTask {
 public:
  AsyncEvictReplicaTask(Master *master,
                        scoped_refptr<TabletInfo> tablet,
                        ConsensusStatePB cstate,
                        string peer_uuid_to_evict);

  string type_name() const override;

 protected:
  bool SendRequest(int attempt) override;

 private:
  const string peer_uuid_to_evict_;
};

AsyncEvictReplicaTask::AsyncEvictReplicaTask(Master* master,
                                             scoped_refptr<TabletInfo> tablet,
                                             ConsensusStatePB cstate,
                                             string peer_uuid_to_evict)
    : AsyncChangeConfigTask(master, std::move(tablet), std::move(cstate),
                            consensus::REMOVE_PEER),
      peer_uuid_to_evict_(std::move(peer_uuid_to_evict)) {
}

string AsyncEvictReplicaTask::type_name() const {
  return Substitute("ChangeConfig:$0",
                    consensus::ChangeConfigType_Name(change_config_type_));
}

bool AsyncEvictReplicaTask::SendRequest(int attempt) {
  // Bail if we're retrying in vain.
  if (!CheckOpIdIndex()) {
    return false;
  }

  consensus::ChangeConfigRequestPB req;
  req.set_dest_uuid(target_ts_desc_->permanent_uuid());
  req.set_tablet_id(tablet_->id());
  req.set_type(consensus::REMOVE_PEER);
  req.set_cas_config_opid_index(cstate_.committed_config().opid_index());
  RaftPeerPB* peer = req.mutable_server();
  peer->set_permanent_uuid(peer_uuid_to_evict_);
  VLOG(1) << Substitute("Sending $0 request to $1 (attempt $2): $3",
                        type_name(), target_ts_desc_->ToString(), attempt,
                        SecureDebugString(req));
  consensus_proxy_->ChangeConfigAsync(req, &resp_, &rpc_,
                                      [this]() { this->RpcCallback(); });
  return true;
}

Status CatalogManager::ProcessTabletReport(
    TSDescriptor* ts_desc,
    const TabletReportPB& full_report,
    TabletReportUpdatesPB* full_report_update,
    RpcContext* rpc) {
  int num_tablets = full_report.updated_tablets_size();
  TRACE_EVENT2("master", "ProcessTabletReport",
               "requestor", rpc->requestor_string(),
               "num_tablets", num_tablets);
  TRACE_COUNTER_INCREMENT("reported_tablets", num_tablets);

  leader_lock_.AssertAcquiredForReading();

  VLOG(2) << Substitute("Received tablet report from $0:\n$1",
                        RequestorString(rpc), SecureDebugString(full_report));

  // TODO(todd): on a full tablet report, we may want to iterate over the
  // tablets we think the server should have, compare vs the ones being
  // reported, and somehow mark any that have been "lost" (eg somehow the
  // tablet metadata got corrupted or something).

  // Maps a tablet ID to its corresponding tablet report (owned by 'full_report').
  unordered_map<string, const ReportedTabletPB*> reports;

  // Maps a tablet ID to its corresponding tablet report update (owned by
  // 'full_report_update').
  unordered_map<string, ReportedTabletUpdatesPB*> updates;

  // Maps a tablet ID to its corresponding TabletInfo.
  unordered_map<string, scoped_refptr<TabletInfo>> tablet_infos;

  // Keeps track of all RPCs that should be sent when we're done.
  vector<scoped_refptr<RetryingTSRpcTask>> rpcs;

  // Locks the referenced tables (for READ) and tablets (for WRITE).
  //
  // We must hold the tablets' locks while writing to the catalog table, and
  // since they're locked for WRITE, we have to lock them en masse in order to
  // avoid deadlocking.
  //
  // We have more freedom with the table locks: we could acquire them en masse,
  // or we could acquire, use, and release them one at a time. So why do we
  // acquire en masse? Because it reduces the overall number of lock
  // acquisitions by reusing locks for tablets belonging to the same table, and
  // although one-at-a-time acquisition would reduce table lock contention when
  // writing, table writes are very rare events.
  TableMetadataGroupLock tables_lock(LockMode::RELEASED);
  TabletMetadataGroupLock tablets_lock(LockMode::RELEASED);

  // 1. Set up local state.
  full_report_update->mutable_tablets()->Reserve(num_tablets);
  {
    // We only need to acquire lock_ for the tablet_map_ access, but since it's
    // acquired exclusively so rarely, it's probably cheaper to acquire and
    // hold it for all tablets here than to acquire/release it for each tablet.
    shared_lock<LockType> l(lock_);
    for (const ReportedTabletPB& report : full_report.updated_tablets()) {
      const string& tablet_id = report.tablet_id();

      // 1a. Prepare an update entry for this tablet. Every tablet in the
      // report gets one, even if there's no change to it.
      ReportedTabletUpdatesPB* update = full_report_update->add_tablets();
      update->set_tablet_id(tablet_id);

      // 1b. Find the tablet, deleting/skipping it if it can't be found.
      scoped_refptr<TabletInfo> tablet = FindPtrOrNull(tablet_map_, tablet_id);
      if (!tablet) {
        // It'd be unsafe to ask the tserver to delete this tablet without first
        // replicating something to our followers (i.e. to guarantee that we're
        // the leader). For example, if we were a rogue master, we might be
        // deleting a tablet created by a new master accidentally. But masters
        // retain metadata for deleted tablets forever, so a tablet can only be
        // truly unknown in the event of a serious misconfiguration, such as a
        // tserver heartbeating to the wrong cluster. Therefore, it should be
        // reasonable to ignore it and wait for an operator fix the situation.
        LOG(WARNING) << "Ignoring report from unknown tablet " << tablet_id;
        continue;
      }

      // 1c. Found the tablet, update local state. If multiple tablets with the
      // same ID are in the report, all but the last one will be ignored.
      reports[tablet_id] = &report;
      updates[tablet_id] = update;
      tablet_infos[tablet_id] = tablet;
      tables_lock.AddInfo(*tablet->table().get());
      tablets_lock.AddMutableInfo(tablet.get());
    }
  }

  // 2. Lock the affected tables and tablets.
  tables_lock.Lock(LockMode::READ);
  tablets_lock.Lock(LockMode::WRITE);

  // 3. Process each tablet. This may not be in the order that the tablets
  // appear in 'full_report', but that has no bearing on correctness.
  vector<scoped_refptr<TabletInfo>> mutated_tablets;
  unordered_set<string> uuids_ignored_for_underreplication =
      master_->ts_manager()->GetUuidsToIgnoreForUnderreplication();
  for (const auto& e : tablet_infos) {
    const string& tablet_id = e.first;
    const scoped_refptr<TabletInfo>& tablet = e.second;
    const scoped_refptr<TableInfo>& table = tablet->table();
    const ReportedTabletPB& report = *FindOrDie(reports, tablet_id);
    ReportedTabletUpdatesPB* update = FindOrDie(updates, tablet_id);
    bool tablet_was_mutated = false;

    // 4. Delete the tablet if it (or its table) have been deleted.
    if (tablet->metadata().state().is_deleted() ||
        table->metadata().state().is_deleted()) {
      const string& msg = tablet->metadata().state().pb.state_msg();
      update->set_state_msg(msg);
      VLOG(1) << Substitute("Got report from deleted tablet $0 ($1)", tablet->ToString(), msg);

      // TODO(unknown): Cancel tablet creation, instead of deleting, in cases
      // where that might be possible (tablet creation timeout & replacement).
      rpcs.emplace_back(new AsyncDeleteReplica(
          master_, ts_desc->permanent_uuid(), table, tablet_id,
          TABLET_DATA_DELETED, none, msg));
      continue;
    }

    // 5. Tombstone a replica that is no longer part of the Raft config (and
    // not already tombstoned or deleted outright).
    //
    // If the report includes a committed raft config, we only tombstone if
    // the opid_index is strictly less than the latest reported committed
    // config. This prevents us from spuriously deleting replicas that have
    // just been added to the committed config and are in the process of copying.
    const ConsensusStatePB& prev_cstate = tablet->metadata().state().pb.consensus_state();
    const int64_t prev_opid_index = prev_cstate.committed_config().opid_index();
    const int64_t report_opid_index = (report.has_consensus_state() &&
        report.consensus_state().committed_config().has_opid_index()) ?
            report.consensus_state().committed_config().opid_index() :
            consensus::kInvalidOpIdIndex;
    if (FLAGS_master_tombstone_evicted_tablet_replicas &&
        report.tablet_data_state() != TABLET_DATA_TOMBSTONED &&
        report.tablet_data_state() != TABLET_DATA_DELETED &&
        !IsRaftConfigMember(ts_desc->permanent_uuid(), prev_cstate.committed_config()) &&
        report_opid_index < prev_opid_index) {
      const string delete_msg = report_opid_index == consensus::kInvalidOpIdIndex ?
          "Replica has no consensus available" :
          Substitute("Replica with old config index $0", report_opid_index);
      rpcs.emplace_back(new AsyncDeleteReplica(
          master_, ts_desc->permanent_uuid(), table, tablet_id,
          TABLET_DATA_TOMBSTONED, prev_opid_index,
          Substitute("$0 (current committed config index is $1)",
                     delete_msg, prev_opid_index)));
      continue;
    }

    // 6. Skip a non-deleted tablet which reports an error.
    if (report.has_error()) {
      Status s = StatusFromPB(report.error());
      DCHECK(!s.ok());
      LOG(WARNING) << Substitute("Tablet $0 has failed on TS $1: $2",
                                 tablet->ToString(), ts_desc->ToString(), s.ToString());
      continue;
    }

    const auto replication_factor = table->metadata().state().pb.num_replicas();
    bool consensus_state_updated = false;
    // 7. Process the report's consensus state. There may be one even when the
    // replica has been tombstoned.
    if (report.has_consensus_state()) {
      // 7a. The master only processes reports for replicas with committed
      // consensus configurations since it needs the committed index to only
      // cache the most up-to-date config. Since it's possible for TOMBSTONED
      // replicas with no ConsensusMetadata on disk to be reported as having no
      // committed config opid_index, we skip over those replicas.
      if (!report.consensus_state().committed_config().has_opid_index()) {
        continue;
      }

      // 7b. Disregard the leader state if the reported leader is not a member
      // of the committed config.
      ConsensusStatePB cstate = report.consensus_state();
      if (cstate.leader_uuid().empty() ||
          !IsRaftConfigMember(cstate.leader_uuid(), cstate.committed_config())) {
        cstate.clear_leader_uuid();
      }

      // 7c. Mark the tablet as RUNNING if it makes sense to do so.
      //
      // We need to wait for a leader before marking a tablet as RUNNING, or
      // else we could incorrectly consider a tablet created when only a
      // minority of its replicas were successful. In that case, the tablet
      // would be stuck in this bad state forever.
      if (ShouldTransitionTabletToRunning(tablet, report, cstate)) {
        DCHECK_EQ(SysTabletsEntryPB::CREATING, tablet->metadata().state().pb.state())
            << Substitute("Tablet in unexpected state: $0: $1", tablet->ToString(),
                          SecureShortDebugString(tablet->metadata().state().pb));
        VLOG(1) << Substitute("Tablet $0 is now online", tablet->ToString());
        tablet->mutable_metadata()->mutable_dirty()->set_state(
            SysTabletsEntryPB::RUNNING, "Tablet reported with an active leader");
        tablet_was_mutated = true;
      }

      // 7d. Update the consensus state if:
      // - A config change operation was committed (reflected by a change to
      //   the committed config's opid_index).
      // - The new cstate has a leader, and either the old cstate didn't, or
      //   there was a term change.
      consensus_state_updated = (cstate.committed_config().opid_index() >
                                 prev_cstate.committed_config().opid_index()) ||
          (!cstate.leader_uuid().empty() &&
           (prev_cstate.leader_uuid().empty() ||
            cstate.current_term() > prev_cstate.current_term()));
      if (consensus_state_updated) {
        // 7d(i). Retain knowledge of the leader even if it wasn't reported in
        // the latest config.
        //
        // When a config change is reported to the master, it may not include
        // the leader because the follower doing the reporting may not know who
        // the leader is yet (it may have just started up). It is safe to reuse
        // the previous leader if the reported cstate has the same term as the
        // previous cstate, and the leader was known for that term.
        if (cstate.current_term() == prev_cstate.current_term()) {
          if (cstate.leader_uuid().empty() && !prev_cstate.leader_uuid().empty()) {
            cstate.set_leader_uuid(prev_cstate.leader_uuid());
            // Sanity check to detect consensus divergence bugs.
          } else if (!cstate.leader_uuid().empty() &&
              !prev_cstate.leader_uuid().empty() &&
              cstate.leader_uuid() != prev_cstate.leader_uuid()) {
            LOG(DFATAL) << Substitute("Previously reported cstate for tablet $0 gave "
                "a different leader for term $1 than the current cstate. "
                "Previous cstate: $2. Current cstate: $3.",
                tablet->ToString(), cstate.current_term(),
                SecureShortDebugString(prev_cstate),
                SecureShortDebugString(cstate));
            continue;
          }
        }

        LOG(INFO) << Substitute("T $0 P $1 reported cstate change: $2. New cstate: $3",
                                tablet->id(), ts_desc->permanent_uuid(),
                                DiffConsensusStates(prev_cstate, cstate),
                                SecureShortDebugString(cstate));
        VLOG(2) << Substitute("Updating cstate for tablet $0 from config reported by $1 "
            "to that committed in log index $2 with leader state from term $3",
            tablet_id, ts_desc->ToString(), cstate.committed_config().opid_index(),
            cstate.current_term());


        // 7d(ii). Update the consensus state.
        // Strip the health report from the cstate before persisting it.
        auto* dirty_cstate =
            tablet->mutable_metadata()->mutable_dirty()->pb.mutable_consensus_state();
        *dirty_cstate = cstate; // Copy in the updated cstate.
        // Strip out the health reports from the persisted copy *only*.
        for (auto& peer : *dirty_cstate->mutable_committed_config()->mutable_peers()) {
          peer.clear_health_report();
        }
        tablet_was_mutated = true;

        // 7d(iii). Delete any replicas from the previous config that are not
        // in the new one.
        if (FLAGS_master_tombstone_evicted_tablet_replicas) {
          unordered_set<string> current_member_uuids;
          for (const auto& p : cstate.committed_config().peers()) {
            InsertOrDie(&current_member_uuids, p.permanent_uuid());
          }
          for (const auto& p : prev_cstate.committed_config().peers()) {
            DCHECK(!p.has_health_report()); // Health report shouldn't be persisted.
            const string& peer_uuid = p.permanent_uuid();
            if (!ContainsKey(current_member_uuids, peer_uuid)) {
              rpcs.emplace_back(new AsyncDeleteReplica(
                  master_, peer_uuid, table, tablet_id,
                  TABLET_DATA_TOMBSTONED, prev_cstate.committed_config().opid_index(),
                  Substitute("TS $0 not found in new config with opid_index $1",
                             peer_uuid, cstate.committed_config().opid_index())));
            }
          }
        }
      }

      // 7e. Make tablet configuration change depending on the mode the server
      // is running with. The choice between two alternative modes is controlled
      // by the 'raft_prepare_replacement_before_eviction' run-time flag.
      if (!FLAGS_raft_prepare_replacement_before_eviction) {
        if (consensus_state_updated &&
            FLAGS_master_add_server_when_underreplicated &&
            CountVoters(cstate.committed_config()) < replication_factor) {
          // Add a server to the config if it is under-replicated.
          //
          // This is an idempotent operation due to a CAS enforced on the
          // committed config's opid_index.
          rpcs.emplace_back(new AsyncAddReplicaTask(
              master_, tablet, cstate, RaftPeerPB::VOTER, &rng_));
        }

      // When --raft_prepare_replacement_before_eviction is enabled, we
      // consider whether to add or evict replicas based on the health report
      // included in the leader's tablet report. Since only the leader tracks
      // health, we ignore reports from non-leaders in this case. Also, making
      // the changes recommended by Should{Add,Evict}Replica() assumes that the
      // leader replica has already committed the configuration it's working with.
      } else if (!cstate.has_pending_config() &&
                 !cstate.leader_uuid().empty() &&
                 cstate.leader_uuid() == ts_desc->permanent_uuid()) {
        const auto& config = cstate.committed_config();
        string to_evict;
        if (PREDICT_TRUE(FLAGS_catalog_manager_evict_excess_replicas) &&
            ShouldEvictReplica(config, cstate.leader_uuid(), replication_factor, &to_evict)) {
          DCHECK(!to_evict.empty());
          rpcs.emplace_back(new AsyncEvictReplicaTask(
              master_, tablet, cstate, std::move(to_evict)));
        } else if (FLAGS_master_add_server_when_underreplicated &&
                   ShouldAddReplica(config, replication_factor,
                                    uuids_ignored_for_underreplication)) {
          rpcs.emplace_back(new AsyncAddReplicaTask(
              master_, tablet, cstate, RaftPeerPB::NON_VOTER, &rng_));
        }
      }
    }

    // 8. Send an AlterSchema RPC if the tablet has an old schema version.
    uint32_t table_schema_version = table->metadata().state().pb.version();
    if (report.has_schema_version() &&
        report.schema_version() != table_schema_version) {
      if (report.schema_version() > table_schema_version) {
        LOG(ERROR) << Substitute("TS $0 has reported a schema version greater "
            "than the current one for tablet $1. Expected version $2 got $3 (corruption)",
            ts_desc->ToString(), tablet->ToString(), table_schema_version,
            report.schema_version());
      } else {
        LOG(INFO) << Substitute("TS $0 does not have the latest schema for tablet $1. "
            "Expected version $2 got $3", ts_desc->ToString(), tablet->ToString(),
            table_schema_version, report.schema_version());
      }

      // It's possible that the tablet being reported is a laggy replica, and
      // in fact the leader has already received an AlterTable RPC. That's OK,
      // though -- it'll safely ignore it if we send another.
      rpcs.emplace_back(new AsyncAlterTable(master_, tablet));
    }

    // 9. If the tablet was mutated, add it to the tablets to be re-persisted.
    //
    // Done here and not on a per-mutation basis to avoid duplicate entries.
    if (tablet_was_mutated) {
      mutated_tablets.push_back(tablet);
    }

    // 10. Process the report's tablet statistics.
    //
    // The tserver only reports the LEADER replicas it owns.
    if (report.has_consensus_state() &&
        report.consensus_state().leader_uuid() == ts_desc->permanent_uuid()) {
      if (report.has_stats()) {
        // For the versions >= 1.11.x, the tserver reports stats. But keep in
        // mind that 'live_row_count' is not supported for the legacy replicas.
        tablet->table()->UpdateMetrics(tablet_id, tablet->GetStats(), report.stats());
        tablet->UpdateStats(report.stats());
      } else {
        // For the versions < 1.11.x, the tserver doesn't report stats. Thus,
        // the metrics from the stats should be hidden, for example, when it's
        // in the upgrade/downgrade process or in a mixed environment.
        tablet->table()->InvalidateMetrics(tablet_id);
      }
    }
  }

  // 11. Unlock the tables; we no longer need to access their state.
  tables_lock.Unlock();

  // 12. Write all tablet mutations to the catalog table.
  //
  // SysCatalogTable::Write will short-circuit the case where the data has not
  // in fact changed since the previous version and avoid any unnecessary
  // mutations. The generated sequence of actions may be split into multiple
  // writes to the system catalog tablet to keep the size of each write request
  // under the specified threshold.
  {
    SysCatalogTable::Actions actions;
    actions.tablets_to_update = std::move(mutated_tablets);
    // Updating the status of replicas on the same tablet server can be safely
    // chunked. Even if some chunks of the update fails, it should not lead to
    // bigger inconsistencies than simply not updating the status of a single
    // replica on that tablet server (i.e., rejecting the whole tablet report).
    // In addition, the nature of such failures is transient, and it's expected
    // that the next successfully processed tablet report from the tablet server
    // will fix the partial update.
    const auto write_mode = FLAGS_catalog_manager_enable_chunked_tablet_reports
        ? SysCatalogTable::WriteMode::CHUNKED
        : SysCatalogTable::WriteMode::ATOMIC;
    auto s = sys_catalog_->Write(std::move(actions), write_mode);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(ERROR) << Substitute(
          "Error updating tablets from $0: $1. Tablet report was: $2",
          ts_desc->permanent_uuid(), s.ToString(), SecureShortDebugString(full_report));
      return s;
    }
  }

  // Having successfully written the tablet mutations, this function cannot
  // fail from here on out.

  // 13. Publish the in-memory tablet mutations and release the locks.
  tablets_lock.Commit();

  // 14. Process all tablet schema version changes.
  //
  // This is separate from tablet state mutations because only tablet in-memory
  // state (and table on-disk state) is changed.
  for (const auto& e : tablet_infos) {
    const string& tablet_id = e.first;
    const scoped_refptr<TabletInfo>& tablet = e.second;
    const ReportedTabletPB& report = *FindOrDie(reports, tablet_id);
    if (report.has_schema_version()) {
      HandleTabletSchemaVersionReport(tablet, report.schema_version());
    }
  }

  // 15. Send all queued RPCs.
  for (auto& rpc : rpcs) {
    if (rpc->table()->ContainsTask(rpc->tablet_id(), rpc->description())) {
      // There are some tasks with the same tablet_id, alter type (and permanent_uuid
      // for some specific tasks) already running, here we just ignore the rpc to avoid
      // sending duplicate requests, maybe it will be sent the next time the tserver heartbeats.
      VLOG(1) << Substitute("Not sending duplicate request: $0", rpc->description());
      continue;
    }
    rpc->table()->AddTask(rpc->tablet_id(), rpc);
    WARN_NOT_OK(rpc->Run(), Substitute("Failed to send $0", rpc->description()));
  }

  return Status::OK();
}

int64_t CatalogManager::GetLatestNotificationLogEventId() {
  DCHECK(hms_catalog_);
  leader_lock_.AssertAcquiredForReading();
  return hms_notification_log_event_id_;
}

Status CatalogManager::InitLatestNotificationLogEventId() {
  DCHECK(hms_catalog_);
  leader_lock_.AssertAcquiredForWriting();
  int64_t hms_notification_log_event_id;
  RETURN_NOT_OK(sys_catalog_->GetLatestNotificationLogEventId(&hms_notification_log_event_id));
  hms_notification_log_event_id_ = hms_notification_log_event_id;
  return Status::OK();
}

Status CatalogManager::StoreLatestNotificationLogEventId(int64_t event_id) {
  DCHECK(hms_catalog_);
  DCHECK_GT(event_id, hms_notification_log_event_id_);
  leader_lock_.AssertAcquiredForReading();
  {
    SysCatalogTable::Actions actions;
    actions.hms_notification_log_event_id = event_id;
    RETURN_NOT_OK_PREPEND(sys_catalog()->Write(std::move(actions)),
                          "Failed to update processed Hive Metastore "
                          "notification log ID in the sys catalog table");
  }
  hms_notification_log_event_id_ = event_id;
  return Status::OK();
}

std::shared_ptr<RaftConsensus> CatalogManager::master_consensus() const {
  // CatalogManager::InitSysCatalogAsync takes lock_ in exclusive mode in order
  // to initialize sys_catalog_, so it's sufficient to take lock_ in shared mode
  // here to protect access to sys_catalog_.
  shared_lock<LockType> l(lock_);
  if (!sys_catalog_) {
    return nullptr;
  }
  return sys_catalog_->tablet_replica()->shared_consensus();
}

void CatalogManager::SendAlterTableRequest(const scoped_refptr<TableInfo>& table) {
  vector<scoped_refptr<TabletInfo>> tablets;
  table->GetAllTablets(&tablets);

  for (const scoped_refptr<TabletInfo>& tablet : tablets) {
    scoped_refptr<AsyncAlterTable> task = new AsyncAlterTable(master_, tablet);
    table->AddTask(tablet->id(), task);
    WARN_NOT_OK(task->Run(), "Failed to send alter table request");
  }
}

void CatalogManager::SendDeleteTableRequest(const scoped_refptr<TableInfo>& table,
                                            const string& deletion_msg) {
  vector<scoped_refptr<TabletInfo>> tablets;
  table->GetAllTablets(&tablets);

  for (const scoped_refptr<TabletInfo>& tablet : tablets) {
    TabletMetadataLock l(tablet.get(), LockMode::READ);
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
              << tablet->id();
    return;
  }
  const ConsensusStatePB& cstate = tablet_lock.data().pb.consensus_state();
  LOG_WITH_PREFIX(INFO)
      << "Sending DeleteTablet for " << cstate.committed_config().peers().size()
      << " replicas of tablet " << tablet->id();
  for (const auto& peer : cstate.committed_config().peers()) {
    scoped_refptr<AsyncDeleteReplica> task = new AsyncDeleteReplica(
        master_, peer.permanent_uuid(), tablet->table(), tablet->id(),
        TABLET_DATA_DELETED, none, deletion_msg);
    tablet->table()->AddTask(tablet->id(), task);
    WARN_NOT_OK(task->Run(), Substitute(
        "Failed to send DeleteReplica request for tablet $0", tablet->id()));
  }
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
    TableMetadataLock table_lock(table.get(), LockMode::READ);
    if (table_lock.data().is_deleted()) {
      continue;
    }

    vector<scoped_refptr<TabletInfo>> tablets;
    table->GetAllTablets(&tablets);
    for (const auto& tablet : tablets) {
      TabletMetadataLock tablet_lock(tablet.get(), LockMode::READ);
      if (tablet_lock.data().is_deleted() ||
          tablet_lock.data().is_running()) {
        continue;
      }
      tablets_to_process->emplace_back(tablet);
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
    *expired_entry_ids = std::move(ref);
  }
  return master_->token_signer()->ImportKeys(loader.entries());
}

Status CatalogManager::LoadTspkEntries(vector<TokenSigningPublicKeyPB>* keys) {
  TskEntryLoader loader;
  RETURN_NOT_OK(sys_catalog_->VisitTskEntries(&loader));
  for (const auto& private_key : loader.entries()) {
    // Extract public parts of the loaded keys for the verifier.
    TokenSigningPrivateKey tsk(private_key);
    TokenSigningPublicKeyPB key;
    tsk.ExportPublicKeyPB(&key);
    auto key_seq_num = key.key_seq_num();
    keys->emplace_back(std::move(key));
    VLOG(2) << "read public part of TSK " << key_seq_num;
  }
  return Status::OK();
}

Status CatalogManager::DeleteTskEntries(const set<string>& entry_ids) {
  leader_lock_.AssertAcquiredForWriting();
  RETURN_NOT_OK(sys_catalog_->RemoveTskEntries(entry_ids));
  string msg = "Deleted TSKs: ";
  msg += JoinMapped(
      entry_ids,
      [](const string& id) {
        return Substitute("$0", SysCatalogTable::TskEntryIdToSeqNumber(id));
      },
      " ");
  LOG_WITH_PREFIX(INFO) << msg;
  return Status::OK();
}

struct DeferredAssignmentActions {
  vector<scoped_refptr<TabletInfo>> tablets_to_add;
  vector<scoped_refptr<TabletInfo>> tablets_to_update;
  vector<scoped_refptr<TabletInfo>> needs_create_rpc;
};

void CatalogManager::HandleAssignPreparingTablet(const scoped_refptr<TabletInfo>& tablet,
                                                 DeferredAssignmentActions* deferred) {
  // The tablet was just created (probably by a CreateTable RPC).
  // Update the state to "creating" to be ready for the creation request.
  tablet->mutable_metadata()->mutable_dirty()->set_state(
    SysTabletsEntryPB::CREATING, "Sending initial creation of tablet");
  deferred->tablets_to_update.emplace_back(tablet);
  deferred->needs_create_rpc.emplace_back(tablet);
  VLOG(1) << "Assign new tablet " << tablet->ToString();
}

void CatalogManager::HandleAssignCreatingTablet(const scoped_refptr<TabletInfo>& tablet,
                                                DeferredAssignmentActions* deferred,
                                                scoped_refptr<TabletInfo>* new_tablet) {
  MonoDelta time_since_updated =
      MonoTime::Now() - tablet->last_create_tablet_time();
  int64_t remaining_timeout_ms =
      FLAGS_tablet_creation_timeout_ms - time_since_updated.ToMilliseconds();

  // Skip the tablet if the assignment timeout is not yet expired
  if (remaining_timeout_ms > 0) {
    VLOG(2) << Substitute("Tablet $0 still being created. $1ms remain until timeout",
                          tablet->ToString(), remaining_timeout_ms);
    return;
  }

  const PersistentTabletInfo& old_info = tablet->metadata().state();

  optional<string> dimension_label = old_info.pb.has_dimension_label() ?
      make_optional<string>(old_info.pb.dimension_label()) : none;
  // The "tablet creation" was already sent, but we didn't receive an answer
  // within the timeout. So the tablet will be replaced by a new one.
  scoped_refptr<TabletInfo> replacement = CreateTabletInfo(tablet->table(),
                                                           old_info.pb.partition(),
                                                           dimension_label);
  LOG_WITH_PREFIX(WARNING) << Substitute("Tablet $0 was not created within the "
      "allowed timeout. Replacing with a new tablet $1",
      tablet->ToString(), replacement->id());

  // Mark old tablet as replaced.
  tablet->mutable_metadata()->mutable_dirty()->set_state(
    SysTabletsEntryPB::REPLACED,
    Substitute("Replaced by $0 at $1",
               replacement->id(), LocalTimeAsString()));

  // Mark new tablet as being created.
  replacement->mutable_metadata()->mutable_dirty()->set_state(
    SysTabletsEntryPB::CREATING,
    Substitute("Replacement for $0", tablet->id()));

  deferred->tablets_to_update.emplace_back(tablet);
  deferred->tablets_to_add.emplace_back(replacement);
  deferred->needs_create_rpc.emplace_back(replacement);
  VLOG(1) << Substitute("Replaced tablet $0 with $1 (table $2)",
                        tablet->id(), replacement->id(),
                        tablet->table()->ToString());

  new_tablet->swap(replacement);
}

// TODO(unknown): we could batch the IO onto a background thread.
//                but this is following the current HandleReportedTablet()
void CatalogManager::HandleTabletSchemaVersionReport(
    const scoped_refptr<TabletInfo>& tablet,
    uint32_t version) {
  // Update the schema version if it's the latest
  tablet->set_reported_schema_version(version);

  // Verify if it's the last tablet report, and the alter completed.
  const scoped_refptr<TableInfo>& table = tablet->table();
  TableMetadataLock l(table.get(), LockMode::WRITE);
  if (l.data().is_deleted() || l.data().pb.state() != SysTablesEntryPB::ALTERING) {
    return;
  }

  uint32_t current_version = l.data().pb.version();
  if (table->IsAlterInProgress(current_version)) {
    return;
  }

  // Update the state from altering to running and remove the last fully
  // applied schema (if it exists).
  l.mutable_data()->pb.clear_fully_applied_schema();
  l.mutable_data()->set_state(SysTablesEntryPB::RUNNING,
                              Substitute("Current schema version=$0", current_version));

  {
    SysCatalogTable::Actions actions;
    actions.table_to_update = table;
    Status s = sys_catalog_->Write(std::move(actions));
    if (PREDICT_FALSE(!s.ok())) {
      LOG_WITH_PREFIX(WARNING)
          << "An error occurred while updating sys-tables: " << s.ToString();
      return;
    }
  }

  l.Commit();
  LOG_WITH_PREFIX(INFO) << Substitute("$0 alter complete (version $1)",
                                      table->ToString(), current_version);
}

Status CatalogManager::ProcessPendingAssignments(
    const vector<scoped_refptr<TabletInfo>>& tablets) {
  VLOG(1) << "Processing pending assignments";

  // Take write locks on all tablets to be processed, and ensure that they are
  // unlocked at the end of this scope.
  TabletMetadataGroupLock lock_in(LockMode::RELEASED);
  lock_in.AddMutableInfos(tablets);
  lock_in.Lock(LockMode::WRITE);

  DeferredAssignmentActions deferred;

  // Any tablets created by the helper functions will also be created in a
  // locked state, so we must ensure they are unlocked before we return to
  // avoid deadlocks.
  //
  // Must be declared after 'deferred' so that tablets are properly unlocked
  // before being destroyed.
  TabletMetadataGroupLock lock_out(LockMode::WRITE);

  // Iterate over each of the tablets and handle it, whatever state
  // it may be in. The actions required for the tablet are collected
  // into 'deferred'.
  for (const auto& tablet : tablets) {
    SysTabletsEntryPB::State t_state = tablet->metadata().state().pb.state();

    switch (t_state) {
      case SysTabletsEntryPB::PREPARING:
        HandleAssignPreparingTablet(tablet, &deferred);
        break;

      case SysTabletsEntryPB::CREATING:
      {
        scoped_refptr<TabletInfo> new_tablet;
        HandleAssignCreatingTablet(tablet, &deferred, &new_tablet);
        if (new_tablet) {
          lock_out.AddMutableInfo(new_tablet.get());
        }
        break;
      }
      default:
        VLOG(2) << Substitute("Nothing to do for tablet $0: $1", tablet->id(),
                              SysTabletsEntryPB_State_Name(t_state));
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
  {
    TSDescriptorVector ts_descs;
    master_->ts_manager()->GetDescriptorsAvailableForPlacement(&ts_descs);
    PlacementPolicy policy(std::move(ts_descs), &rng_);
    for (auto& tablet : deferred.needs_create_rpc) {
      // NOTE: if we fail to select replicas on the first pass (due to
      // insufficient Tablet Servers being online), we will still try
      // again unless the tablet/table creation is cancelled.
      RETURN_NOT_OK_PREPEND(SelectReplicasForTablet(policy, tablet.get()),
                            Substitute("error selecting replicas for tablet $0",
                                       tablet->id()));
    }
  }

  // Update the sys catalog with the new set of tablets/metadata.
  {
    SysCatalogTable::Actions actions;
    actions.tablets_to_add = deferred.tablets_to_add;
    actions.tablets_to_update = deferred.tablets_to_update;
    RETURN_NOT_OK_PREPEND(sys_catalog_->Write(std::move(actions)),
                          "error persisting updated tablet metadata");
  }

  // Expose tablet metadata changes before the new tablets themselves.
  lock_out.Commit();
  lock_in.Commit();

  for (const auto& t : deferred.tablets_to_add) {
    // We can't reuse the WRITE tablet locks from committer_out for this
    // because AddRemoveTablets() will read from the clean state, which is
    // empty for these brand new tablets.
    TabletMetadataLock l(t.get(), LockMode::READ);
    t->table()->AddRemoveTablets({ t }, {});
  }

  // Acquire the global lock to publish the new tablets.
  {
    std::lock_guard<LockType> l(lock_);
    for (const auto& t : deferred.tablets_to_add) {
      tablet_map_[t->id()] = t;
    }
  }

  // Send DeleteTablet requests to tablet servers serving deleted tablets.
  // This is asynchronous / non-blocking.
  for (const auto& tablet : deferred.tablets_to_update) {
    TabletMetadataLock l(tablet.get(), LockMode::READ);
    if (l.data().is_deleted()) {
      SendDeleteTabletRequest(tablet, l, l.data().pb.state_msg());
    }
  }
  // Send the CreateTablet() requests to the servers. This is asynchronous / non-blocking.
  for (const auto& tablet : deferred.needs_create_rpc) {
    TabletMetadataLock l(tablet.get(), LockMode::READ);
    SendCreateTabletRequest(tablet, l);
  }
  return Status::OK();
}

Status CatalogManager::SelectReplicasForTablet(const PlacementPolicy& policy,
                                               TabletInfo* tablet) {
  DCHECK(tablet);
  TableMetadataLock table_guard(tablet->table().get(), LockMode::READ);

  if (!table_guard.data().pb.IsInitialized()) {
    return Status::InvalidArgument(
        Substitute("TableInfo for tablet $0 is not initialized (aborted CreateTable attempt?)",
                   tablet->id()));
  }

  const auto nreplicas = table_guard.data().pb.num_replicas();
  if (policy.ts_num() < nreplicas) {
    return Status::InvalidArgument(
        Substitute("Not enough tablet servers are online for table '$0'. Need at least $1 "
                   "replicas, but only $2 tablet servers are available",
                   table_guard.data().name(), nreplicas, policy.ts_num()));
  }

  ConsensusStatePB* cstate = tablet->mutable_metadata()->
      mutable_dirty()->pb.mutable_consensus_state();
  cstate->set_current_term(kMinimumTerm);
  RaftConfigPB* config = cstate->mutable_committed_config();
  DCHECK_EQ(0, config->peers_size()) << "RaftConfig not empty: "
                                     << SecureShortDebugString(*config);
  config->clear_peers();
  // Maintain ability to downgrade Kudu to a version with LocalConsensus.
  config->set_obsolete_local(nreplicas == 1);
  config->set_opid_index(consensus::kInvalidOpIdIndex);

  // Get the dimension of the tablet. Otherwise, it will be none.
  optional<string> dimension = none;
  if (tablet->metadata().state().pb.has_dimension_label()) {
    dimension = tablet->metadata().state().pb.dimension_label();
  }

  // Select the set of replicas for the tablet.
  TSDescriptorVector descriptors;
  RETURN_NOT_OK_PREPEND(policy.PlaceTabletReplicas(nreplicas, dimension, &descriptors),
                        Substitute("failed to place replicas for tablet $0 "
                                   "(table '$1')",
                                   tablet->id(), table_guard.data().name()));
  for (const auto& desc : descriptors) {
    ServerRegistrationPB reg;
    desc->GetRegistration(&reg);

    RaftPeerPB* peer = config->add_peers();
    peer->set_member_type(RaftPeerPB::VOTER);
    peer->set_permanent_uuid(desc->permanent_uuid());

    for (const HostPortPB& addr : reg.rpc_addresses()) {
      peer->mutable_last_known_addr()->CopyFrom(addr);
    }
  }

  return Status::OK();
}

void CatalogManager::SendCreateTabletRequest(const scoped_refptr<TabletInfo>& tablet,
                                             const TabletMetadataLock& tablet_lock) {
  const RaftConfigPB& config =
      tablet_lock.data().pb.consensus_state().committed_config();
  tablet->set_last_create_tablet_time(MonoTime::Now());
  for (const RaftPeerPB& peer : config.peers()) {
    scoped_refptr<AsyncCreateReplica> task = new AsyncCreateReplica(
        master_, peer.permanent_uuid(), tablet, tablet_lock);
    tablet->table()->AddTask(tablet->id(), task);
    WARN_NOT_OK(task->Run(), "Failed to send new tablet request");
  }
}

Status CatalogManager::BuildLocationsForTablet(
    const scoped_refptr<TabletInfo>& tablet,
    ReplicaTypeFilter filter,
    TabletLocationsPB* locs_pb,
    TSInfosDict* ts_infos_dict) {
  TabletMetadataLock l_tablet(tablet.get(), LockMode::READ);
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
    DCHECK(!peer.has_health_report()); // Health report shouldn't be persisted.
    switch (filter) {
      case VOTER_REPLICA:
        if (!peer.has_member_type() ||
            peer.member_type() != consensus::RaftPeerPB::VOTER) {
          // Jump to the next iteration of the outside cycle.
          continue;
        }
        break;

      case ANY_REPLICA:
        break;

      default:
        {
          const string err_msg = Substitute(
              "$0: unsupported replica type filter", filter);
          LOG(DFATAL) << err_msg;
          return Status::InvalidArgument(err_msg);
        }
    }

    // Helper function to create a TSInfoPB.
    auto make_tsinfo_pb = [this, &peer]() {
      unique_ptr<TSInfoPB> tsinfo_pb(new TSInfoPB);
      tsinfo_pb->set_permanent_uuid(peer.permanent_uuid());
      shared_ptr<TSDescriptor> ts_desc;
      if (master_->ts_manager()->LookupTSByUUID(peer.permanent_uuid(), &ts_desc)) {
        ServerRegistrationPB reg;
        ts_desc->GetRegistration(&reg);
        tsinfo_pb->mutable_rpc_addresses()->Swap(reg.mutable_rpc_addresses());
        if (reg.has_unix_domain_socket_path()) {
          tsinfo_pb->set_unix_domain_socket_path(reg.unix_domain_socket_path());
        }
        if (ts_desc->location()) tsinfo_pb->set_location(*(ts_desc->location()));
      } else {
        // If we've never received a heartbeat from the tserver, we'll fall back
        // to the last known RPC address in the RaftPeerPB.
        //
        // TODO(wdberkeley): We should track these RPC addresses in the master table itself.
        tsinfo_pb->add_rpc_addresses()->CopyFrom(peer.last_known_addr());
      }
      return tsinfo_pb;
    };

    const auto role = GetParticipantRole(peer, cstate);
    optional<string> dimension = none;
    if (l_tablet.data().pb.has_dimension_label()) {
      dimension = l_tablet.data().pb.dimension_label();
    }
    if (ts_infos_dict) {
      const auto idx = *ComputePairIfAbsent(
          &ts_infos_dict->uuid_to_idx, peer.permanent_uuid(),
          [&]() -> pair<StringPiece, int> {
            auto& ts_info_pbs = ts_infos_dict->ts_info_pbs;
            auto ts_info_idx = ts_info_pbs.size();
            ts_info_pbs.emplace_back(make_tsinfo_pb().release());
            return { ts_info_pbs.back()->permanent_uuid(), ts_info_idx };
          });

      auto* interned_replica_pb = locs_pb->add_interned_replicas();
      interned_replica_pb->set_ts_info_idx(idx);
      interned_replica_pb->set_role(role);
      if (dimension) {
        interned_replica_pb->set_dimension_label(*dimension);
      }
    } else {
      TabletLocationsPB_DEPRECATED_ReplicaPB* replica_pb = locs_pb->add_deprecated_replicas();
      replica_pb->set_allocated_ts_info(make_tsinfo_pb().release());
      replica_pb->set_role(role);
    }
  }

  locs_pb->mutable_partition()->CopyFrom(tablet->metadata().state().pb.partition());
  locs_pb->set_tablet_id(tablet->id());

  // No longer used; always set to false.
  locs_pb->set_deprecated_stale(false);

  return Status::OK();
}

Status CatalogManager::GetTabletLocations(const string& tablet_id,
                                          ReplicaTypeFilter filter,
                                          TabletLocationsPB* locs_pb,
                                          TSInfosDict* ts_infos_dict,
                                          optional<const string&> user) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  locs_pb->mutable_deprecated_replicas()->Clear();
  locs_pb->mutable_interned_replicas()->Clear();
  scoped_refptr<TabletInfo> tablet_info;
  {
    shared_lock<LockType> l(lock_);
    // It's OK to return NOT_FOUND back to the client, even with authorization enabled,
    // because tablet IDs are randomly generated and don't carry user data.
    if (!FindCopy(tablet_map_, tablet_id, &tablet_info)) {
      return Status::NotFound(Substitute("Unknown tablet $0", tablet_id));
    }
  }
  if (user) {
    // Acquire the table lock and then check that the user is authorized to operate on
    // the table that the tablet belongs to.
    TableMetadataLock table_lock(tablet_info->table().get(), LockMode::READ);
    RETURN_NOT_OK(authz_provider_->AuthorizeGetTableMetadata(
        NormalizeTableName(table_lock.data().name()), *user));
  }

  return BuildLocationsForTablet(tablet_info, filter, locs_pb, ts_infos_dict);
}

Status CatalogManager::ReplaceTablet(const string& tablet_id, ReplaceTabletResponsePB* resp) {
  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // Lookup the tablet-to-be-replaced and get its table.
  scoped_refptr<TabletInfo> old_tablet;
  {
    shared_lock<LockType> l(lock_);
    if (!FindCopy(tablet_map_, tablet_id, &old_tablet)) {
      return Status::NotFound(Substitute("Unknown tablet $0", tablet_id));
    }
  }
  scoped_refptr<TableInfo> table = old_tablet->table();

  // Lock the tablet-to-be-replaced (the "old" tablet).
  // We don't need to lock the table because we are not modifying its TableInfo.
  TabletMetadataLock l_old_tablet(old_tablet.get(), LockMode::WRITE);

  // It's possible that between when we looked up the old tablet and when we
  // acquired its lock that the old tablet was deleted.
  if (old_tablet->metadata().state().is_deleted()) {
    return Status::NotFound(Substitute("Tablet $0 already deleted", tablet_id));
  }

  // Create the TabletInfo for the replacement tablet.
  const SysTabletsEntryPB& replaced_pb = l_old_tablet.data().pb;
  scoped_refptr<TabletInfo> new_tablet(new TabletInfo(table, GenerateId()));
  TabletMetadataLock l_new_tablet(new_tablet.get(), LockMode::WRITE);
  SysTabletsEntryPB* new_metadata = &new_tablet->mutable_metadata()->mutable_dirty()->pb;
  new_metadata->set_state(SysTabletsEntryPB::PREPARING);
  new_metadata->mutable_partition()->CopyFrom(replaced_pb.partition());
  new_metadata->set_table_id(table->id());
  if (replaced_pb.has_dimension_label()) {
    new_metadata->set_dimension_label(replaced_pb.dimension_label());
  }

  const string replace_msg = Substitute("replaced by tablet $0", new_tablet->id());
  old_tablet->mutable_metadata()->mutable_dirty()->set_state(SysTabletsEntryPB::DELETED,
                                                             replace_msg);

  // Persist the changes to the syscatalog table.
  {
    SysCatalogTable::Actions actions;
    actions.tablets_to_add.push_back(new_tablet);
    actions.tablets_to_update.push_back(old_tablet);
    Status s = sys_catalog_->Write(std::move(actions));
    if (PREDICT_FALSE(!s.ok())) {
      s = s.CloneAndPrepend("an error occurred while writing to the sys-catalog");
      LOG(WARNING) << s.ToString();
      CheckIfNoLongerLeaderAndSetupError(s, resp);
      return s;
    }
  }

  // Now commit the in-memory state and modify the global tablet map.
  // The order of operations here is based on AlterTable.

  // Commit the in-memory state of the new tablet. This doesn't require the global
  // lock because the new tablet is not visible yet.
  l_new_tablet.Commit();

  // Add the new tablet to the global tablet map.
  {
    std::lock_guard<LockType> l(lock_);
    InsertOrDie(&tablet_map_, new_tablet->id(), new_tablet);
  }

  // Next, add the new tablet and remove the old tablet from the table.
  {
    TabletMetadataLock l_new_tablet(new_tablet.get(), LockMode::READ);
    table->AddRemoveTablets({new_tablet}, {old_tablet});
  }

  // Commit state changes for the old tablet.
  l_old_tablet.Commit();

  // Finish up by kicking off the delete of the old tablet.
  {
    TabletMetadataLock l_old_tablet(old_tablet.get(), LockMode::READ);
    SendDeleteTabletRequest(old_tablet, l_old_tablet, replace_msg);
    background_tasks_->Wake();
  }

  LOG(INFO) << "ReplaceTablet: tablet " << old_tablet->id()
            << " deleted and replaced by tablet " << new_tablet->id();
  resp->set_replacement_tablet_id(new_tablet->id());
  return Status::OK();
}

Status CatalogManager::GetTableLocations(const GetTableLocationsRequestPB* req,
                                         GetTableLocationsResponsePB* resp,
                                         optional<const string&> user) {
  // If start-key is > end-key report an error instead of swapping the two
  // since probably there is something wrong app-side.
  if (req->has_partition_key_start() && req->has_partition_key_end()
      && req->partition_key_start() > req->partition_key_end()) {
    return Status::InvalidArgument("start partition key is greater than the end partition key");
  }
  if (req->max_returned_locations() <= 0) {
    return Status::InvalidArgument("max_returned_locations must be greater than 0");
  }

  leader_lock_.AssertAcquiredForReading();
  RETURN_NOT_OK(CheckOnline());

  // Lookup the table, verify if it exists, and then check that
  // the user is authorized to operate on the table.
  scoped_refptr<TableInfo> table;
  TableMetadataLock l;
  auto authz_func = [&] (const string& username, const string& table_name) {
    return SetupError(authz_provider_->AuthorizeGetTableMetadata(table_name, username),
                      resp, MasterErrorPB::NOT_AUTHORIZED);
  };
  RETURN_NOT_OK(FindLockAndAuthorizeTable(*req, resp, LockMode::READ, authz_func, user,
                                          &table, &l));
  RETURN_NOT_OK(CheckIfTableDeletedOrNotRunning(&l, resp));

  vector<scoped_refptr<TabletInfo>> tablets_in_range;
  table->GetTabletsInRange(req, &tablets_in_range);

  TSInfosDict infos_dict;

  for (const auto& tablet : tablets_in_range) {
    Status s = BuildLocationsForTablet(
        tablet, req->replica_type_filter(), resp->add_tablet_locations(),
        req->intern_ts_infos_in_response() ? &infos_dict : nullptr);
    if (s.ok()) {
      continue;
    }
    if (s.IsNotFound()) {
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
  for (auto& pb : infos_dict.ts_info_pbs) {
    resp->mutable_ts_infos()->AddAllocated(pb.release());
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
    names_copy = normalized_table_names_map_;
    tablets_copy = tablet_map_;
    // TODO(aserbin): add information about root CA certs, if any
  }

  *out << "Tables:\n";
  for (const TableInfoMap::value_type& e : ids_copy) {
    const scoped_refptr<TableInfo>& table = e.second;
    TableMetadataLock l(table.get(), LockMode::READ);
    const string& name = l.data().name();

    *out << table->id() << ":\n";
    *out << "  name: \"" << strings::CHexEscape(name) << "\"\n";
    // Erase from the map, so later we can check that we don't have
    // any orphaned tables in the by-name map that aren't in the
    // by-id map.
    if (names_copy.erase(name) != 1) {
      *out << "  [not present in by-name map]\n";
    }
    *out << "  metadata: " << SecureShortDebugString(l.data().pb) << "\n";

    *out << "  tablets:\n";

    vector<scoped_refptr<TabletInfo>> tablets;
    table->GetAllTablets(&tablets);
    for (const auto& tablet : tablets) {
      TabletMetadataLock l_tablet(tablet.get(), LockMode::READ);
      *out << "    " << tablet->id() << ": "
           << SecureShortDebugString(l_tablet.data().pb) << "\n";

      if (tablets_copy.erase(tablet->id()) != 1) {
        *out << "  [ERROR: not present in CM tablet map!]\n";
      }
    }
  }

  if (!tablets_copy.empty()) {
    *out << "Orphaned tablets (not referenced by any table):\n";
    for (const auto& entry : tablets_copy) {
      const scoped_refptr<TabletInfo>& tablet = entry.second;
      TabletMetadataLock l_tablet(tablet.get(), LockMode::READ);
      *out << "    " << tablet->id() << ": "
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

string CatalogManager::LogPrefix() const {
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

template<typename RespClass>
Status CatalogManager::WaitForNotificationLogListenerCatchUp(RespClass* resp,
                                                             rpc::RpcContext* rpc) {
  if (hms_catalog_) {
    CHECK(rpc);
    Status s = hms_notification_log_listener_->WaitForCatchUp(rpc->GetClientDeadline());
    // ServiceUnavailable indicates the master has lost leadership.
    MasterErrorPB::Code code = s.IsServiceUnavailable() ?
      MasterErrorPB::NOT_THE_LEADER :
      MasterErrorPB::HIVE_METASTORE_ERROR;
    return SetupError(s, resp, code);
  }
  return Status::OK();
}

string CatalogManager::NormalizeTableName(const string& table_name) {
  // Force a deep copy on platforms with reference counted strings.
  string normalized_table_name(table_name.data(), table_name.size());
  if (hms::HmsCatalog::IsEnabled()) {
    // If HmsCatalog::NormalizeTableName returns an error, the table name is not
    // modified. In this case the table is guaranteed to be a legacy table which
    // has survived since before the cluster was configured to integrate with
    // the HMS. It's safe to use the unmodified table name as the normalized
    // name in this case, since there cannot be a name conflict with a table in
    // the HMS. When the table gets 'upgraded' to be included in the HMS it will
    // need to be renamed with a Hive compatible name.
    //
    // Note: not all legacy tables will fail normalization; if a table happens
    // to be named with a Hive compatible name ("Legacy.Table"), it will be
    // normalized according to the Hive rules ("legacy.table"). We check in
    // TableLoader::VisitTables that such legacy tables do not have conflicting
    // names when normalized.
    ignore_result(hms::HmsCatalog::NormalizeTableName(&normalized_table_name));
  }
  return normalized_table_name;
}

const char* CatalogManager::StateToString(State state) {
  switch (state) {
    case CatalogManager::kConstructed: return "Constructed";
    case CatalogManager::kStarting: return "Starting";
    case CatalogManager::kRunning: return "Running";
    case CatalogManager::kClosing: return "Closing";
  }
  __builtin_unreachable();
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
  int64_t leader_ready_term;
  {
    std::lock_guard<simple_spinlock> l(catalog_->state_lock_);
    if (PREDICT_FALSE(catalog_->state_ != kRunning)) {
      catalog_status_ = Status::ServiceUnavailable(
          Substitute("Catalog manager is not initialized. State: $0",
                     StateToString(catalog_->state_)));
      return;
    }
    leader_ready_term = catalog_->leader_ready_term_;
  }

  ConsensusStatePB cstate;
  Status s = catalog_->sys_catalog_->tablet_replica()->consensus()->ConsensusState(&cstate);
  if (PREDICT_FALSE(!s.ok())) {
    DCHECK(s.IsIllegalState()) << s.ToString();
    catalog_status_ = s.CloneAndPrepend("ConsensusState is not available");
    return;
  }

  catalog_status_ = Status::OK();

  // Check if the catalog manager is the leader.
  initial_term_ = cstate.current_term();
  const string& uuid = catalog_->master_->fs_manager()->uuid();
  if (PREDICT_FALSE(cstate.leader_uuid() != uuid)) {
    leader_status_ = Status::IllegalState(
        Substitute("Not the leader. Local UUID: $0, Raft Consensus state: $1",
                   uuid, SecureShortDebugString(cstate)));
    return;
  }
  if (PREDICT_FALSE(leader_ready_term != initial_term_ ||
                    !leader_shared_lock_.owns_lock())) {
    leader_status_ = Status::ServiceUnavailable(
        "Leader not yet ready to serve requests");
    return;
  }
  leader_status_ = Status::OK();
}

bool CatalogManager::ScopedLeaderSharedLock::has_term_changed() const {
  DCHECK(leader_status().ok());
  const auto current_term = catalog_->sys_catalog_->tablet_replica()->consensus()->CurrentTerm();
  return current_term != initial_term_;
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
      RespClass* resp, RpcContext* rpc) /* NOLINT */
#define INITTED_AND_LEADER_OR_RESPOND(RespClass) \
  template bool \
  CatalogManager::ScopedLeaderSharedLock::CheckIsInitializedAndIsLeaderOrRespond( \
      RespClass* resp, RpcContext* rpc) /* NOLINT */

INITTED_OR_RESPOND(ConnectToMasterResponsePB);
INITTED_OR_RESPOND(GetMasterRegistrationResponsePB);
INITTED_OR_RESPOND(TSHeartbeatResponsePB);
INITTED_AND_LEADER_OR_RESPOND(AlterTableResponsePB);
INITTED_AND_LEADER_OR_RESPOND(ChangeTServerStateResponsePB);
INITTED_AND_LEADER_OR_RESPOND(CreateTableResponsePB);
INITTED_AND_LEADER_OR_RESPOND(DeleteTableResponsePB);
INITTED_AND_LEADER_OR_RESPOND(IsAlterTableDoneResponsePB);
INITTED_AND_LEADER_OR_RESPOND(IsCreateTableDoneResponsePB);
INITTED_AND_LEADER_OR_RESPOND(ListTablesResponsePB);
INITTED_AND_LEADER_OR_RESPOND(GetTableLocationsResponsePB);
INITTED_AND_LEADER_OR_RESPOND(GetTableSchemaResponsePB);
INITTED_AND_LEADER_OR_RESPOND(GetTableStatisticsResponsePB);
INITTED_AND_LEADER_OR_RESPOND(GetTabletLocationsResponsePB);
INITTED_AND_LEADER_OR_RESPOND(ReplaceTabletResponsePB);

#undef INITTED_OR_RESPOND
#undef INITTED_AND_LEADER_OR_RESPOND

////////////////////////////////////////////////////////////
// TabletInfo
////////////////////////////////////////////////////////////

TabletInfo::TabletInfo(scoped_refptr<TableInfo> table, string tablet_id)
    : tablet_id_(std::move(tablet_id)),
      table_(std::move(table)),
      last_create_tablet_time_(MonoTime::Now()),
      reported_schema_version_(NOT_YET_REPORTED) {}

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

void TabletInfo::set_reported_schema_version(int64_t version) {
  {
    std::lock_guard<simple_spinlock> l(lock_);

    // Fast path: there's no schema version change.
    if (version <= reported_schema_version_) {
      return;
    }
  }

  // Slow path: we have a schema version change.
  //
  // We need to hold both the table and tablet spinlocks to make the change. By
  // convention, table locks are always acquired first.
  //
  // We also need to hold the tablet metadata lock in order to read the partition
  // key, but it's OK to make a local copy of it (and release the lock) because
  // the key is immutable.
  string key_start;
  {
    TabletMetadataLock l(this, LockMode::READ);
    key_start = l.data().pb.partition().partition_key_start();
  }
  std::lock_guard<rw_spinlock> table_l(table_->lock_);
  std::lock_guard<simple_spinlock> tablet_l(lock_);

  // Check again in case the schema version changed underneath us.
  int64_t old_version = reported_schema_version_;
  if (version <= old_version) {
    return;
  }

  // Check that we weren't dropped from the table before acquiring the table lock.
  //
  // We also have to compare the returned object to 'this' in case our entry in
  // the map was replaced with a new tablet (i.e. DROP RANGE PARTITION followed
  // by ADD RANGE PARTITION).
  auto* t = FindPtrOrNull(table_->tablet_map_, key_start);
  if (!t || t != this) {
    return;
  }

  // Perform the changes.
  VLOG(3) << Substitute("$0: schema version changed from $1 to $2",
                        ToString(), old_version, version);
  reported_schema_version_ = version;
  table_->DecrementSchemaVersionCountUnlocked(old_version);
  table_->IncrementSchemaVersionCountUnlocked(version);
}

int64_t TabletInfo::reported_schema_version() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return reported_schema_version_;
}

string TabletInfo::ToString() const {
  return Substitute("$0 (table $1)", tablet_id_,
                    (table_ != nullptr ? table_->ToString() : "MISSING"));
}


void TabletInfo::UpdateStats(ReportedTabletStatsPB stats) {
  std::lock_guard<simple_spinlock> l(lock_);
  stats_ = std::move(stats);
}

ReportedTabletStatsPB TabletInfo::GetStats() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return stats_;
}

void PersistentTabletInfo::set_state(SysTabletsEntryPB::State state, const string& msg) {
  pb.set_state(state);
  pb.set_state_msg(msg);
}

////////////////////////////////////////////////////////////
// TableInfo
////////////////////////////////////////////////////////////

TableInfo::TableInfo(string table_id) : table_id_(std::move(table_id)) {}

TableInfo::~TableInfo() {
}

string TableInfo::ToString() const {
  TableMetadataLock l(this, LockMode::READ);
  return Substitute("$0 [id=$1]", l.data().pb.name(), table_id_);
}

uint32_t TableInfo::schema_version() const {
  TableMetadataLock l(this, LockMode::READ);
  return l.data().pb.version();
}

void TableInfo::AddRemoveTablets(const vector<scoped_refptr<TabletInfo>>& tablets_to_add,
                                 const vector<scoped_refptr<TabletInfo>>& tablets_to_drop) {
  std::lock_guard<rw_spinlock> l(lock_);
  for (const auto& tablet : tablets_to_drop) {
    const auto& lower_bound = tablet->metadata().state().pb.partition().partition_key_start();
    CHECK(EraseKeyReturnValuePtr(&tablet_map_, lower_bound) != nullptr);
    DecrementSchemaVersionCountUnlocked(tablet->reported_schema_version());
    // Remove the table metrics for the deleted tablets.
    RemoveMetrics(tablet->id(), tablet->GetStats());
  }
  for (const auto& tablet : tablets_to_add) {
    TabletInfo* old = nullptr;
    if (UpdateReturnCopy(&tablet_map_,
                         tablet->metadata().state().pb.partition().partition_key_start(),
                         tablet.get(), &old)) {
      VLOG(1) << Substitute("Replaced tablet $0 with $1",
                            old->id(), tablet->id());
      DecrementSchemaVersionCountUnlocked(old->reported_schema_version());

      // TODO(unknown): can we assert that the replaced tablet is not in Running state?
      // May be a little tricky since we don't know whether to look at its committed or
      // uncommitted state.
    }
    IncrementSchemaVersionCountUnlocked(tablet->reported_schema_version());
  }

#ifndef NDEBUG
  if (tablet_map_.empty()) {
    DCHECK(schema_version_counts_.empty());
  }
#endif
}

void TableInfo::GetTabletsInRange(const GetTableLocationsRequestPB* req,
                                  vector<scoped_refptr<TabletInfo>>* ret) const {
  shared_lock<rw_spinlock> l(lock_);
  int max_returned_locations = req->max_returned_locations();

  RawTabletInfoMap::const_iterator it, it_end;
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
    ret->emplace_back(make_scoped_refptr(it->second));
    count++;
  }
}

bool TableInfo::IsAlterInProgress(uint32_t version) const {
  shared_lock<rw_spinlock> l(lock_);
  auto it = schema_version_counts_.begin();
  if (it == schema_version_counts_.end()) {
    // The table has no tablets.
    return false;
  }
  DCHECK_GT(it->second, 0);

  // 'it->first' is either NOT_YET_REPORTED (if at least one tablet has yet to
  // report), or it's the lowest schema version belonging to at least one
  // tablet. The numeric value of NOT_YET_REPORTED is -1 so we can compare it
  // to 'version' either way.
  return it->first < static_cast<int64_t>(version);
}

bool TableInfo::IsCreateInProgress() const {
  shared_lock<rw_spinlock> l(lock_);
  for (const auto& e : tablet_map_) {
    TabletMetadataLock tablet_lock(e.second, LockMode::READ);
    if (!tablet_lock.data().is_running()) {
      return true;
    }
  }
  return false;
}

void TableInfo::AddTask(const string& tablet_id, const scoped_refptr<MonitoredTask>& task) {
  std::lock_guard<rw_spinlock> l(lock_);
  pending_tasks_.emplace(tablet_id, task);
}

void TableInfo::RemoveTask(const string& tablet_id, MonitoredTask* task) {
  std::lock_guard<rw_spinlock> l(lock_);
  auto range = pending_tasks_.equal_range(tablet_id);
  for (auto it = range.first; it != range.second; ++it) {
    if (it->second.get() == task) {
      pending_tasks_.erase(it);
      break;
    }
  }
}

void TableInfo::AbortTasks() {
  shared_lock<rw_spinlock> l(lock_);
  for (auto& task : pending_tasks_) {
    task.second->Abort();
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

bool TableInfo::ContainsTask(const string& tablet_id, const string& task_description) {
  shared_lock<rw_spinlock> l(lock_);
  auto range = pending_tasks_.equal_range(tablet_id);
  for (auto it = range.first; it != range.second; ++it) {
    if (it->second->description() == task_description) {
      return true;
    }
  }
  return false;
}

void TableInfo::GetTaskList(vector<scoped_refptr<MonitoredTask>>* tasks) {
  tasks->clear();
  {
    shared_lock<rw_spinlock> l(lock_);
    for (const auto& task : pending_tasks_) {
      tasks->push_back(task.second);
    }
  }
}

void TableInfo::GetAllTablets(vector<scoped_refptr<TabletInfo>>* ret) const {
  ret->clear();
  shared_lock<rw_spinlock> l(lock_);
  for (const auto& e : tablet_map_) {
    ret->emplace_back(make_scoped_refptr(e.second));
  }
}

void TableInfo::RegisterMetrics(MetricRegistry* metric_registry, const string& table_name) {
  if (metric_registry) {
    MetricEntity::AttributeMap attrs;
    attrs["table_name"] = table_name;
    metric_entity_ = METRIC_ENTITY_table.Instantiate(metric_registry, table_id_, attrs);
    metrics_.reset(new TableMetrics(metric_entity_));
  }
}

void TableInfo::UnregisterMetrics() {
  if (metric_entity_) {
    metric_entity_->Unpublish();
  }
}

void TableInfo::UpdateMetrics(const string& tablet_id,
                              const tablet::ReportedTabletStatsPB& old_stats,
                              const tablet::ReportedTabletStatsPB& new_stats) {
  if (!metrics_) return;

  if (PREDICT_TRUE(!metrics_->on_disk_size->IsInvisible())) {
    metrics_->on_disk_size->IncrementBy(
        static_cast<int64_t>(new_stats.on_disk_size()) -
        static_cast<int64_t>(old_stats.on_disk_size()));
  } else {
    // When is the 'on disk size' invisible?
    // 1. there is a tablet(legacy or not) under the old tserver version;
    if (metrics_->ContainsTabletNoOnDiskSize(tablet_id)) {
      metrics_->DeleteTabletNoOnDiskSize(tablet_id);
      // The tserver version has been updated since the 'on_disk_size' of the
      // tablet was not supported before but now it is supported, so we need
      // to check that the metric could be visible.
      if (metrics_->TableSupportsOnDiskSize()) {
        DCHECK(new_stats.has_on_disk_size());
        uint64_t on_disk_size = new_stats.on_disk_size();
        {
          std::lock_guard<rw_spinlock> l(lock_);
          for (const auto& e : tablet_map_) {
            if (e.first != tablet_id) {
              on_disk_size += e.second->GetStats().on_disk_size();
            }
          }
        }
        // Set the metric value and it will be visible again.
        metrics_->on_disk_size->set_value(static_cast<int64_t>(on_disk_size));
      }
    }
  }

  if (PREDICT_TRUE(!metrics_->live_row_count->IsInvisible())) {
    if (new_stats.has_live_row_count()) {
      metrics_->live_row_count->IncrementBy(
          static_cast<int64_t>(new_stats.live_row_count()) -
          static_cast<int64_t>(old_stats.live_row_count()));
    } else {
      // The legacy tablet makes the metric invisible by invalidating the epoch.
      metrics_->AddTabletNoLiveRowCount(tablet_id);
      metrics_->live_row_count->InvalidateEpoch();
    }
  } else {
    // When is the 'live row count' invisible?
    // 1. there is a legacy tablet under the new tserver version;
    // 2. there is a newly created tablet which has 'live_row_count',
    //    but the tserver rolls back to the old version;
    if (metrics_->ContainsTabletNoLiveRowCount(tablet_id) && new_stats.has_live_row_count()) {
      // It is case 2 and the tserver version has been updated.
      metrics_->DeleteTabletNoLiveRowCount(tablet_id);
      if (metrics_->TableSupportsLiveRowCount()) {
        uint64_t live_row_count = new_stats.live_row_count();
        {
          std::lock_guard<rw_spinlock> l(lock_);
          for (const auto& e : tablet_map_) {
            if (e.first != tablet_id) {
              live_row_count += e.second->GetStats().live_row_count();
            }
          }
        }
        metrics_->live_row_count->set_value(static_cast<int64_t>(live_row_count));
      }
    }
  }
}

void TableInfo::InvalidateMetrics(const std::string& tablet_id) {
  if (!metrics_) return;
  if (!metrics_->ContainsTabletNoOnDiskSize(tablet_id)) {
    metrics_->AddTabletNoOnDiskSize(tablet_id);
    metrics_->on_disk_size->InvalidateEpoch();
  }
  if (!metrics_->ContainsTabletNoLiveRowCount(tablet_id)) {
    metrics_->AddTabletNoLiveRowCount(tablet_id);
    metrics_->live_row_count->InvalidateEpoch();
  }
}

void TableInfo::RemoveMetrics(const string& tablet_id,
                              const tablet::ReportedTabletStatsPB& old_stats) {
  DCHECK(lock_.is_locked());
  if (!metrics_) return;

  if (PREDICT_TRUE(!metrics_->on_disk_size->IsInvisible())) {
    metrics_->on_disk_size->IncrementBy(-static_cast<int64_t>(old_stats.on_disk_size()));
  } else {
    if (metrics_->ContainsTabletNoOnDiskSize(tablet_id)) {
      metrics_->DeleteTabletNoOnDiskSize(tablet_id);
      if (metrics_->TableSupportsOnDiskSize()) {
        uint64_t on_disk_size = 0;
        for (const auto& e : tablet_map_) {
          on_disk_size += e.second->GetStats().on_disk_size();
        }
        metrics_->on_disk_size->set_value(static_cast<int64_t>(on_disk_size));
      }
    }
  }

  if (PREDICT_TRUE(!metrics_->live_row_count->IsInvisible())) {
    metrics_->live_row_count->IncrementBy(-static_cast<int64_t>(old_stats.live_row_count()));
  } else {
    if (metrics_->ContainsTabletNoLiveRowCount(tablet_id)) {
      metrics_->DeleteTabletNoLiveRowCount(tablet_id);
      if (metrics_->TableSupportsLiveRowCount()) {
        uint64_t live_row_count = 0;
        for (const auto& e : tablet_map_) {
          live_row_count += e.second->GetStats().live_row_count();
        }
        metrics_->live_row_count->set_value(static_cast<int64_t>(live_row_count));
      }
    }
  }
}

void TableInfo::UpdateMetricsAttrs(const string& new_table_name) {
  if (metric_entity_) {
    metric_entity_->SetAttribute("table_name", new_table_name);
  }
}

const TableMetrics* TableInfo::GetMetrics() const {
  return metrics_.get();
}

void TableInfo::IncrementSchemaVersionCountUnlocked(int64_t version) {
  DCHECK(lock_.is_write_locked());
  schema_version_counts_[version]++;
}

void TableInfo::DecrementSchemaVersionCountUnlocked(int64_t version) {
  DCHECK(lock_.is_write_locked());

  // The schema version map invariant is that every tablet should be
  // represented. To enforce this, if the decrement reduces a particular key's
  // value to 0, we must erase the key too.
  auto it = schema_version_counts_.find(version);
  DCHECK(it != schema_version_counts_.end())
      << Substitute("$0 not in schema version map", version);
  DCHECK_GT(it->second, 0);
  it->second--;
  if (it->second == 0) {
    schema_version_counts_.erase(it);
  }
}

void PersistentTableInfo::set_state(SysTablesEntryPB::State state, const string& msg) {
  pb.set_state(state);
  pb.set_state_msg(msg);
}

} // namespace master
} // namespace kudu
