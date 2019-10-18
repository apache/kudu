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

#include <atomic>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>
#include <sparsehash/dense_hash_map>

#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/master/master.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tablet_replica_lookup.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/oid_generator.h"
#include "kudu/util/random.h"
#include "kudu/util/rw_mutex.h"
#include "kudu/util/status.h"

template <class X> struct GoodFastHash;

namespace kudu {

class AuthzTokenTest_TestSingleMasterUnavailable_Test;
class CreateTableStressTest_TestConcurrentCreateTableAndReloadMetadata_Test;
class MetricEntity;
class MetricRegistry;
class MonitoredTask;
class NodeInstancePB;
class PartitionPB;
class PartitionSchema;
class Schema;
class TableExtraConfigPB;
class ThreadPool;
struct ColumnId;

// Working around FRIEND_TEST() ugliness.
namespace client {
class ServiceUnavailableRetryClientTest_CreateTable_Test;
} // namespace client

namespace consensus {
class RaftConsensus;
class StartTabletCopyRequestPB;
} // namespace consensus

namespace hms {
class HmsCatalog;
} // namespace hms

namespace rpc {
class RpcContext;
} // namespace rpc

namespace security {
class Cert;
class PrivateKey;
class TokenSigner;
class TokenSigningPublicKeyPB;
} // namespace security

namespace tserver {
class TsTabletManagerITest_TestTableStats_Test;
}

namespace tablet {
class TabletReplica;
} // namespace tablet

namespace master {

class AuthzProvider;
class CatalogManagerBgTasks;
class HmsNotificationLogListenerTask;
class Master;
class PlacementPolicy;
class SysCatalogTable;
class TSDescriptor;
class TableInfo;
struct DeferredAssignmentActions;
struct TableMetrics;

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

// The information about a single tablet which exists in the cluster.
//
// This object uses copy-on-write for the portions of data which are persisted
// on disk. This allows the mutated data to be staged and written to disk
// while readers continue to access the previous version. These portions
// of data are in PersistentTabletInfo above, and typically accessed using
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

  enum {
    // Schema version when the tablet has yet to report.
    //
    // This value must be less than all possible schema versions. -1 is
    // appropriate; schema versions range from 0 to UINT32_MAX.
    NOT_YET_REPORTED = -1L
  };

  TabletInfo(scoped_refptr<TableInfo> table, std::string tablet_id);

  const std::string& id() const { return tablet_id_; }
  const scoped_refptr<TableInfo>& table() const { return table_; }

  // Access the persistent metadata. Typically you should use
  // TabletMetadataLock to gain access to this data.
  const CowObject<PersistentTabletInfo>& metadata() const { return metadata_; }
  CowObject<PersistentTabletInfo>* mutable_metadata() { return &metadata_; }

  // Accessors for the last time create tablet RPCs were sent for this tablet.
  void set_last_create_tablet_time(const MonoTime& ts);
  MonoTime last_create_tablet_time() const;

  // Sets the reported schema version to 'version' provided it's not already
  // equal to or greater than it.
  //
  // Also reflects the version change to the table's schema version map.
  void set_reported_schema_version(int64_t version);

  // Simple accessor for reported_schema_version_.
  int64_t reported_schema_version() const;

  // No synchronization needed.
  std::string ToString() const;

  // Update the stats.
  void UpdateStats(tablet::ReportedTabletStatsPB stats);

  // Return the stats.
  tablet::ReportedTabletStatsPB GetStats() const;

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
  //
  // Set to NOT_YET_REPORTED when the tablet hasn't yet reported.
  int64_t reported_schema_version_;

  // Cached stats for the LEADER replica.
  tablet::ReportedTabletStatsPB stats_;

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
  typedef std::map<std::string, scoped_refptr<TabletInfo>> TabletInfoMap;

  explicit TableInfo(std::string table_id);

  std::string ToString() const;

  // Return the table's ID. Does not require synchronization.
  const std::string& id() const { return table_id_; }

  // Atomically add and remove multiple tablets from this table.
  //
  // Tablet locks in READ mode or greater must be held for all tablets to be
  // added or dropped.
  void AddRemoveTablets(const std::vector<scoped_refptr<TabletInfo>>& tablets_to_add,
                        const std::vector<scoped_refptr<TabletInfo>>& tablets_to_drop);

  // This only returns tablets which are in RUNNING state.
  void GetTabletsInRange(const GetTableLocationsRequestPB* req,
                         std::vector<scoped_refptr<TabletInfo>>* ret) const;

  // Adds all tablets to the vector in partition key sorted order.
  void GetAllTablets(std::vector<scoped_refptr<TabletInfo>>* ret) const;

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
    TabletInfoMap ret;
    for (const auto& e : tablet_map_) {
      ret.emplace(e.first, make_scoped_refptr(e.second));
    }
    return ret;
  }

  // Returns the number of tablets.
  int num_tablets() const {
    shared_lock<rw_spinlock> l(lock_);
    return tablet_map_.size();
  }

  // Register metrics for the table.
  void RegisterMetrics(MetricRegistry* metric_registry, const std::string& table_name);

  // Unregister metrics for the table.
  void UnregisterMetrics();

  // Update stats belonging to 'tablet_id' in the table's metrics.
  void UpdateMetrics(const std::string& tablet_id,
                     const tablet::ReportedTabletStatsPB& old_stats,
                     const tablet::ReportedTabletStatsPB& new_stats);

  // Remove stats belonging to 'tablet_id' from table metrics.
  void RemoveMetrics(const std::string& tablet_id,
                     const tablet::ReportedTabletStatsPB& old_stats);

  // Update the attributes of the metrics.
  void UpdateMetricsAttrs(const std::string& new_table_name);

  // Return the metrics.
  const TableMetrics* GetMetrics() const;

 private:
  friend class RefCountedThreadSafe<TableInfo>;
  friend class TabletInfo;
  FRIEND_TEST(kudu::tserver::TsTabletManagerITest, TestTableStats);

  ~TableInfo();

  // Increments or decrements the value for the key 'version' in
  // 'schema_version_counts'.
  //
  // Must be called with 'lock_' held for writing.
  void IncrementSchemaVersionCountUnlocked(int64_t version);
  void DecrementSchemaVersionCountUnlocked(int64_t version);

  const std::string table_id_;

  // Sorted index of tablet start partition-keys to TabletInfo.
  //
  // Every TabletInfo has a strong backpointer to its TableInfo, so these
  // pointers must be raw.
  typedef std::map<std::string, TabletInfo*> RawTabletInfoMap;
  RawTabletInfoMap tablet_map_;

  // Protects tablet_map_, pending_tasks_, and schema_version_counts_.
  mutable rw_spinlock lock_;

  CowObject<PersistentTableInfo> metadata_;

  // List of pending tasks (e.g. create/alter tablet requests)
  std::unordered_set<MonitoredTask*> pending_tasks_;

  // Map of schema version to the number of tablets that reported that version.
  //
  // All tablets are represented here regardless of whether they've reported.
  // Tablets yet to report will count towards the special NOT_YET_REPORTED key.
  //
  // The contents of this map are equivalent to iterating over every table in
  // tablet_map_ and summing up the tablets' reported schema versions.
  std::map<int64_t, int64_t> schema_version_counts_;

  scoped_refptr<MetricEntity> metric_entity_;
  std::unique_ptr<TableMetrics> metrics_;

  DISALLOW_COPY_AND_ASSIGN(TableInfo);
};

// Helper to manage locking on the persistent metadata of TabletInfo or TableInfo.
template<class MetadataClass>
class MetadataLock : public CowLock<typename MetadataClass::cow_state> {
 public:
  typedef CowLock<typename MetadataClass::cow_state> super;
  MetadataLock()
      : super() {
  }
  MetadataLock(MetadataClass* info, LockMode mode)
      : super(DCHECK_NOTNULL(info)->mutable_metadata(), mode) {
  }
  MetadataLock(const MetadataClass* info, LockMode mode)
      : super(&(DCHECK_NOTNULL(info))->metadata(), mode) {
  }
};

// Helper to manage locking on the persistent metadata of multiple TabletInfo
// or TableInfo objects.
template<class MetadataClass>
class MetadataGroupLock : public CowGroupLock<std::string,
                                              typename MetadataClass::cow_state> {
 public:
  typedef CowGroupLock<std::string, typename MetadataClass::cow_state> super;
  explicit MetadataGroupLock(LockMode mode)
      : super(mode) {
  }

  void AddInfo(const MetadataClass& info) {
    this->AddObject(info.id(), &info.metadata());
  }

  void AddMutableInfo(MetadataClass* info) {
    this->AddMutableObject(info->id(), info->mutable_metadata());
  }

  void AddInfos(const std::vector<scoped_refptr<MetadataClass>>& infos) {
    for (const auto& i : infos) {
      AddInfo(*i);
    }
  }

  void AddMutableInfos(const std::vector<scoped_refptr<MetadataClass>>& infos) {
    for (const auto& i : infos) {
      AddMutableInfo(i.get());
    }
  }
};

// Convenience aliases for the above lock guards.
typedef MetadataLock<TableInfo> TableMetadataLock;
typedef MetadataLock<TabletInfo> TabletMetadataLock;
typedef MetadataGroupLock<TableInfo> TableMetadataGroupLock;
typedef MetadataGroupLock<TabletInfo> TabletMetadataGroupLock;

// The component of the master which tracks the state and location
// of tables/tablets in the cluster.
//
// This is the master-side counterpart of TSTabletManager, which tracks
// the state of each tablet on a given tablet-server.
//
// Thread-safe.
class CatalogManager : public tserver::TabletReplicaLookupIf {
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
    // Creates a new shared lock, trying to acquire the catalog manager's
    // leader_lock_ for reading in the process. If acquired, the lock is
    // released when this object is destroyed.
    //
    // In most common use cases, where write lock semantics is assumed, call
    // CheckIsInitializedAndIsLeaderOrRespond() to verify that the leader_lock_
    // has been acquired (as shown in the class-wide comment above). In rare
    // cases, where both read and write semantics are applicable, use the
    // combination of CheckIsInitializedOrRespond() and owns_lock() methods
    // to verify that the leader_lock_ is acquired.
    //
    // The object pointed by the 'catalog' parameter must outlive this object.
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

    // Whether the underlying leader lock of the system catalog is acquired.
    bool owns_lock() const {
      return leader_shared_lock_.owns_lock();
    }

    // Check whether the consensus configuration term has changed from the term
    // captured at object construction (initial_term_).
    // Requires: leader_status() returns OK().
    bool has_term_changed() const;

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
    int64_t initial_term_;

    DISALLOW_COPY_AND_ASSIGN(ScopedLeaderSharedLock);
  };

  // Temporarily forces the catalog manager to be a follower. Only for tests!
  class ScopedLeaderDisablerForTests {
   public:

    explicit ScopedLeaderDisablerForTests(CatalogManager* catalog)
        : catalog_(catalog),
        old_leader_ready_term_(catalog->leader_ready_term_) {
      std::lock_guard<simple_spinlock> l(catalog_->state_lock_);
      catalog_->leader_ready_term_ = -1;
    }

    ~ScopedLeaderDisablerForTests() {
      std::lock_guard<simple_spinlock> l(catalog_->state_lock_);
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

  // Create a new Table with the specified attributes.
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status CreateTable(const CreateTableRequestPB* req,
                     CreateTableResponsePB* resp,
                     rpc::RpcContext* rpc);

  // Get the information about an in-progress create operation. If 'user' is
  // provided, checks that the user is authorized to get such information.
  Status IsCreateTableDone(const IsCreateTableDoneRequestPB* req,
                           IsCreateTableDoneResponsePB* resp,
                           boost::optional<const std::string&> user);

  // Delete the specified table in response to a DeleteTableRequest RPC.
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status DeleteTableRpc(const DeleteTableRequestPB& req,
                        DeleteTableResponsePB* resp,
                        rpc::RpcContext* rpc) WARN_UNUSED_RESULT;

  // Delete the specified table in response to a 'DROP TABLE' HMS notification
  // log listener event.
  Status DeleteTableHms(const std::string& table_name,
                        const std::string& table_id,
                        int64_t notification_log_event_id) WARN_UNUSED_RESULT;

  // Alter the specified table in response to an AlterTableRequest RPC.
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status AlterTableRpc(const AlterTableRequestPB& req,
                       AlterTableResponsePB* resp,
                       rpc::RpcContext* rpc);

  // Rename the specified table in response to an 'ALTER TABLE RENAME' HMS
  // notification log listener event.
  Status RenameTableHms(const std::string& table_id,
                        const std::string& table_name,
                        const std::string& new_table_name,
                        int64_t notification_log_event_id) WARN_UNUSED_RESULT;

  // Get the information about an in-progress alter operation. If 'user' is
  // provided, checks that the user is authorized to get such information.
  Status IsAlterTableDone(const IsAlterTableDoneRequestPB* req,
                          IsAlterTableDoneResponsePB* resp,
                          boost::optional<const std::string&> user);

  // Get the information about the specified table. If 'user' is provided,
  // checks that the user is authorized to get such information. If a token
  // signer is provided (e.g. authz token generation is enabled), an authz
  // token will be attached to the response.
  Status GetTableSchema(const GetTableSchemaRequestPB* req,
                        GetTableSchemaResponsePB* resp,
                        boost::optional<const std::string&> user,
                        const security::TokenSigner* token_signer);

  // Lists all the running tables. If 'user' is provided, only lists those that
  // the given user is authorized to see.
  Status ListTables(const ListTablesRequestPB* req,
                    ListTablesResponsePB* resp,
                    boost::optional<const std::string&> user);

  // Get table statistics. If 'user' is provided, checks if the user is
  // authorized to get such statistics.
  Status GetTableStatistics(const GetTableStatisticsRequestPB* req,
                            GetTableStatisticsResponsePB* resp,
                            boost::optional<const std::string&> user);

  // Lookup the tablets contained in the partition range of the request. If 'user'
  // is provided, checks that the user is authorized to get such information.
  //
  // Returns an error if any of the tablets are not running.
  Status GetTableLocations(const GetTableLocationsRequestPB* req,
                           GetTableLocationsResponsePB* resp,
                           boost::optional<const std::string&> user);

  struct TSInfosDict {
    std::vector<std::unique_ptr<TSInfoPB>> ts_info_pbs;
    google::dense_hash_map<StringPiece, int, GoodFastHash<StringPiece>> uuid_to_idx;
    TSInfosDict() {
      uuid_to_idx.set_empty_key(StringPiece());
    }
  };

  // Look up the locations of the given tablet. If 'user' is provided, checks
  // that the user is authorized to get such information. Adds only information
  // on replicas which satisfy the 'filter'. The locations vector is overwritten
  // (not appended to).
  //
  // If 'ts_infos_dict' is not null, the returned locations use it as a dictionary
  // to reference TSInfoPBs instead of making many copies.
  //
  // If the tablet is not found, returns Status::NotFound.
  // If the tablet is not running, returns Status::ServiceUnavailable.
  // Otherwise, returns Status::OK and puts the result in 'locs_pb'.
  // This only returns tablets which are in RUNNING state.
  Status GetTabletLocations(const std::string& tablet_id,
                            master::ReplicaTypeFilter filter,
                            TabletLocationsPB* locs_pb,
                            TSInfosDict* ts_infos_dict,
                            boost::optional<const std::string&> user);

  // Replace the given tablet with a new, empty one. The replaced tablet is
  // deleted and its data is permanently lost.
  Status ReplaceTablet(const std::string& tablet_id, master::ReplaceTabletResponsePB* resp);

  // Handle a tablet report from the given tablet server.
  //
  // The RPC context is provided for logging/tracing purposes,
  // but this function does not itself respond to the RPC.
  Status ProcessTabletReport(TSDescriptor* ts_desc,
                             const TabletReportPB& full_report,
                             TabletReportUpdatesPB* full_report_update,
                             rpc::RpcContext* rpc);

  // Returns the latest handled Hive Metastore notification log event ID.
  int64_t GetLatestNotificationLogEventId();

  // Initializes the cached latest handled Hive Metastore notification log event ID
  // after a leader election.
  Status InitLatestNotificationLogEventId();

  // Stores the latest handled Hive Metastore notification log event ID.
  //
  // Must only be called by the singleton notification log listener thread.
  Status StoreLatestNotificationLogEventId(int64_t event_id);

  SysCatalogTable* sys_catalog() { return sys_catalog_.get(); }

  // Returns the Master tablet's RaftConsensus instance if it is initialized, or
  // else a nullptr.
  std::shared_ptr<consensus::RaftConsensus> master_consensus() const;

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

  // Used by ConsensusService to retrieve the TabletReplica for a system
  // table specified by 'tablet_id'.
  //
  // See also: TabletReplicaLookupIf, ConsensusServiceImpl.
  virtual Status GetTabletReplica(const std::string& tablet_id,
                                  scoped_refptr<tablet::TabletReplica>* replica) const override;

  virtual void GetTabletReplicas(
      std::vector<scoped_refptr<tablet::TabletReplica>>* replicas) const override;

  virtual const NodeInstancePB& NodeInstance() const override;

  bool IsInitialized() const;

  virtual void StartTabletCopy(
      const consensus::StartTabletCopyRequestPB* req,
      std::function<void(const Status&, tserver::TabletServerErrorPB::Code)> cb) override;

  // Returns this CatalogManager's role in a consensus configuration. CatalogManager
  // must be initialized before calling this method.
  consensus::RaftPeerPB::Role Role() const;

  hms::HmsCatalog* HmsCatalog() const {
    return hms_catalog_.get();
  }

  master::AuthzProvider* authz_provider() const {
    return authz_provider_.get();
  }

  // Returns the normalized form of the provided table name.
  //
  // If the HMS integration is configured and the table name is a valid HMS
  // table name, the normalized form is returned (see HmsCatalog::NormalizeTableName),
  // otherwise a copy of the original table name is returned.
  //
  // If the HMS integration is not configured then a copy of the original table
  // name is returned.
  static std::string NormalizeTableName(const std::string& table_name);

 private:
  // These tests call ElectedAsLeaderCb() directly.
  FRIEND_TEST(MasterTest, TestShutdownDuringTableVisit);
  FRIEND_TEST(MasterTest, TestGetTableLocationsDuringRepeatedTableVisit);
  FRIEND_TEST(kudu::AuthzTokenTest, TestSingleMasterUnavailable);

  // This test calls VisitTablesAndTablets() directly.
  FRIEND_TEST(kudu::CreateTableStressTest, TestConcurrentCreateTableAndReloadMetadata);

  // This test exclusively acquires the leader_lock_ directly.
  FRIEND_TEST(kudu::client::ServiceUnavailableRetryClientTest, CreateTable);

  friend class TableLoader;
  friend class TabletLoader;

  typedef std::unordered_map<std::string, scoped_refptr<TableInfo>> TableInfoMap;
  typedef std::unordered_map<std::string, scoped_refptr<TabletInfo>> TabletInfoMap;

  // Delete the specified table in the catalog. If 'user' is provided,
  // checks that the user is authorized to delete the table. Otherwise,
  // it indicates its an internal operation (originates from catalog
  // manager).
  //
  // If a notification log event ID is provided, it will be written to the sys
  // catalog.
  Status DeleteTable(const DeleteTableRequestPB& req,
                     DeleteTableResponsePB* resp,
                     boost::optional<int64_t> hms_notification_log_event_id,
                     boost::optional<const std::string&> user) WARN_UNUSED_RESULT;

  // Alter the specified table in the catalog. If 'user' is provided,
  // checks that the user is authorized to alter the table. Otherwise,
  // it indicates its an internal operation (originates from catalog
  // manager).
  //
  // If a notification log event ID is provided, it will be written to the sys
  // catalog along with the altered table metadata.
  Status AlterTable(const AlterTableRequestPB& req,
                    AlterTableResponsePB* resp,
                    boost::optional<int64_t> hms_notification_log_event_id,
                    boost::optional<const std::string&> user) WARN_UNUSED_RESULT;

  // Called by SysCatalog::SysCatalogStateChanged when this node
  // becomes the leader of a consensus configuration. Executes
  // PrepareForLeadershipTask() via 'worker_pool_'.
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

  // Performs several checks before calling VisitTablesAndTablets() to actually
  // reload table/tablet metadata into memory and do other work to update the
  // internal state of this object upon becoming the leader.
  void PrepareForLeadershipTask();

  // Perform necessary work to prepare for running in the follower role.
  // Currently, it's about having a means to authenticate clients by authn tokens.
  Status PrepareFollower(MonoTime* last_tspk_run);

  // Prepare CA-related information for the follower catalog manager. Currently,
  // this includes reading the CA information from the system table, creating
  // TLS server certificate request, signing it with the CA key, and installing
  // the certificate TLS server certificates.
  Status PrepareFollowerCaInfo();

  // Read currently active TSK keys from the system table and import their
  // public parts into the token verifier, so it's possible to verify signatures
  // of authn tokens.
  Status PrepareFollowerTokenVerifier();

  // Clears out the existing metadata (by-name map, table-id map, and tablet
  // map), and loads table and tablet metadata into memory.
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

  // Initialize the IPKI certificate authority: load the CA information record
  // from the system table. If the CA information record is not present in the
  // table, generate and store a new one.
  Status InitCertAuthority();

  // Initialize the IPKI certificate authority with the specified private key
  // and certificate.
  Status InitCertAuthorityWith(std::unique_ptr<security::PrivateKey> key,
                               std::unique_ptr<security::Cert> cert);

  // Load the IPKI certficate authority information from the system
  // table: the private key and the certificate. If the CA info entry is not
  // found in the table, return Status::NotFound.
  Status LoadCertAuthorityInfo(std::unique_ptr<security::PrivateKey>* key,
                               std::unique_ptr<security::Cert>* cert);

  // Store the IPKI certificate authority information into the system table.
  Status StoreCertAuthorityInfo(const security::PrivateKey& key,
                                const security::Cert& cert);

  // 1. Initialize the TokenSigner (the component which signs authn tokens):
  //      a. Load TSK records from the system table.
  //      b. Import the newly loaded TSK records into the TokenSigner.
  // 2. Check whether it's time to generate a new token signing key.
  //    If yes, then:
  //      a. Generate a new TSK.
  //      b. Store the new TSK one into the system catalog table.
  // 3. Purge expired TSKs from the system table.
  Status InitTokenSigner();

  // Helper for creating the initial TableInfo state
  // Leaves the table "write locked" with the new info in the
  // "dirty" state field.
  scoped_refptr<TableInfo> CreateTableInfo(const CreateTableRequestPB& req,
                                           const Schema& schema,
                                           const PartitionSchema& partition_schema,
                                           TableExtraConfigPB extra_config_pb);

  // Helper for creating the initial TabletInfo state.
  // Leaves the tablet "write locked" with the new info in the
  // "dirty" state field.
  scoped_refptr<TabletInfo> CreateTabletInfo(const scoped_refptr<TableInfo>& table,
                                             const PartitionPB& partition,
                                             const boost::optional<std::string>& dimension_label);


  // Builds the TabletLocationsPB for a tablet based on the provided TabletInfo
  // and the replica type fiter specified. Populates locs_pb and returns
  // Status::OK on success.
  //
  // If 'ts_infos_dict' is not null, the returned locations use it as a dictionary
  // to reference TSInfoPBs.
  //
  // Returns Status::ServiceUnavailable if tablet is not running.
  Status BuildLocationsForTablet(const scoped_refptr<TabletInfo>& tablet,
                                 master::ReplicaTypeFilter filter,
                                 TabletLocationsPB* locs_pb,
                                 TSInfosDict* ts_infos_dict);

  // Looks up the table, locks it with the provided lock mode, and, if 'user' is
  // provided, checks that the user is authorized to operate on the table.
  //
  // We expect that our external authorization metadata store uses table name to
  // identify the protected resource. Therefore, the authorization step happens
  // after table locking to ensure the table name is consistent between authorization
  // of a particular action on the table and the client-visible execution of that
  // action. Consider the following scenario,
  //  1. Alice has RENAME privileges on table B. Bob has DROP privileges on table B.
  //  2. Alice tries to rename B to A, at the same time that Bob tries to drop B.
  //  3. If we were to authorize before locking, there may be a privilege
  //     escalation where Bob drops table A that he has no permission to drop.
  //
  // If the table does not exist, NOT_AUTHORIZED error status is returned and the
  // appropriate error code is set in the response, to avoid leaking whether the
  // table exists. One exception is when the user is authorized to operate on the
  // table, then TABLE_NOT_FOUND error status is returned.
  template<typename ReqClass, typename RespClass, typename F>
  Status FindLockAndAuthorizeTable(const ReqClass& request,
                                   RespClass* response,
                                   LockMode lock_mode,
                                   F authz_func,
                                   boost::optional<const std::string&> user,
                                   scoped_refptr<TableInfo>* table_info,
                                   TableMetadataLock* table_lock) WARN_UNUSED_RESULT;

  // Extract the set of tablets that must be processed because not running yet.
  void ExtractTabletsToProcess(std::vector<scoped_refptr<TabletInfo>>* tablets_to_process);

  // Check if it's time to generate a new Token Signing Key for TokenSigner.
  // If so, generate one and persist it into the system table. After that,
  // push it into the TokenSigner's key queue.
  Status TryGenerateNewTskUnlocked();

  // Load non-expired TSK entries from the system table.
  // Once done, initialize TokenSigner with the loaded entries.
  Status LoadTskEntries(std::set<std::string>* expired_entry_ids);

  // Load non-expired TSK entries from the system table, extract the public
  // part from those, and return them with the 'key' output parameter.
  Status LoadTspkEntries(std::vector<security::TokenSigningPublicKeyPB>* keys);

  // Delete TSK entries with the specified entry identifiers
  // (identifiers correspond to the 'entry_id' column). The 'entry_ids'
  // container must not be empty.
  Status DeleteTskEntries(const std::set<std::string>& entry_ids);

  Status ApplyAlterSchemaSteps(const SysTablesEntryPB& current_pb,
                               std::vector<AlterTableRequestPB::Step> steps,
                               Schema* new_schema,
                               ColumnId* next_col_id);

  Status ApplyAlterPartitioningSteps(const TableMetadataLock& l,
                                     const scoped_refptr<TableInfo>& table,
                                     const Schema& client_schema,
                                     std::vector<AlterTableRequestPB::Step> steps,
                                     std::vector<scoped_refptr<TabletInfo>>* tablets_to_add,
                                     std::vector<scoped_refptr<TabletInfo>>* tablets_to_drop);

  // Task that takes care of the tablet assignments/creations.
  // Loops through the "not created" tablets and sends a CreateTablet() request.
  Status ProcessPendingAssignments(const std::vector<scoped_refptr<TabletInfo> >& tablets);

  // Selects the tservers where the newly-created tablet's replicas will be
  // placed, populating its consensus configuration in the process.
  // Returns InvalidArgument if there are not enough live tservers to host
  // the required number of replicas. 'tablet' must not be null.
  Status SelectReplicasForTablet(const PlacementPolicy& policy,
                                 TabletInfo* tablet);

  // Handles 'tablet' currently in the PREPARING state.
  //
  // Transitions it to the CREATING state and prepares CreateTablet RPCs.
  void HandleAssignPreparingTablet(const scoped_refptr<TabletInfo>& tablet,
                                   DeferredAssignmentActions* deferred);

  // Handles 'tablet' currently in the CREATING state.
  //
  // If a CreateTablet RPC timed out, marks 'tablet' as REPLACED and replaces
  // it with a new CREATING tablet, which is written to in 'new_tablet'.
  void HandleAssignCreatingTablet(const scoped_refptr<TabletInfo>& tablet,
                                  DeferredAssignmentActions* deferred,
                                  scoped_refptr<TabletInfo>* new_tablet);

  void HandleTabletSchemaVersionReport(const scoped_refptr<TabletInfo>& tablet,
                                       uint32_t version);

  // Send the "create tablet request" to all peers of a particular tablet.
  //
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

  std::string GenerateId() { return oid_generator_.Next(); }

  // Conventional "T xxx P yyy: " prefix for logging.
  std::string LogPrefix() const;

  // Aborts all tasks belonging to 'tables' and waits for them to finish.
  void AbortAndWaitForAllTasks(const std::vector<scoped_refptr<TableInfo>>& tables);

  // Wait for the Hive Metastore notification log listener to process the latest
  // events, if the HMS integration is enabled. Handles setting the correct
  // response code in the case of an error.
  template<typename RespClass>
  Status WaitForNotificationLogListenerCatchUp(RespClass* resp,
                                               rpc::RpcContext* rpc) WARN_UNUSED_RESULT;

  // TODO(unknown): the maps are a little wasteful of RAM, since the TableInfo/TabletInfo
  // objects have a copy of the string key. But STL doesn't make it
  // easy to make a "gettable set".

  // Lock protecting the various maps and sets below.
  typedef rw_spinlock LockType;
  mutable LockType lock_;

  // Table maps: table-id -> TableInfo and normalized-table-name -> TableInfo
  TableInfoMap table_ids_map_;
  TableInfoMap normalized_table_names_map_;

  // Tablet maps: tablet-id -> TabletInfo
  TabletInfoMap tablet_map_;

  // Names of tables that are currently reserved by CreateTable() or AlterTable().
  //
  // As a rule, operations that add new table names should do so as follows:
  // 1. Acquire lock_.
  // 2. Ensure normalized_table_names_map_ does not contain the new normalized name.
  // 3. Ensure reserved_normalized_table_names_ does not contain the new normalized name.
  // 4. Add the new normalized name to reserved_normalized_table_names_.
  // 5. Release lock_.
  // 6. Perform the operation.
  // 7. If it succeeded, add the normalized name to normalized_table_names_map_ with lock_ held.
  // 8. Remove the new normalized name from reserved_normalized_table_names_ with lock_ held.
  std::unordered_set<std::string> reserved_normalized_table_names_;

  Master *master_;
  ObjectIdGenerator oid_generator_;

  // Random number generator used for selecting replica locations.
  ThreadSafeRandom rng_;

  std::unique_ptr<SysCatalogTable> sys_catalog_;

  // Background thread, used to execute the catalog manager tasks
  // like the assignment and cleaner
  friend class CatalogManagerBgTasks;
  std::unique_ptr<CatalogManagerBgTasks> background_tasks_;

  std::unique_ptr<hms::HmsCatalog> hms_catalog_;
  std::unique_ptr<HmsNotificationLogListenerTask> hms_notification_log_listener_;

  std::unique_ptr<master::AuthzProvider> authz_provider_;

  enum State {
    kConstructed,
    kStarting,
    kRunning,
    kClosing
  };

  static const char* StateToString(State state);

  // Lock protecting state_, leader_ready_term_
  mutable simple_spinlock state_lock_;
  State state_;

  // Singleton pool that serializes invocations of ElectedAsLeaderCb().
  std::unique_ptr<ThreadPool> leader_election_pool_;

  // This field is updated when a node becomes leader master,
  // waits for all outstanding uncommitted metadata (table and tablet metadata)
  // in the sys catalog to commit, and then reads that metadata into in-memory
  // data structures. This is used to "fence" client and tablet server requests
  // that depend on the in-memory state until this master can respond
  // correctly.
  int64_t leader_ready_term_;

  // This field is updated when a node becomes leader master, and the HMS
  // integration is enabled. It caches the latest processed Hive Metastore
  // notification log event ID so that every request does not need to hit the
  // sys catalog. Must only be written to by the HMS notification log listener
  // thread.
  std::atomic<int64_t> hms_notification_log_event_id_;

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
