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
#ifndef KUDU_MASTER_SYS_CATALOG_H_
#define KUDU_MASTER_SYS_CATALOG_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <set>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gtest/gtest_prod.h>

#include "kudu/common/schema.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/status.h"

namespace kudu {

class Counter;
class FsManager;
class MetricRegistry;
class RowBlockRow;

namespace consensus {
class ConsensusMetadataManager;
}

namespace tablet {
class TabletMetadata;
}

namespace tserver {
class WriteRequestPB;
} // namespace tserver

namespace master {

class Master;
class SysCertAuthorityEntryPB;
class SysTServerStateEntryPB;
class SysTablesEntryPB;
class SysTabletsEntryPB;
class SysTskEntryPB;
struct MasterOptions;

// The SysCatalogTable has two separate visitors because the tables
// data must be loaded into memory before the tablets data.
class TableVisitor {
 public:
  virtual ~TableVisitor() = default;
  virtual Status VisitTable(const std::string& table_id,
                            const SysTablesEntryPB& metadata) = 0;
};

class TabletVisitor {
 public:
  virtual ~TabletVisitor() = default;
  virtual Status VisitTablet(const std::string& table_id,
                             const std::string& tablet_id,
                             const SysTabletsEntryPB& metadata) = 0;
};

// Visitor for TSK-related (Token Signing Key) entries. Actually, only the
// public part of those are stored in the system catalog table. That information
// is preserved to allow any master to verify token which might be signed
// by current or former master leader.
class TskEntryVisitor {
 public:
  virtual ~TskEntryVisitor() = default;
  virtual Status Visit(const std::string& entry_id,
                       const SysTskEntryPB& metadata) = 0;
};

class TServerStateVisitor {
 public:
  virtual ~TServerStateVisitor() = default;
  virtual Status Visit(const std::string& tserver_id,
                       const SysTServerStateEntryPB& metadata) = 0;
};

// SysCatalogTable is a Kudu table that keeps track of the following
// system information:
//   * table metadata
//   * tablet metadata
//   * root CA (certificate authority) certificate of the Kudu IPKI
//   * Kudu IPKI root CA cert's private key
//   * TSK (Token Signing Key) entries
//   * Latest handled Hive Metastore notification log event ID
//   * tserver state (e.g. maintenance mode)
//
// The essential properties of the SysCatalogTable are:
//   * SysCatalogTable has only one tablet.
//   * SysCatalogTable is managed by the master and not exposed to the user
//     as a "normal table", instead we have Master APIs to query the table.
//
// It has the schema:
//
//    (entry_type INT8, entry_id STRING) -> metadata STRING
//
//  * entry_type is one of CatalogEntryType. It indicates whether an entry is a
//    table, tablet, or some other piece of metadata. It is the first part of
//    the compound key to allow efficient scans of entries of only a single
//    type (e.g. only scan all of the tables, or only scan all of the tablets).
//  * entry_id is the ID of the entry type (e.g. table ID or tablet ID for
//    table and tablet entries respectively).
//  * metadata is a string containing a protobuf message specific to the
//    entry_type. These are defined in master.proto under "Sys Tables
//    Metadata".
class SysCatalogTable {
 public:
  // Magic ID of the system tablet.
  static const char* const kSysCatalogTabletId;

  // Root certificate authority (CA) entry identifier in the system table.
  // There should be no more than one entry of this type in the system table.
  static const char* const kSysCertAuthorityEntryId;

  // The row ID of the latest notification log entry in the sys catalog table.
  static const char* const kLatestNotificationLogEntryIdRowId;

  typedef Callback<Status()> ElectedLeaderCallback;

  enum CatalogEntryType {
    TABLES_ENTRY = 1,
    TABLETS_ENTRY = 2,
    CERT_AUTHORITY_INFO = 3,  // Kudu's root certificate authority entry.
    TSK_ENTRY = 4,            // Token Signing Key entry.
    HMS_NOTIFICATION_LOG = 5, // HMS notification log latest event ID.
    TSERVER_STATE = 6,        // TServer state.
  };

  // 'leader_cb_' is invoked whenever this node is elected as a leader
  // of the consensus configuration for this tablet, including for local standalone
  // master consensus configurations. It used to initialize leader state, submit any
  // leader-specific tasks and so forth.
  //
  /// NOTE: Since 'leader_cb_' is invoked synchronously and can block
  // the consensus configuration's progress, any long running tasks (e.g., scanning
  // tablets) should be performed asynchronously (by, e.g., submitting
  // them to a to a separate threadpool).
  SysCatalogTable(Master* master,  ElectedLeaderCallback leader_cb);

  ~SysCatalogTable();

  // Allow for orderly shutdown of TabletReplica, etc.
  void Shutdown();

  // Load the Metadata from disk, and initialize the TabletReplica for the sys-table
  Status Load(FsManager *fs_manager);

  // Create the new Metadata and initialize the TabletReplica for the sys-table.
  Status CreateNew(FsManager *fs_manager);

  // Perform a series of table/tablet actions in one WriteTransaction.
  struct Actions {
    Actions() = default;

    scoped_refptr<TableInfo> table_to_add;
    scoped_refptr<TableInfo> table_to_update;
    scoped_refptr<TableInfo> table_to_delete;
    std::vector<scoped_refptr<TabletInfo>> tablets_to_add;
    std::vector<scoped_refptr<TabletInfo>> tablets_to_update;
    std::vector<scoped_refptr<TabletInfo>> tablets_to_delete;
    boost::optional<int64_t> hms_notification_log_event_id;
  };

  // The way how actions are persisted into the system catalog table when
  // calling the Write() method below.
  enum class WriteMode {
    // Write all the actions in a single atomic update to the system
    // catalog tablet.
    ATOMIC,

    // Chunk the actions into pieces of maximum size corresponding to the
    // maximum RPC size limit. This is to allow the leader replica of the system
    // tablet to propagate the update to followers via Raft UpdateConsensus RPC.
    CHUNKED,
  };

  // Persist the specified actions into the system tablet. Set the 'mode' to
  // WriteMode::CHUNKED to split requests larger than the maximum RPC size
  // into chunks if non-atomic update is acceptable. In case of chunked mode,
  // no atomicity is guaranteed while persisting the specified actions.
  Status Write(Actions actions, WriteMode mode = WriteMode::ATOMIC);

  // Scan of the table-related entries.
  Status VisitTables(TableVisitor* visitor);

  // Scan of the tablet-related entries.
  Status VisitTablets(TabletVisitor* visitor);

  // Scan for TSK-related entries in the system table.
  Status VisitTskEntries(TskEntryVisitor* visitor);

  // Scan for tserver state entries in the system table.
  Status VisitTServerStates(TServerStateVisitor* visitor);

  // Get the latest processed HMS notification log event ID.
  Status GetLatestNotificationLogEventId(int64_t* event_id) WARN_UNUSED_RESULT;

  // Retrive the CA entry (private key and certificate) from the system table.
  Status GetCertAuthorityEntry(SysCertAuthorityEntryPB* entry);

  // Add root CA entry (private key and certificate) into the system table.
  // There should be no more than one CA entry in the system table.
  Status AddCertAuthorityEntry(const SysCertAuthorityEntryPB& entry);

  // Add TSK (Token Signing Key) entry into the system table.
  Status AddTskEntry(const SysTskEntryPB& entry);

  // Remove TSK (Token Signing Key) entries with the specified entry identifiers
  // (as in 'entry_id' column) from the system table. The container of the
  // entry identifiers must not be empty.
  Status RemoveTskEntries(const std::set<std::string>& entry_ids);

  // Add a tserver state entry to the system table.
  Status WriteTServerState(const std::string& tserver_id,
                           const SysTServerStateEntryPB& entry);

  // Remove a tserver state entry from the system table.
  Status RemoveTServerState(const std::string& tserver_id);

  // Return the underlying TabletReplica instance hosting the metadata.
  // This should be used with caution -- typically the various methods
  // above should be used rather than directly accessing the replica.
  const scoped_refptr<tablet::TabletReplica>& tablet_replica() const {
    return tablet_replica_;
  }

 private:
  FRIEND_TEST(MasterTest, TestMasterMetadataConsistentDespiteFailures);
  DISALLOW_COPY_AND_ASSIGN(SysCatalogTable);

  friend class CatalogManager;

  const char *table_name() const { return "sys.catalog"; }
  const char *table_id() const { return "sys.catalog.id"; }

  // Return the schema of the table.
  // NOTE: This is the "server-side" schema, so it must have the column IDs.
  Schema BuildTableSchema();

  // Returns 'Status::OK()' if the WriteTransaction completed
  Status SyncWrite(const tserver::WriteRequestPB& req);

  void SysCatalogStateChanged(const std::string& tablet_id, const std::string& reason);

  Status SetupTablet(const scoped_refptr<tablet::TabletMetadata>& metadata);

  // Use the master options to generate a new consensus configuration.
  // In addition, resolve all UUIDs of this consensus configuration.
  Status CreateDistributedConfig(const MasterOptions& options,
                                 consensus::RaftConfigPB* committed_config);

  std::string tablet_id() const {
    return tablet_replica_->tablet_id();
  }

  // Conventional "T xxx P xxxx..." prefix for logging.
  std::string LogPrefix() const;

  // Waits for the tablet to reach 'RUNNING' state.
  //
  // Contrary to tablet servers, in master we actually wait for the master tablet
  // to become online synchronously, this allows us to fail fast if something fails
  // and shouldn't induce the all-workers-blocked-waiting-for-tablets problem
  // that we've seen in tablet servers since the master only has to boot a few
  // tablets.
  Status WaitUntilRunning();

  template<typename T>
  Status GetEntryFromRow(const RowBlockRow& row,
                         std::string* entry_id, T* entry_data) const;

  template<typename T, CatalogEntryType entry_type>
  Status ProcessRows(std::function<Status(const std::string&, const T&)>) const;

  // Tablet related private methods.

  // Initializes the RaftPeerPB for the local peer.
  // Crashes due to an invariant check if the rpc server is not running.
  void InitLocalRaftPeerPB();

  // Add an operation to a write adding/updating/deleting a table or tablet.
  void ReqAddTable(tserver::WriteRequestPB* req,
                   const scoped_refptr<TableInfo>& table);
  void ReqUpdateTable(tserver::WriteRequestPB* req,
                      const scoped_refptr<TableInfo>& table);
  void ReqDeleteTable(tserver::WriteRequestPB* req,
                      const scoped_refptr<TableInfo>& table);

  // These three methods below generate WriteRequestPB to persist the
  // information on the tablet metadata updates in the system catalog. The size
  // of the generated write request is limited by the 'max_size' parameter.
  // The information on the tablets' metadata updates is provided with the
  // 'tablets' parameter. The 'excess_tablets' output parameter is populated
  // with elements which didn't fit into the result request due to the sizing
  // limitations. The result request is returned via the 'req' output parameter.
  // Note the best effort behavior of these methods: the resulting request
  // cannot be empty, and the very first row might be over the limit already.
  // However, that will be caught later by SyncWrite() before actually writing
  // the generated data into the system tablet.
  void ReqAddTablets(size_t max_size,
                     std::vector<scoped_refptr<TabletInfo>> tablets,
                     std::vector<scoped_refptr<TabletInfo>>* excess_tablets,
                     tserver::WriteRequestPB* req);
  void ReqUpdateTablets(size_t max_size,
                        std::vector<scoped_refptr<TabletInfo>> tablets,
                        std::vector<scoped_refptr<TabletInfo>>* excess_tablets,
                        tserver::WriteRequestPB* req);
  void ReqDeleteTablets(size_t max_size,
                        std::vector<scoped_refptr<TabletInfo>> tablets,
                        std::vector<scoped_refptr<TabletInfo>>* excess_tablets,
                        tserver::WriteRequestPB* req);

  // Generator function used by the ChunkedWrite() method below. The three
  // methods above (ReqAddTablets, ReqUpdateTablets, and ReqDeleteTablets)
  // are the generators with corresponding signature.
  typedef std::function<void(SysCatalogTable&,
                             size_t,
                             std::vector<scoped_refptr<TabletInfo>>,
                             std::vector<scoped_refptr<TabletInfo>>*,
                             tserver::WriteRequestPB*)> Generator;

  // A method for chunked write into the system tablet.
  Status ChunkedWrite(const Generator& generator,
                      size_t max_chunk_size,
                      std::vector<scoped_refptr<TabletInfo>> tablets_info,
                      tserver::WriteRequestPB* req);

  // Overwrite (upsert) the latest event ID in the table with the provided ID.
  void ReqSetNotificationLogEventId(tserver::WriteRequestPB* req, int64_t event_id);

  static std::string TskSeqNumberToEntryId(int64_t seq_number);

  // Special string injected into SyncWrite() random failures (if enabled).
  //
  // Only useful for tests.
  static const char* const kInjectedFailureStatusMsg;

  // Table schema, without IDs, used to send messages to the TabletReplica
  Schema schema_;
  Schema key_schema_;

  MetricRegistry* metric_registry_;

  scoped_refptr<tablet::TabletReplica> tablet_replica_;

  Master* master_;

  const scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager_;

  ElectedLeaderCallback leader_cb_;

  consensus::RaftPeerPB local_peer_pb_;

  scoped_refptr<Counter> oversized_write_requests_;
};

} // namespace master
} // namespace kudu

#endif
