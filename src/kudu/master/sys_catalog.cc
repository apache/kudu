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

#include "kudu/master/sys_catalog.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>

#include "kudu/clock/clock.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/key_encoder.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/partition.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master_options.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/security/token.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/cow_object.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"

DEFINE_double(sys_catalog_fail_during_write, 0.0,
              "Fraction of the time when system table writes will fail");
TAG_FLAG(sys_catalog_fail_during_write, hidden);

using kudu::consensus::ConsensusMetadata;
using kudu::consensus::ConsensusMetadataManager;
using kudu::consensus::ConsensusStatePB;
using kudu::consensus::RaftConfigPB;
using kudu::consensus::RaftPeerPB;
using kudu::log::Log;
using kudu::pb_util::SecureShortDebugString;
using kudu::tablet::LatchTransactionCompletionCallback;
using kudu::tablet::Tablet;
using kudu::tablet::TabletReplica;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using std::function;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;


namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {
namespace master {

static const char* const kSysCatalogTableColType = "entry_type";
static const char* const kSysCatalogTableColId = "entry_id";
static const char* const kSysCatalogTableColMetadata = "metadata";

const char* const SysCatalogTable::kSysCatalogTabletId =
    "00000000000000000000000000000000";
const char* const SysCatalogTable::kSysCertAuthorityEntryId =
    "root-ca-info";
const char* const SysCatalogTable::kInjectedFailureStatusMsg =
    "INJECTED FAILURE";
const char* const SysCatalogTable::kLatestNotificationLogEntryIdRowId =
  "latest_notification_log_entry_id";

namespace {

// Return true if the two PBs are equal.
//
// If 'diff_str' is not null, stores a textual description of the
// difference.
bool ArePBsEqual(const google::protobuf::Message& prev_pb,
                 const google::protobuf::Message& new_pb,
                 string* diff_str) {
  google::protobuf::util::MessageDifferencer md;
  if (diff_str) {
    md.ReportDifferencesToString(diff_str);
  }
  return md.Compare(prev_pb, new_pb);
}

} // anonymous namespace


SysCatalogTable::SysCatalogTable(Master* master,
                                 ElectedLeaderCallback leader_cb)
    : metric_registry_(master->metric_registry()),
      master_(master),
      cmeta_manager_(new ConsensusMetadataManager(master_->fs_manager())),
      leader_cb_(std::move(leader_cb)) {
}

SysCatalogTable::~SysCatalogTable() {
}

void SysCatalogTable::Shutdown() {
  if (tablet_replica_) {
    tablet_replica_->Shutdown();
  }
}

Status SysCatalogTable::Load(FsManager *fs_manager) {
  // Load Metadata Information from disk
  scoped_refptr<tablet::TabletMetadata> metadata;
  RETURN_NOT_OK(tablet::TabletMetadata::Load(fs_manager, kSysCatalogTabletId, &metadata));

  // Verify that the schema is the current one
  if (!metadata->schema().Equals(BuildTableSchema())) {
    // TODO: In this case we probably should execute the migration step.
    return(Status::Corruption("Unexpected schema", metadata->schema().ToString()));
  }

  if (master_->opts().IsDistributed()) {
    LOG(INFO) << "Verifying existing consensus state";
    string tablet_id = metadata->tablet_id();
    scoped_refptr<ConsensusMetadata> cmeta;
    RETURN_NOT_OK_PREPEND(cmeta_manager_->Load(tablet_id, &cmeta),
                          "Unable to load consensus metadata for tablet " + tablet_id);
    ConsensusStatePB cstate = cmeta->ToConsensusStatePB();
    RETURN_NOT_OK(consensus::VerifyRaftConfig(cstate.committed_config()));
    CHECK(!cstate.has_pending_config());

    // Make sure the set of masters passed in at start time matches the set in
    // the on-disk cmeta.
    set<string> peer_addrs_from_opts;
    for (const auto& hp : master_->opts().master_addresses) {
      peer_addrs_from_opts.insert(hp.ToString());
    }
    if (peer_addrs_from_opts.size() < master_->opts().master_addresses.size()) {
      LOG(WARNING) << Substitute("Found duplicates in --master_addresses: "
                                 "the unique set of addresses is $0",
                                 JoinStrings(peer_addrs_from_opts, ", "));
    }
    set<string> peer_addrs_from_disk;
    for (const auto& p : cstate.committed_config().peers()) {
      HostPort hp;
      RETURN_NOT_OK(HostPortFromPB(p.last_known_addr(), &hp));
      peer_addrs_from_disk.insert(hp.ToString());
    }
    vector<string> symm_diff;
    std::set_symmetric_difference(peer_addrs_from_opts.begin(),
                                  peer_addrs_from_opts.end(),
                                  peer_addrs_from_disk.begin(),
                                  peer_addrs_from_disk.end(),
                                  std::back_inserter(symm_diff));
    if (!symm_diff.empty()) {
      string msg = Substitute(
          "on-disk master list ($0) and provided master list ($1) differ. "
          "Their symmetric difference is: $2",
          JoinStrings(peer_addrs_from_disk, ", "),
          JoinStrings(peer_addrs_from_opts, ", "),
          JoinStrings(symm_diff, ", "));
      return Status::InvalidArgument(msg);
    }
  }

  RETURN_NOT_OK(SetupTablet(metadata));
  return Status::OK();
}

Status SysCatalogTable::CreateNew(FsManager *fs_manager) {
  // Create the new Metadata
  scoped_refptr<tablet::TabletMetadata> metadata;
  Schema schema = BuildTableSchema();
  PartitionSchema partition_schema;
  RETURN_NOT_OK(PartitionSchema::FromPB(PartitionSchemaPB(), schema, &partition_schema));

  vector<KuduPartialRow> split_rows;
  vector<Partition> partitions;
  RETURN_NOT_OK(partition_schema.CreatePartitions(split_rows, {}, schema, &partitions));
  DCHECK_EQ(1, partitions.size());

  RETURN_NOT_OK(tablet::TabletMetadata::CreateNew(fs_manager,
                                                  kSysCatalogTabletId,
                                                  table_name(),
                                                  table_id(),
                                                  schema, partition_schema,
                                                  partitions[0],
                                                  tablet::TABLET_DATA_READY,
                                                  /*tombstone_last_logged_opid=*/ boost::none,
                                                  &metadata));

  RaftConfigPB config;
  if (master_->opts().IsDistributed()) {
    RETURN_NOT_OK_PREPEND(CreateDistributedConfig(master_->opts(), &config),
                          "Failed to create new distributed Raft config");
  } else {
    config.set_obsolete_local(true);
    config.set_opid_index(consensus::kInvalidOpIdIndex);
    RaftPeerPB* peer = config.add_peers();
    peer->set_permanent_uuid(fs_manager->uuid());
    peer->set_member_type(RaftPeerPB::VOTER);
  }

  string tablet_id = metadata->tablet_id();
  RETURN_NOT_OK_PREPEND(cmeta_manager_->Create(tablet_id, config, consensus::kMinimumTerm),
                        "Unable to persist consensus metadata for tablet " + tablet_id);

  return SetupTablet(metadata);
}

Status SysCatalogTable::CreateDistributedConfig(const MasterOptions& options,
                                                RaftConfigPB* committed_config) {
  DCHECK(options.IsDistributed());

  RaftConfigPB new_config;
  new_config.set_obsolete_local(false);
  new_config.set_opid_index(consensus::kInvalidOpIdIndex);

  // Build the set of followers from our server options.
  for (const HostPort& host_port : options.master_addresses) {
    RaftPeerPB peer;
    HostPortPB peer_host_port_pb;
    RETURN_NOT_OK(HostPortToPB(host_port, &peer_host_port_pb));
    peer.mutable_last_known_addr()->CopyFrom(peer_host_port_pb);
    peer.set_member_type(RaftPeerPB::VOTER);
    new_config.add_peers()->CopyFrom(peer);
  }

  // Now resolve UUIDs.
  // By the time a SysCatalogTable is created and initted, the masters should be
  // starting up, so this should be fine to do.
  DCHECK(master_->messenger());
  RaftConfigPB resolved_config = new_config;
  resolved_config.clear_peers();
  for (const RaftPeerPB& peer : new_config.peers()) {
    if (peer.has_permanent_uuid()) {
      resolved_config.add_peers()->CopyFrom(peer);
    } else {
      LOG(INFO) << SecureShortDebugString(peer)
                << " has no permanent_uuid. Determining permanent_uuid...";
      RaftPeerPB new_peer = peer;
      RETURN_NOT_OK_PREPEND(consensus::SetPermanentUuidForRemotePeer(master_->messenger(),
                                                                     &new_peer),
                            Substitute("Unable to resolve UUID for peer $0",
                                       SecureShortDebugString(peer)));
      resolved_config.add_peers()->CopyFrom(new_peer);
    }
  }

  RETURN_NOT_OK(consensus::VerifyRaftConfig(resolved_config));
  VLOG(1) << "Distributed Raft configuration: " << SecureShortDebugString(resolved_config);

  *committed_config = resolved_config;
  return Status::OK();
}

void SysCatalogTable::SysCatalogStateChanged(const string& tablet_id, const string& reason) {
  CHECK_EQ(tablet_id, tablet_replica_->tablet_id());
  shared_ptr<consensus::RaftConsensus> consensus = tablet_replica_->shared_consensus();
  if (!consensus) {
    LOG_WITH_PREFIX(WARNING) << "Received notification of tablet state change "
                             << "but tablet no longer running. Tablet ID: "
                             << tablet_id << ". Reason: " << reason;
    return;
  }
  consensus::ConsensusStatePB cstate;
  Status s = consensus->ConsensusState(&cstate);
  if (PREDICT_FALSE(!s.ok())) {
    LOG_WITH_PREFIX(WARNING) << s.ToString();
    return;
  }
  LOG_WITH_PREFIX(INFO) << "SysCatalogTable state changed. Reason: " << reason << ". "
                        << "Latest consensus state: " << SecureShortDebugString(cstate);
  RaftPeerPB::Role new_role = GetConsensusRole(tablet_replica_->permanent_uuid(), cstate);
  LOG_WITH_PREFIX(INFO) << "This master's current role is: "
                        << RaftPeerPB::Role_Name(new_role);
  if (new_role == RaftPeerPB::LEADER) {
    Status s = leader_cb_.Run();

    // Callback errors are non-fatal only if the catalog manager has shut down.
    if (!s.ok()) {
      CHECK(!master_->catalog_manager()->IsInitialized()) << s.ToString();
    }
  }
}

Status SysCatalogTable::SetupTablet(const scoped_refptr<tablet::TabletMetadata>& metadata) {
  shared_ptr<Tablet> tablet;
  scoped_refptr<Log> log;

  InitLocalRaftPeerPB();

  // TODO(matteo): handle crash mid-creation of tablet? do we ever end up with
  // a partially created tablet here?
  tablet_replica_.reset(new TabletReplica(
      metadata,
      cmeta_manager_,
      local_peer_pb_,
      master_->tablet_apply_pool(),
      Bind(&SysCatalogTable::SysCatalogStateChanged, Unretained(this), metadata->tablet_id())));
  Status s = tablet_replica_->Init(master_->raft_pool());
  if (!s.ok()) {
    tablet_replica_->SetError(s);
    tablet_replica_->Shutdown();
    return s;
  }

  scoped_refptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK(cmeta_manager_->Load(metadata->tablet_id(), &cmeta));

  consensus::ConsensusBootstrapInfo consensus_info;
  tablet_replica_->SetBootstrapping();
  RETURN_NOT_OK(BootstrapTablet(metadata,
                                cmeta->CommittedConfig(),
                                scoped_refptr<clock::Clock>(master_->clock()),
                                master_->mem_tracker(),
                                scoped_refptr<rpc::ResultTracker>(),
                                metric_registry_,
                                tablet_replica_,
                                &tablet,
                                &log,
                                tablet_replica_->log_anchor_registry(),
                                &consensus_info));

  // TODO(matteo): Do we have a setSplittable(false) or something from the
  // outside is handling split in the TS?

  RETURN_NOT_OK_PREPEND(tablet_replica_->Start(consensus_info,
                                               tablet,
                                               scoped_refptr<clock::Clock>(master_->clock()),
                                               master_->messenger(),
                                               scoped_refptr<rpc::ResultTracker>(),
                                               log,
                                               master_->tablet_prepare_pool()),
                        "Failed to Start() TabletReplica");

  tablet_replica_->RegisterMaintenanceOps(master_->maintenance_manager());

  const Schema* schema = tablet->schema();
  schema_ = SchemaBuilder(*schema).BuildWithoutIds();
  key_schema_ = schema_.CreateKeyProjection();
  return Status::OK();
}

std::string SysCatalogTable::LogPrefix() const {
  return Substitute("T $0 P $1 [$2]: ",
                    tablet_replica_->tablet_id(),
                    tablet_replica_->permanent_uuid(),
                    table_name());
}

Status SysCatalogTable::WaitUntilRunning() {
  TRACE_EVENT0("master", "SysCatalogTable::WaitUntilRunning");
  int seconds_waited = 0;
  while (true) {
    Status status = tablet_replica_->WaitUntilConsensusRunning(MonoDelta::FromSeconds(1));
    seconds_waited++;
    if (status.ok()) {
      LOG_WITH_PREFIX(INFO) << "configured and running, proceeding with master startup.";
      break;
    }
    if (status.IsTimedOut()) {
      LOG_WITH_PREFIX(INFO) <<  "not online yet (have been trying for "
                               << seconds_waited << " seconds)";
      continue;
    }
    // if the status is not OK or TimedOut return it.
    return status;
  }
  return Status::OK();
}

Status SysCatalogTable::SyncWrite(const WriteRequestPB *req, WriteResponsePB *resp) {
  MAYBE_RETURN_FAILURE(FLAGS_sys_catalog_fail_during_write,
                       Status::RuntimeError(kInjectedFailureStatusMsg));

  CountDownLatch latch(1);
  gscoped_ptr<tablet::TransactionCompletionCallback> txn_callback(
      new LatchTransactionCompletionCallback<WriteResponsePB>(&latch, resp));
  unique_ptr<tablet::WriteTransactionState> tx_state(
      new tablet::WriteTransactionState(tablet_replica_.get(),
                                        req,
                                        nullptr, // No RequestIdPB
                                        resp));
  tx_state->set_completion_callback(std::move(txn_callback));

  RETURN_NOT_OK(tablet_replica_->SubmitWrite(std::move(tx_state)));
  latch.Wait();

  if (resp->has_error()) {
    return StatusFromPB(resp->error().status());
  }
  if (resp->per_row_errors_size() > 0) {
    for (const WriteResponsePB::PerRowErrorPB& error : resp->per_row_errors()) {
      LOG(WARNING) << "row " << error.row_index() << ": " << StatusFromPB(error.error()).ToString();
    }
    return Status::Corruption("One or more rows failed to write");
  }
  return Status::OK();
}

// Schema for the unified SysCatalogTable:
//
// (entry_type, entry_id) -> metadata
//
// entry_type is a enum defined in sys_tables. It indicates
// whether an entry is a table or a tablet.
//
// entry_type is the first part of a compound key as to allow
// efficient scans of entries of only a single type (e.g., only
// scan all of the tables, or only scan all of the tablets).
//
// entry_id is either a table id or a tablet id. For tablet entries,
// the table id that the tablet is associated with is stored in the
// protobuf itself.
Schema SysCatalogTable::BuildTableSchema() {
  SchemaBuilder builder;
  CHECK_OK(builder.AddKeyColumn(kSysCatalogTableColType, INT8));
  CHECK_OK(builder.AddKeyColumn(kSysCatalogTableColId, STRING));
  CHECK_OK(builder.AddColumn(kSysCatalogTableColMetadata, STRING));
  return builder.Build();
}

Status SysCatalogTable::Write(const Actions& actions) {
  TRACE_EVENT0("master", "SysCatalogTable::Write");

  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysCatalogTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  if (actions.table_to_add) {
    ReqAddTable(&req, actions.table_to_add);
  }
  if (actions.table_to_update) {
    ReqUpdateTable(&req, actions.table_to_update);
  }
  if (actions.table_to_delete) {
    ReqDeleteTable(&req, actions.table_to_delete);
  }

  ReqAddTablets(&req, actions.tablets_to_add);
  ReqUpdateTablets(&req, actions.tablets_to_update);
  ReqDeleteTablets(&req, actions.tablets_to_delete);

  if (actions.hms_notification_log_event_id) {
    ReqSetNotificationLogEventId(&req, *actions.hms_notification_log_event_id);
  }

  if (req.row_operations().rows().empty()) {
    // No actual changes were written (i.e the data to be updated matched the
    // previous version of the data).
    return Status::OK();
  }
  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

// ==================================================================
// Table related methods
// ==================================================================

void SysCatalogTable::ReqAddTable(WriteRequestPB* req,
                                  const scoped_refptr<TableInfo>& table) {
  VLOG(2) << "Adding table " << table->id() << " in catalog: " <<
      SecureShortDebugString(table->metadata().dirty().pb);

  faststring metadata_buf;
  pb_util::SerializeToString(table->metadata().dirty().pb, &metadata_buf);

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLES_ENTRY));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, table->id()));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColMetadata, metadata_buf));
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  enc.Add(RowOperationsPB::INSERT, row);
}

void SysCatalogTable::ReqUpdateTable(WriteRequestPB* req,
                                     const scoped_refptr<TableInfo>& table) {
  string diff;
  if (ArePBsEqual(table->metadata().state().pb,
                  table->metadata().dirty().pb,
                  VLOG_IS_ON(2) ? &diff : nullptr)) {
    // Short-circuit empty updates.
    return;
  }
  VLOG(2) << "Updating table " << table->id() << " in catalog: " << diff;

  faststring metadata_buf;
  pb_util::SerializeToString(table->metadata().dirty().pb, &metadata_buf);

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLES_ENTRY));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, table->id()));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColMetadata, metadata_buf));
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  enc.Add(RowOperationsPB::UPDATE, row);
}

void SysCatalogTable::ReqDeleteTable(WriteRequestPB* req,
                                     const scoped_refptr<TableInfo>& table) {
  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLES_ENTRY));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, table->id()));
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  enc.Add(RowOperationsPB::DELETE, row);
}

// Convert the sequence number into string padded with zeroes in the
// beginning -- that's the key for the new TSK record.
string SysCatalogTable::TskSeqNumberToEntryId(int64_t seq_number) {
  string entry_id;
  KeyEncoderTraits<DataType::INT64, string>::Encode(seq_number, &entry_id);
  return entry_id;
}

Status SysCatalogTable::VisitTables(TableVisitor* visitor) {
  TRACE_EVENT0("master", "SysCatalogTable::VisitTables");
  auto processor = [&](
      const string& entry_id,
      const SysTablesEntryPB& entry_data) {
    return visitor->VisitTable(entry_id, entry_data);
  };
  return ProcessRows<SysTablesEntryPB, TABLES_ENTRY>(processor);
}

template<typename T>
Status SysCatalogTable::GetEntryFromRow(
    const RowBlockRow& row, string* entry_id, T* entry_data) const {
  const Slice* id = schema_.ExtractColumnFromRow<STRING>(
      row, schema_.find_column(kSysCatalogTableColId));
  const Slice* data = schema_.ExtractColumnFromRow<STRING>(
      row, schema_.find_column(kSysCatalogTableColMetadata));
  string str_id = id->ToString();
  RETURN_NOT_OK_PREPEND(
      pb_util::ParseFromArray(entry_data, data->data(), data->size()),
      "unable to parse metadata field for row " + str_id);
  *entry_id = std::move(str_id);

  return Status::OK();
}

// Scan for entries of the specified type and run the specified function
// with each entry found.
template<typename T, SysCatalogTable::CatalogEntryType entry_type>
Status SysCatalogTable::ProcessRows(
    function<Status(const string&, const T&)> processor) const {
  const int type_col_idx = schema_.find_column(kSysCatalogTableColType);
  CHECK(type_col_idx != Schema::kColumnNotFound)
      << "cannot find sys catalog table column " << kSysCatalogTableColType
      << " in schema: " << schema_.ToString();

  static const int8_t kEntryType = entry_type;
  auto pred = ColumnPredicate::Equality(schema_.column(type_col_idx),
                                        &kEntryType);
  ScanSpec spec;
  spec.AddPredicate(pred);

  gscoped_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(tablet_replica_->tablet()->NewRowIterator(schema_, &iter));
  RETURN_NOT_OK(iter->Init(&spec));

  Arena arena(32 * 1024);
  RowBlock block(iter->schema(), 512, &arena);
  while (iter->HasNext()) {
    RETURN_NOT_OK(iter->NextBlock(&block));
    const size_t nrows = block.nrows();
    for (size_t i = 0; i < nrows; ++i) {
      if (!block.selection_vector()->IsRowSelected(i)) {
        continue;
      }
      string entry_id;
      T entry_data;
      RETURN_NOT_OK(GetEntryFromRow(block.row(i), &entry_id, &entry_data));
      RETURN_NOT_OK(processor(entry_id, entry_data));
    }
  }
  return Status::OK();
}

Status SysCatalogTable::VisitTskEntries(TskEntryVisitor* visitor) {
  TRACE_EVENT0("master", "SysCatalogTable::VisitTskEntries");
  auto processor = [&](
      const string& entry_id,
      const SysTskEntryPB& entry_data) {
    return visitor->Visit(entry_id, entry_data);
  };
  return ProcessRows<SysTskEntryPB, TSK_ENTRY>(processor);
}

Status SysCatalogTable::GetLatestNotificationLogEventId(int64_t* event_id) {
  TRACE_EVENT0("master", "SysCatalogTable::GetLatestNotificationLogEventId");

  *event_id = -1;
  auto processor = [&](const string& entry_id, const SysNotificationLogEventIdPB& entry_data) {
    if (entry_id != kLatestNotificationLogEntryIdRowId) {
      // This is not the row we're looking for.
      return Status::OK();
    }
    DCHECK(entry_data.has_latest_notification_log_event_id());
    DCHECK(entry_data.latest_notification_log_event_id() >= 0);
    *event_id = entry_data.latest_notification_log_event_id();
    return Status::OK();
  };

  return ProcessRows<SysNotificationLogEventIdPB, HMS_NOTIFICATION_LOG>(processor);
}

Status SysCatalogTable::GetCertAuthorityEntry(SysCertAuthorityEntryPB* entry) {
  CHECK(entry);
  vector<SysCertAuthorityEntryPB> entries;
  auto processor = [&](
      const string& entry_id,
      const SysCertAuthorityEntryPB& entry_data) {
    CHECK_EQ(entry_id, kSysCertAuthorityEntryId);
    entries.push_back(entry_data);
    return Status::OK();
  };
  RETURN_NOT_OK((
      ProcessRows<SysCertAuthorityEntryPB, CERT_AUTHORITY_INFO>(processor)));
  // There should be no more than one root CA entry in the system table.
  CHECK_LE(entries.size(), 1);
  if (entries.empty()) {
    return Status::NotFound("root CA entry not found");
  }
  entries.front().Swap(entry);

  return Status::OK();
}

Status SysCatalogTable::AddCertAuthorityEntry(
    const SysCertAuthorityEntryPB& entry) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysCatalogTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  faststring metadata_buf;
  pb_util::SerializeToString(entry, &metadata_buf);

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetInt8(kSysCatalogTableColType, CERT_AUTHORITY_INFO));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, kSysCertAuthorityEntryId));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColMetadata, metadata_buf));
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::INSERT, row);

  return SyncWrite(&req, &resp);
}

Status SysCatalogTable::AddTskEntry(const SysTskEntryPB& entry) {
  WriteRequestPB req;
  WriteResponsePB resp;

  req.set_tablet_id(kSysCatalogTabletId);
  CHECK_OK(SchemaToPB(schema_, req.mutable_schema()));

  CHECK(entry.tsk().has_key_seq_num());
  CHECK(entry.tsk().has_expire_unix_epoch_seconds());
  CHECK(entry.tsk().has_rsa_key_der());

  faststring metadata_buf;
  pb_util::SerializeToString(entry, &metadata_buf);

  // This is crucial to keep entry_id alive until its put into the
  // WriteRequestPB object by RowOperationsPBEncoder.
  const string entry_id = TskSeqNumberToEntryId(entry.tsk().key_seq_num());

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetInt8(kSysCatalogTableColType, TSK_ENTRY));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, entry_id));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColMetadata, metadata_buf));
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::INSERT, row);

  return SyncWrite(&req, &resp);
}

Status SysCatalogTable::RemoveTskEntries(const set<string>& entry_ids) {
  WriteRequestPB req;
  WriteResponsePB resp;

  req.set_tablet_id(kSysCatalogTabletId);
  CHECK_OK(SchemaToPB(schema_, req.mutable_schema()));
  for (const auto& id : entry_ids) {
    KuduPartialRow row(&schema_);
    CHECK_OK(row.SetInt8(kSysCatalogTableColType, TSK_ENTRY));
    CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, id));
    RowOperationsPBEncoder enc(req.mutable_row_operations());
    enc.Add(RowOperationsPB::DELETE, row);
  }

  return SyncWrite(&req, &resp);
}

// ==================================================================
// Tablet related methods
// ==================================================================

void SysCatalogTable::ReqAddTablets(WriteRequestPB* req,
                                    const vector<scoped_refptr<TabletInfo>>& tablets) {
  faststring metadata_buf;
  KuduPartialRow row(&schema_);
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  for (const auto& tablet : tablets) {
    VLOG(2) << "Adding tablet " << tablet->id() << " in catalog: "
            << SecureShortDebugString(tablet->metadata().dirty().pb);
    pb_util::SerializeToString(tablet->metadata().dirty().pb, &metadata_buf);
    CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLETS_ENTRY));
    CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, tablet->id()));
    CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColMetadata, metadata_buf));
    enc.Add(RowOperationsPB::INSERT, row);
  }
}

void SysCatalogTable::ReqUpdateTablets(WriteRequestPB* req,
                                       const vector<scoped_refptr<TabletInfo>>& tablets) {
  faststring metadata_buf;
  KuduPartialRow row(&schema_);
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  for (const auto& tablet : tablets) {
    string diff;
    if (ArePBsEqual(tablet->metadata().state().pb,
                    tablet->metadata().dirty().pb,
                    VLOG_IS_ON(2) ? &diff : nullptr)) {
      // Short-circuit empty updates.
      continue;
    }
    VLOG(2) << "Updating tablet " << tablet->id() << " in catalog: "
            << diff;
    pb_util::SerializeToString(tablet->metadata().dirty().pb, &metadata_buf);
    CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLETS_ENTRY));
    CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, tablet->id()));
    CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColMetadata, metadata_buf));
    enc.Add(RowOperationsPB::UPDATE, row);
  }
}

void SysCatalogTable::ReqDeleteTablets(WriteRequestPB* req,
                                       const vector<scoped_refptr<TabletInfo>>& tablets) {
  KuduPartialRow row(&schema_);
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  for (const auto& tablet : tablets) {
    CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLETS_ENTRY));
    CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, tablet->id()));
    enc.Add(RowOperationsPB::DELETE, row);
  }
}

void SysCatalogTable::ReqSetNotificationLogEventId(WriteRequestPB* req, int64_t event_id) {
  SysNotificationLogEventIdPB pb;
  pb.set_latest_notification_log_event_id(event_id);
  faststring metadata_buf;
  pb_util::SerializeToString(pb, &metadata_buf);

  KuduPartialRow row(&schema_);
  RowOperationsPBEncoder enc(req->mutable_row_operations());
  CHECK_OK(row.SetInt8(kSysCatalogTableColType, HMS_NOTIFICATION_LOG));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColId, kLatestNotificationLogEntryIdRowId));
  CHECK_OK(row.SetStringNoCopy(kSysCatalogTableColMetadata, metadata_buf));
  enc.Add(RowOperationsPB::UPSERT, row);
}

Status SysCatalogTable::VisitTablets(TabletVisitor* visitor) {
  TRACE_EVENT0("master", "SysCatalogTable::VisitTablets");
  auto processor = [&](
      const string& entry_id,
      const SysTabletsEntryPB& entry_data) {
    if (entry_data.has_partition()) {
      return visitor->VisitTablet(entry_data.table_id(), entry_id, entry_data);
    }
    // Upgrade from the deprecated start/end-key fields to the 'partition' field.
    SysTabletsEntryPB metadata = entry_data;
    metadata.mutable_partition()->set_partition_key_start(
        entry_data.deprecated_start_key());
    metadata.clear_deprecated_start_key();
    metadata.mutable_partition()->set_partition_key_end(
        entry_data.deprecated_end_key());
    metadata.clear_deprecated_end_key();
    return visitor->VisitTablet(metadata.table_id(), entry_id, metadata);
  };
  return ProcessRows<SysTabletsEntryPB, TABLETS_ENTRY>(processor);
}

void SysCatalogTable::InitLocalRaftPeerPB() {
  local_peer_pb_.set_permanent_uuid(master_->fs_manager()->uuid());
  Sockaddr addr = master_->first_rpc_address();
  HostPort hp;
  CHECK_OK(HostPortFromSockaddrReplaceWildcard(addr, &hp));
  CHECK_OK(HostPortToPB(hp, local_peer_pb_.mutable_last_known_addr()));
}

} // namespace master
} // namespace kudu
