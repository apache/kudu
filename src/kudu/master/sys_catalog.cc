// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/master/sys_catalog.h"

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/schema.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_peers.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/quorum_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/tablet/tablet_bootstrap.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/logging.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/threadpool.h"

using kudu::consensus::ConsensusMetadata;
using kudu::consensus::QuorumPB;
using kudu::consensus::QuorumPeerPB;
using kudu::log::Log;
using kudu::log::LogAnchorRegistry;
using kudu::tablet::LatchTransactionCompletionCallback;
using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using strings::Substitute;

namespace kudu {
namespace master {

static const char* const kSysCatalogTabletId = "00000000000000000000000000000000";

static const char* const kSysCatalogTableColType = "entry_type";
static const char* const kSysCatalogTableColId = "entry_id";
static const char* const kSysCatalogTableColMetadata = "metadata";

SysCatalogTable::SysCatalogTable(Master* master,
                                 MetricRegistry* metrics,
                                 const ElectedLeaderCallback& leader_cb)
    : metric_registry_(metrics),
      master_(master),
      leader_cb_(leader_cb),
      old_role_(QuorumPeerPB::FOLLOWER) {
  CHECK_OK(ThreadPoolBuilder("apply").Build(&apply_pool_));
}

SysCatalogTable::~SysCatalogTable() {
}

void SysCatalogTable::Shutdown() {
  if (tablet_peer_) {
    tablet_peer_->Shutdown();
  }
  apply_pool_->Shutdown();
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

  // Allow for statically and explicitly assigning the quorum and roles through
  // the master configuration on startup.
  //
  // TODO: The following assumptions need revisiting:
  // 1. We always believe the local config options for who is in the quorum.
  // 2. We always want to look up all node's UUIDs on start (via RPC).
  //    - TODO: Cache UUIDs. See KUDU-526.
  if (master_->opts().IsDistributed()) {
    LOG(INFO) << "Configuring the quorum for distributed operation...";

    string tablet_id = metadata->tablet_id();
    gscoped_ptr<ConsensusMetadata> cmeta;
    RETURN_NOT_OK_PREPEND(ConsensusMetadata::Load(fs_manager, tablet_id, &cmeta),
                          "Unable to load consensus metadata for tablet " + tablet_id);

    RETURN_NOT_OK(SetupDistributedQuorum(master_->opts(),
                                         cmeta->mutable_pb()->mutable_committed_quorum()));
    RETURN_NOT_OK_PREPEND(cmeta->Flush(),
                          "Unable to persist consensus metadata for tablet " + tablet_id);
  }

  RETURN_NOT_OK(SetupTablet(metadata));
  return Status::OK();
}

Status SysCatalogTable::CreateNew(FsManager *fs_manager) {
  // Create the new Metadata
  scoped_refptr<tablet::TabletMetadata> metadata;
  RETURN_NOT_OK(tablet::TabletMetadata::CreateNew(fs_manager,
                                                  kSysCatalogTabletId,
                                                  table_name(),
                                                  BuildTableSchema(),
                                                  "", "",
                                                  tablet::REMOTE_BOOTSTRAP_DONE,
                                                  &metadata));

  QuorumPB quorum;
  if (master_->opts().IsDistributed()) {
    RETURN_NOT_OK_PREPEND(SetupDistributedQuorum(master_->opts(), &quorum),
                          "Failed to initialize distributed quorum");
  } else {
    quorum.set_local(true);
    quorum.set_opid_index(consensus::kInvalidOpIdIndex);
    QuorumPeerPB* peer = quorum.add_peers();
    peer->set_permanent_uuid(fs_manager->uuid());
    peer->set_member_type(QuorumPeerPB::VOTER);
  }

  string tablet_id = metadata->tablet_id();
  gscoped_ptr<ConsensusMetadata> cmeta;
  RETURN_NOT_OK_PREPEND(ConsensusMetadata::Create(fs_manager, tablet_id, quorum,
                                                  consensus::kMinimumTerm, &cmeta),
                        "Unable to persist consensus metadata for tablet " + tablet_id);

  return SetupTablet(metadata);
}

Status SysCatalogTable::SetupDistributedQuorum(const MasterOptions& options,
                                               QuorumPB* committed_quorum) {
  DCHECK(options.IsDistributed());

  QuorumPB new_quorum;
  new_quorum.set_local(false);
  new_quorum.set_opid_index(consensus::kInvalidOpIdIndex);

  // Build the set of followers from our server options.
  BOOST_FOREACH(const HostPort& host_port, options.master_quorum) {
    QuorumPeerPB peer;
    HostPortPB peer_host_port_pb;
    RETURN_NOT_OK(HostPortToPB(host_port, &peer_host_port_pb));
    peer.mutable_last_known_addr()->CopyFrom(peer_host_port_pb);
    peer.set_member_type(QuorumPeerPB::VOTER);
    new_quorum.add_peers()->CopyFrom(peer);
  }

  // Now resolve UUIDs.
  // By the time a SysCatalogTable is created and initted, the masters should be
  // starting up, so this should be fine to do.
  DCHECK(master_->messenger());
  QuorumPB resolved_quorum = new_quorum;
  resolved_quorum.clear_peers();
  BOOST_FOREACH(const QuorumPeerPB& peer, new_quorum.peers()) {
    if (peer.has_permanent_uuid()) {
      resolved_quorum.add_peers()->CopyFrom(peer);
    } else {
      LOG(INFO) << peer.ShortDebugString()
                << " has no permanent_uuid. Determining permanent_uuid...";
      QuorumPeerPB new_peer = peer;
      // TODO: Use ConsensusMetadata to cache the results of these lookups so
      // we only require RPC access to the full quorum on first startup.
      // See KUDU-526.
      RETURN_NOT_OK_PREPEND(consensus::SetPermanentUuidForRemotePeer(master_->messenger(),
                                                                     &new_peer),
                            Substitute("Unable to resolve UUID for peer $0",
                                       peer.ShortDebugString()));
      resolved_quorum.add_peers()->CopyFrom(new_peer);
    }
  }

  RETURN_NOT_OK(consensus::VerifyQuorum(resolved_quorum, consensus::COMMITTED_QUORUM));
  VLOG(1) << "Distributed quorum configuration: " << resolved_quorum.ShortDebugString();

  *committed_quorum = resolved_quorum;
  return Status::OK();
}

void SysCatalogTable::SysCatalogStateChanged(const std::string& tablet_id) {
  CHECK_EQ(tablet_peer_->tablet_id(), tablet_id);
  QuorumPB quorum = tablet_peer_->consensus()->Quorum();
  LOG_WITH_PREFIX(INFO) << " SysCatalogTable state changed. New quorum config:"
                        << quorum.ShortDebugString();
  QuorumPeerPB::Role new_role = tablet_peer_->consensus()->role();
  LOG_WITH_PREFIX(INFO) << " This master's current role is: "
                        << QuorumPeerPB::Role_Name(new_role)
                        << ", previous role was: " << QuorumPeerPB::Role_Name(old_role_);
  if (new_role == QuorumPeerPB::LEADER) {
    CHECK_OK(leader_cb_.Run());
  }
}

Status SysCatalogTable::SetupTablet(const scoped_refptr<tablet::TabletMetadata>& metadata) {
  shared_ptr<Tablet> tablet;
  scoped_refptr<Log> log;

  // TODO: handle crash mid-creation of tablet? do we ever end up with a
  // partially created tablet here?
  tablet_peer_.reset(new TabletPeer(
      metadata,
      apply_pool_.get(),
      Bind(&SysCatalogTable::SysCatalogStateChanged, Unretained(this), metadata->tablet_id())));

  consensus::ConsensusBootstrapInfo consensus_info;
  RETURN_NOT_OK(BootstrapTablet(metadata,
                                scoped_refptr<server::Clock>(master_->clock()),
                                master_->mem_tracker(),
                                metric_registry_,
                                tablet_peer_->status_listener(),
                                &tablet,
                                &log,
                                tablet_peer_->log_anchor_registry(),
                                &consensus_info));

  // TODO: Do we have a setSplittable(false) or something from the outside is
  // handling split in the TS?

  RETURN_NOT_OK_PREPEND(tablet_peer_->Init(tablet,
                                           scoped_refptr<server::Clock>(master_->clock()),
                                           master_->messenger(),
                                           log,
                                           tablet->GetMetricEntity(),
                                           master_->mem_tracker()),
                        "Failed to Init() TabletPeer");

  RETURN_NOT_OK_PREPEND(tablet_peer_->Start(consensus_info),
                        "Failed to Start() TabletPeer");

  tablet_peer_->RegisterMaintenanceOps(master_->maintenance_manager());

  shared_ptr<Schema> schema(tablet->schema());
  schema_ = SchemaBuilder(*schema.get()).BuildWithoutIds();
  key_schema_ = schema_.CreateKeyProjection();
  return Status::OK();
}

std::string SysCatalogTable::LogPrefix() const {
  return Substitute("T $0 P $1 [$2]: ",
                    tablet_peer_->tablet_id(),
                    tablet_peer_->consensus()->peer_uuid(),
                    table_name());
}

Status SysCatalogTable::WaitUntilRunning() {
  int seconds_waited = 0;
  while (true) {
    Status status = tablet_peer_->WaitUntilConsensusRunning(MonoDelta::FromSeconds(1));
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
  CountDownLatch latch(1);
  gscoped_ptr<tablet::TransactionCompletionCallback> txn_callback(
    new LatchTransactionCompletionCallback<WriteResponsePB>(&latch, resp));
  tablet::WriteTransactionState *tx_state =
    new tablet::WriteTransactionState(tablet_peer_.get(), req, resp);
  tx_state->set_completion_callback(txn_callback.Pass());

  RETURN_NOT_OK(tablet_peer_->SubmitWrite(tx_state));
  latch.Wait();

  if (resp->has_error()) {
    return StatusFromPB(resp->error().status());
  }
  if (resp->per_row_errors_size() > 0) {
    BOOST_FOREACH(const WriteResponsePB::PerRowErrorPB& error, resp->per_row_errors()) {
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

// ==================================================================
// Table related methods
// ==================================================================

Status SysCatalogTable::AddTable(const TableInfo *table) {
  faststring metadata_buf;
  if (!pb_util::SerializeToString(table->metadata().dirty().pb, &metadata_buf)) {
    return Status::Corruption("Unable to serialize SysCatalogTablesEntryPB for tablet",
                              table->metadata().dirty().name());
  }

  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysCatalogTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLES_ENTRY));
  CHECK_OK(row.SetString(kSysCatalogTableColId, table->id()));
  CHECK_OK(row.SetString(kSysCatalogTableColMetadata, metadata_buf));
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::INSERT, row);

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

Status SysCatalogTable::UpdateTable(const TableInfo *table) {
  faststring metadata_buf;
  if (!pb_util::SerializeToString(table->metadata().dirty().pb, &metadata_buf)) {
    return Status::Corruption("Unable to serialize SysCatalogTablesEntryPB for tablet",
                              table->id());
  }

  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysCatalogTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLES_ENTRY));
  CHECK_OK(row.SetString(kSysCatalogTableColId, table->id()));
  CHECK_OK(row.SetString(kSysCatalogTableColMetadata, metadata_buf));
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::UPDATE, row);

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

Status SysCatalogTable::DeleteTable(const TableInfo *table) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysCatalogTableColMetadata);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLES_ENTRY));
  CHECK_OK(row.SetString(kSysCatalogTableColId, table->id()));

  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::DELETE, row);

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

Status SysCatalogTable::VisitTables(TableVisitor* visitor) {
  const int8_t tables_entry = TABLES_ENTRY;
  const int type_col_idx = schema_.find_column(kSysCatalogTableColType);
  CHECK(type_col_idx != Schema::kColumnNotFound);

  ColumnRangePredicate pred_tables(schema_.column(type_col_idx),
                                   &tables_entry, &tables_entry);
  ScanSpec spec;
  spec.AddPredicate(pred_tables);

  gscoped_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(tablet_peer_->tablet()->NewRowIterator(schema_, &iter));
  RETURN_NOT_OK(iter->Init(&spec));

  Arena arena(32 * 1024, 256 * 1024);
  RowBlock block(iter->schema(), 512, &arena);
  while (iter->HasNext()) {
    RETURN_NOT_OK(iter->NextBlock(&block));
    for (size_t i = 0; i < block.nrows(); i++) {
      if (!block.selection_vector()->IsRowSelected(i)) continue;

      RETURN_NOT_OK(VisitTableFromRow(block.row(i), visitor));
    }
  }
  return Status::OK();
}

Status SysCatalogTable::VisitTableFromRow(const RowBlockRow& row,
                                          TableVisitor* visitor) {
  const Slice* table_id =
      schema_.ExtractColumnFromRow<STRING>(row, schema_.find_column(kSysCatalogTableColId));
  const Slice* data =
      schema_.ExtractColumnFromRow<STRING>(row, schema_.find_column(kSysCatalogTableColMetadata));

  SysTablesEntryPB metadata;
  RETURN_NOT_OK_PREPEND(pb_util::ParseFromArray(&metadata, data->data(), data->size()),
                        "Unable to parse metadata field for table " + table_id->ToString());

  RETURN_NOT_OK(visitor->VisitTable(table_id->ToString(), metadata));
  return Status::OK();
}

// ==================================================================
// Tablet related methods
// ==================================================================

Status SysCatalogTable::AddTabletsToPB(const vector<TabletInfo*>& tablets,
                                       RowOperationsPB::Type op_type,
                                       RowOperationsPB* ops) const {
  faststring metadata_buf;
  KuduPartialRow row(&schema_);
  RowOperationsPBEncoder enc(ops);
  BOOST_FOREACH(const TabletInfo *tablet, tablets) {
    if (!pb_util::SerializeToString(tablet->metadata().dirty().pb, &metadata_buf)) {
      return Status::Corruption("Unable to serialize SysCatalogTabletsEntryPB for tablet",
                                tablet->tablet_id());
    }

    CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLETS_ENTRY));
    CHECK_OK(row.SetString(kSysCatalogTableColId, tablet->tablet_id()));
    CHECK_OK(row.SetString(kSysCatalogTableColMetadata, metadata_buf));
    enc.Add(op_type, row);
  }
  return Status::OK();
}

Status SysCatalogTable::AddAndUpdateTablets(const vector<TabletInfo*>& tablets_to_add,
                                            const vector<TabletInfo*>& tablets_to_update) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysCatalogTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  // Insert new Tablets
  if (!tablets_to_add.empty()) {
    RETURN_NOT_OK(AddTabletsToPB(tablets_to_add, RowOperationsPB::INSERT,
                                 req.mutable_row_operations()));
  }

  // Update already existing Tablets
  if (!tablets_to_update.empty()) {
    RETURN_NOT_OK(AddTabletsToPB(tablets_to_update, RowOperationsPB::UPDATE,
                                 req.mutable_row_operations()));
  }

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

Status SysCatalogTable::AddTablets(const vector<TabletInfo*>& tablets) {
  vector<TabletInfo*> empty_tablets;
  return AddAndUpdateTablets(tablets, empty_tablets);
}

Status SysCatalogTable::UpdateTablets(const vector<TabletInfo*>& tablets) {
  vector<TabletInfo*> empty_tablets;
  return AddAndUpdateTablets(empty_tablets, tablets);
}

Status SysCatalogTable::DeleteTablets(const vector<TabletInfo*>& tablets) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysCatalogTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  RowOperationsPBEncoder enc(req.mutable_row_operations());
  KuduPartialRow row(&schema_);
  BOOST_FOREACH(const TabletInfo* tablet, tablets) {
    CHECK_OK(row.SetInt8(kSysCatalogTableColType, TABLETS_ENTRY));
    CHECK_OK(row.SetString(kSysCatalogTableColId, tablet->tablet_id()));
    enc.Add(RowOperationsPB::DELETE, row);
  }

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

Status SysCatalogTable::VisitTabletFromRow(const RowBlockRow& row, TabletVisitor *visitor) {
  const Slice *tablet_id =
    schema_.ExtractColumnFromRow<STRING>(row, schema_.find_column(kSysCatalogTableColId));
  const Slice *data =
    schema_.ExtractColumnFromRow<STRING>(row, schema_.find_column(kSysCatalogTableColMetadata));

  SysTabletsEntryPB metadata;
  RETURN_NOT_OK_PREPEND(pb_util::ParseFromArray(&metadata, data->data(), data->size()),
                        "Unable to parse metadata field for tablet " + tablet_id->ToString());

  RETURN_NOT_OK(visitor->VisitTablet(metadata.table_id(), tablet_id->ToString(), metadata));
  return Status::OK();
}

Status SysCatalogTable::VisitTablets(TabletVisitor* visitor) {
  const int8_t tablets_entry = TABLETS_ENTRY;
  const int type_col_idx = schema_.find_column(kSysCatalogTableColType);
  CHECK(type_col_idx != Schema::kColumnNotFound);

  ColumnRangePredicate pred_tablets(schema_.column(type_col_idx),
                                   &tablets_entry, &tablets_entry);
  ScanSpec spec;
  spec.AddPredicate(pred_tablets);

  gscoped_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(tablet_peer_->tablet()->NewRowIterator(schema_, &iter));
  RETURN_NOT_OK(iter->Init(&spec));

  Arena arena(32 * 1024, 256 * 1024);
  RowBlock block(iter->schema(), 512, &arena);
  while (iter->HasNext()) {
    RETURN_NOT_OK(iter->NextBlock(&block));
    for (size_t i = 0; i < block.nrows(); i++) {
      if (!block.selection_vector()->IsRowSelected(i)) continue;

      RETURN_NOT_OK(VisitTabletFromRow(block.row(i), visitor));
    }
  }
  return Status::OK();
}


} // namespace master
} // namespace kudu
