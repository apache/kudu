// Copyright (c) 2013, Cloudera, inc.

#include "kudu/master/sys_tables.h"

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/schema.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/opid_anchor_registry.h"
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
#include "kudu/util/pb_util.h"

using kudu::log::Log;
using kudu::log::OpIdAnchorRegistry;
using kudu::metadata::QuorumPB;
using kudu::metadata::QuorumPeerPB;
using kudu::tablet::LatchTransactionCompletionCallback;
using kudu::tablet::Tablet;
using kudu::tablet::TabletMasterBlockPB;
using kudu::tablet::TabletPeer;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using strings::Substitute;

namespace kudu {
namespace master {

// ===========================================================================
//  Abstract SysTable
// ===========================================================================
SysTable::SysTable(Master* master,
                   MetricRegistry* metrics,
                   const string& name)
  : metric_ctx_(metrics, name),
    master_(master) {
  CHECK_OK(TaskExecutorBuilder("ldr-apply").Build(&leader_apply_executor_));
  CHECK_OK(TaskExecutorBuilder("repl-apply").Build(&replica_apply_executor_));
}

void SysTable::Shutdown() {
  if (tablet_peer_) {
    tablet_peer_->Shutdown();
  }
  leader_apply_executor_->Shutdown();
  replica_apply_executor_->Shutdown();
}

Status SysTable::Load(FsManager *fs_manager) {
  tablet::TabletMasterBlockPB master_block;
  SetupTabletMasterBlock(&master_block);

  // Load Metadata Information from disk
  scoped_refptr<tablet::TabletMetadata> metadata;
  RETURN_NOT_OK(tablet::TabletMetadata::Load(fs_manager, master_block, &metadata));

  // Verify that the schema is the current one
  if (!metadata->schema().Equals(BuildTableSchema())) {
    // TODO: In this case we probably should execute the migration step.
    return(Status::Corruption("Unexpected schema", metadata->schema().ToString()));
  }

  QuorumPeerPB quorum_peer;
  quorum_peer.set_permanent_uuid(fs_manager->uuid());

  RETURN_NOT_OK(SetupTablet(metadata, quorum_peer));
  return Status::OK();
}

Status SysTable::CreateNew(FsManager *fs_manager) {
  TabletMasterBlockPB master_block;
  SetupTabletMasterBlock(&master_block);

  QuorumPeerPB quorum_peer;
  quorum_peer.set_permanent_uuid(fs_manager->uuid());

  // TODO For dist consensus get the quorum with other peers.
  QuorumPB quorum;
  quorum.set_local(true);
  quorum.set_seqno(0);
  quorum.add_peers()->CopyFrom(quorum_peer);

  // Create the new Metadata
  scoped_refptr<tablet::TabletMetadata> metadata;
  RETURN_NOT_OK(tablet::TabletMetadata::CreateNew(fs_manager,
                                                    master_block,
                                                    table_name(),
                                                    BuildTableSchema(),
                                                    quorum,
                                                    "", "",
                                                    tablet::REMOTE_BOOTSTRAP_DONE,
                                                    &metadata));
  return SetupTablet(metadata, quorum_peer);
}

void SysTable::SysTableStateChanged(TabletPeer* tablet_peer) {
  LOG(INFO) << "SysTable state changed. New quorum config: "
      << tablet_peer->Quorum().ShortDebugString();
}

Status SysTable::SetupTablet(const scoped_refptr<tablet::TabletMetadata>& metadata,
                             const QuorumPeerPB& quorum_peer) {
  shared_ptr<Tablet> tablet;
  gscoped_ptr<Log> log;
  scoped_refptr<OpIdAnchorRegistry> opid_anchor_registry;

  // TODO: handle crash mid-creation of tablet? do we ever end up with a
  // partially created tablet here?
  // TODO right now sys tables are hard coded to be single quorum, so the MarkDirty
  // callback that allows to notify of state changes (such as consensus role changes)
  // just points to SysTableStateChanged(), which currently LOG(FATAL)s.
  tablet_peer_.reset(new TabletPeer(metadata,
                                    quorum_peer,
                                    leader_apply_executor_.get(),
                                    replica_apply_executor_.get(),
                                    boost::bind(&SysTable::SysTableStateChanged, this, _1)));
  consensus::ConsensusBootstrapInfo consensus_info;
  RETURN_NOT_OK(BootstrapTablet(metadata,
                                scoped_refptr<server::Clock>(master_->clock()),
                                &metric_ctx_,
                                tablet_peer_->status_listener(),
                                &tablet,
                                &log,
                                &opid_anchor_registry,
                                &consensus_info));

  // TODO: Do we have a setSplittable(false) or something from the outside is
  // handling split in the TS?

  RETURN_NOT_OK_PREPEND(tablet_peer_->Init(tablet,
                                           scoped_refptr<server::Clock>(master_->clock()),
                                           master_->messenger(),
                                           log.Pass(),
                                           *tablet->GetMetricContext()),
                        "Failed to Init() TabletPeer");

  RETURN_NOT_OK_PREPEND(tablet_peer_->Start(consensus_info),
                        "Failed to Start() TabletPeer");

  // We need to wait for the tablet to become online before proceeding.
  // We just try forever since we can't do anything with the systable unless it is
  // running, we just make sure to make it clear to the user the reason of the
  // wait.
  Status status = WaitUntilRunning();
  if (!status.ok()) {
    LOG(FATAL) << "Illegal State while waiting for the tablet to become online: "
        << status.ToString();
  }

  shared_ptr<Schema> schema(tablet->schema());
  schema_ = SchemaBuilder(*schema.get()).BuildWithoutIds();
  key_schema_ = schema_.CreateKeyProjection();
  return Status::OK();
}

Status SysTable::WaitUntilRunning() {
  int seconds_waited = 0;
  while (true) {
    Status status = tablet_peer_->WaitUntilRunning(MonoDelta::FromSeconds(1));
    seconds_waited++;
    if (status.ok()) {
      LOG(INFO) << "SysTable tablet configured and running, proceeding with master startup.";
      break;
    }
    if (status.IsTimedOut()) {
      LOG(WARNING) << "SysTable tablet not online yet. Have been trying for "
          << seconds_waited << " seconds.";
      continue;
    }
    // if the status is not OK or TimedOut return it.
    return status;
  }
  return Status::OK();
}

Status SysTable::SyncWrite(const WriteRequestPB *req, WriteResponsePB *resp) {
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

// ===========================================================================
//  Sys-Bootstrap Locations Table
// ===========================================================================
static const char *kSysTabletsTabletId = "00000000000000000000000000000000";

static const char *kSysTabletsColTableId  = "table_id";
static const char *kSysTabletsColTabletId = "tablet_id";
static const char *kSysTabletsColMetadata = "metadata";

Schema SysTabletsTable::BuildTableSchema() {
  SchemaBuilder builder;
  CHECK_OK(builder.AddKeyColumn(kSysTabletsColTableId, STRING));
  CHECK_OK(builder.AddKeyColumn(kSysTabletsColTabletId, STRING));
  CHECK_OK(builder.AddColumn(kSysTabletsColMetadata, STRING));
  return builder.Build();
}

void SysTabletsTable::SetupTabletMasterBlock(TabletMasterBlockPB *master_block) {
  master_block->set_tablet_id(kSysTabletsTabletId);
  master_block->set_block_a("00000000000000000000000000000000");
  master_block->set_block_b("11111111111111111111111111111111");
}

// TODO: move out as a generic FullTableScan()?
Status SysTabletsTable::VisitTablets(Visitor *visitor) {
  gscoped_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(tablet_peer_->tablet()->NewRowIterator(schema_, &iter));
  RETURN_NOT_OK(iter->Init(NULL));

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

Status SysTabletsTable::VisitTabletFromRow(const RowBlockRow& row, Visitor *visitor) {
  const Slice *table_id =
    schema_.ExtractColumnFromRow<STRING>(row, schema_.find_column(kSysTabletsColTableId));
  const Slice *tablet_id =
    schema_.ExtractColumnFromRow<STRING>(row, schema_.find_column(kSysTabletsColTabletId));
  const Slice *data =
    schema_.ExtractColumnFromRow<STRING>(row, schema_.find_column(kSysTabletsColMetadata));

  SysTabletsEntryPB metadata;
  RETURN_NOT_OK_PREPEND(pb_util::ParseFromArray(&metadata, data->data(), data->size()),
                        "Unable to parse metadata field for tablet " + tablet_id->ToString());

  RETURN_NOT_OK(visitor->VisitTablet(table_id->ToString(), tablet_id->ToString(), metadata));
  return Status::OK();
}

Status SysTabletsTable::AddTablets(const vector<TabletInfo*>& tablets) {
  vector<TabletInfo*> empty_tablets;
  return AddAndUpdateTablets(tablets, empty_tablets);
}

Status SysTabletsTable::UpdateTablets(const vector<TabletInfo*>& tablets) {
  vector<TabletInfo*> empty_tablets;
  return AddAndUpdateTablets(empty_tablets, tablets);
}

Status SysTabletsTable::AddTabletsToPB(const vector<TabletInfo*>& tablets,
                                       RowOperationsPB::Type op_type,
                                       RowOperationsPB* ops) const {
  faststring metadata_buf;
  KuduPartialRow row(&schema_);
  RowOperationsPBEncoder enc(ops);
  BOOST_FOREACH(const TabletInfo *tablet, tablets) {
    if (!pb_util::SerializeToString(tablet->metadata().dirty().pb, &metadata_buf)) {
      return Status::Corruption("Unable to serialize SysTabletsEntryPB for tablet",
                                tablet->tablet_id());
    }

    CHECK_OK(row.SetString(kSysTabletsColTableId, tablet->table()->id()));
    CHECK_OK(row.SetString(kSysTabletsColTabletId, tablet->tablet_id()));
    CHECK_OK(row.SetString(kSysTabletsColMetadata, metadata_buf));
    enc.Add(op_type, row);
  }
  return Status::OK();
}

Status SysTabletsTable::AddAndUpdateTablets(const vector<TabletInfo*>& tablets_to_add,
                                            const vector<TabletInfo*>& tablets_to_update) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysTabletsTabletId);
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

Status SysTabletsTable::DeleteTablets(const vector<TabletInfo*>& tablets) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysTabletsTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  RowOperationsPBEncoder enc(req.mutable_row_operations());
  KuduPartialRow row(&schema_);
  BOOST_FOREACH(const TabletInfo* tablet, tablets) {
    CHECK_OK(row.SetString(kSysTabletsColTableId, tablet->table()->id()));
    CHECK_OK(row.SetString(kSysTabletsColTabletId, tablet->tablet_id()));
    enc.Add(RowOperationsPB::DELETE, row);
  }

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

// ===========================================================================
//  Sys-Bootstrap Descriptors Table
// ===========================================================================
static const char *kSysTablesTabletId = "11111111111111111111111111111111";

static const char *kSysTablesColTableId    = "table_id";
static const char *kSysTablesColMetadata   = "metadata";

Schema SysTablesTable::BuildTableSchema() {
  SchemaBuilder builder;
  CHECK_OK(builder.AddKeyColumn(kSysTablesColTableId, STRING));
  CHECK_OK(builder.AddColumn(kSysTablesColMetadata, STRING));
  return builder.Build();
}

void SysTablesTable::SetupTabletMasterBlock(TabletMasterBlockPB *master_block) {
  master_block->set_tablet_id(kSysTablesTabletId);
  master_block->set_block_a("22222222222222222222222222222222");
  master_block->set_block_b("33333333333333333333333333333333");
}

// TODO: move out as a generic FullTableScan()?
Status SysTablesTable::VisitTables(Visitor *visitor) {
  gscoped_ptr<RowwiseIterator> iter;
  RETURN_NOT_OK(tablet_peer_->tablet()->NewRowIterator(schema_, &iter));
  RETURN_NOT_OK(iter->Init(NULL));

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

Status SysTablesTable::VisitTableFromRow(const RowBlockRow& row, Visitor *visitor) {
  const Slice *table_id =
    schema_.ExtractColumnFromRow<STRING>(row, schema_.find_column(kSysTablesColTableId));
  const Slice *data =
    schema_.ExtractColumnFromRow<STRING>(row, schema_.find_column(kSysTablesColMetadata));

  SysTablesEntryPB metadata;
  RETURN_NOT_OK_PREPEND(pb_util::ParseFromArray(&metadata, data->data(), data->size()),
                        "Unable to parse metadata field for table " + table_id->ToString());

  RETURN_NOT_OK(visitor->VisitTable(table_id->ToString(), metadata));
  return Status::OK();
}

Status SysTablesTable::AddTable(const TableInfo *table) {
  faststring metadata_buf;
  if (!pb_util::SerializeToString(table->metadata().dirty().pb, &metadata_buf)) {
    return Status::Corruption("Unable to serialize SysTablesEntryPB for tablet",
                              table->metadata().dirty().name());
  }

  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysTablesTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetString(kSysTablesColTableId, table->id()));
  CHECK_OK(row.SetString(kSysTablesColMetadata, metadata_buf));
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::INSERT, row);

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

Status SysTablesTable::UpdateTable(const TableInfo *table) {
  faststring metadata_buf;
  if (!pb_util::SerializeToString(table->metadata().dirty().pb, &metadata_buf)) {
    return Status::Corruption("Unable to serialize SysTablesEntryPB for tablet",
                              table->id());
  }

  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysTablesTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetString(kSysTablesColTableId, table->id()));
  CHECK_OK(row.SetString(kSysTablesColMetadata, metadata_buf));
  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::UPDATE, row);

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

Status SysTablesTable::DeleteTable(const TableInfo *table) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysTablesTabletId);
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  KuduPartialRow row(&schema_);
  CHECK_OK(row.SetString(kSysTablesColTableId, table->id()));

  RowOperationsPBEncoder enc(req.mutable_row_operations());
  enc.Add(RowOperationsPB::DELETE, row);

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

} // namespace master
} // namespace kudu
