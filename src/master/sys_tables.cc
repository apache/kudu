// Copyright (c) 2013, Cloudera, inc.

#include "master/sys_tables.h"

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/schema.h"
#include "common/partial_row.h"
#include "common/row_operations.h"
#include "common/wire_protocol.h"
#include "consensus/opid_anchor_registry.h"
#include "gutil/strings/substitute.h"
#include "master/catalog_manager.h"
#include "master/master.h"
#include "master/master.pb.h"
#include "rpc/rpc_context.h"
#include "server/fsmanager.h"
#include "tablet/tablet_bootstrap.h"
#include "tablet/transactions/write_transaction.h"
#include "tablet/tablet.h"
#include "tserver/tserver.pb.h"
#include "util/pb_util.h"

using kudu::log::Log;
using kudu::log::OpIdAnchorRegistry;
using kudu::metadata::QuorumPB;
using kudu::metadata::QuorumPeerPB;
using kudu::tablet::LatchTransactionCompletionCallback;
using kudu::tablet::Tablet;
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
}

void SysTable::Shutdown() {
  if (tablet_peer_) {
    tablet_peer_->Shutdown();
  }
}

Status SysTable::Load(FsManager *fs_manager) {
  metadata::TabletMasterBlockPB master_block;
  SetupTabletMasterBlock(&master_block);

  // Load Metadata Information from disk
  gscoped_ptr<metadata::TabletMetadata> metadata;
  RETURN_NOT_OK(metadata::TabletMetadata::Load(fs_manager, master_block, &metadata));

  // Verify that the schema is the current one
  if (!metadata->schema().Equals(BuildTableSchema())) {
    // TODO: In this case we probably should execute the migration step.
    return(Status::Corruption("Unexpected schema", metadata->schema().ToString()));
  }

  RETURN_NOT_OK(SetupTablet(metadata.Pass()));
  return Status::OK();
}

Status SysTable::CreateNew(FsManager *fs_manager) {
  metadata::TabletMasterBlockPB master_block;
  SetupTabletMasterBlock(&master_block);

  QuorumPeerPB quorum_peer;
  quorum_peer.set_permanent_uuid(master_->instance_pb().permanent_uuid());

  // TODO For dist consensus get the quorum with other peers.
  QuorumPB quorum;
  quorum.set_local(true);
  quorum.set_seqno(0);
  quorum.add_peers()->CopyFrom(quorum_peer);

  // Create the new Metadata
  gscoped_ptr<metadata::TabletMetadata> metadata;
  RETURN_NOT_OK(metadata::TabletMetadata::CreateNew(fs_manager,
                                                    master_block,
                                                    table_name(),
                                                    BuildTableSchema(),
                                                    quorum,
                                                    "", "", &metadata));
  return SetupTablet(metadata.Pass());
}

Status SysTable::SetupTablet(gscoped_ptr<metadata::TabletMetadata> metadata) {
  shared_ptr<Tablet> tablet;
  gscoped_ptr<Log> log;
  scoped_refptr<OpIdAnchorRegistry> opid_anchor_registry;

  // TODO: handle crash mid-creation of tablet? do we ever end up with a
  // partially created tablet here?
  tablet_peer_.reset(new TabletPeer(*metadata));
  RETURN_NOT_OK(BootstrapTablet(metadata.Pass(),
                                scoped_refptr<server::Clock>(master_->clock()),
                                &metric_ctx_,
                                tablet_peer_->status_listener(),
                                &tablet,
                                &log,
                                &opid_anchor_registry));

  // TODO: Do we have a setSplittable(false) or something from the outside is
  // handling split in the TS?

  RETURN_NOT_OK_PREPEND(tablet_peer_->Init(tablet,
                                           scoped_refptr<server::Clock>(master_->clock()),
                                           master_->messenger(),
                                           tablet->metadata()->Quorum().peers(0),
                                           log.Pass(),
                                           opid_anchor_registry.get(),
                                           tablet->metadata()->Quorum().local()),
                        "Failed to Init() TabletPeer");

  RETURN_NOT_OK_PREPEND(tablet_peer_->Start(tablet->metadata()->Quorum()),
                                            "Failed to Start() TabletPeer");

  schema_ = SchemaBuilder(tablet->schema()).BuildWithoutIds();
  key_schema_ = schema_.CreateKeyProjection();
  return Status::OK();
}

Status SysTable::SyncWrite(const WriteRequestPB *req, WriteResponsePB *resp) {
  CountDownLatch latch(1);
  gscoped_ptr<tablet::TransactionCompletionCallback> txn_callback(
    new LatchTransactionCompletionCallback<WriteResponsePB>(&latch, resp));
  tablet::WriteTransactionContext *tx_ctx =
    new tablet::WriteTransactionContext(tablet_peer_.get(), req, resp);
  tx_ctx->set_completion_callback(txn_callback.Pass());

  RETURN_NOT_OK(tablet_peer_->SubmitWrite(tx_ctx));
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

void SysTabletsTable::SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block) {
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
    RETURN_NOT_OK(RowwiseIterator::CopyBlock(iter.get(), &block));
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

Status SysTabletsTable::AddAndUpdateTablets(const vector<TabletInfo*>& tablets_to_add,
                                            const vector<TabletInfo*>& tablets_to_update) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysTabletsTabletId);

  faststring metadata_buf;

  // Insert new Tablets
  if (!tablets_to_add.empty()) {
    RowOperationsPB* data = req.mutable_to_insert_rows();
    RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

    PartialRow row(&schema_);
    BOOST_FOREACH(const TabletInfo *tablet, tablets_to_add) {
      if (!pb_util::SerializeToString(tablet->metadata().dirty().pb, &metadata_buf)) {
        return Status::Corruption("Unable to serialize SysTabletsEntryPB for tablet",
                                  tablet->tablet_id());
      }

      row.SetString(kSysTabletsColTableId, tablet->table()->id());
      row.SetString(kSysTabletsColTabletId, tablet->tablet_id());
      row.SetString(kSysTabletsColMetadata, metadata_buf);
      RowOperationsPBEncoder enc(data);
      enc.Add(RowOperationsPB::INSERT, row);
    }
  }

  // Update already existing Tablets
  if (!tablets_to_update.empty()) {
    RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
    RETURN_NOT_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
    data->set_num_key_columns(schema_.num_key_columns());

    faststring buf;
    faststring mutations;
    RowBuilder rb(key_schema_);
    RowChangeListEncoder encoder(schema_, &buf);
    BOOST_FOREACH(const TabletInfo* tablet, tablets_to_update) {
      if (!pb_util::SerializeToString(tablet->metadata().dirty().pb, &metadata_buf)) {
        return Status::Corruption("Unable to serialize SysTabletsEntryPB for tablet",
                                  tablet->tablet_id());
      }

      // Write the key
      rb.Reset();
      rb.AddString(tablet->table()->id());
      rb.AddString(tablet->tablet_id());
      AddRowToRowBlockPB(rb.row(), data);

      // Write the mutation
      encoder.Reset();
      Slice metadata(metadata_buf);
      encoder.AddColumnUpdate(schema_.find_column(kSysTabletsColMetadata), &metadata);
      PutFixed32LengthPrefixedSlice(&mutations, Slice(buf));
    }
    req.set_encoded_mutations(mutations.data(), mutations.size());
  }

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

Status SysTabletsTable::DeleteTablets(const vector<TabletInfo*>& tablets) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysTabletsTabletId);

  RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
  RETURN_NOT_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
  data->set_num_key_columns(schema_.num_key_columns());

  faststring buf;
  faststring mutations;
  RowBuilder rb(key_schema_);
  RowChangeListEncoder encoder(schema_, &buf);
  BOOST_FOREACH(const TabletInfo* tablet, tablets) {
    // Write the key
    rb.Reset();
    rb.AddString(tablet->table()->id());
    rb.AddString(tablet->tablet_id());
    AddRowToRowBlockPB(rb.row(), data);

    // Write the mutation
    encoder.Reset();
    encoder.SetToDelete();

    PutFixed32LengthPrefixedSlice(&mutations, Slice(buf));
  }
  req.set_encoded_mutations(mutations.data(), mutations.size());

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

void SysTablesTable::SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block) {
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
    RETURN_NOT_OK(RowwiseIterator::CopyBlock(iter.get(), &block));
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

  RowOperationsPB* data = req.mutable_to_insert_rows();
  RETURN_NOT_OK(SchemaToPB(schema_, req.mutable_schema()));

  PartialRow row(&schema_);
  row.SetString(kSysTablesColTableId, table->id());
  row.SetString(kSysTablesColMetadata, metadata_buf);
  RowOperationsPBEncoder enc(data);
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

  RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
  RETURN_NOT_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
  data->set_num_key_columns(schema_.num_key_columns());

  // Write the key
  RowBuilder rb(key_schema_);
  rb.AddString(table->id());
  AddRowToRowBlockPB(rb.row(), data);

  // Write the mutation
  faststring buf;
  Slice metadata(metadata_buf);
  RowChangeListEncoder encoder(schema_, &buf);
  encoder.AddColumnUpdate(schema_.find_column(kSysTablesColMetadata), &metadata);

  faststring mutation;
  PutFixed32LengthPrefixedSlice(&mutation, Slice(buf));
  req.set_encoded_mutations(mutation.data(), mutation.size());

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

Status SysTablesTable::DeleteTable(const TableInfo *table) {
  WriteRequestPB req;
  WriteResponsePB resp;
  req.set_tablet_id(kSysTablesTabletId);

  RowwiseRowBlockPB* data = req.mutable_to_mutate_row_keys();
  RETURN_NOT_OK(SchemaToColumnPBs(schema_, data->mutable_schema()));
  data->set_num_key_columns(schema_.num_key_columns());

  // Write the key
  RowBuilder rb(key_schema_);
  rb.AddString(table->id());
  AddRowToRowBlockPB(rb.row(), data);

  // Write the mutation
  faststring buf;
  RowChangeListEncoder encoder(schema_, &buf);
  encoder.SetToDelete();

  faststring mutation;
  PutFixed32LengthPrefixedSlice(&mutation, Slice(buf));
  req.set_encoded_mutations(mutation.data(), mutation.size());

  RETURN_NOT_OK(SyncWrite(&req, &resp));
  return Status::OK();
}

} // namespace master
} // namespace kudu
