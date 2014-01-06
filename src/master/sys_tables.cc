// Copyright (c) 2013, Cloudera, inc.

#include "master/sys_tables.h"

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/schema.h"
#include "common/wire_protocol.h"
#include "master/master.h"
#include "rpc/rpc_context.h"
#include "server/fsmanager.h"
#include "tablet/tablet_bootstrap.h"
#include "tablet/transactions/write_transaction.h"
#include "tablet/tablet.h"
#include "tserver/tserver.pb.h"
#include "util/pb_util.h"

using kudu::log::Log;
using kudu::metadata::QuorumPB;
using kudu::metadata::QuorumPeerPB;
using kudu::tablet::LatchTransactionCompletionCallback;
using kudu::tablet::Tablet;
using kudu::tablet::TabletPeer;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;

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

  return SetupTablet(metadata.Pass());
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
                                                    BuildTableSchema(),
                                                    quorum,
                                                    "", "", &metadata));
  return SetupTablet(metadata.Pass());
}

Status SysTable::SetupTablet(gscoped_ptr<metadata::TabletMetadata> metadata) {
  shared_ptr<Tablet> tablet;
  gscoped_ptr<Log> log;

  // TODO: handle crash mid-creation of tablet? do we ever end up with a
  // partially created tablet here?
  RETURN_NOT_OK(BootstrapTablet(metadata.Pass(), &metric_ctx_, &tablet, &log));

  // TODO: Do we have a setSplittable(false) or something from the outside is
  // handling split in the TS?
  tablet_peer_.reset(new TabletPeer(tablet, tablet->metadata()->Quorum().peers(0), log.Pass()));
  RETURN_NOT_OK_PREPEND(tablet_peer_->Init(), "Failed to Init() TabletPeer");

  RETURN_NOT_OK_PREPEND(tablet_peer_->Start(tablet->metadata()->Quorum()),
                                            "Failed to Start() TabletPeer");

  schema_ = SchemaBuilder(tablet->schema()).BuildWithoutIds();
  return Status::OK();
}

Status SysTable::SyncWrite(const WriteRequestPB *req, WriteResponsePB *resp) {
  CountDownLatch latch(1);
  gscoped_ptr<tablet::TransactionCompletionCallback> txn_callback(
    new LatchTransactionCompletionCallback(&latch));
  tablet::WriteTransactionContext *tx_ctx =
    new tablet::WriteTransactionContext(tablet_peer_.get(), req, resp);
  tx_ctx->set_completion_callback(txn_callback.Pass());
  RETURN_NOT_OK(tablet_peer_->SubmitWrite(tx_ctx));
  latch.Wait();
  return Status::OK();
}

// ===========================================================================
//  Sys-Bootstrap Locations Table
// ===========================================================================
Schema SysTabletsTable::BuildTableSchema() {
  SchemaBuilder builder;
  CHECK_OK(builder.AddKeyColumn("table_id", STRING));
  CHECK_OK(builder.AddKeyColumn("start_key", STRING));
  CHECK_OK(builder.AddKeyColumn("end_key", STRING));
  CHECK_OK(builder.AddColumn("metadata", STRING));
  return builder.Build();
}

void SysTabletsTable::SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block) {
  master_block->set_tablet_id("00000000000000000000000000000000");
  master_block->set_block_a("00000000000000000000000000000000");
  master_block->set_block_b("11111111111111111111111111111111");
}

// ===========================================================================
//  Sys-Bootstrap Descriptors Table
// ===========================================================================
static const char *kSysDescriptorsTabletId = "11111111111111111111111111111111";

Schema SysTablesTable::BuildTableSchema() {
  SchemaBuilder builder;
  CHECK_OK(builder.AddKeyColumn("table_name", STRING));
  CHECK_OK(builder.AddColumn("table_id", STRING));
  CHECK_OK(builder.AddColumn("version", UINT32));
  CHECK_OK(builder.AddColumn("metadata", STRING));
  return builder.Build();
}

void SysTablesTable::SetupTabletMasterBlock(metadata::TabletMasterBlockPB *master_block) {
  master_block->set_tablet_id(kSysDescriptorsTabletId);
  master_block->set_block_a("22222222222222222222222222222222");
  master_block->set_block_b("33333333333333333333333333333333");
}

} // namespace master
} // namespace kudu
