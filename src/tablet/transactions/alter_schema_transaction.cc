// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/alter_schema_transaction.h"
#include "tablet/transactions/write_util.h"

#include "common/wire_protocol.h"
#include "rpc/rpc_context.h"
#include "tablet/tablet.h"
#include "tablet/tablet_peer.h"
#include "tablet/tablet_metrics.h"
#include "tserver/tserver.pb.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::OP_ABORT;
using consensus::ALTER_SCHEMA_OP;
using boost::shared_lock;
using tserver::TabletServerErrorPB;
using tserver::AlterSchemaRequestPB;
using tserver::AlterSchemaResponsePB;
using boost::bind;

LeaderAlterSchemaTransaction::LeaderAlterSchemaTransaction(AlterSchemaTransactionContext* tx_ctx,
                                               consensus::Consensus* consensus,
                                               TaskExecutor* prepare_executor,
                                               TaskExecutor* apply_executor,
                                               simple_spinlock& prepare_replicate_lock)
: LeaderTransaction(consensus,
                    prepare_executor,
                    apply_executor,
                    prepare_replicate_lock),
  tx_ctx_(tx_ctx) {
}

void LeaderAlterSchemaTransaction::NewReplicateMsg(gscoped_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(ALTER_SCHEMA_OP);
  (*replicate_msg)->mutable_alter_schema_request()->CopyFrom(*tx_ctx()->request());
}

Status LeaderAlterSchemaTransaction::Prepare() {
  TRACE("PREPARE ALTER-SCHEMA: Starting");

  // Decode schema
  gscoped_ptr<Schema> schema(new Schema);
  Status s = SchemaFromPB(tx_ctx_->request()->schema(), schema.get());
  if (!s.ok()) {
    tx_ctx_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }

  Tablet* tablet = tx_ctx_->tablet_peer()->tablet();
  RETURN_NOT_OK(tablet->CreatePreparedAlterSchema(tx_ctx(), schema.get()));

  tx_ctx_->AddToAutoReleasePool(schema.release());

  TRACE("PREPARE ALTER-SCHEMA: finished");
  return s;
}

void LeaderAlterSchemaTransaction::PrepareFailedPreCommitHooks(gscoped_ptr<CommitMsg>* commit_msg) {
  // Release the tablet lock (no effect if no locks were acquired).
  tx_ctx_->release_tablet_lock();

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(OP_ABORT);
  (*commit_msg)->mutable_alter_schema_response()->CopyFrom(*tx_ctx_->response());
}

Status LeaderAlterSchemaTransaction::Apply() {
  TRACE("APPLY ALTER-SCHEMA: Starting");

  Tablet* tablet = tx_ctx_->tablet_peer()->tablet();
  RETURN_NOT_OK(tablet->AlterSchema(tx_ctx()));

  gscoped_ptr<CommitMsg> commit(new CommitMsg());
  commit->set_op_type(ALTER_SCHEMA_OP);

  TRACE("APPLY ALTER-SCHEMA: finished, triggering COMMIT");

  RETURN_NOT_OK(tx_ctx_->consensus_ctx()->Commit(commit.Pass()));
  // NB: do not use tx_ctx_ after this point, because the commit may have
  // succeeded, in which case the context may have been torn down.
  return Status::OK();
}

void LeaderAlterSchemaTransaction::ApplySucceeded() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("AlterSchemaCommitCallback: making edits visible");
  tx_ctx()->commit();
  LeaderTransaction::ApplySucceeded();
}

}  // namespace tablet
}  // namespace kudu
