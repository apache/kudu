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

AlterSchemaTransaction::AlterSchemaTransaction(AlterSchemaTransactionState* state,
                                               DriverType type)
    : Transaction(state, type),
      tx_state_(state) {
}

void AlterSchemaTransaction::NewReplicateMsg(gscoped_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(ALTER_SCHEMA_OP);
  (*replicate_msg)->mutable_alter_schema_request()->CopyFrom(*state()->request());
}

Status AlterSchemaTransaction::Prepare() {
  TRACE("PREPARE ALTER-SCHEMA: Starting");

  // Decode schema
  gscoped_ptr<Schema> schema(new Schema);
  Status s = SchemaFromPB(tx_state_->request()->schema(), schema.get());
  if (!s.ok()) {
    tx_state_->completion_callback()->set_error(s, TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }

  Tablet* tablet = tx_state_->tablet_peer()->tablet();
  RETURN_NOT_OK(tablet->CreatePreparedAlterSchema(state(), schema.get()));

  // now that we've acquired the locks set the transaction timestamp
  tx_state_->set_timestamp(tx_state_->tablet_peer()->clock()->Now());

  tx_state_->AddToAutoReleasePool(schema.release());

  TRACE("PREPARE ALTER-SCHEMA: finished");
  return s;
}

void AlterSchemaTransaction::NewCommitAbortMessage(gscoped_ptr<CommitMsg>* commit_msg) {
  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(OP_ABORT);
  (*commit_msg)->mutable_alter_schema_response()->CopyFrom(*tx_state_->response());
  (*commit_msg)->set_timestamp(tx_state_->timestamp().ToUint64());
}

Status AlterSchemaTransaction::Apply(gscoped_ptr<CommitMsg>* commit_msg) {
  TRACE("APPLY ALTER-SCHEMA: Starting");

  Tablet* tablet = tx_state_->tablet_peer()->tablet();
  RETURN_NOT_OK(tablet->AlterSchema(state()));

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(ALTER_SCHEMA_OP);
  (*commit_msg)->set_timestamp(tx_state_->timestamp().ToUint64());
  return Status::OK();
}

void AlterSchemaTransaction::Finish() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  TRACE("AlterSchemaCommitCallback: making edits visible");
  state()->commit();
}

}  // namespace tablet
}  // namespace kudu
