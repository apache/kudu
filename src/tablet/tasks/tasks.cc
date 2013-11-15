// Copyright (c) 2013, Cloudera, inc.

#include "tablet/tasks/tasks.h"

#include <vector>

#include "common/wire_protocol.h"
#include "tablet/tablet_peer.h"
#include "tablet/transaction_context.h"
#include "tserver/tserver.pb.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using consensus::CommitMsg;
using consensus::WRITE_OP;
using boost::shared_lock;
using tserver::TabletServerErrorPB;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;

static void SetupError(TabletServerErrorPB* error,
                       const Status& s,
                       TabletServerErrorPB::Code code) {
  StatusToPB(s, error->mutable_status());
  error->set_code(code);
}

// ============================================================================
//  Write related methods
// ============================================================================
static Status DecodeRowBlock(RowwiseRowBlockPB* block_pb,
                             WriteResponsePB* resp,
                             rpc::RpcContext* context,
                             const Schema& tablet_key_projection,
                             bool is_inserts_block,
                             Schema* client_schema,
                             vector<const uint8_t*>* row_block) {
  // Extract the schema of the row block
  Status s = ColumnPBsToSchema(block_pb->schema(), client_schema);
  if (!s.ok()) {
    SetupError(resp->mutable_error(), s,
               TabletServerErrorPB::INVALID_SCHEMA);
    return s;
  }

  // Check that the schema sent by the user matches the key projection of the tablet.
  Schema client_key_projection = client_schema->CreateKeyProjection();
  if (!client_key_projection.Equals(tablet_key_projection)) {
    s = Status::InvalidArgument("Mismatched key projection schema, expected",
                                tablet_key_projection.ToString());
    SetupError(resp->mutable_error(), s,
               TabletServerErrorPB::MISMATCHED_SCHEMA);
    return s;
  }

  // Extract the row block
  if (is_inserts_block) {
    s = ExtractRowsFromRowBlockPB(*client_schema, block_pb, row_block);
  } else {
    s = ExtractRowsFromRowBlockPB(client_key_projection, block_pb, row_block);
  }

  if (!s.ok()) {
    SetupError(resp->mutable_error(), s,
               TabletServerErrorPB::INVALID_ROW_BLOCK);
    return s;
  }
  return Status::OK();
}

PrepareWriteTask::PrepareWriteTask(WriteTransactionContext *tx_ctx)
  : tx_ctx_(tx_ctx) {
}

Status PrepareWriteTask::Run() {
  if (tx_ctx_->rpc_context()) {
    tx_ctx_->rpc_context()->trace()->Message("PREPARE: Starting");
  }

  // In order to avoid a copy, we mutate the row blocks for insert and
  // mutate in-place. Because the RPC framework gives us our request as a
  // const argument, we have to const_cast it away here. It's a little hacky,
  // but the alternative of making all RPCs get non-const requests doesn't
  // seem that great either, since this is a rare circumstance.
  WriteRequestPB* mutable_request =
      const_cast<WriteRequestPB*>(tx_ctx_->request());

  Tablet* tablet = tx_ctx_->tablet_peer()->tablet();

  // Decode everything first so that we give up if something major is wrong.
  vector<const uint8_t *> to_insert;
  Status s = Status::OK();

  // TODO
  gscoped_ptr<Schema> inserts_client_schema(new Schema);
  if (mutable_request->has_to_insert_rows()) {
    s = DecodeRowBlock(mutable_request->mutable_to_insert_rows(),
                       tx_ctx_->response(),
                       tx_ctx_->rpc_context(),
                       tablet->key_schema(),
                       true,
                       inserts_client_schema.get(),
                       &to_insert);
    if (PREDICT_FALSE(!s.ok())) {
      return s;
    }
  }

  gscoped_ptr<Schema> mutates_client_schema(new Schema);
  vector<const uint8_t *> to_mutate;
  if (mutable_request->has_to_mutate_row_keys()) {
    s = DecodeRowBlock(mutable_request->mutable_to_mutate_row_keys(),
                       tx_ctx_->response(),
                       tx_ctx_->rpc_context(),
                       tablet->key_schema(),
                       false,
                       mutates_client_schema.get(),
                       &to_mutate);
    if (PREDICT_FALSE(!s.ok())) {
      return s;
    }
  }

  vector<const RowChangeList *> mutations;
  s = ExtractMutationsFromBuffer(to_mutate.size(),
                                 reinterpret_cast<const uint8_t*>(
                                     mutable_request->encoded_mutations().data()),
                                     mutable_request->encoded_mutations().size(),
                                     &mutations);

  if (PREDICT_FALSE(to_mutate.size() != mutations.size())) {
    s = Status::InvalidArgument(strings::Substitute("Different number of row keys: $0 and mutations: $1",
                                                    to_mutate.size(),
                                                    mutations.size()));
  }

  if (PREDICT_FALSE(!s.ok())) {
    SetupError(tx_ctx_->response()->mutable_error(), s,
               TabletServerErrorPB::INVALID_MUTATION);
    return s;
  }

  if (tx_ctx_->rpc_context()) {
    tx_ctx_->rpc_context()->trace()->SubstituteAndTrace(
      "PREPARE: Acquiring row locks ($0 insertions, $1 mutations)",
      to_insert.size(), to_mutate.size());
  }

  // Now acquire row locks and prepare everything for apply
  BOOST_FOREACH(const uint8_t* row_ptr, to_insert) {
    // TODO pass 'row_ptr' to the PreparedRowWrite once we get rid of the
    // old API that has a Mutate method that receives the row as a reference.
    ConstContiguousRow* row = new ConstContiguousRow(*inserts_client_schema,
                                                     row_ptr);
    row = tx_ctx_->AddToAutoReleasePool(row);

    gscoped_ptr<PreparedRowWrite> row_write;
    RETURN_NOT_OK(tablet->CreatePreparedInsert(tx_ctx_, row, &row_write));
    tx_ctx_->add_prepared_row(row_write.Pass());
  }

  gscoped_ptr<Schema> mutates_key_projection_ptr(
      new Schema(mutates_client_schema->CreateKeyProjection()));

  int i = 0;
  BOOST_FOREACH(const uint8_t* row_key_ptr, to_mutate) {
    // TODO pass 'row_key_ptr' to the PreparedRowWrite once we get rid of the
    // old API that has a Mutate method that receives the row as a reference.
    ConstContiguousRow* row_key = new ConstContiguousRow(*mutates_key_projection_ptr,
                                                         row_key_ptr);
    row_key = tx_ctx_->AddToAutoReleasePool(row_key);
    const RowChangeList* mutation = tx_ctx_->AddToAutoReleasePool(mutations[i]);

    gscoped_ptr<PreparedRowWrite> row_write;
    RETURN_NOT_OK(tablet->CreatePreparedMutate(tx_ctx_, row_key,
        mutates_client_schema.get(), mutation, &row_write));
    tx_ctx_->add_prepared_row(row_write.Pass());
    ++i;
  }

  if (tx_ctx_->rpc_context()) {
    tx_ctx_->rpc_context()->trace()->Message("PREPARE: Acquiring component lock");
  }
  // acquire the component lock just before finishing prepare
  gscoped_ptr<shared_lock<rw_spinlock> > component_lock_(
      new shared_lock<rw_spinlock>(tablet->component_lock()->get_lock()));
  tx_ctx_->set_component_lock(component_lock_.Pass());

  tx_ctx_->AddToAutoReleasePool(inserts_client_schema.release());
  tx_ctx_->AddToAutoReleasePool(mutates_client_schema.release());
  tx_ctx_->AddToAutoReleasePool(mutates_key_projection_ptr.release());

  if (tx_ctx_->rpc_context()) {
    tx_ctx_->rpc_context()->trace()->Message("PREPARE: finished");
  }
  return s;
}

// No support for abort, currently
bool PrepareWriteTask::Abort() {
  return false;
}

ApplyWriteTask::ApplyWriteTask(WriteTransactionContext *tx_ctx)
  : tx_ctx_(tx_ctx) {
}

Status ApplyWriteTask::Run() {
  if (tx_ctx_->rpc_context()) {
    tx_ctx_->rpc_context()->trace()->Message("APPLY: Starting");
  }

  tx_ctx_->start_mvcc_tx();
  Tablet* tablet = tx_ctx_->tablet_peer()->tablet();

  int i = 0;
  Status s;
  BOOST_FOREACH(const PreparedRowWrite *row, tx_ctx_->rows()) {
    switch (row->write_type()) {
      case TxOperationPB::INSERT: {
        s = tablet->InsertUnlocked(tx_ctx_, row);
        break;
      }
      case TxOperationPB::MUTATE: {
        s = tablet->MutateRowUnlocked(tx_ctx_, row);
        break;
      }
    }
    if (PREDICT_FALSE(!s.ok())) {
      WriteResponsePB::PerRowErrorPB* error = tx_ctx_->response()->add_per_row_errors();
      error->set_row_index(i);
      error->set_is_insert(row->write_type() == TxOperationPB::INSERT);
      StatusToPB(s, error->mutable_error());
    }
    i++;
  }

  if (tx_ctx_->rpc_context()) {
    tx_ctx_->rpc_context()->trace()->Message("APPLY: Releasing row locks");
  }

  // Perform early lock release after we've applied all changes
  tx_ctx_->release_row_locks();

  gscoped_ptr<CommitMsg> commit(new CommitMsg());
  commit->mutable_result()->CopyFrom(tx_ctx_->Result());
  commit->set_op_type(WRITE_OP);

  if (tx_ctx_->rpc_context()) {
    tx_ctx_->rpc_context()->trace()->Message("APPLY: finished, triggering COMMIT");
  }

  tx_ctx_->consensus_ctx()->Commit(commit.Pass());
  // NB: do not use tx_ctx_ after this point, because the commit may have
  // succeeded, in which case the context may have been torn down.
  return Status::OK();
}

bool ApplyWriteTask::Abort() {
  return false;
}

} // namespace tablet
} // namespace kudu
