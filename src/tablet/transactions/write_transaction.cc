// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/write_transaction.h"

#include <vector>

#include "common/wire_protocol.h"
#include "gutil/stl_util.h"
#include "rpc/rpc_context.h"
#include "tablet/tablet.h"
#include "tablet/tablet_peer.h"
#include "tablet/tablet_metrics.h"
#include "tablet/transactions/write_util.h"
#include "tserver/tserver.pb.h"
#include "util/trace.h"

namespace kudu {
namespace tablet {

using consensus::ReplicateMsg;
using consensus::CommitMsg;
using consensus::OP_ABORT;
using consensus::WRITE_OP;
using boost::shared_lock;
using tserver::TabletServerErrorPB;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;
using boost::bind;

LeaderWriteTransaction::LeaderWriteTransaction(WriteTransactionContext* tx_ctx,
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

void LeaderWriteTransaction::NewReplicateMsg(gscoped_ptr<ReplicateMsg>* replicate_msg) {
  replicate_msg->reset(new ReplicateMsg);
  (*replicate_msg)->set_op_type(WRITE_OP);
  (*replicate_msg)->mutable_write_request()->CopyFrom(*tx_ctx()->request());
}

Status LeaderWriteTransaction::Prepare() {
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

  gscoped_ptr<Schema> inserts_client_schema(new Schema);
  if (mutable_request->has_to_insert_rows()) {
    s = DecodeRowBlockAndSetupClientErrors(
        mutable_request->mutable_to_insert_rows(),
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
    s = DecodeRowBlockAndSetupClientErrors(
        mutable_request->mutable_to_mutate_row_keys(),
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
    SetupClientError(tx_ctx_->response()->mutable_error(), s,
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
    RETURN_NOT_OK(tablet->CreatePreparedInsert(tx_ctx(), row, &row_write));
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
    RETURN_NOT_OK(tablet->CreatePreparedMutate(tx_ctx(), row_key,
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

void LeaderWriteTransaction::PrepareFailedPreCommitHooks(gscoped_ptr<CommitMsg>* commit_msg) {
  // Release all row locks (no effect if no locks were acquired).
  tx_ctx_->release_row_locks();

  // if there is no error in the write response, set it.
  if (!tx_ctx_->response()->has_error()) {
    TabletServerErrorPB* error = tx_ctx_->response()->mutable_error();
    StatusToPB(prepare_status_, error->mutable_status());
    error->set_code(TabletServerErrorPB::UNKNOWN_ERROR);
  }

  commit_msg->reset(new CommitMsg());
  (*commit_msg)->set_op_type(OP_ABORT);
  (*commit_msg)->mutable_write_response()->CopyFrom(*tx_ctx_->response());
  (*commit_msg)->mutable_result()->CopyFrom(tx_ctx_->Result());
}

Status LeaderWriteTransaction::Apply() {
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
        s = tablet->InsertUnlocked(tx_ctx(), row);
        break;
      }
      case TxOperationPB::MUTATE: {
        s = tablet->MutateRowUnlocked(tx_ctx(), row);
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

void LeaderWriteTransaction::ApplySucceeded() {
  // Now that all of the changes have been applied and the commit is durable
  // make the changes visible to readers.
  if (tx_ctx()->rpc_context()) {
    tx_ctx()->rpc_context()->trace()->Message("WriteCommitCallback: making edits visible");
  }
  tx_ctx()->commit();
  LeaderTransaction::ApplySucceeded();
}

void LeaderWriteTransaction::UpdateMetrics() {
  // Update tablet server metrics.
  TabletMetrics* metrics = tx_ctx_->tablet_peer()->tablet()->metrics();
  if (metrics) {
    // TODO: should we change this so it's actually incremented by the
    // Tablet code itself instead of this wrapper code?
    metrics->rows_inserted->IncrementBy(tx_ctx_->metrics().successful_inserts);
    metrics->rows_updated->IncrementBy(tx_ctx_->metrics().successful_updates);
  }
}

Status WriteTransactionContext::AddInsert(const txid_t &tx_id, int64_t mrs_id) {
  if (PREDICT_FALSE(mvcc_tx_.get() != NULL)) {
    DCHECK_EQ(mvcc_tx_->txid(), tx_id) << "tx_id doesn't match the id of the ongoing transaction";
  }
  TxOperationPB* insert = result_pb_.add_inserts();
  insert->set_type(TxOperationPB::INSERT);
  insert->set_mrs_id(mrs_id);
  tx_metrics_.successful_inserts++;
  return Status::OK();
}

void WriteTransactionContext::AddFailedInsert(const Status &status) {
  TxOperationPB* insert = result_pb_.add_inserts();
  insert->set_type(TxOperationPB::INSERT);
  StatusToPB(status, insert->mutable_failed_status());
  failed_operations_++;
}

Status WriteTransactionContext::AddMutation(const txid_t &tx_id, gscoped_ptr<MutationResultPB> result) {
  if (PREDICT_FALSE(mvcc_tx_.get() != NULL)) {
    DCHECK_EQ(mvcc_tx_->txid(), tx_id) << "tx_id doesn't match the id of the ongoing transaction";
  }
  result->set_type(MutationType(result.get()));
  TxOperationPB* mutation = result_pb_.add_mutations();
  mutation->set_type(TxOperationPB::MUTATE);
  mutation->mutable_mutation_result()->Swap(result.get());
  tx_metrics_.successful_updates++;
  return Status::OK();
}

Status WriteTransactionContext::AddMissedMutation(
    const txid_t &tx_id,
    gscoped_ptr<RowwiseRowBlockPB> row_key,
    const RowChangeList& changelist,
    gscoped_ptr<MutationResultPB> result) {

  result->set_type(MutationType(result.get()));
  TxOperationPB* mutation = result_pb_.add_mutations();
  mutation->set_type(TxOperationPB::MUTATE);
  mutation->set_allocated_mutation_result(result.release());

  MissedDeltaMutationPB* missed_delta_mutation = mutation->mutable_missed_delta_mutation();
  missed_delta_mutation->set_allocated_row_key(row_key.release());
  missed_delta_mutation->set_changelist(changelist.slice().data(),
                                        changelist.slice().size());
  return Status::OK();
}

void WriteTransactionContext::AddFailedMutation(const Status &status) {
  TxOperationPB* mutation = result_pb_.add_mutations();
  mutation->set_type(TxOperationPB::MUTATE);
  StatusToPB(status, mutation->mutable_failed_status());
  failed_operations_++;
}

txid_t WriteTransactionContext::start_mvcc_tx() {
  DCHECK(mvcc_tx_.get() == NULL) << "Mvcc transaction already started/set.";
  mvcc_tx_.reset(new ScopedTransaction(tablet_peer_->tablet()->mvcc_manager()));
  result_pb_.set_txid(mvcc_tx_->txid().v);
  return mvcc_tx_->txid();
}

void WriteTransactionContext::set_current_mvcc_tx(gscoped_ptr<ScopedTransaction> mvcc_tx) {
  DCHECK(mvcc_tx_.get() == NULL) << "Mvcc transaction already started/set.";
  mvcc_tx_.reset(mvcc_tx.release());
  result_pb_.set_txid(mvcc_tx_->txid().v);
}

void WriteTransactionContext::commit() {
  if (mvcc_tx_.get() != NULL) {
    // commit the transaction
    mvcc_tx_->Commit();
  }
  mvcc_tx_.reset();
  component_lock_.reset();
  release_row_locks();
}

void WriteTransactionContext::release_row_locks() {
  // free the row locks
  STLDeleteElements(&rows_);
}

txid_t WriteTransactionContext::mvcc_txid() {
  if (mvcc_tx_.get() == NULL) {
    return txid_t::kInvalidTxId;
  }
  return mvcc_tx_->txid();
}

void WriteTransactionContext::Reset() {
  commit();
  result_pb_.Clear();
  tx_metrics_.Reset();
  failed_operations_ = 0;
}

PreparedRowWrite::PreparedRowWrite(const ConstContiguousRow* row,
                                   gscoped_ptr<RowSetKeyProbe> probe,
                                   gscoped_ptr<ScopedRowLock> lock)
    : schema_(&row->schema()),
      row_(row),
      row_key_(NULL),
      changelist_(NULL),
      probe_(probe.Pass()),
      row_lock_(lock.Pass()),
      op_type_(TxOperationPB::INSERT) {
}

PreparedRowWrite::PreparedRowWrite(const ConstContiguousRow* row_key,
                                   const Schema* changelist_schema,
                                   const RowChangeList* changelist,
                                   gscoped_ptr<RowSetKeyProbe> probe,
                                   gscoped_ptr<tablet::ScopedRowLock> lock)
    : schema_(changelist_schema),
      row_(NULL),
      row_key_(row_key),
      changelist_(changelist),
      probe_(probe.Pass()),
      row_lock_(lock.Pass()),
      op_type_(TxOperationPB::MUTATE) {
}

}  // namespace tablet
}  // namespace kudu


