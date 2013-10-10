// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transaction_context.h"

#include "gutil/stl_util.h"
#include "tablet/tablet_peer.h"

namespace kudu {
namespace tablet {

using tablet::ScopedRowLock;

MutationResultPB::MutationTypePB MutationType(const MutationResultPB* result) {
  if (result->mutations_size() == 0) {
    return MutationResultPB::NO_MUTATION;
  }
  if (result->mutations_size() == 1) {
    return result->mutations(0).has_mrs_id() ?
        MutationResultPB::MRS_MUTATION :
        MutationResultPB::DELTA_MUTATION;
  }
  DCHECK_EQ(result->mutations_size(), 2);
  return MutationResultPB::DUPLICATED_MUTATION;
}

Status TransactionContext::AddInsert(const txid_t &tx_id,
                                     int64_t mrs_id) {
  if (PREDICT_FALSE(mvcc_tx_.get() != NULL)) {
    DCHECK_EQ(mvcc_tx_->txid(), tx_id) << "tx_id doesn't match the id of the ongoing transaction";
  }
  TxOperationPB* insert = result_pb_.add_inserts();
  insert->set_type(TxOperationPB::INSERT);
  insert->set_mrs_id(mrs_id);
  return Status::OK();
}

void TransactionContext::AddFailedInsert(const Status &status) {
  TxOperationPB* insert = result_pb_.add_inserts();
  insert->set_type(TxOperationPB::INSERT);
  StatusToPB(status, insert->mutable_failed_status());
  unsuccessful_ops_++;
}

Status TransactionContext::AddMutation(const txid_t &tx_id,
                                       gscoped_ptr<MutationResultPB> result) {
  if (PREDICT_FALSE(mvcc_tx_.get() != NULL)) {
    DCHECK_EQ(mvcc_tx_->txid(), tx_id) << "tx_id doesn't match the id of the ongoing transaction";
  }
  result->set_type(MutationType(result.get()));
  TxOperationPB* mutation = result_pb_.add_mutations();
  mutation->set_type(TxOperationPB::MUTATE);
  mutation->mutable_mutation_result()->Swap(result.get());
  return Status::OK();
}

Status TransactionContext::AddMissedMutation(
    const txid_t &tx_id,
    const rowid_t& row_idx,
    const RowChangeList& changelist,
    gscoped_ptr<MutationResultPB> result) {

  result->set_type(MutationType(result.get()));
  TxOperationPB* mutation = result_pb_.add_mutations();
  mutation->set_type(TxOperationPB::MUTATE);
  mutation->set_allocated_mutation_result(result.release());

  MissedDeltaMutationPB* missed_delta_mutation = mutation->mutable_missed_delta_mutation();
  missed_delta_mutation->set_row_idx(row_idx);
  missed_delta_mutation->set_changelist(changelist.slice().data(),
                                        changelist.slice().size());
  return Status::OK();
}

void TransactionContext::AddFailedMutation(const Status &status) {
  TxOperationPB* mutation = result_pb_.add_mutations();
  mutation->set_type(TxOperationPB::MUTATE);
  StatusToPB(status, mutation->mutable_failed_status());
  unsuccessful_ops_++;
}

txid_t TransactionContext::start_mvcc_tx() {
  DCHECK(mvcc_tx_.get() == NULL) << "Mvcc transaction already started/set.";
  mvcc_tx_.reset(new ScopedTransaction(tablet_peer_->tablet()->mvcc_manager()));
  result_pb_.set_txid(mvcc_tx_->txid().v);
  return mvcc_tx_->txid();
}

void TransactionContext::set_current_mvcc_tx(gscoped_ptr<ScopedTransaction> mvcc_tx) {
  DCHECK(mvcc_tx_.get() == NULL) << "Mvcc transaction already started/set.";
  mvcc_tx_.reset(mvcc_tx.release());
  result_pb_.set_txid(mvcc_tx_->txid().v);
}

void TransactionContext::commit() {
  if (mvcc_tx_.get() != NULL) {
    // commit the transaction
    mvcc_tx_->Commit();
  }
  mvcc_tx_.reset();
  component_lock_.reset();
  release_row_locks();
}

void TransactionContext::release_row_locks() {
  // free the row locks
  STLDeleteElements(&rows_);
}

txid_t TransactionContext::mvcc_txid() {
  if (mvcc_tx_.get() == NULL) {
    return txid_t::kInvalidTxId;
  }
  return mvcc_tx_->txid();
}

void TransactionContext::Reset() {
  commit();
  result_pb_.Clear();
  unsuccessful_ops_ = 0;
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

