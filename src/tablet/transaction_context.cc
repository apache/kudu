// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transaction_context.h"
#include "gutil/stl_util.h"

namespace kudu {
namespace tablet {

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

Status TransactionContext::SetOrCheckTxId(const txid_t& tx_id) {
  if (tx_id_ == txid_t::kInvalidTxId) {
    tx_id_ = tx_id;
  } else if (PREDICT_FALSE(!(tx_id_ == tx_id))) {
    return Status::IllegalState("Operation is in a different mvcc transaction.");
  }
  return Status::OK();
}

Status TransactionContext::AddInsert(const txid_t &tx_id,
                                     int64_t mrs_id) {
  RETURN_NOT_OK(SetOrCheckTxId(tx_id));
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
  RETURN_NOT_OK(SetOrCheckTxId(tx_id));
  result->set_type(MutationType(result.get()));
  TxOperationPB* mutation = result_pb_.add_mutations();
  mutation->set_type(TxOperationPB::MUTATE);
  mutation->mutable_mutation_result()->Swap(result.get());
  return Status::OK();
}

void TransactionContext::AddFailedMutation(const Status &status) {
  TxOperationPB* mutation = result_pb_.add_mutations();
  mutation->set_type(TxOperationPB::MUTATE);
  StatusToPB(status, mutation->mutable_failed_status());
  unsuccessful_ops_++;
}

void TransactionContext::Reset() {
  result_pb_.Clear();
  tx_id_ = txid_t::kInvalidTxId;
  unsuccessful_ops_ = 0;
}

}  // namespace tablet
}  // namespace kudu

