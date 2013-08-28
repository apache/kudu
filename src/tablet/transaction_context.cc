// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transaction_context.h"
#include "gutil/stl_util.h"

namespace kudu {
namespace tablet {

void TransactionContext::AddInsert(const string &tablet_id,
                                   const txid_t &tx_id,
                                   const ConstContiguousRow& row,
                                   int64_t mrs_id) {
  operations_.push_back(new InsertOp(tablet_id, tx_id, row, mrs_id));
}

void TransactionContext::AddFailedInsert(const string &tablet_id,
                                         const ConstContiguousRow& row,
                                         const Status &status) {
  operations_.push_back(new InsertOp(tablet_id, row, status));
  unsuccessful_ops_++;
}

void TransactionContext::AddMutation(const string &tablet_id,
                                     const txid_t &tx_id,
                                     const RowChangeList &update,
                                     gscoped_ptr<MutationResult> result) {
  operations_.push_back(new MutationOp(tablet_id, tx_id, update, result.Pass()));
}

void TransactionContext::AddFailedMutation(const string &tablet_id,
                                           const RowChangeList &update,
                                           gscoped_ptr<MutationResult> result,
                                           const Status &status) {
  operations_.push_back(new MutationOp(tablet_id, update, result.Pass(), status));
  unsuccessful_ops_++;
}

void TransactionContext::Reset() {
  STLDeleteElements(&operations_);
  unsuccessful_ops_ = 0;
}

TransactionContext::~TransactionContext() {
  STLDeleteElements(&operations_);
}

}  // namespace tablet
}  // namespace kudu

