// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <glog/logging.h>
#include "kudu/tablet/transaction_order_verifier.h"

namespace kudu {
namespace tablet {

TransactionOrderVerifier::TransactionOrderVerifier()
  : prev_idx_(0),
    prev_prepare_phys_timestamp_(0) {
}

TransactionOrderVerifier::~TransactionOrderVerifier() {
}

void TransactionOrderVerifier::CheckApply(int64_t op_idx,
                                          MicrosecondsInt64 prepare_phys_timestamp) {
  DFAKE_SCOPED_LOCK(fake_lock_);

  if (prev_idx_ != 0) {
    // We need to allow skips because certain ops (like NO_OP) don't have an
    // Apply() phase and are not managed by Transactions.
    CHECK_GE(op_idx, prev_idx_ + 1) << "Should apply operations in monotonic index order";
    CHECK_GE(prepare_phys_timestamp, prev_prepare_phys_timestamp_)
      << "Prepare phases should have executed in the same order as the op indexes. "
      << "op_idx=" << op_idx;
  }
  prev_idx_ = op_idx;
  prev_prepare_phys_timestamp_ = prepare_phys_timestamp;
}

} // namespace tablet
} // namespace kudu
