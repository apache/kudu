// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/tablet/transactions/transaction.h"

namespace kudu {
namespace tablet {

using consensus::DriverType;

Transaction::Transaction(TransactionState* state, DriverType type, TransactionType tx_type)
    : state_(state),
      type_(type),
      tx_type_(tx_type) {
}

TransactionState::TransactionState(TabletPeer* tablet_peer)
    : tablet_peer_(tablet_peer),
      completion_clbk_(new TransactionCompletionCallback()),
      timestamp_error_(0),
      arena_(32 * 1024, 4 * 1024 * 1024),
      external_consistency_mode_(CLIENT_PROPAGATED) {
}

TransactionState::~TransactionState() {
}

TransactionCompletionCallback::TransactionCompletionCallback()
    : code_(tserver::TabletServerErrorPB::UNKNOWN_ERROR) {
}

void TransactionCompletionCallback::set_error(const Status& status,
                                              tserver::TabletServerErrorPB::Code code) {
  status_ = status;
  code_ = code;
}

void TransactionCompletionCallback::set_error(const Status& status) {
  status_ = status;
}

bool TransactionCompletionCallback::has_error() const {
  return !status_.ok();
}

const Status& TransactionCompletionCallback::status() const {
  return status_;
}

const tserver::TabletServerErrorPB::Code TransactionCompletionCallback::error_code() const {
  return code_;
}

void TransactionCompletionCallback::TransactionCompleted() {}

TransactionCompletionCallback::~TransactionCompletionCallback() {}

TransactionMetrics::TransactionMetrics()
  : successful_inserts(0),
    successful_updates(0),
    successful_deletes(0),
    commit_wait_duration_usec(0) {
}

void TransactionMetrics::Reset() {
  successful_inserts = 0;
  successful_updates = 0;
  successful_deletes = 0;
  commit_wait_duration_usec = 0;
}


}  // namespace tablet
}  // namespace kudu
