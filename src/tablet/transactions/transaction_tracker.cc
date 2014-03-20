// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/transaction_tracker.h"

#include <algorithm>

#include <boost/thread/mutex.hpp>

namespace kudu {
namespace tablet {

TransactionTracker::~TransactionTracker() {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(pending_txns_.size(), 0);
}

void TransactionTracker::Add(Transaction *txn) {
  boost::lock_guard<simple_spinlock> l(lock_);
  pending_txns_.insert(txn);
}

void TransactionTracker::Release(Transaction *txn) {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (PREDICT_FALSE(pending_txns_.erase(txn) != 1)) {
    LOG(FATAL) << "Could not remove pending transaction from map: " << txn->tx_ctx()->ToString();
  }
}

void TransactionTracker::WaitForAllToFinish() {
  int wait_time = 250;
  while (1) {
    {
      boost::lock_guard<simple_spinlock> l(lock_);
      if (pending_txns_.empty()) {
        break;
      }
    }
    usleep(wait_time);
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
}


}  // namespace tablet
}  // namespace kudu
