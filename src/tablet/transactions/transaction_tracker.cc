// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/transaction_tracker.h"

#include <algorithm>
#include <vector>

#include <boost/thread/mutex.hpp>

#include "gutil/strings/substitute.h"

namespace kudu {
namespace tablet {

using std::vector;
using strings::Substitute;

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

void TransactionTracker::GetPendingTransactions(vector<scoped_refptr<Transaction> >* pending_out) {
  DCHECK(pending_out->empty());
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const scoped_refptr<Transaction>& tx, pending_txns_) {
    // Increments refcount of each transaction.
    pending_out->push_back(tx);
  }
}

int TransactionTracker::GetNumPendingForTests() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return pending_txns_.size();
}

void TransactionTracker::WaitForAllToFinish() {
  const int complain_ms = 1000;
  int wait_time = 250;
  int num_complaints = 0;
  MonoTime start_time = MonoTime::Now(MonoTime::FINE);
  while (1) {
    {
      boost::lock_guard<simple_spinlock> l(lock_);
      if (pending_txns_.empty()) {
        break;
      }
    }
    usleep(wait_time);
    MonoDelta diff = MonoTime::Now(MonoTime::FINE).GetDeltaSince(start_time);
    int64_t waited_ms = diff.ToMilliseconds();
    if (waited_ms / complain_ms > num_complaints) {
      LOG(WARNING) << Substitute("TransactionTracker waiting for outstanding transactions to"
                                 " complete now for $0 ms", waited_ms);
      num_complaints++;
    }
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
}


}  // namespace tablet
}  // namespace kudu
