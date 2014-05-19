// Copyright (c) 2013, Cloudera, inc.

#include "tablet/transactions/transaction_tracker.h"

#include <algorithm>
#include <vector>

#include <boost/thread/mutex.hpp>

#include "gutil/strings/substitute.h"
#include "tablet/transactions/transaction_driver.h"

namespace kudu {
namespace tablet {

using std::vector;
using strings::Substitute;

TransactionTracker::TransactionTracker() {
}

TransactionTracker::~TransactionTracker() {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(pending_txns_.size(), 0);
}

void TransactionTracker::Add(TransactionDriver *driver) {
  boost::lock_guard<simple_spinlock> l(lock_);
  pending_txns_.insert(driver);
}

void TransactionTracker::Release(TransactionDriver *driver) {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (PREDICT_FALSE(pending_txns_.erase(driver) != 1)) {
    LOG(FATAL) << "Could not remove pending transaction from map: " << driver->ToString();
  }
}

void TransactionTracker::GetPendingTransactions(
    vector<scoped_refptr<TransactionDriver> >* pending_out) const {
  DCHECK(pending_out->empty());
  boost::lock_guard<simple_spinlock> l(lock_);
  BOOST_FOREACH(const scoped_refptr<TransactionDriver>& tx, pending_txns_) {
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
      LOG(WARNING) << Substitute("TransactionTracker waiting for $0 outstanding transactions to"
                                 " complete now for $1 ms", pending_txns_.size(), waited_ms);
      num_complaints++;
    }
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
}


}  // namespace tablet
}  // namespace kudu
