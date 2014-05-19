// Copyright (c) 2014, Cloudera, inc.

#ifndef KUDU_TABLET_TRANSACTION_TRACKER_H_
#define KUDU_TABLET_TRANSACTION_TRACKER_H_

#include <string>
#include <vector>

#include <tr1/unordered_set>

#include "gutil/ref_counted.h"
#include "util/locks.h"

namespace kudu {
namespace tablet {
class TransactionDriver;

// Each TabletPeer has a TransactionTracker which keeps track of pending transactions.
// Each "LeaderTransaction" will register itself by calling Add().
// It will remove itself by calling Release().
class TransactionTracker {
 public:
  TransactionTracker();
  ~TransactionTracker();

  // Adds a transaction to the set of tracked transactions.
  void Add(TransactionDriver *driver);

  // Removes the txn from the pending list.
  // Also triggers the deletion of the Transaction object, if its refcount == 0.
  void Release(TransactionDriver *driver);

  // Populates list of currently-running transactions into 'pending_out' vector.
  void GetPendingTransactions(std::vector<scoped_refptr<TransactionDriver> >* pending_out) const;

  // Returns number of pending transactions.
  int GetNumPendingForTests() const;

  void WaitForAllToFinish();

 private:
  mutable simple_spinlock lock_;
  std::tr1::unordered_set<scoped_refptr<TransactionDriver>,
                          ScopedRefPtrHashFunctor<TransactionDriver>,
                          ScopedRefPtrEqualToFunctor<TransactionDriver> > pending_txns_;

  DISALLOW_COPY_AND_ASSIGN(TransactionTracker);
};

}  // namespace tablet
}  // namespace kudu

#endif // KUDU_TABLET_TRANSACTION_TRACKER_H_
