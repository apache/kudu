// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_MVCC_H
#define KUDU_TABLET_MVCC_H

#include <gtest/gtest.h>
#include <tr1/unordered_set>
#include <string>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "server/clock.h"
#include "util/locks.h"

namespace kudu {
class CountDownLatch;
namespace tablet {
class MvccManager;

using std::tr1::unordered_set;
using std::string;

// A snapshot of the current MVCC state, which can determine whether
// a transaction ID should be considered visible.
class MvccSnapshot {
 public:
  MvccSnapshot();

  // Create a snapshot with the current state of the given manager
  explicit MvccSnapshot(const MvccManager &manager);

  // Create a snapshot at a specific Timestamp
  explicit MvccSnapshot(const Timestamp& timestamp);

  // Create a snapshot which considers all transactions as committed.
  // This is mostly useful in test contexts.
  static MvccSnapshot CreateSnapshotIncludingAllTransactions();

  // Creates a snapshot which considers no transactions committed.
  static MvccSnapshot CreateSnapshotIncludingNoTransactions();

  // Return true if the given transaction ID should be considered committed
  // in this snapshot.
  bool IsCommitted(const Timestamp& timestamp) const;

  // Returns true if this snapshot may have any committed transactions with ID
  // equal to or higher than the provided 'timestamp'.
  // This is mostly useful to avoid scanning REDO deltas in certain cases.
  // If MayHaveCommittedTransactionsAtOrAfter(delta_stats.min) returns true
  // it means that there might be transactions that need to be applied in the
  // context of this snapshot; otherwise no scanning is necessary.
  bool MayHaveCommittedTransactionsAtOrAfter(const Timestamp& timestamp) const;

  // Returns true if this snapshot may have any uncommitted transactions with ID
  // equal to or lower than the provided 'timestamp'.
  // This is mostly useful to avoid scanning UNDO deltas in certain cases.
  // If MayHaveUncommittedTransactionsAtOrBefore(delta_stats.max) returns false it
  // means that all UNDO delta transactions are committed in the context of this
  // snapshot and no scanning is necessary; otherwise there might be some
  // transactions that need to be undone.
  bool MayHaveUncommittedTransactionsAtOrBefore(const Timestamp& timestamp) const;

  // Return a string representation of the set of committed transactions
  // in this snapshot, suitable for debug printouts.
  string ToString() const;

  // Return the number of transactions in flight during this snapshot.
  size_t num_transactions_in_flight() const;

 private:
  friend class MvccManager;
  FRIEND_TEST(MvccTest, TestMayHaveCommittedTransactionsAtOrAfter);
  FRIEND_TEST(MvccTest, TestMayHaveUncommittedTransactionsBefore);
  FRIEND_TEST(MvccTest, TestWaitUntilAllCommitted_SnapAtTimestampWithInFlights);

  typedef unordered_set<Timestamp::val_type>::iterator InFlightsIterator;

  // Summary rule:
  // A transaction T is committed if and only if:
  //    T < all_committed_before_timestamp_
  // or (T < none_committed_after_timestamp && !timestamps_in_flight.contains(T))

  // A transaction ID below which all transactions have been committed.
  // For any timestamp X, if X < all_committed_timestamp_, then X is committed.
  Timestamp all_committed_before_timestamp_;

  // A transaction ID above which no transactions have been committed.
  // For any timestamp X, if X >= none_committed_after_timestamp_, then X is not committed.
  Timestamp none_committed_after_timestamp_;

  // The current set of transactions which are in flight.
  // For any timestamp X, if X is in timestamps_in_flight_, it is not yet committed.
  unordered_set<Timestamp::val_type> timestamps_in_flight_;

};

// Coordinator of MVCC transactions. Threads wishing to make updates use
// the MvccManager to obtain a unique timestamp, usually through the ScopedTransaction
// class defined below.
//
// NOTE: There is no support for transaction abort/rollback, since
// transaction support is quite simple. Transactions are only used to
// defer visibility of updates until commit time, and allow iterators to
// operate on a snapshot which contains only committed transactions.
class MvccManager {
 public:
  explicit MvccManager(const scoped_refptr<server::Clock>& clock);

  // Begin a new transaction, assigning it a transaction ID.
  // Callers should generally prefer using the ScopedTransaction class defined
  // below, which will automatically finish the transaction when it goes out
  // of scope.
  Timestamp StartTransaction();

  // The same as the above but but starts the transaction at the latest possible
  // time, i.e. now + max_error. Returns Timestamp::kInvalidTimestamp if it was
  // not possible to obtain the latest time.
  Timestamp StartTransactionAtLatest();

  // Commit the given transaction.
  //
  // If the transaction is not currently in-flight, this will trigger an
  // assertion error. It is an error to commit the same transaction more
  // than once.
  void CommitTransaction(Timestamp timestamp);

  // Take a snapshot of the current MVCC state, which indicates which
  // transactions have been committed at the time of this call.
  void TakeSnapshot(MvccSnapshot *snapshot) const;

  // Take a snapshot of the MVCC state at 'timestamp'. The difference
  // between this method and the above is that in-flight transactions
  // whose timestamp is bigger than or equal to 'timestamp' will not
  // be included in the returned snapshot.
  void TakeSnapshotAtTimestamp(MvccSnapshot *snapshot, Timestamp timestamp) const;

  // Blocks until all transactions in the provided snapshot are committed.
  // 'snap' is assumed to having been obtained from a previous call
  // to TakeSnapshot() or TakeSnapshotAtTimestamp().
  void WaitUntilAllCommitted(const MvccSnapshot& snap);

  ~MvccManager();

 private:
  bool InitTransactionUnlocked(const Timestamp& timestamp);

  FRIEND_TEST(MvccTest, TestAreAllTransactionsCommitted);

  struct WaitingState {
    const MvccSnapshot* snap;
    CountDownLatch* latch;
  };

  // Returns true if all transactions in the provided snapshot are committed.
  // NOTE: This does not take 'lock_', it is assumed that the caller has
  // previously obtained the lock.
  bool AreAllTransactionsCommitted(const MvccSnapshot& snapshot);

  typedef simple_spinlock LockType;
  mutable LockType lock_;
  MvccSnapshot cur_snap_;
  scoped_refptr<server::Clock> clock_;
  std::vector<WaitingState*> waiters_;

  DISALLOW_COPY_AND_ASSIGN(MvccManager);
};

// A scoped handle to a running transaction.
// When this object goes out of scope, the transaction is automatically
// committed.
class ScopedTransaction {
 public:
  // Create a new transaction from the given MvccManager.
  // If 'latest' is true this transaction will use MvccManager::StartTransactionAtLatest()
  // instead of MvccManager::StartTransaction().
  //
  // The MvccManager must remain valid for the lifetime of this object.
  explicit ScopedTransaction(MvccManager *manager, bool start_at_latest = false);

  // Commit the transaction referenced by this scoped object, if it hasn't
  // already been committed.
  ~ScopedTransaction();

  Timestamp timestamp() const {
    return timestamp_;
  }

  // Commit the in-flight transaction.
  void Commit();

 private:
  bool committed_;
  MvccManager * const manager_;
  Timestamp timestamp_;

  DISALLOW_COPY_AND_ASSIGN(ScopedTransaction);
};


} // namespace tablet
} // namespace kudu

#endif
