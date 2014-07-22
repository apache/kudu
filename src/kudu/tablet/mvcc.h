// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_TABLET_MVCC_H
#define KUDU_TABLET_MVCC_H

#include <gtest/gtest_prod.h>
#include <tr1/unordered_set>
#include <string>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/server/clock.h"
#include "kudu/util/locks.h"

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

  // Create a snapshot at a specific Timestamp.
  //
  // This snapshot considers all transactions with lower timestamps to
  // be committed, and those with higher timestamps to be uncommitted.
  explicit MvccSnapshot(const Timestamp& timestamp);

  // Create a snapshot which considers all transactions as committed.
  // This is mostly useful in test contexts.
  static MvccSnapshot CreateSnapshotIncludingAllTransactions();

  // Creates a snapshot which considers no transactions committed.
  static MvccSnapshot CreateSnapshotIncludingNoTransactions();

  // Return true if the given transaction ID should be considered committed
  // in this snapshot.
  inline bool IsCommitted(const Timestamp& timestamp) const {
    // Inline the most likely path, in which our watermarks determine
    // whether a transaction is committed.
    if (PREDICT_TRUE(timestamp.CompareTo(all_committed_before_) < 0)) {
      return true;
    }
    if (PREDICT_TRUE(timestamp.CompareTo(none_committed_at_or_after_) >= 0)) {
      return false;
    }
    // Out-of-line the unlikely case which involves more complex (loopy) code.
    return IsCommittedFallback(timestamp);
  }

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

  // Return true if the snapshot is considered 'clean'. A clean snapshot is one
  // which is determined only by a timestamp -- the snapshot considers all
  // transactions with timestamps less than some timestamp to be committed,
  // and all other transactions to be uncommitted.
  bool is_clean() const {
    return committed_timestamps_.empty();
  }

  // Consider the given list of timestamps to be committed in this snapshot,
  // even if they weren't when the snapshot was constructed.
  // This is used in the flush path, where the set of commits going into a
  // flushed file may not be a consistent snapshot from the MVCC point of view,
  // yet we need to construct a scanner that accurately represents that set.
  void AddCommittedTimestamps(const std::vector<Timestamp>& timestamps);

 private:
  friend class MvccManager;
  FRIEND_TEST(MvccTest, TestMayHaveCommittedTransactionsAtOrAfter);
  FRIEND_TEST(MvccTest, TestMayHaveUncommittedTransactionsBefore);
  FRIEND_TEST(MvccTest, TestWaitUntilAllCommitted_SnapAtTimestampWithInFlights);

  bool IsCommittedFallback(const Timestamp& timestamp) const;

  void AddCommittedTimestamp(Timestamp timestamp);

  // Summary rule:
  //   A transaction T is committed if and only if:
  //      T < all_committed_before_ or
  //   or committed_timestamps_.contains(T)
  //
  // In ASCII form, where 'C' represents a committed transaction,
  // and 'U' represents an uncommitted one:
  //
  //   CCCCCCCCCCCCCCCCCUUUUUCUUUCU
  //                    |    \___\___ committed_timestamps_
  //                    |
  //                    \- all_committed_before_


  // A transaction ID below which all transactions have been committed.
  // For any timestamp X, if X < all_committed_timestamp_, then X is committed.
  Timestamp all_committed_before_;

  // A transaction ID at or beyond which no transactions have been committed.
  // For any timestamp X, if X >= none_committed_after_, then X is uncommitted.
  // This is equivalent to max(committed_timestamps_) + 1, but since
  // that vector is unsorted, we cache it.
  Timestamp none_committed_at_or_after_;

  // The set of transactions higher than all_committed_before_timestamp_ which
  // are committed in this snapshot.
  // It might seem like using an unordered_set<> or a set<> would be faster here,
  // but in practice, this list tends to be stay pretty small, and is only
  // rarely consulted (most data will be culled by 'all_committed_before_'
  // or none_committed_at_or_after_. So, using the compact vector structure fits
  // the whole thing on one or two cache lines, and it ends up going faster.
  std::vector<Timestamp::val_type> committed_timestamps_;

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

  // Begins a new transaction, which is assigned the provided timestamp.
  // Returns Status::OK() if the transaction was started successfully or
  // Status::IllegalState() if the provided timestamp is already considered
  // committed, e.g. if timestamp < 'all_committed_before_'.
  Status StartTransactionAtTimestamp(Timestamp timestamp);

  // Commit the given transaction.
  //
  // If the transaction is not currently in-flight, this will trigger an
  // assertion error. It is an error to commit the same transaction more
  // than once.
  //
  // This should be used for 'true' online transaction processing on LEADER
  // replicas and not for delayed processing on FOLLOWER/LEARNER replicas or
  // on bootstrap, as this advances 'all_committed_before_' to clock_->Now()
  // when possible.
  void CommitTransaction(Timestamp timestamp);

  // Returns the current safe time, i.e. the minimum timestamp such that
  // all transactions with timestamps lower than it have been committed.
  Timestamp GetSafeTime();

  // Same as commit transaction but does not advance 'all_committed_before_'.
  // Used for bootstrap and delayed processing in FOLLOWERS/LEARNERS.
  void OfflineCommitTransaction(Timestamp timestamp);

  // Used in conjunction with OfflineCommitTransaction() so that the mvcc
  // manager can trim state.
  void OfflineAdjustSafeTime(Timestamp safe_time);

  // Take a snapshot of the current MVCC state, which indicates which
  // transactions have been committed at the time of this call.
  void TakeSnapshot(MvccSnapshot *snapshot) const;

  // Take a snapshot of the MVCC state at 'timestamp' (i.e which includes
  // all transactions which have a lower timestamp)
  //
  // If there are any in-flight transactions at a lower timestamp, waits for
  // them to complete before returning. Hence, we guarantee that, upon return,
  // snapshot->is_clean().
  //
  // REQUIRES: 'timestamp' must be in the past according to the configured
  // clock.
  void WaitForCleanSnapshotAtTimestamp(Timestamp timestamp,
                                       MvccSnapshot* snapshot) const;

  // Take a snapshot at the current timestamp, and then wait for any
  // currently running transactions at an earlier timestamp to finish.
  //
  // The returned snapshot acts as a "barrier":
  // - all transactions which started prior to this call are included in
  //   snapshot
  // - no transactions which start after the call returns will be included
  //   in snapshot
  // - snapshot->is_clean() is guaranteed
  //
  // Note that transactions are not blocked during this call.
  void WaitForCleanSnapshot(MvccSnapshot* snapshot) const;


  // Wait until all of the currently in-flight transactions have committed.
  //
  // NOTE: this does _not_ guarantee that no transactions are in flight upon
  // return -- just that those that were in-flight at call time are finished
  // upon return.
  void WaitForAllInFlightToCommit() const;

  bool AreAllTransactionsCommitted(Timestamp ts) const;

  // Return the number of transactions in flight..
  int CountTransactionsInFlight() const;

  // Returns the earliest possible timestamp for an uncommitted transaction.
  // All timestamps before this one are guaranteed to be committed.
  Timestamp GetSafeTimestamp() const;

  // Return the timestamps of all currently in-flight transactions.
  void GetInFlightTransactionTimestamps(std::vector<Timestamp>* timestamps) const;

  ~MvccManager();

 private:
  friend class MvccTest;
  FRIEND_TEST(MvccTest, TestAreAllTransactionsCommitted);

  bool InitTransactionUnlocked(const Timestamp& timestamp);

  struct WaitingState {
    Timestamp timestamp;
    CountDownLatch* latch;
  };

  // Returns true if all transactions before the given timestamp are committed.
  //
  // If 'ts' is not in the past, it's still possible that new transactions could
  // start with a lower timestamp after this returns.
  bool AreAllTransactionsCommittedUnlocked(Timestamp ts) const;

  // Waits until all transactions before the given time are committed.
  void WaitUntilAllCommitted(Timestamp ts) const;

  // Commits the given transaction.
  // Sets *was_earliest to true if this was the earliest in-flight transaction.
  void CommitTransactionUnlocked(Timestamp timestamp,
                                 bool* was_earliest);

  void AdjustSafeTime(Timestamp safe_time);

  typedef simple_spinlock LockType;
  mutable LockType lock_;

  MvccSnapshot cur_snap_;

  // The set of timestamps corresponding to currently in-flight transactions.
  std::tr1::unordered_set<Timestamp::val_type> timestamps_in_flight_;

  // The minimum timestamp in timestamps_in_flight_, or Timestamp::kMax
  // if that set is empty. This is cached in order to avoid having to iterate
  // over timestamps_in_flight_ on every commit.
  Timestamp earliest_in_flight_;

  scoped_refptr<server::Clock> clock_;
  mutable std::vector<WaitingState*> waiters_;

  DISALLOW_COPY_AND_ASSIGN(MvccManager);
};

// A scoped handle to a running transaction.
// When this object goes out of scope, the transaction is automatically
// committed.
class ScopedTransaction {
 public:

  // How to assign the timestamp to this transaction:
  // NOW - Based on the value obtained from clock_->Now().
  // NOW_LATEST - Based on the value obtained from clock_->NowLatest().
  // PRE_ASSIGNED - Based on the value passed in the ctor.
  enum TimestampAssignmentType {
    NOW,
    NOW_LATEST,
    PRE_ASSIGNED
  };

  // Create a new transaction from the given MvccManager.
  // If 'latest' is true this transaction will use MvccManager::StartTransactionAtLatest()
  // instead of MvccManager::StartTransaction().
  //
  // The MvccManager must remain valid for the lifetime of this object.
  ScopedTransaction(MvccManager *manager, TimestampAssignmentType assignment_type = NOW);

  // Like the ctor above but starts the transaction at a pre-defined timestamp.
  // When this transaction is committed it will use MvccManager::OfflineCommitTransaction()
  // so this is appropriate for offline replaying of transactions for replica catch-up or
  // bootstrap.
  explicit ScopedTransaction(MvccManager *manager, Timestamp timestamp);

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
  TimestampAssignmentType assignment_type_;
  Timestamp timestamp_;

  DISALLOW_COPY_AND_ASSIGN(ScopedTransaction);
};


} // namespace tablet
} // namespace kudu

#endif
