// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <algorithm>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/logical_clock.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/stopwatch.h"

namespace kudu { namespace tablet {

MvccManager::MvccManager(const scoped_refptr<server::Clock>& clock)
  : earliest_in_flight_(Timestamp::kMax),
    clock_(clock) {
  cur_snap_.all_committed_before_ = Timestamp::kInitialTimestamp;
  cur_snap_.none_committed_at_or_after_ = Timestamp::kInitialTimestamp;
}

Timestamp MvccManager::StartTransaction() {
  while (true) {
    Timestamp now = clock_->Now();
    boost::lock_guard<LockType> l(lock_);
    if (PREDICT_TRUE(InitTransactionUnlocked(now))) {
      return now;
    }
  }
  // dummy return to avoid compiler warnings
  LOG(FATAL) << "Unreachable, added to avoid compiler warning.";
  return Timestamp::kInvalidTimestamp;
}

Timestamp MvccManager::StartTransactionAtLatest() {
  boost::lock_guard<LockType> l(lock_);
  Timestamp now_latest = clock_->NowLatest();
  while (PREDICT_FALSE(!InitTransactionUnlocked(now_latest))) {
    now_latest = clock_->NowLatest();
  }

  // If in debug mode enforce that transactions have monotonically increasing
  // timestamps at all times
#ifndef NDEBUG
  if (!timestamps_in_flight_.empty()) {
    Timestamp max(*std::max_element(timestamps_in_flight_.begin(),
                                    timestamps_in_flight_.end()));
    CHECK_EQ(max.value(), now_latest.value());
  }
#endif

  return now_latest;
}

Status MvccManager::StartTransactionAtTimestamp(Timestamp timestamp) {
  boost::lock_guard<LockType> l(lock_);
  if (PREDICT_FALSE(cur_snap_.IsCommitted(timestamp))) {
    return Status::IllegalState(
        strings::Substitute("Timestamp: $0 is already committed.", timestamp.value()));
  }
  if (!InitTransactionUnlocked(timestamp)) {
    return Status::IllegalState(
        strings::Substitute("There is already a transaction with timestamp: $0 in flight.",
                            timestamp.value()));
  }
  return Status::OK();
}

bool MvccManager::InitTransactionUnlocked(const Timestamp& timestamp) {
  // Ensure that we didn't mark the given timestamp as "safe" in between
  // acquiring the time and taking the lock. This allows us to acquire timestamps
  // outside of the MVCC lock.
  if (PREDICT_FALSE(cur_snap_.all_committed_before_.CompareTo(timestamp) > 0)) {
    return false;
  }
  // Since transactions only commit once they are in the past, and new
  // transactions always start either in the current time or the future,
  // we should never be trying to start a new transaction at the same time
  // as an already-committed one.
  DCHECK(!cur_snap_.IsCommitted(timestamp))
    << "Trying to start a new txn at already-committed timestamp "
    << timestamp.ToString()
    << " cur_snap_: " << cur_snap_.ToString();

  if (timestamp.CompareTo(earliest_in_flight_) < 0) {
    earliest_in_flight_ = timestamp;
  }

  return timestamps_in_flight_.insert(timestamp.value()).second;
}

void MvccManager::CommitTransaction(Timestamp timestamp) {
  boost::lock_guard<LockType> l(lock_);
  bool was_earliest = false;
  CommitTransactionUnlocked(timestamp, &was_earliest);

  if (was_earliest) {
    // This is an 'online' commit so adjust the 'all_committed_before_' watermark
    // up to clock_->Now().
    AdjustSafeTime(clock_->Now());
  }
}

Timestamp MvccManager::GetSafeTime() {
  boost::lock_guard<LockType> l(lock_);
  return cur_snap_.all_committed_before_;
}

void MvccManager::OfflineCommitTransaction(Timestamp timestamp) {
  boost::lock_guard<LockType> l(lock_);

  // Commit the transaction, but do not adjust 'all_committed_before_', that will
  // be done with a separate OfflineAdjustCurSnap() call.
  bool was_earliest = false;
  CommitTransactionUnlocked(timestamp, &was_earliest);
}

void MvccManager::CommitTransactionUnlocked(Timestamp timestamp,
                                            bool* was_earliest_in_flight) {
  DCHECK(clock_->IsAfter(timestamp))
    << "Trying to commit a transaction with a future timestamp: "
    << timestamp.ToString() << ". Current time: " << clock_->Stringify(clock_->Now());

  *was_earliest_in_flight = earliest_in_flight_ == timestamp;

  // Remove from our in-flight list.
  CHECK_EQ(timestamps_in_flight_.erase(timestamp.value()), 1)
    << "Trying to commit timestamp which isn't in the in-flight set: "
    << timestamp.ToString();

  // Add to snapshot's committed list
  cur_snap_.AddCommittedTimestamp(timestamp);

  // If we're committing the earliest transaction that was in flight,
  // update our cached value.
  if (*was_earliest_in_flight) {
    if (timestamps_in_flight_.empty()) {
      earliest_in_flight_ = Timestamp::kMax;
    } else {
      earliest_in_flight_ = Timestamp(*std::min_element(timestamps_in_flight_.begin(),
                                                        timestamps_in_flight_.end()));
    }
  }
}

void MvccManager::OfflineAdjustSafeTime(Timestamp safe_time) {
  boost::lock_guard<LockType> l(lock_);
  AdjustSafeTime(safe_time);
}

// Remove any elements from 'v' which are < the given watermark.
static void FilterTimestamps(std::vector<Timestamp::val_type>* v,
                             Timestamp::val_type watermark) {
  int j = 0;
  for (int i = 0; i < v->size(); i++) {
    if ((*v)[i] >= watermark) {
      (*v)[j++] = (*v)[i];
    }
  }
  v->resize(j);
}

void MvccManager::AdjustSafeTime(Timestamp safe_time) {

  // There are two possibilities:
  //
  // 1) We still have an in-flight transaction earlier than 'safe_time'.
  //    In this case, we update the watermark to that transaction's timestamp.
  //
  // 2) There are no in-flight transactions earlier than 'safe_time'.
  //    (There may still be in-flight transactions with future timestamps due to
  //    commit-wait transactions which start in the future). In this case, we update
  //    the watermark to 'safe_time', since we know that no new transactions can start
  //    with an earlier timestamp.
  //
  // In either case, we have to add the newly committed ts only if it remains higher
  // than the new watermark.

  if (earliest_in_flight_.CompareTo(safe_time) < 0) {
    cur_snap_.all_committed_before_ = earliest_in_flight_;
  } else {
    cur_snap_.all_committed_before_ = safe_time;
  }

  // Filter out any committed timestamps that now fall below the watermark
  FilterTimestamps(&cur_snap_.committed_timestamps_, cur_snap_.all_committed_before_.value());

  // it may also have unblocked some waiters.
  // Check if someone is waiting for transactions to be committed.
  if (PREDICT_FALSE(!waiters_.empty())) {
    vector<WaitingState*>::iterator iter = waiters_.begin();
    while (iter != waiters_.end()) {
      WaitingState* waiter = *iter;
      if (AreAllTransactionsCommittedUnlocked(waiter->timestamp)) {
        iter = waiters_.erase(iter);
        waiter->latch->CountDown();
        continue;
      }
      iter++;
    }
  }
}

void MvccManager::WaitUntilAllCommitted(Timestamp ts) const {
  CountDownLatch latch(1);
  WaitingState waiting_state;
  {
    boost::lock_guard<LockType> l(lock_);
    if (AreAllTransactionsCommittedUnlocked(ts)) return;
    waiting_state.timestamp = ts;
    waiting_state.latch = &latch;
    waiters_.push_back(&waiting_state);
  }
  waiting_state.latch->Wait();
}

bool MvccManager::AreAllTransactionsCommittedUnlocked(Timestamp ts) const {
  if (timestamps_in_flight_.empty()) {
    // If nothing is in-flight, then check the clock. If the timestamp is in the past,
    // we know that no new uncommitted transactions may start before this ts.
    return ts.CompareTo(clock_->Now()) <= 0;
  }
  // If some transactions are in flight, then check the in-flight list.
  return !cur_snap_.MayHaveUncommittedTransactionsAtOrBefore(ts);
}

void MvccManager::TakeSnapshot(MvccSnapshot *snap) const {
  boost::lock_guard<LockType> l(lock_);
  *snap = cur_snap_;
}

void MvccManager::WaitForCleanSnapshotAtTimestamp(Timestamp timestamp, MvccSnapshot *snap) const {
  DCHECK(clock_->IsAfter(timestamp))
    << "timestamp " << timestamp.ToString() << " must be in the past";
  WaitUntilAllCommitted(timestamp);
  *snap = MvccSnapshot(timestamp);
}

void MvccManager::WaitForCleanSnapshot(MvccSnapshot* snap) const {
  WaitForCleanSnapshotAtTimestamp(clock_->Now(), snap);
}

void MvccManager::WaitForAllInFlightToCommit() const {
  Timestamp wait_for;

  {
    boost::lock_guard<LockType> l(lock_);
    if (timestamps_in_flight_.empty()) {
      return;
    }

    // We could wait exactly for the set of in-flight transactions to commit,
    // but that's a little trickier to implement. So for now, we just wait
    // until all transactions lower than the current highest timestamp.
    // This may wait longer than necessary if new transactions start with
    // a lower timestamp, but that's OK - there is no "wait for the minimum
    // amount of time" guarantee in this API.
    wait_for = Timestamp(*std::max_element(timestamps_in_flight_.begin(),
                                           timestamps_in_flight_.end()));
  }

  WaitUntilAllCommitted(wait_for);
}

bool MvccManager::AreAllTransactionsCommitted(Timestamp ts) const {
  boost::lock_guard<LockType> l(lock_);
  return AreAllTransactionsCommittedUnlocked(ts);
}

int MvccManager::CountTransactionsInFlight() const {
  boost::lock_guard<LockType> l(lock_);
  return timestamps_in_flight_.size();
}

Timestamp MvccManager::GetSafeTimestamp() const {
  boost::lock_guard<LockType> l(lock_);
  return cur_snap_.all_committed_before_;
}

void MvccManager::GetInFlightTransactionTimestamps(std::vector<Timestamp>* timestamps) const {
  boost::lock_guard<LockType> l(lock_);
  timestamps->reserve(timestamps_in_flight_.size());
  BOOST_FOREACH(const Timestamp::val_type val, timestamps_in_flight_) {
    timestamps->push_back(Timestamp(val));
  }
}

MvccManager::~MvccManager() {
  CHECK(waiters_.empty());
}

////////////////////////////////////////////////////////////
// MvccSnapshot
////////////////////////////////////////////////////////////

MvccSnapshot::MvccSnapshot()
  : all_committed_before_(Timestamp::kInitialTimestamp),
    none_committed_at_or_after_(Timestamp::kInitialTimestamp) {
}

MvccSnapshot::MvccSnapshot(const MvccManager &manager) {
  manager.TakeSnapshot(this);
}

MvccSnapshot::MvccSnapshot(const Timestamp& timestamp)
  : all_committed_before_(timestamp),
    none_committed_at_or_after_(timestamp) {
 }

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingAllTransactions() {
  return MvccSnapshot(Timestamp::kMax);
}

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingNoTransactions() {
  return MvccSnapshot(Timestamp::kMin);
}

bool MvccSnapshot::IsCommittedFallback(const Timestamp& timestamp) const {
  BOOST_FOREACH(const Timestamp::val_type& v, committed_timestamps_) {
    if (v == timestamp.value()) return true;
  }

  return false;
}

bool MvccSnapshot::MayHaveCommittedTransactionsAtOrAfter(const Timestamp& timestamp) const {
  return timestamp.CompareTo(none_committed_at_or_after_) < 0;
}

bool MvccSnapshot::MayHaveUncommittedTransactionsAtOrBefore(const Timestamp& timestamp) const {
  return timestamp.CompareTo(all_committed_before_) >= 0;
}

std::string MvccSnapshot::ToString() const {
  string ret("MvccSnapshot[committed={T|");

  if (committed_timestamps_.size() == 0) {
    StrAppend(&ret, "T < ", all_committed_before_.ToString(),"}]");
    return ret;
  }
  StrAppend(&ret, "T < ", all_committed_before_.ToString(),
            " or (T in {");

  bool first = true;
  BOOST_FOREACH(Timestamp::val_type t, committed_timestamps_) {
    if (!first) {
      ret.push_back(',');
    }
    first = false;
    StrAppend(&ret, t);
  }
  ret.append("})}]");
  return ret;
}

void MvccSnapshot::AddCommittedTimestamps(const std::vector<Timestamp>& timestamps) {
  BOOST_FOREACH(const Timestamp& ts, timestamps) {
    AddCommittedTimestamp(ts);
  }
}

void MvccSnapshot::AddCommittedTimestamp(Timestamp timestamp) {
  if (IsCommitted(timestamp)) return;

  committed_timestamps_.push_back(timestamp.value());

  // If this is a new upper bound commit mark, update it.
  if (none_committed_at_or_after_.CompareTo(timestamp) <= 0) {
    none_committed_at_or_after_ = Timestamp(timestamp.value() + 1);
  }
}

////////////////////////////////////////////////////////////
// ScopedTransaction
////////////////////////////////////////////////////////////
ScopedTransaction::ScopedTransaction(MvccManager *mgr, TimestampAssignmentType assignment_type)
  : committed_(false),
    manager_(DCHECK_NOTNULL(mgr)),
    assignment_type_(assignment_type) {

  switch (assignment_type_) {
    case NOW: {
      timestamp_ = mgr->StartTransaction();
      break;
    }
    case NOW_LATEST: {
      timestamp_ = mgr->StartTransactionAtLatest();
      break;
    }
    default: {
      LOG(FATAL) << "Illegal TransactionAssignmentType. Only NOW and NOW_LATEST are supported"
          " by this ctor.";
    }
  }
}

ScopedTransaction::ScopedTransaction(MvccManager *mgr, Timestamp timestamp)
  : committed_(false),
    manager_(DCHECK_NOTNULL(mgr)),
    assignment_type_(PRE_ASSIGNED),
    timestamp_(timestamp) {
  CHECK_OK(mgr->StartTransactionAtTimestamp(timestamp));
}

ScopedTransaction::~ScopedTransaction() {
  if (!committed_) {
    Commit();
  }
}

void ScopedTransaction::Commit() {
  switch (assignment_type_) {
    case NOW:
    case NOW_LATEST: {
      manager_->CommitTransaction(timestamp_);
      break;
    }
    case PRE_ASSIGNED: {
      manager_->OfflineCommitTransaction(timestamp_);
      break;
    }
    default: {
      LOG(FATAL) << "Unexpected transaction assignment type.";
    }
  }

  committed_ = true;
}


} // namespace tablet
} // namespace kudu
