// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <algorithm>

#include "gutil/mathlimits.h"
#include "gutil/port.h"
#include "gutil/stringprintf.h"
#include "gutil/strings/strcat.h"
#include "server/logical_clock.h"
#include "tablet/mvcc.h"
#include "util/countdown_latch.h"
#include "util/stopwatch.h"

namespace kudu { namespace tablet {

MvccManager::MvccManager(const scoped_refptr<server::Clock>& clock)
    : clock_(clock) {
  cur_snap_.none_committed_after_timestamp_ = Timestamp::kInitialTimestamp;
  cur_snap_.all_committed_before_timestamp_ = Timestamp::kInitialTimestamp;
}

Timestamp MvccManager::StartTransaction() {
  boost::lock_guard<LockType> l(lock_);
  Timestamp now = clock_->Now();
  while (PREDICT_FALSE(!InitTransactionUnlocked(now))) {
    now = clock_->Now();
  }
  return now;
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
  if (!cur_snap_.timestamps_in_flight_.empty()) {
    Timestamp max(*std::max_element(cur_snap_.timestamps_in_flight_.begin(),
                                    cur_snap_.timestamps_in_flight_.end()));
    CHECK_EQ(max.value(), now_latest.value());
  }
#endif

  return now_latest;
}

bool MvccManager::InitTransactionUnlocked(const Timestamp& timestamp) {
  cur_snap_.none_committed_after_timestamp_ = Timestamp(timestamp.value() + 1);
  if (cur_snap_.timestamps_in_flight_.empty()) {
    cur_snap_.all_committed_before_timestamp_ = timestamp;
  }
  return cur_snap_.timestamps_in_flight_.insert(timestamp.value()).second;
}

void MvccManager::CommitTransaction(Timestamp timestamp) {
  boost::lock_guard<LockType> l(lock_);
  DCHECK(timestamp.CompareTo(cur_snap_.none_committed_after_timestamp_) <= 0)
    << "Trying to commit timestamp which isn't in the in-flight range yet";
  unordered_set<Timestamp::val_type>::iterator it =
      cur_snap_.timestamps_in_flight_.find(timestamp.value());
  CHECK(it != cur_snap_.timestamps_in_flight_.end())
    << "Trying to commit timestamp which isn't in the in-flight set";
  cur_snap_.timestamps_in_flight_.erase(it);

  // If the timestamp was the earliest in the in-flight range, we can now advance
  // the in-flight range
  if (timestamp.CompareTo(cur_snap_.all_committed_before_timestamp_) == 0) {

    if (!cur_snap_.timestamps_in_flight_.empty()) {
      Timestamp new_min(*std::min_element(cur_snap_.timestamps_in_flight_.begin(),
                                          cur_snap_.timestamps_in_flight_.end()));
      cur_snap_.all_committed_before_timestamp_ = new_min;
    } else {
      cur_snap_.all_committed_before_timestamp_ = cur_snap_.none_committed_after_timestamp_;
    }
  }

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
  DCHECK_LT(ts.CompareTo(clock_->Now()), 0)
    << "timestamp " << ts.ToString() << " must be in the past";
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
  if (cur_snap_.timestamps_in_flight_.empty()) {
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
  DCHECK_LT(timestamp.CompareTo(clock_->Now()), 0)
    << "timestamp " << timestamp.ToString() << " must be in the past";
  WaitUntilAllCommitted(timestamp);
  *snap = MvccSnapshot(timestamp);
}

void MvccManager::WaitForCleanSnapshot(MvccSnapshot* snap) const {
  WaitForCleanSnapshotAtTimestamp(clock_->Now(), snap);
}

bool MvccManager::AreAllTransactionsCommitted(Timestamp ts) const {
  boost::lock_guard<LockType> l(lock_);
  return AreAllTransactionsCommittedUnlocked(ts);
}

MvccManager::~MvccManager() {
  CHECK(waiters_.empty());
}

////////////////////////////////////////////////////////////
// MvccSnapshot
////////////////////////////////////////////////////////////

MvccSnapshot::MvccSnapshot()
 : all_committed_before_timestamp_(Timestamp::kInitialTimestamp),
   none_committed_after_timestamp_(Timestamp::kInitialTimestamp) {
}

MvccSnapshot::MvccSnapshot(const MvccManager &manager) {
  manager.TakeSnapshot(this);
}

MvccSnapshot::MvccSnapshot(const Timestamp& timestamp)
  : all_committed_before_timestamp_(timestamp),
    none_committed_after_timestamp_(timestamp) {
 }

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingAllTransactions() {
  return MvccSnapshot(Timestamp::kMax);
}

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingNoTransactions() {
  return MvccSnapshot(Timestamp::kMin);
}

bool MvccSnapshot::IsCommitted(const Timestamp& timestamp) const {
  if (PREDICT_TRUE(timestamp.CompareTo(all_committed_before_timestamp_) < 0)) {
    return true;
  }

  if (timestamp.CompareTo(none_committed_after_timestamp_) >= 0) {
    return false;
  }

  if (timestamps_in_flight_.count(timestamp.value())) {
    return false;
  }

  return true;
}

bool MvccSnapshot::MayHaveCommittedTransactionsAtOrAfter(const Timestamp& timestamp) const {
  return timestamp.CompareTo(none_committed_after_timestamp_) < 0;
}

bool MvccSnapshot::MayHaveUncommittedTransactionsAtOrBefore(const Timestamp& timestamp) const {
  return timestamp.CompareTo(all_committed_before_timestamp_) >= 0;
}

std::string MvccSnapshot::ToString() const {
  string ret("MvccSnapshot[committed={T|");

  if (timestamps_in_flight_.size() == 0) {
    // if there are no transactions in flight these must be the same.
    CHECK(all_committed_before_timestamp_ == none_committed_after_timestamp_);
    StrAppend(&ret, "T < ", none_committed_after_timestamp_.ToString(),"}]");
    return ret;
  }
  StrAppend(&ret, "T < ", all_committed_before_timestamp_.ToString(),
            " or (T < ", none_committed_after_timestamp_.ToString(), " and T not in {");

  bool first = true;
  BOOST_FOREACH(Timestamp::val_type t, timestamps_in_flight_) {
    if (!first) {
      ret.push_back(',');
    }
    first = false;
    StrAppend(&ret, t);
  }
  ret.append("})}]");
  return ret;
}

size_t MvccSnapshot::num_transactions_in_flight() const {
  return timestamps_in_flight_.size();
}

////////////////////////////////////////////////////////////
// ScopedTransaction
////////////////////////////////////////////////////////////
ScopedTransaction::ScopedTransaction(MvccManager *mgr, bool start_at_latest)
  : committed_(false),
    manager_(DCHECK_NOTNULL(mgr)) {
  if (PREDICT_TRUE(!start_at_latest)) {
    timestamp_ = mgr->StartTransaction();
  } else {
    timestamp_ = mgr->StartTransactionAtLatest();
  }
}

ScopedTransaction::~ScopedTransaction() {
  if (!committed_) {
    Commit();
  }
}

void ScopedTransaction::Commit() {
  manager_->CommitTransaction(timestamp_);
  committed_ = true;
}


} // namespace tablet
} // namespace kudu
