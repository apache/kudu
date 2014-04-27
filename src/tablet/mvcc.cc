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
      if (AreAllTransactionsCommitted(*waiter->snap)) {
        iter = waiters_.erase(iter);
        waiter->latch->CountDown();
        continue;
      }
      iter++;
    }
  }
}

void MvccManager::WaitUntilAllCommitted(const MvccSnapshot& snap) {

  CountDownLatch latch(1);
  WaitingState waiting_state;
  {
    boost::lock_guard<LockType> l(lock_);
    if (AreAllTransactionsCommitted(snap)) return;
    waiting_state.snap = &snap;
    waiting_state.latch = &latch;
    waiters_.push_back(&waiting_state);
  }
  waiting_state.latch->Wait();
}

bool MvccManager::AreAllTransactionsCommitted(const MvccSnapshot& snap) {
  // if 'snap' has no in-flight transactions report all committed.
  if (snap.timestamps_in_flight_.empty()) {
    return true;
  }
  // if it does, go through all of them and check if they are still
  // in the in-flight set
  BOOST_FOREACH(Timestamp::val_type in_flight_in_snap, snap.timestamps_in_flight_) {
    if (!cur_snap_.IsCommitted(Timestamp(in_flight_in_snap))) return false;
  }
  // if all the in_flights in snap are committed return true
  return true;
}

void MvccManager::TakeSnapshot(MvccSnapshot *snap) const {
  boost::lock_guard<LockType> l(lock_);
  *snap = cur_snap_;
}

void MvccManager::TakeSnapshotAtTimestamp(MvccSnapshot *snap, Timestamp timestamp) const {
  {
    boost::lock_guard<LockType> l(lock_);
    *snap = cur_snap_;
  }
  // If the current snapshot can't have uncommitted transactions before
  // 'timestamp' just return a plain snapshot in the past.
  if (!snap->MayHaveUncommittedTransactionsAtOrBefore(timestamp)) {
     *snap = MvccSnapshot(timestamp);
  }

  // If not we use the current snapshot but remove in-flight transactions from
  // it whose timestamp is greater than or equal to 'timestamp'.
  // TODO we can optimize slightly by decreasing 'none_committed_after_timestamp_'
  // to the first erased in_flight, if there is one
  MvccSnapshot::InFlightsIterator iter = snap->timestamps_in_flight_.begin();
  while (iter != snap->timestamps_in_flight_.end()) {
    if (Timestamp((*iter)).CompareTo(timestamp) >= 0) {
      iter = snap->timestamps_in_flight_.erase(iter);
      continue;
    }
    iter++;
  }
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
