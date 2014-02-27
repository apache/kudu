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

namespace kudu { namespace tablet {

MvccManager::MvccManager()
    : clock_(new server::LogicalClock(Timestamp::kInitialTimestamp.value() - 1)) {
  cur_snap_.none_committed_after_timestamp_ = Timestamp::kInitialTimestamp;
  cur_snap_.all_committed_before_timestamp_ = Timestamp::kInitialTimestamp;
}

Timestamp MvccManager::StartTransaction() {
  boost::lock_guard<LockType> l(lock_);
  Timestamp now = clock_->Now();
  cur_snap_.none_committed_after_timestamp_ = Timestamp(now.value() + 1);
  bool was_inserted = cur_snap_.timestamps_in_flight_.insert(now.value()).second;
  DCHECK(was_inserted) << "Already had timestamp " << now.ToString() << " in in-flight list";
  return now;
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
}

void MvccManager::TakeSnapshot(MvccSnapshot *snap) const {
  boost::lock_guard<LockType> l(lock_);
  *snap = cur_snap_;
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
ScopedTransaction::ScopedTransaction(MvccManager *mgr)
  : committed_(false),
    manager_(DCHECK_NOTNULL(mgr)),
    timestamp_(mgr->StartTransaction()) {
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
