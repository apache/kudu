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
#include "tablet/mvcc.h"

namespace kudu { namespace tablet {

const txid_t txid_t::kMin(MathLimits<txid_t::val_type>::kMin);
const txid_t txid_t::kMax(MathLimits<txid_t::val_type>::kMax);
const txid_t txid_t::kInitialTxId(MathLimits<txid_t::val_type>::kMin + 1);
const txid_t txid_t::kInvalidTxId(MathLimits<txid_t::val_type>::kMax - 1);

MvccManager::MvccManager()
{}

txid_t MvccManager::StartTransaction() {
  boost::lock_guard<LockType> l(lock_);
  txid_t my_txid(cur_snap_.none_committed_after_txid_.v++);
  bool was_inserted = cur_snap_.txids_in_flight_.insert(my_txid.v).second;
  DCHECK(was_inserted) << "Already had txid " << my_txid.ToString() << " in in-flight list";
  return my_txid;
}

void MvccManager::CommitTransaction(txid_t txid) {
  boost::lock_guard<LockType> l(lock_);
  DCHECK(txid.CompareTo(cur_snap_.none_committed_after_txid_) <= 0)
    << "Trying to commit txid which isn't in the in-flight range yet";
  unordered_set<txid_t::val_type>::iterator it = cur_snap_.txids_in_flight_.find(txid.v);
  CHECK(it != cur_snap_.txids_in_flight_.end())
    << "Trying to commit txid which isn't in the in-flight set";
  cur_snap_.txids_in_flight_.erase(it);

  // If the txid was the earliest in the in-flight range, we can now advance
  // the in-flight range
  if (txid == cur_snap_.all_committed_before_txid_) {

    if (!cur_snap_.txids_in_flight_.empty()) {
      txid_t new_min(*std::min_element(cur_snap_.txids_in_flight_.begin(),
                                       cur_snap_.txids_in_flight_.end()));
      cur_snap_.all_committed_before_txid_ = new_min;
    } else {
      cur_snap_.all_committed_before_txid_ = cur_snap_.none_committed_after_txid_;
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
 : all_committed_before_txid_(txid_t::kInitialTxId),
   none_committed_after_txid_(txid_t::kInitialTxId) {
}

MvccSnapshot::MvccSnapshot(const MvccManager &manager) {
  manager.TakeSnapshot(this);
}

MvccSnapshot::MvccSnapshot(const txid_t& txid)
  : all_committed_before_txid_(txid),
    none_committed_after_txid_(txid) {
 }

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingAllTransactions() {
  return MvccSnapshot(txid_t::kMax);
}

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingNoTransactions() {
  return MvccSnapshot(txid_t::kMin);
}

bool MvccSnapshot::IsCommitted(const txid_t& txid) const {
  if (PREDICT_TRUE(txid.CompareTo(all_committed_before_txid_) < 0)) {
    return true;
  }

  if (txid.CompareTo(none_committed_after_txid_) >= 0) {
    return false;
  }

  if (txids_in_flight_.count(txid.v)) {
    return false;
  }

  return true;
}

bool MvccSnapshot::MayHaveCommittedTransactionsAtOrAfter(const txid_t& txid) const {
  return txid.CompareTo(none_committed_after_txid_) < 0;
}

bool MvccSnapshot::MayHaveUncommittedTransactionsAtOrBefore(const txid_t& txid) const {
  return txid.CompareTo(all_committed_before_txid_) >= 0;
}

std::string MvccSnapshot::ToString() const {
  string ret("MvccSnapshot[committed={T|");

 if (txids_in_flight_.size() + all_committed_before_txid_.v == none_committed_after_txid_.v) {
    StrAppend(&ret, "T < ", all_committed_before_txid_.ToString(),"}]");
    return ret;
  }
 StrAppend(&ret, "T < ", all_committed_before_txid_.ToString(),
           " or (T < ", none_committed_after_txid_.ToString(), " and T not in {");

  bool first = true;
  BOOST_FOREACH(txid_t::val_type t, txids_in_flight_) {
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
  return txids_in_flight_.size();
}

////////////////////////////////////////////////////////////
// ScopedTransaction
////////////////////////////////////////////////////////////
ScopedTransaction::ScopedTransaction(MvccManager *mgr)
  : committed_(false),
    manager_(DCHECK_NOTNULL(mgr)),
    txid_(mgr->StartTransaction()) {
}

ScopedTransaction::~ScopedTransaction() {
  if (!committed_) {
    Commit();
  }
}

void ScopedTransaction::Commit() {
  manager_->CommitTransaction(txid_);
  committed_ = true;
}


} // namespace tablet
} // namespace kudu
