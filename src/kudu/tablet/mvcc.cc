// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tablet/mvcc.h"

#include <algorithm>
#include <glog/logging.h>
#include <mutex>

#include "kudu/clock/logical_clock.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/mathlimits.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/stopwatch.h"

namespace kudu {
namespace tablet {

using strings::Substitute;

MvccManager::MvccManager()
  : safe_time_(Timestamp::kMin),
    earliest_in_flight_(Timestamp::kMax) {
  cur_snap_.all_committed_before_ = Timestamp::kInitialTimestamp;
  cur_snap_.none_committed_at_or_after_ = Timestamp::kInitialTimestamp;
}

void MvccManager::StartTransaction(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);
  CHECK(!cur_snap_.IsCommitted(timestamp)) << "Trying to start a new txn at an already-committed"
                                           << " timestamp: " << timestamp.ToString()
                                           << " cur_snap_: " << cur_snap_.ToString();
  CHECK(InitTransactionUnlocked(timestamp)) << "There is already a transaction with timestamp: "
                                            << timestamp.value() << " in flight or this timestamp "
                                            << "is before than or equal to \"safe\" time."
                                            << "Current Snapshot: " << cur_snap_.ToString()
                                            << " Current safe time: " << safe_time_;
}

void MvccManager::StartApplyingTransaction(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);
  auto it = timestamps_in_flight_.find(timestamp.value());
  if (PREDICT_FALSE(it == timestamps_in_flight_.end())) {
    LOG(FATAL) << "Cannot mark timestamp " << timestamp.ToString() << " as APPLYING: "
               << "not in the in-flight map.";
  }

  TxnState cur_state = it->second;
  if (PREDICT_FALSE(cur_state != RESERVED)) {
    LOG(FATAL) << "Cannot mark timestamp " << timestamp.ToString() << " as APPLYING: "
               << "wrong state: " << cur_state;
  }

  it->second = APPLYING;
}

bool MvccManager::InitTransactionUnlocked(const Timestamp& timestamp) {
  // Ensure that we didn't mark the given timestamp as "safe".
  if (PREDICT_FALSE(timestamp <= safe_time_)) {
    return false;
  }

  if (timestamp < earliest_in_flight_) {
    earliest_in_flight_ = timestamp;
  }

  return InsertIfNotPresent(&timestamps_in_flight_, timestamp.value(), RESERVED);
}

void MvccManager::AbortTransaction(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);

  // Remove from our in-flight list.
  TxnState old_state = RemoveInFlightAndGetStateUnlocked(timestamp);
  CHECK_EQ(old_state, RESERVED) << "transaction with timestamp " << timestamp.ToString()
                                << " cannot be aborted in state " << old_state;

  // If we're aborting the earliest transaction that was in flight,
  // update our cached value.
  if (earliest_in_flight_ == timestamp) {
    AdvanceEarliestInFlightTimestamp();
  }
}

void MvccManager::CommitTransaction(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);

  // Commit the transaction, but do not adjust 'all_committed_before_', that will
  // be done with a separate OfflineAdjustCurSnap() call.
  bool was_earliest = false;
  CommitTransactionUnlocked(timestamp, &was_earliest);

  if (was_earliest && safe_time_ >= timestamp) {
    // If this transaction was the earliest in-flight, we might have to adjust
    // the "clean" timestamp.
    AdjustCleanTime();
  }
}

MvccManager::TxnState MvccManager::RemoveInFlightAndGetStateUnlocked(Timestamp ts) {
  DCHECK(lock_.is_locked());

  auto it = timestamps_in_flight_.find(ts.value());
  if (it == timestamps_in_flight_.end()) {
    LOG(FATAL) << "Trying to remove timestamp which isn't in the in-flight set: "
               << ts.ToString();
  }
  TxnState state = it->second;
  timestamps_in_flight_.erase(it);
  return state;
}

void MvccManager::CommitTransactionUnlocked(Timestamp timestamp,
                                            bool* was_earliest_in_flight) {
  *was_earliest_in_flight = earliest_in_flight_ == timestamp;

  // Remove from our in-flight list.
  TxnState old_state = RemoveInFlightAndGetStateUnlocked(timestamp);
  CHECK_EQ(old_state, APPLYING)
    << "Trying to commit a transaction which never entered APPLYING state: "
    << timestamp.ToString() << " state=" << old_state;

  // Add to snapshot's committed list
  cur_snap_.AddCommittedTimestamp(timestamp);

  // If we're committing the earliest transaction that was in flight,
  // update our cached value.
  if (*was_earliest_in_flight) {
    AdvanceEarliestInFlightTimestamp();
  }
}

void MvccManager::AdvanceEarliestInFlightTimestamp() {
  if (timestamps_in_flight_.empty()) {
    earliest_in_flight_ = Timestamp::kMax;
  } else {
    earliest_in_flight_ = Timestamp(std::min_element(timestamps_in_flight_.begin(),
                                                     timestamps_in_flight_.end())->first);
  }
}

void MvccManager::AdjustSafeTime(Timestamp safe_time) {
  std::lock_guard<LockType> l(lock_);

  // No more transactions will start with a ts that is lower than or equal
  // to 'safe_time', so we adjust the snapshot accordingly.
  if (PREDICT_TRUE(safe_time_ <= safe_time)) {
    safe_time_ = safe_time;
  } else {
    // TODO(dralves) This shouldn't happen, the safe time passed to MvccManager should be
    // monotically increasing. If if does though, the impact is on scan snapshot correctness,
    // not on corruption of state and some test-only code sets this back (LocalTabletWriter).
    // Note that we will still crash if a transaction comes in with a timestamp that is lower
    // than 'cur_snap_.all_committed_before_'.
    LOG_EVERY_N(ERROR, 10) << Substitute("Tried to move safe_time back from $0 to $1. "
                                         "Current Snapshot: $2", safe_time_.ToString(),
                                         safe_time.ToString(), cur_snap_.ToString());
    return;
  }

  AdjustCleanTime();
}

// Remove any elements from 'v' which are < the given watermark.
static void FilterTimestamps(std::vector<Timestamp::val_type>* v,
                             Timestamp::val_type watermark) {
  int j = 0;
  for (const auto& ts : *v) {
    if (ts >= watermark) {
      (*v)[j++] = ts;
    }
  }
  v->resize(j);
}

void MvccManager::AdjustCleanTime() {
  // There are two possibilities:
  //
  // 1) We still have an in-flight transaction earlier than 'safe_time_'.
  //    In this case, we update the watermark to that transaction's timestamp.
  //
  // 2) There are no in-flight transactions earlier than 'safe_time_'.
  //    (There may still be in-flight transactions with future timestamps due to
  //    commit-wait transactions which start in the future). In this case, we update
  //    the watermark to 'safe_time_', since we know that no new
  //    transactions can start with an earlier timestamp.
  //
  // In either case, we have to add the newly committed ts only if it remains higher
  // than the new watermark.

  if (earliest_in_flight_ < safe_time_) {
    cur_snap_.all_committed_before_ = earliest_in_flight_;
  } else {
    cur_snap_.all_committed_before_ = safe_time_;
  }

  // Filter out any committed timestamps that now fall below the watermark
  FilterTimestamps(&cur_snap_.committed_timestamps_, cur_snap_.all_committed_before_.value());

  // it may also have unblocked some waiters.
  // Check if someone is waiting for transactions to be committed.
  if (PREDICT_FALSE(!waiters_.empty())) {
    auto iter = waiters_.begin();
    while (iter != waiters_.end()) {
      WaitingState* waiter = *iter;
      if (IsDoneWaitingUnlocked(*waiter)) {
        iter = waiters_.erase(iter);
        waiter->latch->CountDown();
        continue;
      }
      iter++;
    }
  }
}

Status MvccManager::WaitUntil(WaitFor wait_for, Timestamp ts, const MonoTime& deadline) const {
  TRACE_EVENT2("tablet", "MvccManager::WaitUntil",
               "wait_for", wait_for == ALL_COMMITTED ? "all_committed" : "none_applying",
               "ts", ts.ToUint64())

  CountDownLatch latch(1);
  WaitingState waiting_state;
  {
    waiting_state.timestamp = ts;
    waiting_state.latch = &latch;
    waiting_state.wait_for = wait_for;

    std::lock_guard<LockType> l(lock_);
    if (IsDoneWaitingUnlocked(waiting_state)) return Status::OK();
    waiters_.push_back(&waiting_state);
  }
  if (waiting_state.latch->WaitUntil(deadline)) {
    return Status::OK();
  }
  // We timed out. We need to clean up our entry in the waiters_ array.

  std::lock_guard<LockType> l(lock_);
  // It's possible that while we were re-acquiring the lock, we did get
  // notified. In that case, we have no cleanup to do.
  if (waiting_state.latch->count() == 0) {
    return Status::OK();
  }

  waiters_.erase(std::find(waiters_.begin(), waiters_.end(), &waiting_state));
  return Status::TimedOut(Substitute("Timed out waiting for all transactions with ts < $0 to $1",
                                     ts.ToString(),
                                     wait_for == ALL_COMMITTED ? "commit" : "finish applying"));
}

bool MvccManager::IsDoneWaitingUnlocked(const WaitingState& waiter) const {
  switch (waiter.wait_for) {
    case ALL_COMMITTED:
      return AreAllTransactionsCommittedUnlocked(waiter.timestamp);
    case NONE_APPLYING:
      return !AnyApplyingAtOrBeforeUnlocked(waiter.timestamp);
  }
  LOG(FATAL); // unreachable
}

bool MvccManager::AreAllTransactionsCommittedUnlocked(Timestamp ts) const {
  // If ts is before the 'all_committed_before_' watermark on the current snapshot then
  // all transactions before it are committed.
  if (ts < cur_snap_.all_committed_before_) return true;

  // We might not have moved 'cur_snap_.all_committed_before_' (the clean time) but 'ts'
  // might still come before any possible in-flights.
  return ts < earliest_in_flight_;
}

bool MvccManager::AnyApplyingAtOrBeforeUnlocked(Timestamp ts) const {
  // TODO(todd) this is not actually checking on the applying txns, it's checking on
  // _all in-flight_. Is this a bug?
  for (const InFlightMap::value_type entry : timestamps_in_flight_) {
    if (entry.first <= ts.value()) {
      return true;
    }
  }
  return false;
}

void MvccManager::TakeSnapshot(MvccSnapshot *snap) const {
  std::lock_guard<LockType> l(lock_);
  *snap = cur_snap_;
}

Status MvccManager::WaitForSnapshotWithAllCommitted(Timestamp timestamp,
                                                    MvccSnapshot* snapshot,
                                                    const MonoTime& deadline) const {
  TRACE_EVENT0("tablet", "MvccManager::WaitForSnapshotWithAllCommitted");

  RETURN_NOT_OK(WaitUntil(ALL_COMMITTED, timestamp, deadline));
  *snapshot = MvccSnapshot(timestamp);
  return Status::OK();
}

void MvccManager::WaitForApplyingTransactionsToCommit() const {
  TRACE_EVENT0("tablet", "MvccManager::WaitForApplyingTransactionsToCommit");

  // Find the highest timestamp of an APPLYING transaction.
  Timestamp wait_for = Timestamp::kMin;
  {
    std::lock_guard<LockType> l(lock_);
    for (const InFlightMap::value_type entry : timestamps_in_flight_) {
      if (entry.second == APPLYING) {
        wait_for = Timestamp(std::max(entry.first, wait_for.value()));
      }
    }
  }

  // Wait until there are no transactions applying with that timestamp
  // or below. It's possible that we're a bit conservative here - more transactions
  // may enter the APPLYING set while we're waiting, but we will eventually
  // succeed.
  if (wait_for == Timestamp::kMin) {
    // None were APPLYING: we can just return.
    return;
  }
  CHECK_OK(WaitUntil(NONE_APPLYING, wait_for, MonoTime::Max()));
}

bool MvccManager::AreAllTransactionsCommitted(Timestamp ts) const {
  std::lock_guard<LockType> l(lock_);
  return AreAllTransactionsCommittedUnlocked(ts);
}

int MvccManager::CountTransactionsInFlight() const {
  std::lock_guard<LockType> l(lock_);
  return timestamps_in_flight_.size();
}

Timestamp MvccManager::GetCleanTimestamp() const {
  std::lock_guard<LockType> l(lock_);
  return cur_snap_.all_committed_before_;
}

void MvccManager::GetApplyingTransactionsTimestamps(std::vector<Timestamp>* timestamps) const {
  std::lock_guard<LockType> l(lock_);
  timestamps->reserve(timestamps_in_flight_.size());
  for (const InFlightMap::value_type entry : timestamps_in_flight_) {
    if (entry.second == APPLYING) {
      timestamps->push_back(Timestamp(entry.first));
    }
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
  for (const Timestamp::val_type& v : committed_timestamps_) {
    if (v == timestamp.value()) return true;
  }

  return false;
}

bool MvccSnapshot::MayHaveCommittedTransactionsAtOrAfter(const Timestamp& timestamp) const {
  return timestamp < none_committed_at_or_after_;
}

bool MvccSnapshot::MayHaveUncommittedTransactionsAtOrBefore(const Timestamp& timestamp) const {
  // The snapshot may have uncommitted transactions before 'timestamp' if:
  // - 'all_committed_before_' comes before 'timestamp'
  // - 'all_committed_before_' is precisely 'timestamp' but 'timestamp' isn't in the
  //   committed set.
  return timestamp > all_committed_before_ ||
      (timestamp == all_committed_before_ && !IsCommittedFallback(timestamp));
}

std::string MvccSnapshot::ToString() const {
  std::string ret("MvccSnapshot[committed={T|");

  if (committed_timestamps_.size() == 0) {
    StrAppend(&ret, "T < ", all_committed_before_.ToString(),"}]");
    return ret;
  }
  StrAppend(&ret, "T < ", all_committed_before_.ToString(),
            " or (T in {");

  bool first = true;
  for (Timestamp::val_type t : committed_timestamps_) {
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
  for (const Timestamp& ts : timestamps) {
    AddCommittedTimestamp(ts);
  }
}

void MvccSnapshot::AddCommittedTimestamp(Timestamp timestamp) {
  if (IsCommitted(timestamp)) return;

  committed_timestamps_.push_back(timestamp.value());

  // If this is a new upper bound commit mark, update it.
  if (none_committed_at_or_after_ <= timestamp) {
    none_committed_at_or_after_ = Timestamp(timestamp.value() + 1);
  }
}

////////////////////////////////////////////////////////////
// ScopedTransaction
////////////////////////////////////////////////////////////
ScopedTransaction::ScopedTransaction(MvccManager* manager, Timestamp timestamp)
  : done_(false),
    manager_(DCHECK_NOTNULL(manager)),
    timestamp_(timestamp) {
  manager_->StartTransaction(timestamp);
}

ScopedTransaction::~ScopedTransaction() {
  if (!done_) {
    Abort();
  }
}

void ScopedTransaction::StartApplying() {
  manager_->StartApplyingTransaction(timestamp_);
}

void ScopedTransaction::Commit() {
  manager_->CommitTransaction(timestamp_);
  done_ = true;
}

void ScopedTransaction::Abort() {
  manager_->AbortTransaction(timestamp_);
  done_ = true;
}

} // namespace tablet
} // namespace kudu
