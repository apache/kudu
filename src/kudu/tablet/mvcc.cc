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
#include <mutex>
#include <ostream>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"

DEFINE_int32(inject_latency_ms_before_starting_op, 0,
             "Amount of latency in ms to inject before registering "
             "an op with MVCC.");
TAG_FLAG(inject_latency_ms_before_starting_op, advanced);
TAG_FLAG(inject_latency_ms_before_starting_op, hidden);

namespace kudu {
namespace tablet {

using strings::Substitute;

MvccManager::MvccManager()
  : new_op_timestamp_exc_lower_bound_(Timestamp::kMin),
    earliest_op_in_flight_(Timestamp::kMax),
    open_(true) {
  cur_snap_.type_ = MvccSnapshot::kLatest;
  cur_snap_.all_applied_before_ = Timestamp::kInitialTimestamp;
  cur_snap_.none_applied_at_or_after_ = Timestamp::kInitialTimestamp;
}

Status MvccManager::CheckIsCleanTimeInitialized() const {
  if (GetCleanTimestamp() == Timestamp::kInitialTimestamp) {
    return Status::Uninitialized("clean time has not yet been initialized");
  }
  return Status::OK();
}

void MvccManager::StartOp(Timestamp timestamp) {
  MAYBE_INJECT_RANDOM_LATENCY(FLAGS_inject_latency_ms_before_starting_op);
  std::lock_guard<LockType> l(lock_);
  CHECK(!cur_snap_.IsApplied(timestamp)) <<
      Substitute("Trying to start a new op at an already applied "
                 "timestamp: $0, current MVCC snapshot: $1",
                 timestamp.ToString(), cur_snap_.ToString());
  CHECK(InitOpUnlocked(timestamp)) <<
      Substitute("There is already an op with timestamp: $0 in flight, or "
                 "this timestamp is below or equal to the exclusive lower "
                 "bound for new op timestamps. Current lower bound: "
                 "$1, current MVCC snapshot: $2", timestamp.ToString(),
                 new_op_timestamp_exc_lower_bound_.ToString(),
                 cur_snap_.ToString());
}

void MvccManager::StartApplyingOp(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);
  auto it = ops_in_flight_.find(timestamp.value());
  if (PREDICT_FALSE(it == ops_in_flight_.end())) {
    LOG(FATAL) << "Cannot mark timestamp " << timestamp.ToString() << " as APPLYING: "
               << "not in the in-flight map.";
  }

  OpState cur_state = it->second;
  if (PREDICT_FALSE(cur_state != RESERVED)) {
    LOG(FATAL) << "Cannot mark timestamp " << timestamp.ToString() << " as APPLYING: "
               << "wrong state: " << cur_state;
  }
  it->second = APPLYING;
}

bool MvccManager::InitOpUnlocked(const Timestamp& timestamp) {
  // Ensure we're not trying to start an op that falls before our lower
  // bound.
  if (PREDICT_FALSE(timestamp <= new_op_timestamp_exc_lower_bound_)) {
    return false;
  }

  earliest_op_in_flight_ = std::min(timestamp, earliest_op_in_flight_);

  return InsertIfNotPresent(&ops_in_flight_, timestamp.value(), RESERVED);
}

void MvccManager::AbortOp(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);

  // Remove from our in-flight list.
  OpState old_state = RemoveInFlightAndGetStateUnlocked(timestamp);

  // If the tablet is shutting down, we can ignore the state of the
  // ops.
  if (PREDICT_FALSE(!open_.load())) {
    LOG(WARNING) << "aborting op with timestamp " << timestamp.ToString()
        << " in state " << old_state << "; MVCC is closed";
    return;
  }

  CHECK_EQ(old_state, RESERVED) << "op with timestamp " << timestamp.ToString()
                                << " cannot be aborted in state " << old_state;

  // If we're aborting the earliest op that was in flight,
  // update our cached value.
  if (earliest_op_in_flight_ == timestamp) {
    AdvanceEarliestInFlightTimestamp();
  }
}

void MvccManager::FinishApplyingOp(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);

  // Apply the op, but do not adjust 'all_applied_before_', that will
  // be done with a separate OfflineAdjustCurSnap() call.
  bool was_earliest = false;
  ApplyOpUnlocked(timestamp, &was_earliest);

  // NOTE: we should have pushed the lower bound forward before applying, but
  // we may not have in tests.
  if (was_earliest && new_op_timestamp_exc_lower_bound_ >= timestamp) {
    // If this op was the earliest in-flight, we might have to adjust
    // the "clean" timestamp.
    AdjustCleanTimeUnlocked();
  }
}

MvccManager::OpState MvccManager::RemoveInFlightAndGetStateUnlocked(Timestamp ts) {
  DCHECK(lock_.is_locked());

  auto it = ops_in_flight_.find(ts.value());
  if (it == ops_in_flight_.end()) {
    LOG(FATAL) << "Trying to remove timestamp which isn't in the in-flight set: "
               << ts.ToString();
  }
  OpState state = it->second;
  ops_in_flight_.erase(it);
  return state;
}

void MvccManager::ApplyOpUnlocked(Timestamp timestamp,
                                  bool* was_earliest_op_in_flight) {
  *was_earliest_op_in_flight = earliest_op_in_flight_ == timestamp;

  // Remove from our in-flight list.
  OpState old_state = RemoveInFlightAndGetStateUnlocked(timestamp);
  CHECK_EQ(old_state, APPLYING)
      << "Trying to apply an op which never entered APPLYING state: "
      << timestamp.ToString() << " state=" << old_state;

  // Add to snapshot's applied list
  cur_snap_.AddAppliedTimestamp(timestamp);

  // If we're applying the earliest op that was in flight, update our cached
  // value.
  if (*was_earliest_op_in_flight) {
    AdvanceEarliestInFlightTimestamp();
  }
}

void MvccManager::AdvanceEarliestInFlightTimestamp() {
  if (ops_in_flight_.empty()) {
    earliest_op_in_flight_ = Timestamp::kMax;
  } else {
    earliest_op_in_flight_ = Timestamp(std::min_element(ops_in_flight_.begin(),
                                                     ops_in_flight_.end())->first);
  }
}

void MvccManager::AdjustNewOpLowerBound(Timestamp timestamp) {
  std::lock_guard<LockType> l(lock_);
  // No more ops will start with a timestamp that is lower than or
  // equal to 'timestamp', so we adjust the snapshot accordingly.
  if (PREDICT_TRUE(new_op_timestamp_exc_lower_bound_ <= timestamp)) {
    DVLOG(4) << "Adjusting new op lower bound to: " << timestamp;
    new_op_timestamp_exc_lower_bound_ = timestamp;
  } else {
    // Note: Getting here means that we are about to apply an op out of
    // order. This out-of-order applying is only safe because concurrrent
    // ops are guaranteed to not affect the same state based on locks
    // taken before starting the op (e.g. row locks, schema locks).
    KLOG_EVERY_N(INFO, 10) <<
        Substitute("Tried to move back new op lower bound from $0 to $1. "
                   "Current Snapshot: $2", new_op_timestamp_exc_lower_bound_.ToString(),
                   timestamp.ToString(), cur_snap_.ToString());
    return;
  }

  AdjustCleanTimeUnlocked();
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

void MvccManager::Close() {
  open_.store(false);
  std::lock_guard<LockType> l(lock_);
  auto iter = waiters_.begin();
  while (iter != waiters_.end()) {
    auto* waiter = *iter;
    iter = waiters_.erase(iter);
    waiter->latch->CountDown();
  }
}

void MvccManager::AdjustCleanTimeUnlocked() {
  DCHECK(lock_.is_locked());

  // There are two possibilities:
  //
  // 1) We still have an in-flight op earlier than
  //    'new_op_timestamp_exc_lower_bound_'. In this case, we update the
  //    watermark to that op's timestamp.
  //
  // 2) There are no in-flight ops earlier than
  //    'new_op_timestamp_exc_lower_bound_'. In this case, we update the
  //    watermark to that lower bound, since we know that no new ops
  //    can start with an earlier timestamp.
  //    NOTE: there may still be in-flight ops with future timestamps
  //    due to commit-wait ops which start in the future.
  //
  // In either case, we have to add the newly applied ts only if it remains
  // higher than the new watermark.

  if (earliest_op_in_flight_ < new_op_timestamp_exc_lower_bound_) {
    cur_snap_.all_applied_before_ = earliest_op_in_flight_;
  } else {
    cur_snap_.all_applied_before_ = new_op_timestamp_exc_lower_bound_;
  }

  DVLOG(4) << "Adjusted clean time to: " << cur_snap_.all_applied_before_;

  // Filter out any applied timestamps that now fall below the watermark
  FilterTimestamps(&cur_snap_.applied_timestamps_, cur_snap_.all_applied_before_.value());

  // If the current snapshot doesn't have any applied timestamps, then make sure we still
  // advance the 'none_applied_at_or_after_' watermark so that it never falls below
  // 'all_applied_before_'.
  if (cur_snap_.applied_timestamps_.empty()) {
    cur_snap_.none_applied_at_or_after_ = cur_snap_.all_applied_before_;
  }

  // it may also have unblocked some waiters.
  // Check if someone is waiting for ops to be applied.
  if (PREDICT_FALSE(!waiters_.empty())) {
    auto iter = waiters_.begin();
    while (iter != waiters_.end()) {
      auto* waiter = *iter;
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
               "wait_for", wait_for == ALL_APPLIED ? "all_applied" : "none_applying",
               "ts", ts.ToUint64());

  // If MVCC is closed, there's no point in waiting.
  RETURN_NOT_OK(CheckOpen());
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
    // If the wait ended because MVCC is shutting down, return an error.
    return CheckOpen();
  }
  // We timed out. We need to clean up our entry in the waiters_ array.

  std::lock_guard<LockType> l(lock_);
  // It's possible that we got notified while we were re-acquiring the lock. In
  // that case, we have no cleanup to do.
  if (waiting_state.latch->count() == 0) {
    return CheckOpen();
  }

  // TODO(awong): the distinction here seems nuanced. Do we actually need to
  // wait for clean-time advancement, or can we wait to finish applying ops?
  waiters_.erase(std::find(waiters_.begin(), waiters_.end(), &waiting_state));
  return Status::TimedOut(Substitute("Timed out waiting for all ops with ts < $0 to $1",
                                     ts.ToString(),
                                     wait_for == ALL_APPLIED ?
                                     "finish applying and guarantee no earlier applying ops" :
                                     "finish applying"));
}

bool MvccManager::IsDoneWaitingUnlocked(const WaitingState& waiter) const {
  switch (waiter.wait_for) {
    case ALL_APPLIED:
      return AreAllOpsAppliedUnlocked(waiter.timestamp);
    case NONE_APPLYING:
      return !AnyApplyingAtOrBeforeUnlocked(waiter.timestamp);
  }
  LOG(FATAL); // unreachable
}

Status MvccManager::CheckOpen() const {
  if (PREDICT_TRUE(open_.load())) {
    return Status::OK();
  }
  return Status::Aborted("MVCC is closed");
}

bool MvccManager::AreAllOpsAppliedUnlocked(Timestamp ts) const {
  // If ts is before the 'all_applied_before_' watermark on the current snapshot then
  // all ops before it are applied.
  if (ts < cur_snap_.all_applied_before_) return true;

  // We might not have moved 'cur_snap_.all_applied_before_' (the clean time) but 'ts'
  // might still come before any possible in-flights.
  return ts < earliest_op_in_flight_;
}

bool MvccManager::AnyApplyingAtOrBeforeUnlocked(Timestamp ts) const {
  // TODO(todd) this is not actually checking on the applying ops, it's checking on
  // _all in-flight_. Is this a bug?
  for (const InFlightOpsMap::value_type entry : ops_in_flight_) {
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

Status MvccManager::WaitForSnapshotWithAllApplied(Timestamp timestamp,
                                                  MvccSnapshot* snapshot,
                                                  const MonoTime& deadline) const {
  TRACE_EVENT0("tablet", "MvccManager::WaitForSnapshotWithAllApplied");

  RETURN_NOT_OK(WaitUntil(ALL_APPLIED, timestamp, deadline));
  *snapshot = MvccSnapshot(timestamp);
  return Status::OK();
}

Status MvccManager::WaitForApplyingOpsToApply() const {
  TRACE_EVENT0("tablet", "MvccManager::WaitForApplyingOpsToApply");
  RETURN_NOT_OK(CheckOpen());

  // Find the highest timestamp of an APPLYING op.
  Timestamp wait_for = Timestamp::kMin;
  {
    std::lock_guard<LockType> l(lock_);
    for (const auto& entry : ops_in_flight_) {
      if (entry.second == APPLYING) {
        wait_for = Timestamp(std::max(entry.first, wait_for.value()));
      }
    }
  }

  // Wait until there are no ops applying with that timestamp or below. It's
  // possible that we're a bit conservative here - more ops may enter the
  // APPLYING set while we're waiting, but we will eventually succeed.
  if (wait_for == Timestamp::kMin) {
    // None were APPLYING: we can just return.
    return Status::OK();
  }
  return WaitUntil(NONE_APPLYING, wait_for, MonoTime::Max());
}

Timestamp MvccManager::GetCleanTimestamp() const {
  std::lock_guard<LockType> l(lock_);
  return cur_snap_.all_applied_before_;
}

void MvccManager::GetApplyingOpsTimestamps(std::vector<Timestamp>* timestamps) const {
  std::lock_guard<LockType> l(lock_);
  timestamps->reserve(ops_in_flight_.size());
  for (const auto& entry : ops_in_flight_) {
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
  : type_(MvccSnapshot::kTimestamp),
    all_applied_before_(Timestamp::kInitialTimestamp),
    none_applied_at_or_after_(Timestamp::kInitialTimestamp) {
}

MvccSnapshot::MvccSnapshot(const MvccManager& manager) {
  manager.TakeSnapshot(this);
}

MvccSnapshot::MvccSnapshot(const Timestamp& timestamp)
  : type_(MvccSnapshot::kTimestamp),
    all_applied_before_(timestamp),
    none_applied_at_or_after_(timestamp) {
}

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingAllOps() {
  return MvccSnapshot(Timestamp::kMax);
}

MvccSnapshot MvccSnapshot::CreateSnapshotIncludingNoOps() {
  return MvccSnapshot(Timestamp::kMin);
}

bool MvccSnapshot::IsAppliedFallback(const Timestamp& timestamp) const {
  for (const Timestamp::val_type& v : applied_timestamps_) {
    if (v == timestamp.value()) return true;
  }

  return false;
}

bool MvccSnapshot::MayHaveAppliedOpsAtOrAfter(const Timestamp& timestamp) const {
  return timestamp < none_applied_at_or_after_;
}

bool MvccSnapshot::MayHaveNonAppliedOpsAtOrBefore(const Timestamp& timestamp) const {
  // The snapshot may have nonapplied ops before 'timestamp' if:
  // - 'all_applied_before_' comes before 'timestamp'
  // - 'all_applied_before_' is precisely 'timestamp' but 'timestamp' isn't in the
  //   applied set.
  return timestamp > all_applied_before_ ||
      (timestamp == all_applied_before_ && !IsAppliedFallback(timestamp));
}

std::string MvccSnapshot::ToString() const {
  return Substitute("MvccSnapshot[applied={T|T < $0$1}]", all_applied_before_.ToString(),
      applied_timestamps_.empty() ?  "" :
          Substitute(" or (T in {$0})", JoinInts(applied_timestamps_, ",")));
}

void MvccSnapshot::AddAppliedTimestamps(const std::vector<Timestamp>& timestamps) {
  for (const Timestamp& ts : timestamps) {
    AddAppliedTimestamp(ts);
  }
}

void MvccSnapshot::AddAppliedTimestamp(Timestamp timestamp) {
  DCHECK_EQ(kLatest, type_);
  if (IsApplied(timestamp)) return;

  applied_timestamps_.push_back(timestamp.value());

  // If this is a new upper bound apply mark, update it.
  if (none_applied_at_or_after_ <= timestamp) {
    none_applied_at_or_after_ = Timestamp(timestamp.value() + 1);
  }
}

bool MvccSnapshot::Equals(const MvccSnapshot& other) const {
  if (type_ != other.type_) {
    return false;
  }
  if (all_applied_before_ != other.all_applied_before_) {
    return false;
  }
  if (none_applied_at_or_after_ != other.none_applied_at_or_after_) {
    return false;
  }
  return applied_timestamps_ == other.applied_timestamps_;
}

////////////////////////////////////////////////////////////
// ScopedOp
////////////////////////////////////////////////////////////
ScopedOp::ScopedOp(MvccManager* manager, Timestamp timestamp)
  : done_(false),
    manager_(DCHECK_NOTNULL(manager)),
    timestamp_(timestamp) {
  manager_->StartOp(timestamp);
}

ScopedOp::~ScopedOp() {
  if (!done_) {
    Abort();
  }
}

void ScopedOp::StartApplying() {
  manager_->StartApplyingOp(timestamp_);
}

void ScopedOp::FinishApplying() {
  manager_->FinishApplyingOp(timestamp_);
  done_ = true;
}

void ScopedOp::Abort() {
  manager_->AbortOp(timestamp_);
  done_ = true;
}

} // namespace tablet
} // namespace kudu
