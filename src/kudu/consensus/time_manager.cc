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

#include <mutex>
#include <gflags/gflags.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/flag_tags.h"

DEFINE_bool(safe_time_advancement_without_writes, true,
            "Whether to enable the advancement of \"safe\" time in "
            "the absense of write operations");
TAG_FLAG(safe_time_advancement_without_writes, advanced);

namespace kudu {
namespace consensus {

using server::Clock;
using strings::Substitute;

typedef std::lock_guard<simple_spinlock> Lock;

ExternalConsistencyMode TimeManager::GetMessageConsistencyMode(const ReplicateMsg& message) {
  // TODO(dralves): We should have no-ops (?) and config changes be COMMIT_WAIT
  // transactions. See KUDU-798.
  // TODO(dralves) Move external consistency mode to ReplicateMsg. This will be useful
  // for consistent alter table ops.
  if (PREDICT_TRUE(message.has_write_request())) {
    return message.write_request().external_consistency_mode();
  }
  return CLIENT_PROPAGATED;
}

TimeManager::TimeManager(scoped_refptr<Clock> clock, Timestamp initial_safe_time)
  : last_serial_ts_assigned_(initial_safe_time),
    last_safe_ts_(initial_safe_time),
    mode_(NON_LEADER),
    clock_(std::move(clock)) {}

void TimeManager::SetLeaderMode() {
  Lock l(lock_);
  mode_ = LEADER;
  AdvanceSafeTimeAndWakeUpWaitersUnlocked(clock_->Now());
}

void TimeManager::SetNonLeaderMode() {
  Lock l(lock_);
  mode_ = NON_LEADER;
}

Status TimeManager::AssignTimestamp(ReplicateMsg* message) {
  Lock l(lock_);
  if (PREDICT_FALSE(mode_ == NON_LEADER)) {
    return Status::IllegalState("Cannot assign timestamp. TimeManager is not in Leader mode.");
  }
  Timestamp t;
  switch (GetMessageConsistencyMode(*message)) {
    case COMMIT_WAIT: t = GetSerialTimestampPlusMaxError(); break;
    case CLIENT_PROPAGATED:  t = GetSerialTimestamp(); break;
    default: return Status::NotSupported("Unsupported external consistency mode.");
  }
  message->set_timestamp(t.value());
  return Status::OK();
}

Status TimeManager::MessageReceivedFromLeader(const ReplicateMsg& message) {
  // NOTE: Currently this method just updates the clock and stores the message's timestamp.
  //       It always returns Status::OK() if the clock returns an OK status on Update().
  //
  //       When we have leader leases we can trust that the timestamps of messages sent by
  //       any valid leader are safe and we could increase safe time here. However, since
  //       this is not yet the case we will only increase safe time later, when the message is
  //       committed, at the cost of additional delay in moving safe time.
  //
  //       This greatly reduces the opportunity for non-repeatable reads. On a busy cluster
  //       with a lot of writes (i.e. no empty, "heartbeat" messages) safe time moves only
  //       with committed message timestamps, which are forcibly 'safe'.
  //
  //       The only opportunity for unrepeatable reads in this setup is if an old leader sends
  //       an (accepted) empty heartbeat message to a follower that immediately afterwards
  //       receives a non-empty message from another higher term leader but with a lower timestamp
  //       than the empty heartbeat.
  DCHECK(message.has_timestamp());
  Timestamp t(message.timestamp());
  RETURN_NOT_OK(clock_->Update(t));
  {
    Lock l(lock_);
    CHECK_EQ(mode_, NON_LEADER) << "Cannot receive messages from a leader in leader mode.";
    if (GetMessageConsistencyMode(message) == CLIENT_PROPAGATED) {
      last_serial_ts_assigned_ = t;
    }
  }
  return Status::OK();
}

void TimeManager::AdvanceSafeTimeWithMessage(const ReplicateMsg& message) {
  Lock l(lock_);
  if (GetMessageConsistencyMode(message) == CLIENT_PROPAGATED) {
    AdvanceSafeTimeAndWakeUpWaitersUnlocked(Timestamp(message.timestamp()));
  }
}

void TimeManager::AdvanceSafeTime(Timestamp safe_time) {
  Lock l(lock_);
  CHECK_EQ(mode_, NON_LEADER) << "Cannot advance safe time by timestamp in leader mode.";;
  AdvanceSafeTimeAndWakeUpWaitersUnlocked(safe_time);
}

Status TimeManager::WaitUntilSafe(Timestamp timestamp, const MonoTime& deadline) {
  // First wait for the clock to be past 'timestamp'.
  RETURN_NOT_OK(clock_->WaitUntilAfterLocally(timestamp, deadline));

  if (PREDICT_FALSE(MonoTime::Now() > deadline)) {
    return Status::TimedOut("Timed out waiting for the local clock.");
  }

  CountDownLatch latch(1);
  WaitingState waiter;
  waiter.timestamp = timestamp;
  waiter.latch = &latch;

  // Register a waiter in waiters_
  {
    Lock l(lock_);
    if (IsTimestampSafeUnlocked(timestamp)) return Status::OK();
    waiters_.push_back(&waiter);
  }

  // Wait until we get notified or 'deadline' elapses.
  if (waiter.latch->WaitUntil(deadline)) return Status::OK();

  // Timed out, clean up.
  {
    Lock l(lock_);
    // Address the case where we were notified after the timeout.
    if (waiter.latch->count() == 0) return Status::OK();

    waiters_.erase(std::find(waiters_.begin(), waiters_.end(), &waiter));
    return Status::TimedOut(Substitute(
        "Timed out waiting for ts: $0 to be safe (mode: $1). "
        "Current safe time: $2 Physical time difference: $3",
        clock_->Stringify(waiter.timestamp),
        (mode_ == LEADER ? "LEADER" : "NON-LEADER"),
        clock_->Stringify(last_safe_ts_),
        (clock_->HasPhysicalComponent() ?
         clock_->GetPhysicalComponentDifference(timestamp, last_safe_ts_).ToString() :
         "None (Logical clock)")));
  }
}

void TimeManager::AdvanceSafeTimeAndWakeUpWaitersUnlocked(Timestamp safe_time) {
  if (safe_time <= last_safe_ts_) {
    return;
  }
  last_safe_ts_ = safe_time;

  if (PREDICT_FALSE(!waiters_.empty())) {
    auto iter = waiters_.begin();
    while (iter != waiters_.end()) {
      WaitingState* waiter = *iter;
      if (IsTimestampSafeUnlocked(waiter->timestamp)) {
        iter = waiters_.erase(iter);
        waiter->latch->CountDown();
        continue;
      }
      iter++;
    }
  }
}

bool TimeManager::IsTimestampSafeUnlocked(Timestamp timestamp) {
  return timestamp <= GetSafeTimeUnlocked();
}

Timestamp TimeManager::GetSafeTime()  {
  Lock l(lock_);
  return GetSafeTimeUnlocked();
}

Timestamp TimeManager::GetSafeTimeUnlocked() {
  switch (mode_) {
    case LEADER: {
      // In ASCII form, where 'S' represents a safe timestamp, 'A' represents the last assigned
      // timestamp, and 'N' represents the current clock value, the internal state can look like
      // the following diagrams (time moves from left to right):
      //
      // a)
      //   SSSSSSSSSSSSSSSSSS N
      //                  |  \- last_safe_ts_
      //                  |
      //                   \- last_serial_ts_assigned_
      // or like:
      // b)
      //   SSSSSSSSSSSSSSSSSS A N
      //                    |  \- last_serial_ts_assigned_
      //                    |
      //                    \- last_safe_ts_
      //
      // If the current internal state is a), then we can advance safe time to 'N'. We know the
      // leader will never assign a new timestamp lower than it.
      if (PREDICT_TRUE(last_serial_ts_assigned_ <= last_safe_ts_)) {
        last_safe_ts_ = clock_->Now();
        return last_safe_ts_;
      }
      // If the current state is b), then there might be transaction with a timestamp that is lower
      // than 'N' in between assignment and being appended to the queue. We can't consider 'N'
      // safe and thus have to return the last known safe timestamp.
      // Note that there can be at most one single transaction in this state, because prepare
      // is single threaded.
      return last_safe_ts_;
    }
    case NON_LEADER:
      return last_safe_ts_;
  }
  __builtin_unreachable(); // silence gcc warnings
}

Timestamp TimeManager::GetSerialTimestamp() {
  last_serial_ts_assigned_ = clock_->Now();
  return last_serial_ts_assigned_;
}

Timestamp TimeManager::GetSerialTimestampPlusMaxError() {
  return clock_->NowLatest();
}


} // namespace consensus
} // namespace kudu
