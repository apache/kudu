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
#pragma once

#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/timestamp.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class CountDownLatch;
namespace clock {
class Clock;
}  // namespace clock

namespace consensus {
class ReplicateMsg;

// Manages timestamp assignment to consensus rounds and safe time advancement.
//
// The TimeManager is used in conjunction with the MvccManager to define and
// uphold the safe time, i.e. a timestamp before which all ops have been
// applied to the tablet or are in-flight, which is useful for enabling
// repeatable reads.
//
// Example of a leader TimeManager updating the safe time:
// - TimeManager::AssignTimestamp() returns a timestamp T that is higher than
//   any other replicated by a leader.
// - An op is registered with the MvccManager with the timestamp T.
//   Conceptually, T is thusly considered safe. Since all future ops will be
//   assigned higher timestamps, all ops that would have assigned a lower
//   timestamps are already complete or in-flight.
// - The op is written to the WAL via the ConsensusQueue, and then replicated;
//   TimeManger::AdvanceSafeTimeWithMessage() is called with the replicate
//   message of the op with timestamp T, signifying the new safe time.
//   - Until this step completes, snapshot scans, which call WaitUntilSafe(t)
//     where t >= T, will block.
//
// On followers, safe time is advanced via the following methods, called from
// the Raft threads:
// - MessageReceivedFromLeader() is used by followers on replicate messages
//   received by the leader to ensure that, if the follower were to become
//   leader, any timestamps assigned would be higher than any previously
//   replicated timestamps. This does not bump safe time, but it does ensure
//   that, were this node to become leader, any later assigned timestamp will
//   be higher than T.
// - AdvanceSafeTime() is used by followers using the safe timestamps that are
//   heartbeated from leader, used for the sake of snapshot scans.
//
// The leader's safe time moves with its clock unless there has been a op that
// was assigned a timestamp that is not yet known by the queue (i.e.
// AdvanceSafeTimeWithMessage() hasn't been called on the corresponding
// message). In this case the TimeManager returns the last known safe time.
//
// This class's leadership status is meant to be in tune with the queue's as
// the queue is responsible for broadcasting safe time from a leader (and will
// eventually be responsible for calculating that leader's lease).
//
// NOTE: Until leader leases are implemented the cluster's safe time can
// occasionally move back.  This does not mean, however, that the timestamp
// returned by GetSafeTime() can move back.  GetSafeTime will still return
// monotonically increasing timestamps, it's just that, in certain corner
// cases, the timestamp returned by GetSafeTime() can't be trusted to mean that
// all future messages will be assigned future timestamps.  This anomaly can
// cause non-repeatable reads in certain conditions.
//
// Multi-op transactions
// ---------------------
// Transaction participant leaders also help orchestrate the assignment of its
// commit timestamp. Below is an example of how the TimeManager and MvccManager
// can be used to handle assignment of a commit timestamp:
// - The BEGIN_COMMIT op is assigned a timestamp T_bc by the participant
//   leader via the above steps, denoting T_bc as safe.
// - Unlike a regular (e.g. write) op, the MVCC op registered for BEGIN_COMMIT
//   is not finished when the op is applied -- instead, the MVCC op is
//   maintained in memory for the time being.
//   - Until the MVCC op is completed below, further snapshot scans at t where
//     t > T_bc will block.
// - T_bc is sent to the transaction coordinator, and a commit timestamp
//   T_commit is determined that is higher than all timestamps that were
//   returned by the participants.
// - The coordinator effects a FINALIZE_COMMIT op on participants; the request
//   contains T_commit.
// - Before starting the FINALIZE_COMMIT op, the leader updates its clock using
//   TimeManager::UpdateClockAndLastAssignedTimestamp(), guaranteeing the next
//   timestamp assigned will be higher than T_commit.
// - The FINALIZE_COMMIT op is assigned a timestamp T_fc > T_commit by the
//   participant leader via the above steps, denoting T_fc as safe.
// - The MVCC op from earlier is used in lieu of a new MVCC op; since this op
//   has been in-flight for the duration of the commit process, any scans at
//   time t >= T_commit must have called WaitUntilSafe(t), followed by
//   MvccManager::WaitForSnapshotWithAllCommitted(t), thereby waiting for all
//   ops below t to complete.
//
// This class is thread safe.
class TimeManager {
 public:

  // Constructs a TimeManager in non-leader mode.
  TimeManager(clock::Clock* clock, Timestamp initial_safe_time);

  // Sets this TimeManager to leader mode.
  void SetLeaderMode();

  // Sets this TimeManager to non-leader mode.
  void SetNonLeaderMode();

  // Assigns a timestamp to 'message' according to the message's
  // ExternalConsistencyMode and/or message type.
  //
  // The timestamp in 'message' is not considered safe until the message has
  // been written to the WAL and begun replicating to followers.  Until then,
  // safe time is pinned to the last known value.  When the message is appended
  // later on, AdvanceSafeTimeWithMessage() is called and safe time is
  // advanced.
  //
  // Requires Leader mode (non-OK status otherwise).
  Status AssignTimestamp(ReplicateMsg* message);

  // Updates the internal state based on 'message' received from a leader
  // replica.  Replicas are expected to call this for every message received
  // from a valid leader.
  //
  // Returns Status::OK if the message/leader is valid and the clock was
  // correctly updated.
  //
  // Requires non-leader mode (CHECK failure if it isn't).
  Status MessageReceivedFromLeader(const ReplicateMsg& message);

  // Updates the clock to move forward to 'timestamp' if it is in the future,
  // and updates internal state to indicate that all further timestamps
  // assigned should be higher than 'timestamp'.
  //
  // It is expected that leader participants of a transaction call this before
  // assigning a timestamp to the op that finalizes the commit, ensuring that
  // the finalizing op will be assigned a later timestamp.
  //
  // Returns a not-OK status if called while not the leader or if there is an
  // error updating the clock ('timestamp' was too far in the future).
  Status UpdateClockAndLastAssignedTimestamp(const Timestamp& timestamp);

  // Advances safe time based on the timestamp and type of 'message'.
  //
  // This only moves safe time if 'message's timestamp is higher than the currently known one.
  //
  // Allowed in both leader and non-leader modes.
  void AdvanceSafeTimeWithMessage(const ReplicateMsg& message);

  // Same as above but for a specific timestamp.
  //
  // This only moves safe time if 'safe_time' is higher than the currently known one.
  //
  // Requires non-leader mode (CHECK failure if it isn't).
  void AdvanceSafeTime(Timestamp safe_time);

  // Waits until 'timestamp' is less than or equal to safe time or until 'deadline' has elapsed.
  //
  // Returns Status::OK() if it safe time advanced past 'timestamp' before 'deadline'
  // Returns Status::TimeOut() if deadline elapsed without safe time moving enough.
  // Returns Status::ServiceUnavailable() is the request should be retried somewhere else.
  Status WaitUntilSafe(Timestamp timestamp, const MonoTime& deadline);

  // Returns the current safe time.
  //
  // In leader mode returns clock_->Now() or some value close to it.
  //
  // In non-leader mode returns the last safe time received from a leader.
  Timestamp GetSafeTime();

  // Returns a timestamp that is guaranteed to be higher than all other timestamps
  // that have been assigned by calls to GetSerialTimestamp() (in this or another
  // replica).
  Timestamp GetSerialTimestamp();

 private:
  FRIEND_TEST(TimeManagerTest, TestTimeManagerNonLeaderMode);
  FRIEND_TEST(TimeManagerTest, TestTimeManagerLeaderMode);

  // Returns whether we've advanced safe time recently.
  // If this returns false we might be partitioned or there might be election churn.
  // The client should try again.
  // If this returns false, sets error information in 'error_message'.
  bool HasAdvancedSafeTimeRecentlyUnlocked(std::string* error_message);

  // Returns whether safe time is lagging too much behind 'timestamp' and the client
  // should be forced to retry.
  // If this returns true, sets error information in 'error_message'.
  bool IsSafeTimeLaggingUnlocked(Timestamp timestamp, std::string* error_message);

  // Helper to build the final error message of WaitUntilSafe().
  void MakeWaiterTimeoutMessageUnlocked(Timestamp timestamp, std::string* error_message);

  // Helper to return the external consistency mode of 'message'.
  static ExternalConsistencyMode GetMessageConsistencyMode(const ReplicateMsg& message);

  // The mode of this TimeManager.
  enum Mode {
    LEADER,
    NON_LEADER
  };

  // State for waiters.
  struct WaitingState {
    // The timestamp the waiter requires be safe.
    Timestamp timestamp;
    // Latch that will be count down once 'timestamp' if safe, unblocking the waiter.
    CountDownLatch* latch;
  };

  // Returns whether 'timestamp' is safe.
  // Requires that we've waited for the local clock to move past 'timestamp'.
  bool IsTimestampSafe(Timestamp timestamp);

  // Internal, unlocked implementation of IsTimestampSafe().
  bool IsTimestampSafeUnlocked(Timestamp timestamp);

  // Advances safe time and wakes up any waiters.
  void AdvanceSafeTimeAndWakeUpWaitersUnlocked(Timestamp safe_time);

  // Internal, unlocked implementation of GetSerialTimestamp().
  Timestamp GetSerialTimestampUnlocked();

  // Like GetSerialTimestamp(), but returns a serial timestamp plus the maximum error.
  // NOTE: GetSerialTimestamp() might still return timestamps that are smaller.
  Timestamp GetSerialTimestampPlusMaxError();

  // Internal, unlocked implementation of GetSafeTime().
  Timestamp GetSafeTimeUnlocked();

  // Lock to protect the non-const fields below.
  mutable simple_spinlock lock_;

  // Vector of waiters to be notified when the safe time advances.
  std::vector<WaitingState*> waiters_;

  // The last serial timestamp that was assigned.
  Timestamp last_serial_ts_assigned_;

  // On followers this is the latest safe time received from the leader, on the
  // leader this is the last serial timestamp appended to the queue.
  Timestamp last_safe_ts_;

  // The last time we advanced safe time.
  // Used in the decision of whether we should have waiters wait or try again.
  MonoTime last_advanced_safe_time_;

  // The current mode of the TimeManager.
  Mode mode_;

  clock::Clock* clock_;
  const std::string local_peer_uuid_;
};

} // namespace consensus
} // namespace kudu
