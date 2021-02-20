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

#include <cstdint>
#include <memory>
#include <vector>

#include "kudu/common/txn_id.h"
#include "kudu/gutil/macros.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/semaphore.h"
#include "kudu/util/slice.h"

namespace kudu {

template <typename T>
class ArrayView;

namespace tablet {

class LockEntry;
class LockTable;
class OpState;
class PartitionLockState;

// Lock manager implementation that only supports exclusive locks. It is
// composed of two types of lock: 'ScopedRowLock' for single-row lock and
// 'ScopedPartitionLock' for partition lock.
//
// For deadlock prevention of multi-row transactions, a wait-die scheme is
// used. When a transaction B attempts to take a lock that's already held by
// transaction A:
// - B > A: transaction B should be aborted, and applications should retry
//   B at later time, signified by the returning of TXN_LOCKED_ABORT.
// - B < A: transaction B should wait for the lock to be taken by retrying the
//   transactional op, signified by the returning of TXN_LOCKED_RETRY_OP.
class LockManager {
 public:
  LockManager();
  ~LockManager();

  enum LockMode {
    LOCK_EXCLUSIVE
  };

  enum LockWaitMode {
    // The attempt to take the lock must wait until the lock is taken. This is
    // only appropriate if it is guaranteed that the lock can be waited on
    // without a deadlock.
    WAIT_FOR_LOCK,

    // Try to acquire the lock with a time out, if not available return without
    // acquiring it.
    TRY_LOCK,
  };

 int64_t partition_lock_refs() const { return partition_lock_refs_; }

 private:
  friend class ScopedPartitionLock;
  friend class ScopedRowLock;
  friend class LockManagerTest;

  std::vector<LockEntry*> LockBatch(ArrayView<Slice> keys, const OpState* op);

  bool TryLock(const Slice& key, const OpState* op, LockEntry** entry);
  void Release(LockEntry* lock);
  void ReleaseBatch(ArrayView<LockEntry*> locks);

  // Tries to acquire the partition lock with the given txn ID with the given
  // timeout, or tries indefinitely if no timeout is set. A partition lock can
  // only be held by a single transaction at a time; the same transaction can
  // acquire the lock multiple times. Both transactional and non-transactional
  // ops must try to acquire the lock (non-transactional ops are signified with
  // an invalid 'txn_id').
  //
  // If the attempt to lock fails, an appropriate error code is returned based
  // on the transaction ID and the deadlock prevention policy described above.
  PartitionLockState* TryAcquirePartitionLock(const TxnId& txn_id,
                                              tserver::TabletServerErrorPB::Code* code,
                                              const MonoDelta& timeout = MonoDelta());

  // Similar to the above, but waits until the lock is acquired.
  //
  // Note that the caller is expected to ensure there is no deadlock. For
  // example, when running on followers in the prepare phase, or running
  // serially in an order that has already been successful with
  // TryAcquirePartitionLock() calls.
  PartitionLockState* WaitUntilAcquiredPartitionLock(const TxnId& txn_id);
  void ReleasePartitionLock();

  static void AcquireLockOnEntry(LockEntry* e, const OpState* op);

  // Semaphore used by the LockManager to signal the release of the partition
  // lock. If its value is >= 0, the partition lock is already held, and
  // callers can use TimedAcquire() to wait for it to be released.
  Semaphore partition_sem_;

  // Lock to protect 'partition_lock_' and 'partition_lock_refs_'.
  simple_spinlock p_lock_;

  // If 'partition_lock_' has been held by a transaction,
  // 'partition_lock_refs_' keeps track of the number of times that the
  // partition lock has been held by that transaction.
  //
  // NOTE: 'partition_lock_' is only set to non-null by a single thread (i.e.
  // the prepare thread), but it may be released from a different thread (e.g.
  // an apply thread).
  int64_t partition_lock_refs_;
  std::unique_ptr<PartitionLockState> partition_lock_;

  LockTable *locks_;

  DISALLOW_COPY_AND_ASSIGN(LockManager);
};

// Hold a lock on a set of rows, for the scope of this object.
// Usage:
//   {
//     ScopedRowLock(&manager, op, my_encoded_row_keys, LOCK_EXCLUSIVE);
//     .. do stuff with the row ..
//   }
//   // lock is released when the object exits its scope.
//
// This class implements C++11 move constructors and thus can be
// transferred around using std::move(). For example:
//
// void DoSomething(ScopedRowLock l) {
//   // l owns the lock and will release at the end of this function
// }
// ScopedRowLock my_lock(&manager, ...);
// DoSomething(std::move(l);
// CHECK(!l.acquired()); // doesn't own lock anymore, since it moved
class ScopedRowLock {
 public:

  // Construct an initially-unlocked lock holder.
  // You can later assign this to actually hold a lock using
  // the move-constructor:
  //   ScopedRowLock l;
  //   l = ScopedRowLock(...);
  // or
  //   l = std::move(other_row_lock);
  ScopedRowLock() {}

  // Lock rows in the given LockManager. The 'key' slices must remain
  // valid and un-changed for the duration of this object's lifetime.
  ScopedRowLock(LockManager* manager,
                const OpState* op,
                ArrayView<Slice> keys,
                LockManager::LockMode mode);

  // Move constructor and assignment.
  ScopedRowLock(ScopedRowLock&& other) noexcept;
  ScopedRowLock& operator=(ScopedRowLock&& other) noexcept;

  void Release();

  bool acquired() const { return !entries_.empty(); }

  ~ScopedRowLock();

 private:
  void TakeState(ScopedRowLock* other);

  LockManager* manager_ = nullptr;

  std::vector<LockEntry*> entries_;
};

// Similar to ScopedRowLock, hold a lock on a partition, for the scope of
// this object. Usage:
//  {
//    ScopedPartitionLock(&manager, txn_id);
//    .. do stuff ..
//  }
//  // lock is released when the object exits its scope.
class ScopedPartitionLock {
 public:
  // Construct an initially-unlocked lock holder.
  // You can later assign this to actually hold a lock using
  // the move-constructor:
  //   ScopedPartitionLock l;
  //   l = ScopedPartitionLock(...);
  // or
  //   l = std::move(other_partition_lock);
  ScopedPartitionLock()
      : code_(tserver::TabletServerErrorPB::UNKNOWN_ERROR) {}

  // 'wait_mode' indicates whether or not to wait until
  // the lock is acquired.
  ScopedPartitionLock(LockManager* manager,
      const TxnId& txn_id,
      LockManager::LockWaitMode wait_mode = LockManager::TRY_LOCK);
  ~ScopedPartitionLock();

  // Move constructor and assignment operator.
  ScopedPartitionLock(ScopedPartitionLock&& other) noexcept;
  ScopedPartitionLock& operator=(ScopedPartitionLock&& other) noexcept;

  // Disable the copy constructor.
  ScopedPartitionLock(const ScopedPartitionLock&) = delete;

  // Check whether the partition lock is acquired by the transaction.
  // If false, set the tablet server error code accordingly to abort
  // or retry the transaction. Otherwise, no error code is set.
  bool IsAcquired(tserver::TabletServerErrorPB::Code* code) const;

  // Release a reference of the partition lock held by the transaction.
  void Release();

 private:
  void TakeState(ScopedPartitionLock* other);

  LockManager* manager_ = nullptr;
  PartitionLockState* lock_state_ = nullptr;
  // The tablet server error code is only set when the lock
  // is not acquired. Otherwise, the default is 'UNKNOWN_ERROR'.
  tserver::TabletServerErrorPB::Code code_;
};

} // namespace tablet
} // namespace kudu
