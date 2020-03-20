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

#include "kudu/gutil/macros.h"

#ifdef __linux__
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/linux_syscall_support.h"
#else
#include "kudu/util/countdown_latch.h"
#endif

namespace kudu {

// This class defines a `Notification` abstraction, which allows threads
// to receive notification of a single occurrence of a single event.
//
// NOTE: this class is modeled after absl::Notification but re-implemented
// to not have dependencies on other absl-specific code. If absl is ever
// imported, this can be removed.
//
// The `Notification` object maintains a private boolean "notified" state that
// transitions to `true` at most once. The `Notification` class provides the
// following primary member functions:
//   * `HasBeenNotified() `to query its state
//   * `WaitForNotification*()` to have threads wait until the "notified" state
//      is `true`.
//   * `Notify()` to set the notification's "notified" state to `true` and
//     notify all waiting threads that the event has occurred.
//     This method may only be called once.
//
// Note that while `Notify()` may only be called once, it is perfectly valid to
// call any of the `WaitForNotification*()` methods multiple times, from
// multiple threads -- even after the notification's "notified" state has been
// set -- in which case those methods will immediately return.
//
// Note that the lifetime of a `Notification` requires careful consideration;
// it might not be safe to destroy a notification after calling `Notify()` since
// it is still legal for other threads to call `WaitForNotification*()` methods
// on the notification. However, observers responding to a "notified" state of
// `true` can safely delete the notification without interfering with the call
// to `Notify()` in the other thread.
//
// Memory ordering: For any threads X and Y, if X calls `Notify()`, then any
// action taken by X before it calls `Notify()` is visible to thread Y after:
//  * Y returns from `WaitForNotification()`, or
//  * Y receives a `true` return value from `HasBeenNotified()`.
#ifdef __linux__
class Notification {
 public:
  Notification() : state_(NOT_NOTIFIED_NO_WAITERS) {}
  ~Notification() = default;

  bool HasBeenNotified() const {
    return base::subtle::Acquire_Load(&state_) == NOTIFIED;
  }

  void WaitForNotification() const {
    while (true) {
      auto s = base::subtle::Acquire_Load(&state_);
      if (s == NOT_NOTIFIED_NO_WAITERS) {
        s = base::subtle::Acquire_CompareAndSwap(
            &state_, NOT_NOTIFIED_NO_WAITERS, NOT_NOTIFIED_HAS_WAITERS);
        if (s == NOT_NOTIFIED_NO_WAITERS) {
          // We succeeded in the CAS -- sets 's' to be the new value of the
          // state rather than the previous value.
          s = NOT_NOTIFIED_HAS_WAITERS;
        }
      }
      if (s == NOTIFIED) return;
      DCHECK_EQ(s, NOT_NOTIFIED_HAS_WAITERS);
      sys_futex(&state_, FUTEX_WAIT | FUTEX_PRIVATE_FLAG, NOT_NOTIFIED_HAS_WAITERS,
          /* timeout */ nullptr, nullptr /* ignored */, 0 /* ignored */);
    }
  }

  void Notify() {
    auto s = base::subtle::Release_AtomicExchange(&state_, NOTIFIED);
    DCHECK_NE(s, NOTIFIED) << "may only notify once";
    if (s == NOT_NOTIFIED_HAS_WAITERS) {
      sys_futex(&state_, FUTEX_WAKE | FUTEX_PRIVATE_FLAG, INT_MAX,
          nullptr /* ignored */, nullptr /* ignored */, 0 /* ignored */);
    }
  }

 private:
  enum {
    NOT_NOTIFIED_NO_WAITERS = 1,
    NOT_NOTIFIED_HAS_WAITERS = 2,
    NOTIFIED = 3
  };
  mutable Atomic32 state_;

  DISALLOW_COPY_AND_ASSIGN(Notification);
};
#else
// macOS doesn't have futex, so we just use the mutex-based latch instead.
class Notification {
 public:
  Notification() : latch_(1) { }
  ~Notification() = default;

  bool HasBeenNotified() const {
    return latch_.count() == 0;
  }

  void WaitForNotification() const {
    latch_.Wait();
  }

  void Notify() {
    latch_.CountDown();
  }

 private:
  mutable CountDownLatch latch_;

  DISALLOW_COPY_AND_ASSIGN(Notification);
};

#endif
} // namespace kudu
