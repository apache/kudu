#ifndef BASE_SPINLOCK_WAIT_H_
#define BASE_SPINLOCK_WAIT_H_
// Copyright 2010 Google, Inc.
// All rights reserved.

// Operations to make atomic transitions on a word, and to allow
// waiting for those tranistions to become possible.

// This file is used internally in spinlock.cc and once.cc, and a few other
// places listing in //base:spinlock_wait_users.  If you need to use it outside
// of //base, please request permission to be added to that list.

#include "gutil/integral_types.h"
#include "gutil/atomicops.h"

namespace base {
namespace subtle {

// SpinLockWait() waits until it can perform one of several transitions from
// "from" to "to".  It returns when it performs a transition where done==true.
struct SpinLockWaitTransition {
  int32 from;
  int32 to;
  bool done;
};

// Wait until *w can transition from trans[i].from to trans[i].to for some i
// satisfying 0<=i<n && trans[i].done, atomically make the transition,
// then return the old value of *w.   Make any other atomic tranistions
// where !trans[i].done, but continue waiting.
int32 SpinLockWait(volatile Atomic32 *w, int n,
                   const SpinLockWaitTransition trans[]);

// If possible, wake some thread that has called SpinLockDelay(w, ...). If
// "all" is true, wake all such threads.  This call is a hint, and on some
// systems it may be a no-op; threads calling SpinLockDelay() will always wake
// eventually even if SpinLockWake() is never called.
void SpinLockWake(volatile Atomic32 *w, bool all);

// Wait for an apprproate spin delay on iteration "loop" of a
// spin loop on location *w, whose previously observed value was "value".
// SpinLockDelay() may do nothing, may yield the CPU, may sleep a clock tick,
// or may wait for a delay that can be truncated by a call to SpinlockWake(w).
// In all cases, it must return in bounded time even if SpinlockWake() is not
// called.
void SpinLockDelay(volatile Atomic32 *w, int32 value, int loop);

} // namespace subtle
} // namespace base
#endif
