// Copyright 2010 Google, Inc.
// All rights reserved.

// The OS-specific header included below must provide two calls:
// base::subtle::SpinLockDelay() and base::subtle::SpinLockWake().
// See spinlock_wait.h for the specs.

#include "gutil/spinlock_wait.h"

// forward declaration for use by spinlock_*-inl.h
namespace base { namespace subtle { static int SuggestedDelayNS(int loop); }}

#if defined(OS_WINDOWS)
#include "gutil/spinlock_win32-inl.h"
#elif defined(OS_LINUX)
#include "gutil/spinlock_linux-inl.h"
#else
#include "gutil/spinlock_posix-inl.h"
#endif

namespace base {
namespace subtle {

// See spinlock_wait.h for spec.
int32 SpinLockWait(volatile Atomic32 *w, int n,
                   const SpinLockWaitTransition trans[]) {
  int32 v;
  bool done = false;
  for (int loop = 0; !done; loop++) {
    v = base::subtle::Acquire_Load(w);
    int i;
    for (i = 0; i != n && v != trans[i].from; i++) {
    }
    if (i == n) {
      SpinLockDelay(w, v, loop);     // no matching transition
    } else if (trans[i].to == v ||   // null transition
               base::subtle::Acquire_CompareAndSwap(w, v, trans[i].to) == v) {
      done = trans[i].done;
    }
  }
  return v;
}

// Return a suggested delay in nanoseconds for iteration number "loop"
static int SuggestedDelayNS(int loop) {
  // Weak pseudo-random number generator to get some spread between threads
  // when many are spinning.
  static base::subtle::Atomic64 rand;
  uint64 r = base::subtle::NoBarrier_Load(&rand);
  r = 0x5deece66dLL * r + 0xb;   // numbers from nrand48()
  base::subtle::NoBarrier_Store(&rand, r);

  r <<= 16;   // 48-bit random number now in top 48-bits.
  if (loop < 0 || loop > 32) {   // limit loop to 0..32
    loop = 32;
  }
  // loop>>3 cannot exceed 4 because loop cannot exceed 32.
  // Select top 20..24 bits of lower 48 bits,
  // giving approximately 0ms to 16ms.
  // Mean is exponential in loop for first 32 iterations, then 8ms.
  // The futex path multiplies this by 16, since we expect explicit wakeups
  // almost always on that path.
  return r >> (44 - (loop >> 3));
}

} // namespace subtle
} // namespace base
