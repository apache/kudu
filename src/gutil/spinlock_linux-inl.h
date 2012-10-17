// Copyright 2009 Google, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// All rights reserved.

// This file is a Linux-specific part of spinlock_wait.cc

#include <errno.h>
#include <sched.h>
#include <time.h>
#include <limits.h>
#include "base/linux_syscall_support.h"

#define FUTEX_WAIT 0
#define FUTEX_WAKE 1
#define FUTEX_PRIVATE_FLAG 128

// futexes are ints, so we can use them only when
// that's the same size as the lockword_ in SpinLock.
// This is a const, and therefore the generated code should
// not have a corresponding branch when we test for it.
#if defined (__arm__)
static const bool kHaveFutex = 0;
#else
static const bool kHaveFutex = (sizeof(Atomic32) == sizeof(int));
#endif

static int futex_private_flag = FUTEX_PRIVATE_FLAG;

namespace {
static struct InitModule {
  InitModule() {
    int x = 0;
    if (kHaveFutex) {
      if (sys_futex(&x, FUTEX_WAKE | futex_private_flag, 1, 0) < 0) {
        futex_private_flag = 0;
      }
    }
  }
} init_module;

}  // anonymous namespace


namespace base {
namespace subtle {

void SpinLockDelay(volatile Atomic32 *w, int32 value, int loop) {
  if (loop != 0) {
    int save_errno = errno;
    struct timespec tm;
    tm.tv_sec = 0;
    tm.tv_nsec = base::subtle::SuggestedDelayNS(loop);
    if (kHaveFutex) {
      tm.tv_nsec *= 16;  // increase the delay; we expect explicit wakeups
      sys_futex(reinterpret_cast<int *>(const_cast<Atomic32 *>(w)),
                FUTEX_WAIT | futex_private_flag,
                value, reinterpret_cast<struct kernel_timespec *>(&tm));
    } else {
      nanosleep(&tm, NULL);
    }
    errno = save_errno;
  }
}

void SpinLockWake(volatile Atomic32 *w, bool all) {
  if (kHaveFutex) {
    sys_futex(reinterpret_cast<int *>(const_cast<Atomic32 *>(w)),
              FUTEX_WAKE | futex_private_flag, all? INT_MAX : 1, 0);
  }
}

} // namespace subtle
} // namespace base
