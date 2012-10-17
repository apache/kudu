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

// This file is a Posix-specific part of spinlock_wait.cc

#include <sched.h>
#include <time.h>
#include <errno.h>

namespace base {
namespace subtle {

void SpinLockDelay(volatile Atomic32 *w, int32 value, int loop) {
  int save_errno = errno;
  if (loop == 0) {
  } else if (loop == 1) {
    sched_yield();
  } else {
    struct timespec tm;
    tm.tv_sec = 0;
    tm.tv_nsec = base::subtle::SuggestedDelayNS(loop);
    nanosleep(&tm, NULL);
  }
  errno = save_errno;
}

void SpinLockWake(volatile Atomic32 *w, bool all) {
}

} // namespace subtle
} // namespace base
