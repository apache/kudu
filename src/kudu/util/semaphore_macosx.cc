// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/semaphore.h"

#include <semaphore.h>
#include <glog/logging.h>
#include "kudu/gutil/walltime.h"

namespace kudu {

Semaphore::Semaphore(int capacity)
  : count_(capacity) {
  DCHECK_GE(capacity, 0);
  sem_ = dispatch_semaphore_create(capacity);
  CHECK_NOTNULL(sem_);
}

Semaphore::~Semaphore() {
  dispatch_release(sem_);
}

void Semaphore::Acquire() {
  // If the timeout is DISPATCH_TIME_FOREVER, then dispatch_semaphore_wait()
  // waits forever and always returns zero.
  CHECK(dispatch_semaphore_wait(sem_, DISPATCH_TIME_FOREVER) == 0);
  count_.IncrementBy(-1);
}

bool Semaphore::TryAcquire() {
  // The dispatch_semaphore_wait() function returns zero upon success and
  // non-zero after the timeout expires.
  if (dispatch_semaphore_wait(sem_, DISPATCH_TIME_NOW) == 0) {
    count_.IncrementBy(-1);
    return true;
  }
  return false;
}

bool Semaphore::TimedAcquire(const MonoDelta& timeout) {
  dispatch_time_t t = dispatch_time(DISPATCH_TIME_NOW, timeout.ToNanoseconds());
  if (dispatch_semaphore_wait(sem_, t) == 0) {
    count_.IncrementBy(-1);
    return true;
  }
  return false;
}

void Semaphore::Release() {
  dispatch_semaphore_signal(sem_);
  count_.IncrementBy(1);
}

int Semaphore::GetValue() {
  return count_.Load();
}

} // namespace kudu
