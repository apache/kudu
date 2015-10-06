// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/semaphore.h"

#include <semaphore.h>
#include <glog/logging.h>
#include "kudu/gutil/walltime.h"

namespace kudu {

Semaphore::Semaphore(int capacity)
  : count_(0) {
  DCHECK_GE(capacity, 0);
  sem_ = dispatch_semaphore_create(capacity);
  CHECK_NOTNULL(sem_);
}

Semaphore::~Semaphore() {
  dispatch_release(sem_);
}

void Semaphore::Acquire() {
  // If the timeout is DISPATCH_TIME_FOREVER, then dispatch_semaphore_wait() waits forever and
  // always returns zero.
  if (dispatch_semaphore_wait(sem_, DISPATCH_TIME_FOREVER) == 0) {
    count_.IncrementBy(-1);
    return;
  } else {
    Fatal("wait");
  }
}

bool Semaphore::TryAcquire() {
  if (dispatch_semaphore_wait(sem_, DISPATCH_TIME_NOW) == 0) {
    count_.IncrementBy(-1);
    return true;
  }
  return false;
}

bool Semaphore::TimedAcquire(const MonoDelta& timeout) {
  dispatch_time_t t = dispatch_time(DISPATCH_TIME_NOW,
                        timeout.ToMicroseconds() * MonoTime::kNanosecondsPerMicrosecond);
  if (dispatch_semaphore_wait(sem_, t) == 0) {
    count_.IncrementBy(-1);
    return true;
  }
  return false;
}

void Semaphore::Release() {
  PCHECK(dispatch_semaphore_signal(sem_) == 0);
  count_.IncrementBy(1);
}

int Semaphore::GetValue() {
  return count_.Load();
}

void Semaphore::Fatal(const char* action) {
  PLOG(FATAL) << "Could not " << action << " semaphore "
              << reinterpret_cast<void*>(&sem_);
  abort(); // unnecessary, but avoids gcc complaining
}

} // namespace kudu
