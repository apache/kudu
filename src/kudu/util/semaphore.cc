// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/semaphore.h"

#include <semaphore.h>
#include <glog/logging.h>
#include "kudu/gutil/walltime.h"
namespace kudu {

Semaphore::Semaphore(int capacity) {
  DCHECK_GE(capacity, 0);
  if (sem_init(&sem_, 0, capacity) != 0) {
    Fatal("init");
  }
}

Semaphore::~Semaphore() {
  if (sem_destroy(&sem_) != 0) {
    Fatal("destroy");
  }
}

void Semaphore::Acquire() {
  while (true) {
    int ret = sem_wait(&sem_);
    if (ret == 0) {
      // TODO: would be nice to track acquisition time, etc.
      return;
    }

    if (errno == EINTR) continue;
    Fatal("wait");
  }
}

bool Semaphore::TryAcquire() {
  int ret = sem_trywait(&sem_);
  if (ret == 0) {
    return true;
  }
  if (errno == EAGAIN || errno == EINTR) {
    return false;
  }
  Fatal("trywait");
}

bool Semaphore::TimedAcquire(const MonoDelta& timeout) {
  int64_t microtime = GetCurrentTimeMicros();
  microtime += timeout.ToMicroseconds();

  struct timespec abs_timeout;
  MonoDelta::NanosToTimeSpec(microtime * MonoTime::kNanosecondsPerMicrosecond,
                             &abs_timeout);

  while (true) {
    int ret = sem_timedwait(&sem_, &abs_timeout);
    if (ret == 0) return true;
    if (errno == ETIMEDOUT) return false;
    if (errno == EINTR) continue;
    Fatal("timedwait");
  }
}

void Semaphore::Release() {
  PCHECK(sem_post(&sem_) == 0);
}

int Semaphore::GetValue() {
  int val;
  PCHECK(sem_getvalue(&sem_, &val) == 0);
  return val;
}

void Semaphore::Fatal(const char* action) {
  PLOG(FATAL) << "Could not " << action << " semaphore "
              << reinterpret_cast<void*>(&sem_);
  abort(); // unnecessary, but avoids gcc complaining
}

} // namespace kudu
