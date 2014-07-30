// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_PTHREAD_SPINLOCK_H
#define KUDU_UTIL_PTHREAD_SPINLOCK_H

#include <pthread.h>

namespace kudu {

// Wrapper around pthread spinlocks to satisfy the boost lock interface.
class PThreadSpinLock {
 public:
  PThreadSpinLock() {
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
  }

  ~PThreadSpinLock() {
    pthread_spin_destroy(&lock_);
  }

  void lock() {
    pthread_spin_lock(&lock_);
  }

  void unlock() {
    pthread_spin_unlock(&lock_);
  }

  bool trylock() {
    return pthread_spin_trylock(&lock_) == 0;
  }

 private:
  pthread_spinlock_t lock_;
};

} // namespace kudu
#endif
