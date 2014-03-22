// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_LOCKS_H
#define KUDU_UTIL_LOCKS_H

#include <boost/smart_ptr/detail/spinlock.hpp>
#include <boost/smart_ptr/detail/yield_k.hpp>
#include <glog/logging.h>

#include "gutil/atomicops.h"
#include "gutil/dynamic_annotations.h"
#include "gutil/macros.h"
#include "gutil/port.h"
#include "gutil/spinlock.h"
#include "gutil/sysinfo.h"
#include "util/errno.h"

namespace kudu {

using base::subtle::Acquire_CompareAndSwap;
using base::subtle::NoBarrier_Load;
using base::subtle::Release_Store;

// Wrapper around the Google SpinLock class to adapt it to the method names
// expected by Boost.
class simple_spinlock {
 public:
  simple_spinlock() {}

  void lock() {
    l_.Lock();
  }

  void unlock() {
    l_.Unlock();
  }

  bool try_lock() {
    return l_.TryLock();
  }

  // Return whether the lock is currently held.
  //
  // This state can change at any instant, so this is only really useful
  // for assertions where you expect to hold the lock. The success of
  // such an assertion isn't a guarantee that the current thread is the
  // holder, but the failure of such an assertion _is_ a guarantee that
  // the current thread is _not_ holding the lock!
  bool is_locked() {
    return l_.IsHeld();
  }

 private:
  base::SpinLock l_;

  DISALLOW_COPY_AND_ASSIGN(simple_spinlock);
};

// Read-Write lock. 32bit uint that contains the number of readers.
// When someone wants to write, tries to set the 32bit, and waits until
// the readers have finished. Readers are spinning while the write flag is set.
//
// This rw-lock makes no attempt at fairness, though it does avoid write
// starvation (no new readers may obtain the lock if a write is waiting).
//
// Given that this is a spin-lock, it should only be used in cases where the
// lock is held for very short time intervals.
class rw_spinlock {
 public:
  rw_spinlock() : state_(0) {
    ANNOTATE_RWLOCK_CREATE(this);
  }
  ~rw_spinlock() {
    ANNOTATE_RWLOCK_DESTROY(this);
  }

  void lock_shared() {
    int loop_count = 0;
    Atomic32 cur_state = NoBarrier_Load(&state_);
    while (true) {
      Atomic32 expected = cur_state & kNumReadersMask;   // I expect no write lock
      Atomic32 try_new_state = expected + 1;          // Add me as reader
      cur_state = Acquire_CompareAndSwap(&state_, expected, try_new_state);
      if (cur_state == expected)
        break;
      // Either was already locked by someone else, or CAS failed.
      boost::detail::yield(loop_count++);
    }
    ANNOTATE_RWLOCK_ACQUIRED(this, 0);
  }

  void unlock_shared() {
    ANNOTATE_RWLOCK_RELEASED(this, 0);
    int loop_count = 0;
    Atomic32 cur_state = NoBarrier_Load(&state_);
    while (true) {
      DCHECK_GT(cur_state & kNumReadersMask, 0)
        << "unlock_shared() called when there are no shared locks held";
      Atomic32 expected = cur_state;           // I expect a write lock and other readers
      Atomic32 try_new_state = expected - 1;   // Drop me as reader
      cur_state = Acquire_CompareAndSwap(&state_, expected, try_new_state);
      if (cur_state == expected)
        break;
      // Either was already locked by someone else, or CAS failed.
      boost::detail::yield(loop_count++);
    }
  }

  // Tries to acquire a write lock, if no one else has it.
  // This function retries on CAS failure and waits for readers to complete.
  bool try_lock() {
    int loop_count = 0;
    Atomic32 cur_state = NoBarrier_Load(&state_);
    while (true) {
      // someone else has already the write lock
      if (cur_state & kWriteFlag)
        return false;

      Atomic32 expected = cur_state & kNumReadersMask;   // I expect some 0+ readers
      Atomic32 try_new_state = kWriteFlag | expected;    // I want to lock the other writers
      cur_state = Acquire_CompareAndSwap(&state_, expected, try_new_state);
      if (cur_state == expected)
        break;
      // Either was already locked by someone else, or CAS failed.
      boost::detail::yield(loop_count++);
    }

    WaitPendingReaders();
    ANNOTATE_RWLOCK_ACQUIRED(this, 1);
    return true;
  }

  void lock() {
    int loop_count = 0;
    Atomic32 cur_state = NoBarrier_Load(&state_);
    while (true) {
      Atomic32 expected = cur_state & kNumReadersMask;   // I expect some 0+ readers
      Atomic32 try_new_state = kWriteFlag | expected;    // I want to lock the other writers
      cur_state = Acquire_CompareAndSwap(&state_, expected, try_new_state);
      if (cur_state == expected)
        break;
      // Either was already locked by someone else, or CAS failed.
      boost::detail::yield(loop_count++);
    }

    WaitPendingReaders();
    ANNOTATE_RWLOCK_ACQUIRED(this, 1);
  }

  void unlock() {
    // I expect to be the only writer
    DCHECK_EQ(ANNOTATE_UNPROTECTED_READ(state_), kWriteFlag);
    ANNOTATE_RWLOCK_RELEASED(this, 1);
    // reset: no writers/no readers
    Release_Store(&state_, 0);
  }

  // Return true if the lock is currently held for write by any thread.
  // See simple_spinlock::is_locked() for details about where this is useful.
  bool is_write_locked() const {
    return NoBarrier_Load(&state_) & kWriteFlag;
  }

  // Return true if the lock is currently held, either for read or write
  // by any thread.
  // See simple_spinlock::is_locked() for details about where this is useful.
  bool is_locked() const {
    return NoBarrier_Load(&state_);
  }

 private:
  static const uint32_t kNumReadersMask = 0x7fffffff;
  static const uint32_t kWriteFlag = 1 << 31;

  void WaitPendingReaders() {
    int loop_count = 0;
    while ((NoBarrier_Load(&state_) & kNumReadersMask) > 0) {
      boost::detail::yield(loop_count++);
    }
  }

 private:
  volatile Atomic32 state_;
};

// A reader-writer lock implementation which is biased for use cases where
// the write lock is taken infrequently, but the read lock is used often.
//
// Internally, this creates N underlying mutexes, one per CPU. When a thread
// wants to lock in read (shared) mode, it locks only its own CPU's mutex. When it
// wants to lock in write (exclusive) mode, it locks all CPU's mutexes.
//
// This means that in the read-mostly case, different readers will not cause any
// cacheline contention.
//
// Usage:
//   percpu_rwlock mylock;
//
//   // Lock shared:
//   {
//     boost::shared_lock<rw_spinlock> lock(mylock.get_lock());
//     ...
//   }
//
//   // Lock exclusive:
//
//   {
//     boost::lock_guard<percpu_rwlock> lock(mylock);
//     ...
//   }
class percpu_rwlock {
 public:
  percpu_rwlock() {
    errno = 0;
    n_cpus_ = base::NumCPUs();
    CHECK_EQ(errno, 0) << ErrnoToString(errno);
    CHECK_GT(n_cpus_, 0);
    locks_ = new padded_lock[n_cpus_];
  }

  ~percpu_rwlock() {
    delete [] locks_;
  }

  rw_spinlock &get_lock() {
    int cpu = sched_getcpu();
    CHECK_LT(cpu, n_cpus_);
    return locks_[cpu].lock;
  }

  bool try_lock() {
    for (int i = 0; i < n_cpus_; i++) {
      if (!locks_[i].lock.try_lock()) {
        while (i--) {
          locks_[i].lock.unlock();
        }
        return false;
      }
    }
    return true;
  }

  // Return true if this lock is held on any CPU.
  // See simple_spinlock::is_locked() for details about where this is useful.
  bool is_locked() const {
    for (int i = 0; i < n_cpus_; i++) {
      if (locks_[i].lock.is_locked()) return true;
    }
    return false;
  }

  void lock() {
    for (int i = 0; i < n_cpus_; i++) {
      locks_[i].lock.lock();
    }
  }

  void unlock() {
    for (int i = 0; i < n_cpus_; i++) {
      locks_[i].lock.unlock();
    }
  }

 private:
  struct padded_lock {
    rw_spinlock lock;
    char padding[CACHELINE_SIZE - sizeof(rw_spinlock)];
  };

  int n_cpus_;
  padded_lock *locks_;
};

} // namespace kudu

#endif
