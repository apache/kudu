// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_UTIL_COUNTDOWN_LATCH_H
#define KUDU_UTIL_COUNTDOWN_LATCH_H

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

#include "gutil/macros.h"
#include "util/monotime.h"

namespace kudu {

// This is a C++ implementation of the Java CountDownLatch
// class.
// See http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/CountDownLatch.html
class CountDownLatch {
 public:
  // Initialize the latch with the given initial count.
  explicit CountDownLatch(int count) :
    count_(count)
  {}

  // Decrement the count of this latch.
  // If the new count is zero, then all waiting threads are woken up.
  // If the count is already zero, this has no effect.
  void CountDown() {
    boost::lock_guard<boost::mutex> lock(lock_);
    if (count_ == 0) {
      return;
    }

    if (--count_ == 0) {
      // Latch has triggered.
      cond_.notify_all();
    }
  }

  // Wait until the count on the latch reaches zero.
  // If the count is already zero, this returns immediately.
  void Wait() {
    boost::unique_lock<boost::mutex> lock(lock_);
    while (count_ > 0) {
      cond_.wait(lock);
    }
  }

  // Waits for the count on the latch to reach zero, or until 'until' time is reached.
  // Returns true if the count became zero, false otherwise.
  bool WaitUntil(const MonoTime& when) {
    MonoDelta relative = when.GetDeltaSince(MonoTime::Now(MonoTime::FINE));
    return WaitFor(relative);
  }

  // Waits for the count on the latch to reach zero, or until 'delta' time elapses.
  // Returns true if the count became zero, false otherwise.
  bool WaitFor(const MonoDelta& delta) {
    boost::unique_lock<boost::mutex> lock(lock_);
    while (count_ > 0) {
      if (!cond_.timed_wait(lock, boost::posix_time::microseconds(delta.ToMicroseconds()))) {
        return false;
      }
    }
    return true;
  }

  // Reset the latch with the given count. This is equivalent to reconstructing
  // the latch.
  void Reset(uint64_t count) {
    boost::unique_lock<boost::mutex> lock(lock_);
    count_ = count;
  }

  uint64_t count() const {
    boost::lock_guard<boost::mutex> lock(lock_);
    return count_;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(CountDownLatch);
  mutable boost::mutex lock_;
  boost::condition_variable cond_;

  uint64_t count_;
};

// Utility class which calls latch->CountDown() in its destructor.
class CountDownOnScopeExit {
 public:
  explicit CountDownOnScopeExit(CountDownLatch *latch) : latch_(latch) {}
  ~CountDownOnScopeExit() {
    latch_->CountDown();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(CountDownOnScopeExit);

  CountDownLatch *latch_;
};

} // namespace kudu
#endif
