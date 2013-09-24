// Copyright (c) 2013, Cloudera, inc.
//
// Utility functions for working with threads.
#ifndef KUDU_UTIL_THREAD_UTIL_H
#define KUDU_UTIL_THREAD_UTIL_H

#include <boost/thread/thread.hpp>
#include "gutil/atomicops.h"
#include "gutil/gscoped_ptr.h"
#include "util/status.h"

namespace kudu {

// Start a thread running the given function and assign it to *thread.
// The function is only called within the started thread after the
// thread has been assigned to *thread, so you may safely use assertions
// on this same gscoped_ptr object from within the thread code.
//
// Returns a RuntimeError if the thread fails to start.
template<typename F>
Status StartThread(F function, gscoped_ptr<boost::thread>* thread);

namespace thread_util_internal {

enum {
  THREAD_NOT_ASSIGNED,
  THREAD_ASSIGNED,
  THREAD_RUNNING
};

// Spin loop until *x changes value from 'from' to 'to'.
void SpinWait(Atomic32* x, uint32_t from, int32_t to) {
  int loop_count = 0;
  while (Acquire_CompareAndSwap(x, from, to) != from) {
    boost::detail::yield(loop_count++);
  }
}

template<class F>
struct ThreadStarter {
  ThreadStarter(const F& f, Atomic32* indicator)
    : f_(f),
      indicator_(indicator) {
  }

  void operator()() {
    SpinWait(indicator_, THREAD_ASSIGNED, THREAD_RUNNING);
    f_();
  }

  F f_;
  Atomic32* indicator_;
};

} // namespace thread_util_internal

template<typename F>
Status StartThread(F function, gscoped_ptr<boost::thread>* thread) {
  Atomic32 indicator = thread_util_internal::THREAD_NOT_ASSIGNED;
  thread_util_internal::ThreadStarter<F> ts(function, &indicator);
  try {
    thread->reset(new boost::thread(ts));
  } catch(boost::thread_resource_error &e) {
    // boost::thread uses exceptions to signal failure to create a thread
    return Status::RuntimeError(e.what());
  }

  // Signal the thread that it may continue running.
  Release_Store(&indicator, thread_util_internal::THREAD_ASSIGNED);

  // Loop until the thread actually sees the signal. If we were to return
  // at this point without waiting, then the 'indicator' object would be
  // popped off the stack and potentially overwritten, so it's possible
  // the thread would see arbitrary data here.
  int loop_count = 0;
  while (NoBarrier_Load(&indicator) != thread_util_internal::THREAD_RUNNING) {
    boost::detail::yield(loop_count++);
  }
  return Status::OK();
}

} // namespace kudu
#endif /* KUDU_UTIL_THREAD_UTIL_H */
