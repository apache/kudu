// Copyright (c) 2013, Cloudera, inc.
//
// Utility functions for working with threads.
#ifndef KUDU_UTIL_THREAD_UTIL_H
#define KUDU_UTIL_THREAD_UTIL_H

#include <boost/thread/thread.hpp>
#include <string>

#include "gutil/atomicops.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
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

// Sets the thread name visible to debuggers/tools. This has no effect
// otherwise. This may have no effect on older kernels and versions of
// gdb (e.g RHEL6 prior to 6.5 does not expose this in gdb).
void SetThreadName(const std::string& name);

namespace thread_util_internal {

enum {
  THREAD_NOT_ASSIGNED,
  THREAD_ASSIGNED,
  THREAD_RUNNING
};

// Spin loop until *x changes value from 'from' to 'to'.
inline void SpinWait(Atomic32* x, uint32_t from, int32_t to) {
  int loop_count = 0;

  while (base::subtle::Acquire_Load(x) != from) {
    boost::detail::yield(loop_count++);
  }

  // We use an Acquire_Load spin and a Release_Store because we need both
  // directions of memory barrier here, and atomicops.h doesn't offer a
  // Barrier_CompareAndSwap call. TSAN will fail with either Release or Acquire
  // CAS above.
  base::subtle::Release_Store(x, to);
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
  while (base::subtle::Acquire_Load(&indicator) != thread_util_internal::THREAD_RUNNING) {
    boost::detail::yield(loop_count++);
  }
  return Status::OK();
}

// Utility to join on a thread, printing warning messages if it
// takes too long. For example:
//
//   ThreadJoiner(&my_thread, "processing thread")
//     .warn_after_ms(1000)
//     .warn_every_ms(5000)
//     .Join();
//
// TODO: would be nice to offer a way to use ptrace() or signals to
// dump the stack trace of the thread we're trying to join on if it
// gets stuck. But, after looking for 20 minutes or so, it seems
// pretty complicated to get right.
class ThreadJoiner {
 public:
  ThreadJoiner(boost::thread* thread, const std::string& name);

  // Start emitting warnings after this many milliseconds.
  ThreadJoiner& warn_after_ms(int ms);

  // After the warnings after started, emit another warning at the
  // given interval.
  ThreadJoiner& warn_every_ms(int ms);

  // If the thread has not stopped after this number of milliseconds, give up
  // joining on it and return Status::Aborted.
  //
  // -1 (the default) means to wait forever trying to join.
  ThreadJoiner& give_up_after_ms(int ms);

  // Join the thread, subject to the above parameters. If the thread joining
  // fails for any reason, returns RuntimeError. If it times out, returns
  // Aborted.
  Status Join();

 private:
  enum {
    kDefaultWarnAfterMs = 1000,
    kDefaultWarnEveryMs = 1000,
    kDefaultGiveUpAfterMs = -1 // forever
  };

  boost::thread* thread_;
  const string& thread_name_;

  int warn_after_ms_;
  int warn_every_ms_;
  int give_up_after_ms_;

  DISALLOW_COPY_AND_ASSIGN(ThreadJoiner);
};

} // namespace kudu
#endif /* KUDU_UTIL_THREAD_UTIL_H */
