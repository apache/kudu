// Copyright (c) 2014, Cloudera, inc.
//
// Copied from Impala and adapted to Kudu.

#ifndef KUDU_UTIL_THREAD_H
#define KUDU_UTIL_THREAD_H

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <string>

#include "gutil/atomicops.h"
#include "gutil/ref_counted.h"
#include "util/status.h"

namespace kudu {

class MetricRegistry;
class Thread;
class WebCallbackRegistry;

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
  explicit ThreadJoiner(const Thread* thread);

  // Start emitting warnings after this many milliseconds.
  //
  // Default: 1000 ms.
  ThreadJoiner& warn_after_ms(int ms);

  // After the warnings after started, emit another warning at the
  // given interval.
  //
  // Default: 1000 ms.
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

  const Thread* thread_;

  int warn_after_ms_;
  int warn_every_ms_;
  int give_up_after_ms_;

  DISALLOW_COPY_AND_ASSIGN(ThreadJoiner);
};

// Thin wrapper around boost::thread that can register itself with the singleton ThreadMgr
// (a private class implemented in thread.cc entirely, which tracks all live threads so
// that they may be monitored via the debug webpages). This class has a limited subset of
// boost::thread's API. Construction is almost the same, but clients must supply a
// category and a name for each thread so that they can be identified in the debug web
// UI. Otherwise, Join() is the only supported method from boost::thread.
//
// Each Thread object knows its operating system thread ID (tid), which can be used to
// attach debuggers to specific threads, to retrieve resource-usage statistics from the
// operating system, and to assign threads to resource control groups.
//
// TODO: Consider allowing fragment IDs as category parameters.
class Thread : public base::RefCountedThreadSafe<Thread> {
  friend class ThreadJoiner;
 public:
  // This constructor pattern mimics that in boost::thread. There is
  // one constructor for each number of arguments that the thread
  // function accepts. To extend the set of acceptable signatures, add
  // another constructor with <class F, class A1.... class An>.
  //
  // In general:
  //  - category: string identifying the thread category to which this thread belongs,
  //    used for organising threads together on the debug UI.
  //  - name: name of this thread. Will be appended with "-<thread-id>" to ensure
  //    uniqueness.
  //  - F - a method type that supports operator(), and the instance passed to the
  //    constructor is executed immediately in a separate thread.
  //  - A1...An - argument types whose instances are passed to f(...)
  //  - holder - optional shared pointer to hold a reference to the created thread.
  template <class F>
  static Status Create(const std::string& category, const std::string& name, const F& f,
                       scoped_refptr<Thread>* holder) {
    return StartThread(category, name, f, holder);
  }

  template <class F, class A1>
  static Status Create(const std::string& category, const std::string& name, const F& f,
                       const A1& a1, scoped_refptr<Thread>* holder) {
    return StartThread(category, name, boost::bind(f, a1), holder);
  }

  template <class F, class A1, class A2>
  static Status Create(const std::string& category, const std::string& name, const F& f,
                       const A1& a1, const A2& a2, scoped_refptr<Thread>* holder) {
    return StartThread(category, name, boost::bind(f, a1, a2), holder);
  }

  template <class F, class A1, class A2, class A3>
  static Status Create(const std::string& category, const std::string& name, const F& f,
                       const A1& a1, const A2& a2, const A3& a3, scoped_refptr<Thread>* holder) {
    return StartThread(category, name, boost::bind(f, a1, a2, a3), holder);
  }

  template <class F, class A1, class A2, class A3, class A4>
  static Status Create(const std::string& category, const std::string& name, const F& f,
                       const A1& a1, const A2& a2, const A3& a3, const A4& a4,
                       scoped_refptr<Thread>* holder) {
    return StartThread(category, name, boost::bind(f, a1, a2, a3, a4), holder);
  }

  template <class F, class A1, class A2, class A3, class A4, class A5>
  static Status Create(const std::string& category, const std::string& name, const F& f,
                       const A1& a1, const A2& a2, const A3& a3, const A4& a4, const A5& a5,
                       scoped_refptr<Thread>* holder) {
    return StartThread(category, name, boost::bind(f, a1, a2, a3, a4, a5), holder);
  }

  template <class F, class A1, class A2, class A3, class A4, class A5, class A6>
  static Status Create(const std::string& category, const std::string& name, const F& f,
                       const A1& a1, const A2& a2, const A3& a3, const A4& a4, const A5& a5,
                       const A6& a6, scoped_refptr<Thread>* holder) {
    return StartThread(category, name, boost::bind(f, a1, a2, a3, a4, a5, a6), holder);
  }

  // Blocks until this thread finishes execution. Once this method returns, the thread
  // will be unregistered with the ThreadMgr and will not appear in the debug UI.
  void Join() const { ThreadJoiner(this).Join(); }

  // The thread ID assigned to this thread by the operating system. If the OS does not
  // support retrieving the tid, returns Thread::INVALID_THREAD_ID.
  int64_t tid() const { return tid_; }

  // The current thread of execution, or NULL if the current thread isn't a Thread.
  static Thread* current_thread() { return tls_; }

  static const int64_t INVALID_THREAD_ID = -1;

 private:
  // To distinguish between a thread ID that can't be determined, and one that hasn't been
  // assigned. Since tid_ is set in the constructor, this value will never be seen by
  // clients of this class.
  static const int64_t UNINITIALISED_THREAD_ID = -2;

  // Function object that wraps the user-supplied function to run in a separate thread.
  typedef boost::function<void ()> ThreadFunctor;

  Thread(const std::string& category, const std::string& name)
    : category_(category),
      name_(name),
      tid_(UNINITIALISED_THREAD_ID) {
  }

  // The actual thread object that runs the user's method via SuperviseThread().
  boost::scoped_ptr<boost::thread> thread_;

  // Name and category for this thread
  const std::string category_;
  const std::string name_;

  // OS-specific thread ID. Set to UNINITIALISED_THREAD_ID initially, but once the
  // constructor returns from StartThread() the tid_ is guaranteed to be set either to a
  // non-negative integer, or INVALID_THREAD_ID.
  int64_t tid_;

  // Thread local pointer to the current thread of execution. Will be NULL if the current
  // thread is not a Thread.
  static __thread Thread* tls_;

  // Starts the thread running SuperviseThread(), and returns once that thread has
  // initialised and its TID has been read. Waits for notification from the started
  // thread that initialisation is complete before returning. On success, stores a
  // reference to the thread in holder.
  static Status StartThread(const std::string& category, const std::string& name,
                            const ThreadFunctor& functor, scoped_refptr<Thread>* holder);

  // Wrapper for the user-supplied function. Always invoked from thread_. Executes the
  // method in functor, but before doing so registers with the global ThreadMgr and reads
  // the thread's system TID. After the method terminates, it is unregistered.
  //
  // SuperviseThread() notifies StartThread() when thread initialisation is completed via
  // the c_p_tid parameter, which is set to the new thread's system ID. By that point in
  // time SuperviseThread() has also taken a reference to thread', allowing it to refer
  // to it even after the caller moves on.
  //
  // Additionally, StartThread() notifies SuperviseThread() when the actual thread object
  // has been assigned (SuperviseThread() is spinning during this time). Without this,
  // the new thread may reference the actual thread object before it has been assigned by
  // StartThread(). See KUDU-11 for more details.
  void SuperviseThread(ThreadFunctor functor, Atomic64* c_p_tid);
};

// Registers /threadz with the debug webserver, and creates thread-tracking metrics under
// the "thread-manager." prefix
Status StartThreadInstrumentation(MetricRegistry* metric, WebCallbackRegistry* web);
} // namespace kudu

#endif /* KUDU_UTIL_THREAD_H */
