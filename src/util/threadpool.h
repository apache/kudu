// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_UTIL_THREAD_POOL_H
#define KUDU_UTIL_THREAD_POOL_H

#include <boost/function.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <tr1/memory>
#include <list>
#include <string>
#include <vector>

#include "gutil/macros.h"
#include "gutil/port.h"
#include "util/monotime.h"
#include "util/status.h"

namespace kudu {

class Trace;

class Runnable {
 public:
  virtual void Run() = 0;
  virtual ~Runnable() {}
};

// Simple Thread-Pool with a fixed number of threads.
// You can submit a class that implements the Runnable interface
// or use the boost::bind() to bind a function.
//
// Usage Example:
//    static void Func(int n) { ... }
//    class Task : public Runnable { ... }
//
//    ThreadPool thread_pool("my_pool", 0, 5, ThreadPool::DEFAULT_TIMEOUT);
//    thread_pool.Submit(shared_ptr<Runnable>(new Task()));
//    thread_pool.Submit(boost::bind(&Func, 10));
class ThreadPool {
 public:
  // Default timeout to use for threads.
  static const MonoDelta DEFAULT_TIMEOUT;

  // Create a new thread pool.
  //
  // name: used for debugging output and default names of the worker threads.
  //    Since thread names are limited to 16 characters on Linux, it's good to
  //    choose a short name here.
  //
  // min_threads: minimum number of threads we'll have at any time.

  // max_threads: maximum number of threads we'll have at any time.
  //
  // timeout: how long we'll keep around a thread before timing it out.
  //    We always keep at least min_threads.
  //
  // TODO: this class needs a builder.
  ThreadPool(const std::string& name, int min_threads,
             int max_threads, const MonoDelta &timeout);

  ~ThreadPool();

  // Initialize the thread pool.  We will start the minimum number of threads.
  Status Init();

  // Wait for the running tasks to complete and then shutdown the threads.
  // All the other pending tasks in the queue will be removed.
  // NOTE: That the user may implement an external abort logic for the
  //       runnables, that must be called before Shutdown(), if the system
  //       should know about the non-execution of these tasks, or the runnable
  //       require an explicit "abort" notification to exit from the run loop.
  void Shutdown();

  // Submit a function binded using boost::bind(&FuncName, args...)
  Status SubmitFunc(const boost::function<void()>& func)
      WARN_UNUSED_RESULT;

  // Submit a Runnable class
  Status Submit(const std::tr1::shared_ptr<Runnable>& task)
      WARN_UNUSED_RESULT;

  // Wait until all the tasks are completed.
  void Wait();

  // Waits for the idle state for the given duration of time.
  // Returns true if the pool is idle within the given timeout. Otherwise false.
  //
  // For example:
  //  thread_pool.TimedWait(boost::posix_time::milliseconds(100));
  template<class TimeDuration>
  bool TimedWait(const TimeDuration& relative_time) {
    return TimedWait(boost::get_system_time() + relative_time);
  }

  // Waits for the idle state for the given duration of time.
  // Returns true if the pool is idle within the given timeout. Otherwise false.
  bool TimedWait(const boost::system_time& time_until);

 private:
  // Clear all entries from queue_. Requires that lock_ is held.
  void ClearQueue();

  // Decrements number of thread, wakes up any thread waiting for all
  // threads to be finished. Requires that lock_ is held.
  void ThreadFinishedUnlocked(int expected_num_threads);

  // Dispatcher responsible for dequeueing and executing the tasks
  void DispatchThread(bool permanent);

  // Create new thread. Required that lock_ is held.
  Status CreateThreadUnlocked();

 private:
  FRIEND_TEST(TestThreadPool, TestThreadPoolWithNoMinimum);
  FRIEND_TEST(TestThreadPool, TestVariableSizeThreadPool);
  DISALLOW_COPY_AND_ASSIGN(ThreadPool);

  struct QueueEntry {
    std::tr1::shared_ptr<Runnable> runnable;
    Trace* trace;
  };

  const std::string name_;
  std::list<QueueEntry> queue_;
  boost::mutex lock_;
  Status pool_status_;
  int num_threads_;
  int active_threads_;
  const int min_threads_;
  const int max_threads_;
  const MonoDelta timeout_;
  boost::condition_variable queue_changed_;
  boost::condition_variable idle_cond_;
  boost::condition_variable no_threads_cond_;
};

} // namespace kudu
#endif
