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
//    ThreadPool thread_pool;
//    thread_pool.Submit(shared_ptr<Runnable>(new Task()));
//    thread_pool.Submit(boost::bind(&Func, 10));
class ThreadPool {
 public:
  // Create a new thread pool. The 'name' is only used for debugging
  // output and default names of the worker threads. Since thread names
  // are limited to 16 characters on Linux, it's good to choose a short
  // name here.
  explicit ThreadPool(const std::string& name);
  ~ThreadPool();

  // Initialize the thread pool with the specified number of threads
  Status Init(size_t num_threads);

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
  // Dispatcher responsible for dequeueing and executing the tasks
  void DispatchThread();

  // Require external locking
  bool is_idle() const {
    return queue_.empty() && active_threads_ == 0;
  }

 private:
  // Clear all entries from queue_. Requires lock_ is held.
  void ClearQueue();

  struct QueueEntry {
    std::tr1::shared_ptr<Runnable> runnable;
    Trace* trace;
  };

  const std::string name_;

  boost::mutex lock_;
  boost::condition_variable queue_changed_;
  std::list<QueueEntry> queue_;

  bool closing_;
  size_t active_threads_;
  boost::condition_variable no_active_thread_;
  std::vector<boost::thread *> threads_;
};

} // namespace kudu
#endif
