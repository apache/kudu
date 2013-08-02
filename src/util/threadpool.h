// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_UTIL_THREAD_POOL_H
#define KUDU_UTIL_THREAD_POOL_H

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread.hpp>
#include <tr1/memory>
#include <vector>

#include "gutil/macros.h"
#include "util/status.h"

namespace kudu {

class Runnable {
 public:
  virtual void Run() = 0;
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
  ThreadPool();
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
  void Submit(const boost::function<void()>& func);

  // Submit a Runnable class
  void Submit(const std::tr1::shared_ptr<Runnable>& task);

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
  boost::mutex lock_;

  boost::condition_variable queue_changed_;
  std::list<std::tr1::shared_ptr<Runnable> > queue_;

  bool closing_;
  size_t active_threads_;
  boost::condition_variable no_active_thread_;
  std::vector<boost::thread *> threads_;
};

} // namespace kudu
#endif
