// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_UTIL_TASK_EXECUTOR_H
#define KUDU_UTIL_TASK_EXECUTOR_H

#include <boost/foreach.hpp>
#include <tr1/memory>
#include <vector>

#include "gutil/macros.h"
#include "gutil/port.h"
#include "util/countdown_latch.h"
#include "util/locks.h"
#include "util/status.h"
#include "util/threadpool.h"

namespace kudu {

// A Callback for a Task. Added to a Future through AddListener() the
// FutureCallback gets called when the Task the future is attached to
// finished execution.
//
// Future Callbacks are executed in the same thread has the task, therefore
// potentially consuming Threadpool resources. It is the users responsibility
// deferring execution to another Threadpool for long-running Callbacks.
class FutureCallback {
 public:
  // Called when the FutureTask completes successfully.
  virtual void OnSuccess() = 0;

  // Called when the FutureTask fails or is aborted.
  // The status describes the reason for failure.
  virtual void OnFailure(const Status& status) = 0;

  virtual ~FutureCallback() {
  }
};

// A Future for a Task. Allows to inspect the execution state of the task,
// block waiting for completion.
class Future {
 public:

  // Returns true if the task was aborted
  virtual bool is_aborted() const = 0;

  // Returns true if the task is done executing (having finished
  // normally or being aborted)
  virtual bool is_done() const = 0;

  // Returns the exit status of the task.
  // If you call this method when the task is not completed the result is undefined.
  virtual Status status() const = 0;

  // Wait for the task finished state.
  virtual void Wait() = 0;

  // Waits for the task finished state for the given duration of time.
  // Returns true if the task is finished within the given timeout. Otherwise false.
  virtual bool TimedWait(const boost::system_time& time_until) = 0;

  // Send an abort signal to the task
  // If the result is True, the task is handling the abort signal. Otherwise false.
  virtual bool Abort() = 0;

  // Add a FutureCallback to the listener list.
  // This callback will be called once the task if finished.
  //
  // If the task is not completed, the listener will be added to a listener
  // queue and it will be executed in the same execution thread of the task.
  // otherwise it will be execution in the caller thread.
  virtual void AddListener(
      std::tr1::shared_ptr<FutureCallback> callback) = 0;

  // Add a binded function to the listener list.
  // This function will be called on task success or failure.
  // (See AddListener(FutureCallback) for execution details)
  //
  // Example of a simple function:
  //   static void SimpleFunc(int a, int b) {}
  //   future->AddListener(boost::bind(&SimpleFunc, 10, 20));
  //
  // Example of a listener function running in a specified thread pool:
  //   static void SimpleFunc(int a, int b) {}
  //   boost::function<void()> func = boost::bind(&SimpleFunc, 30, 40);
  //   future->AddListener(boost::bind(&ThreadPool::SubmitFunc, thread_pool, func));
  //
  // Example of a listener task running in a specified executor
  //   std::tr1::shared_ptr<Task> listener_task(new MyTask);
  //   std::tr1::shared_ptr<Future> listener_future;
  //   future->AddListener(boost::bind(&TaskExecutor::Submit, executor,
  //                                   listener_task, &listener_future));
  void AddListener(const boost::function<void()>& func) {
    AddListener(func, func);
  }

  // Add a binded function to call on task success and a binded function to call on failure
  // (See AddListener(boost::function) for details)
  void AddListener(const boost::function<void()>& on_success,
                   const boost::function<void()>& on_failure) {
    AddListener(std::tr1::shared_ptr<FutureCallback>(
        new BindedFuncCallback(on_success, on_failure)));
  }

  // Waits for the task finished state for the given duration of time.
  // Returns true if the task is finished within the given timeout. Otherwise false.
  //
  // For example:
  //  future.TimedWait(boost::posix_time::milliseconds(100));
  template<class TimeDuration>
  bool TimedWait(TimeDuration const &relative_time) {
    return TimedWait(boost::get_system_time() + relative_time);
  }

  virtual ~Future() {
  }

 private:
  class BindedFuncCallback : public FutureCallback {
   public:
    BindedFuncCallback(const boost::function<void()>& on_success,
                       const boost::function<void()>& on_failure)
   : on_success_(on_success),
     on_failure_(on_failure) {
    }

    void OnSuccess() {
      on_success_();
    }
    void OnFailure(const Status& status) {
      on_failure_();
    }

   private:
    boost::function<void()> on_success_;
    boost::function<void()> on_failure_;
  };
};

// A generic task to be executed by Task executor.
class Task {
 public:

  // Called by TaskExecutor to Run() the task.
  virtual Status Run() = 0;

  // Aborts the task, if possible.
  virtual bool Abort() = 0;
  virtual ~Task() {
  }
};

// And implementation of Runnable and Future that takes a Task, executes it,
// tracks the current state and allows to Wait() for the Task's completion.
// Also executes any callbacks/listeners upon task completion *after* the task
// is considered completed. In particular listeners maybe executed after Wait()
// unblocks.
class FutureTask : public Runnable, public Future {
 private:
  enum TaskState {
    kTaskPendingState,
    kTaskRunningState,
    kTaskAbortedState,
    kTaskFinishedState,
  };

 public:
  explicit FutureTask(const std::tr1::shared_ptr<Task>& task);

  void Run();

  bool Abort();

  void AddListener(std::tr1::shared_ptr<FutureCallback> callback);

  bool is_aborted() const;

  bool is_done() const;

  bool is_running() const;

  Status status() const {
    return status_;
  }

  void Wait();

  bool TimedWait(const boost::system_time& time_until);

  virtual ~FutureTask() {
  }

 private:
  bool set_state(TaskState state);

 private:
  typedef std::tr1::shared_ptr<FutureCallback> ListenerCallback;
  typedef simple_spinlock LockType;

  mutable LockType lock_;
  TaskState state_;
  Status status_;
  std::tr1::shared_ptr<Task> task_;
  std::vector<ListenerCallback> listeners_;
  CountDownLatch latch_;
};

class TaskExecutor {
 public:
  // Initialize a TaskExecutor using an external ThreadPool
  explicit TaskExecutor(const std::tr1::shared_ptr<ThreadPool>& thread_pool);
  ~TaskExecutor();

  // Create a new Executor with its own ThreadPool.
  static TaskExecutor *CreateNew(size_t num_threads);

  // Wait for the running tasks to complete and then shutdown the threads.
  // All the other pending tasks in the queue will be removed.
  // NOTE: That the user may implement an external abort logic for the
  //       runnables, that must be called before Shutdown(), if the system
  //       should know about the non-execution of these tasks, or the runnable
  //       require an explicit "abort" notification to exit from the run loop.
  void Shutdown();

  // Submit a Task to the executor.
  // If the Future<> pointer is not NULL, it will be set with the task tracker object.
  //
  // Example without using the Future:
  //    std::tr1::shared_ptr<Task> task(new MyTask);
  //    executor->Submit(task, NULL);
  //
  // Example using the Future:
  //    std::tr1::shared_ptr<Task> task(new MyTask);
  //    std::tr1::shared_ptr<Future> future;
  //    executor->Submit(task, &future);
  //    future->Wait();
  Status Submit(const std::tr1::shared_ptr<Task>& task,
                std::tr1::shared_ptr<Future> *future)
                WARN_UNUSED_RESULT;

  // Submit a FutureTask to the executor.
  //
  // By adding Listeners to the FutureTask before calling this method,
  // you are guaranteed that the callbacks will be called from the executor thread if:
  // 1. SubmitFutureTask() returns Status::OK.
  // 2. The TaskExecutor is not Shutdown() while your FutureTask is in the threadpool queue.
  Status SubmitFutureTask(const std::tr1::shared_ptr<FutureTask>* future_task)
      WARN_UNUSED_RESULT;

  // Wait until all the tasks are completed.
  void Wait();

  // Waits for the idle state for the given duration of time.
  // Returns true if the pool is idle within the given timeout. Otherwise false.
  //
  // For example:
  //  executor.TimedWait(boost::posix_time::milliseconds(100));
  template<class TimeDuration>
  bool TimedWait(const TimeDuration& relative_time) {
    return thread_pool_->TimedWait(relative_time);
  }

  // Waits for the idle state for the given duration of time.
  // Returns true if the pool is idle within the given timeout. Otherwise false.
  bool TimedWait(const boost::system_time& time_until);

  // Returns the thread pool used by the TaskExecutor
  std::tr1::shared_ptr<ThreadPool> thread_pool() const {
    return thread_pool_;
  }

 private:
  std::tr1::shared_ptr<ThreadPool> thread_pool_;
};

}  // namespace kudu
#endif
