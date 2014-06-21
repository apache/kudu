// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_UTIL_TASK_EXECUTOR_H
#define KUDU_UTIL_TASK_EXECUTOR_H

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <gtest/gtest_prod.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "gutil/macros.h"
#include "gutil/port.h"
#include "util/async_util.h"
#include "util/countdown_latch.h"
#include "util/locks.h"
#include "util/status.h"
#include "util/threadpool.h"

namespace kudu {

class Future;
class FutureTask;
class Task;
class TaskExecutor;

// Builder for instantiating a TaskExecutor.
// See ThreadPoolBuilder for documentation on each of these properties.
class TaskExecutorBuilder {
 public:
  explicit TaskExecutorBuilder(const std::string& name);

  TaskExecutorBuilder& set_min_threads(int min_threads);
  TaskExecutorBuilder& set_max_threads(int max_threads);
  TaskExecutorBuilder& set_max_queue_size(int max_queue_size);
  TaskExecutorBuilder& set_idle_timeout(const MonoDelta& idle_timeout);

  const std::string& name() const { return pool_builder_.name(); }
  int min_threads() const { return pool_builder_.min_threads(); }
  int max_threads() const { return pool_builder_.max_threads(); }
  int max_queue_size() const { return pool_builder_.max_queue_size(); }
  const MonoDelta& idle_timeout() const { return pool_builder_.idle_timeout(); }

  Status Build(gscoped_ptr<TaskExecutor>* executor) const;

 private:
  ThreadPoolBuilder pool_builder_;

  DISALLOW_COPY_AND_ASSIGN(TaskExecutorBuilder);
};

// Abstraction of a thread pool that allows for execution and sophisticated
// failure handling of arbitrary tasks, support for futures, etc.
class TaskExecutor {
 public:
  ~TaskExecutor();

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

  // Submit a method to be executed through this executor.
  Status Submit(const boost::function<Status()>& run_method,
                std::tr1::shared_ptr<Future> *future)
                WARN_UNUSED_RESULT;

  // Submit a method to be executed through this executor and a method
  // to be executed on Future::Abort().
  Status Submit(const boost::function<Status()>& run_method,
                const boost::function<bool()>& abort_method,
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

 private:
  friend class TaskExecutorBuilder;

  FRIEND_TEST(TestTaskExecutor, TestFutureListeners);

  // Initialize a TaskExecutor using an external ThreadPool.
  explicit TaskExecutor(gscoped_ptr<ThreadPool> thread_pool);

  // Return the thread pool used by the TaskExecutor.
  const gscoped_ptr<ThreadPool>& thread_pool() const {
    return thread_pool_;
  }

  gscoped_ptr<ThreadPool> thread_pool_;

  DISALLOW_COPY_AND_ASSIGN(TaskExecutor);
};

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

// Adapter which lets FutureCallbacks be used in the context of a StatusCallback.
// This simply translates from one API to a another.
class FutureToStatusCallback {
 public:
  explicit FutureToStatusCallback(const std::tr1::shared_ptr<FutureCallback>& future)
    : future_(future) {
  }

  void operator()(const Status& s) {
    if (PREDICT_TRUE(s.ok())) {
      future_->OnSuccess();
    } else {
      future_->OnFailure(s);
    }
  }

 private:
  const std::tr1::shared_ptr<FutureCallback> future_;
};

// FutureCallback implementation that can be waited on.
// Helper to make async methods with callback args, sync.
class LatchCallback : public FutureCallback {
 public:
  LatchCallback() : latch_(1) {}

  virtual void OnSuccess() OVERRIDE {
    latch_.CountDown();
  }

  virtual void OnFailure(const Status& status) OVERRIDE {
    status_ = status;
    latch_.CountDown();
  }

  Status Wait() {
    latch_.Wait();
    return status_;
  }

  Status TimedWait(const MonoDelta& delta) {
    bool done = latch_.TimedWait(boost::posix_time::microseconds(delta.ToMicroseconds()));
    if (!done) {
      return Status::TimedOut("Timeout waiting on LatchCallback.");
    }
    return status_;
  }

  Status status() const {
    return status_;
  }

 private:
  Status status_;
  CountDownLatch latch_;

  DISALLOW_COPY_AND_ASSIGN(LatchCallback);
};

class BoundFunctionCallback : public FutureCallback {
 public:
  explicit BoundFunctionCallback(const boost::function<void()>& on_success)
      : on_success_(on_success),
        on_failure_(boost::bind(&BoundFunctionCallback::DefaultOnFailure, _1)) {
  }

  BoundFunctionCallback(const boost::function<void()>& on_success,
                        const boost::function<void(const Status&)>& on_failure)
      : on_success_(on_success),
        on_failure_(on_failure) {
  }

  void OnSuccess() OVERRIDE {
    on_success_();
  }
  void OnFailure(const Status& status) OVERRIDE {
    on_failure_(status);
  }

 private:
  static void DefaultOnFailure(const Status& status) {
    DLOG(WARNING) << "Task failed silently with status: " << status.ToString();
  }

  boost::function<void()> on_success_;
  boost::function<void(const Status&)> on_failure_;

  DISALLOW_COPY_AND_ASSIGN(BoundFunctionCallback);
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
    AddListener(std::tr1::shared_ptr<FutureCallback>(
           new BoundFunctionCallback(func)));
  }

  // Add a binded function to call on task success and a binded function to call on failure
  // (See AddListener(boost::function) for details)
  void AddListener(const boost::function<void()>& on_success,
                   const boost::function<void(const Status&)>& on_failure) {
    AddListener(std::tr1::shared_ptr<FutureCallback>(
        new BoundFunctionCallback(on_success, on_failure)));
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

// A generic task that accepts methods and executes them on Run() and
// Abort().
class BoundTask : public Task {
 public:

  explicit BoundTask(const boost::function<Status()>& run)
      : run_(run),
        abort_(boost::bind(&BoundTask::DefaultAbort)) {
  }

  BoundTask(const boost::function<Status()>& run,
            const boost::function<bool()>& abort)
      : run_(run),
        abort_(abort) {
  }

  Status Run() OVERRIDE {
    return run_();
  }

  bool Abort() OVERRIDE {
    return abort_();
  }

 private:

  static bool DefaultAbort() {
    return false;
  }

  boost::function<Status()> run_;
  boost::function<bool()> abort_;

  DISALLOW_COPY_AND_ASSIGN(BoundTask);
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

  void Run() OVERRIDE;

  bool Abort() OVERRIDE;

  void AddListener(std::tr1::shared_ptr<FutureCallback> callback) OVERRIDE;

  bool is_aborted() const OVERRIDE;

  bool is_done() const OVERRIDE;

  bool is_running() const;

  Status status() const OVERRIDE {
    return status_;
  }

  void Wait() OVERRIDE;

  bool TimedWait(const boost::system_time& time_until) OVERRIDE;

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

} // namespace kudu
#endif
