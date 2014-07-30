// Copyright (c) 2013, Cloudera, inc.

#include <boost/thread/locks.hpp>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/locks.h"
#include "kudu/util/task_executor.h"
#include "kudu/util/test_macros.h"

namespace kudu {

using std::tr1::shared_ptr;

TEST(TestTaskExecutor, TestNoTaskOpenClose) {
  gscoped_ptr<TaskExecutor> executor;
  ASSERT_OK(TaskExecutorBuilder("test").set_max_threads(4).Build(&executor));
  executor->Shutdown();
}

static void SimpleListenerFunc(int a, int b) {
  LOG(INFO)<< "Executing SimpleListenerFunc(" << a << ", " << b << ")";
}

static Status SimpleRunFunc(Status status) {
  LOG(INFO)<< "Executing SimpleRunFunc(" << status.ToString() << ")";
  return status;
}

class SimpleTask : public Task {
 public:

  explicit SimpleTask(int *id)
      : id_(id) {
  }

  Status Run() OVERRIDE {
    LOG(INFO)<< "Executing SimpleTask(" << *id_ << ")::Run()";
    ++(*id_);
    return Status::OK();
  }

  bool Abort() OVERRIDE {return false;}

 private:
  int *id_;
};

class SimpleRunnable : public Runnable {
 public:
  void Run() OVERRIDE {
    LOG(INFO)<< "SimpleRunnable()::Run()";
  }
};

class SimpleCallback : public FutureCallback {
 public:
  explicit SimpleCallback(int *id)
      : id_(id) {
  }

  void OnSuccess() OVERRIDE {
    LOG(INFO)<< "OnSuccess Callback result=" << *id_;
  }

  void OnFailure(const Status& status) OVERRIDE {
    LOG(INFO) << "OnFailure Callback status=" << status.ToString();
  }
 private:
  int *id_;
};

TEST(TestTaskExecutor, TestFutures) {
  gscoped_ptr<TaskExecutor> executor;
  ASSERT_OK(TaskExecutorBuilder("test").set_max_threads(1).Build(&executor));

  // caller manages the task out parameter (on the stack, here)
  int result1 = 1;
  std::tr1::shared_ptr<Task> task(new SimpleTask(&result1));
  std::tr1::shared_ptr<Future> future;
  ASSERT_STATUS_OK(executor->Submit(task, &future));

  int result2 = 2;
  // Add a task associated to an executor pool as listener
  std::tr1::shared_ptr<Task> task2(new SimpleTask(&result2));
  std::tr1::shared_ptr<Future> future2;
  ASSERT_STATUS_OK(executor->Submit(task2, &future2));

  future->Wait();
  ASSERT_TRUE(future->is_done());
  ASSERT_FALSE(future->is_aborted());
  ASSERT_EQ(2, result1);

  executor->Wait();
  ASSERT_TRUE(future2->is_done());
  ASSERT_FALSE(future2->is_aborted());
  ASSERT_EQ(3, result2);

  executor->Shutdown();
}

TEST(TestTaskExecutor, TestFutureListeners) {
  gscoped_ptr<TaskExecutor> executor;
  ASSERT_OK(TaskExecutorBuilder("test").set_max_threads(1).Build(&executor));

  ThreadPool *thread_pool = executor->thread_pool().get();

  // caller manages the task out /future callback in parameter (on the heap, here)
  gscoped_ptr<int> result1(new int(1));

  std::tr1::shared_ptr<Future> future;
  ASSERT_STATUS_OK(executor->Submit(std::tr1::shared_ptr<Task>(new SimpleTask(result1.get())),
                   &future));

  // Add simple generic function as listener
  future->AddListener(boost::bind(&SimpleListenerFunc, 10, 20));

  // Add simple function on a thread pool as listener
  boost::function<void()> func = boost::bind(&SimpleListenerFunc, 30, 40);
  future->AddListener(boost::bind(&ThreadPool::SubmitFunc, thread_pool, func));

  // Add simple runnable on a thread pool as listener
  future->AddListener(
      boost::bind(&ThreadPool::Submit, thread_pool,
                  std::tr1::shared_ptr<Runnable>(new SimpleRunnable)));

  // Add a FutureCallback as listener
  future->AddListener(
      std::tr1::shared_ptr<FutureCallback>(new SimpleCallback(result1.get())));

  future->Wait();
  ASSERT_TRUE(future->is_done());
  ASSERT_EQ(2, *result1);

  executor->Shutdown();
}

//////////////////////////////////////////////////////////////////////////////

// A task that always succeeds and which cannot be aborted.
class SucceedingTask : public Task {
 public:
  virtual Status Run() OVERRIDE {
    return Status::OK();
  }
  virtual bool Abort() OVERRIDE {
    // Cannot abort.
    return false;
  }
};

// A task that always fails and which cannot be aborted.
class FailingTask : public Task {
 public:
  virtual Status Run() OVERRIDE {
    return Status::RuntimeError("FailingTask has failed");
  }
  virtual bool Abort() OVERRIDE {
    // Cannot abort.
    return false;
  }
};

// Flags to indicate whether a Success or Failure callback was invoked.
enum ResultCode {
  kIndicatorNew = 0,      // No callback method invoked.
  kIndicatorSuccess = 1,  // OnSuccess() callback method was invoked.
  kIndicatorFailure = 2,  // OnFailure() callback method was invoked.
};

// A thread-safe class to store the result of various test indicators.
class TaskResult {
 public:
  TaskResult()
    : code_(kIndicatorNew),
      status_(Status::OK()),
      count_(0) {
  }
  void reset() {
    code_ = kIndicatorNew;
    status_ = Status::OK();
    count_ = 0;
  }
  // Store which callback method was invoked.
  void set_code(ResultCode code) {
    boost::lock_guard<simple_spinlock> l(lock_);
    code_ = code;
  }
  // Returns an indicator for which callback method was invoked.
  ResultCode code() {
    boost::lock_guard<simple_spinlock> l(lock_);
    return code_;
  }
  // Stores the result of a Task::Run() invocation.
  void set_status(Status status) {
    boost::lock_guard<simple_spinlock> l(lock_);
    status_ = status;
  }
  // Returns the result of a Task::Run() invocation.
  Status status() {
    boost::lock_guard<simple_spinlock> l(lock_);
    return status_;
  }
  // Increments the number of times any callbacks were invoked.
  void IncrementCount() {
    boost::lock_guard<simple_spinlock> l(lock_);
    ++count_;
  }
  // Returns the number of times any callbacks were invoked.
  int count() {
    boost::lock_guard<simple_spinlock> l(lock_);
    return count_;
  }
 private:
  ResultCode code_;
  Status status_;
  int count_;
  simple_spinlock lock_;
};

// A callback that sets fields on a TaskResult instance according to which callback methods were
// invoked as well as the results of the associated Task's invocation.
class IndicatorCallback : public FutureCallback {
 public:
  explicit IndicatorCallback(TaskResult* result)
    : result_(DCHECK_NOTNULL(result)) {
    }
  virtual void OnSuccess() OVERRIDE {
    result_->set_code(kIndicatorSuccess);
    result_->set_status(Status::OK());
    result_->IncrementCount();
  }
  virtual void OnFailure(const Status& status) OVERRIDE {
    result_->set_code(kIndicatorFailure);
    result_->set_status(status);
    result_->IncrementCount();
  }
 private:
  TaskResult* result_;
};

// Run Task to completion, then attach IndicatorCallback listener.
static void RunWithPostCompletionIndicator(Task* t, TaskResult* result) {
  gscoped_ptr<TaskExecutor> executor;
  ASSERT_OK(TaskExecutorBuilder("test").set_max_threads(1).Build(&executor));

  shared_ptr<Task> task(t);
  shared_ptr<Future> future;
  ASSERT_STATUS_OK(executor->Submit(task, &future));
  future->Wait(); // We wait for the task to complete before installing a callback.
  future->AddListener(shared_ptr<FutureCallback>(new IndicatorCallback(result)));

  executor->Shutdown();
}

// Ensure OnSuccess() callbacks execute even after a task completes.
TEST(TestTaskExecutor, TestAddListenerAfterTaskSuccess) {
  TaskResult result;
  RunWithPostCompletionIndicator(new SucceedingTask(), &result);
  ASSERT_EQ(kIndicatorSuccess, result.code());
  ASSERT_TRUE(result.status().ok());
  ASSERT_EQ(1, result.count());
}

// Ensure OnFailure() callbacks execute even after a task completes.
TEST(TestTaskExecutor, TestAddListenerAfterTaskFailure) {
  TaskResult result;
  RunWithPostCompletionIndicator(new FailingTask(), &result);
  ASSERT_EQ(kIndicatorFailure, result.code());
  ASSERT_TRUE(result.status().IsRuntimeError());
  ASSERT_EQ(1, result.count());
}

//////////////////////////////////////////////////////////////////////////////

// This is a test class that hangs in Run() until Abort() is called, and acknowledges,
// via return true, that the Abort() call was successful.
// It also always returns Status::Aborted from Run() after Abort() is called.
class AbortableHangingTask : public Task {
 public:
  AbortableHangingTask()
    : started_latch_(1),
      continue_latch_(1) {
  }
  virtual Status Run() OVERRIDE {
    LOG(INFO) << "Task Run() called";
    started_latch_.CountDown();
    continue_latch_.Wait();
    return Status::Aborted("Aborted task");
  }
  virtual bool Abort() OVERRIDE {
    LOG(INFO) << "Task Abort() called";
    // Allow Run() to complete.
    started_latch_.CountDown();
    continue_latch_.CountDown();
    return true;
  }
  // Call this to be sure that Run() has started before continuing.
  void WaitUntilRunning() {
    started_latch_.Wait();
  }
 private:
  CountDownLatch started_latch_;
  CountDownLatch continue_latch_;
};

// This test class hangs, and technically is abortable, but we pretend it's not, by
// returning false from Abort().
// It also always returns Status::OK from Run() after Abort() is called.
class FalselyNonAbortableHangingTask : public AbortableHangingTask {
 public:
  virtual Status Run() OVERRIDE {
    ignore_result(AbortableHangingTask::Run());
    return Status::OK();
  }
  virtual bool Abort() OVERRIDE {
    AbortableHangingTask::Abort();
    return false;
  }
};

// Ensure FutureTask::Abort() returns true when a Task is able to Abort().
// Ensure FutureTask::Abort() returns false when a Task indicates that it is unable to Abort().
TEST(TestTaskExecutor, TestAbortSuccessAndFailure) {
  gscoped_ptr<TaskExecutor> executor;
  ASSERT_OK(TaskExecutorBuilder("test").set_max_threads(1).Build(&executor));

  // Able to abort.
  shared_ptr<AbortableHangingTask> task(new AbortableHangingTask());
  shared_ptr<FutureTask> future(new FutureTask(task));
  TaskResult result;
  future->AddListener(shared_ptr<FutureCallback>(new IndicatorCallback(&result)));
  ASSERT_STATUS_OK(executor->SubmitFutureTask(&future));
  task->WaitUntilRunning();
  ASSERT_TRUE(future->Abort());
  future->Wait();

  ASSERT_EQ(kIndicatorFailure, result.code());
  ASSERT_TRUE(result.status().IsAborted());
  ASSERT_EQ(1, result.count());

  // Unable to abort.
  task.reset(new FalselyNonAbortableHangingTask());
  future.reset(new FutureTask(task));
  result.reset();
  future->AddListener(shared_ptr<FutureCallback>(new IndicatorCallback(&result)));
  ASSERT_STATUS_OK(executor->SubmitFutureTask(&future));
  task->WaitUntilRunning();
  ASSERT_FALSE(future->Abort());
  future->Wait();

  ASSERT_EQ(kIndicatorSuccess, result.code());
  ASSERT_TRUE(result.status().ok());
  ASSERT_EQ(1, result.count());

  executor->Shutdown();
}

TEST(TestTaskExecutor, TestRunAndAbortBindMethods) {
  gscoped_ptr<TaskExecutor> executor;
  ASSERT_OK(TaskExecutorBuilder("test").set_max_threads(1).Build(&executor));

  std::tr1::shared_ptr<Future> future;
  // test bind with a function that returns OK
  ASSERT_STATUS_OK(executor->Submit(boost::bind(&SimpleRunFunc, Status::OK()), &future));
  future->Wait();
  ASSERT_EQ(future->status().CodeAsString(), Status::OK().CodeAsString());

  future.reset();
  // test bind with a function that returns Status::IllegalState
  ASSERT_STATUS_OK(executor->Submit(boost::bind(&SimpleRunFunc, Status::IllegalState("")),
                                    &future));
  future->Wait();
  ASSERT_EQ(future->status().CodeAsString(), Status::IllegalState("").CodeAsString());

  // test bind with an abortable task
  future.reset();
  gscoped_ptr<AbortableHangingTask> task(new AbortableHangingTask);
  ASSERT_STATUS_OK(executor->Submit(boost::bind(&AbortableHangingTask::Run, task.get()),
                                    boost::bind(&AbortableHangingTask::Abort, task.get()),
                                    &future));
  ASSERT_TRUE(future->Abort());
  future->Wait();
  ASSERT_TRUE(future->is_aborted());
}

// Test Abort() before SubmitFutureTask().
// That shouldn't really be done by API clients but we need to test the backlogged Abort() case.
// Add listener to FutureTask, call Abort(), then Submit the FutureTask.
TEST(TestTaskExecutor, TestAbortBeforeSubmitFutureTask) {
  gscoped_ptr<TaskExecutor> executor;
  ASSERT_OK(TaskExecutorBuilder("test").set_max_threads(1).Build(&executor));

  shared_ptr<AbortableHangingTask> task(new AbortableHangingTask());
  shared_ptr<FutureTask> future(new FutureTask(task));
  TaskResult result;
  future->AddListener(shared_ptr<FutureCallback>(new IndicatorCallback(&result)));
  ASSERT_TRUE(future->Abort());
  ASSERT_STATUS_OK(executor->SubmitFutureTask(&future));
  future->Wait();

  ASSERT_EQ(kIndicatorFailure, result.code());
  ASSERT_TRUE(result.status().IsAborted());
  ASSERT_EQ(1, result.count());

  executor->Shutdown();
}

} // namespace kudu
