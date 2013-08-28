// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "gutil/atomicops.h"
#include "util/task_executor.h"
#include "util/test_macros.h"

namespace kudu {

TEST(TestTaskExecutor, TestNoTaskOpenClose) {
  gscoped_ptr<TaskExecutor> executor(TaskExecutor::CreateNew(4));
  executor->Shutdown();
}

static void SimpleFunc(int a, int b) {
  LOG(INFO)<< "Executing SimpleFunc(" << a << ", " << b << ")";
}

class SimpleTask : public Task {
 public:

  explicit SimpleTask(int *id)
      : id_(id) {
  }

  Status Run() {
    LOG(INFO)<< "Executing SimpleTask(" << *id_ << ")::Run()";
    ++(*id_);
    return Status::OK();
  }

  bool Abort() {return false;}

 private:
  int *id_;
};

class SimpleRunnable : public Runnable {
 public:
  void Run() {
    LOG(INFO)<< "SimpleRunnable()::Run()";
  }
};

class SimpleCallback : public FutureCallback {
 public:
  explicit SimpleCallback(int *id)
      : id_(id) {
  }

  void OnSuccess() {
    LOG(INFO)<< "OnSuccess Callback result=" << *id_;
  }

  void OnFailure(const Status& status) {
    LOG(INFO) << "OnFailure Callback status=" << status.ToString();
  }
 private:
  int *id_;
};

TEST(TestTaskExecutor, TestFutures) {
  gscoped_ptr<TaskExecutor> executor(TaskExecutor::CreateNew(1));

  // caller manages the task out parameter (on the stack, here)
  int result1 = 1;
  std::tr1::shared_ptr<Task> task(new SimpleTask(&result1));
  std::tr1::shared_ptr<Future> future;
  executor->Submit(task, &future);

  int result2 = 2;
  // Add a task associated to an executor pool as listener
  std::tr1::shared_ptr<Task> task2(new SimpleTask(&result2));
  std::tr1::shared_ptr<Future> future2;
  executor->Submit(task2, &future2);

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
  gscoped_ptr<TaskExecutor> executor(TaskExecutor::CreateNew(1));

  ThreadPool *thread_pool = executor->thread_pool().get();

  // caller manages the task out /future callback in parameter (on the heap, here)
  gscoped_ptr<int> result1(new int(1));

  std::tr1::shared_ptr<Future> future;
  executor->Submit(std::tr1::shared_ptr<Task>(new SimpleTask(result1.get())),
                   &future);

  // Add simple generic function as listener
  future->AddListener(boost::bind(&SimpleFunc, 10, 20));

  // Add simple function on a thread pool as listener
  boost::function<void()> func = boost::bind(&SimpleFunc, 30, 40);
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

} // namespace kudu
