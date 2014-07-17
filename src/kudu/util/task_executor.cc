// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <string>

#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/task_executor.h"

namespace kudu {

using strings::Substitute;

//////////////////////////////////////////////////
// FutureTask
//////////////////////////////////////////////////

FutureTask::FutureTask(const std::tr1::shared_ptr<Task>& task)
: state_(kTaskPendingState),
  task_(task),
  latch_(1) {
}

void FutureTask::Run() {
  if (!set_state(kTaskRunningState)) {
    Status s = Status::Aborted("Task was aborted before it ran");
    BOOST_FOREACH(ListenerCallback callback, listeners_) {
      callback->OnFailure(s);
    }
    latch_.CountDown();
    return;
  }

  status_ = task_->Run();
  set_state(kTaskFinishedState);

  {
    lock_guard<LockType> l(&lock_);
    if (status_.ok()) {
      BOOST_FOREACH(ListenerCallback callback, listeners_) {
        callback->OnSuccess();
      }
    } else {
      BOOST_FOREACH(ListenerCallback callback, listeners_) {
        callback->OnFailure(status_);
      }
    }
  }

  latch_.CountDown();
}

bool FutureTask::Abort() {
  lock_guard<LockType> l(&lock_);
  if (state_ != kTaskFinishedState && task_->Abort()) {
    state_ = kTaskAbortedState;
    return true;
  }
  return false;
}

// TODO: Consider making it so that all callbacks are invoked on the executor thread.
void FutureTask::AddListener(
    std::tr1::shared_ptr<FutureCallback> callback) {
  lock_guard<LockType> l(&lock_);
  if (state_ != kTaskFinishedState && state_ != kTaskAbortedState) {
    listeners_.push_back(callback);
  } else if (status_.ok()) {
    callback->OnSuccess();
  } else {
    callback->OnFailure(status_);
  }
}

bool FutureTask::is_aborted() const {
  lock_guard<LockType> l(&lock_);
  return state_ == kTaskAbortedState;
}

bool FutureTask::is_done() const {
  lock_guard<LockType> l(&lock_);
  return state_ == kTaskFinishedState || state_ == kTaskAbortedState;
}

bool FutureTask::is_running() const {
  lock_guard<LockType> l(&lock_);
  return state_ == kTaskRunningState;
}

void FutureTask::Wait() {
  latch_.Wait();
}

bool FutureTask::WaitUntil(const MonoTime& until) {
  return latch_.WaitUntil(until);
}

bool FutureTask::WaitFor(const MonoDelta& delta) {
  return latch_.WaitFor(delta);
}

bool FutureTask::set_state(TaskState state) {
  lock_guard<LockType> l(&lock_);
  if (state_ == kTaskAbortedState) {
    return false;
  }
  state_ = state;
  return true;
}

//////////////////////////////////////////////////
// TaskExecutorBuilder
//////////////////////////////////////////////////

TaskExecutorBuilder::TaskExecutorBuilder(const string& name)
  : pool_builder_(name) {
}

TaskExecutorBuilder& TaskExecutorBuilder::set_min_threads(int min_threads) {
  pool_builder_.set_min_threads(min_threads);
  return *this;
}

TaskExecutorBuilder& TaskExecutorBuilder::set_max_threads(int max_threads) {
  pool_builder_.set_max_threads(max_threads);
  return *this;
}

TaskExecutorBuilder& TaskExecutorBuilder::set_max_queue_size(int max_queue_size) {
  pool_builder_.set_max_queue_size(max_queue_size);
  return *this;
}

TaskExecutorBuilder& TaskExecutorBuilder::set_idle_timeout(const MonoDelta& idle_timeout) {
  pool_builder_.set_idle_timeout(idle_timeout);
  return *this;
}

Status TaskExecutorBuilder::Build(gscoped_ptr<TaskExecutor>* executor) const {
  gscoped_ptr<ThreadPool> pool;
  RETURN_NOT_OK_PREPEND(pool_builder_.Build(&pool),
      Substitute("Unable to initialize the TaskExecutor ThreadPool for $0",
                 pool_builder_.name()));
  executor->reset(new TaskExecutor(pool.Pass()));
  return Status::OK();
}

//////////////////////////////////////////////////
// TaskExecutor
//////////////////////////////////////////////////

TaskExecutor::TaskExecutor(gscoped_ptr<ThreadPool> thread_pool)
  : thread_pool_(thread_pool.Pass()) {
}

Status TaskExecutor::Submit(const std::tr1::shared_ptr<Task>& task,
                          std::tr1::shared_ptr<Future> *future) {
  std::tr1::shared_ptr<FutureTask> future_task(new FutureTask(task));
  if (future != NULL) {
    DCHECK(future->get() == NULL);
    *future = future_task;
  }
  return thread_pool_->Submit(future_task);
}

Status TaskExecutor::Submit(const boost::function<Status()>& run_method,
                            std::tr1::shared_ptr<Future>* future) {
  return Submit(std::tr1::shared_ptr<Task>(new BoundTask(run_method)), future);
}

Status TaskExecutor::Submit(const boost::function<Status()>& run_method,
                            const boost::function<bool()>& abort_method,
                            std::tr1::shared_ptr<Future>* future) {
  return Submit(std::tr1::shared_ptr<Task>(new BoundTask(run_method, abort_method)), future);
}

Status TaskExecutor::SubmitFutureTask(const std::tr1::shared_ptr<FutureTask>* future_task) {
  CHECK(future_task != NULL);
  return thread_pool_->Submit(*future_task);
}

void TaskExecutor::Wait() {
  thread_pool_->Wait();
}

bool TaskExecutor::WaitUntil(const MonoTime& until) {
  return thread_pool_->WaitUntil(until);
}

bool TaskExecutor::WaitFor(const MonoDelta& delta) {
  return thread_pool_->WaitFor(delta);
}

TaskExecutor::~TaskExecutor() {
  Shutdown();
}

void TaskExecutor::Shutdown() {
  thread_pool_->Shutdown();
}

} // namespace kudu
