// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "gutil/stl_util.h"
#include "util/countdown_latch.h"
#include "util/task_executor.h"

namespace kudu {

FutureTask::FutureTask(const std::tr1::shared_ptr<Task>& task)
: state_(kTaskPendingState),
  task_(task),
  latch_(1) {
}

void FutureTask::Run() {
  if (!set_state(kTaskRunningState)) {
    return;
  }

  status_ = task_->Run();
  set_state(kTaskFinishedState);

  latch_.CountDown();
  boost::lock_guard<LockType> l(lock_);
  if (status_.ok()) {
    BOOST_FOREACH(ListenerCallback callback, listeners_){
      callback->OnSuccess();
    }
  } else {
    BOOST_FOREACH(ListenerCallback callback, listeners_) {
      callback->OnFailure(status_);
    }
  }
}

bool FutureTask::Abort() {
  boost::lock_guard<LockType> l(lock_);
  if (state_ != kTaskFinishedState && task_->Abort()) {
    state_ = kTaskAbortedState;
  }
  return false;
}

void FutureTask::AddListener(
    const std::tr1::shared_ptr<FutureCallback>& callback) {
  boost::lock_guard<LockType> l(lock_);
  if (state_ != kTaskFinishedState || kTaskAbortedState) {
    listeners_.push_back(callback);
  } else if (status_.ok()) {
    callback->OnSuccess();
  } else {
    callback->OnFailure(status_);
  }
}

bool FutureTask::is_aborted() const {
  boost::lock_guard<LockType> l(lock_);
  return state_ == kTaskAbortedState;
}

bool FutureTask::is_done() const {
  boost::lock_guard<LockType> l(lock_);
  return state_ == kTaskFinishedState || kTaskAbortedState;
}

bool FutureTask::is_running() const {
  boost::lock_guard<LockType> l(lock_);
  return state_ == kTaskRunningState;
}

void FutureTask::Wait() {
  latch_.Wait();
}
bool FutureTask::TimedWait(const boost::system_time& time_until) {
  return latch_.TimedWait(time_until);
}

bool FutureTask::set_state(TaskState state) {
  boost::lock_guard<LockType> l(lock_);
  if (state_ == kTaskAbortedState) {
    return false;
  }
  state_ = state;
  return true;
}

TaskExecutor::TaskExecutor(const std::tr1::shared_ptr<ThreadPool>& thread_pool)
: thread_pool_(thread_pool) {
}

void TaskExecutor::Submit(const std::tr1::shared_ptr<Task>& task,
                          std::tr1::shared_ptr<Future> *future) {
  std::tr1::shared_ptr<FutureTask> future_task(
      new FutureTask(task));
  if (future != NULL) {
    *future = future_task;
  }
  thread_pool_->Submit(future_task);
}

void TaskExecutor::Wait() {
  thread_pool_->Wait();
}

bool TaskExecutor::TimedWait(const boost::system_time& time_until) {
  return thread_pool_->TimedWait(time_until);
}

TaskExecutor::~TaskExecutor() {
  Shutdown();
}

void TaskExecutor::Shutdown() {
  thread_pool_->Shutdown();
}

TaskExecutor *TaskExecutor::CreateNew(size_t num_threads) {
  std::tr1::shared_ptr<ThreadPool> thread_pool(new ThreadPool);

  Status s = thread_pool->Init(num_threads);
  if (!s.ok()) {
    LOG(ERROR)<< "Unable to initialize the TaskExecutor ThreadPool: " << s.ToString();
    return(NULL);
  }

  return new TaskExecutor(thread_pool);
}

}  // namespace kudu

