// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "gutil/stl_util.h"
#include "util/threadpool.h"

namespace kudu {

class FunctionRunnable : public Runnable {
 public:
  FunctionRunnable(const boost::function<void()>& func)
    : func_(func) {
  }

  void Run() {
    func_();
  }

 private:
  boost::function<void()> func_;
};

ThreadPool::ThreadPool()
  : closing_(true), active_threads_(0) {
}

ThreadPool::~ThreadPool() {
  Shutdown();
}

Status ThreadPool::Init(size_t num_threads) {
  if (!threads_.empty()) {
    return Status::NotSupported("The thread pool is already initialized");
  }

  closing_ = false;
  try {
    for (size_t i = 0; i < num_threads; i++) {
      active_threads_++;
      threads_.push_back(
          new boost::thread(boost::bind(&ThreadPool::DispatchThread, this)));
    }
  } catch(const boost::thread_resource_error& exception) {
    Shutdown();
    return Status::RuntimeError("boost thread creation error", exception.what());
  }
  return Status::OK();
}

void ThreadPool::Shutdown() {
  {
    boost::unique_lock<boost::mutex> unique_lock(lock_);
    closing_ = true;
    queue_.clear();

    // The Runnable doesn't have Abort() so we must wait
    // and hopefully the abort is done outside before calling Shutdown().
    while (!is_idle()) {
      no_active_thread_.wait(unique_lock);
    }
  }

  queue_changed_.notify_all();
  BOOST_FOREACH(boost::thread *thread, threads_) {
    thread->join();
  }

  STLDeleteElements(&threads_);
}

void ThreadPool::Submit(const boost::function<void()>& func) {
  Submit(std::tr1::shared_ptr<Runnable>(new FunctionRunnable(func)));
}

void ThreadPool::Submit(const std::tr1::shared_ptr<Runnable>& task) {
  DCHECK_GT(threads_.size(), 0) << "No threads in the pool";
  boost::lock_guard<boost::mutex> guard(lock_);
  if (!closing_) {
    queue_.push_back(task);
    queue_changed_.notify_one();
  }
}

void ThreadPool::Wait() {
  boost::unique_lock<boost::mutex> unique_lock(lock_);
  while (!is_idle()) {
    no_active_thread_.wait(unique_lock);
  }
}

bool ThreadPool::TimedWait(const boost::system_time& time_until) {
  boost::unique_lock<boost::mutex> unique_lock(lock_);
  while (!is_idle()) {
    if (!no_active_thread_.timed_wait(unique_lock, time_until)) {
      return false;
    }
  }
  return true;
}

void ThreadPool::DispatchThread() {
  while (true) {
    std::tr1::shared_ptr<Runnable> task;
    {
      boost::unique_lock<boost::mutex> unique_lock(lock_);

      // Shutdown this thread
      if (closing_) {
        if (--active_threads_ == 0) {
          no_active_thread_.notify_all();
        }
        break;
      }

      // No pending task, wait...
      if (queue_.empty()) {
        if (--active_threads_ == 0) {
          no_active_thread_.notify_all();
        }
        queue_changed_.wait(unique_lock);
        active_threads_++;
        continue;
      }

      // Fetch a pending task
      task = queue_.front();
      queue_.pop_front();
    }

    // Execute the task
    task->Run();
  }
}

} // namespace kudu
