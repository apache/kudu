// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <boost/thread/locks.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <string>

#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "util/threadpool.h"
#include "util/thread_util.h"
#include "util/trace.h"

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

ThreadPool::ThreadPool(const string& name)
  : name_(name), closing_(true), active_threads_(0) {
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
      threads_.push_back(
          new boost::thread(boost::bind(&ThreadPool::DispatchThread, this)));
    }
  } catch(const boost::thread_resource_error& exception) {
    Shutdown();
    return Status::RuntimeError("boost thread creation error", exception.what());
  }
  return Status::OK();
}

void ThreadPool::ClearQueue() {
  BOOST_FOREACH(QueueEntry& e, queue_) {
    if (e.trace) {
      e.trace->Release();
    }
  }
  queue_.clear();
}

void ThreadPool::Shutdown() {
  {
    boost::unique_lock<boost::mutex> unique_lock(lock_);
    closing_ = true;
    ClearQueue();
    queue_changed_.notify_all();

    // The Runnable doesn't have Abort() so we must wait
    // and hopefully the abort is done outside before calling Shutdown().
    while (!is_idle()) {
      no_active_thread_.wait(unique_lock);
    }
  }

  BOOST_FOREACH(boost::thread *thread, threads_) {
    const string msg = strings::Substitute("worker in threadpool '$0'", name_);
    CHECK_OK(ThreadJoiner(thread, msg).Join());
  }

  STLDeleteElements(&threads_);
}

Status ThreadPool::SubmitFunc(const boost::function<void()>& func) {
  return Submit(std::tr1::shared_ptr<Runnable>(new FunctionRunnable(func)));
}

Status ThreadPool::Submit(const std::tr1::shared_ptr<Runnable>& task) {
  DCHECK_GT(threads_.size(), 0) << "No threads in the pool";
  boost::lock_guard<boost::mutex> guard(lock_);
  if (PREDICT_FALSE(closing_)) {
    return Status::IllegalState("ThreadPool is closing, unable to accept new Runnables");
  }
  QueueEntry e;
  e.runnable = task;
  e.trace = Trace::CurrentTrace();
  // Need to AddRef, since the thread which submitted the task may go away,
  // and we don't want the trace to be destructed while waiting in the queue.
  if (e.trace) {
    e.trace->AddRef();
  }
  queue_.push_back(e);
  queue_changed_.notify_one();
  return Status::OK();
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
  SetThreadName(name_ + " [worker]");

  bool has_processed_task = false;
  while (true) {
    QueueEntry entry;
    {
      boost::unique_lock<boost::mutex> unique_lock(lock_);

      if (has_processed_task) {
        if (--active_threads_ == 0) {
          no_active_thread_.notify_all();
        }
      }

      // Shutdown this thread
      if (closing_) {
        break;
      }

      // No pending task, wait...
      if (queue_.empty()) {
        queue_changed_.wait(unique_lock);
        has_processed_task = false;
        continue;
      }

      // Fetch a pending task
      entry = queue_.front();
      queue_.pop_front();
      active_threads_++;
      has_processed_task = true;
    }

    ADOPT_TRACE(entry.trace);
    // Release the reference which was held by the queued item.
    if (entry.trace) {
      entry.trace->Release();
    }
    // Execute the task
    entry.runnable->Run();
  }
}

} // namespace kudu
