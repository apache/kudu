// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/util/threadpool.h"

#include <boost/function.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <limits>
#include <string>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/metrics.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"

namespace kudu {

using strings::Substitute;

////////////////////////////////////////////////////////
// FunctionRunnable
////////////////////////////////////////////////////////

class FunctionRunnable : public Runnable {
 public:
  explicit FunctionRunnable(boost::function<void()> func) : func_(std::move(func)) {}

  void Run() OVERRIDE {
    func_();
  }

 private:
  boost::function<void()> func_;
};

////////////////////////////////////////////////////////
// ThreadPoolBuilder
////////////////////////////////////////////////////////

ThreadPoolBuilder::ThreadPoolBuilder(std::string name)
    : name_(std::move(name)),
      min_threads_(0),
      max_threads_(base::NumCPUs()),
      max_queue_size_(std::numeric_limits<int>::max()),
      idle_timeout_(MonoDelta::FromMilliseconds(500)) {}

ThreadPoolBuilder& ThreadPoolBuilder::set_trace_metric_prefix(
    const std::string& prefix) {
  trace_metric_prefix_ = prefix;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_min_threads(int min_threads) {
  CHECK_GE(min_threads, 0);
  min_threads_ = min_threads;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_max_threads(int max_threads) {
  CHECK_GT(max_threads, 0);
  max_threads_ = max_threads;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_max_queue_size(int max_queue_size) {
  max_queue_size_ = max_queue_size;
  return *this;
}

ThreadPoolBuilder& ThreadPoolBuilder::set_idle_timeout(const MonoDelta& idle_timeout) {
  idle_timeout_ = idle_timeout;
  return *this;
}

Status ThreadPoolBuilder::Build(gscoped_ptr<ThreadPool>* pool) const {
  pool->reset(new ThreadPool(*this));
  RETURN_NOT_OK((*pool)->Init());
  return Status::OK();
}

////////////////////////////////////////////////////////
// ThreadPool
////////////////////////////////////////////////////////

ThreadPool::ThreadPool(const ThreadPoolBuilder& builder)
  : name_(builder.name_),
    min_threads_(builder.min_threads_),
    max_threads_(builder.max_threads_),
    max_queue_size_(builder.max_queue_size_),
    idle_timeout_(builder.idle_timeout_),
    pool_status_(Status::Uninitialized("The pool was not initialized.")),
    idle_cond_(&lock_),
    no_threads_cond_(&lock_),
    not_empty_(&lock_),
    num_threads_(0),
    active_threads_(0),
    queue_size_(0) {

  string prefix = !builder.trace_metric_prefix_.empty() ?
      builder.trace_metric_prefix_ : builder.name_;

  queue_time_trace_metric_name_ = TraceMetrics::InternName(
      prefix + ".queue_time_us");
  run_wall_time_trace_metric_name_ = TraceMetrics::InternName(
      prefix + ".run_wall_time_us");
  run_cpu_time_trace_metric_name_ = TraceMetrics::InternName(
      prefix + ".run_cpu_time_us");
}

ThreadPool::~ThreadPool() {
  Shutdown();
}

Status ThreadPool::Init() {
  MutexLock unique_lock(lock_);
  if (!pool_status_.IsUninitialized()) {
    return Status::NotSupported("The thread pool is already initialized");
  }
  pool_status_ = Status::OK();
  for (int i = 0; i < min_threads_; i++) {
    Status status = CreateThreadUnlocked();
    if (!status.ok()) {
      Shutdown();
      return status;
    }
  }
  return Status::OK();
}

void ThreadPool::Shutdown() {
  MutexLock unique_lock(lock_);
  CheckNotPoolThreadUnlocked();

  pool_status_ = Status::ServiceUnavailable("The pool has been shut down.");

  // Clear the queue_ member under the lock, but defer the releasing
  // of the entries outside the lock, in case there are concurrent threads
  // wanting to access the ThreadPool. The task's destructors may acquire
  // locks, etc, so this also prevents lock inversions.
  auto to_release = std::move(queue_);
  queue_.clear();
  queue_size_ = 0;
  not_empty_.Broadcast();

  // The Runnable doesn't have Abort() so we must wait
  // and hopefully the abort is done outside before calling Shutdown().
  while (num_threads_ > 0) {
    no_threads_cond_.Wait();
  }

  // Finally release the tasks that were in the queue, outside the lock.
  unique_lock.Unlock();
  for (QueueEntry& e : to_release) {
    if (e.trace) {
      e.trace->Release();
    }
  }
}

Status ThreadPool::SubmitClosure(const Closure& task) {
  // TODO: once all uses of boost::bind-based tasks are dead, implement this
  // in a more straight-forward fashion.
  return SubmitFunc(boost::bind(&Closure::Run, task));
}

Status ThreadPool::SubmitFunc(const boost::function<void()>& func) {
  return Submit(std::shared_ptr<Runnable>(new FunctionRunnable(func)));
}

Status ThreadPool::Submit(const std::shared_ptr<Runnable>& task) {
  MonoTime submit_time = MonoTime::Now();

  MutexLock guard(lock_);
  if (PREDICT_FALSE(!pool_status_.ok())) {
    return pool_status_;
  }

  // Size limit check.
  int64_t capacity_remaining = static_cast<int64_t>(max_threads_) - active_threads_ +
                               static_cast<int64_t>(max_queue_size_) - queue_size_;
  if (capacity_remaining < 1) {
    return Status::ServiceUnavailable(
        Substitute("Thread pool is at capacity ($0/$1 tasks running, $2/$3 tasks queued)",
                   num_threads_, max_threads_, queue_size_, max_queue_size_));
  }

  // Should we create another thread?
  // We assume that each current inactive thread will grab one item from the
  // queue.  If it seems like we'll need another thread, we create one.
  // In theory, a currently active thread could finish immediately after this
  // calculation.  This would mean we created a thread we didn't really need.
  // However, this race is unavoidable, since we don't do the work under a lock.
  // It's also harmless.
  //
  // Of course, we never create more than max_threads_ threads no matter what.
  int inactive_threads = num_threads_ - active_threads_;
  int additional_threads = (queue_size_ + 1) - inactive_threads;
  if (additional_threads > 0 && num_threads_ < max_threads_) {
    Status status = CreateThreadUnlocked();
    if (!status.ok()) {
      if (num_threads_ == 0) {
        // If we have no threads, we can't do any work.
        return status;
      }
      // If we failed to create a thread, but there are still some other
      // worker threads, log a warning message and continue.
      LOG(ERROR) << "Thread pool failed to create thread: "
                 << status.ToString();
    }
  }

  QueueEntry e;
  e.runnable = task;
  e.trace = Trace::CurrentTrace();
  // Need to AddRef, since the thread which submitted the task may go away,
  // and we don't want the trace to be destructed while waiting in the queue.
  if (e.trace) {
    e.trace->AddRef();
  }
  e.submit_time = submit_time;

  queue_.push_back(e);
  int length_at_submit = queue_size_++;

  guard.Unlock();
  not_empty_.Signal();

  if (queue_length_histogram_) {
    queue_length_histogram_->Increment(length_at_submit);
  }

  return Status::OK();
}

void ThreadPool::Wait() {
  MutexLock unique_lock(lock_);
  CheckNotPoolThreadUnlocked();
  while ((!queue_.empty()) || (active_threads_ > 0)) {
    idle_cond_.Wait();
  }
}

bool ThreadPool::WaitUntil(const MonoTime& until) {
  return WaitFor(until - MonoTime::Now());
}

bool ThreadPool::WaitFor(const MonoDelta& delta) {
  MutexLock unique_lock(lock_);
  CheckNotPoolThreadUnlocked();
  while ((!queue_.empty()) || (active_threads_ > 0)) {
    if (!idle_cond_.TimedWait(delta)) {
      return false;
    }
  }
  return true;
}


void ThreadPool::SetQueueLengthHistogram(const scoped_refptr<Histogram>& hist) {
  queue_length_histogram_ = hist;
}

void ThreadPool::SetQueueTimeMicrosHistogram(const scoped_refptr<Histogram>& hist) {
  queue_time_us_histogram_ = hist;
}

void ThreadPool::SetRunTimeMicrosHistogram(const scoped_refptr<Histogram>& hist) {
  run_time_us_histogram_ = hist;
}

void ThreadPool::DispatchThread(bool permanent) {
  MutexLock unique_lock(lock_);
  while (true) {
    // Note: Status::Aborted() is used to indicate normal shutdown.
    if (!pool_status_.ok()) {
      VLOG(2) << "DispatchThread exiting: " << pool_status_.ToString();
      break;
    }

    if (queue_.empty()) {
      if (permanent) {
        not_empty_.Wait();
      } else {
        if (!not_empty_.TimedWait(idle_timeout_)) {
          // After much investigation, it appears that pthread condition variables have
          // a weird behavior in which they can return ETIMEDOUT from timed_wait even if
          // another thread did in fact signal. Apparently after a timeout there is some
          // brief period during which another thread may actually grab the internal mutex
          // protecting the state, signal, and release again before we get the mutex. So,
          // we'll recheck the empty queue case regardless.
          if (queue_.empty()) {
            VLOG(3) << "Releasing worker thread from pool " << name_ << " after "
                    << idle_timeout_.ToMilliseconds() << "ms of idle time.";
            break;
          }
        }
      }
      continue;
    }

    // Fetch a pending task
    QueueEntry entry = std::move(queue_.front());
    queue_.pop_front();
    queue_size_--;
    ++active_threads_;

    unique_lock.Unlock();

    // Release the reference which was held by the queued item.
    ADOPT_TRACE(entry.trace);
    if (entry.trace) {
      entry.trace->Release();
    }

    // Update metrics
    MonoTime now(MonoTime::Now());
    int64_t queue_time_us = (now - entry.submit_time).ToMicroseconds();
    TRACE_COUNTER_INCREMENT(queue_time_trace_metric_name_, queue_time_us);
    if (queue_time_us_histogram_) {
      queue_time_us_histogram_->Increment(queue_time_us);
    }

    // Execute the task
    {
      MicrosecondsInt64 start_wall_us = GetMonoTimeMicros();
      MicrosecondsInt64 start_cpu_us = GetThreadCpuTimeMicros();

      entry.runnable->Run();

      int64_t wall_us = GetMonoTimeMicros() - start_wall_us;
      int64_t cpu_us = GetThreadCpuTimeMicros() - start_cpu_us;

      if (run_time_us_histogram_) {
        run_time_us_histogram_->Increment(wall_us);
      }
      TRACE_COUNTER_INCREMENT(run_wall_time_trace_metric_name_, wall_us);
      TRACE_COUNTER_INCREMENT(run_cpu_time_trace_metric_name_, cpu_us);
    }
    // Destruct the task while we do not hold the lock.
    //
    // The task's destructor may be expensive if it has a lot of bound
    // objects, and we don't want to block submission of the threadpool.
    // In the worst case, the destructor might even try to do something
    // with this threadpool, and produce a deadlock.
    entry.runnable.reset();
    unique_lock.Lock();

    if (--active_threads_ == 0) {
      idle_cond_.Broadcast();
    }
  }

  // It's important that we hold the lock between exiting the loop and dropping
  // num_threads_. Otherwise it's possible someone else could come along here
  // and add a new task just as the last running thread is about to exit.
  CHECK(unique_lock.OwnsLock());

  CHECK_EQ(threads_.erase(Thread::current_thread()), 1);
  if (--num_threads_ == 0) {
    no_threads_cond_.Broadcast();

    // Sanity check: if we're the last thread exiting, the queue ought to be
    // empty. Otherwise it will never get processed.
    CHECK(queue_.empty());
    DCHECK_EQ(0, queue_size_);
  }
}

Status ThreadPool::CreateThreadUnlocked() {
  // The first few threads are permanent, and do not time out.
  bool permanent = (num_threads_ < min_threads_);
  scoped_refptr<Thread> t;
  Status s = kudu::Thread::Create("thread pool", strings::Substitute("$0 [worker]", name_),
                                  &ThreadPool::DispatchThread, this, permanent, &t);
  if (s.ok()) {
    InsertOrDie(&threads_, t.get());
    num_threads_++;
  }
  return s;
}

void ThreadPool::CheckNotPoolThreadUnlocked() {
  Thread* current = Thread::current_thread();
  if (ContainsKey(threads_, current)) {
    LOG(FATAL) << Substitute("Thread belonging to thread pool '$0' with "
        "name '$1' called pool function that would result in deadlock",
        name_, current->name());
  }
}

} // namespace kudu
