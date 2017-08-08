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

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
#include <string>

#include <boost/function.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/metrics.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"

namespace kudu {

using std::shared_ptr;
using std::unique_ptr;
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
// ClosureRunnable
////////////////////////////////////////////////////////

class ClosureRunnable : public Runnable {
 public:
  explicit ClosureRunnable(Closure cl) : cl_(std::move(cl)) {}

  void Run() OVERRIDE {
    cl_.Run();
  }

 private:
  Closure cl_;
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

ThreadPoolBuilder& ThreadPoolBuilder::set_metrics(ThreadPoolMetrics metrics) {
  metrics_ = std::move(metrics);
  return *this;
}

Status ThreadPoolBuilder::Build(gscoped_ptr<ThreadPool>* pool) const {
  pool->reset(new ThreadPool(*this));
  RETURN_NOT_OK((*pool)->Init());
  return Status::OK();
}

////////////////////////////////////////////////////////
// ThreadPoolToken
////////////////////////////////////////////////////////

ThreadPoolToken::ThreadPoolToken(ThreadPool* pool,
                                 ThreadPool::ExecutionMode mode,
                                 ThreadPoolMetrics metrics)
    : mode_(mode),
      metrics_(std::move(metrics)),
      pool_(pool),
      state_(State::IDLE),
      not_running_cond_(&pool->lock_),
      active_threads_(0) {
}

ThreadPoolToken::~ThreadPoolToken() {
  Shutdown();
  pool_->ReleaseToken(this);
}

Status ThreadPoolToken::SubmitClosure(Closure c) {
  return Submit(std::make_shared<ClosureRunnable>(std::move(c)));
}

Status ThreadPoolToken::SubmitFunc(boost::function<void()> f) {
  return Submit(std::make_shared<FunctionRunnable>(std::move(f)));
}

Status ThreadPoolToken::Submit(shared_ptr<Runnable> r) {
  return pool_->DoSubmit(std::move(r), this);
}

void ThreadPoolToken::Shutdown() {
  MutexLock unique_lock(pool_->lock_);
  pool_->CheckNotPoolThreadUnlocked();

  // Clear the queue under the lock, but defer the releasing of the tasks
  // outside the lock, in case there are concurrent threads wanting to access
  // the ThreadPool. The task's destructors may acquire locks, etc, so this
  // also prevents lock inversions.
  std::deque<ThreadPool::Task> to_release = std::move(entries_);
  pool_->total_queued_tasks_ -= to_release.size();

  switch (state()) {
    case State::IDLE:
      // There were no tasks outstanding; we can quiesce the token immediately.
      Transition(State::QUIESCED);
      break;
    case State::RUNNING:
      // There were outstanding tasks. If any are still running, switch to
      // QUIESCING and wait for them to finish (the worker thread executing
      // the token's last task will switch the token to QUIESCED). Otherwise,
      // we can quiesce the token immediately.

      // Note: this is an O(n) operation, but it's expected to be infrequent.
      // Plus doing it this way (rather than switching to QUIESCING and waiting
      // for a worker thread to process the queue entry) helps retain state
      // transition symmetry with ThreadPool::Shutdown.
      for (auto it = pool_->queue_.begin(); it != pool_->queue_.end();) {
        if (*it == this) {
          it = pool_->queue_.erase(it);
        } else {
          it++;
        }
      }

      if (active_threads_ == 0) {
        Transition(State::QUIESCED);
        break;
      }
      Transition(State::QUIESCING);
      FALLTHROUGH_INTENDED;
    case State::QUIESCING:
      // The token is already quiescing. Just wait for a worker thread to
      // switch it to QUIESCED.
      while (state() != State::QUIESCED) {
        not_running_cond_.Wait();
      }
      break;
    default:
      break;
  }

  // Finally release the queued tasks, outside the lock.
  unique_lock.Unlock();
  for (auto& t : to_release) {
    if (t.trace) {
      t.trace->Release();
    }
  }
}

void ThreadPoolToken::Wait() {
  MutexLock unique_lock(pool_->lock_);
  pool_->CheckNotPoolThreadUnlocked();
  while (IsActive()) {
    not_running_cond_.Wait();
  }
}

bool ThreadPoolToken::WaitUntil(const MonoTime& until) {
  return WaitFor(until - MonoTime::Now());
}

bool ThreadPoolToken::WaitFor(const MonoDelta& delta) {
  MutexLock unique_lock(pool_->lock_);
  pool_->CheckNotPoolThreadUnlocked();
  while (IsActive()) {
    if (!not_running_cond_.TimedWait(delta)) {
      return false;
    }
  }
  return true;
}

void ThreadPoolToken::Transition(State new_state) {
#ifndef NDEBUG
  CHECK_NE(state_, new_state);

  switch (state_) {
    case State::IDLE:
      CHECK(new_state == State::RUNNING ||
            new_state == State::QUIESCED);
      if (new_state == State::RUNNING) {
        CHECK(!entries_.empty());
      } else {
        CHECK(entries_.empty());
        CHECK_EQ(active_threads_, 0);
      }
      break;
    case State::RUNNING:
      CHECK(new_state == State::IDLE ||
            new_state == State::QUIESCING ||
            new_state == State::QUIESCED);
      CHECK(entries_.empty());
      if (new_state == State::QUIESCING) {
        CHECK_GT(active_threads_, 0);
      }
      break;
    case State::QUIESCING:
      CHECK(new_state == State::QUIESCED);
      CHECK_EQ(active_threads_, 0);
      break;
    case State::QUIESCED:
      CHECK(false); // QUIESCED is a terminal state
      break;
    default:
      LOG(FATAL) << "Unknown token state: " << state_;
  }
#endif

  // Take actions based on the state we're entering.
  switch (new_state) {
    case State::IDLE:
    case State::QUIESCED:
      not_running_cond_.Broadcast();
      break;
    default:
      break;
  }

  state_ = new_state;
}

const char* ThreadPoolToken::StateToString(State s) {
  switch (s) {
    case State::IDLE: return "IDLE"; break;
    case State::RUNNING: return "RUNNING"; break;
    case State::QUIESCING: return "QUIESCING"; break;
    case State::QUIESCED: return "QUIESCED"; break;
  }
  return "<cannot reach here>";
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
    total_queued_tasks_(0),
    tokenless_(NewToken(ExecutionMode::CONCURRENT)),
    metrics_(builder.metrics_) {
  std::string prefix = !builder.trace_metric_prefix_.empty() ?
      builder.trace_metric_prefix_ : builder.name_;

  queue_time_trace_metric_name_ = TraceMetrics::InternName(
      prefix + ".queue_time_us");
  run_wall_time_trace_metric_name_ = TraceMetrics::InternName(
      prefix + ".run_wall_time_us");
  run_cpu_time_trace_metric_name_ = TraceMetrics::InternName(
      prefix + ".run_cpu_time_us");
}

ThreadPool::~ThreadPool() {
  // There should only be one live token: the one used in tokenless submission.
  CHECK_EQ(1, tokens_.size()) << Substitute(
      "Threadpool $0 destroyed with $1 allocated tokens",
      name_, tokens_.size());
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

  // Note: this is the same error seen at submission if the pool is at
  // capacity, so clients can't tell them apart. This isn't really a practical
  // concern though because shutting down a pool typically requires clients to
  // be quiesced first, so there's no danger of a client getting confused.
  pool_status_ = Status::ServiceUnavailable("The pool has been shut down.");

  // Clear the various queues under the lock, but defer the releasing
  // of the tasks outside the lock, in case there are concurrent threads
  // wanting to access the ThreadPool. The task's destructors may acquire
  // locks, etc, so this also prevents lock inversions.
  queue_.clear();
  std::deque<std::deque<Task>> to_release;
  for (auto* t : tokens_) {
    if (!t->entries_.empty()) {
      to_release.emplace_back(std::move(t->entries_));
    }
    switch (t->state()) {
      case ThreadPoolToken::State::IDLE:
        // The token is idle; we can quiesce it immediately.
        t->Transition(ThreadPoolToken::State::QUIESCED);
        break;
      case ThreadPoolToken::State::RUNNING:
        // The token has tasks associated with it. If they're merely queued
        // (i.e. there are no active threads), the tasks will have been removed
        // above and we can quiesce immediately. Otherwise, we need to wait for
        // the threads to finish.
        t->Transition(t->active_threads_ > 0 ?
            ThreadPoolToken::State::QUIESCING :
            ThreadPoolToken::State::QUIESCED);
        break;
      default:
        break;
    }
  }

  // The queues are empty. Wake any sleeping worker threads and wait for all
  // of them to exit. Some worker threads will exit immediately upon waking,
  // while others will exit after they finish executing an outstanding task.
  total_queued_tasks_ = 0;
  not_empty_.Broadcast();
  while (num_threads_ > 0) {
    no_threads_cond_.Wait();
  }

  // All the threads have exited. Check the state of each token.
  for (auto* t : tokens_) {
    DCHECK(t->state() == ThreadPoolToken::State::IDLE ||
           t->state() == ThreadPoolToken::State::QUIESCED);
  }

  // Finally release the queued tasks, outside the lock.
  unique_lock.Unlock();
  for (auto& token : to_release) {
    for (auto& t : token) {
      if (t.trace) {
        t.trace->Release();
      }
    }
  }
}

unique_ptr<ThreadPoolToken> ThreadPool::NewToken(ExecutionMode mode) {
  return NewTokenWithMetrics(mode, {});
}

unique_ptr<ThreadPoolToken> ThreadPool::NewTokenWithMetrics(
    ExecutionMode mode, ThreadPoolMetrics metrics) {
  MutexLock guard(lock_);
  unique_ptr<ThreadPoolToken> t(new ThreadPoolToken(this,
                                                    mode,
                                                    std::move(metrics)));
  InsertOrDie(&tokens_, t.get());
  return t;
}

void ThreadPool::ReleaseToken(ThreadPoolToken* t) {
  MutexLock guard(lock_);
  CHECK(!t->IsActive()) << Substitute("Token with state $0 may not be released",
                                      ThreadPoolToken::StateToString(t->state()));
  CHECK_EQ(1, tokens_.erase(t));
}

Status ThreadPool::SubmitClosure(Closure c) {
  return Submit(std::make_shared<ClosureRunnable>(std::move(c)));
}

Status ThreadPool::SubmitFunc(boost::function<void()> f) {
  return Submit(std::make_shared<FunctionRunnable>(std::move(f)));
}

Status ThreadPool::Submit(shared_ptr<Runnable> r) {
  return DoSubmit(std::move(r), tokenless_.get());
}

Status ThreadPool::DoSubmit(shared_ptr<Runnable> r, ThreadPoolToken* token) {
  DCHECK(token);
  MonoTime submit_time = MonoTime::Now();

  MutexLock guard(lock_);
  if (PREDICT_FALSE(!pool_status_.ok())) {
    return pool_status_;
  }

  if (PREDICT_FALSE(!token->MaySubmitNewTasks())) {
    return Status::ServiceUnavailable("Thread pool token was shut down");
  }

  // Size limit check.
  int64_t capacity_remaining = static_cast<int64_t>(max_threads_) - active_threads_ +
                               static_cast<int64_t>(max_queue_size_) - total_queued_tasks_;
  if (capacity_remaining < 1) {
    return Status::ServiceUnavailable(
        Substitute("Thread pool is at capacity ($0/$1 tasks running, $2/$3 tasks queued)",
                   num_threads_, max_threads_, total_queued_tasks_, max_queue_size_));
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
  int threads_from_this_submit =
      token->IsActive() && token->mode() == ExecutionMode::SERIAL ? 0 : 1;
  int inactive_threads = num_threads_ - active_threads_;
  int additional_threads = (queue_.size() + threads_from_this_submit) - inactive_threads;
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

  Task task;
  task.runnable = std::move(r);
  task.trace = Trace::CurrentTrace();
  // Need to AddRef, since the thread which submitted the task may go away,
  // and we don't want the trace to be destructed while waiting in the queue.
  if (task.trace) {
    task.trace->AddRef();
  }
  task.submit_time = submit_time;

  // Add the task to the token's queue.
  ThreadPoolToken::State state = token->state();
  DCHECK(state == ThreadPoolToken::State::IDLE ||
         state == ThreadPoolToken::State::RUNNING);
  token->entries_.emplace_back(std::move(task));
  if (state == ThreadPoolToken::State::IDLE ||
      token->mode() == ExecutionMode::CONCURRENT) {
    queue_.emplace_back(token);
    if (state == ThreadPoolToken::State::IDLE) {
      token->Transition(ThreadPoolToken::State::RUNNING);
    }
  }
  int length_at_submit = total_queued_tasks_++;

  guard.Unlock();
  not_empty_.Signal();

  if (metrics_.queue_length_histogram) {
    metrics_.queue_length_histogram->Increment(length_at_submit);
  }
  if (token->metrics_.queue_length_histogram) {
    token->metrics_.queue_length_histogram->Increment(length_at_submit);
  }

  return Status::OK();
}

void ThreadPool::Wait() {
  MutexLock unique_lock(lock_);
  CheckNotPoolThreadUnlocked();
  while (total_queued_tasks_ > 0 || active_threads_ > 0) {
    idle_cond_.Wait();
  }
}

bool ThreadPool::WaitUntil(const MonoTime& until) {
  return WaitFor(until - MonoTime::Now());
}

bool ThreadPool::WaitFor(const MonoDelta& delta) {
  MutexLock unique_lock(lock_);
  CheckNotPoolThreadUnlocked();
  while (total_queued_tasks_ > 0 || active_threads_ > 0) {
    if (!idle_cond_.TimedWait(delta)) {
      return false;
    }
  }
  return true;
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

    // Get the next token and task to execute.
    ThreadPoolToken* token = queue_.front();
    queue_.pop_front();
    DCHECK_EQ(ThreadPoolToken::State::RUNNING, token->state());
    DCHECK(!token->entries_.empty());
    Task task = std::move(token->entries_.front());
    token->entries_.pop_front();
    token->active_threads_++;
    --total_queued_tasks_;
    ++active_threads_;

    unique_lock.Unlock();

    // Release the reference which was held by the queued item.
    ADOPT_TRACE(task.trace);
    if (task.trace) {
      task.trace->Release();
    }

    // Update metrics
    MonoTime now(MonoTime::Now());
    int64_t queue_time_us = (now - task.submit_time).ToMicroseconds();
    TRACE_COUNTER_INCREMENT(queue_time_trace_metric_name_, queue_time_us);
    if (metrics_.queue_time_us_histogram) {
      metrics_.queue_time_us_histogram->Increment(queue_time_us);
    }
    if (token->metrics_.queue_time_us_histogram) {
      token->metrics_.queue_time_us_histogram->Increment(queue_time_us);
    }

    // Execute the task
    {
      MicrosecondsInt64 start_wall_us = GetMonoTimeMicros();
      MicrosecondsInt64 start_cpu_us = GetThreadCpuTimeMicros();

      task.runnable->Run();

      int64_t wall_us = GetMonoTimeMicros() - start_wall_us;
      int64_t cpu_us = GetThreadCpuTimeMicros() - start_cpu_us;

      if (metrics_.run_time_us_histogram) {
        metrics_.run_time_us_histogram->Increment(wall_us);
      }
      if (token->metrics_.run_time_us_histogram) {
        token->metrics_.run_time_us_histogram->Increment(wall_us);
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
    task.runnable.reset();
    unique_lock.Lock();

    // Possible states:
    // 1. The token was shut down while we ran its task. Transition to QUIESCED.
    // 2. The token has no more queued tasks. Transition back to IDLE.
    // 3. The token has more tasks. Requeue it and transition back to RUNNABLE.
    ThreadPoolToken::State state = token->state();
    DCHECK(state == ThreadPoolToken::State::RUNNING ||
           state == ThreadPoolToken::State::QUIESCING);
    if (--token->active_threads_ == 0) {
      if (state == ThreadPoolToken::State::QUIESCING) {
        DCHECK(token->entries_.empty());
        token->Transition(ThreadPoolToken::State::QUIESCED);
      } else if (token->entries_.empty()) {
        token->Transition(ThreadPoolToken::State::IDLE);
      } else if (token->mode() == ExecutionMode::SERIAL) {
        queue_.emplace_back(token);
      }
    }
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
    DCHECK_EQ(0, total_queued_tasks_);
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

std::ostream& operator<<(std::ostream& o, ThreadPoolToken::State s) {
  return o << ThreadPoolToken::StateToString(s);
}

} // namespace kudu
