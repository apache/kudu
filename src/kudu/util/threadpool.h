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
#pragma once

#include <sys/types.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"

namespace kudu {

class Thread;
class ThreadPool;
class ThreadPoolToken;
class Trace;


// Interesting thread pool metrics. Can be applied to the entire pool (see
// ThreadPoolBuilder) or to individual tokens.
struct ThreadPoolMetrics {
  // Measures the queue length seen by tasks when they enter the queue.
  scoped_refptr<Histogram> queue_length_histogram;

  // Measures the amount of time that tasks spend waiting in a queue.
  scoped_refptr<Histogram> queue_time_us_histogram;

  // Measures the amount of time that tasks spend running.
  scoped_refptr<Histogram> run_time_us_histogram;
};

// ThreadPool takes a lot of arguments. We provide sane defaults with a builder.
//
// name: Used for debugging output and default names of the worker threads.
//    Since thread names are limited to 16 characters on Linux, it's good to
//    choose a short name here.
//    Required.
//
// trace_metric_prefix: used to prefix the names of TraceMetric counters.
//    When a task on a thread pool has an associated trace, the thread pool
//    implementation will increment TraceMetric counters to indicate the
//    amount of time spent waiting in the queue as well as the amount of wall
//    and CPU time spent executing. By default, these counters are prefixed
//    with the name of the thread pool. For example, if the pool is named
//    'apply', then counters such as 'apply.queue_time_us' will be
//    incremented.
//
//    The TraceMetrics implementation relies on the number of distinct counter
//    names being small. Thus, if the thread pool name itself is dynamically
//    generated, the default behavior described above would result in an
//    unbounded number of distinct counter names. The 'trace_metric_prefix'
//    setting can be used to override the prefix used in generating the trace
//    metric names.
//
//    For example, the Raft thread pools are named "<tablet id>-raft" which
//    has unbounded cardinality (a server may have thousands of different
//    tablet IDs over its lifetime). In that case, setting the prefix to
//    "raft" will avoid any issues.
//
// min_threads: Minimum number of threads we'll have at any time.
//    Default: 0.
//
// max_threads: Maximum number of threads we'll have at any time.
//    Default: Number of CPUs detected on the system.
//
// max_queue_size: Maximum number of items to enqueue before returning a
//    Status::ServiceUnavailable message from Submit().
//    Default: INT_MAX.
//
// idle_timeout: How long we'll keep around an idle thread before timing it out.
//    We always keep at least min_threads.
//    Default: 500 milliseconds.
//
// metrics: Histograms, counters, etc. to update on various threadpool events.
//    Default: not set.
//
class ThreadPoolBuilder {
 public:
  explicit ThreadPoolBuilder(std::string name);

  // Note: We violate the style guide by returning mutable references here
  // in order to provide traditional Builder pattern conveniences.
  ThreadPoolBuilder& set_trace_metric_prefix(const std::string& prefix);
  ThreadPoolBuilder& set_min_threads(int min_threads);
  ThreadPoolBuilder& set_max_threads(int max_threads);
  ThreadPoolBuilder& set_max_queue_size(int max_queue_size);
  ThreadPoolBuilder& set_idle_timeout(const MonoDelta& idle_timeout);
  ThreadPoolBuilder& set_queue_overload_threshold(const MonoDelta& threshold);
  ThreadPoolBuilder& set_metrics(ThreadPoolMetrics metrics);
  ThreadPoolBuilder& set_enable_scheduler();
  ThreadPoolBuilder& set_schedule_period_ms(uint32_t schedule_period_ms);

  // Instantiate a new ThreadPool with the existing builder arguments.
  Status Build(std::unique_ptr<ThreadPool>* pool) const;

 private:
  friend class ThreadPool;
  const std::string name_;
  std::string trace_metric_prefix_;
  int min_threads_;
  int max_threads_;
  int max_queue_size_;
  MonoDelta idle_timeout_;
  MonoDelta queue_overload_threshold_;
  ThreadPoolMetrics metrics_;
  bool enable_scheduler_;
  uint32_t schedule_period_ms_;

  DISALLOW_COPY_AND_ASSIGN(ThreadPoolBuilder);
};


// SchedulerThread for asynchronized delay task execution.
class SchedulerThread {
public:
  explicit SchedulerThread(std::string thread_pool_name, uint32_t schedule_period_ms);

  ~SchedulerThread();

  // Start a thread to judge which tasks can be execute now.
  Status Start();

  // Shutdown the thread and clear the pending tasks.
  Status Shutdown();

  struct SchedulerTask {
    ThreadPoolToken* thread_pool_token_;
    std::function<void()> f;
  };

  void Schedule(ThreadPoolToken* token, std::function<void()> f, const MonoTime& execute_time) {
    MutexLock unique_lock(mutex_);
    tasks_.insert({execute_time, SchedulerTask({token, std::move(f)})});
  }

  bool empty() const {
    MutexLock unique_lock(mutex_);
    return tasks_.empty();
  }

private:
  friend class ThreadPool;
  friend class ThreadPoolToken;

  void RunLoop();

  const std::string thread_pool_name_;
  // scheduler's period checking time.
  const uint32_t schedule_period_ms_;
  CountDownLatch shutdown_;
  // Protect `tasks_` data race.
  mutable Mutex mutex_;

  scoped_refptr<Thread> thread_;
  std::multimap<MonoTime, SchedulerTask> tasks_;
};

// Thread pool with a variable number of threads.
//
// Tasks submitted directly to the thread pool enter a FIFO queue and are
// dispatched to a worker thread when one becomes free. Tasks may also be
// submitted via ThreadPoolTokens. The token Wait() and Shutdown() functions
// can then be used to block on logical groups of tasks.
//
// A token operates in one of two ExecutionModes, determined at token
// construction time:
// 1. SERIAL: submitted tasks are run one at a time.
// 2. CONCURRENT: submitted tasks may be run in parallel. This isn't unlike
//    tasks submitted without a token, but the logical grouping that tokens
//    impart can be useful when a pool is shared by many contexts (e.g. to
//    safely shut down one context, to derive context-specific metrics, etc.).
//
// Tasks submitted without a token or via ExecutionMode::CONCURRENT tokens are
// processed in FIFO order. On the other hand, ExecutionMode::SERIAL tokens are
// processed in a round-robin fashion, one task at a time. This prevents them
// from starving one another. However, tokenless (and CONCURRENT token-based)
// tasks can starve SERIAL token-based tasks.
//
// Usage Example:
//    static void Func(int n) { ... }
//
//    unique_ptr<ThreadPool> thread_pool;
//    CHECK_OK(
//        ThreadPoolBuilder("my_pool")
//            .set_min_threads(0)
//            .set_max_threads(5)
//            .set_max_queue_size(10)
//            .set_idle_timeout(MonoDelta::FromMilliseconds(2000))
//            .Build(&thread_pool));
//    thread_pool->Submit([](){ Func(10) });
class ThreadPool {
 public:
  ~ThreadPool();

  // Wait for the running tasks to complete and then shutdown the threads.
  // All the other pending tasks in the queue will be removed.
  // NOTE: That the user may implement an external abort logic for the
  //       runnables, that must be called before Shutdown(), if the system
  //       should know about the non-execution of these tasks, or the runnable
  //       require an explicit "abort" notification to exit from the run loop.
  void Shutdown();

  // Submits a new task.
  Status Submit(std::function<void()> f) WARN_UNUSED_RESULT;

  // Waits until all the tasks are completed.
  void Wait();

  // Waits for the pool to reach the idle state, or until 'until' time is reached.
  // Returns true if the pool reached the idle state, false otherwise.
  bool WaitUntil(const MonoTime& until);

  // Waits for the pool to reach the idle state, or until 'delta' time elapses.
  // Returns true if the pool reached the idle state, false otherwise.
  bool WaitFor(const MonoDelta& delta);

  // Allocates a new token for use in token-based task submission. All tokens
  // must be destroyed before their ThreadPool is destroyed.
  //
  // There is no limit on the number of tokens that may be allocated.
  enum class ExecutionMode {
    // Tasks submitted via this token will be executed serially.
    SERIAL,

    // Tasks submitted via this token may be executed concurrently.
    CONCURRENT,
  };
  std::unique_ptr<ThreadPoolToken> NewToken(ExecutionMode mode);

  // Like NewToken(), but lets the caller provide metrics for the token. These
  // metrics are incremented/decremented in addition to the configured
  // pool-wide metrics (if any).
  std::unique_ptr<ThreadPoolToken> NewTokenWithMetrics(ExecutionMode mode,
                                                       ThreadPoolMetrics metrics);

  // Return the number of threads currently running (or in the process of starting up)
  // for this thread pool.
  int num_threads() const {
    MutexLock l(lock_);
    return num_threads_ + num_threads_pending_start_;
  }

  // Whether the ThreadPool's queue is overloaded. If queue overload threshold
  // isn't set, returns 'false'. Otherwise, returns whether the queue is
  // overloaded. If overloaded, 'overloaded_time' and 'threshold' are both
  // populated with corresponding information (if not null).
  bool QueueOverloaded(MonoDelta* overloaded_time = nullptr,
                       MonoDelta* threshold = nullptr) const;

 private:
  FRIEND_TEST(ThreadPoolTest, TestSimpleTasks);
  FRIEND_TEST(ThreadPoolTest, TestThreadPoolWithNoMinimum);
  FRIEND_TEST(ThreadPoolTest, TestThreadPoolWithSchedulerAndNoMinimum);
  FRIEND_TEST(ThreadPoolTest, TestVariableSizeThreadPool);

  friend class ThreadPoolBuilder;
  friend class ThreadPoolToken;

  // Client-provided task to be executed by this pool.
  struct Task {
    std::function<void()> func;
    Trace* trace;

    // Time at which the entry was submitted to the pool.
    MonoTime submit_time;
  };

  // This utility class is used to track how busy the ThreadPool is by
  // monitoring its task queue timings. This class uses logic modeled after
  // CoDel algorithm (see [1], [2], [3]), detecting whether the queue
  // of a thread pool with the maximum of M worker threads is overloaded
  // (it's assumed M > 0). The idea is to keep an eye on the queue times when
  // the thread pool works at its full capacity. Even if there aren't any idle
  // threads, we don't want to declare the queue overloaded when it's still able
  // to dispatch many lightweight tasks pretty fast once they arrived. Instead,
  // we start declaring the queue overloaded only when we see the evidence of
  // the newly arrived tasks being stuck in the queue for a time interval longer
  // than the configured threshold.
  //
  // Let's denote the minimum of the queue times of the last N tasks dispatched
  // by min(QT_historic(N)). If the history of already dispatched tasks is
  // empty, min(QT_historic(N)) is defined to be 0. The time interval that the
  // very first element of the queue has been waiting to be dispatched is
  // denoted by QT_head. The queue time threshold is denoted by T_overload.
  //
  // With that, the criterion to detect the 'overloaded' state of a ThreadPool's
  // queue is defined as the following:
  //
  //   all available worker threads are busy
  //     AND
  //   max(QT_head, min(QT_historic(M))) > T_overload
  //
  // The maximum number of worker threads (M) in a thread pool naturally
  // provides the proper length of the queue time history. To illustrate, let's
  // examine one edge case. It's a case of continuous periodic workload of
  // batches of M tasks, where all M tasks in a batch are scheduled at the same
  // time and batches of M tasks are separated by T_overload time interval.
  // Every batch contain (M - 1) heavyweight tasks and a single lightweight one.
  // Let's assume it takes T_overload to complete a heavyweight task, and a
  // lightweight one completes instantly. With such a workload running against a
  // thread pool, it's able to handle many extra lightweight tasks with
  // resulting queue times well under T_overload threshold. Apparently, the
  // queue should not be declared overloaded in such case. So, if the queue time
  // history length were less than M (e.g. (M - 1)), then the pool would be
  // considered overloaded if capturing a moment when all worker threads are
  // busy handling a newly arrived batch of M tasks, assuming the thread pool
  // has already handled at least two batches of tasks since its start (the
  // history of queue times would be { 0, T_overload, ..., T_overload } repeated
  // many times).
  //
  // From the other side, if the size of the queue time history were greater
  // than M, it would include not-so-relevant information for some patterns
  // of scheduled tasks.
  //
  // References:
  //   [1] https://queue.acm.org/detail.cfm?id=2209336
  //   [2] https://en.wikipedia.org/wiki/CoDel
  //   [3] https://man7.org/linux/man-pages/man8/CoDel.8.html
  class QueueLoadMeter {
   public:
    // Create an instance of QueueLoadMeter class. The pool to attach to
    // is specified by the 'tpool' parameter. The 'queue_time_threshold'
    // parameter corresponds to 'T_overload' parameter from the algorithm
    // description above, and 'queue_time_history_length' corresponds to 'N',
    // respectively.
    explicit QueueLoadMeter(const ThreadPool& tpool,
                            const MonoDelta& queue_time_threshold,
                            size_t queue_time_history_length);

    const MonoDelta& queue_time_threshold() const {
      return queue_time_threshold_;
    }

    // Check whether the queue is overloaded. If the queue is not overloaded,
    // this method returns 'false'. If the queue is overloaded, this method
    // return 'true'. In the latter case, if 'time_overloaded' is not null,
    // it is populated with the information on how long the queue has been
    // in the overloaded state. This method is lock-free.
    bool overloaded(MonoDelta* time_overloaded = nullptr);

    // Notify the meter about updates on the task queue. If a task being
    // dequeued, the queue time of the task dequeued is specified via the
    // 'task_queue_time' parameter, otherwise it's not initialized.
    // Non-initialized 'queue_head_submit_time' means there isn't next task
    // in the queue. The 'has_spare_thread' parameter conveys information
    // on whether a "spare" worker thread is available.
    void UpdateQueueInfoUnlocked(const MonoDelta& task_queue_time,
                                 const MonoTime& queue_head_submit_time,
                                 bool has_spare_thread);
   private:
    // The pool this meter is attached to.
    const ThreadPool& tpool_;

    // The threshold for the minimum queue times when determining whether the
    // thread pool's tasks queue is in overloaded state. This corresponds to the
    // parameter 'T_overload' in the description of the algorithm above.
    const MonoDelta queue_time_threshold_;

    // The measurement window to track task queue times. That's the number
    // of the most recent tasks to check for the queue times. This corresponds
    // to the parameter 'N' in the description of the algorithm above.
    const size_t queue_time_history_length_;

    // Number of elements in the queue history measurement window which are
    // over the threshold specified by 'queue_time_threshold_'. Using the
    // terminology from above, (min(QT_historic(M) > T_overload) iff
    // (over_queue_threshold_num_ == M).
    ssize_t over_queue_threshold_num_;

    // Queue timings of the most recent samples. The size of these containers
    // is kept under queue_time_history_length_ limit.
    std::deque<MonoDelta> queue_times_;

    // Below fields are to store the latest snapshot of the information about
    // the task queue of the pool the meter is attached to.

    // Time when the next queue task was submitted. Set to empty MonoTime() if
    // there isn't a single element in the queue (i.e. Initialized() returns
    // false).
    std::atomic<MonoTime> queue_head_submit_time_;

    // Time when the queue has entered the overloaded state. If the queue isn't
    // in overloaded state, this member field isn't initialized
    // (i.e. overloaded_since.Initialized() returns false).
    std::atomic<MonoTime> overloaded_since_;

    // Whether the TreadPool has a least one "spare" thread: a thread that can
    // be spawned before reaching the maximum allowed number of threads,
    // or one of those already spawned but currently idle.
    std::atomic<bool> has_spare_thread_;
  };

  // Creates a new thread pool using a builder.
  explicit ThreadPool(const ThreadPoolBuilder& builder);

  // Initializes the thread pool by starting the minimum number of threads.
  Status Init();

  // Dispatcher responsible for dequeueing and executing the tasks
  void DispatchThread();

  // Create new thread.
  //
  // REQUIRES: caller has incremented 'num_threads_pending_start_' ahead of this call.
  // NOTE: For performance reasons, lock_ should not be held.
  Status CreateThread();

  // Aborts if the current thread is a member of this thread pool.
  void CheckNotPoolThreadUnlocked();

  // Submits a task to be run via token.
  Status DoSubmit(std::function<void()> f, ThreadPoolToken* token);

  // Releases token 't' and invalidates it.
  void ReleaseToken(ThreadPoolToken* t);

  // Notify the load meter (if enabled) on relevant updates. If no information
  // on dequeued task is available, 'queue_time' should be omitted (or be an
  // uninitialized MonoDelta instance).
  //
  // The LoadMeter should be notified about events which affect the criterion
  // to evaluate the state of the queue (overloaded vs normal). The criterion
  // uses the following information:
  //   * queue time of a newly dequeued task
  //   * availability of spare worker threads
  //   * submit time of the task at the head of the queue
  // This means that LoadMeter should be notified about the following events:
  //  * a task at the head of the queue has been dispatched to be run
  //  * a worker thread completes running a task
  //  * a new task has been scheduled (i.e. added into the queue)
  void NotifyLoadMeterUnlocked(const MonoDelta& queue_time = MonoDelta());

  SchedulerThread* scheduler() {
    return scheduler_;
  }

  // Waits until all the tasks and scheduler's tasks completed.
  void WaitForScheduler();

  // Return the number of threads currently running for this thread pool.
  // Used by tests to avoid tsan test case down.
  int num_active_threads() {
    MutexLock l(lock_);
    return active_threads_;
  }

  const std::string name_;
  const int min_threads_;
  const int max_threads_;
  const int max_queue_size_;
  const MonoDelta idle_timeout_;

  // Overall status of the pool. Set to an error when the pool is shut down.
  //
  // Protected by 'lock_'.
  Status pool_status_;

  // Synchronizes many of the members of the pool and all of its
  // condition variables.
  mutable Mutex lock_;

  // Condition variable for "pool is idling". Waiters wake up when
  // active_threads_ reaches zero.
  ConditionVariable idle_cond_;

  // Condition variable for "pool has no threads". Waiters wake up when
  // num_threads_ and num_pending_threads_ are both 0.
  ConditionVariable no_threads_cond_;

  // Number of threads currently running.
  //
  // Protected by lock_.
  int num_threads_;

  // Number of threads which are in the process of starting.
  // When these threads start, they will decrement this counter and
  // accordingly increment 'num_threads_'.
  //
  // Protected by lock_.
  int num_threads_pending_start_;

  // Number of threads currently running and executing client tasks.
  //
  // Protected by lock_.
  int active_threads_;

  // Total number of client tasks queued, either directly (queue_) or
  // indirectly (tokens_).
  //
  // Protected by lock_.
  int total_queued_tasks_;

  // All allocated tokens.
  //
  // Protected by lock_.
  std::unordered_set<ThreadPoolToken*> tokens_;

  // FIFO of tokens from which tasks should be executed. Does not own the
  // tokens; they are owned by clients and are removed from the FIFO on shutdown.
  //
  // Protected by lock_.
  std::deque<ThreadPoolToken*> queue_;

  // Pointers to all running threads. Raw pointers are safe because a Thread
  // may only go out of scope after being removed from threads_.
  //
  // Protected by lock_.
  std::unordered_set<Thread*> threads_;

  // List of all threads currently waiting for work.
  //
  // A thread is added to the front of the list when it goes idle and is
  // removed from the front and signaled when new work arrives. This produces a
  // LIFO usage pattern that is more efficient than idling on a single
  // ConditionVariable (which yields FIFO semantics).
  //
  // Protected by lock_.
  struct IdleThread : public boost::intrusive::list_base_hook<> {
    explicit IdleThread(Mutex* m)
        : not_empty(m) {}

    // Condition variable for "queue is not empty". Waiters wake up when a new
    // task is queued.
    ConditionVariable not_empty;

    DISALLOW_COPY_AND_ASSIGN(IdleThread);
  };
  boost::intrusive::list<IdleThread> idle_threads_; // NOLINT(build/include_what_you_use)

  // ExecutionMode::CONCURRENT token used by the pool for tokenless submission.
  std::unique_ptr<ThreadPoolToken> tokenless_;

  // The meter to track whether the pool's queue is stalled/overloaded.
  // It's nullptr/none if the queue overload threshold is unset.
  std::unique_ptr<QueueLoadMeter> load_meter_;

  // Metrics for the entire thread pool.
  const ThreadPoolMetrics metrics_;

  // TimerThread is used for some scenarios, such as
  // make a task delay execution.
  SchedulerThread* scheduler_;
  uint32_t schedule_period_ms_;

  bool enable_scheduler_;

  const char* queue_time_trace_metric_name_;
  const char* run_wall_time_trace_metric_name_;
  const char* run_cpu_time_trace_metric_name_;

  DISALLOW_COPY_AND_ASSIGN(ThreadPool);
};

// Entry point for token-based task submission and blocking for a particular
// thread pool. Tokens can only be created via ThreadPool::NewToken().
//
// All functions are thread-safe. Mutable members are protected via the
// ThreadPool's lock.
class ThreadPoolToken {
 public:
  // Destroys the token.
  //
  // May be called on a token with outstanding tasks, as Shutdown() will be
  // called first to take care of them.
  ~ThreadPoolToken();

  // Submits a new task.
  Status Submit(std::function<void()> f) WARN_UNUSED_RESULT;

  // Submit a task, execute the task after delay_ms later.
  Status Schedule(std::function<void()> f, int64_t delay_ms) WARN_UNUSED_RESULT;

  // Marks the token as unusable for future submissions. Any queued tasks not
  // yet running are destroyed. If tasks are in flight, Shutdown() will wait
  // on their completion before returning.
  void Shutdown();

  // Waits until all the tasks submitted via this token are completed.
  void Wait();

  // Waits for all submissions using this token are complete, or until 'until'
  // time is reached.
  //
  // Returns true if all submissions are complete, false otherwise.
  bool WaitUntil(const MonoTime& until);

  // Waits for all submissions using this token are complete, or until 'delta'
  // time elapses.
  //
  // Returns true if all submissions are complete, false otherwise.
  bool WaitFor(const MonoDelta& delta);

 private:
  friend class SchedulerThread;
  // All possible token states. Legal state transitions:
  //   IDLE      -> RUNNING: task is submitted via token
  //   IDLE      -> QUIESCED: token or pool is shut down
  //   RUNNING   -> IDLE: worker thread finishes executing a task and
  //                      there are no more tasks queued to the token
  //   RUNNING   -> QUIESCING: token or pool is shut down while worker thread
  //                           is executing a task
  //   RUNNING   -> QUIESCED: token or pool is shut down
  //   QUIESCING -> QUIESCED:  worker thread finishes executing a task
  //                           belonging to a shut down token or pool
  enum class State {
    // Token has no queued tasks.
    IDLE,

    // A worker thread is running one of the token's previously queued tasks.
    RUNNING,

    // No new tasks may be submitted to the token. A worker thread is still
    // running a previously queued task.
    QUIESCING,

    // No new tasks may be submitted to the token. There are no active tasks
    // either. At this state, the token may only be destroyed.
    QUIESCED,
  };

  // Writes a textual representation of the token state in 's' to 'o'.
  friend std::ostream& operator<<(std::ostream& o, ThreadPoolToken::State s);

  friend class ThreadPool;

  // Returns a textual representation of 's' suitable for debugging.
  static const char* StateToString(State s);

  // Constructs a new token.
  //
  // The token may not outlive its thread pool ('pool').
  ThreadPoolToken(ThreadPool* pool,
                  ThreadPool::ExecutionMode mode,
                  ThreadPoolMetrics metrics);

  // Changes this token's state to 'new_state' taking actions as needed.
  void Transition(State new_state);

  // Returns true if this token has a task queued and ready to run, or if a
  // task belonging to this token is already running.
  bool IsActive() const {
    return state_ == State::RUNNING ||
           state_ == State::QUIESCING;
  }

  // Returns true if new tasks may be submitted to this token.
  bool MaySubmitNewTasks() const {
    return state_ != State::QUIESCING &&
           state_ != State::QUIESCED;
  }

  State state() const { return state_; }
  ThreadPool::ExecutionMode mode() const { return mode_; }

  // Token's configured execution mode.
  const ThreadPool::ExecutionMode mode_;

  // Metrics for just this token.
  const ThreadPoolMetrics metrics_;

  // Pointer to the token's thread pool.
  ThreadPool* pool_;

  // Token state machine.
  State state_;

  // Queued client tasks.
  std::deque<ThreadPool::Task> entries_;

  // Condition variable for "token is idle". Waiters wake up when the token
  // transitions to IDLE or QUIESCED.
  ConditionVariable not_running_cond_;

  // Number of worker threads currently executing tasks belonging to this
  // token.
  int active_threads_;

  DISALLOW_COPY_AND_ASSIGN(ThreadPoolToken);
};

} // namespace kudu
