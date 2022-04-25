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

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <boost/smart_ptr/shared_ptr.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/barrier.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/promise.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/trace.h"

using std::atomic;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;

using strings::Substitute;

DECLARE_int32(thread_inject_start_latency_ms);

namespace kudu {

static const char* kDefaultPoolName = "test";

class ThreadPoolTest : public KuduTest {
 public:

  virtual void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(ThreadPoolBuilder(kDefaultPoolName).Build(&pool_));
  }

  Status RebuildPoolWithBuilder(const ThreadPoolBuilder& builder) {
    return builder.Build(&pool_);
  }

  Status RebuildPoolWithMinMax(int min_threads, int max_threads) {
    return ThreadPoolBuilder(kDefaultPoolName)
        .set_min_threads(min_threads)
        .set_max_threads(max_threads)
        .Build(&pool_);
  }

  Status RebuildPoolWithScheduler(int min_threads, int max_threads) {
    return ThreadPoolBuilder(kDefaultPoolName)
        .set_min_threads(min_threads)
        .set_max_threads(max_threads)
        .set_enable_scheduler()
        .set_schedule_period_ms(100)
        .Build(&pool_);
  }

 protected:
  unique_ptr<ThreadPool> pool_;
};

TEST_F(ThreadPoolTest, TestNoTaskOpenClose) {
  ASSERT_OK(RebuildPoolWithMinMax(4, 4));
  pool_->Shutdown();
  ASSERT_OK(RebuildPoolWithScheduler(4, 4));
  pool_->Shutdown();
}

static void SimpleTaskMethod(int n, Atomic32* counter) {
  while (n--) {
    base::subtle::NoBarrier_AtomicIncrement(counter, 1);
    boost::detail::yield(n);
  }
}

class SimpleTask {
 public:
  SimpleTask(int n, Atomic32* counter)
    : n_(n), counter_(counter) {
  }

  void Run() {
    SimpleTaskMethod(n_, counter_);
  }

 private:
  int n_;
  Atomic32* counter_;
};

TEST_F(ThreadPoolTest, TestSimpleTasks) {
  constexpr int kDelayMs = 1000;

  ASSERT_OK(RebuildPoolWithMinMax(4, 4));

  Atomic32 counter(0);
  SimpleTask task(15, &counter);

  ASSERT_OK(pool_->Submit([&counter]() { SimpleTaskMethod(10, &counter); }));
  ASSERT_OK(pool_->Submit([&task]() { task.Run(); }));
  ASSERT_OK(pool_->Submit([&counter]() { SimpleTaskMethod(20, &counter); }));
  ASSERT_OK(pool_->Submit([&task]() { task.Run(); }));
  ASSERT_OK(pool_->Submit([&counter]() { SimpleTaskMethod(123, &counter); }));
  pool_->Wait();
  ASSERT_EQ(10 + 15 + 20 + 15 + 123, base::subtle::NoBarrier_Load(&counter));

  ASSERT_OK(RebuildPoolWithScheduler(4, 4));
  unique_ptr<ThreadPoolToken> token = pool_->NewToken(ThreadPool::ExecutionMode::SERIAL);
  MonoTime start = MonoTime::Now();
  ASSERT_OK(token->Schedule([&counter]() {
    SimpleTaskMethod(13, &counter);
  }, kDelayMs));

  pool_->WaitForScheduler();
  ASSERT_EQ(10 + 15 + 20 + 15 + 123 + 13, base::subtle::NoBarrier_Load(&counter));
  MonoDelta delta = MonoTime::Now() - start;
  MonoDelta expect_upper_limit = MonoDelta::FromMilliseconds(static_cast<int>(kDelayMs * 1.2));
  MonoDelta expect_lower_limit = MonoDelta::FromMilliseconds(static_cast<int>(kDelayMs * 0.9));
  ASSERT_TRUE(delta.MoreThan(expect_lower_limit) && delta.LessThan(expect_upper_limit));
}

static void IssueTraceStatement() {
  TRACE("hello from task");
}

// Test that the thread-local trace is propagated to tasks
// submitted to the threadpool.
TEST_F(ThreadPoolTest, TestTracePropagation) {
  ASSERT_OK(RebuildPoolWithMinMax(1, 1));

  scoped_refptr<Trace> t(new Trace);
  {
    ADOPT_TRACE(t.get());
    ASSERT_OK(pool_->Submit(&IssueTraceStatement));
  }
  pool_->Wait();
  ASSERT_STR_CONTAINS(t->DumpToString(), "hello from task");
}

TEST_F(ThreadPoolTest, TestSubmitAfterShutdown) {
  ASSERT_OK(RebuildPoolWithMinMax(1, 1));
  pool_->Shutdown();
  Status s = pool_->Submit(&IssueTraceStatement);
  ASSERT_EQ("Service unavailable: The pool has been shut down.",
            s.ToString());

  ASSERT_OK(RebuildPoolWithScheduler(1, 1));
  unique_ptr<ThreadPoolToken> token = pool_->NewToken(ThreadPool::ExecutionMode::SERIAL);
  pool_->Shutdown();
  ASSERT_TRUE(token->Schedule(&IssueTraceStatement, 1000).IsServiceUnavailable());
}

TEST_F(ThreadPoolTest, TestThreadPoolWithNoMinimum) {
  constexpr int kIdleTimeoutMs = 1;
  ASSERT_OK(RebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName)
      .set_min_threads(0)
      .set_max_threads(3)
      .set_idle_timeout(MonoDelta::FromMilliseconds(kIdleTimeoutMs))));

  // There are no threads to start with.
  ASSERT_TRUE(pool_->num_threads() == 0);
  // We get up to 3 threads when submitting work.
  CountDownLatch latch(1);
  SCOPED_CLEANUP({
    latch.CountDown();
  });
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(2, pool_->num_threads());
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(3, pool_->num_threads());
  // The 4th piece of work gets queued.
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(3, pool_->num_threads());
  // Finish all work
  latch.CountDown();
  pool_->Wait();
  ASSERT_EQ(0, pool_->active_threads_);
  // Wait for more than idle timeout: so threads should be gone since
  // min_threads is set to 0.
  SleepFor(MonoDelta::FromMilliseconds(10 * kIdleTimeoutMs));
  ASSERT_EQ(0, pool_->num_threads());
  ASSERT_EQ(0, pool_->active_threads_);
  pool_->Shutdown();
  ASSERT_EQ(0, pool_->num_threads());
}

TEST_F(ThreadPoolTest, TestThreadPoolWithSchedulerAndNoMinimum) {
  constexpr int kIdleTimeoutMs = 1;
  constexpr int kDelayMs = 1000;
  ASSERT_OK(RebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName)
      .set_min_threads(0)
      .set_max_threads(4)
      .set_enable_scheduler()
      .set_idle_timeout(MonoDelta::FromMilliseconds(kIdleTimeoutMs))));
  // There are no threads to start with.
  ASSERT_EQ(0, pool_->active_threads_);
  ASSERT_EQ(0, pool_->num_threads());

  CountDownLatch latch(1);
  SCOPED_CLEANUP({
      latch.CountDown();
  });
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(4, pool_->num_threads());
  unique_ptr<ThreadPoolToken> token = pool_->NewToken(ThreadPool::ExecutionMode::SERIAL);
  ASSERT_OK(token->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(4, pool_->num_threads());

  ASSERT_OK(token->Schedule([&latch]() { latch.Wait(); }, kDelayMs));
  ASSERT_OK(token->Schedule([&latch]() { latch.Wait(); }, static_cast<int>(kDelayMs * 1.2)));
  ASSERT_EQ(4, pool_->num_threads());
  latch.CountDown();
  pool_->Wait();

  latch.Reset(1);
  ASSERT_EQ(0, pool_->num_active_threads());
  SleepFor(MonoDelta::FromMilliseconds(static_cast<int>(kDelayMs * 1.5)));
  ASSERT_GT(pool_->num_active_threads(), 0);
  ASSERT_OK(token->Schedule([&latch]() { latch.Wait(); }, kDelayMs));
  latch.CountDown();
  pool_->Wait();
  ASSERT_EQ(0, pool_->num_active_threads());
  SleepFor(MonoDelta::FromMilliseconds(10 * kIdleTimeoutMs));
  ASSERT_EQ(0, pool_->num_threads());
  ASSERT_EQ(0, pool_->num_active_threads());
  pool_->Shutdown();
  ASSERT_EQ(nullptr, pool_->scheduler());
}

TEST_F(ThreadPoolTest, TestThreadPoolWithNoMaxThreads) {
  // By default a threadpool's max_threads is set to the number of CPUs, so
  // this test submits more tasks than that to ensure that the number of CPUs
  // isn't some kind of upper bound.
  const int kNumCPUs = base::NumCPUs();

  // Build a threadpool with no limit on the maximum number of threads.
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_max_threads(std::numeric_limits<int>::max())));
  CountDownLatch latch(1);
  auto cleanup_latch = MakeScopedCleanup([&]() {
    latch.CountDown();
  });

  // Submit tokenless tasks. Each should create a new thread.
  for (int i = 0; i < kNumCPUs * 2; i++) {
    ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  }
  ASSERT_EQ((kNumCPUs * 2), pool_->num_threads());

  // Submit tasks on two tokens. Only two threads should be created.
  unique_ptr<ThreadPoolToken> t1 = pool_->NewToken(ThreadPool::ExecutionMode::SERIAL);
  unique_ptr<ThreadPoolToken> t2 = pool_->NewToken(ThreadPool::ExecutionMode::SERIAL);
  for (int i = 0; i < kNumCPUs * 2; i++) {
    ThreadPoolToken* t = (i % 2 == 0) ? t1.get() : t2.get();
    ASSERT_OK(t->Submit([&latch]() { latch.Wait(); }));
  }
  ASSERT_EQ((kNumCPUs * 2) + 2, pool_->num_threads());

  // Submit more tokenless tasks. Each should create a new thread.
  for (int i = 0; i < kNumCPUs; i++) {
    ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  }
  ASSERT_EQ((kNumCPUs * 3) + 2, pool_->num_threads());

  latch.CountDown();
  pool_->Wait();
}

// Regression test for a bug where a task is submitted exactly
// as a thread is about to exit. Previously this could hang forever.
TEST_F(ThreadPoolTest, TestRace) {
  alarm(60);
  auto cleanup = MakeScopedCleanup([]() {
    alarm(0); // Disable alarm on test exit.
  });
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_min_threads(0)
                                   .set_max_threads(1)
                                   .set_idle_timeout(MonoDelta::FromMicroseconds(1))));

  for (int i = 0; i < 500; i++) {
    CountDownLatch l(1);
    ASSERT_OK(pool_->Submit([&l]() { l.CountDown(); }));
    l.Wait();
    // Sleeping a different amount in each iteration makes it more likely to hit
    // the bug.
    SleepFor(MonoDelta::FromMicroseconds(i));
  }
}

TEST_F(ThreadPoolTest, TestVariableSizeThreadPool) {
  constexpr int kIdleTimeoutMs = 1;
  ASSERT_OK(RebuildPoolWithBuilder(
      ThreadPoolBuilder(kDefaultPoolName)
      .set_min_threads(1)
      .set_max_threads(4)
      .set_idle_timeout(MonoDelta::FromMilliseconds(kIdleTimeoutMs))));

  // There is 1 thread to start with.
  ASSERT_EQ(1, pool_->num_threads());
  // We get up to 4 threads when submitting work.
  CountDownLatch latch(1);
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(1, pool_->num_threads());
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(2, pool_->num_threads());
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(3, pool_->num_threads());
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(4, pool_->num_threads());
  // The 5th piece of work gets queued.
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_EQ(4, pool_->num_threads());
  // Finish all work
  latch.CountDown();
  pool_->Wait();
  ASSERT_EQ(0, pool_->active_threads_);
  SleepFor(MonoDelta::FromMilliseconds(10 * kIdleTimeoutMs));
  ASSERT_EQ(0, pool_->active_threads_);
  ASSERT_EQ(1, pool_->num_threads());
  pool_->Shutdown();
  ASSERT_EQ(0, pool_->num_threads());
}

TEST_F(ThreadPoolTest, TestMaxQueueSize) {
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_min_threads(1)
                                   .set_max_threads(1)
                                   .set_max_queue_size(1)));

  CountDownLatch latch(1);
  // We will be able to submit two tasks: one for max_threads == 1 and one for
  // max_queue_size == 1.
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  Status s = pool_->Submit([&latch]() { latch.Wait(); });
  CHECK(s.IsServiceUnavailable()) << "Expected failure due to queue blowout:" << s.ToString();
  latch.CountDown();
  pool_->Wait();
}

// Test that when we specify a zero-sized queue, the maximum number of threads
// running is used for enforcement.
TEST_F(ThreadPoolTest, TestZeroQueueSize) {
  const int kMaxThreads = 4;
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_max_queue_size(0)
                                   .set_max_threads(kMaxThreads)));

  CountDownLatch latch(1);
  for (int i = 0; i < kMaxThreads; i++) {
    ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  }
  Status s = pool_->Submit([&latch]() { latch.Wait(); });
  ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Thread pool is at capacity");
  latch.CountDown();
  pool_->Wait();
}

// Regression test for KUDU-2187:
//
// If a threadpool thread is slow to start up, it shouldn't block progress of
// other tasks on the same pool.
TEST_F(ThreadPoolTest, TestSlowThreadStart) {
  // Start a pool of threads from which we'll submit tasks.
  unique_ptr<ThreadPool> submitter_pool;
  ASSERT_OK(ThreadPoolBuilder("submitter")
            .set_min_threads(5)
            .set_max_threads(5)
            .Build(&submitter_pool));

  // Start the actual test pool, which starts with one thread
  // but will start a second one on-demand.
  ASSERT_OK(RebuildPoolWithMinMax(1, 2));
  // Ensure that the second thread will take a long time to start.
  FLAGS_thread_inject_start_latency_ms = 3000;

  // Now submit 10 tasks to the 'submitter' pool, each of which
  // submits a single task to 'pool_'. The 'pool_' task sleeps
  // for 10ms.
  //
  // Because the 'submitter' tasks submit faster than they can be
  // processed on a single thread (due to the sleep), we expect that
  // this will trigger 'pool_' to start up its second worker thread.
  // The thread startup will have some latency injected.
  //
  // We expect that the thread startup will block only one of the
  // tasks in the 'submitter' pool after it submits its task. Other
  // tasks will continue to be processed by the other (already-running)
  // thread on 'pool_'.
  std::atomic<int32_t> total_queue_time_ms(0);
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(submitter_pool->Submit([&]() {
          auto submit_time = MonoTime::Now();
          CHECK_OK(pool_->Submit([&,submit_time]() {
                auto queue_time = MonoTime::Now() - submit_time;
                total_queue_time_ms += queue_time.ToMilliseconds();
                SleepFor(MonoDelta::FromMilliseconds(10));
              }));
        }));
  }
  submitter_pool->Wait();
  pool_->Wait();

  // Since the total amount of work submitted was only 100ms, we expect
  // that the performance would be equivalent to a single-threaded
  // threadpool. So, we expect the total queue time to be approximately
  // 0 + 10 + 20 ... + 80 + 90 = 450ms.
  //
  // If, instead, throughput had been blocked while starting threads,
  // we'd get something closer to 18000ms (3000ms delay * 5 submitter threads).
  ASSERT_GE(total_queue_time_ms, 400);
  ASSERT_LE(total_queue_time_ms, 10000);
}

// Test that setting a promise from another thread yields
// a value on the current thread.
TEST_F(ThreadPoolTest, TestPromises) {
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_min_threads(1)
                                   .set_max_threads(1)
                                   .set_max_queue_size(1)));

  Promise<int> my_promise;
  ASSERT_OK(pool_->Submit([&my_promise]() { my_promise.Set(5); }));
  ASSERT_EQ(5, my_promise.Get());
}

METRIC_DEFINE_entity(test_entity);
METRIC_DEFINE_histogram(test_entity, queue_length, "queue length",
                        MetricUnit::kTasks, "queue length",
                        kudu::MetricLevel::kInfo,
                        1000, 1);

METRIC_DEFINE_histogram(test_entity, queue_time, "queue time",
                        MetricUnit::kMicroseconds, "queue time",
                        kudu::MetricLevel::kInfo,
                        1000000, 1);

METRIC_DEFINE_histogram(test_entity, run_time, "run time",
                        MetricUnit::kMicroseconds, "run time",
                        kudu::MetricLevel::kInfo,
                        1000, 1);

TEST_F(ThreadPoolTest, TestMetrics) {
  MetricRegistry registry;
  vector<ThreadPoolMetrics> all_metrics;
  for (int i = 0; i < 3; i++) {
    scoped_refptr<MetricEntity> entity = METRIC_ENTITY_test_entity.Instantiate(
        &registry, Substitute("test $0", i));
    all_metrics.emplace_back(ThreadPoolMetrics{
      METRIC_queue_length.Instantiate(entity),
      METRIC_queue_time.Instantiate(entity),
      METRIC_run_time.Instantiate(entity)
    });
  }

  // Enable metrics for the thread pool.
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_min_threads(1)
                                   .set_max_threads(1)
                                   .set_metrics(all_metrics[0])));

  unique_ptr<ThreadPoolToken> t1 = pool_->NewTokenWithMetrics(
      ThreadPool::ExecutionMode::SERIAL, all_metrics[1]);
  unique_ptr<ThreadPoolToken> t2 = pool_->NewTokenWithMetrics(
      ThreadPool::ExecutionMode::SERIAL, all_metrics[2]);

  // Submit once to t1, twice to t2, and three times without a token.
  ASSERT_OK(t1->Submit([](){}));
  ASSERT_OK(t2->Submit([](){}));
  ASSERT_OK(t2->Submit([](){}));
  ASSERT_OK(pool_->Submit([](){}));
  ASSERT_OK(pool_->Submit([](){}));
  ASSERT_OK(pool_->Submit([](){}));
  pool_->Wait();

  // The total counts should reflect the number of submissions to each token.
  ASSERT_EQ(1, all_metrics[1].queue_length_histogram->TotalCount());
  ASSERT_EQ(1, all_metrics[1].queue_time_us_histogram->TotalCount());
  ASSERT_EQ(1, all_metrics[1].run_time_us_histogram->TotalCount());
  ASSERT_EQ(2, all_metrics[2].queue_length_histogram->TotalCount());
  ASSERT_EQ(2, all_metrics[2].queue_time_us_histogram->TotalCount());
  ASSERT_EQ(2, all_metrics[2].run_time_us_histogram->TotalCount());

  // And the counts on the pool-wide metrics should reflect all submissions.
  ASSERT_EQ(6, all_metrics[0].queue_length_histogram->TotalCount());
  ASSERT_EQ(6, all_metrics[0].queue_time_us_histogram->TotalCount());
  ASSERT_EQ(6, all_metrics[0].run_time_us_histogram->TotalCount());
}

// Test scenario to verify the functionality of the QueueLoadMeter.
TEST_F(ThreadPoolTest, QueueLoadMeter) {
  const auto kQueueTimeThresholdMs = 100;
  const auto kIdleThreadTimeoutMs = 200;
  constexpr auto kMaxThreads = 3;
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
      .set_min_threads(0)
      .set_max_threads(kMaxThreads)
      .set_queue_overload_threshold(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs))
      .set_idle_timeout(MonoDelta::FromMilliseconds(kIdleThreadTimeoutMs))));
  // An idle pool must not have its queue overloaded.
  ASSERT_FALSE(pool_->QueueOverloaded());

  // One instant tasks cannot make pool's queue overloaded.
  ASSERT_OK(pool_->Submit([](){}));
  ASSERT_FALSE(pool_->QueueOverloaded());
  pool_->Wait();
  ASSERT_FALSE(pool_->QueueOverloaded());

  for (auto i = 0; i < kMaxThreads; ++i) {
    ASSERT_OK(pool_->Submit([](){
      SleepFor(MonoDelta::FromMilliseconds(2 * kQueueTimeThresholdMs));
    }));
  }
  ASSERT_FALSE(pool_->QueueOverloaded());
  pool_->Wait();
  ASSERT_FALSE(pool_->QueueOverloaded());

  for (auto i = 0; i < 2 * kMaxThreads; ++i) {
    ASSERT_OK(pool_->Submit([](){
      SleepFor(MonoDelta::FromMilliseconds(2 * kQueueTimeThresholdMs));
    }));
  }
  ASSERT_FALSE(pool_->QueueOverloaded());
  SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs + 10));
  ASSERT_TRUE(pool_->QueueOverloaded());
  // Should still be overloaded after first kMaxThreads tasks are processed.
  SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs + 10));
  ASSERT_TRUE(pool_->QueueOverloaded());
  pool_->Wait();
  ASSERT_FALSE(pool_->QueueOverloaded());

  // Many instant tasks cannot make pool overloaded.
  for (auto i = 0; i < kMaxThreads; ++i) {
    ASSERT_OK(pool_->Submit([](){}));
  }
  ASSERT_FALSE(pool_->QueueOverloaded());
  pool_->Wait();
  // For for the threads to be shutdown due to inactivity.
  SleepFor(MonoDelta::FromMilliseconds(2 * kIdleThreadTimeoutMs));
  // Even if all threads are shutdown, an idle pool with empty queue should not
  // be overloaded.
  ASSERT_FALSE(pool_->QueueOverloaded());

  // Shovel some light tasks once again: this should not overload the queue.
  for (auto i = 0; i < 10 * kMaxThreads; ++i) {
    ASSERT_OK(pool_->Submit([](){
      SleepFor(MonoDelta::FromMilliseconds(1));
    }));
  }
  ASSERT_FALSE(pool_->QueueOverloaded());
  pool_->Wait();
  ASSERT_FALSE(pool_->QueueOverloaded());

  // Submit a bunch of instant tasks via a single token: the queue should not
  // become overloaded.
  {
    unique_ptr<ThreadPoolToken> t = pool_->NewToken(
        ThreadPool::ExecutionMode::SERIAL);
    ASSERT_OK(t->Submit([](){}));
    ASSERT_FALSE(pool_->QueueOverloaded());
    pool_->Wait();
    ASSERT_FALSE(pool_->QueueOverloaded());

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(t->Submit([](){}));
    }
    ASSERT_FALSE(pool_->QueueOverloaded());
    SleepFor(MonoDelta::FromMilliseconds(1));
    ASSERT_FALSE(pool_->QueueOverloaded());
    pool_->Wait();
    ASSERT_FALSE(pool_->QueueOverloaded());
  }

  // Submit many instant tasks via multiple tokens (more than the maximum
  // number of worker threads in a pool) and many lightweight tasks which can
  // run concurrently: the queue should not become overloaded.
  {
    constexpr auto kNumTokens = 2 * kMaxThreads;
    vector<unique_ptr<ThreadPoolToken>> tokens;
    tokens.reserve(kNumTokens);
    for (auto i = 0; i < kNumTokens; ++i) {
      tokens.emplace_back(pool_->NewToken(ThreadPool::ExecutionMode::SERIAL));
    }

    for (auto& t : tokens) {
      for (auto i = 0; i < 50; ++i) {
        ASSERT_OK(t->Submit([](){}));
      }
      for (auto i = 0; i < 10; ++i) {
        ASSERT_OK(pool_->Submit([](){}));
      }
    }
    ASSERT_FALSE(pool_->QueueOverloaded());
    SleepFor(MonoDelta::FromMilliseconds(1));
    ASSERT_FALSE(pool_->QueueOverloaded());
    pool_->Wait();
    ASSERT_FALSE(pool_->QueueOverloaded());
  }

  // Submit many long running tasks via serial tokens where the number of tokens
  // is less than the maximum number of worker threads in the pool. The queue
  // of the pool should not become overloaded since the pool has one spare
  // thread to spawn.
  {
    constexpr auto kNumTokens = kMaxThreads - 1;
    ASSERT_GT(kNumTokens, 0);
    vector<unique_ptr<ThreadPoolToken>> tokens;
    tokens.reserve(kNumTokens);
    for (auto i = 0; i < kNumTokens; ++i) {
      tokens.emplace_back(pool_->NewToken(ThreadPool::ExecutionMode::SERIAL));
    }

    ASSERT_FALSE(pool_->QueueOverloaded());
    for (auto& t : tokens) {
      for (auto i = 0; i < kMaxThreads; ++i) {
        ASSERT_OK(t->Submit([](){
          SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs));
        }));
      }
    }
    ASSERT_FALSE(pool_->QueueOverloaded());
    SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs));
    ASSERT_FALSE(pool_->QueueOverloaded());
    pool_->Wait();
    ASSERT_FALSE(pool_->QueueOverloaded());
  }

  // Submit many long running tasks via serial tokens where the number of tokens
  // is greater or equal to the maximum number of worker threads in the pool.
  // The queue of the pool should become overloaded since the pool is running
  // at its capacity and queue times are over the threshold.
  {
    constexpr auto kNumTokens = kMaxThreads;
    vector<unique_ptr<ThreadPoolToken>> tokens;
    tokens.reserve(kNumTokens);
    for (auto i = 0; i < kNumTokens; ++i) {
      tokens.emplace_back(pool_->NewToken(ThreadPool::ExecutionMode::SERIAL));
    }

    ASSERT_FALSE(pool_->QueueOverloaded());
    for (auto& t : tokens) {
      for (auto i = 0; i < kMaxThreads; ++i) {
        ASSERT_OK(t->Submit([](){
          SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs));
        }));
      }
    }
    // Since there is exactly kMaxThreads serial pool tokens with tasks,
    // the queue is empty most of the time. This is because active serial tokens
    // are not kept in the queue. So, the status of the queue cannot be reliably
    // determined by peeking at the submission times of the elements in the
    // queue. Then the only way to detect overload of the queue is the history
    // of queue times. The latter will reflect long queue times only after
    // processing two tasks in each of the serial tokens. So, it's expected
    // to get a stable report on the queue status only after two
    // kQueueTimeThresholdMs time intervals.
    SleepFor(MonoDelta::FromMilliseconds(2 * kQueueTimeThresholdMs));
    ASSERT_TRUE(pool_->QueueOverloaded());
    pool_->Wait();
    ASSERT_FALSE(pool_->QueueOverloaded());
  }

  // A mixed case: submit many long running tasks via serial tokens where the
  // number of tokens is less than the maximum number of worker threads in the
  // pool and submit many instant tasks that can run concurrently.
  {
    constexpr auto kNumTokens = kMaxThreads - 1;
    ASSERT_GT(kNumTokens, 0);
    vector<unique_ptr<ThreadPoolToken>> tokens;
    tokens.reserve(kNumTokens);
    for (auto i = 0; i < kNumTokens; ++i) {
      tokens.emplace_back(pool_->NewToken(ThreadPool::ExecutionMode::SERIAL));
    }

    ASSERT_FALSE(pool_->QueueOverloaded());
    for (auto& t : tokens) {
      for (auto i = 0; i < kMaxThreads; ++i) {
        ASSERT_OK(t->Submit([](){
          SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs));
        }));
      }
    }
    ASSERT_FALSE(pool_->QueueOverloaded());

    // Add several light tasks in addition to the scheduled serial ones. This
    // should not overload the queue.
    for (auto i = 0; i < 10; ++i) {
      ASSERT_OK(pool_->Submit([](){
        SleepFor(MonoDelta::FromMilliseconds(1));
      }));
    }
    ASSERT_FALSE(pool_->QueueOverloaded());
    SleepFor(MonoDelta::FromMilliseconds(1));
    ASSERT_FALSE(pool_->QueueOverloaded());
    SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs));
    ASSERT_FALSE(pool_->QueueOverloaded());
    pool_->Wait();
    ASSERT_FALSE(pool_->QueueOverloaded());
  }

  // Another mixed case: submit many long running tasks via a serial token
  // and many long running tasks that can run concurrently. The queue should
  // become overloaded once the tasks in the head of the queue is kept there
  // for longer than kQueueTimeThresholdMs time interval.
  {
    constexpr auto kNumTokens = 1;
    vector<unique_ptr<ThreadPoolToken>> tokens;
    tokens.reserve(kNumTokens);
    for (auto i = 0; i < kNumTokens; ++i) {
      tokens.emplace_back(pool_->NewToken(ThreadPool::ExecutionMode::SERIAL));
    }

    ASSERT_FALSE(pool_->QueueOverloaded());
    for (auto& t : tokens) {
      for (auto i = 0; i < kMaxThreads; ++i) {
        ASSERT_OK(t->Submit([](){
          SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs));
        }));
      }
    }
    ASSERT_FALSE(pool_->QueueOverloaded());

    // Add the heavy tasks in addition to the scheduled serial ones. The queue
    // should become overloaded after kQueueTimeThresholdMs time interval.
    for (auto i = 0; i < 2 * kMaxThreads; ++i) {
      ASSERT_OK(pool_->Submit([](){
        SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs));
      }));
    }
    SleepFor(MonoDelta::FromMilliseconds(kQueueTimeThresholdMs));
    ASSERT_TRUE(pool_->QueueOverloaded());
    pool_->Wait();
    ASSERT_FALSE(pool_->QueueOverloaded());
  }
}

// A test for various scenarios to assess ThreadPool's performance.
class ThreadPoolPerformanceTest :
    public ThreadPoolTest,
    public testing::WithParamInterface<bool> {
};
INSTANTIATE_TEST_SUITE_P(LoadMeterPresence, ThreadPoolPerformanceTest,
                         ::testing::Values(false, true));

// A scenario to assess ThreadPool's performance in the absence/presence
// of the QueueLoadMeter. The scenario uses a mix of serial and concurrent
// task tokens.
TEST_P(ThreadPoolPerformanceTest, ConcurrentAndSerialTasksMix) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  constexpr auto kNumTasksPerSchedulerThread = 25000;
  const auto kNumCPUs = base::NumCPUs();
  const auto kMaxThreads = std::max(1, kNumCPUs / 2);
  const auto kNumSchedulerThreads = std::max(2, kNumCPUs / 2);
  const auto kNumSerialTokens = kNumSchedulerThreads / 4;
  const auto load_meter_enabled = GetParam();

  ThreadPoolBuilder builder(kDefaultPoolName);
  builder.set_min_threads(kMaxThreads);
  builder.set_max_threads(kMaxThreads);
  if (load_meter_enabled) {
    // The exact value of the queue overload threshold isn't important in this
    // test scenario. With low enough setting and huge number of scheduled
    // tasks, this guarantees that the queue becomes overloaded and all code
    // paths in QueueLoadMeter are covered.
    builder.set_queue_overload_threshold(MonoDelta::FromMilliseconds(1));
  }
  ASSERT_OK(RebuildPoolWithBuilder(builder));

  vector<unique_ptr<ThreadPoolToken>> tokens;
  tokens.reserve(kNumSchedulerThreads);
  for (auto i = 0; i < kNumSchedulerThreads; ++i) {
    tokens.emplace_back(pool_->NewToken(
        i < kNumSerialTokens ? ThreadPool::ExecutionMode::SERIAL
                             : ThreadPool::ExecutionMode::CONCURRENT));
  }

  vector<thread> threads;
  threads.reserve(kNumSchedulerThreads);
  Barrier b(kNumSchedulerThreads + 1);

  for (auto si = 0; si < kNumSchedulerThreads; ++si) {
    threads.emplace_back([&, si]() {
      unique_ptr<ThreadPoolToken> token(pool_->NewToken(
          (si < kNumSerialTokens) ? ThreadPool::ExecutionMode::SERIAL
                                  : ThreadPool::ExecutionMode::CONCURRENT));
      b.Wait();
      for (auto i = 0; i < kNumTasksPerSchedulerThread; ++i) {
        CHECK_OK(token->Submit([](){}));
      }
    });
  }

  Stopwatch sw(Stopwatch::ALL_THREADS);
  b.Wait();
  sw.start();
  pool_->Wait();
  sw.stop();

  for (auto& t : threads) {
    t.join();
  }

  const auto time_elapsed = sw.elapsed();
  LOG(INFO) << Substitute("Processed $0 tasks in $1",
                          kNumSchedulerThreads * kNumTasksPerSchedulerThread,
                          time_elapsed.ToString());
  LOG(INFO) << Substitute(
      "Processing rate (QueueLoadMeter $0): $1 tasks/sec",
      load_meter_enabled ? " enabled" : "disabled",
      static_cast<double>(kNumSchedulerThreads * kNumTasksPerSchedulerThread) /
          time_elapsed.wall_seconds());
}

// Test that a thread pool will crash if asked to run its own blocking
// functions in a pool thread.
//
// In a multi-threaded application, TSAN is unsafe to use following a fork().
// After a fork(), TSAN will:
// 1. Disable verification, expecting an exec() soon anyway, and
// 2. Die on future thread creation.
// For some reason, this test triggers behavior #2. We could disable it with
// the TSAN option die_after_fork=0, but this can (supposedly) lead to
// deadlocks, so we'll disable the entire test instead.
#ifndef THREAD_SANITIZER
TEST_F(ThreadPoolTest, TestDeadlocks) {
  const char* death_msg = "called pool function that would result in deadlock";
  ASSERT_DEATH({
    ASSERT_OK(RebuildPoolWithMinMax(1, 1));
    ASSERT_OK(pool_->Submit([this]() { this->pool_->Shutdown(); } ));
    pool_->Wait();
  }, death_msg);

  ASSERT_DEATH({
    ASSERT_OK(RebuildPoolWithMinMax(1, 1));
    ASSERT_OK(pool_->Submit([this]() { this->pool_->Wait(); } ));
    pool_->Wait();
  }, death_msg);
}
#endif

class SlowDestructorRunnable {
 public:
  void Run() {}

  ~SlowDestructorRunnable() {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
};

// Test that if a tasks's destructor is slow, it doesn't cause serialization of the tasks
// in the queue.
TEST_F(ThreadPoolTest, TestSlowDestructor) {
  ASSERT_OK(RebuildPoolWithMinMax(1, 20));
  MonoTime start = MonoTime::Now();
  for (int i = 0; i < 100; i++) {
    // In this particular test, it's important that the task's destructor (and
    // thus the last ref of 'task') be dropped by the threadpool worker thread
    // itself, so that the delay is incurred by that thread and not the task
    // submission thread. Without C++14 capture-by-move semantics we have to
    // work a little bit harder to accomplish that.
    auto task = make_shared<SlowDestructorRunnable>();
    auto wrapper = [task]() { task->Run(); };
    task.reset();
    ASSERT_OK(pool_->Submit(std::move(wrapper)));
  }
  pool_->Wait();
  ASSERT_LT((MonoTime::Now() - start).ToSeconds(), 5);
}

// For test cases that should run with both kinds of tokens.
class ThreadPoolTestTokenTypes : public ThreadPoolTest,
                                 public testing::WithParamInterface<ThreadPool::ExecutionMode> {};

INSTANTIATE_TEST_SUITE_P(Tokens, ThreadPoolTestTokenTypes,
                         ::testing::Values(ThreadPool::ExecutionMode::SERIAL,
                                           ThreadPool::ExecutionMode::CONCURRENT));


TEST_P(ThreadPoolTestTokenTypes, TestTokenSubmitAndWait) {
  unique_ptr<ThreadPoolToken> t = pool_->NewToken(GetParam());
  int i = 0;
  ASSERT_OK(t->Submit([&]() {
    SleepFor(MonoDelta::FromMilliseconds(1));
    i++;
  }));
  t->Wait();
  ASSERT_EQ(1, i);
}

TEST_F(ThreadPoolTest, TestTokenSubmitsProcessedSerially) {
  unique_ptr<ThreadPoolToken> t = pool_->NewToken(ThreadPool::ExecutionMode::SERIAL);
  Random r(SeedRandom());
  string result;
  for (char c = 'a'; c < 'f'; c++) {
    // Sleep a little first so that there's a higher chance of out-of-order
    // appends if the submissions did execute in parallel.
    int sleep_ms = static_cast<int>(r.Uniform(5));
    ASSERT_OK(t->Submit([&result, c, sleep_ms]() {
      SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
      result += c;
    }));
  }
  t->Wait();
  ASSERT_EQ("abcde", result);
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenSubmitsProcessedConcurrently) {
  const int kNumTokens = 5;
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_max_threads(kNumTokens)));
  vector<unique_ptr<ThreadPoolToken>> tokens;

  // A violation to the tested invariant would yield a deadlock, so let's set
  // up an alarm to bail us out.
  alarm(60);
  SCOPED_CLEANUP({
      alarm(0); // Disable alarm on test exit.
  });
  auto b = make_shared<Barrier>(kNumTokens + 1);
  for (int i = 0; i < kNumTokens; i++) {
    tokens.emplace_back(pool_->NewToken(GetParam()));
    ASSERT_OK(tokens.back()->Submit([b]() {
      b->Wait();
    }));
  }

  // This will deadlock if the above tasks weren't all running concurrently.
  b->Wait();
}

TEST_F(ThreadPoolTest, TestTokenSubmitsNonSequential) {
  const int kNumSubmissions = 5;
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_max_threads(kNumSubmissions)));

  // A violation to the tested invariant would yield a deadlock, so let's set
  // up an alarm to bail us out.
  alarm(60);
  SCOPED_CLEANUP({
      alarm(0); // Disable alarm on test exit.
  });
  auto b = make_shared<Barrier>(kNumSubmissions + 1);
  unique_ptr<ThreadPoolToken> t = pool_->NewToken(ThreadPool::ExecutionMode::CONCURRENT);
  for (int i = 0; i < kNumSubmissions; i++) {
    ASSERT_OK(t->Submit([b]() {
      b->Wait();
    }));
  }

  // This will deadlock if the above tasks weren't all running concurrently.
  b->Wait();
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenShutdown) {
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_max_threads(4)));

  unique_ptr<ThreadPoolToken> t1(pool_->NewToken(GetParam()));
  unique_ptr<ThreadPoolToken> t2(pool_->NewToken(GetParam()));
  CountDownLatch l1(1);
  CountDownLatch l2(1);

  // A violation to the tested invariant would yield a deadlock, so let's set
  // up an alarm to bail us out.
  alarm(60);
  SCOPED_CLEANUP({
      alarm(0); // Disable alarm on test exit.
  });

  for (int i = 0; i < 3; i++) {
    ASSERT_OK(t1->Submit([&]() {
      l1.Wait();
    }));
  }
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(t2->Submit([&]() {
      l2.Wait();
    }));
  }

  // Unblock all of t1's tasks, but not t2's tasks.
  l1.CountDown();

  // If this also waited for t2's tasks, it would deadlock.
  t1->Shutdown();

  // We can no longer submit to t1 but we can still submit to t2.
  ASSERT_TRUE(t1->Submit([](){}).IsServiceUnavailable());
  ASSERT_OK(t2->Submit([](){}));

  // Unblock t2's tasks.
  l2.CountDown();
  t2->Shutdown();
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenWaitForAll) {
  const int kNumTokens = 3;
  const int kNumSubmissions = 20;
  Random r(SeedRandom());
  vector<unique_ptr<ThreadPoolToken>> tokens;
  for (int i = 0; i < kNumTokens; i++) {
    tokens.emplace_back(pool_->NewToken(GetParam()));
  }

  atomic<int32_t> v(0);
  for (int i = 0; i < kNumSubmissions; i++) {
    // Sleep a little first to raise the likelihood of the test thread
    // reaching Wait() before the submissions finish.
    int sleep_ms = static_cast<int>(r.Uniform(5));

    auto task = [&v, sleep_ms]() {
      SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
      v++;
    };

    // Half of the submissions will be token-less, and half will use a token.
    if (i % 2 == 0) {
      ASSERT_OK(pool_->Submit(task));
    } else {
      int token_idx = static_cast<int>(r.Uniform(tokens.size()));
      ASSERT_OK(tokens[token_idx]->Submit(task));
    }
  }
  pool_->Wait();
  ASSERT_EQ(kNumSubmissions, v);
}

TEST_F(ThreadPoolTest, TestFuzz) {
  const int kNumOperations = 1000;
  Random r(SeedRandom());
  vector<unique_ptr<ThreadPoolToken>> tokens;

  for (int i = 0; i < kNumOperations; i++) {
    // Operation distribution:
    //
    // - Submit without a token: 40%
    // - Submit with a randomly selected token: 35%
    // - Allocate a new token: 10%
    // - Wait on a randomly selected token: 7%
    // - Shutdown a randomly selected token: 4%
    // - Deallocate a randomly selected token: 2%
    // - Wait for all submissions: 2%
    int op = static_cast<int>(r.Uniform(100));
    if (op < 40) {
      // Submit without a token.
      int sleep_ms = static_cast<int>(r.Uniform(5));
      ASSERT_OK(pool_->Submit([sleep_ms]() {
        // Sleep a little first to increase task overlap.
        SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
      }));
    } else if (op < 75) {
      // Submit with a randomly selected token.
      if (tokens.empty()) {
        continue;
      }
      int sleep_ms = static_cast<int>(r.Uniform(5));
      int token_idx = static_cast<int>(r.Uniform(tokens.size()));
      Status s = tokens[token_idx]->Submit([sleep_ms]() {
        // Sleep a little first to increase task overlap.
        SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
      });
      ASSERT_TRUE(s.ok() || s.IsServiceUnavailable());
    } else if (op < 85) {
      // Allocate a token with a randomly selected policy.
      ThreadPool::ExecutionMode mode = r.OneIn(2) ?
          ThreadPool::ExecutionMode::SERIAL :
          ThreadPool::ExecutionMode::CONCURRENT;
      tokens.emplace_back(pool_->NewToken(mode));
    } else if (op < 92) {
      // Wait on a randomly selected token.
      if (tokens.empty()) {
        continue;
      }
      int token_idx = static_cast<int>(r.Uniform(tokens.size()));
      tokens[token_idx]->Wait();
    } else if (op < 96) {
      // Shutdown a randomly selected token.
      if (tokens.empty()) {
        continue;
      }
      int token_idx = static_cast<int>(r.Uniform(tokens.size()));
      tokens[token_idx]->Shutdown();
    } else if (op < 98) {
      // Deallocate a randomly selected token.
      if (tokens.empty()) {
        continue;
      }
      auto it = tokens.begin();
      int token_idx = static_cast<int>(r.Uniform(tokens.size()));
      std::advance(it, token_idx);
      tokens.erase(it);
    } else {
      // Wait on everything.
      ASSERT_LT(op, 100);
      ASSERT_GE(op, 98);
      pool_->Wait();
    }
  }

  // Some test runs will shut down the pool before the tokens, and some won't.
  // Either way should be safe.
  if (r.OneIn(2)) {
    pool_->Shutdown();
  }
}

TEST_P(ThreadPoolTestTokenTypes, TestTokenSubmissionsAdhereToMaxQueueSize) {
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_min_threads(1)
                                   .set_max_threads(1)
                                   .set_max_queue_size(1)));

  CountDownLatch latch(1);
  unique_ptr<ThreadPoolToken> t = pool_->NewToken(GetParam());
  SCOPED_CLEANUP({
    latch.CountDown();
  });
  // We will be able to submit two tasks: one for max_threads == 1 and one for
  // max_queue_size == 1.
  ASSERT_OK(t->Submit([&latch]() { latch.Wait(); }));
  ASSERT_OK(t->Submit([&latch]() { latch.Wait(); }));
  Status s = t->Submit([&latch]() { latch.Wait(); });
  ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
}

TEST_F(ThreadPoolTest, TestTokenConcurrency) {
  const int kNumTokens = 20;
  const int kTestRuntimeSecs = 1;
  const int kCycleThreads = 2;
  const int kShutdownThreads = 2;
  const int kWaitThreads = 2;
  const int kSubmitThreads = 8;

  vector<shared_ptr<ThreadPoolToken>> tokens;
  Random rng(SeedRandom());

  // Protects 'tokens' and 'rng'.
  simple_spinlock lock;

  // Fetch a token from 'tokens' at random.
  auto GetRandomToken = [&]() {
    std::lock_guard<simple_spinlock> l(lock);
    int idx = rng.Uniform(kNumTokens);
    return tokens[idx];
  };

  // Preallocate all of the tokens.
  for (int i = 0; i < kNumTokens; i++) {
    ThreadPool::ExecutionMode mode;
    {
      std::lock_guard<simple_spinlock> l(lock);
      mode = rng.OneIn(2) ?
          ThreadPool::ExecutionMode::SERIAL :
          ThreadPool::ExecutionMode::CONCURRENT;
    }
    tokens.emplace_back(pool_->NewToken(mode).release());
  }

  atomic<int64_t> total_num_tokens_cycled(0);
  atomic<int64_t> total_num_tokens_shutdown(0);
  atomic<int64_t> total_num_tokens_waited(0);
  atomic<int64_t> total_num_tokens_submitted(0);

  CountDownLatch latch(1);
  vector<thread> threads;

  for (int i = 0; i < kCycleThreads; i++) {
    // Pick a token at random and replace it.
    //
    // The replaced token is only destroyed when the last ref is dropped,
    // possibly by another thread.
    threads.emplace_back([&]() {
      int num_tokens_cycled = 0;
      while (latch.count()) {
        {
          std::lock_guard<simple_spinlock> l(lock);
          int idx = rng.Uniform(kNumTokens);
          ThreadPool::ExecutionMode mode = rng.OneIn(2) ?
              ThreadPool::ExecutionMode::SERIAL :
              ThreadPool::ExecutionMode::CONCURRENT;
          tokens[idx] = shared_ptr<ThreadPoolToken>(pool_->NewToken(mode).release());
        }
        num_tokens_cycled++;

        // Sleep a bit, otherwise this thread outpaces the other threads and
        // nothing interesting happens to most tokens.
        SleepFor(MonoDelta::FromMicroseconds(10));
      }
      total_num_tokens_cycled += num_tokens_cycled;
    });
  }

  for (int i = 0; i < kShutdownThreads; i++) {
    // Pick a token at random and shut it down. Submitting a task to a shut
    // down token will return a ServiceUnavailable error.
    threads.emplace_back([&]() {
      int num_tokens_shutdown = 0;
      while (latch.count()) {
        GetRandomToken()->Shutdown();
        num_tokens_shutdown++;
      }
      total_num_tokens_shutdown += num_tokens_shutdown;
    });
  }

  for (int i = 0; i < kWaitThreads; i++) {
    // Pick a token at random and wait for any outstanding tasks.
    threads.emplace_back([&]() {
      int num_tokens_waited  = 0;
      while (latch.count()) {
        GetRandomToken()->Wait();
        num_tokens_waited++;
      }
      total_num_tokens_waited += num_tokens_waited;
    });
  }

  for (int i = 0; i < kSubmitThreads; i++) {
    // Pick a token at random and submit a task to it.
    threads.emplace_back([&]() {
      int num_tokens_submitted = 0;
      Random rng(SeedRandom());
      while (latch.count()) {
        int sleep_ms = static_cast<int>(rng.Uniform(5));
        Status s = GetRandomToken()->Submit([sleep_ms]() {
          // Sleep a little first so that tasks are running during other events.
          SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
        });
        CHECK(s.ok() || s.IsServiceUnavailable());
        num_tokens_submitted++;
      }
      total_num_tokens_submitted += num_tokens_submitted;
    });
  }

  SleepFor(MonoDelta::FromSeconds(kTestRuntimeSecs));
  latch.CountDown();
  for (auto& t : threads) {
    t.join();
  }

  LOG(INFO) << Substitute("Tokens cycled ($0 threads): $1",
                          kCycleThreads, total_num_tokens_cycled.load());
  LOG(INFO) << Substitute("Tokens shutdown ($0 threads): $1",
                          kShutdownThreads, total_num_tokens_shutdown.load());
  LOG(INFO) << Substitute("Tokens waited ($0 threads): $1",
                          kWaitThreads, total_num_tokens_waited.load());
  LOG(INFO) << Substitute("Tokens submitted ($0 threads): $1",
                          kSubmitThreads, total_num_tokens_submitted.load());
}

TEST_F(ThreadPoolTest, TestLIFOThreadWakeUps) {
  const int kNumThreads = 10;

  // Test with a pool that allows for kNumThreads concurrent threads.
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_max_threads(kNumThreads)));

  // Submit kNumThreads slow tasks and unblock them, in order to produce
  // kNumThreads worker threads.
  CountDownLatch latch(1);
  SCOPED_CLEANUP({
    latch.CountDown();
  });
  for (int i = 0; i < kNumThreads; i++) {
    ASSERT_OK(pool_->Submit([&latch]() { latch.Wait(); }));
  }
  ASSERT_EQ(kNumThreads, pool_->num_threads());
  latch.CountDown();
  pool_->Wait();

  // The kNumThreads threads are idle and waiting for the idle timeout.

  // Submit a slow trickle of lightning fast tasks.
  //
  // If the threads are woken up in FIFO order, this trickle is enough to
  // prevent all of them from idling and the AssertEventually will time out.
  //
  // If LIFO order is used, the same thread will be reused for each task and
  // the other threads will eventually time out.
  AssertEventually([&]() {
    ASSERT_OK(pool_->Submit([](){}));
    SleepFor(MonoDelta::FromMilliseconds(10));
    ASSERT_EQ(1, pool_->num_threads());
  }, MonoDelta::FromSeconds(10), AssertBackoff::NONE);
  NO_PENDING_FATALS();
}

} // namespace kudu
