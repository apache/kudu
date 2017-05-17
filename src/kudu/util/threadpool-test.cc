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

#include <memory>
#include <string>

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bind.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/promise.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/trace.h"

using std::shared_ptr;

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

 protected:
  gscoped_ptr<ThreadPool> pool_;
};

TEST_F(ThreadPoolTest, TestNoTaskOpenClose) {
  ASSERT_OK(RebuildPoolWithMinMax(4, 4));
  pool_->Shutdown();
}

static void SimpleTaskMethod(int n, Atomic32 *counter) {
  while (n--) {
    base::subtle::NoBarrier_AtomicIncrement(counter, 1);
    boost::detail::yield(n);
  }
}

class SimpleTask : public Runnable {
 public:
  SimpleTask(int n, Atomic32 *counter)
    : n_(n), counter_(counter) {
  }

  void Run() OVERRIDE {
    SimpleTaskMethod(n_, counter_);
  }

 private:
  int n_;
  Atomic32 *counter_;
};

TEST_F(ThreadPoolTest, TestSimpleTasks) {
  ASSERT_OK(RebuildPoolWithMinMax(4, 4));

  Atomic32 counter(0);
  std::shared_ptr<Runnable> task(new SimpleTask(15, &counter));

  ASSERT_OK(pool_->SubmitFunc(boost::bind(&SimpleTaskMethod, 10, &counter)));
  ASSERT_OK(pool_->Submit(task));
  ASSERT_OK(pool_->SubmitFunc(boost::bind(&SimpleTaskMethod, 20, &counter)));
  ASSERT_OK(pool_->Submit(task));
  ASSERT_OK(pool_->SubmitClosure(Bind(&SimpleTaskMethod, 123, &counter)));
  pool_->Wait();
  ASSERT_EQ(10 + 15 + 20 + 15 + 123, base::subtle::NoBarrier_Load(&counter));
  pool_->Shutdown();
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
    ASSERT_OK(pool_->SubmitFunc(&IssueTraceStatement));
  }
  pool_->Wait();
  ASSERT_STR_CONTAINS(t->DumpToString(), "hello from task");
}

TEST_F(ThreadPoolTest, TestSubmitAfterShutdown) {
  ASSERT_OK(RebuildPoolWithMinMax(1, 1));
  pool_->Shutdown();
  Status s = pool_->SubmitFunc(&IssueTraceStatement);
  ASSERT_EQ("Service unavailable: The pool has been shut down.",
            s.ToString());
}

class SlowTask : public Runnable {
 public:
  explicit SlowTask(CountDownLatch* latch)
    : latch_(latch) {
  }

  void Run() OVERRIDE {
    latch_->Wait();
  }

 private:
  CountDownLatch* latch_;
};

TEST_F(ThreadPoolTest, TestThreadPoolWithNoMinimum) {
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_min_threads(0)
                                   .set_max_threads(3)
                                   .set_idle_timeout(MonoDelta::FromMilliseconds(1))));

  // There are no threads to start with.
  ASSERT_TRUE(pool_->num_threads_ == 0);
  // We get up to 3 threads when submitting work.
  CountDownLatch latch(1);
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(2, pool_->num_threads_);
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(3, pool_->num_threads_);
  // The 4th piece of work gets queued.
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(3, pool_->num_threads_);
  // Finish all work
  latch.CountDown();
  pool_->Wait();
  ASSERT_EQ(0, pool_->active_threads_);
  pool_->Shutdown();
  ASSERT_EQ(0, pool_->num_threads_);
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
    ASSERT_OK(pool_->SubmitFunc(boost::bind(&CountDownLatch::CountDown, &l)));
    l.Wait();
    // Sleeping a different amount in each iteration makes it more likely to hit
    // the bug.
    SleepFor(MonoDelta::FromMicroseconds(i));
  }
}

TEST_F(ThreadPoolTest, TestVariableSizeThreadPool) {
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_min_threads(1)
                                   .set_max_threads(4)
                                   .set_idle_timeout(MonoDelta::FromMilliseconds(1))));

  // There is 1 thread to start with.
  ASSERT_EQ(1, pool_->num_threads_);
  // We get up to 4 threads when submitting work.
  CountDownLatch latch(1);
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(1, pool_->num_threads_);
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(2, pool_->num_threads_);
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(3, pool_->num_threads_);
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(4, pool_->num_threads_);
  // The 5th piece of work gets queued.
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(4, pool_->num_threads_);
  // Finish all work
  latch.CountDown();
  pool_->Wait();
  ASSERT_EQ(0, pool_->active_threads_);
  pool_->Shutdown();
  ASSERT_EQ(0, pool_->num_threads_);
}

TEST_F(ThreadPoolTest, TestMaxQueueSize) {
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_min_threads(1)
                                   .set_max_threads(1)
                                   .set_max_queue_size(1)));

  CountDownLatch latch(1);
  // We will be able to submit two tasks: one for max_threads == 1 and one for
  // max_queue_size == 1.
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  Status s = pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch)));
  CHECK(s.IsServiceUnavailable()) << "Expected failure due to queue blowout:" << s.ToString();
  latch.CountDown();
  pool_->Wait();
  pool_->Shutdown();
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
    ASSERT_OK(pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch))));
  }
  Status s = pool_->Submit(shared_ptr<Runnable>(new SlowTask(&latch)));
  ASSERT_TRUE(s.IsServiceUnavailable()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Thread pool is at capacity");
  latch.CountDown();
  pool_->Wait();
  pool_->Shutdown();
}

// Test that setting a promise from another thread yields
// a value on the current thread.
TEST_F(ThreadPoolTest, TestPromises) {
  ASSERT_OK(RebuildPoolWithBuilder(ThreadPoolBuilder(kDefaultPoolName)
                                   .set_min_threads(1)
                                   .set_max_threads(1)
                                   .set_max_queue_size(1)));

  Promise<int> my_promise;
  ASSERT_OK(pool_->SubmitClosure(
                     Bind(&Promise<int>::Set, Unretained(&my_promise), 5)));
  ASSERT_EQ(5, my_promise.Get());
  pool_->Shutdown();
}

METRIC_DEFINE_entity(test_entity);
METRIC_DEFINE_histogram(test_entity, queue_length, "queue length",
                        MetricUnit::kTasks, "queue length", 1000, 1);

METRIC_DEFINE_histogram(test_entity, queue_time, "queue time",
                        MetricUnit::kMicroseconds, "queue time", 1000000, 1);

METRIC_DEFINE_histogram(test_entity, run_time, "run time",
                        MetricUnit::kMicroseconds, "run time", 1000, 1);

TEST_F(ThreadPoolTest, TestMetrics) {
  MetricRegistry registry;
  scoped_refptr<MetricEntity> entity = METRIC_ENTITY_test_entity.Instantiate(
      &registry, "test entity");
  ASSERT_OK(RebuildPoolWithMinMax(1, 1));

  // Enable metrics for the thread pool.
  scoped_refptr<Histogram> queue_length = METRIC_queue_length.Instantiate(entity);
  scoped_refptr<Histogram> queue_time = METRIC_queue_time.Instantiate(entity);
  scoped_refptr<Histogram> run_time = METRIC_run_time.Instantiate(entity);
  pool_->SetQueueLengthHistogram(queue_length);
  pool_->SetQueueTimeMicrosHistogram(queue_time);
  pool_->SetRunTimeMicrosHistogram(run_time);

  int kNumItems = 500;
  for (int i = 0; i < kNumItems; i++) {
    ASSERT_OK(pool_->SubmitFunc(boost::bind(&usleep, i)));
  }

  pool_->Wait();

  // Check that all histograms were incremented once per submitted item.
  ASSERT_EQ(kNumItems, queue_length->TotalCount());
  ASSERT_EQ(kNumItems, queue_time->TotalCount());
  ASSERT_EQ(kNumItems, run_time->TotalCount());
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
    ASSERT_OK(pool_->SubmitClosure(
        Bind(&ThreadPool::Shutdown, Unretained(pool_.get()))));
    pool_->Wait();
  }, death_msg);

  ASSERT_DEATH({
    ASSERT_OK(RebuildPoolWithMinMax(1, 1));
    ASSERT_OK(pool_->SubmitClosure(
        Bind(&ThreadPool::Wait, Unretained(pool_.get()))));
    pool_->Wait();
  }, death_msg);
}
#endif

class SlowDestructorRunnable : public Runnable {
 public:
  void Run() override {}

  virtual ~SlowDestructorRunnable() {
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
};

// Test that if a tasks's destructor is slow, it doesn't cause serialization of the tasks
// in the queue.
TEST_F(ThreadPoolTest, TestSlowDestructor) {
  ASSERT_OK(RebuildPoolWithMinMax(1, 20));
  MonoTime start = MonoTime::Now();
  for (int i = 0; i < 100; i++) {
    shared_ptr<Runnable> task(new SlowDestructorRunnable());
    ASSERT_OK(pool_->Submit(std::move(task)));
  }
  pool_->Wait();
  ASSERT_LT((MonoTime::Now() - start).ToSeconds(), 5);
}


} // namespace kudu
