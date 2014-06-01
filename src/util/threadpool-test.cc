// Copyright (c) 2013, Cloudera, inc.

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/memory>

#include "gutil/atomicops.h"
#include "util/countdown_latch.h"
#include "util/threadpool.h"
#include "util/test_macros.h"
#include "util/trace.h"

using std::tr1::shared_ptr;

namespace kudu {

TEST(TestThreadPool, TestNoTaskOpenClose) {
  ThreadPool thread_pool("test", 4, 4, ThreadPool::DEFAULT_TIMEOUT);
  ASSERT_STATUS_OK(thread_pool.Init());
  thread_pool.Shutdown();
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

  void Run() {
    SimpleTaskMethod(n_, counter_);
  }

 private:
  int n_;
  Atomic32 *counter_;
};

TEST(TestThreadPool, TestSimpleTasks) {
  ThreadPool thread_pool("test", 4, 4, ThreadPool::DEFAULT_TIMEOUT);
  ASSERT_STATUS_OK(thread_pool.Init());

  Atomic32 counter(0);
  std::tr1::shared_ptr<Runnable> task(new SimpleTask(15, &counter));

  ASSERT_STATUS_OK(thread_pool.SubmitFunc(boost::bind(&SimpleTaskMethod, 10, &counter)));
  ASSERT_STATUS_OK(thread_pool.Submit(task));
  ASSERT_STATUS_OK(thread_pool.SubmitFunc(boost::bind(&SimpleTaskMethod, 20, &counter)));
  ASSERT_STATUS_OK(thread_pool.Submit(task));
  thread_pool.Wait();
  ASSERT_EQ(10 + 15 + 20 + 15, base::subtle::NoBarrier_Load(&counter));
  thread_pool.Shutdown();
}

static void IssueTraceStatement() {
  TRACE("hello from task");
}

// Test that the thread-local trace is propagated to tasks
// submitted to the threadpool.
TEST(TestThreadPool, TestTracePropagation) {
  ThreadPool thread_pool("test", 1, 1, ThreadPool::DEFAULT_TIMEOUT);
  ASSERT_STATUS_OK(thread_pool.Init());

  scoped_refptr<Trace> t(new Trace);
  {
    ADOPT_TRACE(t.get());
    ASSERT_STATUS_OK(thread_pool.SubmitFunc(&IssueTraceStatement));
  }
  thread_pool.Wait();
  ASSERT_STR_CONTAINS(t->DumpToString(true), "hello from task");
}

TEST(TestThreadPool, TestSubmitAfterShutdown) {
  ThreadPool thread_pool("test", 1, 1, ThreadPool::DEFAULT_TIMEOUT);
  ASSERT_STATUS_OK(thread_pool.Init());
  thread_pool.Shutdown();
  Status s = thread_pool.SubmitFunc(&IssueTraceStatement);
  ASSERT_EQ("Service unavailable: The pool has been shut down.",
            s.ToString());
}

class SlowTask : public Runnable {
 public:
  explicit SlowTask(CountDownLatch* latch)
    : latch_(latch) {
  }

  void Run() {
    latch_->Wait();
  }

 private:
  CountDownLatch* latch_;
};

TEST(TestThreadPool, TestThreadPoolWithNoMinimum) {
  MonoDelta timeout = MonoDelta::FromMilliseconds(1);
  ThreadPool thread_pool("test", 0, 3, timeout);
  ASSERT_STATUS_OK(thread_pool.Init());
  // There are no threads to start with.
  ASSERT_TRUE(thread_pool.num_threads_ == 0);
  // We get up to 3 threads when submitting work.
  CountDownLatch latch(1);
  ASSERT_STATUS_OK(thread_pool.Submit(
        shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_STATUS_OK(thread_pool.Submit(
        shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(2, thread_pool.num_threads_);
  ASSERT_STATUS_OK(thread_pool.Submit(
        shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(3, thread_pool.num_threads_);
  // The 4th piece of work gets queued.
  ASSERT_STATUS_OK(thread_pool.Submit(
        shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(3, thread_pool.num_threads_);
  // Finish all work
  latch.CountDown();
  thread_pool.Wait();
  ASSERT_EQ(0, thread_pool.active_threads_);
  thread_pool.Shutdown();
  ASSERT_EQ(0, thread_pool.num_threads_);
}

// Regression test for a bug where a task is submitted exactly
// as a thread is about to exit. Previously this could hang forever.
TEST(TestThreadPool, TestRace) {
  alarm(10);
  MonoDelta timeout = MonoDelta::FromMicroseconds(1);
  ThreadPool thread_pool("test", 0, 1, timeout);
  ASSERT_STATUS_OK(thread_pool.Init());

  for (int i = 0; i < 500; i++) {
    CountDownLatch l(1);
    ASSERT_STATUS_OK(thread_pool.SubmitFunc(boost::bind(&CountDownLatch::CountDown, &l)));
    l.Wait();
    // Sleeping a different amount in each iteration makes it more likely to hit
    // the bug.
    usleep(i);
  }
}

TEST(TestThreadPool, TestVariableSizeThreadPool) {
  MonoDelta timeout = MonoDelta::FromMilliseconds(1);
  ThreadPool thread_pool("test", 1, 4, timeout);
  ASSERT_STATUS_OK(thread_pool.Init());
  // There is 1 thread to start with.
  ASSERT_EQ(1, thread_pool.num_threads_);
  // We get up to 4 threads when submitting work.
  CountDownLatch latch(1);
  ASSERT_STATUS_OK(thread_pool.Submit(
        shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(1, thread_pool.num_threads_);
  ASSERT_STATUS_OK(thread_pool.Submit(
        shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(2, thread_pool.num_threads_);
  ASSERT_STATUS_OK(thread_pool.Submit(
        shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(3, thread_pool.num_threads_);
  ASSERT_STATUS_OK(thread_pool.Submit(
        shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(4, thread_pool.num_threads_);
  // The 5th piece of work gets queued.
  ASSERT_STATUS_OK(thread_pool.Submit(
        shared_ptr<Runnable>(new SlowTask(&latch))));
  ASSERT_EQ(4, thread_pool.num_threads_);
  // Finish all work
  latch.CountDown();
  thread_pool.Wait();
  ASSERT_EQ(0, thread_pool.active_threads_);
  thread_pool.Shutdown();
  ASSERT_EQ(0, thread_pool.num_threads_);
}

} // namespace kudu
