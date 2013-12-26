// Copyright (c) 2013, Cloudera, inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "gutil/atomicops.h"
#include "util/threadpool.h"
#include "util/test_macros.h"

namespace kudu {

TEST(TestThreadPool, TestNoTaskOpenClose) {
  ThreadPool thread_pool("test");
  thread_pool.Init(4);
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
  ThreadPool thread_pool("test");
  ASSERT_STATUS_OK(thread_pool.Init(4));

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

} // namespace kudu
