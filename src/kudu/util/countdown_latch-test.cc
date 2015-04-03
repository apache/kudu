// Copyright (c) 2014, Cloudera, inc.

#include <boost/bind.hpp>
#include <gtest/gtest.h>

#include "kudu/util/countdown_latch.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"
#include "kudu/util/threadpool.h"

namespace kudu {

static void DecrementLatch(CountDownLatch* latch, int amount) {
  if (amount == 1) {
    latch->CountDown();
    return;
  }
  latch->CountDown(amount);
}

// Tests that we can decrement the latch by arbitrary amounts, as well
// as 1 by one.
TEST(TestCountDownLatch, TestLatch) {

  gscoped_ptr<ThreadPool> pool;
  ASSERT_OK(ThreadPoolBuilder("cdl-test").set_max_threads(1).Build(&pool));

  CountDownLatch latch(1000);

  // Decrement the count by 1 in another thread, this should not fire the
  // latch.
  ASSERT_OK(pool->SubmitFunc(boost::bind(DecrementLatch, &latch, 1)));
  ASSERT_FALSE(latch.WaitFor(MonoDelta::FromMilliseconds(200)));
  ASSERT_EQ(999, latch.count());

  // Now decrement by 1000 this should decrement to 0 and fire the latch
  // (even though 1000 is one more than the current count).
  ASSERT_OK(pool->SubmitFunc(boost::bind(DecrementLatch, &latch, 1000)));
  latch.Wait();
  ASSERT_EQ(0, latch.count());
}

// Test that resetting to zero while there are waiters lets the waiters
// continue.
TEST(TestCountDownLatch, TestResetToZero) {
  CountDownLatch cdl(100);
  scoped_refptr<Thread> t;
  ASSERT_OK(Thread::Create("test", "cdl-test", &CountDownLatch::Wait, &cdl, &t));

  // Sleep for a bit until it's likely the other thread is waiting on the latch.
  SleepFor(MonoDelta::FromMilliseconds(10));
  cdl.Reset(0);
  t->Join();
}

} // namespace kudu
