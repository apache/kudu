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

#include "kudu/util/blocking_queue.h"

#include <cstddef>
#include <cstdint>
#include <list>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::thread;
using std::vector;

namespace kudu {

BlockingQueue<int32_t> test1_queue(5);

void InsertSomeThings() {
  ASSERT_EQ(test1_queue.Put(1), QUEUE_SUCCESS);
  ASSERT_EQ(test1_queue.Put(2), QUEUE_SUCCESS);
  ASSERT_EQ(test1_queue.Put(3), QUEUE_SUCCESS);
}

TEST(BlockingQueueTest, Test1) {
  thread inserter_thread(InsertSomeThings);
  int32_t i;
  ASSERT_OK(test1_queue.BlockingGet(&i));
  ASSERT_EQ(1, i);
  ASSERT_OK(test1_queue.BlockingGet(&i));
  ASSERT_EQ(2, i);
  ASSERT_OK(test1_queue.BlockingGet(&i));
  ASSERT_EQ(3, i);
  inserter_thread.join();
}

TEST(BlockingQueueTest, TestBlockingDrainTo) {
  BlockingQueue<int32_t> test_queue(3);
  ASSERT_EQ(test_queue.Put(1), QUEUE_SUCCESS);
  ASSERT_EQ(test_queue.Put(2), QUEUE_SUCCESS);
  ASSERT_EQ(test_queue.Put(3), QUEUE_SUCCESS);
  vector<int32_t> out;
  ASSERT_OK(test_queue.BlockingDrainTo(&out, MonoTime::Now() + MonoDelta::FromSeconds(30)));
  ASSERT_EQ(1, out[0]);
  ASSERT_EQ(2, out[1]);
  ASSERT_EQ(3, out[2]);

  // Set a deadline in the past and ensure we time out.
  Status s = test_queue.BlockingDrainTo(&out, MonoTime::Now() - MonoDelta::FromSeconds(1));
  ASSERT_TRUE(s.IsTimedOut());

  // Ensure that if the queue is shut down, we get Aborted status.
  test_queue.Shutdown();
  s = test_queue.BlockingDrainTo(&out, MonoTime::Now() - MonoDelta::FromSeconds(1));
  ASSERT_TRUE(s.IsAborted());
}

TEST(BlockingQueueTest, TestBlockingPut) {
  const MonoDelta kShortTimeout = MonoDelta::FromMilliseconds(200);
  const MonoDelta kEvenShorterTimeout = MonoDelta::FromMilliseconds(100);
  BlockingQueue<int32_t> test_queue(2);

  // First, a trivial check that we don't do anything if our deadline has
  // already passed.
  Status s = test_queue.BlockingPut(1, MonoTime::Now() - kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // Now put a couple elements onto the queue.
  ASSERT_OK(test_queue.BlockingPut(1));
  ASSERT_OK(test_queue.BlockingPut(2));

  // We're at capacity, so further puts should time out...
  s = test_queue.BlockingPut(3, MonoTime::Now() + kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // ... until space is freed up before the deadline.
  thread t([&] {
    SleepFor(kEvenShorterTimeout);
    int out;
    ASSERT_OK(test_queue.BlockingGet(&out));
  });
  SCOPED_CLEANUP({
    t.join();
  });
  ASSERT_OK(test_queue.BlockingPut(3, MonoTime::Now() + kShortTimeout));

  // If we shut down, we shouldn't be able to put more elements onto the queue.
  test_queue.Shutdown();
  s = test_queue.BlockingPut(3, MonoTime::Now() + kShortTimeout);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();
}

TEST(BlockingQueueTest, TestBlockingGet) {
  const MonoDelta kShortTimeout = MonoDelta::FromMilliseconds(200);
  const MonoDelta kEvenShorterTimeout = MonoDelta::FromMilliseconds(100);
  BlockingQueue<int32_t> test_queue(2);
  ASSERT_OK(test_queue.BlockingPut(1));
  ASSERT_OK(test_queue.BlockingPut(2));

  // Test that if we have stuff in our queue, regardless of deadlines, we'll be
  // able to get them out.
  int32_t val = 0;
  ASSERT_OK(test_queue.BlockingGet(&val, MonoTime::Now() - kShortTimeout));
  ASSERT_EQ(1, val);
  ASSERT_OK(test_queue.BlockingGet(&val, MonoTime::Now() + kShortTimeout));
  ASSERT_EQ(2, val);

  // But without stuff in the queue, we'll time out...
  Status s = test_queue.BlockingGet(&val, MonoTime::Now() - kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  s = test_queue.BlockingGet(&val, MonoTime::Now() + kShortTimeout);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // ... until new elements show up.
  thread t([&] {
    SleepFor(kEvenShorterTimeout);
    ASSERT_OK(test_queue.BlockingPut(3));
  });
  SCOPED_CLEANUP({
    t.join();
  });
  ASSERT_OK(test_queue.BlockingGet(&val, MonoTime::Now() + kShortTimeout));
  ASSERT_EQ(3, val);

  // If we shut down with stuff in our queue, we'll continue to return those
  // elements. Otherwise, we'll return an error.
  ASSERT_OK(test_queue.BlockingPut(4));
  test_queue.Shutdown();
  ASSERT_OK(test_queue.BlockingGet(&val, MonoTime::Now() + kShortTimeout));
  ASSERT_EQ(4, val);
  s = test_queue.BlockingGet(&val, MonoTime::Now() + kShortTimeout);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();
}

// Test that, when the queue is shut down with elements still pending,
// Drain still returns OK until the elements are all gone.
TEST(BlockingQueueTest, TestGetAndDrainAfterShutdown) {
  // Put some elements into the queue and then shut it down.
  BlockingQueue<int32_t> q(3);
  ASSERT_EQ(q.Put(1), QUEUE_SUCCESS);
  ASSERT_EQ(q.Put(2), QUEUE_SUCCESS);

  q.Shutdown();

  // Get() should still return an element.
  int i;
  ASSERT_OK(q.BlockingGet(&i));
  ASSERT_EQ(1, i);

  // Drain should still return OK, since it yielded elements.
  vector<int32_t> out;
  ASSERT_OK(q.BlockingDrainTo(&out));
  ASSERT_EQ(2, out[0]);

  // Now that it's empty, it should return Aborted.
  Status s = q.BlockingDrainTo(&out);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();
  s = q.BlockingGet(&i);
  ASSERT_FALSE(s.ok()) << s.ToString();
}

TEST(BlockingQueueTest, TestTooManyInsertions) {
  BlockingQueue<int32_t> test_queue(2);
  ASSERT_EQ(test_queue.Put(123), QUEUE_SUCCESS);
  ASSERT_EQ(test_queue.Put(123), QUEUE_SUCCESS);
  ASSERT_EQ(test_queue.Put(123), QUEUE_FULL);
}

namespace {

struct LengthLogicalSize {
  static size_t logical_size(const string& s) {
    return s.length();
  }
};

} // anonymous namespace

TEST(BlockingQueueTest, TestLogicalSize) {
  BlockingQueue<string, LengthLogicalSize> test_queue(4);
  ASSERT_EQ(test_queue.Put("a"), QUEUE_SUCCESS);
  ASSERT_EQ(test_queue.Put("bcd"), QUEUE_SUCCESS);
  ASSERT_EQ(test_queue.Put("e"), QUEUE_FULL);
}

TEST(BlockingQueueTest, TestNonPointerParamsMayBeNonEmptyOnDestruct) {
  BlockingQueue<int32_t> test_queue(1);
  ASSERT_EQ(test_queue.Put(123), QUEUE_SUCCESS);
  // No DCHECK failure on destruct.
}

#ifndef NDEBUG
TEST(BlockingQueueDeathTest, TestPointerParamsMustBeEmptyOnDestruct) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_DEATH({
      BlockingQueue<int32_t*> test_queue(1);
      int32_t element = 123;
      ASSERT_EQ(test_queue.Put(&element), QUEUE_SUCCESS);
      // Debug assertion triggered on queue destruction since type is a pointer.
    },
    "BlockingQueue holds bare pointers");
}
#endif // NDEBUG

TEST(BlockingQueueTest, TestGetFromShutdownQueue) {
  BlockingQueue<int64_t> test_queue(2);
  ASSERT_EQ(test_queue.Put(123), QUEUE_SUCCESS);
  test_queue.Shutdown();
  ASSERT_EQ(test_queue.Put(456), QUEUE_SHUTDOWN);
  int64_t i;
  ASSERT_OK(test_queue.BlockingGet(&i));
  ASSERT_EQ(123, i);
  Status s = test_queue.BlockingGet(&i);
  ASSERT_FALSE(s.ok()) << s.ToString();
}

TEST(BlockingQueueTest, TestGscopedPtrMethods) {
  BlockingQueue<int*> test_queue(2);
  gscoped_ptr<int> input_int(new int(123));
  ASSERT_EQ(test_queue.Put(&input_int), QUEUE_SUCCESS);
  gscoped_ptr<int> output_int;
  ASSERT_OK(test_queue.BlockingGet(&output_int));
  ASSERT_EQ(123, *output_int.get());
  test_queue.Shutdown();
}

class MultiThreadTest {
 public:
  MultiThreadTest()
   :  puts_(4),
      blocking_puts_(4),
      nthreads_(5),
      queue_(nthreads_ * puts_),
      num_inserters_(nthreads_),
      sync_latch_(nthreads_) {
  }

  void InserterThread(int arg) {
    for (int i = 0; i < puts_; i++) {
      ASSERT_EQ(queue_.Put(arg), QUEUE_SUCCESS);
    }
    sync_latch_.CountDown();
    sync_latch_.Wait();
    for (int i = 0; i < blocking_puts_; i++) {
      ASSERT_OK(queue_.BlockingPut(arg));
    }
    MutexLock guard(lock_);
    if (--num_inserters_ == 0) {
      queue_.Shutdown();
    }
  }

  void RemoverThread() {
    for (int i = 0; i < puts_ + blocking_puts_; i++) {
      int32_t arg = 0;
      Status s = queue_.BlockingGet(&arg);
      if (!s.ok()) {
        arg = -1;
      }
      MutexLock guard(lock_);
      gotten_[arg] = gotten_[arg] + 1;
    }
  }

  void Run() {
    for (int i = 0; i < nthreads_; i++) {
      threads_.emplace_back(&MultiThreadTest::InserterThread, this, i);
      threads_.emplace_back(&MultiThreadTest::RemoverThread, this);
    }
    // We add an extra thread to ensure that there aren't enough elements in
    // the queue to go around.  This way, we test removal after Shutdown.
    threads_.emplace_back(&MultiThreadTest::RemoverThread, this);
    for (auto& thread : threads_) {
      thread.join();
    }
    // Let's check to make sure we got what we should have.
    MutexLock guard(lock_);
    for (int i = 0; i < nthreads_; i++) {
      ASSERT_EQ(puts_ + blocking_puts_, gotten_[i]);
    }
    // And there were nthreads_ * (puts_ + blocking_puts_)
    // elements removed, but only nthreads_ * puts_ +
    // blocking_puts_ elements added.  So some removers hit the
    // shutdown case.
    ASSERT_EQ(puts_ + blocking_puts_, gotten_[-1]);
  }

  int puts_;
  int blocking_puts_;
  int nthreads_;
  BlockingQueue<int32_t> queue_;
  Mutex lock_;
  std::map<int32_t, int> gotten_;
  vector<thread> threads_;
  int num_inserters_;
  CountDownLatch sync_latch_;
};

TEST(BlockingQueueTest, TestMultipleThreads) {
  MultiThreadTest test;
  test.Run();
}

}  // namespace kudu
