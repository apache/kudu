// Copyright (c) 2013, Cloudera, inc.

#include <boost/thread/thread.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <tr1/memory>

#include "util/blocking_queue.h"

using std::tr1::shared_ptr;

namespace kudu {

BlockingQueue<int32_t> test1_queue(5);

void InsertSomeThings(void) {
  ASSERT_EQ(test1_queue.Put(1), QUEUE_SUCCESS);
  ASSERT_EQ(test1_queue.Put(2), QUEUE_SUCCESS);
  ASSERT_EQ(test1_queue.Put(3), QUEUE_SUCCESS);
}

TEST(BlockingQueueTest, Test1) {
  boost::thread inserter_thread(InsertSomeThings);
  int32_t i;
  ASSERT_TRUE(test1_queue.BlockingGet(&i));
  ASSERT_EQ(1, i);
  ASSERT_TRUE(test1_queue.BlockingGet(&i));
  ASSERT_EQ(2, i);
  ASSERT_TRUE(test1_queue.BlockingGet(&i));
  ASSERT_EQ(3, i);
}

TEST(BlockingQueueTest, TestTooManyInsertions) {
  BlockingQueue<int32_t> test_queue(2);
  ASSERT_EQ(test_queue.Put(123), QUEUE_SUCCESS);
  ASSERT_EQ(test_queue.Put(123), QUEUE_SUCCESS);
  ASSERT_EQ(test_queue.Put(123), QUEUE_FULL);
}

TEST(BlockingQueueTest, TestGetFromShutdownQueue) {
  BlockingQueue<int64_t> test_queue(2);
  ASSERT_EQ(test_queue.Put(123), QUEUE_SUCCESS);
  test_queue.Shutdown();
  ASSERT_EQ(test_queue.Put(456), QUEUE_SHUTDOWN);
  int64_t i;
  ASSERT_TRUE(test_queue.BlockingGet(&i));
  ASSERT_EQ(123, i);
  ASSERT_FALSE(test_queue.BlockingGet(&i));
}

TEST(BlockingQueueTest, TestGscopedPtrMethods) {
  BlockingQueue<int*> test_queue(2);
  gscoped_ptr<int> input_int(new int(123));
  ASSERT_EQ(test_queue.Put(&input_int), QUEUE_SUCCESS);
  gscoped_ptr<int> output_int;
  ASSERT_TRUE(test_queue.BlockingGet(&output_int));
  ASSERT_EQ(123, *output_int.get());
  test_queue.Shutdown();
}

class MultiThreadTest {
 public:
  typedef std::vector<std::tr1::shared_ptr<boost::thread> > thread_vec_t;

  MultiThreadTest()
    : iterations_(4),
      nthreads_(5),
      queue_(nthreads_ * iterations_),
      num_inserters_(nthreads_) {
  }

  void InserterThread(int arg) {
    for (int i = 0; i < iterations_; i++) {
      ASSERT_EQ(queue_.Put(arg), QUEUE_SUCCESS);
    }
    boost::lock_guard<boost::mutex> guard(lock_);
    if (--num_inserters_ == 0) {
      queue_.Shutdown();
    }
  }

  void RemoverThread() {
    for (int i = 0; i < iterations_; i++) {
      int32_t arg;
      bool got = queue_.BlockingGet(&arg);
      if (!got) {
        arg = -1;
      }
      boost::lock_guard<boost::mutex> guard(lock_);
      gotten_[arg] = gotten_[arg] + 1;
    }
  }

  void Run() {
    for (int i = 0; i < nthreads_; i++) {
      threads_.push_back(shared_ptr<boost::thread>(
              new boost::thread(boost::bind(
                &MultiThreadTest::InserterThread, this, i))));
      threads_.push_back(shared_ptr<boost::thread>(
              new boost::thread(boost::bind(
                &MultiThreadTest::RemoverThread, this))));
    }
    // We add an extra thread to ensure that there aren't enough elements in
    // the queue to go around.  This way, we test removal after Shutdown.
    threads_.push_back(shared_ptr<boost::thread>(
            new boost::thread(boost::bind(
              &MultiThreadTest::RemoverThread, this))));
    for (thread_vec_t::iterator t = threads_.begin();
         t != threads_.end(); ++t) {
      (*t)->join();
    }
    // Let's check to make sure we got what we should have.
    boost::lock_guard<boost::mutex> guard(lock_);
    for (int i = 0; i < nthreads_; i++) {
      ASSERT_EQ(iterations_, gotten_[i]);
    }
    // And there were nthreads_ * (iterations_ + 1)  elements removed, but only
    // nthreads_ * iterations_ elements added.  So some removers hit the shutdown
    // case.
    ASSERT_EQ(iterations_, gotten_[-1]);
  }

  int iterations_;
  int nthreads_;
  BlockingQueue<int32_t> queue_;
  boost::mutex lock_;
  std::map<int32_t, int> gotten_;
  thread_vec_t threads_;
  int num_inserters_;
};

TEST(BlockingQueueTest, TestMultipleThreads) {
  MultiThreadTest test;
  test.Run();
}

}  // namespace kudu
