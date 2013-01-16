// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>

#include "tablet/tablet-test-base.h"
#include "util/countdown_latch.h"

DEFINE_int32(num_threads, 16, "Number of threads to test");
DEFINE_int32(inserts_per_thread, 10000,
             "Number of rows inserted by each inserter thread");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;


// Utility class which calls latch->CountDown() in its destructor.
class CountDownOnScopeExit : boost::noncopyable {
public:
  CountDownOnScopeExit(CountDownLatch *latch) : latch_(latch) {}
  ~CountDownOnScopeExit() {
    latch_->CountDown();
  }

private:
  CountDownLatch *latch_;
};

class TestMultiThreadedTablet : public TestTablet {
protected:
  TestMultiThreadedTablet() :
    running_insert_count_(FLAGS_num_threads)
  {}

  void InsertThread(int tid) {
    CountDownOnScopeExit dec_count(&running_insert_count_);

    // TODO: add a test where some of the inserts actually conflict
    // on the same row.
    InsertTestRows(tid * FLAGS_inserts_per_thread,
                   FLAGS_inserts_per_thread);
  }
  
  void UpdateThread() {
    // TODO: impl
  }

  void FlushThread() {
    // Start off with a very short wait time between flushes.
    // But, especially in debug mode, this will only allow a few
    // rows to get inserted between each flush, and the test will take
    // quite a while. So, after every flush, we double the wait time below.
    int wait_time = 30;
    while (running_insert_count_.count() > 0) {
      tablet_->Flush();

      // Wait, unless the inserters are all done.
      running_insert_count_.TimedWait(boost::posix_time::milliseconds(wait_time));
      wait_time *= 2;
    }
  }

  void StartUpdaterThreads(int n_threads) {
    for (int i = 0; i < n_threads; i++) {
      threads_.push_back(new boost::thread(
                           &TestMultiThreadedTablet::UpdateThread, this));
    }
  }

  void StartInserterThreads(int n_threads) {
    for (int i = 0; i < n_threads; i++) {
      threads_.push_back(new boost::thread(
                           &TestMultiThreadedTablet::InsertThread, this,
                           i));
    }
  }

  void StartFlushThread() {
    threads_.push_back(new boost::thread(
                         &TestMultiThreadedTablet::FlushThread, this));
  }

  void JoinThreads() {
    BOOST_FOREACH(boost::thread &thr, threads_) {
      thr.join();
    }
  }

  boost::ptr_vector<boost::thread> threads_;

  CountDownLatch running_insert_count_;
};


TEST_F(TestMultiThreadedTablet, TestInsertAndFlush) {
  // Spawn a bunch of threads, each of which will do updates.
  StartInserterThreads(FLAGS_num_threads);
  StartFlushThread();
  JoinThreads();
  VerifyTestRows(0, FLAGS_inserts_per_thread * FLAGS_num_threads);
}

} // namespace tablet
} // namespace kudu

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
