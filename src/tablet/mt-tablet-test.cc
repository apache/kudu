// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>

#include "tablet/tablet-test-base.h"

DEFINE_int32(num_threads, 16, "Number of threads to test");
DEFINE_int32(inserts_per_thread, 10000,
             "Number of rows inserted by each inserter thread");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

class TestMultiThreadedTablet : public TestTablet {
protected:
  void InsertThread(int tid) {
    // TODO: add a test where some of the inserts actually conflict
    // on the same row.
    InsertTestRows(tid * FLAGS_inserts_per_thread,
                   FLAGS_inserts_per_thread);
  }
  
  void UpdateThread() {
    // TODO: impl
  }

  void FlushThread() {
    for (int i = 0; i < 10; i++) {
      tablet_->Flush();
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
};


TEST_F(TestMultiThreadedTablet, TestInsertAndFlush) {
  // Spawn a bunch of threads, each of which will do updates.
  StartInserterThreads(FLAGS_num_threads);
  StartFlushThread();
  JoinThreads();
}

} // namespace tablet
} // namespace kudu

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
