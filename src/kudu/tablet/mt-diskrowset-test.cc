// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>

#include "kudu/tablet/diskrowset-test-base.h"

DEFINE_int32(num_threads, 2, "Number of threads to test");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;


class TestMultiThreadedRowSet : public TestRowSet {
 public:
  void RowSetUpdateThread(DiskRowSet *rs) {
    unordered_set<uint32_t> updated;
    UpdateExistingRows(rs, 0.5f, &updated);
  }

  void FlushThread(DiskRowSet *rs) {
    for (int i = 0; i < 10; i++) {
      CHECK_OK(rs->FlushDeltas());
    }
  }

  void StartUpdaterThreads(boost::ptr_vector<boost::thread> *threads,
                           DiskRowSet *rs,
                           int n_threads) {
    for (int i = 0; i < n_threads; i++) {
      threads->push_back(new boost::thread(
                           &TestMultiThreadedRowSet::RowSetUpdateThread, this,
                           rs));
    }
  }

  void StartFlushThread(boost::ptr_vector<boost::thread> *threads,
                        DiskRowSet *rs) {
    threads->push_back(new boost::thread(
                         &TestMultiThreadedRowSet::FlushThread, this, rs));
  }

  void JoinThreads(boost::ptr_vector<boost::thread> *threads) {
    BOOST_FOREACH(boost::thread &thr, *threads) {
      thr.join();
    }
  }
};


TEST_F(TestMultiThreadedRowSet, TestMTUpdate) {
  if (2 == FLAGS_num_threads) {
    if (AllowSlowTests()) {
      FLAGS_num_threads = 16;
    }
  }

  WriteTestRowSet();

  // Re-open the rowset
  shared_ptr<DiskRowSet> rs;
  ASSERT_OK(OpenTestRowSet(&rs));

  // Spawn a bunch of threads, each of which will do updates.
  boost::ptr_vector<boost::thread> threads;
  StartUpdaterThreads(&threads, rs.get(), FLAGS_num_threads);

  JoinThreads(&threads);
}

TEST_F(TestMultiThreadedRowSet, TestMTUpdateAndFlush) {
  if (2 == FLAGS_num_threads) {
    if (AllowSlowTests()) {
      FLAGS_num_threads = 16;
    }
  }

  WriteTestRowSet();

  // Re-open the rowset
  shared_ptr<DiskRowSet> rs;
  ASSERT_OK(OpenTestRowSet(&rs));

  // Spawn a bunch of threads, each of which will do updates.
  boost::ptr_vector<boost::thread> threads;
  StartUpdaterThreads(&threads, rs.get(), FLAGS_num_threads);
  StartFlushThread(&threads, rs.get());

  JoinThreads(&threads);

  // TODO: test that updates were successful -- collect the updated
  // row lists from all the threads, and verify them.
}

} // namespace tablet
} // namespace kudu
