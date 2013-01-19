// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>

#include "tablet/tablet-test-base.h"
#include "util/countdown_latch.h"

DEFINE_int32(num_insert_threads, 8, "Number of inserting threads to launch");
DEFINE_int32(num_counter_threads, 8, "Number of counting threads to launch");
DEFINE_int32(num_updater_threads, 1, "Number of updating threads to launch");
DEFINE_int32(inserts_per_thread, 10000,
             "Number of rows inserted by each inserter thread");
DEFINE_double(flusher_backoff, 2.0f, "Ratio to backoff the flusher thread");
DEFINE_int32(flusher_initial_frequency_ms, 30, "Number of ms to wait between flushes");

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
public:
  TestMultiThreadedTablet() :
    running_insert_count_(FLAGS_num_insert_threads)
  {}

  void InsertThread(int tid) {
    CountDownOnScopeExit dec_count(&running_insert_count_);

    // TODO: add a test where some of the inserts actually conflict
    // on the same row.
    InsertTestRows(tid * FLAGS_inserts_per_thread,
                   FLAGS_inserts_per_thread);
  }
  
  void UpdateThread(int tid) {
    uint8_t buf[schema_.byte_size()];
    Slice row_slice(reinterpret_cast<const char *>(buf),
                    schema_.byte_size());
    ScopedRowDelta update(schema_);

    Arena tmp_arena (1024, 1024);

    while (running_insert_count_.count() > 0) {
      scoped_ptr<Tablet::RowIterator> iter;
      ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &iter));

      while (iter->HasNext()) {
        tmp_arena.Reset();
        size_t n = 1;
        ASSERT_STATUS_OK_FAST(iter->CopyNextRows(&n, &buf[0], &tmp_arena));
        CHECK_EQ(n, 1);

        // Grab the key
        Slice key = *schema_.ExtractColumnFromRow<STRING>(row_slice, 0);

        if (rand() % 10 == 7) {
          // Increment the "update count"
          uint32_t old_val = *schema_.ExtractColumnFromRow<UINT32>(row_slice, 2);
          // Issue an update
          uint32_t new_val = old_val + 1;
          update.get().UpdateColumn(schema_, 2, &new_val);
          ASSERT_STATUS_OK_FAST(tablet_->UpdateRow(&key, update.get()));
        }
      }
    }
  }

  // Thread which repeatedly issues CountRows() and makes sure
  // that the count doesn't go ever down.
  void CountThread(int tid) {
    size_t last_count = 0;
    while (running_insert_count_.count() > 0) {
      size_t count;
      ASSERT_STATUS_OK_FAST(tablet_->CountRows(&count));
      ASSERT_GE(count, last_count);
      last_count = count;
    }
  }

  // Thread which iterates slowly over the first 10% of the data.
  // This is meant to test that outstanding iterators don't end up
  // trying to reference already-freed memstore memory.
  void SlowReaderThread(int tid) {
    uint8_t buf[schema_.byte_size()];

    int max_iters = FLAGS_num_insert_threads * FLAGS_inserts_per_thread / 10;

    while (running_insert_count_.count() > 0) {
      scoped_ptr<Tablet::RowIterator> iter;
      ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &iter));


      for (int i = 0; i < max_iters && iter->HasNext(); i++) {
        arena_.Reset();
        size_t n = 1;
        ASSERT_STATUS_OK_FAST(iter->CopyNextRows(&n, &buf[0], &arena_));
        if (running_insert_count_.TimedWait(boost::posix_time::milliseconds(1))) {
          return;
        }
      }
    }
  }

  void FlushThread(int tid) {
    // Start off with a very short wait time between flushes.
    // But, especially in debug mode, this will only allow a few
    // rows to get inserted between each flush, and the test will take
    // quite a while. So, after every flush, we double the wait time below.
    int wait_time = FLAGS_flusher_initial_frequency_ms;
    while (running_insert_count_.count() > 0) {
      tablet_->Flush();

      // Wait, unless the inserters are all done.
      running_insert_count_.TimedWait(boost::posix_time::milliseconds(wait_time));
      wait_time *= FLAGS_flusher_backoff;
    }
  }

  template<typename FunctionType>
  void StartThreads(int n_threads, const FunctionType &function) {
    for (int i = 0; i < n_threads; i++) {
      threads_.push_back(new boost::thread(function, this, i));
    }
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
  StartThreads(FLAGS_num_insert_threads, &TestMultiThreadedTablet::InsertThread);
  StartThreads(FLAGS_num_counter_threads, &TestMultiThreadedTablet::CountThread);
  StartThreads(1, &TestMultiThreadedTablet::FlushThread);
  StartThreads(1, &TestMultiThreadedTablet::SlowReaderThread);
  StartThreads(FLAGS_num_updater_threads, &TestMultiThreadedTablet::UpdateThread);
  JoinThreads();
  VerifyTestRows(0, FLAGS_inserts_per_thread * FLAGS_num_insert_threads);
}

} // namespace tablet
} // namespace kudu

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
