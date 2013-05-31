// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>

#include "tablet/tablet-test-base.h"
#include "util/countdown_latch.h"
#include "util/test_graph.h"

DEFINE_int32(num_insert_threads, 8, "Number of inserting threads to launch");
DEFINE_int32(num_counter_threads, 8, "Number of counting threads to launch");
DEFINE_int32(num_summer_threads, 1, "Number of summing threads to launch");
DEFINE_int32(num_updater_threads, 1, "Number of updating threads to launch");
DEFINE_int32(num_slowreader_threads, 1, "Number of 'slow' reader threads to launch");
DEFINE_int32(num_flush_threads, 1, "Number of flusher reader threads to launch");
DEFINE_int32(num_compact_threads, 1, "Number of compactor threads to launch");


DEFINE_int64(inserts_per_thread, 1000,
             "Number of rows inserted by each inserter thread");
DEFINE_int32(flush_threshold_mb, 0, "Minimum memrowset size to flush");
DEFINE_double(flusher_backoff, 2.0f, "Ratio to backoff the flusher thread");
DEFINE_int32(flusher_initial_frequency_ms, 30, "Number of mrs to wait between flushes");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;


// Utility class which calls latch->CountDown() in its destructor.
class CountDownOnScopeExit : boost::noncopyable {
public:
  explicit CountDownOnScopeExit(CountDownLatch *latch) : latch_(latch) {}
  ~CountDownOnScopeExit() {
    latch_->CountDown();
  }

private:
  CountDownLatch *latch_;
};

template<class SETUP>
class MultiThreadedTabletTest : public TabletTestBase<SETUP> {
  // Import some names from superclass, since C++ is stingy about
  // letting us refer to the members otherwise.
  typedef TabletTestBase<SETUP> superclass;
  using superclass::schema_;
  using superclass::tablet_;
public:
  MultiThreadedTabletTest() :
    running_insert_count_(FLAGS_num_insert_threads),
    ts_collector_(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name())
  {
    ts_collector_.StartDumperThread();
  }

  void InsertThread(int tid) {
    CountDownOnScopeExit dec_count(&running_insert_count_);
    shared_ptr<TimeSeries> inserts = ts_collector_.GetTimeSeries("inserted");

    // TODO: add a test where some of the inserts actually conflict
    // on the same row.
    this->InsertTestRows(tid * FLAGS_inserts_per_thread,
                         FLAGS_inserts_per_thread, 0,
                         inserts.get());
  }

  void UpdateThread(int tid) {
    const Schema &schema = schema_;

    shared_ptr<TimeSeries> updates = ts_collector_.GetTimeSeries("updated");

    // TODO: move the update code into the SETUP class

    Arena tmp_arena(1024, 1024);
    RowBlock block(schema_, 1, &tmp_arena);
    RowBlockRow rb_row = block.row(0);
    faststring update_buf;

    uint64_t updates_since_last_report = 0;
    while (running_insert_count_.count() > 0) {
      gscoped_ptr<RowwiseIterator> iter;
      ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &iter));
      ASSERT_STATUS_OK(iter->Init(NULL));

      while (iter->HasNext() && running_insert_count_.count() > 0) {
        tmp_arena.Reset();
        ASSERT_STATUS_OK_FAST(RowwiseIterator::CopyBlock(iter.get(), &block));
        CHECK_EQ(block.nrows(), 1);

        if (!block.selection_vector()->IsRowSelected(0)) {
          // Don't try to update rows which aren't visible yet --
          // this will crash, since the data in row_slice isn't even copied.
          continue;
        }

        // The key is at the start of the row
        const uint8_t *row_key = rb_row.cell_ptr(schema, 0); // TODO: Fix me on multiple keys
        if (rand() % 10 == 7) {
          // Increment the "update count"
          uint32_t old_val = *schema.ExtractColumnFromRow<UINT32>(rb_row, 2);
          // Issue an update
          uint32_t new_val = old_val + 1;
          update_buf.clear();
          RowChangeListEncoder(schema_, &update_buf).AddColumnUpdate(2, &new_val);
          ASSERT_STATUS_OK_FAST(tablet_->UpdateRow(row_key, RowChangeList(update_buf)));

          if (++updates_since_last_report >= 10) {
            updates->AddValue(updates_since_last_report);
            updates_since_last_report = 0;
          }
        }
      }
    }
  }

  // Thread which repeatedly issues CountRows() and makes sure
  // that the count doesn't go ever down.
  void CountThread(int tid) {
    rowid_t last_count = 0;
    while (running_insert_count_.count() > 0) {
      uint64_t count;
      ASSERT_STATUS_OK_FAST(tablet_->CountRows(&count));
      ASSERT_GE(count, last_count);
      last_count = count;
    }
  }

  // Thread which iterates slowly over the first 10% of the data.
  // This is meant to test that outstanding iterators don't end up
  // trying to reference already-freed memrowset memory.
  void SlowReaderThread(int tid) {
    Arena arena(32*1024, 256*1024);
    RowBlock block(schema_, 1, &arena);

    int max_iters = FLAGS_num_insert_threads * FLAGS_inserts_per_thread / 10;

    while (running_insert_count_.count() > 0) {
      gscoped_ptr<RowwiseIterator> iter;
      ASSERT_STATUS_OK(tablet_->NewRowIterator(schema_, &iter));
      ASSERT_STATUS_OK(iter->Init(NULL));

      for (int i = 0; i < max_iters && iter->HasNext(); i++) {
        size_t n = 1;
        ASSERT_STATUS_OK_FAST(iter->PrepareBatch(&n));
        ASSERT_STATUS_OK_FAST(iter->MaterializeBlock(&block));
        ASSERT_STATUS_OK_FAST(iter->FinishBatch());

        if (running_insert_count_.TimedWait(boost::posix_time::milliseconds(1))) {
          return;
        }
      }
    }
  }

  void SummerThread(int tid) {
    shared_ptr<TimeSeries> scanned_ts = ts_collector_.GetTimeSeries(
      "scanned");

    while (running_insert_count_.count() > 0) {
      CountSum(scanned_ts);
    }
  }

  uint64_t CountSum(const shared_ptr<TimeSeries> &scanned_ts) {
    Arena arena(1024, 1024); // unused, just scanning ints

    // Scan a projection with only an int column.
    // This is provided by both harnesses.
    Schema projection = Schema(boost::assign::list_of
                               (ColumnSchema("val", UINT32)),
                               1);


    static const int kBufInts = 1024*1024 / 8;
    RowBlock block(projection, kBufInts, &arena);
    ColumnBlock column = block.column_block(0);

    uint64_t count_since_report = 0;

    uint64_t sum = 0;

    gscoped_ptr<RowwiseIterator> iter;
    CHECK_OK(tablet_->NewRowIterator(projection, &iter));
    CHECK_OK(iter->Init(NULL));

    while (iter->HasNext()) {
      arena.Reset();
      CHECK_OK(RowwiseIterator::CopyBlock(iter.get(), &block));

      for (size_t j = 0; j < block.nrows(); j++) {
        sum += *reinterpret_cast<const uint32_t *>(column.cell_ptr(j));
      }
      count_since_report += block.nrows();

      // Report metrics if enough time has passed
      if (count_since_report > 100) {
        if (scanned_ts.get()) {
          scanned_ts->AddValue(count_since_report);
        }
        count_since_report = 0;
      }
    }

    if (scanned_ts.get()) {
      scanned_ts->AddValue(count_since_report);
    }

    return sum;
  }



  void FlushThread(int tid) {
    // Start off with a very short wait time between flushes.
    // But, especially in debug mode, this will only allow a few
    // rows to get inserted between each flush, and the test will take
    // quite a while. So, after every flush, we double the wait time below.
    int wait_time = FLAGS_flusher_initial_frequency_ms;
    while (running_insert_count_.count() > 0) {

      if (tablet_->MemRowSetSize() > FLAGS_flush_threshold_mb * 1024 * 1024) {
        ASSERT_STATUS_OK(tablet_->Flush());

      } else {
        LOG(INFO) << "Not flushing, memrowset not very full";
      }
      // Wait, unless the inserters are all done.
      running_insert_count_.TimedWait(boost::posix_time::milliseconds(wait_time));
      wait_time *= FLAGS_flusher_backoff;
    }
  }

  void CompactThread(int tid) {
    int wait_time = 100;
    while (running_insert_count_.count() > 0) {
      ASSERT_STATUS_OK(tablet_->Compact());

      // Wait, unless the inserters are all done.
      running_insert_count_.TimedWait(boost::posix_time::milliseconds(wait_time));
    }
  }

  // Thread which wakes up periodically and collects metrics like memrowset
  // size, etc. Eventually we should have a metrics system to collect things
  // like this, but for now, this is what we've got.
  void CollectStatisticsThread(int tid) {
    shared_ptr<TimeSeries> num_rowsets_ts = ts_collector_.GetTimeSeries(
      "num_rowsets");
    shared_ptr<TimeSeries> memrowset_size_ts = ts_collector_.GetTimeSeries(
      "memrowset_kb");

    while (running_insert_count_.count() > 0) {

      num_rowsets_ts->SetValue( tablet_->num_rowsets() );
      memrowset_size_ts->SetValue( tablet_->MemRowSetSize() / 1024);

      // Wait, unless the inserters are all done.
      running_insert_count_.TimedWait(boost::posix_time::milliseconds(250));
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

  TimeSeriesCollector ts_collector_;
};


TYPED_TEST_CASE(MultiThreadedTabletTest, TabletTestHelperTypes);


TYPED_TEST(MultiThreadedTabletTest, DoTestAllAtOnce) {
  if (1000 == FLAGS_inserts_per_thread) {
    if (this->AllowSlowTests()) {
      FLAGS_inserts_per_thread = 50000;
    }
  }

  // Spawn a bunch of threads, each of which will do updates.
  this->StartThreads(1, &TestFixture::CollectStatisticsThread);
  this->StartThreads(FLAGS_num_insert_threads, &TestFixture::InsertThread);
  this->StartThreads(FLAGS_num_counter_threads, &TestFixture::CountThread);
  this->StartThreads(FLAGS_num_summer_threads, &TestFixture::SummerThread);
  this->StartThreads(FLAGS_num_flush_threads, &TestFixture::FlushThread);
  this->StartThreads(FLAGS_num_compact_threads, &TestFixture::CompactThread);
  this->StartThreads(FLAGS_num_slowreader_threads, &TestFixture::SlowReaderThread);
  this->StartThreads(FLAGS_num_updater_threads, &TestFixture::UpdateThread);
  this->JoinThreads();
  LOG_TIMING(INFO, "Summing int32 column") {
    uint64_t sum = this->CountSum(shared_ptr<TimeSeries>());
    LOG(INFO) << "Sum = " << sum;
  }
  this->VerifyTestRows(0, FLAGS_inserts_per_thread * FLAGS_num_insert_threads);
}

} // namespace tablet
} // namespace kudu
