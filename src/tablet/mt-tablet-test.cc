// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <tr1/unordered_set>

#include "gutil/macros.h"
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
DEFINE_int32(flusher_initial_frequency_ms, 30, "Number of ms to wait between flushes");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;


// Utility class which calls latch->CountDown() in its destructor.
class CountDownOnScopeExit {
 public:
  explicit CountDownOnScopeExit(CountDownLatch *latch) : latch_(latch) {}
  ~CountDownOnScopeExit() {
    latch_->CountDown();
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(CountDownOnScopeExit);

  CountDownLatch *latch_;
};

template<class SETUP>
class MultiThreadedTabletTest : public TabletTestBase<SETUP> {
  // Import some names from superclass, since C++ is stingy about
  // letting us refer to the members otherwise.
  typedef TabletTestBase<SETUP> superclass;
  using superclass::schema_;
  using superclass::tablet_;
  using superclass::setup_;
 public:
  MultiThreadedTabletTest()
    : running_insert_count_(FLAGS_num_insert_threads),
      ts_collector_(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) {
    ts_collector_.StartDumperThread();
  }

  void InsertThread(int tid) {
    CountDownOnScopeExit dec_count(&running_insert_count_);
    shared_ptr<TimeSeries> inserts = ts_collector_.GetTimeSeries("inserted");

    // TODO: add a test where some of the inserts actually conflict
    // on the same row.

    uint64_t max_rows = this->ClampRowCount(FLAGS_inserts_per_thread * FLAGS_num_insert_threads)
        / FLAGS_num_insert_threads;

    if (max_rows < FLAGS_inserts_per_thread) {
      LOG(WARNING) << "Clamping the inserts per thread to " << max_rows << " to prevent overflow";
    }

    this->InsertTestRows(tid * max_rows,
                         max_rows, 0,
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
    int col_idx = schema.num_key_columns() == 1 ? 2 : 3;
    LOG(INFO) << "Update thread using schema: " << schema.ToString();
    RowBuilder rb(schema.CreateKeyProjection());

    while (running_insert_count_.count() > 0) {
      gscoped_ptr<RowwiseIterator> iter;
      CHECK_OK(tablet_->NewRowIterator(schema_, &iter));
      CHECK_OK(iter->Init(NULL));

      while (iter->HasNext() && running_insert_count_.count() > 0) {
        tmp_arena.Reset();
        CHECK_OK(RowwiseIterator::CopyBlock(iter.get(), &block));
        CHECK_EQ(block.nrows(), 1);

        if (!block.selection_vector()->IsRowSelected(0)) {
          // Don't try to update rows which aren't visible yet --
          // this will crash, since the data in row_slice isn't even copied.
          continue;
        }

        rb.Reset();
        // Rebuild the key by extracting the cells from the row
        setup_.BuildRowKeyFromExistingRow(&rb, rb_row);
        if (rand() % 10 == 7) {
          // Increment the "update count"
          uint32_t old_val = *schema.ExtractColumnFromRow<UINT32>(rb_row, col_idx);
          // Issue an update
          uint32_t new_val = old_val + 1;
          update_buf.clear();
          RowChangeListEncoder(schema_, &update_buf).AddColumnUpdate(col_idx, &new_val);
          TransactionContext dummy;
          CHECK_OK(tablet_->MutateRow(&dummy, rb.row(), RowChangeList(update_buf)));

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
      CHECK_OK(tablet_->CountRows(&count));
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

    uint64_t max_rows = this->ClampRowCount(FLAGS_inserts_per_thread * FLAGS_num_insert_threads)
            / FLAGS_num_insert_threads;

    int max_iters = FLAGS_num_insert_threads * max_rows / 10;

    while (running_insert_count_.count() > 0) {
      gscoped_ptr<RowwiseIterator> iter;
      CHECK_OK(tablet_->NewRowIterator(schema_, &iter));
      CHECK_OK(iter->Init(NULL));

      for (int i = 0; i < max_iters && iter->HasNext(); i++) {
        size_t n = 1;
        CHECK_OK(iter->PrepareBatch(&n));
        CHECK_OK(iter->MaterializeBlock(&block));
        CHECK_OK(iter->FinishBatch());

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
                               (ColumnSchema("val", UINT32, true)), // TODO: This should pick the test one
                               0);


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
        CHECK_OK(tablet_->Flush());

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
      CHECK_OK(tablet_->Compact(Tablet::COMPACT_NO_FLAGS));

      // Wait, unless the inserters are all done.
      running_insert_count_.TimedWait(boost::posix_time::milliseconds(wait_time));
    }
  }

  // Thread which cycles between inserting and deleting a test row, each time
  // with a different value.
  void DeleteAndReinsertCycleThread(int tid) {
    uint32_t iteration = 0;
    while (running_insert_count_.count() > 0) {
      for (int i = 0; i < 100; i++) {
        this->InsertTestRows(tid, 1, iteration++);
        TransactionContext tx_ctx;
        CHECK_OK(this->DeleteTestRow(&tx_ctx, tid));
      }
    }
  }

  // Thread which continuously sends updates at the same row, ignoring any
  // "not found" errors that might come back. This is used simultaneously with
  // DeleteAndReinsertCycleThread to check for races where we might accidentally
  // succeed in UPDATING a ghost row.
  void StubbornlyUpdateSameRowThread(int tid) {
    uint32_t iteration = 0;
    while (running_insert_count_.count() > 0) {
      TransactionContext tx_ctx;
      for (int i = 0; i < 100; i++) {
        tx_ctx.Reset();
        Status s = this->UpdateTestRow(&tx_ctx, tid, iteration++);
        if (!s.ok() && !s.IsNotFound()) {
          // We expect "not found", but not any other errors.
          CHECK_OK(s);
        }
      }
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
      num_rowsets_ts->SetValue(tablet_->num_rowsets());
      memrowset_size_ts->SetValue(tablet_->MemRowSetSize() / 1024);

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

  uint64_t max_rows = this->ClampRowCount(FLAGS_inserts_per_thread * FLAGS_num_insert_threads)
          / FLAGS_num_insert_threads;

  this->VerifyTestRows(0, max_rows * FLAGS_num_insert_threads);
}

// Start up a bunch of threads which repeatedly insert and delete the same
// row, while flushing and compacting. This checks various concurrent handling
// of DELETE/REINSERT during flushes.
TYPED_TEST(MultiThreadedTabletTest, DeleteAndReinsert) {
  google::FlagSaver saver;
  FLAGS_flusher_backoff = 1.0f;
  FLAGS_flusher_initial_frequency_ms = 1;
  this->StartThreads(FLAGS_num_flush_threads, &TestFixture::FlushThread);
  this->StartThreads(FLAGS_num_compact_threads, &TestFixture::CompactThread);
  this->StartThreads(10, &TestFixture::DeleteAndReinsertCycleThread);
  this->StartThreads(10, &TestFixture::StubbornlyUpdateSameRowThread);

  // Run very quickly in dev builds, longer in slow builds.
  float runtime_seconds = this->AllowSlowTests() ? 2 : 0.1;
  Stopwatch sw;
  sw.start();
  while (sw.elapsed().wall < runtime_seconds * NANOS_PER_SECOND &&
         !this->HasFatalFailure()) {
    usleep(5000);
  }

  // This is sort of a hack -- the flusher thread stops when it sees this
  // countdown latch go to 0.
  this->running_insert_count_.Reset(0);
  this->JoinThreads();
}

} // namespace tablet
} // namespace kudu
