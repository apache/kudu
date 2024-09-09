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

#include <cstdint>
#include <cstdlib>
#include <deque>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/codegen/compilation_manager.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_graph.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

// IWYU pragma: no_forward_declare kudu::tablet::MultiThreadedTabletTest

DECLARE_int32(tablet_history_max_age_sec);
DECLARE_double(tablet_delta_store_major_compact_min_ratio);
DECLARE_int32(tablet_delta_store_minor_compact_max);
DEFINE_int32(num_insert_threads, 8, "Number of inserting threads to launch");
DEFINE_int32(num_counter_threads, 8, "Number of counting threads to launch");
DEFINE_int32(num_summer_threads, 1, "Number of summing threads to launch");
DEFINE_int32(num_slowreader_threads, 1, "Number of 'slow' reader threads to launch");
DEFINE_int32(num_flush_threads, 1, "Number of flusher reader threads to launch");
DEFINE_int32(num_compact_threads, 1, "Number of compactor threads to launch");
DEFINE_int32(num_undo_delta_gc_threads, 1, "Number of undo delta gc threads to launch");
DEFINE_int32(num_deleted_rowset_gc_threads, 1, "Number of deleted rowset gc threads to launch");
DEFINE_int32(num_updater_threads, 1, "Number of updating threads to launch");
DEFINE_int32(num_flush_delta_threads, 1, "Number of delta flusher reader threads to launch");
DEFINE_int32(num_minor_compact_deltas_threads, 1,
             "Number of delta minor compactor threads to launch");
DEFINE_int32(num_major_compact_deltas_threads, 1,
             "Number of delta major compactor threads to launch");

DEFINE_int64(inserts_per_thread, 1000,
             "Number of rows inserted by each inserter thread");
DEFINE_int32(tablet_test_flush_threshold_mb, 0, "Minimum memrowset size to flush");
DEFINE_double(flusher_backoff, 2.0F, "Ratio to backoff the flusher thread");
DEFINE_int32(flusher_initial_frequency_ms, 30, "Number of ms to wait between flushes");

using std::deque;
using std::shared_ptr;
using std::thread;
using std::unique_ptr;
using strings::Substitute;
using testing::UnitTest;

namespace kudu {
namespace tablet {

namespace {
const MonoDelta kBackgroundOpInterval = MonoDelta::FromMilliseconds(100);
} // anonymous namespace

#define THR_RETURN_NOT_OK(tid, expr) \
  do { \
    if (auto s = (expr); !s.ok()) { \
      threads_statuses_[(tid)] = std::move(s); \
      return; \
    } \
  } while (false)

template<class SETUP>
class MultiThreadedTabletTest : public TabletTestBase<SETUP> {
  // Import some names from superclass, since C++ is stingy about
  // letting us refer to the members otherwise.
  typedef TabletTestBase<SETUP> superclass;
  using superclass::schema_;
  using superclass::client_schema_;
  using superclass::tablet;
  using superclass::setup_;
 public:
  virtual void SetUp() {
    superclass::SetUp();

    // Warm up code cache with all the projections we'll be using.
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(tablet()->NewRowIterator(client_schema_, &iter));
    uint64_t count;
    ASSERT_OK(tablet()->CountRows(&count));
    const Schema* schema = tablet()->schema().get();
    const ColumnSchema& valcol = schema->column(schema->find_column("val"));
    valcol_projection_ = Schema({ valcol }, 0);
    ASSERT_OK(tablet()->NewRowIterator(valcol_projection_, &iter));
    codegen::CompilationManager::GetSingleton()->Wait();

    ts_collector_.StartDumperThread();
  }

  explicit MultiThreadedTabletTest(TabletHarness::Options::ClockType clock_type =
                                   TabletHarness::Options::ClockType::LOGICAL_CLOCK)
      : TabletTestBase<SETUP>(clock_type),
        running_insert_count_(FLAGS_num_insert_threads),
        ts_collector_(UnitTest::GetInstance()->current_test_info()->test_suite_name()) {}

  void InsertThread(size_t group_tid, size_t /*tid*/) {
    CountDownOnScopeExit dec_count(&running_insert_count_);
    shared_ptr<TimeSeries> inserts = ts_collector_.GetTimeSeries("inserted");

    // TODO: add a test where some of the inserts actually conflict
    // on the same row.

    uint64_t max_rows = this->ClampRowCount(FLAGS_inserts_per_thread * FLAGS_num_insert_threads)
        / FLAGS_num_insert_threads;

    if (max_rows < FLAGS_inserts_per_thread) {
      LOG(WARNING) << "Clamping the inserts per thread to " << max_rows << " to prevent overflow";
    }

    this->InsertTestRows(group_tid * max_rows,
                         max_rows, 0,
                         inserts.get());
  }

  void UpdateThread(size_t /*group_tid*/, size_t tid) {
    // This reference is introduced to avoid compiler errors in the code below
    // due to intricacies of typed tests.
    const Schema& schema = schema_;

    shared_ptr<TimeSeries> updates = ts_collector_.GetTimeSeries("updated");

    LocalTabletWriter writer(tablet().get(), &this->client_schema_);

    RowBlockMemory mem(1024);
    RowBlock block(&schema, 1, &mem);
    faststring update_buf;

    uint64_t updates_since_last_report = 0;
    const int col_idx = schema.num_key_columns() == 1 ? 2 : 3;
    LOG(INFO) << "Update thread using schema: " << schema.ToString();

    KuduPartialRow row(&client_schema_);

    while (running_insert_count_.count() > 0) {
      unique_ptr<RowwiseIterator> iter;
      THR_RETURN_NOT_OK(tid, tablet()->NewRowIterator(client_schema_, &iter));
      THR_RETURN_NOT_OK(tid, iter->Init(nullptr));

      while (iter->HasNext() && running_insert_count_.count() > 0) {
        mem.Reset();
        THR_RETURN_NOT_OK(tid, iter->NextBlock(&block));
        CHECK_EQ(1, block.nrows());

        if (!block.selection_vector()->IsRowSelected(0)) {
          // Don't try to update rows which aren't visible yet --
          // this will crash, since the data in row_slice isn't even copied.
          continue;
        }

        RowBlockRow rb_row = block.row(0);
        if (rand() % 10 == 7) {
          // Increment the "val"
          const int32_t* old_val = schema.ExtractColumnFromRow<INT32>(rb_row, col_idx);
          // Issue an update. In the NullableValue setup, many of the rows start with
          // NULL here, so we have to check for it.
          const int32_t new_val = (old_val != nullptr) ? (*old_val + 1) : 0;

          // Rebuild the key by extracting the cells from the row
          setup_.BuildRowKeyFromExistingRow(&row, rb_row);
          THR_RETURN_NOT_OK(tid, row.SetInt32(col_idx, new_val));
          THR_RETURN_NOT_OK(tid, writer.Update(row));

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
  void CountThread(size_t /*group_tid*/, size_t tid) {
    rowid_t last_count = 0;
    while (running_insert_count_.count() > 0) {
      uint64_t count;
      THR_RETURN_NOT_OK(tid, tablet()->CountRows(&count));
      CHECK_GE(count, last_count);
      last_count = count;
    }
  }

  // Thread which iterates slowly over the first 10% of the data.
  // This is meant to test that outstanding iterators don't end up
  // trying to reference already-freed memrowset memory.
  void SlowReaderThread(size_t /*group_tid*/, size_t tid) {
    RowBlockMemory mem(32 * 1024);
    RowBlock block(&schema_, 1, &mem);

    const uint64_t max_rows =
        this->ClampRowCount(FLAGS_inserts_per_thread * FLAGS_num_insert_threads)
            / FLAGS_num_insert_threads;
    const int max_iters = FLAGS_num_insert_threads * max_rows / 10;

    while (running_insert_count_.count() > 0) {
      unique_ptr<RowwiseIterator> iter;
      THR_RETURN_NOT_OK(tid, tablet()->NewRowIterator(client_schema_, &iter));
      THR_RETURN_NOT_OK(tid, iter->Init(nullptr));

      for (int i = 0; i < max_iters && iter->HasNext(); i++) {
        mem.Reset();
        THR_RETURN_NOT_OK(tid, iter->NextBlock(&block));

        if (running_insert_count_.WaitFor(MonoDelta::FromMilliseconds(1))) {
          return;
        }
      }
    }
  }

  void SummerThread(size_t /*group_tid*/, size_t tid) {
    shared_ptr<TimeSeries> scanned_ts = ts_collector_.GetTimeSeries("scanned");
    while (running_insert_count_.count() > 0) {
      THR_RETURN_NOT_OK(tid, CountSum(scanned_ts));
    }
  }

  Status CountSum(const shared_ptr<TimeSeries>& scanned_ts, uint64_t* res = nullptr) {
    RowBlockMemory mem(1024);  // unused, just scanning ints

    constexpr const int kBufInts = 1024 * 1024 / 8;
    RowBlock block(&valcol_projection_, kBufInts, &mem);
    ColumnBlock column = block.column_block(0);

    uint64_t count_since_report = 0;

    int64_t sum = 0;

    unique_ptr<RowwiseIterator> iter;
    RETURN_NOT_OK(tablet()->NewRowIterator(valcol_projection_, &iter));
    RETURN_NOT_OK(iter->Init(nullptr));

    while (iter->HasNext()) {
      mem.Reset();
      RETURN_NOT_OK(iter->NextBlock(&block));

      for (size_t j = 0; j < block.nrows(); j++) {
        sum += *reinterpret_cast<const int32_t *>(column.cell_ptr(j));
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

    if (res) {
      *res = sum;
    }
    return Status::OK();
  }

  void FlushThread(size_t /*group_tid*/, size_t tid) {
    // Start off with a very short wait time between flushes.
    // But, especially in debug mode, this will only allow a few
    // rows to get inserted between each flush, and the test will take
    // quite a while. So, after every flush, we double the wait time below.
    int wait_time = FLAGS_flusher_initial_frequency_ms;
    while (running_insert_count_.count() > 0) {

      if (tablet()->MemRowSetSize() > FLAGS_tablet_test_flush_threshold_mb * 1024 * 1024) {
        THR_RETURN_NOT_OK(tid, tablet()->Flush());
      } else {
        LOG(INFO) << "Not flushing, memrowset not very full";
      }

      if (tablet()->DeltaMemStoresSize() > FLAGS_tablet_test_flush_threshold_mb * 1024 * 1024) {
        THR_RETURN_NOT_OK(tid, tablet()->FlushBiggestDMSForTests());
      }

      // Wait, unless the inserters are all done.
      running_insert_count_.WaitFor(MonoDelta::FromMilliseconds(wait_time));
      wait_time *= FLAGS_flusher_backoff;
    }
  }

  void FlushDeltasThread(size_t /*group_tid*/, size_t tid) {
    while (running_insert_count_.count() > 0) {
      THR_RETURN_NOT_OK(tid, tablet()->FlushBiggestDMSForTests());

      // Wait, unless the inserters are all done.
      running_insert_count_.WaitFor(kBackgroundOpInterval);
    }
  }

  void MinorCompactDeltasThread(size_t /*group_tid*/, size_t tid) {
    THR_RETURN_NOT_OK(tid, CompactDeltas(RowSet::MINOR_DELTA_COMPACTION));
  }

  void MajorCompactDeltasThread(size_t /*group_tid*/, size_t tid) {
    THR_RETURN_NOT_OK(tid, CompactDeltas(RowSet::MAJOR_DELTA_COMPACTION));
  }

  Status CompactDeltas(RowSet::DeltaCompactionType type) {
    while (running_insert_count_.count() > 0) {
      VLOG(1) << "Compacting worst deltas";
      RETURN_NOT_OK(tablet()->CompactWorstDeltas(type));

      // Wait, unless the inserters are all done.
      running_insert_count_.WaitFor(kBackgroundOpInterval);
    }
    return Status::OK();
  }

  void CompactThread(size_t /*group_tid*/, size_t tid) {
    while (running_insert_count_.count() > 0) {
      THR_RETURN_NOT_OK(tid, tablet()->Compact(Tablet::COMPACT_NO_FLAGS));

      // Wait, unless the inserters are all done.
      running_insert_count_.WaitFor(kBackgroundOpInterval);
    }
  }

  void DeleteAncientUndoDeltasThread(size_t /*group_tid*/, size_t tid) {
    while (running_insert_count_.count() > 0) {
      const MonoDelta time_budget = kBackgroundOpInterval;
      int64_t bytes_in_ancient_undos = 0;
      THR_RETURN_NOT_OK(tid, tablet()->InitAncientUndoDeltas(time_budget, &bytes_in_ancient_undos));
      VLOG(1) << "Found " << bytes_in_ancient_undos << " bytes of ancient delta undos";

      int64_t blocks_deleted = 0;
      int64_t bytes_deleted = 0;
      THR_RETURN_NOT_OK(tid, tablet()->DeleteAncientUndoDeltas(&blocks_deleted, &bytes_deleted));
      if (blocks_deleted > 0) {
        LOG(INFO) << "Deleted " << blocks_deleted << " blocks (" << bytes_deleted << " bytes) "
                  << "of ancient delta undos";
      }

      // Wait, unless the inserters are all done.
      running_insert_count_.WaitFor(kBackgroundOpInterval);
    }
  }

  // Thread that looks for rowsets that are ancient and fully deleted, GCing
  // those that are.
  void DeleteAncientDeletedRowsetsThreads(size_t /*group_tid*/, size_t tid) {
    do {
      // Number of bytes in ancient deleted rowsets.
      int64_t adr_bytes = 0;
      THR_RETURN_NOT_OK(tid, tablet()->GetBytesInAncientDeletedRowsets(&adr_bytes));
      VLOG(1) << Substitute("Found $0 bytes in ancient, fully deleted rowsets",
                            adr_bytes);
      if (adr_bytes > 0) {
        THR_RETURN_NOT_OK(tid, tablet()->DeleteAncientDeletedRowsets());
        LOG(INFO) << Substitute("Deleted $0 bytes found in ancient, fully deleted rowsets",
                                adr_bytes);
      }
    } while (!running_insert_count_.WaitFor(kBackgroundOpInterval));
  }

  // Thread which cycles between inserting and deleting a test row, each time
  // with a different value.
  void DeleteAndReinsertCycleThread(size_t group_tid, size_t tid) {
    int32_t iteration = 0;
    LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);

    while (running_insert_count_.count() > 0) {
      for (int i = 0; i < 100; i++) {
        THR_RETURN_NOT_OK(tid, this->InsertTestRow(&writer, group_tid, iteration++));
        THR_RETURN_NOT_OK(tid, this->DeleteTestRow(&writer, group_tid));
      }
    }
  }

  // Thread which continuously sends updates at the same row, ignoring any
  // "not found" errors that might come back. This is used simultaneously with
  // DeleteAndReinsertCycleThread to check for races where we might accidentally
  // succeed in UPDATING a ghost row.
  void StubbornlyUpdateSameRowThread(size_t group_tid, size_t tid) {
    int32_t iteration = 0;
    LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
    while (running_insert_count_.count() > 0) {
      for (int i = 0; i < 100; i++) {
        Status s = this->UpdateTestRow(&writer, group_tid, iteration++);
        if (!s.ok() && !s.IsNotFound()) {
          // We expect "not found", but not any other errors.
          threads_statuses_[tid] = std::move(s);
          return;
        }
      }
    }
  }

  // Thread which wakes up periodically and collects metrics like memrowset
  // size, etc. Eventually we should have a metrics system to collect things
  // like this, but for now, this is what we've got.
  void CollectStatisticsThread(size_t /*group_tid*/, size_t /*tid*/) {
    shared_ptr<TimeSeries> num_rowsets_ts = ts_collector_.GetTimeSeries(
      "num_rowsets");
    shared_ptr<TimeSeries> memrowset_size_ts = ts_collector_.GetTimeSeries(
      "memrowset_kb");
    shared_ptr<TimeSeries> num_live_rows_ts = ts_collector_.GetTimeSeries(
      "num_live_rows");

    while (running_insert_count_.count() > 0) {
      num_rowsets_ts->SetValue(tablet()->num_rowsets());
      memrowset_size_ts->SetValue(tablet()->MemRowSetSize() / 1024.0);
      uint64_t num_live_rows;
      ignore_result(tablet()->CountLiveRows(&num_live_rows));
      num_live_rows_ts->SetValue(num_live_rows);

      // Wait, unless the inserters are all done.
      running_insert_count_.WaitFor(MonoDelta::FromMilliseconds(10));
    }
  }

  // Start a group of 'threads_num' threads, each running the specified
  // 'function' routine. The first parameter of the function is the identifier
  // of the thread in its group, the second parameter is the global identifier
  // of the thread among all the threads started by the test fixture.  The
  // latter is used as the index of the corresponding data structures
  // in the 'threads_' and the 'threads_statuses_' containers.
  void StartThreads(size_t threads_num,
                    const std::function<void(size_t, size_t)>& function) {
    DCHECK_EQ(threads_.size(), threads_statuses_.size());
    const size_t idx_offset = threads_.size();
    for (size_t idx = 0; idx < threads_num; ++idx) {
      threads_statuses_.emplace_back(Status::OK());
      threads_.emplace_back([=]() { function(idx, idx_offset + idx); });
    }
  }

  void JoinThreads() {
    // First, allow all the threads to join.
    for (auto& t : threads_) {
      t.join();
    }

    // Now, check if there were errors reported by any of the threads.
    const auto size = threads_statuses_.size();
    DCHECK_EQ(threads_.size(), size);
    for (auto idx = 0; idx < size; ++idx) {
      SCOPED_TRACE(Substitute(
          "$0 thread ID $1", UnitTest::GetInstance()->current_test_info()->name(), idx));
      // Using EXPECT_OK() to allow for all the non-OK statuses to be reported.
      EXPECT_OK(threads_statuses_[idx]);
    }
  }

  deque<thread> threads_;
  deque<Status> threads_statuses_;

  CountDownLatch running_insert_count_;

  // Projection with only an int column.
  // This is provided by both harnesses.
  Schema valcol_projection_;

  TimeSeriesCollector ts_collector_;
};


TYPED_TEST_SUITE(MultiThreadedTabletTest, TabletTestHelperTypes);


TYPED_TEST(MultiThreadedTabletTest, DoTestAllAtOnce) {
  if (1000 == FLAGS_inserts_per_thread) {
    if (AllowSlowTests()) {
      FLAGS_inserts_per_thread = 50000;
    }
  }

  // Spawn a bunch of threads, each of which will do updates.
  this->StartThreads(1,
                     [this](size_t i, size_t j) { this->CollectStatisticsThread(i, j); });
  this->StartThreads(FLAGS_num_insert_threads,
                     [this](size_t i, size_t j) { this->InsertThread(i, j); });
  this->StartThreads(FLAGS_num_counter_threads,
                     [this](size_t i, size_t j) { this->CountThread(i, j); });
  this->StartThreads(FLAGS_num_summer_threads,
                     [this](size_t i, size_t j) { this->SummerThread(i, j); });
  this->StartThreads(FLAGS_num_flush_threads,
                     [this](size_t i, size_t j) { this->FlushThread(i, j); });
  this->StartThreads(FLAGS_num_compact_threads,
                     [this](size_t i, size_t j) { this->CompactThread(i, j); });
  this->StartThreads(FLAGS_num_undo_delta_gc_threads,
                     [this](size_t i, size_t j) { this->DeleteAncientUndoDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_flush_delta_threads,
                     [this](size_t i, size_t j) { this->FlushDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_minor_compact_deltas_threads,
                     [this](size_t i, size_t j) { this->MinorCompactDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_major_compact_deltas_threads,
                     [this](size_t i, size_t j) { this->MajorCompactDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_slowreader_threads,
                     [this](size_t i, size_t j) { this->SlowReaderThread(i, j); });
  this->StartThreads(FLAGS_num_updater_threads,
                     [this](size_t i, size_t j) { this->UpdateThread(i, j); });
  NO_FATALS(this->JoinThreads());

  LOG_TIMING(INFO, "Summing int32 column") {
    uint64_t sum = 0;
    ASSERT_OK(this->CountSum(shared_ptr<TimeSeries>(), &sum));
    LOG(INFO) << "Sum = " << sum;
  }

  const uint64_t max_rows = this->ClampRowCount(
      FLAGS_inserts_per_thread * FLAGS_num_insert_threads) / FLAGS_num_insert_threads;

  NO_FATALS(this->VerifyTestRows(0, max_rows * FLAGS_num_insert_threads));
}

// Start up a bunch of threads which repeatedly insert and delete the same
// row, while flushing and compacting. This checks various concurrent handling
// of DELETE/REINSERT during flushes.
TYPED_TEST(MultiThreadedTabletTest, DeleteAndReinsert) {
  google::FlagSaver saver;
  FLAGS_flusher_backoff = 1.0F;
  FLAGS_flusher_initial_frequency_ms = 1;
  FLAGS_tablet_delta_store_major_compact_min_ratio = 0.01F;
  FLAGS_tablet_delta_store_minor_compact_max = 10;
  this->StartThreads(1,
                     [this](size_t i, size_t j) { this->CollectStatisticsThread(i, j); });
  this->StartThreads(FLAGS_num_flush_threads,
                     [this](size_t i, size_t j) { this->FlushThread(i, j); });
  this->StartThreads(FLAGS_num_compact_threads,
                     [this](size_t i, size_t j) { this->CompactThread(i, j); });
  this->StartThreads(FLAGS_num_undo_delta_gc_threads,
                     [this](size_t i, size_t j) { this->DeleteAncientUndoDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_flush_delta_threads,
                     [this](size_t i, size_t j) { this->FlushDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_minor_compact_deltas_threads,
                     [this](size_t i, size_t j) { this->MinorCompactDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_major_compact_deltas_threads,
                     [this](size_t i, size_t j) { this->MajorCompactDeltasThread(i, j); });
  this->StartThreads(10,
                     [this](size_t i, size_t j) { this->DeleteAndReinsertCycleThread(i, j); });
  this->StartThreads(10,
                     [this](size_t i, size_t j) { this->StubbornlyUpdateSameRowThread(i, j); });

  // Run very quickly in dev builds, longer in slow builds.
  const float runtime_seconds = AllowSlowTests() ? 2 : 0.1;
  Stopwatch sw;
  sw.start();
  while (sw.elapsed().wall < runtime_seconds * NANOS_PER_SECOND &&
         !this->HasFatalFailure()) {
    SleepFor(MonoDelta::FromMicroseconds(5000));
  }

  // This is sort of a hack -- the flusher thread stops when it sees this
  // countdown latch go to 0.
  this->running_insert_count_.Reset(0);
  NO_FATALS(this->JoinThreads());
}

// For tests where we want to use the hybrid clock. The hybrid clock is
// required for tablet history gc.
template<class SETUP>
class MultiThreadedHybridClockTabletTest : public MultiThreadedTabletTest<SETUP> {
 public:
  MultiThreadedHybridClockTabletTest()
      : MultiThreadedTabletTest<SETUP>(TabletHarness::Options::ClockType::HYBRID_CLOCK) {
  }
};

TYPED_TEST_SUITE(MultiThreadedHybridClockTabletTest, TabletTestHelperTypes);

// Perform many updates and continuously flush and major compact deltas, as
// well as run undo delta gc.
TYPED_TEST(MultiThreadedHybridClockTabletTest, UpdateNoMergeCompaction) {
  google::FlagSaver saver;
  FLAGS_tablet_history_max_age_sec = 0; // GC data as aggressively as possible.

  FLAGS_flusher_backoff = 1.0F;
  FLAGS_flusher_initial_frequency_ms = 1;
  FLAGS_tablet_delta_store_major_compact_min_ratio = 0.01F;
  FLAGS_tablet_delta_store_minor_compact_max = 10;

  // Start up our background op threads, targeting the creation of delta files.
  this->StartThreads(FLAGS_num_flush_threads,
                     [this](size_t i, size_t j) { this->FlushThread(i, j); });
  this->StartThreads(FLAGS_num_flush_delta_threads,
                     [this](size_t i, size_t j) { this->FlushDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_major_compact_deltas_threads,
                     [this](size_t i, size_t j) { this->MajorCompactDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_undo_delta_gc_threads,
                     [this](size_t i, size_t j) { this->DeleteAncientUndoDeltasThread(i, j); });
  this->StartThreads(FLAGS_num_deleted_rowset_gc_threads,
                     [this](size_t i, size_t j) {
                       this->DeleteAncientDeletedRowsetsThreads(i, j); });
  // Start our workload threads, targeting the creation of deltas that we can
  // eventually GC.
  this->StartThreads(10,
                     [this](size_t i, size_t j) { this->DeleteAndReinsertCycleThread(i, j); });
  this->StartThreads(10,
                     [this](size_t i, size_t j) { this->StubbornlyUpdateSameRowThread(i, j); });

  // For good measure, we'll also start a thread that scans.
  this->StartThreads(FLAGS_num_summer_threads,
                     [this](size_t i, size_t j) { this->SummerThread(i, j); });

  // Run very quickly in dev builds, longer in slow builds.
  const float runtime_seconds = AllowSlowTests() ? 2 : 0.1;
  Stopwatch sw;
  sw.start();
  while (sw.elapsed().wall < runtime_seconds * NANOS_PER_SECOND &&
         !this->HasFatalFailure()) {
    SleepFor(MonoDelta::FromMilliseconds(5));
  }

  // This is sort of a hack -- the flusher thread stops when it sees this
  // countdown latch go to 0.
  this->running_insert_count_.Reset(0);
  NO_FATALS(this->JoinThreads());
}

} // namespace tablet
} // namespace kudu
