// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>
#include <tr1/unordered_map>

#include "gutil/stringprintf.h"
#include "gutil/walltime.h"
#include "util/thread_util.h"
#include "tablet/diskrowset-test-base.h"

enum {
  kDefaultNumSecondsPerThread = 1,
  kDefaultNumFlushThreads = 4,
  kDefaultNumCompactionThreads = 4,
};

DEFINE_int32(num_update_threads, 1, "Number of updater threads");
DEFINE_int32(num_alter_schema_threads, 0, "Number of AlterSchema threads");
DEFINE_int32(num_flush_threads, kDefaultNumFlushThreads, "Number of flusher threads");
DEFINE_int32(num_compaction_threads, kDefaultNumCompactionThreads, "Number of compaction threads");
DEFINE_int32(num_seconds_per_thread, kDefaultNumSecondsPerThread, "Minimum number of seconds each thread should work");

namespace kudu {
namespace tablet {

using base::subtle::Release_Store;
using base::subtle::Release_Load;
using base::subtle::NoBarrier_Load;

class TestMultiThreadedRowSetDeltaCompaction : public TestRowSet {
 public:

  TestMultiThreadedRowSetDeltaCompaction()
      : TestRowSet(),
        update_counter_(0) { }

  // This thread read the value of an atomic integer, updates all rows
  // in 'rs' to the value + 1, and then sets the atomic integer back
  // to value + 1. This is done so that the verifying threads knows the
  // latest expected value of the row (simply calling AtomicIncrement
  // won't work as a thread setting a value n+1 is not guaranteed to finish
  // before a thread setting value n).
  void RowSetUpdateThread(DiskRowSet *rs) {
    WallTime start_time = WallTime_Now();
    while (WallTime_Now() - start_time < FLAGS_num_seconds_per_thread) {
      uint32_t val = Release_Load(&update_counter_);
      UpdateRowSet(rs, val + 1);
      Release_Store(&update_counter_, val + 1);
    }
  }

  void RowSetFlushThread(DiskRowSet *rs) {
    WallTime start_time = WallTime_Now();
    while (WallTime_Now() - start_time < FLAGS_num_seconds_per_thread) {
      rs->FlushDeltas();
    }
  }

  void RowSetDeltaCompactionThread(DiskRowSet *rs) {
    WallTime start_time = WallTime_Now();
    while (WallTime_Now() - start_time < FLAGS_num_seconds_per_thread) {
      ASSERT_STATUS_OK(rs->MinorCompactDeltaStores());
    }
  }

  void RowSetAlterSchemaThread(DiskRowSet *rs, int col_prefix) {
    std::vector<ColumnSchema> columns(schema_.columns());
    uint32_t default_value = 10 * (1 + col_prefix);

    size_t count = 0;
    WallTime start_time = WallTime_Now();
    while (WallTime_Now() - start_time < FLAGS_num_seconds_per_thread) {
      columns.insert(columns.begin() + schema_.num_key_columns(),
                     ColumnSchema(StringPrintf("c%d-%zu", col_prefix, count), UINT32,
                                  false, &default_value, &default_value));
      ASSERT_STATUS_OK(rs->AlterSchema(Schema(columns, schema_.num_key_columns())));
      boost::this_thread::sleep(boost::posix_time::milliseconds(200));
      count++;
    }
  }

  void ReadVerify(DiskRowSet *rs) {
    Arena arena(1024, 1024*1024);
    RowBlock dst(schema_, 1000, &arena);
    gscoped_ptr<RowwiseIterator> iter;
    iter.reset(
        rs->NewRowIterator(schema_, MvccSnapshot::CreateSnapshotIncludingAllTransactions()));
    uint32_t expected = NoBarrier_Load(&update_counter_);
    ASSERT_STATUS_OK(iter->Init(NULL));
    while (iter->HasNext()) {
      size_t n = dst.nrows();
      ASSERT_STATUS_OK_FAST(iter->PrepareBatch(&n));
      ASSERT_GT(n, 0);
      ASSERT_STATUS_OK_FAST(iter->MaterializeBlock(&dst));
      ASSERT_STATUS_OK_FAST(iter->FinishBatch());
      for (size_t j = 0; j < n; j++) {
        uint32_t val = *schema_.ExtractColumnFromRow<UINT32>(dst.row(j), 1);
        ASSERT_GE(val, expected);
      }
    }
  }

  void StartThreads(DiskRowSet *rs) {
    for (int i = 0; i < FLAGS_num_update_threads; i++) {
      update_threads_.push_back(new boost::thread(
            &TestMultiThreadedRowSetDeltaCompaction::RowSetUpdateThread,
            this, rs));
    }
    for (int i = 0; i < FLAGS_num_flush_threads; i++) {
      flush_threads_.push_back(new boost::thread(
          &TestMultiThreadedRowSetDeltaCompaction::RowSetFlushThread, this,
          rs));
    }
    for (int i = 0; i < FLAGS_num_compaction_threads; i++) {
      compaction_threads_.push_back(new boost::thread(
          &TestMultiThreadedRowSetDeltaCompaction::RowSetDeltaCompactionThread, this,
          rs));
    }
    for (int i = 0; i < FLAGS_num_alter_schema_threads; i++) {
      alter_schema_threads_.push_back(new boost::thread(
          &TestMultiThreadedRowSetDeltaCompaction::RowSetAlterSchemaThread, this,
          rs, i));
    }
  }

  void JoinThreads() {
    for (int i = 0; i < update_threads_.size(); i++) {
      ASSERT_STATUS_OK(ThreadJoiner(&update_threads_[i],
                                    StringPrintf("rowset update thread %d", i)).Join());
    }
    for (int i = 0; i < flush_threads_.size(); i++) {
      ASSERT_STATUS_OK(ThreadJoiner(&flush_threads_[i],
                                    StringPrintf("delta flush thread %d", i)).Join());
    }
    for (int i = 0; i < compaction_threads_.size(); i++) {
      ASSERT_STATUS_OK(ThreadJoiner(&compaction_threads_[i],
                                    StringPrintf("delta compaction thread %d", i)).Join());
    }
    for (int i = 0; i < alter_schema_threads_.size(); i++) {
      ASSERT_STATUS_OK(ThreadJoiner(&alter_schema_threads_[i],
                                    StringPrintf("alter schema thread %d", i)).Join());
    }
  }

  void WriteTestRowSetWithZeros() {
    WriteTestRowSet(0, true);
  }

  void UpdateRowSet(DiskRowSet *rs, uint32_t value) {
    for (uint32_t idx = 0; idx < n_rows_; idx++) {
      MutationResultPB result;
      ASSERT_STATUS_OK_FAST(UpdateRow(rs, idx, value, &result));
    }
  }

  void TestUpdateAndVerify() {
    WriteTestRowSetWithZeros();
    shared_ptr<DiskRowSet> rs;
    ASSERT_STATUS_OK(OpenTestRowSet(&rs));

    StartThreads(rs.get());
    ASSERT_NO_FATAL_FAILURE(JoinThreads());

    ASSERT_NO_FATAL_FAILURE(ReadVerify(rs.get()));
  }

 protected:

  Atomic32 update_counter_;
  ptr_vector<boost::thread> update_threads_;
  ptr_vector<boost::thread> flush_threads_;
  ptr_vector<boost::thread> compaction_threads_;
  ptr_vector<boost::thread> alter_schema_threads_;
};

static void SetupFlagsForSlowTests() {
  if (kDefaultNumSecondsPerThread == FLAGS_num_seconds_per_thread) {
    FLAGS_num_seconds_per_thread = 40;
  }
  if (kDefaultNumFlushThreads == FLAGS_num_flush_threads) {
    FLAGS_num_flush_threads = 8;
  }
  if (kDefaultNumCompactionThreads == FLAGS_num_compaction_threads) {
    FLAGS_num_compaction_threads = 8;
  }
}

TEST_F(TestMultiThreadedRowSetDeltaCompaction, TestMTUpdateAndCompact) {
  if (AllowSlowTests()) {
    SetupFlagsForSlowTests();
  }

  TestUpdateAndVerify();
}

TEST_F(TestMultiThreadedRowSetDeltaCompaction, TestMTAlterSchemaAndUpdates) {
  if (AllowSlowTests()) {
    SetupFlagsForSlowTests();
  }

  if (FLAGS_num_alter_schema_threads == 0) {
    FLAGS_num_alter_schema_threads = 2;
  }

  TestUpdateAndVerify();
}

} // namespace tablet
} // namespace kudu
