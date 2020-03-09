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

#include "kudu/tablet/deltamemstore.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/logical_clock.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/delta_store.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/mutation.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(benchmark_num_passes,
#ifdef NDEBUG
             100,
#else
             1,
#endif
             "Number of passes to apply deltas in the benchmark");

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

using fs::WritableBlock;

class TestDeltaMemStore : public KuduTest {
 public:
  TestDeltaMemStore()
      : op_id_(consensus::MaximumOpId()),
        schema_(CreateSchema()),
        clock_(Timestamp::kInitialTimestamp) {
    CHECK_OK(DeltaMemStore::Create(0, 0,
                                   new log::LogAnchorRegistry(),
                                   MemTracker::GetRootTracker(), &dms_));
    CHECK_OK(dms_->Init(nullptr));
  }

  static Schema CreateSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddColumn("col1", STRING));
    CHECK_OK(builder.AddColumn("col2", STRING));
    CHECK_OK(builder.AddColumn("col3", UINT32));
    return builder.Build();
  }

  template<class Iterable>
  void UpdateIntsAtIndexes(const Iterable &indexes_to_update) {
    faststring buf;
    RowChangeListEncoder update(&buf);

    for (uint32_t idx_to_update : indexes_to_update) {
      ScopedTransaction tx(&mvcc_, clock_.Now());
      tx.StartApplying();
      update.Reset();
      uint32_t new_val = idx_to_update * 10;
      update.AddColumnUpdate(schema_.column(kIntColumn),
                             schema_.column_id(kIntColumn), &new_val);

      CHECK_OK(dms_->Update(tx.timestamp(), idx_to_update, RowChangeList(buf), op_id_));
      tx.Commit();
    }
  }

  void ApplyUpdates(const MvccSnapshot &snapshot,
                    uint32_t row_idx,
                    size_t col_idx,
                    ColumnBlock *cb) {
    ColumnSchema col_schema(schema_.column(col_idx));
    Schema single_col_projection({ col_schema },
                                 { schema_.column_id(col_idx) },
                                 0);
    RowIteratorOptions opts;
    opts.projection = &single_col_projection;
    opts.snap_to_include = snapshot;
    unique_ptr<DeltaIterator> iter;
    Status s = dms_->NewDeltaIterator(opts, &iter);
    if (s.IsNotFound()) {
      return;
    }
    ASSERT_OK(s);
    ASSERT_OK(iter->Init(nullptr));
    ASSERT_OK(iter->SeekToOrdinal(row_idx));
    ASSERT_OK(iter->PrepareBatch(cb->nrows(), DeltaIterator::PREPARE_FOR_APPLY));
    SelectionVector filter(cb->nrows());
    filter.SetAllTrue();
    ASSERT_OK(iter->ApplyDeletes(&filter));
    ASSERT_OK(iter->ApplyUpdates(0, cb, filter));
  }

 protected:
  static const int kStringColumn = 1;
  static const int kIntColumn = 2;

  consensus::OpId op_id_;

  const Schema schema_;
  shared_ptr<DeltaMemStore> dms_;
  clock::LogicalClock clock_;
  MvccManager mvcc_;
};

static void GenerateRandomIndexes(uint32_t range, uint32_t count,
                                  unordered_set<uint32_t> *out) {
  CHECK_LE(count, range / 2) <<
    "this will be too slow unless count is much smaller than range";
  out->clear();

  for (int i = 0; i < count; i++) {
    bool inserted = false;
    do {
      inserted = out->insert(random() % range).second;
    } while (!inserted);
  }
}

TEST_F(TestDeltaMemStore, TestUpdateCount) {
  uint32_t n_rows = 1000;
  faststring update_buf;

  RowChangeListEncoder update(&update_buf);
  for (uint32_t idx = 0; idx < n_rows; idx++) {
    update.Reset();
    if (idx % 4 == 0) {
      char buf[256] = "update buf";
      Slice s(buf);
      update.AddColumnUpdate(schema_.column(kStringColumn),
                             schema_.column_id(kStringColumn), &s);
    }
    if (idx % 2 == 0) {
      ScopedTransaction tx(&mvcc_, clock_.Now());
      tx.StartApplying();
      uint32_t new_val = idx * 10;
      update.AddColumnUpdate(schema_.column(kIntColumn),
                             schema_.column_id(kIntColumn), &new_val);
      ASSERT_OK_FAST(dms_->Update(tx.timestamp(), idx, RowChangeList(update_buf), op_id_));
      tx.Commit();
    }
  }


  // Flush the delta file so that the stats get updated.

  FsManager fs(env_, FsManagerOpts(GetTestPath("fs_root")));
  ASSERT_OK(fs.CreateInitialFileSystemLayout());
  ASSERT_OK(fs.Open());
  unique_ptr<WritableBlock> block;
  ASSERT_OK(fs.CreateNewBlock({}, &block));
  DeltaFileWriter dfw(std::move(block));
  ASSERT_OK(dfw.Start());
  unique_ptr<DeltaStats> stats;
  dms_->FlushToFile(&dfw, &stats);

  ASSERT_EQ(n_rows / 2, stats->update_count_for_col_id(schema_.column_id(kIntColumn)));
  ASSERT_EQ(n_rows / 4, stats->update_count_for_col_id(schema_.column_id(kStringColumn)));
}

TEST_F(TestDeltaMemStore, TestDMSSparseUpdates) {

  int n_rows = 1000;

  // Update 100 random rows out of the 1000.
  srand(12345);
  unordered_set<uint32_t> indexes_to_update;
  GenerateRandomIndexes(n_rows, 100, &indexes_to_update);
  UpdateIntsAtIndexes(indexes_to_update);
  ASSERT_EQ(100, dms_->Count());

  // Now apply the updates from the DMS back to an array
  ScopedColumnBlock<UINT32> read_back(1000);
  for (int i = 0; i < 1000; i++) {
    read_back[i] = 0xDEADBEEF;
  }
  MvccSnapshot snap(mvcc_);
  ApplyUpdates(snap, 0, kIntColumn, &read_back);

  // And verify that only the rows that we updated are modified within
  // the array.
  for (int i = 0; i < 1000; i++) {
    // If this wasn't one of the ones we updated, expect our marker
    if (indexes_to_update.find(i) == indexes_to_update.end()) {
      // If this wasn't one of the ones we updated, expect our marker
      ASSERT_EQ(0xDEADBEEF, read_back[i]);
    } else {
      // Otherwise expect the updated value
      ASSERT_EQ(i * 10, read_back[i]);
    }
  }
}

// Performance test for KUDU-749: zipfian workloads can cause a lot
// of updates to a single row. This benchmark updates a single row many
// times and times how long it takes to apply those updates during
// the read path.
TEST_F(TestDeltaMemStore, BenchmarkManyUpdatesToOneRow) {
  const int kNumRows = 1000;
  const int kNumUpdates = 10000;
  const int kIdxToUpdate = 10;
  const int kStringDataSize = 1000;

  for (int i = 0; i < kNumUpdates; i++) {
    faststring buf;
    RowChangeListEncoder update(&buf);

    ScopedTransaction tx(&mvcc_, clock_.Now());
    tx.StartApplying();
    string str(kStringDataSize, 'x');
    Slice s(str);
    update.AddColumnUpdate(schema_.column(kStringColumn),
                           schema_.column_id(kStringColumn), &s);
    CHECK_OK(dms_->Update(tx.timestamp(), kIdxToUpdate, RowChangeList(buf), op_id_));
    tx.Commit();
  }
  mvcc_.AdjustNewTransactionLowerBound(clock_.Now());

  MvccSnapshot snap(mvcc_);
  LOG_TIMING(INFO, "Applying updates") {
    for (int i = 0; i < FLAGS_benchmark_num_passes; i++) {
      ScopedColumnBlock<STRING> strings(kNumRows);
      for (int i = 0; i < kNumRows; i++) {
        strings[i] = Slice();
      }

      ApplyUpdates(snap, 0, kStringColumn, &strings);
    }
  }
}

class TestDeltaMemStoreNumUpdates : public TestDeltaMemStore,
                                    public ::testing::WithParamInterface<int> {
};

INSTANTIATE_TEST_CASE_P(DifferentNumUpdates,
                        TestDeltaMemStoreNumUpdates, ::testing::Values(2, 20, 200));

TEST_P(TestDeltaMemStoreNumUpdates, BenchmarkSnapshotScans) {
  const int kNumRows = 100;

  // Populate the DMS with kNumRows * GetParam() updates. For each row, every
  // update is at a different timestamp.
  faststring buf;
  RowChangeListEncoder update(&buf);
  LOG_TIMING(INFO, Substitute("updating $0 rows $1 times each", kNumRows, GetParam())) {
    for (rowid_t row_idx = 0; row_idx < kNumRows; row_idx++) {
      for (int ts_val = 0; ts_val < GetParam(); ts_val++) {
        update.Reset();

        Timestamp ts(ts_val);
        uint32_t new_val = ts_val;
        update.AddColumnUpdate(schema_.column(kIntColumn),
                               schema_.column_id(kIntColumn), &new_val);
        CHECK_OK(dms_->Update(ts, row_idx, RowChangeList(buf), op_id_));
      }
    }
  }

  // Now scan the DMS at each timestamp. The scans are repeated in a number of
  // passes to stabilize the results.
  ScopedColumnBlock<UINT32> ints(kNumRows);
  LOG_TIMING(INFO, Substitute("running $0 scans for each timestamp",
                              FLAGS_benchmark_num_passes)) {
    for (int ts_val = 0; ts_val < GetParam(); ts_val++) {
      LOG_TIMING(INFO, Substitute("running $0 scans at timestamp $1",
                                  FLAGS_benchmark_num_passes, ts_val)) {
        for (int pass = 0; pass < FLAGS_benchmark_num_passes; pass++) {
          Timestamp ts(ts_val);
          MvccSnapshot snap(ts);
          NO_FATALS(ApplyUpdates(snap, 0, kIntColumn, &ints));
        }
      }
    }
  }
}

class TestDeltaMemStoreNumDeletes : public TestDeltaMemStore,
                                    public ::testing::WithParamInterface<int> {
};

INSTANTIATE_TEST_CASE_P(DifferentNumDeletes,
                        TestDeltaMemStoreNumDeletes, ::testing::Values(0, 10, 100, 1000));

TEST_P(TestDeltaMemStoreNumDeletes, BenchmarkScansWithVaryingNumberOfDeletes) {
  const int kNumUpdates = 10000;

  // Populate the DMS with kNumRows updates, one to each row.
  faststring buf;
  RowChangeListEncoder update(&buf);
  rowid_t delete_stepping = GetParam() != 0 ? kNumUpdates / GetParam() : 0;
  for (rowid_t row_idx = 0; row_idx < kNumUpdates; row_idx++) {
    update.Reset();

    uint32_t new_val = row_idx;
    update.AddColumnUpdate(schema_.column(kIntColumn),
                           schema_.column_id(kIntColumn), &new_val);
    ASSERT_OK(dms_->Update(Timestamp(row_idx), row_idx, RowChangeList(buf), op_id_));

    // When appropriate, add a DELETE too.
    if (delete_stepping != 0 && row_idx % delete_stepping == 0) {
      update.Reset();
      update.SetToDelete();
      ASSERT_OK(dms_->Update(Timestamp(row_idx + 1), row_idx, RowChangeList(buf), op_id_));
    }
  }

  // Now scan the DMS. The scans are repeated in a number of passes to stabilize
  // the results.
  ScopedColumnBlock<UINT32> ints(kNumUpdates);
  MvccSnapshot snap(MvccSnapshot::CreateSnapshotIncludingAllTransactions());
  LOG_TIMING(INFO, Substitute("running $0 scans with $1 deletes",
                              FLAGS_benchmark_num_passes, GetParam())) {
    for (int pass = 0; pass < FLAGS_benchmark_num_passes; pass++) {
      NO_FATALS(ApplyUpdates(snap, 0, kIntColumn, &ints));
    }
  }
}

// Test when a slice column has been updated multiple times in the
// memrowset that the referred to values properly end up in the
// right arena.
TEST_F(TestDeltaMemStore, TestReUpdateSlice) {
  faststring update_buf;
  RowChangeListEncoder update(&update_buf);

  // Update a cell, taking care that the buffer we use to perform
  // the update gets cleared after usage. This ensures that the
  // underlying data is properly copied into the DMS arena.
  {
    ScopedTransaction tx(&mvcc_, clock_.Now());
    tx.StartApplying();
    char buf[256] = "update 1";
    Slice s(buf);
    update.AddColumnUpdate(schema_.column(0),
                           schema_.column_id(0), &s);
    ASSERT_OK_FAST(dms_->Update(tx.timestamp(), 123, RowChangeList(update_buf), op_id_));
    memset(buf, 0xff, sizeof(buf));
    tx.Commit();
  }
  MvccSnapshot snapshot_after_first_update(mvcc_);

  // Update the same cell again with a different value
  {
    ScopedTransaction tx(&mvcc_, clock_.Now());
    tx.StartApplying();
    char buf[256] = "update 2";
    Slice s(buf);
    update.Reset();
    update.AddColumnUpdate(schema_.column(0),
                           schema_.column_id(0), &s);
    ASSERT_OK_FAST(dms_->Update(tx.timestamp(), 123, RowChangeList(update_buf), op_id_));
    memset(buf, 0xff, sizeof(buf));
    tx.Commit();
  }
  MvccSnapshot snapshot_after_second_update(mvcc_);

  // Ensure we end up with a second entry for the cell, at the
  // new timestamp
  ASSERT_EQ(2, dms_->Count());

  // Ensure that we ended up with the right data, and that the old MVCC snapshot
  // yields the correct old value.
  ScopedColumnBlock<STRING> read_back(1);
  ApplyUpdates(snapshot_after_first_update, 123, 0, &read_back);
  ASSERT_EQ("update 1", read_back[0].ToString());

  ApplyUpdates(snapshot_after_second_update, 123, 0, &read_back);
  ASSERT_EQ("update 2", read_back[0].ToString());
}

// Test that if two updates come in with out-of-order transaction IDs,
// the one with the higher transaction ID ends up winning.
//
// This is important during flushing when updates against the old rowset
// are carried forward, but may fall behind newer transactions.
TEST_F(TestDeltaMemStore, TestOutOfOrderTxns) {
  faststring update_buf;
  RowChangeListEncoder update(&update_buf);

  {
    ScopedTransaction tx1(&mvcc_, clock_.Now());
    ScopedTransaction tx2(&mvcc_, clock_.Now());

    tx2.StartApplying();
    Slice s("update 2");
    update.AddColumnUpdate(schema_.column(kStringColumn),
                           schema_.column_id(kStringColumn), &s);
    ASSERT_OK(dms_->Update(tx2.timestamp(), 123, RowChangeList(update_buf), op_id_));
    tx2.Commit();


    tx1.StartApplying();
    update.Reset();
    s = Slice("update 1");
    update.AddColumnUpdate(schema_.column(kStringColumn),
                           schema_.column_id(kStringColumn), &s);
    ASSERT_OK(dms_->Update(tx1.timestamp(), 123, RowChangeList(update_buf), op_id_));
    tx1.Commit();
  }

  // Ensure we end up two entries for the cell.
  ASSERT_EQ(2, dms_->Count());

  // Ensure that we ended up with the right data.
  ScopedColumnBlock<STRING> read_back(1);
  ApplyUpdates(MvccSnapshot(mvcc_), 123, kStringColumn, &read_back);
  ASSERT_EQ("update 2", read_back[0].ToString());
}

TEST_F(TestDeltaMemStore, TestDMSBasic) {
  faststring update_buf;
  RowChangeListEncoder update(&update_buf);

  char buf[256];
  for (uint32_t i = 0; i < 1000; i++) {
    ScopedTransaction tx(&mvcc_, clock_.Now());
    tx.StartApplying();
    update.Reset();

    uint32_t val = i * 10;
    update.AddColumnUpdate(schema_.column(kIntColumn),
                           schema_.column_id(kIntColumn), &val);

    snprintf(buf, sizeof(buf), "hello %d", i);
    Slice s(buf);
    update.AddColumnUpdate(schema_.column(kStringColumn),
                           schema_.column_id(kStringColumn), &s);

    ASSERT_OK_FAST(dms_->Update(tx.timestamp(), i, RowChangeList(update_buf), op_id_));
    tx.Commit();
  }

  ASSERT_EQ(1000, dms_->Count());

  // Read back the values and check correctness.
  MvccSnapshot snap(mvcc_);
  ScopedColumnBlock<UINT32> read_back(1000);
  ScopedColumnBlock<STRING> read_back_slices(1000);
  ApplyUpdates(snap, 0, kIntColumn, &read_back);
  ApplyUpdates(snap, 0, kStringColumn, &read_back_slices);

  // When reading back the slice, do so into a different buffer -
  // otherwise if the slice references weren't properly copied above,
  // we'd be writing our comparison value into the same buffer that
  // we're comparing against!
  char buf2[256];
  for (uint32_t i = 0; i < 1000; i++) {
    ASSERT_EQ(i * 10, read_back[i]) << "failed at iteration " << i;
    snprintf(buf2, sizeof(buf2), "hello %d", i);
    Slice s(buf2);
    ASSERT_EQ(0, s.compare(read_back_slices[i]));
  }


  // Update the same rows again, with new transactions. Even though
  // the same rows are updated, new entries should be added because
  // these are separate transactions and we need to maintain the
  // old ones for snapshot consistency purposes.
  for (uint32_t i = 0; i < 1000; i++) {
    ScopedTransaction tx(&mvcc_, clock_.Now());
    tx.StartApplying();
    update.Reset();

    uint32_t val = i * 20;
    update.AddColumnUpdate(schema_.column(kIntColumn),
                           schema_.column_id(kIntColumn), &val);
    ASSERT_OK_FAST(dms_->Update(tx.timestamp(), i, RowChangeList(update_buf), op_id_));
    tx.Commit();
  }

  ASSERT_EQ(2000, dms_->Count());
}

TEST_F(TestDeltaMemStore, TestIteratorDoesUpdates) {
  unordered_set<uint32_t> to_update;
  for (uint32_t i = 0; i < 1000; i++) {
    to_update.insert(i);
  }
  UpdateIntsAtIndexes(to_update);
  ASSERT_EQ(1000, dms_->Count());

  ScopedColumnBlock<UINT32> block(100);

  RowIteratorOptions opts;
  opts.projection = &schema_;
  // TODO(todd): test snapshot reads from different points
  opts.snap_to_include = MvccSnapshot(mvcc_);

  unique_ptr<DeltaIterator> iter;
  Status s = dms_->NewDeltaIterator(opts, &iter);
  if (s.IsNotFound()) {
    FAIL() << "Iterator fell outside of the range of the snapshot";
  }
  ASSERT_OK(s);
  ASSERT_OK(iter->Init(nullptr));

  int block_start_row = 50;
  ASSERT_OK(iter->SeekToOrdinal(block_start_row));
  ASSERT_OK(iter->PrepareBatch(block.nrows(), DeltaIterator::PREPARE_FOR_APPLY));
  SelectionVector sv(block.nrows());
  sv.SetAllTrue();
  ASSERT_OK(iter->ApplyUpdates(kIntColumn, &block, sv));

  for (int i = 0; i < 100; i++) {
    int actual_row = block_start_row + i;
    ASSERT_EQ(actual_row * 10, block[i]) << "at row " << actual_row;
  }

  // Apply the next block
  block_start_row += block.nrows();
  ASSERT_OK(iter->PrepareBatch(block.nrows(), DeltaIterator::PREPARE_FOR_APPLY));
  ASSERT_OK(iter->ApplyUpdates(kIntColumn, &block, sv));
  for (int i = 0; i < 100; i++) {
    int actual_row = block_start_row + i;
    ASSERT_EQ(actual_row * 10, block[i]) << "at row " << actual_row;
  }
}

TEST_F(TestDeltaMemStore, TestCollectMutations) {
  Arena arena(1024);

  // Update rows 5 and 12
  vector<uint32_t> to_update;
  to_update.push_back(5);
  to_update.push_back(12);
  UpdateIntsAtIndexes(to_update);

  ASSERT_EQ(2, dms_->Count());

  const int kBatchSize = 10;
  vector<Mutation *> mutations;
  mutations.resize(kBatchSize);

  RowIteratorOptions opts;
  opts.projection = &schema_;
  opts.snap_to_include = MvccSnapshot(mvcc_);
  unique_ptr<DeltaIterator> iter;
  Status s =  dms_->NewDeltaIterator(opts, &iter);
  if (s.IsNotFound()) {
    FAIL() << "Iterator fell outside of the range of the snapshot";
  }
  ASSERT_OK(s);
  ASSERT_OK(iter->Init(nullptr));
  ASSERT_OK(iter->SeekToOrdinal(0));
  ASSERT_OK(iter->PrepareBatch(kBatchSize, DeltaIterator::PREPARE_FOR_COLLECT));
  ASSERT_OK(iter->CollectMutations(&mutations, &arena));

  // Only row 5 is updated, everything else should be NULL.
  for (int i = 0; i < kBatchSize; i++) {
    string str = Mutation::StringifyMutationList(schema_, mutations[i]);
    VLOG(1) << "row " << i << ": " << str;
    if (i != 5) {
      EXPECT_EQ("[]", str);
    } else {
      EXPECT_EQ("[@1(SET col3=50)]", str);
    }
  }

  // Collect the next batch of 10.
  arena.Reset();
  std::fill(mutations.begin(), mutations.end(), reinterpret_cast<Mutation *>(NULL));
  ASSERT_OK(iter->PrepareBatch(kBatchSize, DeltaIterator::PREPARE_FOR_COLLECT));
  ASSERT_OK(iter->CollectMutations(&mutations, &arena));

  // Only row 2 is updated, everything else should be NULL.
  for (int i = 0; i < 10; i++) {
    string str = Mutation::StringifyMutationList(schema_, mutations[i]);
    VLOG(1) << "row " << i << ": " << str;
    if (i != 2) {
      EXPECT_EQ("[]", str);
    } else {
      EXPECT_EQ("[@2(SET col3=120)]", str);
    }
  }
}

// Generates a series of random deltas,  writes them to a DMS, reads them back
// using a DMSIterator, and verifies the results.
TEST_F(TestDeltaMemStore, TestFuzz) {
  // Arbitrary constants to control the running time and coverage of the test.
  const int kNumColumns = 100;
  const int kNumRows = 1000;
  const int kNumDeltas = 10000;
  const std::pair<uint64_t, uint64_t> kTimestampRange(0, 100);

  // Build a schema with kNumColumns columns, some of which are nullable.
  Random prng(SeedRandom());
  SchemaBuilder sb;
  for (int i = 0; i < kNumColumns; i++) {
    if (prng.Uniform(10) == 0) {
      ASSERT_OK(sb.AddNullableColumn(Substitute("col$0", i), UINT32));
    } else {
      ASSERT_OK(sb.AddColumn(Substitute("col$0", i), UINT32));
    }
  }
  Schema schema(sb.Build());

  MirroredDeltas<DeltaTypeSelector<REDO>> deltas(&schema);

  shared_ptr<DeltaMemStore> dms;
  ASSERT_OK(CreateRandomDMS(
      schema, &prng, kNumDeltas, { 0, kNumRows }, kTimestampRange, &deltas, &dms));

  NO_FATALS(RunDeltaFuzzTest<DeltaTypeSelector<REDO>>(
      *dms, &prng, &deltas, kTimestampRange,
      /*test_filter_column_ids_and_collect_deltas=*/false));
}

TEST_F(TestDeltaMemStore, TestDeletedRowCount) {
  const int kNumUpdates = 10000;

  faststring buf;
  RowChangeListEncoder update(&buf);
  for (rowid_t row_idx = 0; row_idx < kNumUpdates; row_idx++) {
    // UPDATE.
    uint32_t new_val = row_idx;
    update.Reset();
    update.AddColumnUpdate(schema_.column(kIntColumn), schema_.column_id(kIntColumn), &new_val);
    ASSERT_OK(dms_->Update(Timestamp(row_idx), row_idx, RowChangeList(buf), op_id_));

    // DELETE.
    if (row_idx % 2 == 0) {
      update.Reset();
      update.SetToDelete();
      ASSERT_OK(dms_->Update(Timestamp(row_idx + 1), row_idx, RowChangeList(buf), op_id_));
    }
  }
  ASSERT_EQ(kNumUpdates / 2, dms_->deleted_row_count());
}

} // namespace tablet
} // namespace kudu
