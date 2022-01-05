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

#include "kudu/tablet/tablet.h"

#include <algorithm>
#include <cstdint>
#include <ctime>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/cfile/cfile_util.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator.h"
#include "kudu/common/key_range.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/row_operations.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/tablet/delta_key.h"
#include "kudu/tablet/delta_stats.h"
#include "kudu/tablet/deltafile.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/mock-rowsets.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/rowset_tree.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metrics.h" // IWYU pragma: keep
#include "kudu/util/faststring.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"

DEFINE_int32(testflush_num_inserts, 1000,
             "Number of rows inserted in TestFlush");
DEFINE_int32(testiterator_num_inserts, 1000,
             "Number of rows inserted in TestRowIterator/TestInsert");
DEFINE_int32(testcompaction_num_rows, 1000,
             "Number of rows per rowset in TestCompaction");

using kudu::cfile::ReaderOptions;
using kudu::fs::ReadableBlock;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

template<class SETUP>
class TestTablet : public TabletTestBase<SETUP> {
  typedef SETUP Type;

public:
  // Verify that iteration doesn't fail
  void CheckCanIterate() {
    vector<string> out_rows;
    ASSERT_OK(this->IterateToStringList(&out_rows));
  }

  void CheckLiveRowsCount(int64_t expect) {
    uint64_t count = 0;
    ASSERT_OK(this->tablet()->CountLiveRows(&count));
    ASSERT_EQ(expect, count);
  }
};
TYPED_TEST_SUITE(TestTablet, TabletTestHelperTypes);

TYPED_TEST(TestTablet, TestFlush) {
  // Insert 1000 rows into memrowset
  uint64_t max_rows = this->ClampRowCount(FLAGS_testflush_num_inserts);
  this->InsertTestRows(0, max_rows, 0);

  // Pre-flush, there should be no data on disk, and no diskrowsets, so the
  // on-disk size of the tablet should be size of the tablet metadata.
  ASSERT_EQ(0, this->tablet()->OnDiskDataSize());
  ASSERT_GT(this->tablet()->OnDiskSize(), 0);
  ASSERT_EQ(this->tablet()->metadata()->on_disk_size(), this->tablet()->OnDiskSize());
  NO_FATALS(this->CheckLiveRowsCount(max_rows));

  // Flush it.
  ASSERT_OK(this->tablet()->Flush());
  TabletMetadata* tablet_meta = this->tablet()->metadata();

  // Post-flush, there should be data on-disk. On-disk size should exceed
  // on-disk data size due to per-diskrowset metadata and tablet metadata.
  ASSERT_GT(this->tablet()->OnDiskDataSize(), 0);
  ASSERT_GT(this->tablet()->OnDiskSize(), this->tablet()->OnDiskDataSize());
  NO_FATALS(this->CheckLiveRowsCount(max_rows));

  // Make sure the files were created as expected.
  RowSetMetadata* rowset_meta = tablet_meta->GetRowSetForTests(0);
  CHECK(rowset_meta) << "No row set found";
  ASSERT_TRUE(rowset_meta->HasDataForColumnIdForTests(this->schema_.column_id(0)));
  ASSERT_TRUE(rowset_meta->HasDataForColumnIdForTests(this->schema_.column_id(1)));
  ASSERT_TRUE(rowset_meta->HasDataForColumnIdForTests(this->schema_.column_id(2)));
  ASSERT_TRUE(rowset_meta->HasBloomDataBlockForTests());

  // check that undo deltas are present
  vector<BlockId> undo_blocks = rowset_meta->undo_delta_blocks();
  ASSERT_EQ(1, undo_blocks.size());

  // Read the undo delta, we should get one undo mutation (delete) for each row.
  unique_ptr<ReadableBlock> block;
  ASSERT_OK(this->fs_manager()->OpenBlock(undo_blocks[0], &block));

  shared_ptr<DeltaFileReader> dfr;
  ASSERT_OK(DeltaFileReader::Open(std::move(block), UNDO, ReaderOptions(), &dfr));
  // Assert there were 'max_rows' deletions in the undo delta (one for each inserted row)
  ASSERT_EQ(dfr->delta_stats().delete_count(), max_rows);
}

// Test that historical data for a row is maintained even after the row
// is flushed from the memrowset.
TYPED_TEST(TestTablet, TestInsertsAndMutationsAreUndoneWithMVCCAfterFlush) {
  // Insert 5 rows into the memrowset.
  // After the first one, each time we insert a new row we mutate
  // the previous one.

  // Take snapshots after each operation
  vector<MvccSnapshot> snaps;
  snaps.push_back(MvccSnapshot(*this->tablet()->mvcc_manager()));

  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  for (int i = 0; i < 5; i++) {
    this->InsertTestRows(i, 1, 0);
    DVLOG(1) << "Inserted row=" << i << ", row_idx=" << i << ", val=0";
    MvccSnapshot ins_snaphsot(*this->tablet()->mvcc_manager());
    snaps.push_back(ins_snaphsot);
    LOG(INFO) << "After Insert Snapshot: " <<  ins_snaphsot.ToString();
    if (i > 0) {
      ASSERT_OK(this->UpdateTestRow(&writer, i - 1, i));
      DVLOG(1) << "Mutated row=" << i - 1 << ", row_idx=" << i - 1 << ", val=" << i;
      MvccSnapshot mut_snaphsot(*this->tablet()->mvcc_manager());
      snaps.push_back(mut_snaphsot);
      DVLOG(1) << "After Mutate Snapshot: " <<  mut_snaphsot.ToString();
    }
  }

  // Collect the expected rows from the MRS, where there are no
  // undos
  vector<vector<string>* > expected_rows;
  CollectRowsForSnapshots(this->tablet().get(), this->client_schema_,
                          snaps, &expected_rows);

  // Flush the tablet
  ASSERT_OK(this->tablet()->Flush());

  // Now verify that with undos we get the same thing.
  VerifySnapshotsHaveSameResult(this->tablet().get(), this->client_schema_,
                                snaps, expected_rows);

  // Do some more work and flush/compact
  // take a snapshot and mutate the rows so that we have undos and
  // redos
  snaps.push_back(MvccSnapshot(*this->tablet()->mvcc_manager()));
//
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(this->UpdateTestRow(&writer, i, i + 10));
    DVLOG(1) << "Mutated row=" << i << ", row_idx=" << i << ", val=" << i + 10;
    MvccSnapshot mut_snaphsot(*this->tablet()->mvcc_manager());
    snaps.push_back(mut_snaphsot);
    DVLOG(1) << "After Mutate Snapshot: " <<  mut_snaphsot.ToString();
  }

  // also throw a delete in there.
  ASSERT_OK(this->DeleteTestRow(&writer, 4));
  MvccSnapshot delete_snaphsot(*this->tablet()->mvcc_manager());
  snaps.push_back(delete_snaphsot);
  DVLOG(1) << "After Delete Snapshot: " <<  delete_snaphsot.ToString();

  // Collect the expected rows now that we have undos and redos
  STLDeleteElements(&expected_rows);
  CollectRowsForSnapshots(this->tablet().get(), this->client_schema_,
                          snaps, &expected_rows);

  // now flush and the compact everything
  ASSERT_OK(this->tablet()->Flush());
  ASSERT_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  NO_FATALS(this->CheckLiveRowsCount(4));

  // Now verify that with undos and redos we get the same thing.
  VerifySnapshotsHaveSameResult(this->tablet().get(), this->client_schema_,
                                snaps, expected_rows);

  STLDeleteElements(&expected_rows);
}

// This tests KUDU-165, a regression where multiple old ghost rows were appearing in
// compaction outputs and sometimes would be selected as the most recent version
// of the row.
// In particular this makes sure that when there is a ghost row in one row set
// and a live one on another the live one is the only one that survives compaction.
TYPED_TEST(TestTablet, TestGhostRowsOnDiskRowSets) {
  // Create a few INSERT/DELETE pairs on-disk by writing and flushing.
  // Each of the resulting rowsets has a single row which is a "ghost" since its
  // redo data has the DELETE.
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);

  for (int i = 0; i < 3; i++) {
    CHECK_OK(this->InsertTestRow(&writer, 0, 0));
    this->DeleteTestRow(&writer, 0);
    ASSERT_OK(this->tablet()->Flush());
  }

  // Create one more rowset on disk which has just an INSERT (ie a non-ghost row).
  CHECK_OK(this->InsertTestRow(&writer, 0, 0));
  ASSERT_OK(this->tablet()->Flush());

  // Compact. This should result in a rowset with just one row in it.
  ASSERT_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Should still be able to update, since the row is live.
  ASSERT_OK(this->UpdateTestRow(&writer, 0, 1));

  NO_FATALS(this->CheckLiveRowsCount(1));
}

// Test that inserting a row which already exists causes an AlreadyPresent
// error
TYPED_TEST(TestTablet, TestInsertDuplicateKey) {
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);

  CHECK_OK(this->InsertTestRow(&writer, 12345, 0));
  ASSERT_FALSE(writer.last_op_result().has_failed_status());

  // Insert again, should fail!
  Status s = this->InsertTestRow(&writer, 12345, 0);
  ASSERT_STR_CONTAINS(s.ToString(), "key already present");

  ASSERT_EQ(1, this->TabletCount());
  NO_FATALS(this->CheckLiveRowsCount(1));

  // Flush, and make sure that inserting duplicate still fails
  ASSERT_OK(this->tablet()->Flush());

  ASSERT_EQ(1, this->TabletCount());
  NO_FATALS(this->CheckLiveRowsCount(1));

  s = this->InsertTestRow(&writer, 12345, 0);
  ASSERT_STR_CONTAINS(s.ToString(), "key already present");
  ASSERT_EQ(1, this->TabletCount());
  NO_FATALS(this->CheckLiveRowsCount(1));
}

// Tests that we are able to handle reinserts properly.
//
// Namely tests that:
// - We're able to perform multiple reinserts in a MRS, flush them
//   and that all versions of the row are still visible.
// - After we've flushed the reinserts above, we can perform a
//   new reinsert in a new MRS, flush that MRS and compact the row
//   DRS together, all while preserving the full row history.
TYPED_TEST(TestTablet, TestReinserts) {
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);

  vector<MvccSnapshot> snaps;
  // In the first snap there's no row.
  snaps.push_back(MvccSnapshot(*this->tablet()->mvcc_manager()));

  // Insert one row.
  ASSERT_OK(this->InsertTestRow(&writer, 1, 0));

  // In the second snap the row exists and has value 0.
  snaps.push_back(MvccSnapshot(*this->tablet()->mvcc_manager()));

  // Now delete the test row.
  ASSERT_OK(this->DeleteTestRow(&writer, 1));

  // In the third snap the row doesn't exist.
  snaps.push_back(MvccSnapshot(*this->tablet()->mvcc_manager()));

  // Reinsert the row.
  ASSERT_OK(this->InsertTestRow(&writer, 1, 1));

  // In the fourth snap the row exists again and has value 1.
  snaps.push_back(MvccSnapshot(*this->tablet()->mvcc_manager()));

  // .. and delete the row again.
  ASSERT_OK(this->DeleteTestRow(&writer, 1));

  // In the fifth snap the row has been deleted.
  snaps.push_back(MvccSnapshot(*this->tablet()->mvcc_manager()));

  // Now flush the MRS all versions of the tablet should be visible,
  // depending on the chosen snapshot.
  ASSERT_OK(this->tablet()->Flush());

  vector<vector<string>* > expected_rows;
  CollectRowsForSnapshots(this->tablet().get(), this->client_schema_,
                          snaps, &expected_rows);

  ASSERT_EQ(expected_rows.size(), 5);
  ASSERT_EQ(expected_rows[0]->size(), 0) << "Got the wrong result from snap: "
                                         << snaps[0].ToString();
  ASSERT_EQ(expected_rows[1]->size(), 1) << "Got the wrong result from snap: "
                                         << snaps[1].ToString();
  ASSERT_STR_CONTAINS((*expected_rows[1])[0], "int32 key_idx=1, int32 val=0)");
  ASSERT_EQ(expected_rows[2]->size(), 0) << "Got the wrong result from snap: "
                                         << snaps[2].ToString();
  ASSERT_EQ(expected_rows[3]->size(), 1) << "Got the wrong result from snap: "
                                         << snaps[3].ToString();
  ASSERT_STR_CONTAINS((*expected_rows[3])[0], "int32 key_idx=1, int32 val=1)");
  ASSERT_EQ(expected_rows[4]->size(), 0) << "Got the wrong result from snap: "
                                         << snaps[4].ToString();
  NO_FATALS(this->CheckLiveRowsCount(0));
  STLDeleteElements(&expected_rows);
}

// Test flushes and compactions dealing with deleted rows.
TYPED_TEST(TestTablet, TestDeleteWithFlushAndCompact) {
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  CHECK_OK(this->InsertTestRow(&writer, 0, 0));
  ASSERT_OK(this->DeleteTestRow(&writer, 0));
  ASSERT_EQ(0L, writer.last_op_result().mutated_stores(0).mrs_id());

  // The row is deleted, so we shouldn't see it in the iterator.
  vector<string> rows;
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // Flush the tablet and make sure the data doesn't re-appear.
  ASSERT_OK(this->tablet()->Flush());
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // Re-inserting should succeed. This will reinsert into the MemRowSet.
  // Set the int column to '1' this time, so we can differentiate the two
  // versions of the row.
  CHECK_OK(this->InsertTestRow(&writer, 0, 1));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 1, false), rows[0]);

  // Flush again, so the DiskRowSet has the row.
  ASSERT_OK(this->tablet()->Flush());
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 1, false), rows[0]);

  // Delete it again, now that it's in DRS.
  ASSERT_OK(this->DeleteTestRow(&writer, 0));
  ASSERT_EQ(1, writer.last_op_result().mutated_stores_size());
  ASSERT_EQ(1L, writer.last_op_result().mutated_stores(0).rs_id());
  ASSERT_EQ(0L, writer.last_op_result().mutated_stores(0).dms_id());
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());
  NO_FATALS(this->CheckLiveRowsCount(0));

  // We now have an INSERT in the MemRowSet and the
  // deleted row in the DiskRowSet. The new version
  // of the row has '2' in the int column.
  CHECK_OK(this->InsertTestRow(&writer, 0, 2));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 2, false), rows[0]);

  // Flush - now we have the row in two different DRSs.
  ASSERT_OK(this->tablet()->Flush());
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 2, false), rows[0]);

  // Compaction should succeed even with the duplicate rows.
  ASSERT_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 2, false), rows[0]);
  NO_FATALS(this->CheckLiveRowsCount(1));
}

// Test flushes dealing with REINSERT mutations in the MemRowSet.
TYPED_TEST(TestTablet, TestFlushWithReinsert) {
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  // Insert, delete, and re-insert a row in the MRS.

  CHECK_OK(this->InsertTestRow(&writer, 0, 0));
  ASSERT_OK(this->DeleteTestRow(&writer, 0));
  ASSERT_EQ(1, writer.last_op_result().mutated_stores_size());
  ASSERT_EQ(0L, writer.last_op_result().mutated_stores(0).mrs_id());
  CHECK_OK(this->InsertTestRow(&writer, 0, 1));

  // Flush the tablet and make sure the data persists.
  ASSERT_OK(this->tablet()->Flush());
  vector<string> rows;
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 1, false), rows[0]);
}

// Test flushes dealing with REINSERT mutations if they arrive in the middle
// of a flush.
TYPED_TEST(TestTablet, TestReinsertDuringFlush) {
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  // Insert/delete/insert/delete in MemRowStore.

  CHECK_OK(this->InsertTestRow(&writer, 0, 0));
  ASSERT_OK(this->DeleteTestRow(&writer, 0));
  ASSERT_EQ(1, writer.last_op_result().mutated_stores_size());
  ASSERT_EQ(0L, writer.last_op_result().mutated_stores(0).mrs_id());

  CHECK_OK(this->InsertTestRow(&writer, 0, 1));
  ASSERT_OK(this->DeleteTestRow(&writer, 0));

  ASSERT_EQ(1, writer.last_op_result().mutated_stores_size());
  ASSERT_EQ(0L, writer.last_op_result().mutated_stores(0).mrs_id());

  // During the snapshot flush, insert/delete/insert some more during the flush.
  class MyCommonHooks : public Tablet::FlushCompactCommonHooks {
   public:
    explicit MyCommonHooks(TestFixture *test) : test_(test) {}

    Status PostWriteSnapshot() OVERRIDE {
      LocalTabletWriter writer(test_->tablet().get(), &test_->client_schema());
      test_->InsertTestRow(&writer, 0, 1);
      CHECK_OK(test_->DeleteTestRow(&writer, 0));
      CHECK_EQ(1, writer.last_op_result().mutated_stores_size());
      CHECK_EQ(1L, writer.last_op_result().mutated_stores(0).mrs_id());
      test_->InsertTestRow(&writer, 0, 2);
      CHECK_OK(test_->DeleteTestRow(&writer, 0));
      CHECK_EQ(1, writer.last_op_result().mutated_stores_size());
      CHECK_EQ(1L, writer.last_op_result().mutated_stores(0).mrs_id());
      test_->InsertTestRow(&writer, 0, 3);
      return Status::OK();
    }

   private:
    TestFixture *test_;
  };
  shared_ptr<Tablet::FlushCompactCommonHooks> common_hooks(
    reinterpret_cast<Tablet::FlushCompactCommonHooks *>(new MyCommonHooks(this)));
  this->tablet()->SetFlushCompactCommonHooksForTests(common_hooks);

  // Flush the tablet and make sure the data persists.
  ASSERT_OK(this->tablet()->Flush());
  vector<string> rows;
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 3, false), rows[0]);
}

// Test iterating over a tablet which contains data
// in the memrowset as well as two rowsets. This simple test
// only puts one row in each with no updates.
TYPED_TEST(TestTablet, TestRowIteratorSimple) {
  const int kInRowSet1 = 1;
  const int kInRowSet2 = 2;
  const int kInMemRowSet = 3;

  // Put a row in disk rowset 1 (insert and flush)
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  CHECK_OK(this->InsertTestRow(&writer, kInRowSet1, 0));
  ASSERT_OK(this->tablet()->Flush());

  // Put a row in disk rowset 2 (insert and flush)
  CHECK_OK(this->InsertTestRow(&writer, kInRowSet2, 0));
  ASSERT_OK(this->tablet()->Flush());

  // Put a row in memrowset
  CHECK_OK(this->InsertTestRow(&writer, kInMemRowSet, 0));

  // Now iterate the tablet and make sure the rows show up
  unique_ptr<RowwiseIterator> iter;
  ASSERT_OK(this->tablet()->NewRowIterator(this->client_schema_ptr_, &iter));
  ASSERT_OK(iter->Init(nullptr));

  ASSERT_TRUE(iter->HasNext());

  RowBlockMemory mem;
  RowBlock block(&this->schema_, 100, &mem);

  // First call to CopyNextRows should fetch the whole memrowset.
  ASSERT_OK_FAST(iter->NextBlock(&block));
  ASSERT_EQ(1, block.nrows()) << "should get only the one row from memrowset";
  this->VerifyRow(block.row(0), kInMemRowSet, 0);

  // Next, should fetch the older rowset
  ASSERT_TRUE(iter->HasNext());
  ASSERT_OK(iter->NextBlock(&block));
  ASSERT_EQ(1, block.nrows()) << "should get only the one row from rowset 1";
  this->VerifyRow(block.row(0), kInRowSet1, 0);

  // Next, should fetch the newer rowset
  ASSERT_TRUE(iter->HasNext());
  ASSERT_OK(iter->NextBlock(&block));
  ASSERT_EQ(1, block.nrows()) << "should get only the one row from rowset 2";
  this->VerifyRow(block.row(0), kInRowSet2, 0);

  ASSERT_FALSE(iter->HasNext());
}

// Hook implementation which runs a lambda function during the 'duplicating'
// phase of compaction.
template<class HookFunc>
class RunDuringDuplicatingRowSetPhase : public Tablet::FlushCompactCommonHooks {
 public:
  explicit RunDuringDuplicatingRowSetPhase(HookFunc hook)
      : hook_(std::move(hook)) {}

  Status PostSwapInDuplicatingRowSet() override {
    hook_();
    return Status::OK();
  }
 private:
  const HookFunc hook_;
};

TYPED_TEST(TestTablet, TestRowIteratorOrdered) {
  // Create interleaved keys in each rowset, so they are clearly not in order
  const int kNumRows = 128;
  const int kNumBatches = 4;
  LOG(INFO) << "Schema: " << this->schema_.ToString();
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  for (int i = 0; i < kNumBatches; i++) {
    ASSERT_OK(this->tablet()->Flush());
    for (int j = 0; j < kNumRows; j++) {
      if (j % kNumBatches == i) {
        LOG(INFO) << "Inserting row " << j;
        CHECK_OK(this->InsertTestRow(&writer, 654321+j, j));
      }
    }
  }

  // We'll test ordered scans a few times, covering before, during, and
  // after a compaction.
  auto RunScans = [this, kNumRows]() {
    MvccSnapshot snap(*this->tablet()->mvcc_manager());
    // Iterate through with a few different block sizes.
    for (int numBlocks = 1; numBlocks < 5; numBlocks*=2) {
      const int rowsPerBlock = kNumRows / numBlocks;
      // Make a new ordered iterator for the current snapshot.
      RowIteratorOptions opts;
      opts.projection = this->client_schema_ptr_;
      opts.snap_to_include = snap;
      opts.order = ORDERED;
      unique_ptr<RowwiseIterator> iter;
      ASSERT_OK(this->tablet()->NewRowIterator(std::move(opts), &iter));
      ASSERT_OK(iter->Init(nullptr));

      // Iterate the tablet collecting rows.
      vector<shared_ptr<faststring> > rows;
      RowBlockMemory mem;
      for (int i = 0; i < numBlocks; i++) {
        mem.Reset();
        RowBlock block(&this->schema_, rowsPerBlock, &mem);
        ASSERT_TRUE(iter->HasNext());
        ASSERT_OK(iter->NextBlock(&block));
        ASSERT_EQ(rowsPerBlock, block.nrows()) << "unexpected number of rows returned";
        for (int j = 0; j < rowsPerBlock; j++) {
          RowBlockRow row = block.row(j);
          auto encoded(make_shared<faststring>());
          this->client_schema_.EncodeComparableKey(row, encoded.get());
          rows.push_back(std::move(encoded));
        }
      }
      // Verify the collected rows, checking that they are sorted.
      for (int j = 1; j < rows.size(); j++) {
        // Use the schema for comparison, since this test is run with different schemas.
        ASSERT_LT((*rows[j-1]).ToString(), (*rows[j]).ToString());
      }
      ASSERT_FALSE(iter->HasNext());
      ASSERT_EQ(kNumRows, rows.size());
    }
  };

  {
    SCOPED_TRACE("With no compaction");
    NO_FATALS(RunScans());
  }

  {
    SCOPED_TRACE("With duplicating rowset");
    shared_ptr<Tablet::FlushCompactCommonHooks> hooks_shared(
        new RunDuringDuplicatingRowSetPhase<decltype(RunScans)>(RunScans));
    this->tablet()->SetFlushCompactCommonHooksForTests(hooks_shared);
    ASSERT_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
    NO_PENDING_FATALS();
  }

  {
    SCOPED_TRACE("After compaction");
    NO_FATALS(RunScans());
  }
}

template<class SETUP>
bool TestSetupExpectsNulls(int32_t /*key_idx*/) {
  return false;
}

template<>
bool TestSetupExpectsNulls<NullableValueTestSetup>(int32_t key_idx) {
  // If it's a row that the test updates, then we should expect null
  // based on whether it updated to NULL or away from NULL.
  bool should_update = (key_idx % 2 == 1);
  if (should_update) {
    return (key_idx % 10 == 1);
  }

  // Otherwise, expect whatever was inserted.
  return NullableValueTestSetup::ShouldInsertAsNull(key_idx);
}

// Test iterating over a tablet which has a memrowset
// and several rowsets, each with many rows of data.
TYPED_TEST(TestTablet, TestRowIteratorComplex) {

  uint64_t max_rows = this->ClampRowCount(FLAGS_testiterator_num_inserts);

  // Put a row in disk rowset 1 (insert and flush)
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  for (int32_t i = 0; i < max_rows; i++) {
    ASSERT_OK_FAST(this->InsertTestRow(&writer, i, 0));

    if (i % 300 == 0) {
      LOG(INFO) << "Flushing after " << i << " rows inserted";
      ASSERT_OK(this->tablet()->Flush());
    }
  }
  LOG(INFO) << "Successfully inserted " << max_rows << " rows";

  // At this point, we should have several rowsets as well
  // as some data in memrowset.

  // Update a subset of the rows
  for (int32_t i = 0; i < max_rows; i++) {
    bool should_update = (i % 2 == 1);
    if (!should_update) continue;

    bool set_to_null = TestSetupExpectsNulls<TypeParam>(i);
    if (set_to_null) {
      this->UpdateTestRowToNull(&writer, i);
    } else {
      ASSERT_OK_FAST(this->UpdateTestRow(&writer, i, i));
    }
  }

  // Now iterate the tablet and make sure the rows show up.
  unique_ptr<RowwiseIterator> iter;
  const Schema& schema = this->client_schema_;
  ASSERT_OK(this->tablet()->NewRowIterator(this->client_schema_ptr_, &iter));
  ASSERT_OK(iter->Init(nullptr));
  LOG(INFO) << "Created iter: " << iter->ToString();

  vector<bool> seen(max_rows, false);
  int seen_count = 0;

  RowBlockMemory mem;
  RowBlock block(&schema, 100, &mem);
  while (iter->HasNext()) {
    mem.Reset();
    ASSERT_OK(iter->NextBlock(&block));
    LOG(INFO) << "Fetched batch of " << block.nrows();
    for (size_t i = 0; i < block.nrows(); i++) {
      SCOPED_TRACE(schema.DebugRow(block.row(i)));
      // Verify that we see each key exactly once.
      int32_t key_idx = *schema.ExtractColumnFromRow<INT32>(block.row(i), 1);
      if (seen[key_idx]) {
        FAIL() << "Saw row " << key_idx << " multiple times";
      }
      seen[key_idx] = true;
      seen_count++;

      // Verify that we see the correctly updated value
      const int32_t* val = schema.ExtractColumnFromRow<INT32>(block.row(i), 2);

      bool set_to_null = TestSetupExpectsNulls<TypeParam>(key_idx);
      bool should_update = (key_idx % 2 == 1);
      if (val == nullptr) {
        ASSERT_TRUE(set_to_null);
      } else if (should_update) {
        ASSERT_EQ(key_idx, *val);
      } else {
        ASSERT_EQ(0, *val);
      }
    }
  }

  ASSERT_EQ(seen_count, max_rows)
    << "expected to see all inserted data through iterator.";
}

// Test that, when a tablet has flushed data and is
// reopened, that the data persists
TYPED_TEST(TestTablet, TestInsertsPersist) {
  uint64_t max_rows = this->ClampRowCount(FLAGS_testiterator_num_inserts);

  this->InsertTestRows(0, max_rows, 0);
  ASSERT_EQ(max_rows, this->TabletCount());
  NO_FATALS(this->CheckLiveRowsCount(max_rows));

  // Get current timestamp.
  Timestamp t = this->tablet()->clock()->Now();

  // Flush it.
  ASSERT_OK(this->tablet()->Flush());

  ASSERT_EQ(max_rows, this->TabletCount());
  NO_FATALS(this->CheckLiveRowsCount(max_rows));

  // Close and re-open tablet.
  // TODO(unknown): Should we be reopening the tablet in a different way to persist the
  // clock / timestamps?
  this->TabletReOpen();

  // Ensure that rows exist
  ASSERT_EQ(max_rows, this->TabletCount());
  NO_FATALS(this->CheckLiveRowsCount(max_rows));
  this->VerifyTestRowsWithTimestampAndVerifier(0, max_rows, t, boost::none);

  // TODO(unknown): add some more data, re-flush
}

TYPED_TEST(TestTablet, TestInsertIgnore) {
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  KuduPartialRow row(&this->client_schema_);
  vector<string> rows;

  // Single batch, insert then insert ingore of same row, operation should succeed
  this->setup_.BuildRow(&row, 0, 1000);
  vector<LocalTabletWriter::RowOp> ops;
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::INSERT, &row));
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::INSERT_IGNORE, &row));
  ASSERT_OK(writer.WriteBatch(ops));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1000, false) }, rows);

  ASSERT_OK(this->DeleteTestRow(&writer, 0));

  ops.clear();
  this->setup_.BuildRow(&row, 0, 1001);
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::INSERT_IGNORE, &row));
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::INSERT, &row));
  Status s = writer.WriteBatch(ops);
  ASSERT_STR_CONTAINS(s.ToString(), "key already present");
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1001, false) }, rows);

  // INSERT IGNORE a row that is in MRS, ensure value doesn't change
  this->InsertIgnoreTestRows(0, 1, 2000);
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1001, false) }, rows);

  this->UpsertTestRows(0, 1, 1011);

  // Flush it.
  ASSERT_OK(this->tablet()->Flush());
  ASSERT_OK(this->IterateToStringList(&rows));
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1011, false) }, rows);

  // INSERT IGNORE a row that is in DRS, ensure value doesn't change.
  this->InsertIgnoreTestRows(0, 1, 1000);
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1011, false) }, rows);
}

TYPED_TEST(TestTablet, TestUpdateIgnore) {
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  KuduPartialRow row(&this->client_schema_);
  vector<string> rows;

  // update ignore a missing row, operation should succeed.
  this->setup_.BuildRow(&row, 0, 1000);
  vector<LocalTabletWriter::RowOp> ops;
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::UPDATE_IGNORE, &row));
  ASSERT_OK(writer.WriteBatch(ops));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // insert a row that can be updated.
  ops.clear();
  this->setup_.BuildRow(&row, 0, 1000);
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::INSERT, &row));
  ASSERT_OK(writer.WriteBatch(ops));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1000, false) }, rows);

  // update ignore an existing row, implements normal update.
  ops.clear();
  this->setup_.BuildRow(&row, 0, 1011);
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::UPDATE_IGNORE, &row));
  ASSERT_OK(writer.WriteBatch(ops));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1011, false) }, rows);
}

TYPED_TEST(TestTablet, TestDeleteIgnore) {
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  KuduPartialRow row(&this->client_schema_);
  vector<string> rows;

  // delete ignore a missing row, operation should succeed.
  this->setup_.BuildRowKey(&row, 0);
  vector<LocalTabletWriter::RowOp> ops;
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::DELETE_IGNORE, &row));
  ASSERT_OK(writer.WriteBatch(ops));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // insert a row that can be deleted.
  ops.clear();
  this->setup_.BuildRow(&row, 0, 1000);
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::INSERT, &row));
  ASSERT_OK(writer.WriteBatch(ops));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1000, false) }, rows);

  // delete ignore an existing row, implements normal delete.
  ops.clear();
  KuduPartialRow delete_row(&this->client_schema_);
  this->setup_.BuildRowKey(&delete_row, 0);
  ops.emplace_back(LocalTabletWriter::RowOp(RowOperationsPB::DELETE_IGNORE, &delete_row));
  ASSERT_OK(writer.WriteBatch(ops));
  ASSERT_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());
}

TYPED_TEST(TestTablet, TestUpsert) {
  vector<string> rows;
  const auto& upserts_as_updates = this->tablet()->metrics()->upserts_as_updates;

  // Upsert a new row.
  this->UpsertTestRows(0, 1, 1000);
  EXPECT_EQ(0, upserts_as_updates->value());

  // Upsert a row that is in the MRS.
  this->UpsertTestRows(0, 1, 1001);
  EXPECT_EQ(1, upserts_as_updates->value());

  ASSERT_OK(this->IterateToStringList(&rows));
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1001, false) }, rows);

  // Flush it.
  ASSERT_OK(this->tablet()->Flush());
  ASSERT_OK(this->IterateToStringList(&rows));
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1001, false) }, rows);

  // Upsert a row that is in the DRS.
  this->UpsertTestRows(0, 1, 1002);
  EXPECT_EQ(2, upserts_as_updates->value());
  ASSERT_OK(this->IterateToStringList(&rows));
  EXPECT_EQ(vector<string>{ this->setup_.FormatDebugRow(0, 1002, false) }, rows);
  NO_FATALS(this->CheckLiveRowsCount(1));
}

// Test that when a row has been updated many times, it always yields
// the most recent value.
TYPED_TEST(TestTablet, TestMultipleUpdates) {
  // Insert and update several times in MemRowSet
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema_);
  CHECK_OK(this->InsertTestRow(&writer, 0, 0));
  ASSERT_OK(this->UpdateTestRow(&writer, 0, 1));
  ASSERT_EQ(1, writer.last_op_result().mutated_stores_size());
  ASSERT_EQ(0L, writer.last_op_result().mutated_stores(0).mrs_id());
  ASSERT_OK(this->UpdateTestRow(&writer, 0, 2));
  ASSERT_OK(this->UpdateTestRow(&writer, 0, 3));

  // Should see most recent value.
  vector<string> out_rows;
  ASSERT_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(1, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 3, false), out_rows[0]);

  // Flush it.
  ASSERT_OK(this->tablet()->Flush());

  // Should still see most recent value.
  ASSERT_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(1, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 3, false), out_rows[0]);

  // Update the row a few times in DeltaMemStore
  ASSERT_OK(this->UpdateTestRow(&writer, 0, 4));
  ASSERT_EQ(1, writer.last_op_result().mutated_stores_size());
  ASSERT_EQ(0L, writer.last_op_result().mutated_stores(0).rs_id());
  ASSERT_EQ(0L, writer.last_op_result().mutated_stores(0).dms_id());
  ASSERT_OK(this->UpdateTestRow(&writer, 0, 5));
  ASSERT_OK(this->UpdateTestRow(&writer, 0, 6));

  // Should still see most recent value.
  ASSERT_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(1, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 6, false), out_rows[0]);


  // Force a compaction after adding a new rowset with one row.
  CHECK_OK(this->InsertTestRow(&writer, 1, 0));
  ASSERT_OK(this->tablet()->Flush());
  ASSERT_EQ(2, this->tablet()->num_rowsets());

  ASSERT_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  ASSERT_EQ(1, this->tablet()->num_rowsets());

  // Should still see most recent value.
  ASSERT_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(2, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 6, false), out_rows[0]);
  ASSERT_EQ(this->setup_.FormatDebugRow(1, 0, false), out_rows[1]);
}

TYPED_TEST(TestTablet, TestCompaction) {
  uint64_t max_rows = this->ClampRowCount(FLAGS_testcompaction_num_rows);

  uint64_t n_rows = max_rows / 3;
  // Create three rowsets by inserting and flushing
  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(0, n_rows, 0);

    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_OK(this->tablet()->Flush());
    }

    // first MemRowSet had id 0, current one should be 1
    ASSERT_EQ(1, this->tablet()->CurrentMrsIdForTests());
    ASSERT_TRUE(
      this->tablet()->metadata()->GetRowSetForTests(0)->HasDataForColumnIdForTests(
          this->schema_.column_id(0)));
  }

  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(n_rows, n_rows, 0);

    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_OK(this->tablet()->Flush());
    }

    // previous MemRowSet had id 1, current one should be 2
    ASSERT_EQ(2, this->tablet()->CurrentMrsIdForTests());
    ASSERT_TRUE(
      this->tablet()->metadata()->GetRowSetForTests(1)->HasDataForColumnIdForTests(
          this->schema_.column_id(0)));
  }

  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(n_rows * 2, n_rows, 0);

    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_OK(this->tablet()->Flush());
    }

    // previous MemRowSet had id 2, current one should be 3
    ASSERT_EQ(3, this->tablet()->CurrentMrsIdForTests());
    ASSERT_TRUE(
      this->tablet()->metadata()->GetRowSetForTests(2)->HasDataForColumnIdForTests(
          this->schema_.column_id(0)));
  }

  // Issue compaction
  LOG_TIMING(INFO, "Compacting rows") {

    ASSERT_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
    // Compaction does not swap the memrowsets so we should still get 3
    ASSERT_EQ(3, this->tablet()->CurrentMrsIdForTests());
    ASSERT_EQ(n_rows * 3, this->TabletCount());
    NO_FATALS(this->CheckLiveRowsCount(n_rows * 3));

    const RowSetMetadata *rowset_meta = this->tablet()->metadata()->GetRowSetForTests(3);
    ASSERT_TRUE(rowset_meta != nullptr);
    ASSERT_TRUE(rowset_meta->HasDataForColumnIdForTests(this->schema_.column_id(0)));
    ASSERT_TRUE(rowset_meta->HasBloomDataBlockForTests());
  }

  // Old rowsets should not exist anymore
  for (int i = 0; i <= 2; i++) {
    const RowSetMetadata *rowset_meta = this->tablet()->metadata()->GetRowSetForTests(i);
    ASSERT_TRUE(rowset_meta == nullptr);
  }
}

TYPED_TEST(TestTablet, TestCountLiveRowsAfterShutdown) {
  // Insert 1000 rows into memrowset
  uint64_t max_rows = this->ClampRowCount(FLAGS_testflush_num_inserts);
  this->InsertTestRows(0, max_rows, 0);
  ASSERT_OK(this->tablet()->Flush());
  NO_FATALS(this->CheckLiveRowsCount(max_rows));

  // Save the tablet's reference.
  shared_ptr<Tablet> tablet = this->tablet();

  // Shutdown the tablet.
  NO_FATALS(this->tablet()->Shutdown());

  // Call the CountLiveRows().
  uint64_t count = 0;
  ASSERT_TRUE(tablet->CountLiveRows(&count).IsRuntimeError());
}

enum MutationType {
  MRS_MUTATION,
  DELTA_MUTATION,
  DUPLICATED_MUTATION
};

// Hook used by the Test*WithConcurrentMutation tests.
//
// Every time one of these hooks triggers, it inserts a row starting
// at row 20 (and increasing), and updates a row starting at row 10
// (and increasing).
template<class TestFixture>
class MyCommonHooks : public Tablet::FlushCompactCommonHooks {
 public:
  explicit MyCommonHooks(TestFixture *test, bool flushed)
      : test_(test),
        flushed_(flushed),
        i_(0) {
  }
  Status DoHook(MutationType expected_mutation_type) {
    LocalTabletWriter writer(test_->tablet().get(), &test_->client_schema());
    RETURN_NOT_OK(test_->DeleteTestRow(&writer, i_));

    switch (expected_mutation_type) {
      case MRS_MUTATION:
        CHECK_EQ(1, writer.last_op_result().mutated_stores_size());
        CHECK(writer.last_op_result().mutated_stores(0).has_mrs_id());
        break;
      case DELTA_MUTATION:
        CHECK_EQ(1, writer.last_op_result().mutated_stores_size());
        CHECK(writer.last_op_result().mutated_stores(0).has_rs_id());
        CHECK(writer.last_op_result().mutated_stores(0).has_dms_id());
        break;
      case DUPLICATED_MUTATION:
        CHECK_EQ(2, writer.last_op_result().mutated_stores_size());
        break;
    }
    RETURN_NOT_OK(test_->UpdateTestRow(&writer, 10 + i_, 1000 + i_));
    test_->InsertTestRows(20 + i_, 1, 0);
    test_->CheckCanIterate();
    i_++;
    return Status::OK();
  }

  virtual Status PostTakeMvccSnapshot() OVERRIDE {
    // before we flush we update the MemRowSet afterwards we update the
    // DeltaMemStore
    if (!flushed_) {
      return DoHook(MRS_MUTATION);
    } else {
      return DoHook(DELTA_MUTATION);
    }
  }
  virtual Status PostWriteSnapshot() OVERRIDE {
    if (!flushed_) {
      return DoHook(MRS_MUTATION);
    } else {
      return DoHook(DELTA_MUTATION);
    }
  }
  virtual Status PostSwapInDuplicatingRowSet() OVERRIDE {
    return DoHook(DUPLICATED_MUTATION);
  }
  virtual Status PostReupdateMissedDeltas() OVERRIDE {
    return DoHook(DUPLICATED_MUTATION);
  }
  virtual Status PostSwapNewRowSet() OVERRIDE {
    return DoHook(DELTA_MUTATION);
  }
 protected:
  TestFixture *test_;
  bool flushed_;
  int i_;
};

template<class TestFixture>
class MyFlushHooks : public Tablet::FlushFaultHooks, public MyCommonHooks<TestFixture> {
 public:
  explicit MyFlushHooks(TestFixture *test, bool flushed) :
           MyCommonHooks<TestFixture>(test, flushed) {}
  virtual Status PostSwapNewMemRowSet() { return this->DoHook(MRS_MUTATION); }
};

template<class TestFixture>
class MyCompactHooks : public Tablet::CompactionFaultHooks, public MyCommonHooks<TestFixture> {
 public:
  explicit MyCompactHooks(TestFixture *test, bool flushed) :
           MyCommonHooks<TestFixture>(test, flushed) {}
  Status PostSelectIterators() { return this->DoHook(DELTA_MUTATION); }
};

// Test for Flush with concurrent update, delete and insert during the
// various phases.
TYPED_TEST(TestTablet, TestFlushWithConcurrentMutation) {
  this->InsertTestRows(0, 7, 0); // 0-6 inclusive: these rows will be deleted
  this->InsertTestRows(10, 7, 0); // 10-16 inclusive: these rows will be updated
  // Rows 20-26 inclusive will be inserted during the flush

  // Inject hooks which mutate those rows and add more rows at
  // each key stage of flushing.
  auto hooks(make_shared<MyFlushHooks<TestFixture>>(this, false));
  this->tablet()->SetFlushHooksForTests(hooks);
  this->tablet()->SetFlushCompactCommonHooksForTests(hooks);

  // First hook before we do the Flush
  ASSERT_OK(hooks->DoHook(MRS_MUTATION));

  // Then do the flush with the hooks enabled.
  ASSERT_OK(this->tablet()->Flush());

  // Now verify that the results saw all the mutated_stores.
  vector<string> out_rows;
  ASSERT_OK(this->IterateToStringList(&out_rows));
  std::sort(out_rows.begin(), out_rows.end());

  vector<string> expected_rows;
  expected_rows.push_back(this->setup_.FormatDebugRow(10, 1000, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(11, 1001, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(12, 1002, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(13, 1003, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(14, 1004, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(15, 1005, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(16, 1006, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(20, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(21, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(22, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(23, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(24, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(25, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(26, 0, false));

  std::sort(expected_rows.begin(), expected_rows.end());

  // Verify that all the inserts and updates arrived and persisted.
  LOG(INFO) << "Expected: " << JoinStrings(expected_rows, "\n");

  // Verify that all the inserts and updates arrived and persisted.
  LOG(INFO) << "Results: " << JoinStrings(out_rows, "\n");

  ASSERT_EQ(expected_rows.size(), out_rows.size());
  vector<string>::const_iterator exp_it = expected_rows.begin();
  for (vector<string>::const_iterator out_it = out_rows.begin(); out_it!= out_rows.end();) {
    ASSERT_EQ(*out_it, *exp_it);
    out_it++;
    exp_it++;
  }
}

// Test for compaction with concurrent update and insert during the
// various phases.
TYPED_TEST(TestTablet, TestCompactionWithConcurrentMutation) {
  // Create three rowsets by inserting and flushing.
  // The rows from these layers will get updated or deleted during the flush:
  // - rows 0-6 inclusive will be deleted
  // - rows 10-16 inclusive will be updated

  this->InsertTestRows(0, 2, 0);  // rows 0-1
  this->InsertTestRows(10, 2, 0); // rows 10-11
  ASSERT_OK(this->tablet()->Flush());

  this->InsertTestRows(2, 2, 0);  // rows 2-3
  this->InsertTestRows(12, 2, 0); // rows 12-13
  ASSERT_OK(this->tablet()->Flush());

  this->InsertTestRows(4, 3, 0);  // rows 4-6
  this->InsertTestRows(14, 3, 0); // rows 14-16
  ASSERT_OK(this->tablet()->Flush());

  // Rows 20-26 inclusive will be inserted during the flush.

  auto hooks(make_shared<MyCompactHooks<TestFixture>>(this, true));
  this->tablet()->SetCompactionHooksForTests(hooks);
  this->tablet()->SetFlushCompactCommonHooksForTests(hooks);

  // First hook pre-compaction.
  ASSERT_OK(hooks->DoHook(DELTA_MUTATION));

  // Issue compaction
  ASSERT_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Grab the resulting data into a vector.
  vector<string> out_rows;
  ASSERT_OK(this->IterateToStringList(&out_rows));
  std::sort(out_rows.begin(), out_rows.end());

  vector<string> expected_rows;
  expected_rows.push_back(this->setup_.FormatDebugRow(10, 1000, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(11, 1001, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(12, 1002, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(13, 1003, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(14, 1004, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(15, 1005, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(16, 1006, true));
  expected_rows.push_back(this->setup_.FormatDebugRow(20, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(21, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(22, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(23, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(24, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(25, 0, false));
  expected_rows.push_back(this->setup_.FormatDebugRow(26, 0, false));

  std::sort(expected_rows.begin(), expected_rows.end());

  ASSERT_EQ(expected_rows.size(), out_rows.size());

  // Verify that all the inserts and updates arrived and persisted.
  LOG(INFO) << "Expected: " << JoinStrings(expected_rows, "\n");

  // Verify that all the inserts and updates arrived and persisted.
  LOG(INFO) << "Results: " << JoinStrings(out_rows, "\n");

  vector<string>::const_iterator exp_it = expected_rows.begin();
  for (vector<string>::const_iterator out_it = out_rows.begin(); out_it!= out_rows.end();) {
    ASSERT_EQ(*out_it, *exp_it);
    out_it++;
    exp_it++;
  }
}

// Test that metrics behave properly during tablet initialization
TYPED_TEST(TestTablet, TestMetricsInit) {
  // Create a tablet, but do not open it
  this->CreateTestTablet();
  MetricRegistry* registry = this->harness()->metrics_registry();
  {
    std::ostringstream out;
    JsonWriter writer(&out, JsonWriter::PRETTY);
    ASSERT_OK(registry->WriteAsJson(&writer, MetricJsonOptions()));
  }
  // Open tablet, should still work
  this->harness()->Open();
  {
    std::ostringstream out;
    JsonWriter writer(&out, JsonWriter::PRETTY);
    ASSERT_OK(registry->WriteAsJson(&writer, MetricJsonOptions()));
  }
}

// Test that we find the correct log segment size for different indexes.
TEST(TestTablet, TestGetReplaySizeForIndex) {
  std::map<int64_t, int64_t> replay_size_map;

  // We build a map that represents 3 logs.
  // See Log::GetReplaySizeMap(...) for details.
  replay_size_map[100] = 45;
  replay_size_map[200] = 25;
  replay_size_map[300] = 10;

  // -1 indicates that no logs are anchored, and thus we it should report
  // no logs need to be replayed.
  int64_t min_log_index = -1;
  EXPECT_EQ(Tablet::GetReplaySizeForIndex(min_log_index, replay_size_map), 0);

  // A value in or before the first segment retains all the logs.
  min_log_index = 1;
  EXPECT_EQ(Tablet::GetReplaySizeForIndex(min_log_index, replay_size_map), 45);

  // A value at the end of the first segment also retains everything.
  min_log_index = 100;
  EXPECT_EQ(Tablet::GetReplaySizeForIndex(min_log_index, replay_size_map), 45);

  // Beginning of second segment, only retain that one and the next.
  min_log_index = 101;
  EXPECT_EQ(Tablet::GetReplaySizeForIndex(min_log_index, replay_size_map), 25);

  // Beginning of third segment, only retain that one.
  min_log_index = 201;
  EXPECT_EQ(Tablet::GetReplaySizeForIndex(min_log_index, replay_size_map), 10);

  // A value after all the passed segments, doesn't retain anything.
  min_log_index = 301;
  EXPECT_EQ(Tablet::GetReplaySizeForIndex(min_log_index, replay_size_map), 0);
}

class TestTabletStringKey : public TestTablet<StringKeyTestSetup> {
public:
  void AssertChunks(vector<KeyRange> expected, vector<KeyRange> actual) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t idx = 0; idx < actual.size(); ++idx) {
      ASSERT_STREQ(actual[idx].start_primary_key().c_str(),
                   expected[idx].start_primary_key().c_str());
      ASSERT_STREQ(actual[idx].stop_primary_key().c_str(),
                   expected[idx].stop_primary_key().c_str());
      ASSERT_EQ(actual[idx].size_bytes(), expected[idx].size_bytes());
    }
  }
};

// Test for split key range
TEST_F(TestTabletStringKey, TestSplitKeyRange) {
  Tablet* tablet = this->mutable_tablet();

  scoped_refptr<TabletComponents> comps;
  tablet->GetComponents(&comps);

  RowSetVector old_rowset = comps->rowsets->all_rowsets();
  RowSetVector new_rowset = {
    make_shared<MockDiskRowSet>("0", "9", 9000, 90),
    make_shared<MockDiskRowSet>("2", "5", 3000, 30),
    make_shared<MockDiskRowSet>("5", "6", 1000, 10)
  };
  tablet->AtomicSwapRowSets(old_rowset, new_rowset);
  {
    vector<KeyRange> result = {
      KeyRange("", "2", 2000),
      KeyRange("2", "5", 6000),
      KeyRange("5", "6", 2000),
      KeyRange("6", "", 3000)
    };
    vector<ColumnId> col_ids;
    vector<KeyRange> range;
    tablet->SplitKeyRange(nullptr, nullptr, col_ids, 3000, &range);
    AssertChunks(result, range);
  }
  // target chunk size less than the min interval size
  {
    vector<KeyRange> result = {
      KeyRange("", "2", 2000),
      KeyRange("2", "5", 6000),
      KeyRange("5", "6", 2000),
      KeyRange("6", "", 3000)
    };
    vector<ColumnId> col_ids;
    vector<KeyRange> range;
    tablet->SplitKeyRange(nullptr, nullptr, col_ids, 900, &range);
    AssertChunks(result, range);
  }
  // target chunk size greater than the max interval size
  {
    vector<KeyRange> result = {
      KeyRange("", "", 13000)
    };
    vector<ColumnId> col_ids;
    vector<KeyRange> range;
    tablet->SplitKeyRange(nullptr, nullptr, col_ids, 20000, &range);
    AssertChunks(result, range);
  }
  // test split key range with column
  {
    vector<KeyRange> result = {
      KeyRange("", "2", 40),
      KeyRange("2", "5", 120),
      KeyRange("5", "6", 40),
      KeyRange("6", "", 60)
    };
    vector<ColumnId> col_ids;
    col_ids.emplace_back(0);
    col_ids.emplace_back(1);
    vector<KeyRange> range;
    tablet->SplitKeyRange(nullptr, nullptr, col_ids, 60, &range);
    AssertChunks(result, range);
  }
  // test split key range with bound
  {
    EncodedKey* l_enc_key = nullptr;
    EncodedKey* u_enc_key = nullptr;
    Arena arena(256);
    KuduPartialRow lower_bound(&this->schema_);
    CHECK_OK(lower_bound.SetString("key", "1"));
    CHECK_OK(lower_bound.SetInt32("key_idx", 0));
    CHECK_OK(lower_bound.SetInt32("val", 0));
    string l_encoded;
    ASSERT_OK(lower_bound.EncodeRowKey(&l_encoded));
    ASSERT_OK(EncodedKey::DecodeEncodedString(this->schema_, &arena, l_encoded, &l_enc_key));

    KuduPartialRow upper_bound(&this->schema_);
    CHECK_OK(upper_bound.SetString("key", "4"));
    CHECK_OK(upper_bound.SetInt32("key_idx", 0));
    CHECK_OK(upper_bound.SetInt32("val", 0));
    string u_encoded;
    ASSERT_OK(upper_bound.EncodeRowKey(&u_encoded));
    ASSERT_OK(EncodedKey::DecodeEncodedString(this->schema_, &arena, u_encoded, &u_enc_key));
    // split key range in [1, 4)
    {
      vector<KeyRange> result = {
        KeyRange("1", "2", 1000),
        KeyRange("2", "4", 4000)
      };
      vector<ColumnId> col_ids;
      vector<KeyRange> range;
      tablet->SplitKeyRange(l_enc_key, u_enc_key, col_ids, 2000, &range);
      AssertChunks(result, range);
    }
    // split key range in [min, 4)
    {
      vector<KeyRange> result = {
        KeyRange("", "2", 2000),
        KeyRange("2", "4", 4000)
      };
      vector<ColumnId> col_ids;
      vector<KeyRange> range;
      tablet->SplitKeyRange(nullptr, u_enc_key, col_ids, 2000, &range);
      AssertChunks(result, range);
    }
    // split key range in [4, max)
    {
      vector<KeyRange> result = {
        KeyRange("4", "5", 2000),
        KeyRange("5", "6", 2000),
        KeyRange("6", "", 3000)
      };
      vector<ColumnId> col_ids;
      vector<KeyRange> range;
      tablet->SplitKeyRange(u_enc_key, nullptr, col_ids, 2000, &range);
      AssertChunks(result, range);
    }
  }
}

// Test for split key range, tablet with 0 rowsets
TEST_F(TestTabletStringKey, TestSplitKeyRangeWithZeroRowSets) {
  Tablet* tablet = this->mutable_tablet();

  scoped_refptr<TabletComponents> comps;
  tablet->GetComponents(&comps);

  RowSetVector old_rowset = comps->rowsets->all_rowsets();
  RowSetVector new_rowset = {};
  tablet->AtomicSwapRowSets(old_rowset, new_rowset);
  {
    vector<KeyRange> result = {
      KeyRange("", "", 0)
    };
    vector<ColumnId> col_ids;
    vector<KeyRange> range;
    tablet->SplitKeyRange(nullptr, nullptr, col_ids, 3000, &range);
    AssertChunks(result, range);
  }
}

// Test for split key range, tablet with 1 rowset
TEST_F(TestTabletStringKey, TestSplitKeyRangeWithOneRowSet) {
  Tablet* tablet = this->mutable_tablet();

  scoped_refptr<TabletComponents> comps;
  tablet->GetComponents(&comps);

  RowSetVector old_rowset = comps->rowsets->all_rowsets();
  RowSetVector new_rowset = {
    make_shared<MockDiskRowSet>("0", "9", 9000, 90)
  };
  tablet->AtomicSwapRowSets(old_rowset, new_rowset);
  {
    vector<KeyRange> result = {
      KeyRange("", "", 9000)
    };
    vector<ColumnId> col_ids;
    vector<KeyRange> range;
    tablet->SplitKeyRange(nullptr, nullptr, col_ids, 3000, &range);
    AssertChunks(result, range);
  }
}

// Test for split key range, tablet with non-overlapping rowsets
TEST_F(TestTabletStringKey, TestSplitKeyRangeWithNonOverlappingRowSets) {
  Tablet *tablet = this->mutable_tablet();

  // Rowsets without gaps
  scoped_refptr<TabletComponents> comps;
  tablet->GetComponents(&comps);
  RowSetVector old_rowset = comps->rowsets->all_rowsets();
  RowSetVector without_gaps_rowset = {
    make_shared<MockDiskRowSet>("0", "2", 2000, 20),
    make_shared<MockDiskRowSet>("2", "5", 3000, 30),
    make_shared<MockDiskRowSet>("5", "6", 1000, 10),
    make_shared<MockDiskRowSet>("6", "9", 3000, 30)
  };
  tablet->AtomicSwapRowSets(old_rowset, without_gaps_rowset);
  {
    vector<KeyRange> result = {
      KeyRange("", "2", 2000),
      KeyRange("2", "5", 3000),
      KeyRange("5", "", 4000)
    };
    vector<ColumnId> col_ids;
    vector<KeyRange> range;
    tablet->SplitKeyRange(nullptr, nullptr, col_ids, 3000, &range);
    AssertChunks(result, range);
  }

  // Rowsets with gaps
  tablet->GetComponents(&comps);
  old_rowset = comps->rowsets->all_rowsets();
  RowSetVector with_gaps_rowset = {
    make_shared<MockDiskRowSet>("0", "2", 2000, 20),
    make_shared<MockDiskRowSet>("5", "6", 1000, 10),
    make_shared<MockDiskRowSet>("6", "9", 3000, 30)
  };
  tablet->AtomicSwapRowSets(old_rowset, with_gaps_rowset);
  {
    vector<KeyRange> result = {
      KeyRange("", "6", 3000),
      KeyRange("6", "", 3000)
    };
    vector<ColumnId> col_ids;
    vector<KeyRange> range;
    tablet->SplitKeyRange(nullptr, nullptr, col_ids, 3000, &range);
    AssertChunks(result, range);
  }
}

// Test for split key range, tablet with rowset whose start is the minimum value
TEST_F(TestTabletStringKey, TestSplitKeyRangeWithMinimumValueRowSet) {
  Tablet *tablet = this->mutable_tablet();

  // Rowsets without gaps
  scoped_refptr<TabletComponents> comps;
  tablet->GetComponents(&comps);
  RowSetVector old_rowset = comps->rowsets->all_rowsets();
  RowSetVector without_gaps_rowset = {
    make_shared<MockDiskRowSet>("", "2", 2500, 20),
    make_shared<MockDiskRowSet>("2", "5", 3000, 30),
    make_shared<MockDiskRowSet>("5", "6", 1000, 10),
    make_shared<MockDiskRowSet>("6", "9", 3000, 30)
  };
  tablet->AtomicSwapRowSets(old_rowset, without_gaps_rowset);
  {
    vector<KeyRange> result = {
      KeyRange("", "2", 2500),
      KeyRange("2", "5", 3000),
      KeyRange("5", "", 4000)
    };
    vector<ColumnId> col_ids;
    vector<KeyRange> range;
    tablet->SplitKeyRange(nullptr, nullptr, col_ids, 3000, &range);
    AssertChunks(result, range);
  }
}

TYPED_TEST(TestTablet, TestDiffScanUnobservableOperations) {
  LocalTabletWriter writer(this->tablet().get(), &this->client_schema());
  vector<LocalTabletWriter::RowOp> ops;

  // Row 0: INSERT -> DELETE.

  KuduPartialRow insert_row0(&this->client_schema());
  this->setup_.BuildRow(&insert_row0, 0);
  ops.emplace_back(RowOperationsPB::INSERT, &insert_row0);

  KuduPartialRow delete_row0(&this->client_schema());
  this->setup_.BuildRowKey(&delete_row0, 0);
  ops.emplace_back(RowOperationsPB::DELETE, &delete_row0);

  // Row 1: INSERT -> UPDATE -> DELETE.

  KuduPartialRow insert_row1(&this->client_schema());
  this->setup_.BuildRow(&insert_row1, 1);
  ops.emplace_back(RowOperationsPB::INSERT, &insert_row1);

  KuduPartialRow update_row1(&this->client_schema());
  this->setup_.BuildRowKey(&update_row1, 1);
  int col_idx = this->client_schema().num_key_columns() == 1 ? 2 : 3;
  ASSERT_OK(update_row1.SetInt32(col_idx, 10));
  ops.emplace_back(RowOperationsPB::UPDATE, &update_row1);

  KuduPartialRow delete_row1(&this->client_schema());
  this->setup_.BuildRowKey(&delete_row1, 1);
  ops.emplace_back(RowOperationsPB::DELETE, &delete_row1);

  // Row 2: INSERT -> DELETE -> REINSERT -> DELETE.

  KuduPartialRow insert_row2(&this->client_schema());
  this->setup_.BuildRow(&insert_row2, 2);
  ops.emplace_back(RowOperationsPB::INSERT, &insert_row2);

  KuduPartialRow first_delete_row2(&this->client_schema());
  this->setup_.BuildRowKey(&first_delete_row2, 2);
  ops.emplace_back(RowOperationsPB::DELETE, &first_delete_row2);

  KuduPartialRow reinsert_row2(&this->client_schema());
  this->setup_.BuildRow(&reinsert_row2, 2);
  ops.emplace_back(RowOperationsPB::INSERT, &reinsert_row2);

  KuduPartialRow second_delete_row2(&this->client_schema());
  this->setup_.BuildRowKey(&second_delete_row2, 2);
  ops.emplace_back(RowOperationsPB::DELETE, &second_delete_row2);

  // Write all operations to the tablet as part of the same batch. This means
  // that they will all be assigned the same timestamp.
  ASSERT_OK(writer.WriteBatch(ops));

  // Performs a diff scan, expecting an empty result set because all three rows
  // are deleted at all times.
  auto diff_scan_no_rows = [&]() {
    // Create a projection with an IS_DELETED virtual column.
    vector<ColumnSchema> col_schemas(this->client_schema().columns());
    bool read_default = false;
    col_schemas.emplace_back("is_deleted", IS_DELETED, /*is_nullable=*/ false,
                             &read_default);
    SchemaPtr projection_ptr = std::make_shared<Schema>(
        col_schemas, this->client_schema().num_key_columns());

    // Do the diff scan.
    RowIteratorOptions opts;
    opts.projection = projection_ptr;
    opts.snap_to_exclude = MvccSnapshot(Timestamp(1));
    opts.snap_to_include = MvccSnapshot(Timestamp(2));
    opts.include_deleted_rows = true;
    unique_ptr<RowwiseIterator> iter;
    ASSERT_OK(this->tablet()->NewRowIterator(std::move(opts), &iter));
    ASSERT_OK(iter->Init(nullptr));
    vector<string> lines;
    ASSERT_OK(IterateToStringList(iter.get(), &lines));

    // Test the results.
    SCOPED_TRACE(JoinStrings(lines, "\n"));
    ASSERT_TRUE(lines.empty());
  };

  NO_FATALS(diff_scan_no_rows());

  ASSERT_OK(this->tablet()->Flush());
  NO_FATALS(diff_scan_no_rows());
}

} // namespace tablet
} // namespace kudu
