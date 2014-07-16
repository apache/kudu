// Copyright (c) 2012, Cloudera, inc.

#include <glog/logging.h>
#include <time.h>
#include <gutil/stl_util.h>
#include <gutil/strings/join.h>

#include "common/iterator.h"
#include "common/row.h"
#include "common/scan_spec.h"
#include "tablet/deltafile.h"
#include "tablet/tablet.h"
#include "tablet/tablet-test-base.h"
#include "util/slice.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

using metadata::RowSetMetadata;
using std::tr1::unordered_set;

DEFINE_int32(testflush_num_inserts, 1000,
             "Number of rows inserted in TestFlush");
DEFINE_int32(testiterator_num_inserts, 1000,
             "Number of rows inserted in TestRowIterator/TestInsert");
DEFINE_int32(testcompaction_num_rows, 1000,
             "Number of rows per rowset in TestCompaction");

template<class SETUP>
class TestTablet : public TabletTestBase<SETUP> {
  typedef SETUP Type;

 public:
  // Verify that iteration doesn't fail
  void CheckCanIterate() {
    vector<string> out_rows;
    ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  }

};
TYPED_TEST_CASE(TestTablet, TabletTestHelperTypes);

TYPED_TEST(TestTablet, TestFlush) {
  // Insert 1000 rows into memrowset
  uint64_t max_rows = this->ClampRowCount(FLAGS_testflush_num_inserts);
  this->InsertTestRows(0, max_rows, 0);

  // Flush it.
  ASSERT_STATUS_OK(this->tablet()->Flush());
  metadata::TabletMetadata* tablet_meta = this->tablet()->metadata();

  // Make sure the files were created as expected.
  metadata::RowSetMetadata* rowset_meta = tablet_meta->GetRowSetForTests(0);
  CHECK(rowset_meta) << "No row set found";
  ASSERT_TRUE(rowset_meta->HasColumnDataBlockForTests(0));
  ASSERT_TRUE(rowset_meta->HasColumnDataBlockForTests(1));
  ASSERT_TRUE(rowset_meta->HasColumnDataBlockForTests(2));
  ASSERT_TRUE(rowset_meta->HasBloomDataBlockForTests());

  // check that undo deltas are present
  vector<BlockId> undo_blocks = rowset_meta->undo_delta_blocks();
  ASSERT_EQ(1, undo_blocks.size());

  // Read the undo delta, we should get one undo mutation (delete) for each row.
  size_t dsize = 0;
  shared_ptr<RandomAccessFile> dfile;
  ASSERT_STATUS_OK(rowset_meta->OpenDataBlock(undo_blocks[0], &dfile, &dsize));

  shared_ptr<DeltaFileReader> dfr;
  ASSERT_STATUS_OK(DeltaFileReader::Open(undo_blocks[0].ToString(), dfile, dsize,
                                         undo_blocks[0], &dfr, UNDO));
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

  WriteTransactionState tx_state;
  for (int i = 0; i < 5; i++) {
    this->InsertTestRows(i, 1, 0);
    DVLOG(1) << "Inserted row=" << i << ", val=" << i << ", update_count=0";
    MvccSnapshot ins_snaphsot(*this->tablet()->mvcc_manager());
    snaps.push_back(ins_snaphsot);
    LOG(INFO) << "After Insert Snapshot: " <<  ins_snaphsot.ToString();
    if (i > 0) {
      tx_state.Reset();
      ASSERT_STATUS_OK(this->UpdateTestRow(&tx_state, i - 1, i));
      DVLOG(1) << "Mutated row=" << i - 1 << ", val=" << i - 1 << ", update_count=" << i;
      MvccSnapshot mut_snaphsot(*this->tablet()->mvcc_manager());
      snaps.push_back(mut_snaphsot);
      DVLOG(1) << "After Mutate Snapshot: " <<  mut_snaphsot.ToString();
    }
  }

  // Collect the expected rows from the MRS, where there are no
  // undos
  vector<vector<string>* > expected_rows;
  CollectRowsForSnapshots(this->tablet().get(), this->schema_, snaps, &expected_rows);

  // Flush the tablet
  ASSERT_STATUS_OK(this->tablet()->Flush());

  // Now verify that with undos we get the same thing.
  VerifySnapshotsHaveSameResult(this->tablet().get(), this->schema_, snaps, expected_rows);

  // Do some more work and flush/compact
  // take a snapshot and mutate the rows so that we have undos and
  // redos
  snaps.push_back(MvccSnapshot(*this->tablet()->mvcc_manager()));
//
  for (int i = 0; i < 4; i++) {
    tx_state.Reset();
    ASSERT_STATUS_OK(this->UpdateTestRow(&tx_state, i, i + 10));
    DVLOG(1) << "Mutated row=" << i << ", val=" << i << ", update_count=" << i + 10;
    MvccSnapshot mut_snaphsot(*this->tablet()->mvcc_manager());
    snaps.push_back(mut_snaphsot);
    DVLOG(1) << "After Mutate Snapshot: " <<  mut_snaphsot.ToString();
  }

  // also throw a delete in there.
  tx_state.Reset();
  ASSERT_STATUS_OK(this->DeleteTestRow(&tx_state, 4));
  MvccSnapshot delete_snaphsot(*this->tablet()->mvcc_manager());
  snaps.push_back(delete_snaphsot);
  DVLOG(1) << "After Delete Snapshot: " <<  delete_snaphsot.ToString();

  // Collect the expected rows now that we have undos and redos
  STLDeleteElements(&expected_rows);
  CollectRowsForSnapshots(this->tablet().get(), this->schema_, snaps, &expected_rows);

  // now flush and the compact everything
  ASSERT_STATUS_OK(this->tablet()->Flush());
  ASSERT_STATUS_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Now verify that with undos and redos we get the same thing.
  VerifySnapshotsHaveSameResult(this->tablet().get(), this->schema_, snaps, expected_rows);

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
  WriteTransactionState tx;

  for (int i = 0; i < 3; i++) {
    this->InsertTestRow(&tx, 0, 0);
    tx.Reset();
    this->DeleteTestRow(&tx, 0);
    tx.Reset();
    ASSERT_STATUS_OK(this->tablet()->Flush());
  }

  // Create one more rowset on disk which has just an INSERT (ie a non-ghost row).
  this->InsertTestRow(&tx, 0, 0);
  tx.Reset();
  ASSERT_STATUS_OK(this->tablet()->Flush());

  // Compact. This should result in a rowset with just one row in it.
  ASSERT_STATUS_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Should still be able to update, since the row is live.
  ASSERT_STATUS_OK(this->UpdateTestRow(&tx, 0, 1));
}

// Test that inserting a row which already exists causes an AlreadyPresent
// error
TYPED_TEST(TestTablet, TestInsertDuplicateKey) {
  RowBuilder rb(this->schema_);
  this->setup_.BuildRow(&rb, 12345);
  ConstContiguousRow row(rb.schema(), rb.data());

  WriteTransactionState tx_state;
  ASSERT_STATUS_OK(this->tablet()->InsertForTesting(&tx_state, row));
  ASSERT_EQ(1, tx_state.Result().ops().size());

  // Insert again, should fail!
  tx_state.Reset();
  Status s = this->tablet()->InsertForTesting(&tx_state, row);
  ASSERT_TRUE(s.IsAlreadyPresent()) <<
    "expected AlreadyPresent, but got: " << s.ToString();
  ASSERT_EQ(1, tx_state.Result().ops().size());
  ASSERT_FALSE(tx_state.is_all_success());

  ASSERT_EQ(1, this->TabletCount());

  // Flush, and make sure that inserting duplicate still fails
  ASSERT_STATUS_OK(this->tablet()->Flush());

  ASSERT_EQ(1, this->TabletCount());

  tx_state.Reset();
  s = this->tablet()->InsertForTesting(&tx_state, row);
  ASSERT_TRUE(s.IsAlreadyPresent())
    << "expected AlreadyPresent, but got: " << s.ToString()
    << " Inserting: " << rb.data().ToDebugString();
  ASSERT_EQ(1, tx_state.Result().ops().size());

  ASSERT_EQ(1, this->TabletCount());
}


// Test flushes and compactions dealing with deleted rows.
TYPED_TEST(TestTablet, TestDeleteWithFlushAndCompact) {

  WriteTransactionState tx_state;
  this->InsertTestRow(&tx_state, 0, 0);
  tx_state.Reset();
  ASSERT_STATUS_OK(this->DeleteTestRow(&tx_state, 0));
  ASSERT_EQ(0L, last_mutation(tx_state).mutated_stores(0).mrs_id());

  // The row is deleted, so we shouldn't see it in the iterator.
  vector<string> rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // Flush the tablet and make sure the data doesn't re-appear.
  ASSERT_STATUS_OK(this->tablet()->Flush());
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // Re-inserting should succeed. This will reinsert into the MemRowSet.
  // Set the int column to '1' this time, so we can differentiate the two
  // versions of the row.
  tx_state.Reset();
  this->InsertTestRow(&tx_state, 0, 1);
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 1), rows[0]);

  // Flush again, so the DiskRowSet has the row.
  ASSERT_STATUS_OK(this->tablet()->Flush());
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 1), rows[0]);

  // Delete it again, now that it's in DRS.
  tx_state.Reset();
  ASSERT_STATUS_OK(this->DeleteTestRow(&tx_state, 0));
  ASSERT_EQ(1, last_mutation(tx_state).mutated_stores_size());
  ASSERT_EQ(1L, last_mutation(tx_state).mutated_stores(0).rs_id());
  ASSERT_EQ(0L, last_mutation(tx_state).mutated_stores(0).dms_id());
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // We now have an INSERT in the MemRowSet and the
  // deleted row in the DiskRowSet. The new version
  // of the row has '2' in the int column.
  tx_state.Reset();
  this->InsertTestRow(&tx_state, 0, 2);
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 2), rows[0]);

  // Flush - now we have the row in two different DRSs.
  ASSERT_STATUS_OK(this->tablet()->Flush());
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 2), rows[0]);

  // Compaction should succeed even with the duplicate rows.
  ASSERT_STATUS_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 2), rows[0]);
}

// Test flushes dealing with REINSERT mutations in the MemRowSet.
TYPED_TEST(TestTablet, TestFlushWithReinsert) {

  // Insert, delete, and re-insert a row in the MRS.
  WriteTransactionState tx_state;
  this->InsertTestRow(&tx_state, 0, 0);
  tx_state.Reset();
  ASSERT_STATUS_OK(this->DeleteTestRow(&tx_state, 0));
  ASSERT_EQ(1, last_mutation(tx_state).mutated_stores_size());
  ASSERT_EQ(0L, last_mutation(tx_state).mutated_stores(0).mrs_id());
  tx_state.Reset();
  this->InsertTestRow(&tx_state, 0, 1);

  // Flush the tablet and make sure the data persists.
  ASSERT_STATUS_OK(this->tablet()->Flush());
  vector<string> rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 1), rows[0]);
}

// Test flushes dealing with REINSERT mutations if they arrive in the middle
// of a flush.
TYPED_TEST(TestTablet, TestReinsertDuringFlush) {
  // Insert/delete/insert/delete in MemRowStore.
  WriteTransactionState tx_state;
  this->InsertTestRow(&tx_state, 0, 0);
  tx_state.Reset();
  ASSERT_STATUS_OK(this->DeleteTestRow(&tx_state, 0));
  ASSERT_EQ(1, last_mutation(tx_state).mutated_stores_size());
  ASSERT_EQ(0L, last_mutation(tx_state).mutated_stores(0).mrs_id());
  tx_state.Reset();
  this->InsertTestRow(&tx_state, 0, 1);
  tx_state.Reset();
  ASSERT_STATUS_OK(this->DeleteTestRow(&tx_state, 0));
  ASSERT_EQ(1, last_mutation(tx_state).mutated_stores_size());
  ASSERT_EQ(0L, last_mutation(tx_state).mutated_stores(0).mrs_id());

  // During the snapshot flush, insert/delete/insert some more during the flush.
  class MyCommonHooks : public Tablet::FlushCompactCommonHooks {
   public:
    explicit MyCommonHooks(TestFixture *test) : test_(test) {}

    Status PostWriteSnapshot() OVERRIDE {
      WriteTransactionState tx_state;
      test_->InsertTestRow(&tx_state, 0, 1);
      tx_state.Reset();
      CHECK_OK(test_->DeleteTestRow(&tx_state, 0));
      CHECK_EQ(1, last_mutation(tx_state).mutated_stores_size());
      CHECK_EQ(1L, last_mutation(tx_state).mutated_stores(0).mrs_id());
      tx_state.Reset();
      test_->InsertTestRow(&tx_state, 0, 2);
      tx_state.Reset();
      CHECK_OK(test_->DeleteTestRow(&tx_state, 0));
      CHECK_EQ(1, last_mutation(tx_state).mutated_stores_size());
      CHECK_EQ(1L, last_mutation(tx_state).mutated_stores(0).mrs_id());
      tx_state.Reset();
      test_->InsertTestRow(&tx_state, 0, 3);
      return Status::OK();
    }

   private:
    TestFixture *test_;
  };
  shared_ptr<Tablet::FlushCompactCommonHooks> common_hooks(
    reinterpret_cast<Tablet::FlushCompactCommonHooks *>(new MyCommonHooks(this)));
  this->tablet()->SetFlushCompactCommonHooksForTests(common_hooks);

  // Flush the tablet and make sure the data persists.
  ASSERT_STATUS_OK(this->tablet()->Flush());
  vector<string> rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 3), rows[0]);
}

// Test iterating over a tablet which contains data
// in the memrowset as well as two rowsets. This simple test
// only puts one row in each with no updates.
TYPED_TEST(TestTablet, TestRowIteratorSimple) {
  const int kInRowSet1 = 1;
  const int kInRowSet2 = 2;
  const int kInMemRowSet = 3;

  // Put a row in disk rowset 1 (insert and flush)
  RowBuilder rb(this->schema_);
  this->setup_.BuildRow(&rb, kInRowSet1);
  WriteTransactionState tx_state;
  ASSERT_STATUS_OK(this->tablet()->InsertForTesting(&tx_state, rb.row()));
  ASSERT_STATUS_OK(this->tablet()->Flush());

  // Put a row in disk rowset 2 (insert and flush)
  rb.Reset();
  tx_state.Reset();
  this->setup_.BuildRow(&rb, kInRowSet2);
  ASSERT_STATUS_OK(this->tablet()->InsertForTesting(&tx_state, rb.row()));
  ASSERT_STATUS_OK(this->tablet()->Flush());

  // Put a row in memrowset
  rb.Reset();
  tx_state.Reset();
  this->setup_.BuildRow(&rb, kInMemRowSet);
  ASSERT_STATUS_OK(this->tablet()->InsertForTesting(&tx_state, rb.row()));

  // Now iterate the tablet and make sure the rows show up
  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(this->tablet()->NewRowIterator(this->schema_, &iter));
  ASSERT_STATUS_OK(iter->Init(NULL));

  ASSERT_TRUE(iter->HasNext());

  RowBlock block(this->schema_, 100, &this->arena_);

  // First call to CopyNextRows should fetch the whole memrowset.
  ASSERT_STATUS_OK_FAST(iter->NextBlock(&block));
  ASSERT_EQ(1, block.nrows()) << "should get only the one row from memrowset";
  this->VerifyRow(block.row(0), kInMemRowSet, 0);

  // Next, should fetch the older rowset
  ASSERT_TRUE(iter->HasNext());
  ASSERT_STATUS_OK(iter->NextBlock(&block));
  ASSERT_EQ(1, block.nrows()) << "should get only the one row from rowset 1";
  this->VerifyRow(block.row(0), kInRowSet1, 0);

  // Next, should fetch the newer rowset
  ASSERT_TRUE(iter->HasNext());
  ASSERT_STATUS_OK(iter->NextBlock(&block));
  ASSERT_EQ(1, block.nrows()) << "should get only the one row from rowset 2";
  this->VerifyRow(block.row(0), kInRowSet2, 0);

  ASSERT_FALSE(iter->HasNext());
}

// Test iterating over a tablet which has a memrowset
// and several rowsets, each with many rows of data.
TYPED_TEST(TestTablet, TestRowIteratorComplex) {

  uint64_t max_rows = this->ClampRowCount(FLAGS_testiterator_num_inserts);

  // Put a row in disk rowset 1 (insert and flush)
  RowBuilder rb(this->schema_);
  unordered_set<uint32_t> inserted;
  WriteTransactionState tx_state;
  for (uint32_t i = 0; i < max_rows; i++) {
    rb.Reset();
    tx_state.Reset();
    this->setup_.BuildRow(&rb, i);
    ASSERT_STATUS_OK(this->tablet()->InsertForTesting(&tx_state, rb.row()));
    inserted.insert(i);

    if (i % 300 == 0) {
      LOG(INFO) << "Flushing after " << i << " rows inserted";
      ASSERT_STATUS_OK(this->tablet()->Flush());
    }
  }
  LOG(INFO) << "Successfully inserted " << inserted.size() << " rows";

  // At this point, we should have several rowsets as well
  // as some data in memrowset.

  // Update a subset of the rows
  for (uint32_t i = 0; i < max_rows; i++) {
    if (!this->setup_.ShouldUpdateRow(i)) {
      continue;
    }

    SCOPED_TRACE(StringPrintf("update %d", i));
    uint32_t new_val = 0;
    tx_state.Reset();
    ASSERT_STATUS_OK_FAST(this->setup_.DoUpdate(&tx_state,
                                                this->tablet().get(),
                                                i,
                                                &new_val));
    inserted.erase(i);
    inserted.insert(new_val);
  }

  // Now iterate the tablet and make sure the rows show up.
  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(this->tablet()->NewRowIterator(this->schema_, &iter));
  ASSERT_STATUS_OK(iter->Init(NULL));
  LOG(INFO) << "Created iter: " << iter->ToString();

  RowBlock block(this->schema_, 100, &this->arena_);
  while (iter->HasNext()) {
    this->arena_.Reset();
    ASSERT_STATUS_OK(iter->NextBlock(&block));
    LOG(INFO) << "Fetched batch of " << block.nrows();
    for (size_t i = 0; i < block.nrows(); i++) {
      uint32_t val_read = this->setup_.GetRowValueAfterUpdate(block.row(i));
      bool removed = inserted.erase(val_read);
      ASSERT_TRUE(removed) << "Got value " << val_read << " but either "
                           << "the value was invalid or was already "
                           << "seen once!";
    }
  }

  ASSERT_TRUE(inserted.empty())
    << "expected to see all inserted data through iterator. "
    << inserted.size() << " elements were not seen.";
}

// Test that, when a tablet hsa flushed data and is
// reopened, that the data persists
TYPED_TEST(TestTablet, TestInsertsPersist) {
  uint64_t max_rows = this->ClampRowCount(FLAGS_testiterator_num_inserts);

  this->InsertTestRows(0, max_rows, 0);
  ASSERT_EQ(max_rows, this->TabletCount());

  // Flush it.
  ASSERT_STATUS_OK(this->tablet()->Flush());

  ASSERT_EQ(max_rows, this->TabletCount());

  // Close and re-open tablet
  this->TabletReOpen();

  // Ensure that rows exist
  ASSERT_EQ(max_rows, this->TabletCount());
  this->VerifyTestRows(0, max_rows);

  // TODO: add some more data, re-flush
}

// Test that when a row has been updated many times, it always yields
// the most recent value.
TYPED_TEST(TestTablet, TestMultipleUpdates) {
  // Insert and update several times in MemRowSet
  WriteTransactionState tx_state;
  this->InsertTestRow(&tx_state, 0, 0);
  tx_state.Reset();
  ASSERT_STATUS_OK(this->UpdateTestRow(&tx_state, 0, 1));
  ASSERT_EQ(1, last_mutation(tx_state).mutated_stores_size());
  ASSERT_EQ(0L, last_mutation(tx_state).mutated_stores(0).mrs_id());
  tx_state.Reset();
  ASSERT_STATUS_OK(this->UpdateTestRow(&tx_state, 0, 2));
  tx_state.Reset();
  ASSERT_STATUS_OK(this->UpdateTestRow(&tx_state, 0, 3));

  // Should see most recent value.
  vector<string> out_rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(1, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 3), out_rows[0]);

  // Flush it.
  ASSERT_STATUS_OK(this->tablet()->Flush());

  // Should still see most recent value.
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(1, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 3), out_rows[0]);

  // Update the row a few times in DeltaMemStore
  tx_state.Reset();
  ASSERT_STATUS_OK(this->UpdateTestRow(&tx_state, 0, 4));
  ASSERT_EQ(1, last_mutation(tx_state).mutated_stores_size());
  ASSERT_EQ(0L, last_mutation(tx_state).mutated_stores(0).rs_id());
  ASSERT_EQ(0L, last_mutation(tx_state).mutated_stores(0).dms_id());
  tx_state.Reset();
  ASSERT_STATUS_OK(this->UpdateTestRow(&tx_state, 0, 5));
  tx_state.Reset();
  ASSERT_STATUS_OK(this->UpdateTestRow(&tx_state, 0, 6));

  // Should still see most recent value.
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(1, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 6), out_rows[0]);


  // Force a compaction after adding a new rowset with one row.
  tx_state.Reset();
  this->InsertTestRow(&tx_state, 1, 0);
  ASSERT_STATUS_OK(this->tablet()->Flush());
  ASSERT_EQ(2, this->tablet()->num_rowsets());

  ASSERT_STATUS_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  ASSERT_EQ(1, this->tablet()->num_rowsets());

  // Should still see most recent value.
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(2, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 6), out_rows[0]);
  ASSERT_EQ(this->setup_.FormatDebugRow(1, 0), out_rows[1]);
}



TYPED_TEST(TestTablet, TestCompaction) {
  uint64_t max_rows = this->ClampRowCount(FLAGS_testcompaction_num_rows);

  uint64_t n_rows = max_rows / 3;
  // Create three rowsets by inserting and flushing
  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(0, n_rows, 0);

    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_STATUS_OK(this->tablet()->Flush());
    }

    // first MemRowSet had id 0, current one should be 1
    ASSERT_EQ(1, this->tablet()->CurrentMrsIdForTests());
    ASSERT_TRUE(
      this->tablet()->metadata()->GetRowSetForTests(0)->HasColumnDataBlockForTests(0));
  }

  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(n_rows, n_rows, 0);

    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_STATUS_OK(this->tablet()->Flush());
    }

    // previous MemRowSet had id 1, current one should be 2
    ASSERT_EQ(2, this->tablet()->CurrentMrsIdForTests());
    ASSERT_TRUE(
      this->tablet()->metadata()->GetRowSetForTests(1)->HasColumnDataBlockForTests(0));
  }

  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(n_rows * 2, n_rows, 0);

    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_STATUS_OK(this->tablet()->Flush());
    }

    // previous MemRowSet had id 2, current one should be 3
    ASSERT_EQ(3, this->tablet()->CurrentMrsIdForTests());
    ASSERT_TRUE(
      this->tablet()->metadata()->GetRowSetForTests(2)->HasColumnDataBlockForTests(0));
  }

  // Issue compaction
  LOG_TIMING(INFO, "Compacting rows") {

    ASSERT_STATUS_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
    // Compaction does not swap the memrowsets so we should still get 3
    ASSERT_EQ(3, this->tablet()->CurrentMrsIdForTests());
    ASSERT_EQ(n_rows * 3, this->TabletCount());

    const RowSetMetadata *rowset_meta = this->tablet()->metadata()->GetRowSetForTests(3);
    ASSERT_TRUE(rowset_meta != NULL);
    ASSERT_TRUE(rowset_meta->HasColumnDataBlockForTests(0));
    ASSERT_TRUE(rowset_meta->HasBloomDataBlockForTests());
  }

  // Old rowsets should not exist anymore
  for (int i = 0; i <= 2; i++) {
    const RowSetMetadata *rowset_meta = this->tablet()->metadata()->GetRowSetForTests(i);
    ASSERT_TRUE(rowset_meta == NULL);
  }
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
    WriteTransactionState tx_state;
    RETURN_NOT_OK(test_->DeleteTestRow(&tx_state, i_));

    switch (expected_mutation_type) {
      case MRS_MUTATION:
        CHECK_EQ(1, last_mutation(tx_state).mutated_stores_size());
        CHECK(last_mutation(tx_state).mutated_stores(0).has_mrs_id());
        break;
      case DELTA_MUTATION:
        CHECK_EQ(1, last_mutation(tx_state).mutated_stores_size());
        CHECK(last_mutation(tx_state).mutated_stores(0).has_rs_id());
        CHECK(last_mutation(tx_state).mutated_stores(0).has_dms_id());
        break;
      case DUPLICATED_MUTATION:
        CHECK_EQ(2, last_mutation(tx_state).mutated_stores_size());
        break;
    }
    tx_state.Reset();
    RETURN_NOT_OK(test_->UpdateTestRow(&tx_state, 10 + i_, 1000 + i_));
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
  shared_ptr<MyFlushHooks<TestFixture> > hooks(new MyFlushHooks<TestFixture>(this, false));
  this->tablet()->SetFlushHooksForTests(hooks);
  this->tablet()->SetFlushCompactCommonHooksForTests(hooks);

  // First hook before we do the Flush
  ASSERT_STATUS_OK(hooks->DoHook(MRS_MUTATION));

  // Then do the flush with the hooks enabled.
  ASSERT_STATUS_OK(this->tablet()->Flush());

  // Now verify that the results saw all the mutated_stores.
  vector<string> out_rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  std::sort(out_rows.begin(), out_rows.end());

  vector<string> expected_rows;
  expected_rows.push_back(this->setup_.FormatDebugRow(10, 1000));
  expected_rows.push_back(this->setup_.FormatDebugRow(11, 1001));
  expected_rows.push_back(this->setup_.FormatDebugRow(12, 1002));
  expected_rows.push_back(this->setup_.FormatDebugRow(13, 1003));
  expected_rows.push_back(this->setup_.FormatDebugRow(14, 1004));
  expected_rows.push_back(this->setup_.FormatDebugRow(15, 1005));
  expected_rows.push_back(this->setup_.FormatDebugRow(16, 1006));
  expected_rows.push_back(this->setup_.FormatDebugRow(20, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(21, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(22, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(23, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(24, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(25, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(26, 0));

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
  ASSERT_STATUS_OK(this->tablet()->Flush());

  this->InsertTestRows(2, 2, 0);  // rows 2-3
  this->InsertTestRows(12, 2, 0); // rows 12-13
  ASSERT_STATUS_OK(this->tablet()->Flush());

  this->InsertTestRows(4, 3, 0);  // rows 4-6
  this->InsertTestRows(14, 3, 0); // rows 14-16
  ASSERT_STATUS_OK(this->tablet()->Flush());

  // Rows 20-26 inclusive will be inserted during the flush.

  shared_ptr<MyCompactHooks<TestFixture> > hooks(new MyCompactHooks<TestFixture>(this, true));
  this->tablet()->SetCompactionHooksForTests(hooks);
  this->tablet()->SetFlushCompactCommonHooksForTests(hooks);

  // First hook pre-compaction.
  ASSERT_STATUS_OK(hooks->DoHook(DELTA_MUTATION));

  // Issue compaction
  ASSERT_STATUS_OK(this->tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Grab the resulting data into a vector.
  vector<string> out_rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  std::sort(out_rows.begin(), out_rows.end());

  vector<string> expected_rows;
  expected_rows.push_back(this->setup_.FormatDebugRow(10, 1000));
  expected_rows.push_back(this->setup_.FormatDebugRow(11, 1001));
  expected_rows.push_back(this->setup_.FormatDebugRow(12, 1002));
  expected_rows.push_back(this->setup_.FormatDebugRow(13, 1003));
  expected_rows.push_back(this->setup_.FormatDebugRow(14, 1004));
  expected_rows.push_back(this->setup_.FormatDebugRow(15, 1005));
  expected_rows.push_back(this->setup_.FormatDebugRow(16, 1006));
  expected_rows.push_back(this->setup_.FormatDebugRow(20, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(21, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(22, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(23, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(24, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(25, 0));
  expected_rows.push_back(this->setup_.FormatDebugRow(26, 0));

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

} // namespace tablet
} // namespace kudu
