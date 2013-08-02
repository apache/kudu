// Copyright (c) 2012, Cloudera, inc.

#include <glog/logging.h>
#include <time.h>
#include <gutil/strings/join.h>
#include "common/iterator.h"
#include "common/row.h"
#include "common/scan_spec.h"
#include "tablet/memrowset.h"
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
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Make sure the files were created as expected.
  const RowSetMetadata *rowset_meta = this->tablet_->metadata()->GetRowSetForTests(0);
  ASSERT_TRUE(rowset_meta->HasColumnDataBlockForTests(0));
  ASSERT_TRUE(rowset_meta->HasColumnDataBlockForTests(1));
  ASSERT_TRUE(rowset_meta->HasColumnDataBlockForTests(2));
  ASSERT_TRUE(rowset_meta->HasBloomDataBlockForTests());
}

// Test that historical data for a row is maintained even after the row
// is flushed from the memrowset.
//
// This test is disabled because the current implementation does not actually
// Flush/Compact UNDO records or any other MVCC data. Instead, it just writes
// the most up-to-date version. We should re-enable this once UNDO files are
// implemented.
TYPED_TEST(TestTablet, DISABLED_TestMVCCAfterFlush) {
  // Insert 5 rows into the memrowset.
  // These rows will be inserted with txid 0 through 4.
  vector<MvccSnapshot> snaps;
  snaps.push_back(MvccSnapshot(this->tablet_->mvcc_manager()));

  for (int i = 0; i < 5; i++) {
    this->InsertTestRows(i, 1, 0);
    snaps.push_back(MvccSnapshot(this->tablet_->mvcc_manager()));
  }

  // TODO: also update a row in a bunch of transactions and make sure
  // that update history is maintained.
  // TODO: when delete is supported, add delete/reinsert.

  // Flush the tablet.
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Verify that reading past snapshots shows the correct number of rows.
  for (int i = 0; i < snaps.size(); i++) {
    vector<string> rows;
    gscoped_ptr<RowwiseIterator> iter;
    ASSERT_STATUS_OK(this->tablet_->NewRowIterator(this->schema_, snaps[i], &iter));
    ASSERT_STATUS_OK(iter->Init(NULL));
    ASSERT_STATUS_OK(kudu::tablet::IterateToStringList(iter.get(), &rows));
    ASSERT_EQ(i, rows.size()) << "Bad result: " << JoinStrings(rows, "\n");
  }
}

// Test that inserting a row which already exists causes an AlreadyPresent
// error
TYPED_TEST(TestTablet, TestInsertDuplicateKey) {
  RowBuilder rb(this->schema_);
  this->setup_.BuildRow(&rb, 12345);
  ConstContiguousRow row(rb.schema(), rb.data());
  ASSERT_STATUS_OK(this->tablet_->Insert(row));

  // Insert again, should fail!
  Status s = this->tablet_->Insert(row);
  ASSERT_TRUE(s.IsAlreadyPresent()) <<
    "expected AlreadyPresent, but got: " << s.ToString();

  ASSERT_EQ(1, this->TabletCount());

  // Flush, and make sure that inserting duplicate still fails
  ASSERT_STATUS_OK(this->tablet_->Flush());

  ASSERT_EQ(1, this->TabletCount());

  s = this->tablet_->Insert(row);
  ASSERT_TRUE(s.IsAlreadyPresent())
    << "expected AlreadyPresent, but got: " << s.ToString()
    << " Inserting: " << rb.data().ToDebugString();

  ASSERT_EQ(1, this->TabletCount());
}


// Test flushes and compactions dealing with deleted rows.
TYPED_TEST(TestTablet, TestDeleteWithFlushAndCompact) {

  this->InsertTestRows(0, 1, 0);
  ASSERT_STATUS_OK(this->DeleteTestRow(0));

  // The row is deleted, so we shouldn't see it in the iterator.
  vector<string> rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // Flush the tablet and make sure the data doesn't re-appear.
  ASSERT_STATUS_OK(this->tablet_->Flush());
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // Re-inserting should succeed. This will reinsert into the MemRowSet.
  // Set the int column to '1' this time, so we can differentiate the two
  // versions of the row.
  this->InsertTestRows(0, 1, 1);
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 1), rows[0]);

  // Flush again, so the DiskRowSet has the row.
  ASSERT_STATUS_OK(this->tablet_->Flush());
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 1), rows[0]);

  // Delete it again, now that it's in DRS.
  ASSERT_STATUS_OK(this->DeleteTestRow(0));
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(0, rows.size());

  // We now have an INSERT in the MemRowSet and the
  // deleted row in the DiskRowSet. The new version
  // of the row has '2' in the int column.
  this->InsertTestRows(0, 1, 2);
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 2), rows[0]);

  // Flush - now we have the row in two different DRSs.
  ASSERT_STATUS_OK(this->tablet_->Flush());
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 2), rows[0]);

  // Compaction should succeed even with the duplicate rows.
  ASSERT_STATUS_OK(this->tablet_->Compact(Tablet::FORCE_COMPACT_ALL));
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 2), rows[0]);
}

// Test flushes dealing with REINSERT mutations in the MemRowSet.
TYPED_TEST(TestTablet, TestFlushWithReinsert) {

  // Insert, delete, and re-insert a row in the MRS.
  this->InsertTestRows(0, 1, 0);
  ASSERT_STATUS_OK(this->DeleteTestRow(0));
  this->InsertTestRows(0, 1, 1);

  // Flush the tablet and make sure the data persists.
  ASSERT_STATUS_OK(this->tablet_->Flush());
  vector<string> rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(this->setup_.FormatDebugRow(0, 1), rows[0]);
}

// Test flushes dealing with REINSERT mutations if they arrive in the middle
// of a flush.
TYPED_TEST(TestTablet, TestReinsertDuringFlush) {
  // Insert/delete/insert/delete in MemRowStore.
  this->InsertTestRows(0, 1, 0);
  ASSERT_STATUS_OK(this->DeleteTestRow(0));
  this->InsertTestRows(0, 1, 1);
  ASSERT_STATUS_OK(this->DeleteTestRow(0));

  // During the snapshot flush, insert/delete/insert some more during the flush.
  class MyCommonHooks : public Tablet::FlushCompactCommonHooks {
   public:
    explicit MyCommonHooks(TestFixture *test) : test_(test) {}

    Status PostWriteSnapshot() {
      test_->InsertTestRows(0, 1, 1);
      CHECK_OK(test_->DeleteTestRow(0));
      test_->InsertTestRows(0, 1, 2);
      CHECK_OK(test_->DeleteTestRow(0));
      test_->InsertTestRows(0, 1, 3);
      return Status::OK();
    }

   private:
    TestFixture *test_;
  };
  shared_ptr<Tablet::FlushCompactCommonHooks> common_hooks(
    reinterpret_cast<Tablet::FlushCompactCommonHooks *>(new MyCommonHooks(this)));
  this->tablet_->SetFlushCompactCommonHooksForTests(common_hooks);

  // Flush the tablet and make sure the data persists.
  ASSERT_STATUS_OK(this->tablet_->Flush());
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
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.row()));
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Put a row in disk rowset 2 (insert and flush)
  rb.Reset();
  this->setup_.BuildRow(&rb, kInRowSet2);
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.row()));
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Put a row in memrowset
  rb.Reset();
  this->setup_.BuildRow(&rb, kInMemRowSet);
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.row()));

  // Now iterate the tablet and make sure the rows show up
  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(this->tablet_->NewRowIterator(this->schema_, &iter));
  ASSERT_STATUS_OK(iter->Init(NULL));

  ASSERT_TRUE(iter->HasNext());

  RowBlock block(this->schema_, 100, &this->arena_);

  // First call to CopyNextRows should fetch the whole memrowset.
  ASSERT_STATUS_OK_FAST(RowwiseIterator::CopyBlock(iter.get(), &block));
  ASSERT_EQ(1, block.nrows()) << "should get only the one row from memrowset";
  this->VerifyRow(block.row(0), kInMemRowSet, 0);

  // Next, should fetch the older rowset
  ASSERT_TRUE(iter->HasNext());
  ASSERT_STATUS_OK(RowwiseIterator::CopyBlock(iter.get(), &block));
  ASSERT_EQ(1, block.nrows()) << "should get only the one row from rowset 1";
  this->VerifyRow(block.row(0), kInRowSet1, 0);

  // Next, should fetch the newer rowset
  ASSERT_TRUE(iter->HasNext());
  ASSERT_STATUS_OK(RowwiseIterator::CopyBlock(iter.get(), &block));
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
  for (uint32_t i = 0; i < max_rows; i++) {
    rb.Reset();
    this->setup_.BuildRow(&rb, i);
    ASSERT_STATUS_OK(this->tablet_->Insert(rb.row()));
    inserted.insert(i);

    if (i % 300 == 0) {
      LOG(INFO) << "Flushing after " << i << " rows inserted";
      ASSERT_STATUS_OK(this->tablet_->Flush());
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
    ASSERT_STATUS_OK_FAST(this->setup_.DoUpdate(this->tablet_.get(), i, &new_val));
    inserted.erase(i);
    inserted.insert(new_val);
  }

  // Now iterate the tablet and make sure the rows show up.
  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(this->tablet_->NewRowIterator(this->schema_, &iter));
  ASSERT_STATUS_OK(iter->Init(NULL));
  LOG(INFO) << "Created iter: " << iter->ToString();

  RowBlock block(this->schema_, 100, &this->arena_);
  while (iter->HasNext()) {
    this->arena_.Reset();
    ASSERT_STATUS_OK(RowwiseIterator::CopyBlock(iter.get(), &block));
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
  ASSERT_STATUS_OK(this->tablet_->Flush());

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
  this->InsertTestRows(0, 1, 0);
  ASSERT_STATUS_OK(this->UpdateTestRow(0, 1));
  ASSERT_STATUS_OK(this->UpdateTestRow(0, 2));
  ASSERT_STATUS_OK(this->UpdateTestRow(0, 3));

  // Should see most recent value.
  vector<string> out_rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(1, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 3), out_rows[0]);

  // Flush it.
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Should still see most recent value.
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(1, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 3), out_rows[0]);

  // Update the row a few times in DeltaMemStore
  ASSERT_STATUS_OK(this->UpdateTestRow(0, 4));
  ASSERT_STATUS_OK(this->UpdateTestRow(0, 5));
  ASSERT_STATUS_OK(this->UpdateTestRow(0, 6));

  // Should still see most recent value.
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(1, out_rows.size());
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 6), out_rows[0]);


  // Force a compaction after adding a new rowset with one row.
  this->InsertTestRows(1, 1, 0);
  ASSERT_STATUS_OK(this->tablet_->Flush());
  ASSERT_EQ(2, this->tablet_->num_rowsets());

  ASSERT_STATUS_OK(this->tablet_->Compact(Tablet::FORCE_COMPACT_ALL));
  ASSERT_EQ(1, this->tablet_->num_rowsets());

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
      ASSERT_STATUS_OK(this->tablet_->Flush());
    }

    ASSERT_TRUE(this->tablet_->metadata()->GetRowSetForTests(0)->HasColumnDataBlockForTests(0));
  }

  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(n_rows, n_rows, 0);
    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_STATUS_OK(this->tablet_->Flush());
    }

    ASSERT_TRUE(this->tablet_->metadata()->GetRowSetForTests(1)->HasColumnDataBlockForTests(0));
  }

  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(n_rows * 2, n_rows, 0);
    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_STATUS_OK(this->tablet_->Flush());
    }

    ASSERT_TRUE(this->tablet_->metadata()->GetRowSetForTests(2)->HasColumnDataBlockForTests(0));
  }

  // Issue compaction
  LOG_TIMING(INFO, "Compacting rows") {
    ASSERT_STATUS_OK(this->tablet_->Compact(Tablet::FORCE_COMPACT_ALL));
    ASSERT_EQ(n_rows * 3, this->TabletCount());

    const RowSetMetadata *rowset_meta = this->tablet_->metadata()->GetRowSetForTests(3);
    ASSERT_TRUE(rowset_meta != NULL);
    ASSERT_TRUE(rowset_meta->HasColumnDataBlockForTests(0));
    ASSERT_TRUE(rowset_meta->HasBloomDataBlockForTests());
  }

  // Old rowsets should not exist anymore
  for (int i = 0; i <= 2; i++) {
    const RowSetMetadata *rowset_meta = this->tablet_->metadata()->GetRowSetForTests(i);
    ASSERT_TRUE(rowset_meta == NULL);
  }
}

// Hook used by the Test*WithConcurrentMutation tests.
//
// Every time one of these hooks triggers, it inserts a row starting
// at row 20 (and increasing), and updates a row starting at row 10
// (and increasing).
template<class TestFixture>
class MyCommonHooks : public Tablet::FlushCompactCommonHooks {
 public:
  explicit MyCommonHooks(TestFixture *test) : test_(test), i_(0) {}
  Status DoHook() {
    RETURN_NOT_OK(test_->DeleteTestRow(i_));
    RETURN_NOT_OK(test_->UpdateTestRow(10 + i_, 1000 + i_));
    test_->InsertTestRows(20 + i_, 1, 0);
    test_->CheckCanIterate();
    i_++;
    return Status::OK();
  }

  virtual Status PostTakeMvccSnapshot() { return DoHook(); }
  virtual Status PostWriteSnapshot() { return DoHook(); }
  virtual Status PostSwapInDuplicatingRowSet() { return DoHook(); }
  virtual Status PostReupdateMissedDeltas() { return DoHook(); }
  virtual Status PostSwapNewRowSet() { return DoHook(); }
 protected:
  TestFixture *test_;
  int i_;
};

template<class TestFixture>
class MyFlushHooks : public Tablet::FlushFaultHooks, public MyCommonHooks<TestFixture> {
 public:
  explicit MyFlushHooks(TestFixture *test) : MyCommonHooks<TestFixture>(test) {}
  virtual Status PostSwapNewMemRowSet() { return this->DoHook(); }
};

template<class TestFixture>
class MyCompactHooks : public Tablet::CompactionFaultHooks, public MyCommonHooks<TestFixture> {
 public:
  explicit MyCompactHooks(TestFixture *test) : MyCommonHooks<TestFixture>(test) {}
  Status PostSelectIterators() { return this->DoHook(); }
};

// Test for Flush with concurrent update, delete and insert during the
// various phases.
TYPED_TEST(TestTablet, TestFlushWithConcurrentMutation) {
  this->InsertTestRows(0, 7, 0); // 0-6 inclusive: these rows will be deleted
  this->InsertTestRows(10, 7, 0); // 10-16 inclusive: these rows will be updated
  // Rows 20-26 inclusive will be inserted during the flush

  // Inject hooks which mutate those rows and add more rows at
  // each key stage of flushing.
  shared_ptr<MyFlushHooks<TestFixture> > hooks(new MyFlushHooks<TestFixture>(this));
  this->tablet_->SetFlushHooksForTests(hooks);
  this->tablet_->SetFlushCompactCommonHooksForTests(hooks);

  // First hook before we do the Flush
  ASSERT_STATUS_OK(hooks->DoHook());

  // Then do the flush with the hooks enabled.
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Now verify that the results saw all the mutations.
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
  ASSERT_STATUS_OK(this->tablet_->Flush());

  this->InsertTestRows(2, 2, 0);  // rows 2-3
  this->InsertTestRows(12, 2, 0); // rows 12-13
  ASSERT_STATUS_OK(this->tablet_->Flush());

  this->InsertTestRows(4, 3, 0);  // rows 4-6
  this->InsertTestRows(14, 3, 0); // rows 14-16
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Rows 20-26 inclusive will be inserted during the flush.

  shared_ptr<MyCompactHooks<TestFixture> > hooks(new MyCompactHooks<TestFixture>(this));
  this->tablet_->SetCompactionHooksForTests(hooks);
  this->tablet_->SetFlushCompactCommonHooksForTests(hooks);

  // First hook pre-compaction.
  ASSERT_STATUS_OK(hooks->DoHook());

  // Issue compaction
  ASSERT_STATUS_OK(this->tablet_->Compact(Tablet::FORCE_COMPACT_ALL));

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
