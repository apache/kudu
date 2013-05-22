// Copyright (c) 2012, Cloudera, inc.

#include <glog/logging.h>
#include <time.h>

#include "common/iterator.h"
#include "common/row.h"
#include "common/scan_spec.h"
#include <gutil/strings/join.h>
#include "tablet/memrowset.h"
#include "tablet/tablet.h"
#include "tablet/tablet-test-base.h"
#include "util/slice.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

DEFINE_int32(testflush_num_inserts, 1000,
             "Number of rows inserted in TestFlush");
DEFINE_int32(testcompaction_num_rows, 1000,
             "Number of rows per rowset in TestCompaction");

template<class SETUP>
class TestTablet : public TabletTestBase<SETUP> {
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
  this->InsertTestRows(0, FLAGS_testflush_num_inserts);

  // Flush it.
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Make sure the files were created as expected.
  string rowset_dir_ = Tablet::GetRowSetPath(this->tablet_dir_, 0);
  ASSERT_FILE_EXISTS(this->env_, DiskRowSet::GetColumnPath(rowset_dir_, 0));
  ASSERT_FILE_EXISTS(this->env_, DiskRowSet::GetColumnPath(rowset_dir_, 1));
  ASSERT_FILE_EXISTS(this->env_, DiskRowSet::GetColumnPath(rowset_dir_, 2));
  ASSERT_FILE_EXISTS(this->env_, DiskRowSet::GetBloomPath(rowset_dir_))
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
    this->InsertTestRows(i, 1);
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
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.data()));

  // Insert again, should fail!
  Status s = this->tablet_->Insert(rb.data());
  ASSERT_TRUE(s.IsAlreadyPresent()) <<
    "expected AlreadyPresent, but got: " << s.ToString();

  ASSERT_EQ(1, this->TabletCount());

  // Flush, and make sure that inserting duplicate still fails
  ASSERT_STATUS_OK(this->tablet_->Flush());

  ASSERT_EQ(1, this->TabletCount());

  s = this->tablet_->Insert(rb.data());
  ASSERT_TRUE(s.IsAlreadyPresent())
    << "expected AlreadyPresent, but got: " << s.ToString()
    << " Inserting: " << rb.data().ToDebugString();

  ASSERT_EQ(1, this->TabletCount());
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
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.data()));
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Put a row in disk rowset 2 (insert and flush)
  rb.Reset();
  this->setup_.BuildRow(&rb, kInRowSet2);
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.data()));
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Put a row in memrowset
  rb.Reset();
  this->setup_.BuildRow(&rb, kInMemRowSet);
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.data()));

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
  // Put a row in disk rowset 1 (insert and flush)
  RowBuilder rb(this->schema_);
  unordered_set<uint32_t> inserted;
  for (uint32_t i = 0; i < 1000; i++) {
    rb.Reset();
    this->setup_.BuildRow(&rb, i);
    ASSERT_STATUS_OK(this->tablet_->Insert(rb.data()));
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
  for (uint32_t i = 0; i < 1000; i += 15) {
    SCOPED_TRACE(StringPrintf("update %d", i));
    uint32_t new_val = 10000 + i;
    ASSERT_STATUS_OK_FAST(
      this->setup_.DoUpdate(this->tablet_.get(), i, new_val));
    inserted.erase(i);
    inserted.insert(new_val);
  }

  // Now iterate the tablet and make sure the rows show up.
  gscoped_ptr<RowwiseIterator> iter;
  ASSERT_STATUS_OK(this->tablet_->NewRowIterator(this->schema_, &iter));
  ASSERT_STATUS_OK(iter->Init(NULL));
  LOG(INFO) << "Created iter: " << iter->ToString();

  RowBlock block(this->schema_, 100, &this->arena_);

  // Copy schema into local scope, since gcc is getting confused by
  // too many templates.
  const Schema &schema = this->schema_;

  while (iter->HasNext()) {
    this->arena_.Reset();
    ASSERT_STATUS_OK(RowwiseIterator::CopyBlock(iter.get(), &block));
    LOG(INFO) << "Fetched batch of " << block.nrows();
    for (size_t i = 0; i < block.nrows(); i++) {
      uint32_t val_read = *schema.ExtractColumnFromRow<UINT32>(block.row(i), 1);
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
  this->InsertTestRows(0, 1000);
  ASSERT_EQ(1000, this->TabletCount());

  // Flush it.
  ASSERT_STATUS_OK(this->tablet_->Flush());

  ASSERT_EQ(1000, this->TabletCount());

  // Close and re-open tablet
  this->tablet_.reset(new Tablet(this->schema_, this->tablet_dir_));
  ASSERT_STATUS_OK(this->tablet_->Open());

  // Ensure that rows exist
  this->VerifyTestRows(0, 1000);
  ASSERT_EQ(1000, this->TabletCount());

  // TODO: add some more data, re-flush
}

// Test that when a row has been updated many times, it always yields
// the most recent value.
TYPED_TEST(TestTablet, TestMultipleUpdates) {
  // Insert and update several times in MemRowSet
  this->InsertTestRows(0, 1);
  this->UpdateTestRow(0, 1);
  this->UpdateTestRow(0, 2);
  this->UpdateTestRow(0, 3);

  // Should see most recent value.
  vector<string> out_rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 3), out_rows[0]);

  // Flush it.
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Should still see most recent value.
  out_rows.clear();
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 3), out_rows[0]);

  // Update the row a few times in DeltaMemStore
  this->UpdateTestRow(0, 4);
  this->UpdateTestRow(0, 5);
  this->UpdateTestRow(0, 6);

  // Should still see most recent value.
  out_rows.clear();
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 6), out_rows[0]);


  // Force a compaction after adding a new rowset with one row.
  this->InsertTestRows(1, 1);
  ASSERT_STATUS_OK(this->tablet_->Flush());
  ASSERT_STATUS_OK(this->tablet_->Compact());
  ASSERT_EQ(1, this->tablet_->num_rowsets());

  // Should still see most recent value.
  out_rows.clear();
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));
  ASSERT_EQ(this->setup_.FormatDebugRow(0, 6), out_rows[0]);
}



TYPED_TEST(TestTablet, TestCompaction) {
  uint64_t n_rows = FLAGS_testcompaction_num_rows;
  // Create three rowsets by inserting and flushing
  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(0, n_rows);

    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_STATUS_OK(this->tablet_->Flush());
    }
    string rowset_dir_ = Tablet::GetRowSetPath(this->tablet_dir_, 0);
    ASSERT_FILE_EXISTS(this->env_, DiskRowSet::GetColumnPath(rowset_dir_, 0));
  }

  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(n_rows, n_rows);
    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_STATUS_OK(this->tablet_->Flush());
    }
    string rowset_dir_ = Tablet::GetRowSetPath(this->tablet_dir_, 1);
    ASSERT_FILE_EXISTS(this->env_, DiskRowSet::GetColumnPath(rowset_dir_, 0));
  }

  LOG_TIMING(INFO, "Inserting rows") {
    this->InsertTestRows(n_rows * 2, n_rows);
    LOG_TIMING(INFO, "Flushing rows") {
      ASSERT_STATUS_OK(this->tablet_->Flush());
    }
    string rowset_dir_ = Tablet::GetRowSetPath(this->tablet_dir_, 2);
    ASSERT_FILE_EXISTS(this->env_, DiskRowSet::GetColumnPath(rowset_dir_, 0));
  }

  // Issue compaction
  LOG_TIMING(INFO, "Compacting rows") {
    ASSERT_STATUS_OK(this->tablet_->Compact());
    ASSERT_EQ(n_rows * 3, this->TabletCount());
    string rowset_dir_ = Tablet::GetRowSetPath(this->tablet_dir_, 3);
    ASSERT_FILE_EXISTS(this->env_, DiskRowSet::GetColumnPath(rowset_dir_, 0));
    ASSERT_FILE_EXISTS(this->env_, DiskRowSet::GetBloomPath(rowset_dir_))
  }

  // Old rowsets should not exist anymore
  for (int i = 0; i <= 2; i++) {
    string rowset_dir_ = Tablet::GetRowSetPath(this->tablet_dir_, i);
    ASSERT_FILE_NOT_EXISTS(this->env_, DiskRowSet::GetColumnPath(rowset_dir_, 0));
  }
}

// Test for Flush with concurrent update and insert during the
// various phases.
TYPED_TEST(TestTablet, TestFlushWithConcurrentMutation) {
  // Insert several rows into memrowset
  this->InsertTestRows(0, 5);

  // Inject hooks which mutate those rows and add more rows at
  // each key stage of flushing.
  class MyFlushHooks : public Tablet::FlushFaultHooks {
  public:
    explicit MyFlushHooks(TestFixture *test) : test_(test) {}

    Status PostSwapNewMemRowSet() {
      test_->InsertTestRows(5, 1);
      test_->UpdateTestRow(0, 12345);
      test_->CheckCanIterate();
      return Status::OK();
    }
  private:
    TestFixture *test_;
  };
  shared_ptr<Tablet::FlushFaultHooks> flush_hooks(
    reinterpret_cast<Tablet::FlushFaultHooks *>(new MyFlushHooks(this)));
  this->tablet_->SetFlushHooksForTests(flush_hooks);

  class MyCommonHooks : public Tablet::FlushCompactCommonHooks {
  public:
    explicit MyCommonHooks(TestFixture *test) : test_(test) {}

    Status PostWriteSnapshot() {
      test_->InsertTestRows(6, 1);
      test_->UpdateTestRow(1, 12345);
      test_->CheckCanIterate();
      return Status::OK();
    }

    Status PostSwapInDuplicatingRowSet() {
      test_->InsertTestRows(7, 1);
      test_->UpdateTestRow(2, 12345);
      test_->CheckCanIterate();
      return Status::OK();
    }

    Status PostReupdateMissedDeltas() {
      test_->InsertTestRows(8, 1);
      test_->UpdateTestRow(3, 12345);
      test_->CheckCanIterate();
      return Status::OK();
    }

    Status PostSwapNewRowSet() {
      test_->InsertTestRows(9, 1);
      test_->UpdateTestRow(4, 12345);
      test_->CheckCanIterate();
      return Status::OK();
    }

  private:
    TestFixture *test_;
  };
  shared_ptr<Tablet::FlushCompactCommonHooks> common_hooks(
    reinterpret_cast<Tablet::FlushCompactCommonHooks *>(new MyCommonHooks(this)));
  this->tablet_->SetFlushCompactCommonHooksForTests(common_hooks);


  ASSERT_STATUS_OK(this->tablet_->Flush());

  vector<string> out_rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));

  // Verify that all the inserts and updates arrived and persisted.
  LOG(INFO) << "Results: " << JoinStrings(out_rows, "\n");

  ASSERT_EQ(10, out_rows.size());

  ASSERT_EQ(this->setup_.FormatDebugRow(0, 12345), out_rows[0]);
  ASSERT_EQ(this->setup_.FormatDebugRow(1, 12345), out_rows[1]);
  ASSERT_EQ(this->setup_.FormatDebugRow(2, 12345), out_rows[2]);
  ASSERT_EQ(this->setup_.FormatDebugRow(3, 12345), out_rows[3]);
  ASSERT_EQ(this->setup_.FormatDebugRow(4, 12345), out_rows[4]);
  ASSERT_EQ(this->setup_.FormatDebugRow(5, 0), out_rows[5]);
  ASSERT_EQ(this->setup_.FormatDebugRow(6, 0), out_rows[6]);
  ASSERT_EQ(this->setup_.FormatDebugRow(7, 0), out_rows[7]);
  ASSERT_EQ(this->setup_.FormatDebugRow(8, 0), out_rows[8]);
  ASSERT_EQ(this->setup_.FormatDebugRow(9, 0), out_rows[9]);
}


// Test for compaction with concurrent update and insert during the
// various phases.
TYPED_TEST(TestTablet, TestCompactionWithConcurrentMutation) {
  // Create three rowsets by inserting and flushing
  this->InsertTestRows(0, 1);
  ASSERT_STATUS_OK(this->tablet_->Flush());

  this->InsertTestRows(1, 1);
  ASSERT_STATUS_OK(this->tablet_->Flush());

  this->InsertTestRows(2, 1);
  this->InsertTestRows(3, 1);
  ASSERT_STATUS_OK(this->tablet_->Flush());

  class MyCompactHooks : public Tablet::CompactionFaultHooks {
   public:
    explicit MyCompactHooks(TestFixture *test) : test_(test) {}

    Status PostSelectIterators() {
      test_->InsertTestRows(4, 1);
      test_->UpdateTestRow(0, 12345);

      test_->CheckCanIterate();
      return Status::OK();
    }
   private:
    TestFixture *test_;
  };
  shared_ptr<Tablet::CompactionFaultHooks> hooks(
    reinterpret_cast<Tablet::CompactionFaultHooks *>(new MyCompactHooks(this)));
  this->tablet_->SetCompactionHooksForTests(hooks);

  class MyCommonHooks : public Tablet::FlushCompactCommonHooks {
   public:
    explicit MyCommonHooks(TestFixture *test) : test_(test) {}

    Status PostWriteSnapshot() {
      test_->InsertTestRows(5, 1);
      test_->UpdateTestRow(1, 12345);

      test_->CheckCanIterate();
      return Status::OK();
    }

    Status PostSwapInDuplicatingRowSet() {
      test_->InsertTestRows(6, 1);
      test_->UpdateTestRow(2, 12345);

      test_->CheckCanIterate();
      return Status::OK();
    }

    Status PostReupdateMissedDeltas() {
      test_->InsertTestRows(7, 1);
      test_->UpdateTestRow(3, 12345);
      test_->CheckCanIterate();
      return Status::OK();
    }

    Status PostSwapNewRowSet() {
      test_->InsertTestRows(8, 1);
      test_->UpdateTestRow(4, 12345);
      test_->CheckCanIterate();
      return Status::OK();
    }

   private:
    TestFixture *test_;
  };
  shared_ptr<Tablet::FlushCompactCommonHooks> common_hooks(
    reinterpret_cast<Tablet::FlushCompactCommonHooks *>(new MyCommonHooks(this)));
  this->tablet_->SetFlushCompactCommonHooksForTests(common_hooks);


  // Issue compaction
  ASSERT_STATUS_OK(this->tablet_->Compact());

  // Grab the resulting data into a vector.
  vector<string> out_rows;
  ASSERT_STATUS_OK(this->IterateToStringList(&out_rows));

  // Verify that all the inserts and updates arrived and persisted.
  LOG(INFO) << "Results: " << JoinStrings(out_rows, "\n");

  ASSERT_EQ(9, out_rows.size());

  ASSERT_EQ(this->setup_.FormatDebugRow(0, 12345), out_rows[0]);
  ASSERT_EQ(this->setup_.FormatDebugRow(1, 12345), out_rows[1]);
  ASSERT_EQ(this->setup_.FormatDebugRow(2, 12345), out_rows[2]);
  ASSERT_EQ(this->setup_.FormatDebugRow(3, 12345), out_rows[3]);
  ASSERT_EQ(this->setup_.FormatDebugRow(4, 12345), out_rows[4]);
  ASSERT_EQ(this->setup_.FormatDebugRow(5, 0), out_rows[5]);
  ASSERT_EQ(this->setup_.FormatDebugRow(6, 0), out_rows[6]);
  ASSERT_EQ(this->setup_.FormatDebugRow(7, 0), out_rows[7]);
  ASSERT_EQ(this->setup_.FormatDebugRow(8, 0), out_rows[8]);
}


} // namespace tablet
} // namespace kudu
