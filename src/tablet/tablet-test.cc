// Copyright (c) 2012, Cloudera, inc.

#include <glog/logging.h>
#include <time.h>

#include "common/iterator.h"
#include "common/row.h"
#include "tablet/memstore.h"
#include "tablet/tablet.h"
#include "tablet/tablet-test-base.h"
#include "util/slice.h"
#include "util/test_macros.h"

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

DEFINE_int32(testflush_num_inserts, 1000,
             "Number of rows inserted in TestFlush");

template<class SETUP>
class TestTablet : public TabletTestBase<SETUP> {};
TYPED_TEST_CASE(TestTablet, TabletTestHelperTypes);


TYPED_TEST(TestTablet, TestFlush) {
  // Insert 1000 rows into memstore
  this->InsertTestRows(0, FLAGS_testflush_num_inserts);

  // Flush it.
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Make sure the files were created as expected.
  string layer_dir_ = Tablet::GetLayerPath(this->test_dir_, 0);
  ASSERT_FILE_EXISTS(this->env_, Layer::GetColumnPath(layer_dir_, 0));
  ASSERT_FILE_EXISTS(this->env_, Layer::GetColumnPath(layer_dir_, 1));
  ASSERT_FILE_EXISTS(this->env_, Layer::GetColumnPath(layer_dir_, 2));
  ASSERT_FILE_EXISTS(this->env_, Layer::GetBloomPath(layer_dir_))
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
  ASSERT_TRUE(s.IsAlreadyPresent()) <<
    "expected AlreadyPresent, but got: " << s.ToString();

  ASSERT_EQ(1, this->TabletCount());
}

// Test iterating over a tablet which contains data
// in the memstore as well as two layers. This simple test
// only puts one row in each with no updates.
TYPED_TEST(TestTablet, TestRowIteratorSimple) {
  const int kInLayer1 = 1;
  const int kInLayer2 = 2;
  const int kInMemstore = 3;

  // Put a row in disk layer 1 (insert and flush)
  RowBuilder rb(this->schema_);
  this->setup_.BuildRow(&rb, kInLayer1);
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.data()));
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Put a row in disk layer 2 (insert and flush)
  rb.Reset();
  this->setup_.BuildRow(&rb, kInLayer2);
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.data()));
  ASSERT_STATUS_OK(this->tablet_->Flush());

  // Put a row in memstore
  rb.Reset();
  this->setup_.BuildRow(&rb, kInMemstore);
  ASSERT_STATUS_OK(this->tablet_->Insert(rb.data()));

  // Now iterate the tablet and make sure the rows show up
  scoped_ptr<Tablet::RowIterator> iter;
  ASSERT_STATUS_OK(this->tablet_->NewRowIterator(this->schema_, &iter));
  ASSERT_TRUE(iter->HasNext());

  scoped_array<uint8_t> buf(new uint8_t[this->schema_.byte_size() * 100]);
  RowBlock block(this->schema_, &buf[0], 100, &this->arena_);

  // First call to CopyNextRows should fetch the whole memstore.
  size_t n = 100;
  ASSERT_STATUS_OK(iter->CopyNextRows(&n, &block));
  ASSERT_EQ(1, n) << "should get only the one row from memstore";
  this->setup_.VerifyRow(&buf[0], kInMemstore);

  // Next, should fetch the older layer
  ASSERT_TRUE(iter->HasNext());
  n = 100;
  ASSERT_STATUS_OK(iter->CopyNextRows(&n, &block));
  ASSERT_EQ(1, n) << "should get only the one row from layer 1";
  this->setup_.VerifyRow(&buf[0], kInLayer1);

  // Next, should fetch the newer layer
  ASSERT_TRUE(iter->HasNext());
  n = 100;
  ASSERT_STATUS_OK(iter->CopyNextRows(&n, &block));
  ASSERT_EQ(1, n) << "should get only the one row from layer 2";
  this->setup_.VerifyRow(&buf[0], kInLayer2);

  ASSERT_FALSE(iter->HasNext());
}

// Test iterating over a tablet which has a memstore
// and several layers, each with many rows of data.
TYPED_TEST(TestTablet, TestRowIteratorComplex) {
  // Put a row in disk layer 1 (insert and flush)
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

  // At this point, we should have several layers as well
  // as some data in memstore.

  // Update a subset of the rows
  ScopedRowDelta update(this->schema_);
  for (uint32_t i = 0; i < 1000; i += 15) {
    uint32_t new_val = 10000 + i;
    ASSERT_STATUS_OK_FAST(
      this->setup_.DoUpdate(this->tablet_.get(), &update, i, new_val));
    inserted.erase(i);
    inserted.insert(new_val);
  }

  // Now iterate the tablet and make sure the rows show up.
  scoped_ptr<Tablet::RowIterator> iter;
  ASSERT_STATUS_OK(this->tablet_->NewRowIterator(this->schema_, &iter));
  scoped_array<uint8_t> buf(new uint8_t[this->schema_.byte_size() * 100]);
  RowBlock block(this->schema_, &buf[0], 100, &this->arena_);

  // Copy schema into local scope, since gcc is getting confused by
  // too many templates.
  const Schema &schema = this->schema_;

  while (iter->HasNext()) {
    this->arena_.Reset();
    size_t n = 100;
    ASSERT_STATUS_OK(iter->CopyNextRows(&n, &block));
    LOG(INFO) << "Fetched batch of " << n;
    for (size_t i = 0; i < n; i++) {
      const char *row_ptr = reinterpret_cast<const char *>(
        &buf[schema.byte_size() * i]);
      Slice row_slice(row_ptr, schema.byte_size());
      uint32_t val_read = *schema.ExtractColumnFromRow<UINT32>(row_slice, 1);
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
  this->tablet_.reset(new Tablet(this->schema_, this->test_dir_));
  ASSERT_STATUS_OK(this->tablet_->Open());

  // Ensure that rows exist
  this->VerifyTestRows(0, 1000);
  ASSERT_EQ(1000, this->TabletCount());

  // TODO: add some more data, re-flush
}

TYPED_TEST(TestTablet, TestCompaction) {
  // Create three layers by inserting and flushing
  {
    this->InsertTestRows(0, 1000);
    ASSERT_STATUS_OK(this->tablet_->Flush());
    string layer_dir_ = Tablet::GetLayerPath(this->test_dir_, 0);
    ASSERT_FILE_EXISTS(this->env_, Layer::GetColumnPath(layer_dir_, 0));
  }

  {
    this->InsertTestRows(1000, 1000);
    ASSERT_STATUS_OK(this->tablet_->Flush());
    string layer_dir_ = Tablet::GetLayerPath(this->test_dir_, 1);
    ASSERT_FILE_EXISTS(this->env_, Layer::GetColumnPath(layer_dir_, 0));
  }

  {
    this->InsertTestRows(2000, 1000);
    ASSERT_STATUS_OK(this->tablet_->Flush());
    string layer_dir_ = Tablet::GetLayerPath(this->test_dir_, 2);
    ASSERT_FILE_EXISTS(this->env_, Layer::GetColumnPath(layer_dir_, 0));
  }

  // Issue compaction
  {
    ASSERT_STATUS_OK(this->tablet_->Compact());
    ASSERT_EQ(3000, this->TabletCount());
    string layer_dir_ = Tablet::GetLayerPath(this->test_dir_, 3);
    ASSERT_FILE_EXISTS(this->env_, Layer::GetColumnPath(layer_dir_, 0));
    ASSERT_FILE_EXISTS(this->env_, Layer::GetBloomPath(layer_dir_))
  }

  // Old layers should not exist anymore
  for (int i = 0; i <= 2; i++) {
    string layer_dir_ = Tablet::GetLayerPath(this->test_dir_, i);
    ASSERT_FILE_NOT_EXISTS(this->env_, Layer::GetColumnPath(layer_dir_, 0));
  }
}

} // namespace tablet
} // namespace kudu


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
