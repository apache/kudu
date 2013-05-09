// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <tr1/unordered_set>

#include "common/schema.h"
#include "gutil/casts.h"
#include "gutil/gscoped_ptr.h"
#include "tablet/deltamemstore.h"
#include "util/test_macros.h"
#include "util/test_util.h"

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

class TestDeltaMemStore : public KuduTest {
 public:
  TestDeltaMemStore() :
    schema_(boost::assign::list_of
                 (ColumnSchema("col1", STRING))
                 (ColumnSchema("col2", STRING))
                 (ColumnSchema("col3", UINT32)),
            1),
    dms_(new DeltaMemStore(schema_))
  {}

  void UpdateIntsAtIndexes(const unordered_set<uint32_t> &indexes_to_update) {
    faststring buf;
    RowChangeListEncoder update(schema_, &buf);

    BOOST_FOREACH(uint32_t idx_to_update, indexes_to_update) {
      ScopedTransaction tx(&mvcc_);
      buf.clear();
      uint32_t new_val = idx_to_update * 10;
      update.AddColumnUpdate(kIntColumn, &new_val);

      dms_->Update(tx.txid(), idx_to_update, RowChangeList(buf));
    }
  }

  void ApplyUpdates(const MvccSnapshot &snapshot,
                    uint32_t row_idx,
                    size_t col_idx,
                    ColumnBlock *cb) {
    ColumnSchema col_schema(dms_->schema().column(col_idx));
    Schema single_col_projection(boost::assign::list_of(col_schema), 0);

    gscoped_ptr<DeltaIteratorInterface> iter(
      dms_->NewDeltaIterator(single_col_projection, snapshot));
    ASSERT_STATUS_OK(iter->Init());
    ASSERT_STATUS_OK(iter->SeekToOrdinal(row_idx));
    ASSERT_STATUS_OK(iter->PrepareBatch(cb->nrows()));
    ASSERT_STATUS_OK(iter->ApplyUpdates(0, cb));
  }


 protected:
  static const int kStringColumn = 1;
  static const int kIntColumn = 2;

  const Schema schema_;
  shared_ptr<DeltaMemStore> dms_;
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

// Test when a slice column has been updated multiple times in the
// memstore that the referred to values properly end up in the
// right arena.
TEST_F(TestDeltaMemStore, TestReUpdateSlice) {
  faststring update_buf;
  RowChangeListEncoder update(schema_, &update_buf);

  // Update a cell, taking care that the buffer we use to perform
  // the update gets cleared after usage. This ensures that the
  // underlying data is properly copied into the DMS arena.
  {
    ScopedTransaction tx(&mvcc_);
    char buf[256] = "update 1";
    Slice s(buf);
    update.AddColumnUpdate(0, &s);
    dms_->Update(tx.txid(), 123, RowChangeList(update_buf));
    memset(buf, 0xff, sizeof(buf));
  }
  MvccSnapshot snapshot_after_first_update(mvcc_);

  // Update the same cell again with a different value
  {
    ScopedTransaction tx(&mvcc_);
    char buf[256] = "update 2";
    Slice s(buf);
    update_buf.clear();
    update.AddColumnUpdate(0, &s);
    dms_->Update(tx.txid(), 123, RowChangeList(update_buf));
    memset(buf, 0xff, sizeof(buf));
  }
  MvccSnapshot snapshot_after_second_update(mvcc_);

  // Ensure we end up with a second entry for the cell, at the
  // new txid
  ASSERT_EQ(2, dms_->Count());

  // Ensure that we ended up with the right data, and that the old MVCC snapshot
  // yields the correct old value.
  ScopedColumnBlock<STRING> read_back(1);
  ApplyUpdates(snapshot_after_first_update, 123, 0, &read_back);
  ASSERT_EQ("update 1", read_back[0].ToString());

  ApplyUpdates(snapshot_after_second_update, 123, 0, &read_back);
  ASSERT_EQ("update 2", read_back[0].ToString());
}


TEST_F(TestDeltaMemStore, TestDMSBasic) {
  faststring update_buf;
  RowChangeListEncoder update(schema_, &update_buf);

  char buf[256];
  for (uint32_t i = 0; i < 1000; i++) {
    ScopedTransaction tx(&mvcc_);
    update_buf.clear();

    uint32_t val = i * 10;
    update.AddColumnUpdate(kIntColumn, &val);

    snprintf(buf, sizeof(buf), "hello %d", i);
    Slice s(buf);
    update.AddColumnUpdate(kStringColumn, &s);

    dms_->Update(tx.txid(), i, RowChangeList(update_buf));
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
    ScopedTransaction tx(&mvcc_);
    update_buf.clear();

    uint32_t val = i * 20;
    update.AddColumnUpdate(kIntColumn, &val);
    dms_->Update(tx.txid(), i, RowChangeList(update_buf));
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

  // TODO: test snapshot reads from different points
  MvccSnapshot snap(mvcc_);
  ScopedColumnBlock<UINT32> block(100);
  gscoped_ptr<DMSIterator> iter(down_cast<DMSIterator *>(dms_->NewDeltaIterator(schema_, snap)));
  ASSERT_STATUS_OK(iter->Init(););

  int block_start_row = 50;
  ASSERT_STATUS_OK(iter->SeekToOrdinal(block_start_row));
  ASSERT_STATUS_OK(iter->PrepareBatch(block.nrows()));
  VLOG(1) << "prepared: " << Slice(iter->prepared_buf_).ToDebugString();

  ASSERT_STATUS_OK(iter->ApplyUpdates(kIntColumn, &block));

  for (int i = 0; i < 100; i++) {
    int actual_row = block_start_row + i;
    ASSERT_EQ(actual_row * 10, block[i]) << "at row " << actual_row;
  }

  // Apply the next block
  block_start_row += block.nrows();
  ASSERT_STATUS_OK(iter->PrepareBatch(block.nrows()));
  ASSERT_STATUS_OK(iter->ApplyUpdates(kIntColumn, &block));
  for (int i = 0; i < 100; i++) {
    int actual_row = block_start_row + i;
    ASSERT_EQ(actual_row * 10, block[i]) << "at row " << actual_row;
  }
}

} // namespace tabletype
} // namespace kudu
