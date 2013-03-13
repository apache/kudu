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

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;

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

static void ApplyUpdates(DeltaMemStore *dms,
                         uint32_t row_idx,
                         size_t col_idx,
                         ColumnBlock *cb) {
  ColumnSchema col_schema(dms->schema().column(col_idx));
  Schema single_col_projection(boost::assign::list_of(col_schema), 0);

  gscoped_ptr<DeltaIteratorInterface> iter(
    dms->NewDeltaIterator(single_col_projection));
  ASSERT_STATUS_OK(iter->Init());
  ASSERT_STATUS_OK(iter->SeekToOrdinal(row_idx));

  RowBlock block(single_col_projection, reinterpret_cast<uint8_t *>(cb->data()),
                 cb->size(), cb->arena());
  ASSERT_STATUS_OK(iter->PrepareToApply(&block));
  ASSERT_STATUS_OK(iter->ApplyUpdates(&block, 0));
  
}

TEST(TestDeltaMemStore, TestDMSSparseUpdates) {
  Schema schema(boost::assign::list_of
                (ColumnSchema("col1", UINT32)),
                1);

  shared_ptr<DeltaMemStore> dms(new DeltaMemStore(schema));
  faststring buf;
  RowChangeListEncoder update(schema, &buf);

  int n_rows = 1000;

  // Update 100 random rows out of the 1000.
  srand(12345);
  unordered_set<uint32_t> indexes_to_update;
  GenerateRandomIndexes(n_rows, 100, &indexes_to_update);
  BOOST_FOREACH(uint32_t idx_to_update, indexes_to_update) {
    buf.clear();
    update.AddColumnUpdate(0, &idx_to_update);

    dms->Update(idx_to_update, RowChangeList(buf));
  }
  ASSERT_EQ(100, dms->Count());

  // Now apply the updates from the DMS back to an array
  ScopedColumnBlock<UINT32> read_back(1000);
  for (int i = 0; i < 1000; i++) {
    read_back[i] = 0xDEADBEEF;
  }
  ApplyUpdates(dms.get(), 0, 0, &read_back);

  // And verify that only the rows that we updated are modified within
  // the array.
  for (int i = 0; i < 1000; i++) {
    // If this wasn't one of the ones we updated, expect our marker
    if (indexes_to_update.find(i) == indexes_to_update.end()) {
      // If this wasn't one of the ones we updated, expect our marker
      ASSERT_EQ(0xDEADBEEF, read_back[i]);
    } else {
      // Otherwise expect the updated value
      ASSERT_EQ(i, read_back[i]);
    }
  }
}

// Test when a slice column has been updated multiple times in the
// memstore that the referred to values properly end up in the
// right arena.
TEST(TestDeltaMemStore, TestReUpdateSlice) {
  Schema schema(boost::assign::list_of
                (ColumnSchema("col1", STRING)),
                1);
  shared_ptr<DeltaMemStore> dms(new DeltaMemStore(schema));
  faststring update_buf;
  RowChangeListEncoder update(schema, &update_buf);

  // Update a cell, taking care that the buffer we use to perform
  // the update gets cleared after usage. This ensures that the
  // underlying data is properly copied into the DMS arena.
  {
    char buf[256] = "update 1";
    Slice s(buf);
    update.AddColumnUpdate(0, &s);
    dms->Update(123, RowChangeList(update_buf));
    memset(buf, 0xff, sizeof(buf));
  }

  // Update the same cell again with a different value
  {
    char buf[256] = "update 2";
    Slice s(buf);
    update_buf.clear();
    update.AddColumnUpdate(0, &s);
    dms->Update(123, RowChangeList(update_buf));
    memset(buf, 0xff, sizeof(buf));
  }

  // Ensure we only ended up with one entry
  ASSERT_EQ(1, dms->Count());

  // Ensure that we ended up with the right data.
  ScopedColumnBlock<STRING> read_back(1);
  ApplyUpdates(dms.get(), 123, 0, &read_back);
  ASSERT_EQ("update 2", read_back[0].ToString());
}


TEST(TestDeltaMemStore, TestDMSBasic) {
  Schema schema(boost::assign::list_of
                 (ColumnSchema("col1", STRING))
                 (ColumnSchema("col2", STRING))
                 (ColumnSchema("col3", UINT32)),
                1);
  shared_ptr<DeltaMemStore> dms(new DeltaMemStore(schema));
  faststring update_buf;
  RowChangeListEncoder update(schema, &update_buf);

  char buf[256];

  for (uint32_t i = 0; i < 1000; i++) {
    update_buf.clear();

    uint32_t val = i * 10;
    update.AddColumnUpdate(2, &val);

    snprintf(buf, sizeof(buf), "hello %d", i);
    Slice s(buf);
    update.AddColumnUpdate(0, &s);

    dms->Update(i, RowChangeList(update_buf));
  }

  ASSERT_EQ(1000, dms->Count());

  // Read back the values and check correctness.
  ScopedColumnBlock<UINT32> read_back(1000);
  ScopedColumnBlock<STRING> read_back_slices(1000);
  ApplyUpdates(dms.get(), 0, 2, &read_back);
  ApplyUpdates(dms.get(), 0, 0, &read_back_slices);

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


  // Update the same rows again, and ensure that no new
  // insertions happen
  for (uint32_t i = 0; i < 1000; i++) {
    update_buf.clear();

    uint32_t val = i * 20;
    update.AddColumnUpdate(2, &val);
    dms->Update(i, RowChangeList(update_buf));
  }

  ASSERT_EQ(1000, dms->Count());
}

TEST(TestDMSIterator, TestIteratorDoesUpdates) {
  // TODO: share some code with above test case
  Schema schema(boost::assign::list_of
                (ColumnSchema("col1", UINT32)),
                1);
  shared_ptr<DeltaMemStore> dms(new DeltaMemStore(schema));
  faststring update_buf;
  RowChangeListEncoder update(schema, &update_buf);

  for (uint32_t i = 0; i < 1000; i++) {
    update_buf.clear();
    uint32_t val = i * 10;
    update.AddColumnUpdate(0, &val);
    dms->Update(i, RowChangeList(update_buf));
  }
  ASSERT_EQ(1000, dms->Count());

  Arena arena(1024, 1024);
  ScopedRowBlock block(schema, 100, &arena);

  gscoped_ptr<DMSIterator> iter(down_cast<DMSIterator *>(dms->NewDeltaIterator(schema)));
  ASSERT_STATUS_OK(iter->Init(););

  int block_start_row = 50;
  ASSERT_STATUS_OK(iter->SeekToOrdinal(block_start_row));
  ASSERT_STATUS_OK(iter->PrepareToApply(&block));
  VLOG(1) << "prepared: " << Slice(iter->prepared_buf_).ToDebugString();

  ASSERT_STATUS_OK(iter->ApplyUpdates(&block, 0));

  for (int i = 0; i < 100; i++) {
    int actual_row = block_start_row + i;
    ASSERT_EQ(actual_row * 10, *reinterpret_cast<const uint32_t *>(block.row_ptr(i)));
  }

  // Apply the next block
  block_start_row += block.nrows();
  ASSERT_STATUS_OK(iter->PrepareToApply(&block));
  ASSERT_STATUS_OK(iter->ApplyUpdates(&block, 0));
  for (int i = 0; i < 100; i++) {
    int actual_row = block_start_row + i;
    ASSERT_EQ(actual_row * 10, *reinterpret_cast<const uint32_t *>(block.row_ptr(i)));
  }

  
}

} // namespace tabletype
} // namespace kudu
