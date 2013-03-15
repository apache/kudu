// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <tr1/unordered_set>

#include "common/schema.h"
#include "gutil/gscoped_ptr.h"
#include "tablet/deltamemstore.h"
#include "util/hexdump.h"

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

TEST(TestDeltaMemStore, TestDMSSparseUpdates) {
  Schema schema(boost::assign::list_of
                (ColumnSchema("col1", UINT32)),
                1);

  DeltaMemStore dms(schema);
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

    dms.Update(idx_to_update, RowChangeList(buf));
  }
  ASSERT_EQ(100, dms.Count());

  // Now apply the updates from the DMS back to an array
  ScopedColumnBlock<UINT32> read_back(1000);
  for (int i = 0; i < 1000; i++) {
    read_back[i] = 0xDEADBEEF;
  }
  dms.ApplyUpdates(0, 0, &read_back);

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
  DeltaMemStore dms(schema);
  faststring update_buf;
  RowChangeListEncoder update(schema, &update_buf);

  // Update a cell, taking care that the buffer we use to perform
  // the update gets cleared after usage. This ensures that the
  // underlying data is properly copied into the DMS arena.
  {
    char buf[256] = "update 1";
    Slice s(buf);
    update.AddColumnUpdate(0, &s);
    dms.Update(123, RowChangeList(update_buf));
    memset(buf, 0xff, sizeof(buf));
  }

  // Update the same cell again with a different value
  {
    char buf[256] = "update 2";
    Slice s(buf);
    update_buf.clear();
    update.AddColumnUpdate(0, &s);
    dms.Update(123, RowChangeList(update_buf));
    memset(buf, 0xff, sizeof(buf));
  }

  // Ensure we only ended up with one entry
  ASSERT_EQ(1, dms.Count());

  // Ensure that we ended up with the right data.
  ScopedColumnBlock<STRING> read_back(1);
  dms.ApplyUpdates(0, 123, &read_back);
  ASSERT_EQ("update 2", read_back[0].ToString());
}


TEST(TestDeltaMemStore, TestDMSBasic) {
  Schema schema(boost::assign::list_of
                 (ColumnSchema("col1", STRING))
                 (ColumnSchema("col2", STRING))
                 (ColumnSchema("col3", UINT32)),
                1);
  DeltaMemStore dms(schema);
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

    dms.Update(i, RowChangeList(update_buf));
  }

  ASSERT_EQ(1000, dms.Count());

  // Read back the values and check correctness.
  ScopedColumnBlock<UINT32> read_back(1000);
  ScopedColumnBlock<STRING> read_back_slices(1000);
  dms.ApplyUpdates(2, 0, &read_back);
  dms.ApplyUpdates(0, 0, &read_back_slices);

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
    dms.Update(i, RowChangeList(update_buf));
  }

  ASSERT_EQ(1000, dms.Count());
}

} // namespace tabletype
} // namespace kudu
