// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <boost/scoped_array.hpp>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <tr1/unordered_set>

#include "common/schema.h"
#include "tablet/deltamemstore.h"
#include "util/hexdump.h"

namespace kudu {
namespace tablet {

using boost::scoped_array;
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

  ScopedRowDelta s_update(schema);
  RowDelta &update = s_update.get();

  int n_rows = 1000;

  // Update 100 random rows out of the 1000.
  srand(12345);
  unordered_set<uint32_t> indexes_to_update;
  GenerateRandomIndexes(n_rows, 100, &indexes_to_update);
  BOOST_FOREACH(uint32_t idx_to_update, indexes_to_update) {
    update.Clear(schema);
    update.UpdateColumn(schema, 0, &idx_to_update);

    dms.Update(idx_to_update, update);
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

  ScopedRowDelta s_update(schema);
  RowDelta &update = s_update.get();

  // Update a cell, taking care that the buffer we use to perform
  // the update gets cleared after usage. This ensures that the
  // underlying data is properly copied into the DMS arena.
  {
    char buf[256] = "update 1";
    Slice s(buf);
    update.UpdateColumn(schema, 0, &s);
    dms.Update(123, update);
    memset(buf, 0xff, sizeof(buf));
  }

  // Update the same cell again with a different value
  {
    char buf[256] = "update 2";
    Slice s(buf);
    update.UpdateColumn(schema, 0, &s);
    dms.Update(123, update);
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

  ScopedRowDelta s_update(schema);
  RowDelta &update = s_update.get();

  char buf[256];

  for (uint32_t i = 0; i < 1000; i++) {
    update.Clear(schema);

    uint32_t val = i * 10;
    update.UpdateColumn(schema, 2, &val);

    snprintf(buf, sizeof(buf), "hello %d", i);
    Slice s(buf);
    update.UpdateColumn(schema, 0, &s);

    dms.Update(i, update);
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
    ASSERT_EQ(i * 10, read_back[i]);
    snprintf(buf2, sizeof(buf2), "hello %d", i);
    Slice s(buf2);
    ASSERT_EQ(0, s.compare(read_back_slices[i]));
  }


  // Update the same rows again, and ensure that no new
  // insertions happen
  for (uint32_t i = 0; i < 1000; i++) {
    update.Clear(schema);

    uint32_t val = i * 20;
    update.UpdateColumn(schema, 2, &val);
    dms.Update(i, update);
  }

  ASSERT_EQ(1000, dms.Count());
}

TEST(TestDeltaMemStore, TestRowDelta) {
  Arena arena(1024, 1024*1024);

  Schema schema(boost::assign::list_of
                 (ColumnSchema("col1", STRING))
                 (ColumnSchema("col2", STRING))
                 (ColumnSchema("col3", UINT32)),
                 1);

  // Delta starts with no updates.
  ScopedRowDelta srd(schema);
  RowDelta &rd = srd.get();
  EXPECT_FALSE(rd.IsUpdated(0));
  EXPECT_FALSE(rd.IsUpdated(1));
  EXPECT_FALSE(rd.IsUpdated(2));

  // Trying to apply updates to a bad memory location
  // should not segfault, since there are no updates.
  rd.ApplyColumnUpdate(schema, 0, NULL);
  rd.ApplyColumnUpdate(schema, 1, NULL);
  rd.ApplyColumnUpdate(schema, 2, NULL);

  // Set an updated field.
  uint32_t new_int = 12345;
  rd.UpdateColumn(schema, 2, &new_int);
  EXPECT_FALSE(rd.IsUpdated(0));
  EXPECT_FALSE(rd.IsUpdated(1));
  EXPECT_TRUE (rd.IsUpdated(2));

  // Apply the update to a piece of memory
  // and ensure that the memory is mutated.
  uint32_t retrieved = 0;
  rd.ApplyColumnUpdate(schema, 2, &retrieved);
  EXPECT_EQ(new_int, retrieved);

  // Merge another set of updates with this one
  ScopedRowDelta srd2(schema);
  RowDelta &rd2 = srd2.get();

  Slice s("hello world");
  rd2.UpdateColumn(schema, 1, &s);
  EXPECT_TRUE(rd2.IsUpdated(1));

  new_int = 54321;
  rd2.UpdateColumn(schema, 2, &new_int);

  rd.MergeUpdatesFrom(schema, rd2, &arena);

  EXPECT_FALSE(rd.IsUpdated(0));
  EXPECT_TRUE (rd.IsUpdated(1));
  EXPECT_TRUE (rd.IsUpdated(2));

  // Test serialization
  faststring buf;
  rd.SerializeToBuffer(schema, &buf);
  ASSERT_EQ(
    "000000: 060b 6865 6c6c 6f20 776f 726c 6431 d400 ..hello world1..\n"
    "000010: 00                                      .\n",
    HexDump(buf));
}

} // namespace tabletype
} // namespace kudu
