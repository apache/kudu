// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <time.h>

#include "common/row.h"
#include "common/schema.h"
#include "gutil/stringprintf.h"
#include "tablet/diskrowset.h"
#include "tablet/diskrowset-test-base.h"
#include "tablet/tablet-test-util.h"
#include "util/env.h"
#include "util/status.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

DEFINE_double(update_fraction, 0.1f, "fraction of rows to update");

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;


// TODO: add test which calls CopyNextRows on an iterator with no more
// rows - i think it segfaults!

// Test round-trip writing and reading back a layer with
// multiple columns. Does not test any modifications.
TEST_F(TestLayer, TestLayerRoundTrip) {
  WriteTestLayer();

  // Now open the Layer for read
  shared_ptr<Layer> l;
  ASSERT_STATUS_OK(OpenTestLayer(&l));

  // First iterate over all columns
  LOG_TIMING(INFO, "Iterating over all columns") {
    IterateProjection(*l, schema_, n_rows_);
  }

  // Now iterate only over the key column
  Schema proj_key(boost::assign::list_of
                  (ColumnSchema("key", STRING)),
                  1);

  LOG_TIMING(INFO, "Iterating over only key column") {
    IterateProjection(*l, proj_key, n_rows_);
  }


  // Now iterate only over the non-key column
  Schema proj_val(boost::assign::list_of
                  (ColumnSchema("val", UINT32)),
                  1);
  LOG_TIMING(INFO, "Iterating over only val column") {
    IterateProjection(*l, proj_val, n_rows_);
  }

  // Test that CheckRowPresent returns correct results

  // 1. Check a key which comes before all keys in layer
  {
    Slice key("h");
    LayerKeyProbe probe(schema_, &key);
    bool present;
    ASSERT_STATUS_OK(l->CheckRowPresent(probe, &present));
    ASSERT_FALSE(present);
  }

  // 2. Check a key which comes after all keys in layer
  {
    Slice key("z");
    LayerKeyProbe probe(schema_, &key);
    bool present;
    ASSERT_STATUS_OK(l->CheckRowPresent(probe, &present));
    ASSERT_FALSE(present);
  }

  // 3. Check a key which is not present, but comes between present
  // keys
  {
    Slice key("hello 00000000000049x");
    LayerKeyProbe probe(schema_, &key);
    bool present;
    ASSERT_STATUS_OK(l->CheckRowPresent(probe, &present));
    ASSERT_FALSE(present);
  }

  // 4. Check a key which is present
  {
    char buf[256];
    FormatKey(49, buf, sizeof(buf));
    Slice key(buf);
    LayerKeyProbe probe(schema_, &key);
    bool present;
    ASSERT_STATUS_OK(l->CheckRowPresent(probe, &present));
    ASSERT_TRUE(present);
  }
}

// Test writing a layer, and then updating some rows in it.
TEST_F(TestLayer, TestLayerUpdate) {
  WriteTestLayer();

  // Now open the Layer for read
  shared_ptr<Layer> l;
  ASSERT_STATUS_OK(OpenTestLayer(&l));

  // Add an update to the delta tracker for a number of keys
  // which exist. These updates will change the value to
  // equal idx*5 (whereas in the original data, value = idx)
  unordered_set<uint32_t> updated;
  UpdateExistingRows(l.get(), FLAGS_update_fraction, &updated);
  ASSERT_EQ((int)(n_rows_ * FLAGS_update_fraction),
            l->delta_tracker_->dms_->Count());

  // Try to add an update for a key not in the file (but which falls
  // between two valid keys)
  txid_t txid(0);
  Slice bad_key = Slice("hello 00000000000049x");
  Status s = l->UpdateRow(txid, &bad_key, RowChangeList(Slice()));
  ASSERT_TRUE(s.IsNotFound());

  // Now read back the value column, and verify that the updates
  // are visible.
  VerifyUpdates(*l, updated);
}

TEST_F(TestLayer, TestDMSFlush) {
  WriteTestLayer();

  unordered_set<uint32_t> updated;

  // Now open the Layer for read
  {
    shared_ptr<Layer> l;
    ASSERT_STATUS_OK(OpenTestLayer(&l));

    // Add an update to the delta tracker for a number of keys
    // which exist. These updates will change the value to
    // equal idx*5 (whereas in the original data, value = idx)
    UpdateExistingRows(l.get(), FLAGS_update_fraction, &updated);
    ASSERT_EQ((int)(n_rows_ * FLAGS_update_fraction),
              l->delta_tracker_->dms_->Count());

    l->FlushDeltas();

    // Check that the Layer's DMS has now been emptied.
    ASSERT_EQ(0, l->delta_tracker_->dms_->Count());

    // Now read back the value column, and verify that the updates
    // are visible.
    VerifyUpdates(*l, updated);
  }

  // Close and re-open the layer and ensure that the updates were
  // persistent.
  {
    shared_ptr<Layer> l;
    ASSERT_STATUS_OK(OpenTestLayer(&l));

    // Now read back the value column, and verify that the updates
    // are visible.
    VerifyUpdates(*l, updated);
  }
}

// Test that when a single row is updated multiple times, we can query the
// historical values using MVCC, even after it is flushed.
TEST_F(TestLayer, TestFlushedUpdatesRespectMVCC) {
  const Slice key_slice("row");

  // Write a single row into a new Layer.
  LOG_TIMING(INFO, "Writing layer") {
    LayerWriter lw(env_.get(), schema_, layer_dir_,
                   BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f));

    ASSERT_STATUS_OK(lw.Open());

    RowBuilder rb(schema_);
    rb.AddString(key_slice);
    rb.AddUint32(1);
    ASSERT_STATUS_OK_FAST(lw.WriteRow(rb.data()));
    ASSERT_STATUS_OK(lw.Finish());
  }


  // Reopen the layer.
  shared_ptr<Layer> l;
  ASSERT_STATUS_OK(OpenTestLayer(&l));

  // Take a snapshot of the pre-update state.
  vector<MvccSnapshot> snaps;
  snaps.push_back(MvccSnapshot(mvcc_));


  // Update the single row multiple times, taking an MVCC snapshot
  // after each update.
  faststring update_buf;
  RowChangeListEncoder update(schema_, &update_buf);
  for (uint32_t i = 2; i <= 5; i++) {
    {
      ScopedTransaction tx(&mvcc_);
      update_buf.clear();
      update.AddColumnUpdate(1, &i);
      ASSERT_STATUS_OK_FAST(l->UpdateRow(tx.txid(),
                                         &key_slice, RowChangeList(update_buf)));
    }
    snaps.push_back(MvccSnapshot(mvcc_));
  }

  // Ensure that MVCC is respected by reading the value at each of the stored
  // snapshots.
  ASSERT_EQ(5, snaps.size());
  for (int i = 0; i < 5; i++) {
    SCOPED_TRACE(i);
    gscoped_ptr<RowwiseIterator> iter(l->NewRowIterator(schema_, snaps[i]));
    string data = InitAndDumpIterator(iter.Pass());
    EXPECT_EQ(StringPrintf("(string key=row, uint32 val=%d)", i + 1), data);
  }

  // Flush deltas to disk and ensure that the historical versions are still
  // accessible.
  ASSERT_STATUS_OK(l->FlushDeltas());

  for (int i = 0; i < 5; i++) {
    SCOPED_TRACE(i);
    gscoped_ptr<RowwiseIterator> iter(l->NewRowIterator(schema_, snaps[i]));
    string data = InitAndDumpIterator(iter.Pass());
    EXPECT_EQ(StringPrintf("(string key=row, uint32 val=%d)", i + 1), data);
  }

}

// Similar to TestDMSFlush above, except does not actually verify
// the results (since the verification step is expensive). Additionally,
// loops the "read" side of the benchmark a number of times, so that
// the speed of applying deltas during read can be micro-benchmarked.
//
// This is most usefully run with an invocation like:
// ./layer-test --gtest_filter=\*Performance --roundtrip_num_rows=1000000
//    --n_read_passes=1000 --update_fraction=0.01
TEST_F(TestLayer, TestDeltaApplicationPerformance) {
  WriteTestLayer();

  // Now open the Layer for read
  {
    shared_ptr<Layer> l;
    ASSERT_STATUS_OK(OpenTestLayer(&l));

    BenchmarkIterationPerformance(*l.get(),
      StringPrintf("Reading %zd rows prior to updates %d times",
                   n_rows_, FLAGS_n_read_passes));

    UpdateExistingRows(l.get(), FLAGS_update_fraction, NULL);

    BenchmarkIterationPerformance(*l.get(),
      StringPrintf("Reading %zd rows with %.2f%% updates %d times (updates in DMS)",
                   n_rows_, FLAGS_update_fraction * 100.0f,
                   FLAGS_n_read_passes));
    l->FlushDeltas();

    BenchmarkIterationPerformance(*l.get(),
      StringPrintf("Reading %zd rows with %.2f%% updates %d times (updates on disk)",
                   n_rows_, FLAGS_update_fraction * 100.0f,
                   FLAGS_n_read_passes));
  }
}

} // namespace tablet
} // namespace kudu
