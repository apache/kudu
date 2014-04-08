// Copyright (c) 2012, Cloudera, inc.

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <time.h>

#include "common/row.h"
#include "common/schema.h"
#include "gutil/algorithm.h"
#include "gutil/stringprintf.h"
#include "tablet/delta_compaction.h"
#include "tablet/diskrowset.h"
#include "tablet/diskrowset-test-base.h"
#include "tablet/tablet-test-util.h"
#include "util/env.h"
#include "util/status.h"
#include "util/stopwatch.h"
#include "util/test_macros.h"

DEFINE_double(update_fraction, 0.1f, "fraction of rows to update");
DECLARE_int32(cfile_default_block_size);

namespace kudu {
namespace tablet {

using std::tr1::unordered_set;
using util::gtl::is_sorted;
using metadata::RowSetMetadata;

// TODO: add test which calls CopyNextRows on an iterator with no more
// rows - i think it segfaults!

// Test round-trip writing and reading back a rowset with
// multiple columns. Does not test any modifications.
TEST_F(TestRowSet, TestRowSetRoundTrip) {
  WriteTestRowSet();

  // Now open the DiskRowSet for read
  shared_ptr<DiskRowSet> rs;
  ASSERT_STATUS_OK(OpenTestRowSet(&rs));

  // First iterate over all columns
  LOG_TIMING(INFO, "Iterating over all columns") {
    IterateProjection(*rs, schema_, n_rows_);
  }

  // Now iterate only over the key column
  Schema proj_key(boost::assign::list_of
                  (ColumnSchema("key", STRING)),
                  1);

  LOG_TIMING(INFO, "Iterating over only key column") {
    IterateProjection(*rs, proj_key, n_rows_);
  }


  // Now iterate only over the non-key column
  Schema proj_val(boost::assign::list_of
                  (ColumnSchema("val", UINT32)),
                  1);
  LOG_TIMING(INFO, "Iterating over only val column") {
    IterateProjection(*rs, proj_val, n_rows_);
  }

  // Test that CheckRowPresent returns correct results
  ProbeStats stats;

  // 1. Check a key which comes before all keys in rowset
  {
    RowBuilder rb(schema_.CreateKeyProjection());
    rb.AddString(Slice("h"));
    RowSetKeyProbe probe(rb.row());
    bool present;
    ASSERT_STATUS_OK(rs->CheckRowPresent(probe, &present, &stats));
    ASSERT_FALSE(present);
  }

  // 2. Check a key which comes after all keys in rowset
  {
    RowBuilder rb(schema_.CreateKeyProjection());
    rb.AddString(Slice("z"));
    RowSetKeyProbe probe(rb.row());
    bool present;
    ASSERT_STATUS_OK(rs->CheckRowPresent(probe, &present, &stats));
    ASSERT_FALSE(present);
  }

  // 3. Check a key which is not present, but comes between present
  // keys
  {
    RowBuilder rb(schema_.CreateKeyProjection());
    rb.AddString(Slice("hello 00000000000049x"));
    RowSetKeyProbe probe(rb.row());
    bool present;
    ASSERT_STATUS_OK(rs->CheckRowPresent(probe, &present, &stats));
    ASSERT_FALSE(present);
  }

  // 4. Check a key which is present
  {
    char buf[256];
    RowBuilder rb(schema_.CreateKeyProjection());
    FormatKey(49, buf, sizeof(buf));
    rb.AddString(Slice(buf));
    RowSetKeyProbe probe(rb.row());
    bool present;
    ASSERT_STATUS_OK(rs->CheckRowPresent(probe, &present, &stats));
    ASSERT_TRUE(present);
  }
}

// Test writing a rowset, and then updating some rows in it.
TEST_F(TestRowSet, TestRowSetUpdate) {
  WriteTestRowSet();

  // Now open the DiskRowSet for read
  shared_ptr<DiskRowSet> rs;
  ASSERT_STATUS_OK(OpenTestRowSet(&rs));

  // Add an update to the delta tracker for a number of keys
  // which exist. These updates will change the value to
  // equal idx*5 (whereas in the original data, value = idx)
  unordered_set<uint32_t> updated;
  UpdateExistingRows(rs.get(), FLAGS_update_fraction, &updated);
  ASSERT_EQ(static_cast<int>(n_rows_ * FLAGS_update_fraction),
            rs->delta_tracker_->dms_->Count());

  // Try to add a mutation for a key not in the file (but which falls
  // between two valid keys)
  faststring buf;
  RowChangeListEncoder enc(schema_, &buf);
  enc.SetToDelete();

  Timestamp timestamp(0);
  RowBuilder rb(schema_.CreateKeyProjection());
  rb.AddString(Slice("hello 00000000000049x"));
  RowSetKeyProbe probe(rb.row());

  OperationResultPB result;
  ProbeStats stats;
  Status s = rs->MutateRow(timestamp, probe, enc.as_changelist(), op_id_, &stats, &result);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(0, result.mutated_stores_size());

  // Now read back the value column, and verify that the updates
  // are visible.
  VerifyUpdates(*rs, updated);
}

// Test Delete() support within a DiskRowSet.
TEST_F(TestRowSet, TestDelete) {
  // Write and open a DiskRowSet with 2 rows.
  WriteTestRowSet(2);
  shared_ptr<DiskRowSet> rs;
  ASSERT_STATUS_OK(OpenTestRowSet(&rs));
  MvccSnapshot snap_before_delete(mvcc_);

  // Delete one of the two rows
  OperationResultPB result;
  ASSERT_STATUS_OK(DeleteRow(rs.get(), 0, &result));
  ASSERT_EQ(1, result.mutated_stores_size());
  ASSERT_EQ(0L, result.mutated_stores(0).rs_id());
  ASSERT_EQ(0L, result.mutated_stores(0).delta_id());
  MvccSnapshot snap_after_delete(mvcc_);

  vector<string> rows;
  Status s;

  for (int i = 0; i < 2; i++) {
    // Reading the MVCC snapshot prior to deletion should show the row.
    ASSERT_STATUS_OK(DumpRowSet(*rs, schema_, snap_before_delete, &rows));
    ASSERT_EQ(2, rows.size());
    EXPECT_EQ("(string key=hello 000000000000000, uint32 val=0)", rows[0]);
    EXPECT_EQ("(string key=hello 000000000000001, uint32 val=1)", rows[1]);

    // Reading the MVCC snapshot after the deletion should hide the row.
    ASSERT_STATUS_OK(DumpRowSet(*rs, schema_, snap_after_delete, &rows));
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ("(string key=hello 000000000000001, uint32 val=1)", rows[0]);

    // Trying to delete or update the same row again should fail.
    OperationResultPB result;
    s = DeleteRow(rs.get(), 0, &result);
    ASSERT_TRUE(s.IsNotFound()) << "bad status: " << s.ToString();
    ASSERT_EQ(0, result.mutated_stores_size());
    result.Clear();
    s = UpdateRow(rs.get(), 0, 12345, &result);
    ASSERT_TRUE(s.IsNotFound()) << "bad status: " << s.ToString();
    ASSERT_EQ(0, result.mutated_stores_size());

    // CheckRowPresent should return false.
    bool present;
    ASSERT_STATUS_OK(CheckRowPresent(*rs, 0, &present));
    EXPECT_FALSE(present);

    if (i == 1) {
      // Flush DMS. The second pass through the loop will re-verify that the
      // externally visible state of the layer has not changed.
      // deletions now in a DeltaFile.
      ASSERT_STATUS_OK(rs->FlushDeltas());
    }
  }
}


TEST_F(TestRowSet, TestDMSFlush) {
  WriteTestRowSet();

  unordered_set<uint32_t> updated;

  // Now open the DiskRowSet for read
  {
    shared_ptr<DiskRowSet> rs;
    ASSERT_STATUS_OK(OpenTestRowSet(&rs));

    // Add an update to the delta tracker for a number of keys
    // which exist. These updates will change the value to
    // equal idx*5 (whereas in the original data, value = idx)
    UpdateExistingRows(rs.get(), FLAGS_update_fraction, &updated);
    ASSERT_EQ(static_cast<int>(n_rows_ * FLAGS_update_fraction),
              rs->delta_tracker_->dms_->Count());

    ASSERT_STATUS_OK(rs->FlushDeltas());

    // Check that the DiskRowSet's DMS has now been emptied.
    ASSERT_EQ(0, rs->delta_tracker_->dms_->Count());

    // Now read back the value column, and verify that the updates
    // are visible.
    VerifyUpdates(*rs, updated);
  }

  // Close and re-open the rowset and ensure that the updates were
  // persistent.
  {
    shared_ptr<DiskRowSet> rs;
    ASSERT_STATUS_OK(OpenTestRowSet(&rs));

    // Now read back the value column, and verify that the updates
    // are visible.
    VerifyUpdates(*rs, updated);
  }
}

// Test that when a single row is updated multiple times, we can query the
// historical values using MVCC, even after it is flushed.
TEST_F(TestRowSet, TestFlushedUpdatesRespectMVCC) {
  const Slice key_slice("row");

  // Write a single row into a new DiskRowSet.
  LOG_TIMING(INFO, "Writing rowset") {
    DiskRowSetWriter drsw(rowset_meta_.get(),
                   BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f));

    ASSERT_STATUS_OK(drsw.Open());

    RowBuilder rb(schema_);
    rb.AddString(key_slice);
    rb.AddUint32(1);
    ASSERT_STATUS_OK_FAST(WriteRow(rb.data(), &drsw));
    ASSERT_STATUS_OK(drsw.Finish());
  }


  // Reopen the rowset.
  shared_ptr<DiskRowSet> rs;
  ASSERT_STATUS_OK(OpenTestRowSet(&rs));

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
      update.Reset();
      update.AddColumnUpdate(1, &i);
      RowBuilder rb(schema_.CreateKeyProjection());
      rb.AddString(key_slice);
      RowSetKeyProbe probe(rb.row());
      OperationResultPB result;
      ProbeStats stats;
      ASSERT_STATUS_OK_FAST(rs->MutateRow(tx.timestamp(),
                                          probe,
                                          RowChangeList(update_buf),
                                          op_id_,
                                          &stats,
                                          &result));
      ASSERT_EQ(1, result.mutated_stores_size());
      ASSERT_EQ(0L, result.mutated_stores(0).rs_id());
      ASSERT_EQ(0L, result.mutated_stores(0).delta_id());
    }
    snaps.push_back(MvccSnapshot(mvcc_));
  }

  // Ensure that MVCC is respected by reading the value at each of the stored
  // snapshots.
  ASSERT_EQ(5, snaps.size());
  for (int i = 0; i < 5; i++) {
    SCOPED_TRACE(i);
    gscoped_ptr<RowwiseIterator> iter(rs->NewRowIterator(&schema_, snaps[i]));
    string data = InitAndDumpIterator(iter.Pass());
    EXPECT_EQ(StringPrintf("(string key=row, uint32 val=%d)", i + 1), data);
  }

  // Flush deltas to disk and ensure that the historical versions are still
  // accessible.
  ASSERT_STATUS_OK(rs->FlushDeltas());

  for (int i = 0; i < 5; i++) {
    SCOPED_TRACE(i);
    gscoped_ptr<RowwiseIterator> iter(rs->NewRowIterator(&schema_, snaps[i]));
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
// ./rowset-test --gtest_filter=\*Performance --roundtrip_num_rows=1000000
//    --n_read_passes=1000 --update_fraction=0.01
TEST_F(TestRowSet, TestDeltaApplicationPerformance) {
  WriteTestRowSet();

  // Now open the DiskRowSet for read
  {
    shared_ptr<DiskRowSet> rs;
    ASSERT_STATUS_OK(OpenTestRowSet(&rs));

    BenchmarkIterationPerformance(*rs.get(),
      StringPrintf("Reading %zd rows prior to updates %d times",
                   n_rows_, FLAGS_n_read_passes));

    UpdateExistingRows(rs.get(), FLAGS_update_fraction, NULL);

    BenchmarkIterationPerformance(*rs.get(),
      StringPrintf("Reading %zd rows with %.2f%% updates %d times (updates in DMS)",
                   n_rows_, FLAGS_update_fraction * 100.0f,
                   FLAGS_n_read_passes));
    ASSERT_STATUS_OK(rs->FlushDeltas());

    BenchmarkIterationPerformance(*rs.get(),
      StringPrintf("Reading %zd rows with %.2f%% updates %d times (updates on disk)",
                   n_rows_, FLAGS_update_fraction * 100.0f,
                   FLAGS_n_read_passes));
  }
}

TEST_F(TestRowSet, TestRollingDiskRowSetWriter) {
  // Set small block size so that we can roll frequently. Otherwise
  // we couldn't output such small files.
  google::FlagSaver saver;
  FLAGS_cfile_default_block_size = 4096;

  RollingDiskRowSetWriter writer(tablet_->metadata(), schema_,
                                 BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f),
                                 64 * 1024); // roll every 64KB
  DoWriteTestRowSet(10000, &writer);

  // Should have rolled 4 times.
  vector<shared_ptr<RowSetMetadata> > metas;
  writer.GetWrittenRowSetMetadata(&metas);
  EXPECT_EQ(4, metas.size());
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, metas) {
    ASSERT_TRUE(meta->HasColumnDataBlockForTests(0));
  }
}

TEST_F(TestRowSet, TestMakeDeltaCompactionInput) {
  WriteTestRowSet();

  // Now open the DiskRowSet for read
  shared_ptr<DiskRowSet> rs;
  ASSERT_STATUS_OK(OpenTestRowSet(&rs));
  UpdateExistingRows(rs.get(), FLAGS_update_fraction, NULL);
  ASSERT_STATUS_OK(rs->FlushDeltas());
  DeltaTracker *dt = rs->delta_tracker();
  int num_stores = dt->redo_delta_stores_.size();
  vector<shared_ptr<DeltaStore> > compacted_stores;
  vector<int64_t> compacted_ids;
  gscoped_ptr<DeltaCompactionInput> dci;
  ASSERT_STATUS_OK(dt->MakeCompactionInput(0, num_stores - 1, &compacted_stores,
                                           &compacted_ids, &dci));
  vector<string> results;
  ASSERT_STATUS_OK(DebugDumpDeltaCompactionInput(dci.get(), &results, schema_));
  BOOST_FOREACH(const string &str, results) {
    VLOG(1) << str;
  }
  ASSERT_EQ(compacted_stores.size(), num_stores);
  ASSERT_EQ(compacted_ids.size(), num_stores);
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
}

TEST_F(TestRowSet, TestCompactStores) {
  WriteTestRowSet();
  shared_ptr<DiskRowSet> rs;
  ASSERT_STATUS_OK(OpenTestRowSet(&rs));

  // Generate 3 deltafiles
  UpdateExistingRows(rs.get(), FLAGS_update_fraction, NULL);
  ASSERT_STATUS_OK(rs->FlushDeltas());
  UpdateExistingRows(rs.get(), FLAGS_update_fraction, NULL);
  ASSERT_STATUS_OK(rs->FlushDeltas());
  UpdateExistingRows(rs.get(), FLAGS_update_fraction, NULL);
  ASSERT_STATUS_OK(rs->FlushDeltas());

  // Compact the deltafiles
  DeltaTracker *dt = rs->delta_tracker();
  int num_stores = dt->redo_delta_stores_.size();
  VLOG(1) << "Number of stores before compaction: " << num_stores;
  ASSERT_EQ(num_stores, 3);
  ASSERT_STATUS_OK(dt->CompactStores(0, num_stores - 1));
  num_stores = dt->redo_delta_stores_.size();
  VLOG(1) << "Number of stores after compaction: " << num_stores;
  ASSERT_EQ(1,  num_stores);

  // Verify that the resulting deltafile is valid
  vector<shared_ptr<DeltaStore> > compacted_stores;
  vector<int64_t> compacted_ids;
  gscoped_ptr<DeltaCompactionInput> dci;
  ASSERT_STATUS_OK(dt->MakeCompactionInput(0, num_stores - 1, &compacted_stores,
                                           &compacted_ids, &dci));
  vector<string> results;
  ASSERT_STATUS_OK(DebugDumpDeltaCompactionInput(dci.get(), &results, schema_));
  BOOST_FOREACH(const string &str, results) {
    VLOG(1) << str;
  }
  ASSERT_TRUE(is_sorted(results.begin(), results.end()));
}

} // namespace tablet
} // namespace kudu
