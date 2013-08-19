// Copyright (c) 2013, Cloudera, inc.
// All rights reserved

#include <boost/assign/list_of.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "gutil/strings/util.h"
#include "tablet/compaction.h"
#include "tablet/tablet-test-util.h"
#include "util/stopwatch.h"
#include "util/test_util.h"

DEFINE_string(merge_benchmark_input_dir, "",
              "Directory to benchmark merge. The benchmark will merge "
              "all rowsets from this directory, pointed by the super-block "
              "with id 00000 or 1111 and tablet id 'KuduCompactionBenchTablet', "
              "if this is specified. Otherwise, inputs will "
              "be generated as part of the test itself.");
DEFINE_int32(merge_benchmark_num_rowsets, 3,
             "Number of rowsets as input to the merge");
DEFINE_int32(merge_benchmark_num_rows_per_rowset, 500000,
             "Number of rowsets as input to the merge");

namespace kudu {
namespace tablet {

using metadata::RowSetMetadata;

static const char *kRowKeyFormat = "hello %08d";

class TestCompaction : public KuduRowSetTest {
 public:
  TestCompaction() :
    KuduRowSetTest(Schema(boost::assign::list_of
            (ColumnSchema("key", STRING))
            (ColumnSchema("val", UINT32)),
            1)),
    row_builder_(schema_)
  {}

  // Insert n_rows rows of data.
  // Each row is the tuple: (string key=hello <n*10 + delta>, val=<n>)
  void InsertRows(MemRowSet *mrs, int n_rows, int delta) {
    for (uint32_t i = 0; i < n_rows; i++) {
      InsertRow(mrs, i * 10 + delta, i);
    }
  }

  void InsertRow(MemRowSet *mrs, int row_key, uint32_t val) {
    ScopedTransaction tx(&mvcc_);
    row_builder_.Reset();
    snprintf(key_buf_, sizeof(key_buf_), kRowKeyFormat, row_key);
    row_builder_.AddString(Slice(key_buf_));
    row_builder_.AddUint32(val);
    ASSERT_STATUS_OK_FAST(mrs->Insert(tx.txid(), row_builder_.row()));
  }

  // Update n_rows rows of data.
  // Each row has the key (string key=hello <n*10 + delta>) and its 'val' column
  // is set to new_val.
  void UpdateRows(RowSet *rowset, int n_rows, int delta, uint32_t new_val) {
    char keybuf[256];
    faststring update_buf;
    for (uint32_t i = 0; i < n_rows; i++) {
      SCOPED_TRACE(i);
      ScopedTransaction tx(&mvcc_);
      snprintf(keybuf, sizeof(keybuf), kRowKeyFormat, i * 10 + delta);

      update_buf.clear();
      RowChangeListEncoder update(schema_, &update_buf);
      update.AddColumnUpdate(1, &new_val);

      RowBuilder rb(schema_.CreateKeyProjection());
      rb.AddString(Slice(keybuf));
      RowSetKeyProbe probe(rb.row());
      ASSERT_STATUS_OK(rowset->MutateRow(tx.txid(), probe, RowChangeList(update_buf)));
    }
  }

  // Iterate over the given compaction input, stringifying and dumping each
  // yielded row to *out
  void IterateInput(CompactionInput *input, vector<string> *out) {
    ASSERT_STATUS_OK(DebugDumpCompactionInput(input, out));
  }

  void DoFlush(CompactionInput *input, const MvccSnapshot &snap,
               shared_ptr<RowSetMetadata>* rowset_meta) {
    // Flush with a large roll threshold so we only write a single file.
    // This simplifies the test so we always need to reopen only a single rowset.
    const size_t kRollThreshold = 1024 * 1024 * 1024; // 1GB
    RollingDiskRowSetWriter rsw(tablet_->metadata(), schema_,
                                BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f),
                                kRollThreshold);
    ASSERT_STATUS_OK(rsw.Open());
    ASSERT_STATUS_OK(Flush(input, snap, &rsw));
    ASSERT_STATUS_OK(rsw.Finish());
    vector<shared_ptr<RowSetMetadata> > metas;
    rsw.GetWrittenMetadata(&metas);
    ASSERT_EQ(1, metas.size());
    ASSERT_TRUE(metas[0]->HasBloomDataBlockForTests());
    if (rowset_meta) {
      *rowset_meta = metas[0];
    }
  }

  void DoCompact(const vector<shared_ptr<DiskRowSet> > &rowsets) {
    MvccSnapshot merge_snap(mvcc_);
    vector<shared_ptr<CompactionInput> > merge_inputs;
    BOOST_FOREACH(const shared_ptr<DiskRowSet> &rs, rowsets) {
      merge_inputs.push_back(shared_ptr<CompactionInput>(CompactionInput::Create(*rs, merge_snap)));
    }

    gscoped_ptr<CompactionInput> compact_input(CompactionInput::Merge(merge_inputs, schema_));
    DoFlush(compact_input.get(), merge_snap, NULL);
  }

  void FlushAndReopen(const MemRowSet &mrs, shared_ptr<DiskRowSet> *rs) {
    MvccSnapshot snap(mvcc_);
    shared_ptr<RowSetMetadata> rowset_meta;
    gscoped_ptr<CompactionInput> input(CompactionInput::Create(mrs, snap));
    DoFlush(input.get(), snap, &rowset_meta);
    // Re-open it
    ASSERT_STATUS_OK(DiskRowSet::Open(rowset_meta, rs));
  }

  template<bool OVERLAP_INPUTS>
  void DoBenchmark() {
    vector<shared_ptr<DiskRowSet> > rowsets;

    if (FLAGS_merge_benchmark_input_dir.empty()) {
      // Create inputs.
      for (int i = 0; i < FLAGS_merge_benchmark_num_rowsets; i++) {
        // Create a memrowset with a bunch of rows and updates.
        shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));

        for (int n = 0; n < FLAGS_merge_benchmark_num_rows_per_rowset; n++) {

          int row_key;
          if (OVERLAP_INPUTS) {
            // input 0: 0 3 6 9 ...
            // input 1: 1 4 7 10 ...
            // input 2: 2 5 8 11 ...
            row_key = n * FLAGS_merge_benchmark_num_rowsets + i;
          } else {
            // input 0: 0 1 2 3
            // input 1: 1000 1001 1002 1003
            // ...
            row_key = i * FLAGS_merge_benchmark_num_rows_per_rowset + n;
          }
          InsertRow(mrs.get(), row_key, n);
        }
        shared_ptr<DiskRowSet> rs;
        FlushAndReopen(*mrs, &rs);
        ASSERT_NO_FATAL_FAILURE();
        rowsets.push_back(rs);
      }
    } else {
      // This test will load a tablet with id "KuduCompactionBenchTablet" that have
      // a 0000000 or 1111111 super-block id, in the specified root-dir.
      metadata::TabletMasterBlockPB master_block;
      master_block.set_tablet_id("KuduCompactionBenchTablet");
      master_block.set_block_a("00000000000000000000000000000000");
      master_block.set_block_b("11111111111111111111111111111111");

      FsManager fs_manager(env_.get(), FLAGS_merge_benchmark_input_dir);
      metadata::TabletMetadata input_meta(&fs_manager, master_block);
      ASSERT_STATUS_OK(input_meta.Load());

      BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, input_meta.rowsets()) {
        shared_ptr<DiskRowSet> rs;
        CHECK_OK(DiskRowSet::Open(meta, &rs));
        rowsets.push_back(rs);
      }

      CHECK(!rowsets.empty()) << "No rowsets found in " << FLAGS_merge_benchmark_input_dir;
    }

    LOG_TIMING(INFO, "Compacting") {
      DoCompact(rowsets);
    }
  }

 protected:
  MvccManager mvcc_;

  RowBuilder row_builder_;
  char key_buf_[256];
};

TEST_F(TestCompaction, TestMemRowSetInput) {
  // Create a memrowset with 10 rows and several updates.
  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));
  InsertRows(mrs.get(), 10, 0);
  UpdateRows(mrs.get(), 10, 0, 1);
  UpdateRows(mrs.get(), 10, 0, 2);

  // Ensure that the compaction input yields the expected rows
  // and mutations.
  vector<string> out;
  MvccSnapshot snap(mvcc_);
  gscoped_ptr<CompactionInput> input(CompactionInput::Create(*mrs, snap));
  IterateInput(input.get(), &out);
  ASSERT_EQ(10, out.size());
  ASSERT_EQ("(string key=hello 00000000, uint32 val=0) mutations: [@10(SET val=1), @20(SET val=2)]",
            out[0]);
  ASSERT_EQ("(string key=hello 00000090, uint32 val=9) mutations: [@19(SET val=1), @29(SET val=2)]",
            out[9]);
}

TEST_F(TestCompaction, TestRowSetInput) {
  // Create a memrowset with a bunch of rows, flush and reopen.
  shared_ptr<DiskRowSet> rs;
  {
    shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));
    InsertRows(mrs.get(), 10, 0);
    FlushAndReopen(*mrs, &rs);
    ASSERT_NO_FATAL_FAILURE();
  }

  // Update the rows in the rowset.
  UpdateRows(rs.get(), 10, 0, 1);
  UpdateRows(rs.get(), 10, 0, 2);
  // Flush DMS, update some more.
  ASSERT_STATUS_OK(rs->FlushDeltas());
  UpdateRows(rs.get(), 10, 0, 3);
  UpdateRows(rs.get(), 10, 0, 4);

  // Check compaction input
  vector<string> out;
  gscoped_ptr<CompactionInput> input(CompactionInput::Create(*rs, MvccSnapshot(mvcc_)));
  IterateInput(input.get(), &out);
  ASSERT_EQ(10, out.size());
  ASSERT_EQ("(string key=hello 00000000, uint32 val=0) "
            "mutations: [@10(SET val=1), @20(SET val=2), @30(SET val=3), @40(SET val=4)]",
            out[0]);
  ASSERT_EQ("(string key=hello 00000090, uint32 val=9) "
            "mutations: [@19(SET val=1), @29(SET val=2), @39(SET val=3), @49(SET val=4)]",
            out[9]);
}

// Test case which doesn't do any merging -- just compacts
// a single input rowset (which may be the memrowset) into a single
// output rowset (on disk).
TEST_F(TestCompaction, TestOneToOne) {
  // Create a memrowset with a bunch of rows and updates.
  shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));
  InsertRows(mrs.get(), 1000, 0);
  UpdateRows(mrs.get(), 1000, 0, 1);
  MvccSnapshot snap(mvcc_);

  // Flush it to disk and re-open.
  shared_ptr<DiskRowSet> rs;
  FlushAndReopen(*mrs, &rs);
  ASSERT_NO_FATAL_FAILURE();

  // Update the rows with some updates that weren't in the snapshot.
  UpdateRows(mrs.get(), 1000, 0, 2);

  // Catch the updates that came in after the snapshot flush was made.
  MvccSnapshot snap2(mvcc_);
  gscoped_ptr<CompactionInput> input(CompactionInput::Create(*mrs, snap2));

  // Add some more updates which come into the new rowset while the "reupdate" is happening.
  UpdateRows(rs.get(), 1000, 0, 3);

  ASSERT_STATUS_OK(ReupdateMissedDeltas(input.get(), snap, snap2,
                                        boost::assign::list_of(rs)));

  // If we look at the contents of the DiskRowSet now, we should see the "re-updated" data.
  vector<string> out;
  input.reset(CompactionInput::Create(*rs, MvccSnapshot(mvcc_)));
  IterateInput(input.get(), &out);
  ASSERT_EQ(1000, out.size());
  ASSERT_EQ("(string key=hello 00000000, uint32 val=1) mutations: [@2000(SET val=2), @3000(SET val=3)]",
            out[0]);

  // And compact (1 input to 1 output)
  MvccSnapshot snap3(mvcc_);
  gscoped_ptr<CompactionInput> compact_input(CompactionInput::Create(*rs, snap3));
  shared_ptr<RowSetMetadata> rowset_compact_meta;
  DoFlush(compact_input.get(), snap3, &rowset_compact_meta);
}

TEST_F(TestCompaction, TestMerge) {
  vector<shared_ptr<DiskRowSet> > rowsets;

  // Create three input rowsets
  for (int delta = 0; delta < 3; delta++) {
    // Create a memrowset with a bunch of rows and updates.
    shared_ptr<MemRowSet> mrs(new MemRowSet(schema_));
    InsertRows(mrs.get(), 1000, delta);
    UpdateRows(mrs.get(), 1000, delta, 1);

    // Flush it to disk and re-open it.
    shared_ptr<DiskRowSet> rs;
    FlushAndReopen(*mrs, &rs);
    ASSERT_NO_FATAL_FAILURE();
    rowsets.push_back(rs);

    // Perform some updates into DMS
    UpdateRows(rs.get(), 1000, delta, 2);
  }

  // Merge them.
  DoCompact(rowsets);
}

#ifdef NDEBUG
// Benchmark for the compaction merge input for the case where the inputs
// contain non-overlapping data. In this case the merge can be optimized
// to be block-wise.
TEST_F(TestCompaction, BenchmarkMergeWithoutOverlap) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipped: must enable slow tests.";
    return;
  }
  DoBenchmark<false>();
}

// Benchmark for the compaction merge input when the inputs are entirely
// overlapping (i.e the inputs become fully interleaved in the output)
TEST_F(TestCompaction, BenchmarkMergeWithOverlap) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Skipped: must enable slow tests.";
    return;
  }
  DoBenchmark<true>();
}
#endif

}
}
