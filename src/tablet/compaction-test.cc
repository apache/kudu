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
    KuduRowSetTest(CreateSchema()),
    row_builder_(schema_)
  {}

  static Schema CreateSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("key", STRING));
    CHECK_OK(builder.AddColumn("val", UINT32));
    return builder.Build();
  }

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
    if (!mrs->schema().Equals(row_builder_.schema())) {
      // The MemRowSet is not projecting the row, so must be done by the caller
      RowProjector projector(&row_builder_.schema(), &mrs->schema());
      uint8_t rowbuf[ContiguousRowHelper::row_size(mrs->schema())];
      ContiguousRow dst_row(mrs->schema(), rowbuf);
      ASSERT_STATUS_OK_FAST(projector.Init());
      ASSERT_STATUS_OK_FAST(projector.ProjectRowForWrite(row_builder_.row(),
                            &dst_row, static_cast<Arena*>(NULL)));
      ASSERT_STATUS_OK_FAST(mrs->Insert(tx.txid(), ConstContiguousRow(dst_row)));
    } else {
      ASSERT_STATUS_OK_FAST(mrs->Insert(tx.txid(), row_builder_.row()));
    }
  }

  // Update n_rows rows of data.
  // Each row has the key (string key=hello <n*10 + delta>) and its 'val' column
  // is set to new_val.
  void UpdateRows(RowSet *rowset, int n_rows, int delta, uint32_t new_val) {
    char keybuf[256];
    faststring update_buf;
    const Schema& schema = rowset->schema();
    size_t col_idx = schema.find_column("val");
    for (uint32_t i = 0; i < n_rows; i++) {
      SCOPED_TRACE(i);
      ScopedTransaction tx(&mvcc_);
      snprintf(keybuf, sizeof(keybuf), kRowKeyFormat, i * 10 + delta);

      update_buf.clear();
      RowChangeListEncoder update(schema, &update_buf);
      update.AddColumnUpdate(col_idx, &new_val);

      RowBuilder rb(schema.CreateKeyProjection());
      rb.AddString(Slice(keybuf));
      RowSetKeyProbe probe(rb.row());
      ProbeStats stats;
      MutationResultPB result;
      ASSERT_STATUS_OK(rowset->MutateRow(tx.txid(),
                                         probe,
                                         RowChangeList(update_buf),
                                         &stats,
                                         &result));
    }
  }

  // Iterate over the given compaction input, stringifying and dumping each
  // yielded row to *out
  void IterateInput(CompactionInput *input, vector<string> *out) {
    ASSERT_STATUS_OK(DebugDumpCompactionInput(input, out));
  }

  void DoFlush(CompactionInput *input, const Schema& projection, const MvccSnapshot &snap,
               shared_ptr<RowSetMetadata>* rowset_meta) {
    // Flush with a large roll threshold so we only write a single file.
    // This simplifies the test so we always need to reopen only a single rowset.
    const size_t kRollThreshold = 1024 * 1024 * 1024; // 1GB
    RollingDiskRowSetWriter rsw(tablet_->metadata(), projection,
                                BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f),
                                kRollThreshold);
    ASSERT_STATUS_OK(rsw.Open());
    ASSERT_STATUS_OK(FlushCompactionInput(input, snap, &rsw));
    ASSERT_STATUS_OK(rsw.Finish());
    vector<shared_ptr<RowSetMetadata> > metas;
    rsw.GetWrittenMetadata(&metas);
    ASSERT_EQ(1, metas.size());
    ASSERT_TRUE(metas[0]->HasBloomDataBlockForTests());
    if (rowset_meta) {
      *rowset_meta = metas[0];
    }
  }

  void DoCompact(const vector<shared_ptr<DiskRowSet> >& rowsets, const Schema& projection,
                 shared_ptr<RowSetMetadata>* rowset_meta = NULL) {
    MvccSnapshot merge_snap(mvcc_);
    vector<shared_ptr<CompactionInput> > merge_inputs;
    BOOST_FOREACH(const shared_ptr<DiskRowSet> &rs, rowsets) {
      merge_inputs.push_back(
        shared_ptr<CompactionInput>(CompactionInput::Create(*rs, &projection, merge_snap)));
    }

    gscoped_ptr<CompactionInput> compact_input(CompactionInput::Merge(merge_inputs, &projection));
    DoFlush(compact_input.get(), projection, merge_snap, rowset_meta);
  }

  void FlushAndReopen(const MemRowSet& mrs, shared_ptr<DiskRowSet> *rs, const Schema& projection) {
    MvccSnapshot snap(mvcc_);
    shared_ptr<RowSetMetadata> rowset_meta;
    gscoped_ptr<CompactionInput> input(CompactionInput::Create(mrs, &projection, snap));
    DoFlush(input.get(), projection, snap, &rowset_meta);
    // Re-open it
    ASSERT_STATUS_OK(DiskRowSet::Open(rowset_meta, rs));
  }

  // Test compaction where each of the input rowsets has
  // each of the input schemas. The output rowset will
  // have the 'projection' schema.
  void DoMerge(const Schema& projection, const vector<Schema>& schemas) {
    vector<shared_ptr<DiskRowSet> > rowsets;

    // Create one input rowset for each of the input schemas
    int delta = 0;
    BOOST_FOREACH(const Schema& schema, schemas) {
      // Create a memrowset with a bunch of rows and updates.
      shared_ptr<MemRowSet> mrs(new MemRowSet(delta, schema));
      InsertRows(mrs.get(), 1000, delta);
      UpdateRows(mrs.get(), 1000, delta, 1);

      // Flush it to disk and re-open it.
      shared_ptr<DiskRowSet> rs;
      FlushAndReopen(*mrs, &rs, schema);
      ASSERT_NO_FATAL_FAILURE();
      rowsets.push_back(rs);

      // Perform some updates into DMS
      UpdateRows(rs.get(), 1000, delta, 2);
      delta++;
    }

    // Merge them.
    shared_ptr<RowSetMetadata> meta;
    ASSERT_NO_FATAL_FAILURE(DoCompact(rowsets, projection, &meta));

    // Verify the resulting compaction output has the right number
    // of rows.
    shared_ptr<DiskRowSet> result_rs;
    ASSERT_STATUS_OK(DiskRowSet::Open(meta, &result_rs));

    rowid_t count = 0;
    ASSERT_STATUS_OK(result_rs->CountRows(&count));
    ASSERT_EQ(1000 * schemas.size(), count);
  }

  template<bool OVERLAP_INPUTS>
  void DoBenchmark() {
    vector<shared_ptr<DiskRowSet> > rowsets;

    if (FLAGS_merge_benchmark_input_dir.empty()) {
      // Create inputs.
      for (int i = 0; i < FLAGS_merge_benchmark_num_rowsets; i++) {
        // Create a memrowset with a bunch of rows and updates.
        shared_ptr<MemRowSet> mrs(new MemRowSet(i, schema_));

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
        FlushAndReopen(*mrs, &rs, schema_);
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
      gscoped_ptr<metadata::TabletMetadata> input_meta;
      ASSERT_STATUS_OK(metadata::TabletMetadata::Load(&fs_manager, master_block, &input_meta));

      BOOST_FOREACH(const shared_ptr<RowSetMetadata>& meta, input_meta->rowsets()) {
        shared_ptr<DiskRowSet> rs;
        CHECK_OK(DiskRowSet::Open(meta, &rs));
        rowsets.push_back(rs);
      }

      CHECK(!rowsets.empty()) << "No rowsets found in " << FLAGS_merge_benchmark_input_dir;
    }

    LOG_TIMING(INFO, "Compacting " +
               std::string((OVERLAP_INPUTS ? "with overlap" : "without overlap"))) {
      DoCompact(rowsets, schema_);
    }
  }

 protected:
  MvccManager mvcc_;

  RowBuilder row_builder_;
  char key_buf_[256];
};

TEST_F(TestCompaction, TestMemRowSetInput) {
  // Create a memrowset with 10 rows and several updates.
  shared_ptr<MemRowSet> mrs(new MemRowSet(0, schema_));
  InsertRows(mrs.get(), 10, 0);
  UpdateRows(mrs.get(), 10, 0, 1);
  UpdateRows(mrs.get(), 10, 0, 2);

  // Ensure that the compaction input yields the expected rows
  // and mutations.
  vector<string> out;
  MvccSnapshot snap(mvcc_);
  gscoped_ptr<CompactionInput> input(CompactionInput::Create(*mrs, &schema_, snap));
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
    shared_ptr<MemRowSet> mrs(new MemRowSet(0, schema_));
    InsertRows(mrs.get(), 10, 0);
    FlushAndReopen(*mrs, &rs, schema_);
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
  gscoped_ptr<CompactionInput> input(CompactionInput::Create(*rs, &schema_, MvccSnapshot(mvcc_)));
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
  shared_ptr<MemRowSet> mrs(new MemRowSet(0, schema_));
  InsertRows(mrs.get(), 1000, 0);
  UpdateRows(mrs.get(), 1000, 0, 1);
  MvccSnapshot snap(mvcc_);

  // Flush it to disk and re-open.
  shared_ptr<DiskRowSet> rs;
  FlushAndReopen(*mrs, &rs, schema_);
  ASSERT_NO_FATAL_FAILURE();

  // Update the rows with some updates that weren't in the snapshot.
  UpdateRows(mrs.get(), 1000, 0, 2);

  // Catch the updates that came in after the snapshot flush was made.
  MvccSnapshot snap2(mvcc_);
  gscoped_ptr<CompactionInput> input(CompactionInput::Create(*mrs, &schema_, snap2));

  // Add some more updates which come into the new rowset while the "reupdate" is happening.
  UpdateRows(rs.get(), 1000, 0, 3);

  string dummy_name = "";
  WriteTransactionContext tx_ctx;

  ASSERT_STATUS_OK(ReupdateMissedDeltas(dummy_name,
                                        &tx_ctx,
                                        input.get(),
                                        snap,
                                        snap2,
                                        boost::assign::list_of(rs)));

  // If we look at the contents of the DiskRowSet now, we should see the "re-updated" data.
  vector<string> out;
  input.reset(CompactionInput::Create(*rs, &schema_, MvccSnapshot(mvcc_)));
  IterateInput(input.get(), &out);
  ASSERT_EQ(1000, out.size());
  ASSERT_EQ("(string key=hello 00000000, uint32 val=1) mutations: "
            "[@2000(SET val=2), @3000(SET val=3)]", out[0]);

  // And compact (1 input to 1 output)
  MvccSnapshot snap3(mvcc_);
  gscoped_ptr<CompactionInput> compact_input(CompactionInput::Create(*rs, &schema_, snap3));
  shared_ptr<RowSetMetadata> rowset_compact_meta;
  DoFlush(compact_input.get(), schema_, snap3, &rowset_compact_meta);
}

// Test merging two row sets and the second one has updates, KUDU-102
// We re-create the conditions by providing two DRS that are both the input and the
// output of a compaction, and trying to merge two MRS.
TEST_F(TestCompaction, TestKUDU102) {
  // Create 2 row sets, flush them
  shared_ptr<MemRowSet> mrs(new MemRowSet(0, schema_));
  InsertRows(mrs.get(), 10, 0);
  shared_ptr<DiskRowSet> rs;
  FlushAndReopen(*mrs, &rs, schema_);
  ASSERT_NO_FATAL_FAILURE();

  shared_ptr<MemRowSet> mrs_b(new MemRowSet(1, schema_));
  InsertRows(mrs_b.get(), 10, 100);
  MvccSnapshot snap(mvcc_);
  shared_ptr<DiskRowSet> rs_b;
  FlushAndReopen(*mrs_b, &rs_b, schema_);
  ASSERT_NO_FATAL_FAILURE();

  // Update all the rows in the second row set
  UpdateRows(mrs_b.get(), 10, 100, 2);

  // Catch the updates that came in after the snapshot flush was made.
  // Note that we are merging two MRS, it's a hack
  MvccSnapshot snap2(mvcc_);
  vector<shared_ptr<CompactionInput> > merge_inputs;
  merge_inputs.push_back(
        shared_ptr<CompactionInput>(CompactionInput::Create(*mrs, &schema_, snap2)));
  merge_inputs.push_back(
        shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_b, &schema_, snap2)));
  gscoped_ptr<CompactionInput> input(CompactionInput::Merge(merge_inputs, &schema_));

  string dummy_name = "";
  WriteTransactionContext tx_ctx;

  // This would fail without KUDU-102
  ASSERT_STATUS_OK(ReupdateMissedDeltas(dummy_name,
                                        &tx_ctx,
                                        input.get(),
                                        snap,
                                        snap2,
                                        boost::assign::list_of(rs) (rs_b)));
}


// Test compacting when all of the inputs and the output have the same schema
TEST_F(TestCompaction, TestMerge) {
  vector<Schema> schemas;
  schemas.push_back(schema_);
  schemas.push_back(schema_);
  schemas.push_back(schema_);
  DoMerge(schemas.back(), schemas);
}

// test compacting when the inputs have different base schemas
TEST_F(TestCompaction, TestMergeMultipleSchemas) {
  vector<Schema> schemas;
  SchemaBuilder builder(schema_);
  schemas.push_back(schema_);

  // Add an int column with default
  uint32_t default_c2 = 10;
  CHECK_OK(builder.AddColumn("c2", UINT32, false, &default_c2, &default_c2));
  schemas.push_back(builder.Build());

  // add a string column with default
  Slice default_c3("Hello World");
  CHECK_OK(builder.AddColumn("c3", STRING, false, &default_c3, &default_c3));
  schemas.push_back(builder.Build());

  DoMerge(schemas.back(), schemas);
}

// Test MergeCompactionInput against MemRowSets. This behavior isn't currently
// used (we never compact in-memory), but this is a regression test for a bug
// encountered during development where the first row of each MRS got dropped.
TEST_F(TestCompaction, TestMergeMRS) {
  shared_ptr<MemRowSet> mrs_a(new MemRowSet(0, schema_));
  InsertRows(mrs_a.get(), 10, 0);

  shared_ptr<MemRowSet> mrs_b(new MemRowSet(0, schema_));
  InsertRows(mrs_b.get(), 10, 1);

  MvccSnapshot snap(mvcc_);
  vector<shared_ptr<CompactionInput> > merge_inputs;
  merge_inputs.push_back(
        shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_a, &schema_, snap)));
  merge_inputs.push_back(
        shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_b, &schema_, snap)));
  gscoped_ptr<CompactionInput> input(CompactionInput::Merge(merge_inputs, &schema_));

  vector<string> out;
  IterateInput(input.get(), &out);
  ASSERT_EQ(out.size(), 20);
  EXPECT_EQ(out[0], "(string key=hello 00000000, uint32 val=0) mutations: []");
  EXPECT_EQ(out[19], "(string key=hello 00000091, uint32 val=9) mutations: []");
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

} // namespace tablet
} // namespace kudu
