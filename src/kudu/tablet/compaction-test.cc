// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/tablet/compaction.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <numeric>
#include <ostream>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/logical_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/rowid.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/log_block_manager.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/memrowset.h"
#include "kudu/tablet/mutation.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/tablet_mem_trackers.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

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

DECLARE_string(block_manager);

using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_set;
using std::vector;

namespace kudu {
namespace tablet {

using consensus::OpId;
using log::LogAnchorRegistry;
using strings::Substitute;

static const char *kRowKeyFormat = "hello %08d";
static const size_t kLargeRollThreshold = 1024 * 1024 * 1024; // 1GB
static const size_t kSmallRollThreshold = 1024; // 1KB

class TestCompaction : public KuduRowSetTest {
 public:
  TestCompaction()
      : KuduRowSetTest(CreateSchema()),
        op_id_(consensus::MaximumOpId()),
        row_builder_(&schema_),
        arena_(32*1024),
        clock_(Timestamp::kInitialTimestamp),
        log_anchor_registry_(new log::LogAnchorRegistry()) {
    schema_ptr_ = std::make_shared<Schema>(schema_);
  }

  static Schema CreateSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("key", STRING));
    CHECK_OK(builder.AddColumn("val", INT32));
    CHECK_OK(builder.AddNullableColumn("nullable_val", INT32));
    return builder.BuildWithoutIds();
  }

  // Insert n_rows rows of data.
  // Each row is the tuple: (string key=hello <n*10 + delta>, val=<n>)
  void InsertRows(MemRowSet *mrs, int n_rows, int delta) {
    for (int32_t i = 0; i < n_rows; i++) {
      InsertRow(mrs, i * 10 + delta, i);
    }
  }

  // Inserts a row.
  // The 'nullable_val' column is set to either NULL (when val is odd)
  // or 'val' (when val is even).
  void InsertRow(MemRowSet *mrs, int row_key, int32_t val) {
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();
    InsertRowInOp(mrs, op, row_key, val);
    op.FinishApplying();
  }

  void DeleteAndInsertRow(MemRowSet* mrs_to_delete, MemRowSet* mrs_to_insert,
                          int row_key, int32_t val, bool also_update) {
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();
    DeleteRowInOp(mrs_to_delete, op, row_key);
    InsertRowInOp(mrs_to_insert, op, row_key, val);
    if (also_update) {
      UpdateRowInOp(mrs_to_insert, op, row_key, val);
    }
    op.FinishApplying();
  }

  void InsertAndDeleteRow(MemRowSet* mrs, int row_key, int32_t val) {
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();
    InsertRowInOp(mrs, op, row_key, val);
    DeleteRowInOp(mrs, op, row_key);
    op.FinishApplying();
  }

  void BuildRow(int row_key, int32_t val) {
    row_builder_.Reset();
    snprintf(key_buf_, sizeof(key_buf_), kRowKeyFormat, row_key);
    row_builder_.AddString(Slice(key_buf_));
    row_builder_.AddInt32(val);
    if (val % 2 == 0) {
      row_builder_.AddInt32(val);
    } else {
      row_builder_.AddNull();
    }
  }

  void InsertRowInOp(MemRowSet *mrs,
                     const ScopedOp& op,
                     int row_key,
                     int32_t val) {
    BuildRow(row_key, val);
    if (!mrs->schema()->Equals(*row_builder_.schema())) {
      // The MemRowSet is not projecting the row, so must be done by the caller
      RowProjector projector(row_builder_.schema(), mrs->schema().get());
      uint8_t rowbuf[ContiguousRowHelper::row_size(*mrs->schema())];
      ContiguousRow dst_row(mrs->schema().get(), rowbuf);
      ASSERT_OK_FAST(projector.Init());
      ASSERT_OK_FAST(projector.ProjectRowForWrite(row_builder_.row(),
                                                  &dst_row, static_cast<Arena*>(nullptr)));
      ASSERT_OK_FAST(mrs->Insert(op.timestamp(), ConstContiguousRow(dst_row), op_id_));
    } else {
      ASSERT_OK_FAST(mrs->Insert(op.timestamp(), row_builder_.row(), op_id_));
    }
  }

  // Update n_rows rows of data.
  // Each row has the key (string key=hello <n*10 + delta>) and its 'val' column
  // is set to new_val.
  // If 'val' is even, 'nullable_val' is set to NULL. Otherwise, set to 'val'.
  // Note that this is the opposite of InsertRow() above, so that the updates
  // flop NULL to non-NULL and vice versa.
  void UpdateRows(RowSet *rowset, int n_rows, int delta, int32_t new_val) {
    for (uint32_t i = 0; i < n_rows; i++) {
      SCOPED_TRACE(i);
      UpdateRow(rowset, i * 10 + delta, new_val);
    }
  }

  void UpdateRow(RowSet *rowset, int row_key, int32_t new_val) {
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();
    UpdateRowInOp(rowset, op, row_key, new_val);
    op.FinishApplying();
  }

  void UpdateRowInOp(RowSet *rowset,
                     const ScopedOp& op,
                     int row_key,
                     int32_t new_val) {
    ColumnId col_id = schema_.column_id(schema_.find_column("val"));
    ColumnId nullable_col_id = schema_.column_id(schema_.find_column("nullable_val"));

    char keybuf[256];
    faststring update_buf;
    snprintf(keybuf, sizeof(keybuf), kRowKeyFormat, row_key);

    update_buf.clear();
    RowChangeListEncoder update(&update_buf);
    update.AddColumnUpdate(schema_.column_by_id(col_id), col_id, &new_val);
    if (new_val % 2 == 0) {
      update.AddColumnUpdate(schema_.column_by_id(nullable_col_id),
                             nullable_col_id, nullptr);
    } else {
      update.AddColumnUpdate(schema_.column_by_id(nullable_col_id),
                             nullable_col_id, &new_val);
    }

    Schema proj_key = schema_.CreateKeyProjection();
    RowBuilder rb(&proj_key);
    rb.AddString(Slice(keybuf));
    Arena arena(64);
    RowSetKeyProbe probe(rb.row(), &arena);
    ProbeStats stats;
    OperationResultPB result;
    ASSERT_OK(rowset->MutateRow(op.timestamp(),
                                probe,
                                RowChangeList(update_buf),
                                op_id_,
                                nullptr,
                                &stats,
                                &result));
  }

  void DeleteRows(RowSet* rowset, int n_rows) {
    DeleteRows(rowset, n_rows, 0);
  }

  void DeleteRows(RowSet* rowset, int n_rows, int delta) {
    for (uint32_t i = 0; i < n_rows; i++) {
      SCOPED_TRACE(i);
      DeleteRow(rowset, i * 10 + delta);
    }
  }

  void DeleteRow(RowSet* rowset, int row_key) {
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();
    DeleteRowInOp(rowset, op, row_key);
    op.FinishApplying();
  }

  void DeleteRowInOp(RowSet *rowset, const ScopedOp& op, int row_key) {
    char keybuf[256];
    faststring update_buf;
    snprintf(keybuf, sizeof(keybuf), kRowKeyFormat, row_key);
    update_buf.clear();
    RowChangeListEncoder update(&update_buf);
    update.SetToDelete();

    Schema proj_key = schema_.CreateKeyProjection();
    RowBuilder rb(&proj_key);
    rb.AddString(Slice(keybuf));
    Arena arena(64);
    RowSetKeyProbe probe(rb.row(), &arena);
    ProbeStats stats;
    OperationResultPB result;
    ASSERT_OK(rowset->MutateRow(op.timestamp(),
                                probe,
                                RowChangeList(update_buf),
                                op_id_,
                                nullptr,
                                &stats,
                                &result));
  }

  // Iterate over the given compaction input, stringifying and dumping each
  // yielded row to *out
  void IterateInput(CompactionInput *input, vector<string> *out) {
    ASSERT_OK(DebugDumpCompactionInput(input, out));
  }

  // Flush the given CompactionInput 'input' to disk with the given snapshot.
  // If 'result_rowsets' is not NULL, reopens the resulting rowset(s) and appends
  // them to the vector.
  void DoFlushAndReopen(
      CompactionInput *input, const Schema& projection, const MvccSnapshot &snap,
      int64_t roll_threshold, vector<shared_ptr<DiskRowSet> >* result_rowsets) {
    // Flush with a large roll threshold so we only write a single file.
    // This simplifies the test so we always need to reopen only a single rowset.
    RollingDiskRowSetWriter rsw(tablet()->metadata(), projection,
                                Tablet::DefaultBloomSizing(),
                                roll_threshold);
    ASSERT_OK(rsw.Open());
    ASSERT_OK(FlushCompactionInput(tablet()->metadata()->tablet_id(),
                                   fs_manager()->block_manager()->error_manager(),
                                   input, snap, HistoryGcOpts::Disabled(), &rsw));
    ASSERT_OK(rsw.Finish());

    vector<shared_ptr<RowSetMetadata> > metas;
    rsw.GetWrittenRowSetMetadata(&metas);
    for (const shared_ptr<RowSetMetadata>& meta : metas) {
      ASSERT_TRUE(meta->HasBloomDataBlockForTests());
    }
    if (result_rowsets) {
      // Re-open the outputs
      for (const shared_ptr<RowSetMetadata>& meta : metas) {
        shared_ptr<DiskRowSet> rs;
        ASSERT_OK(DiskRowSet::Open(meta, log_anchor_registry_.get(),
                                   mem_trackers_, nullptr, &rs));
        result_rowsets->push_back(rs);
      }
    }
  }

  static Status BuildCompactionInput(const MvccSnapshot& merge_snap,
                                     const vector<shared_ptr<DiskRowSet> >& rowsets,
                                     const Schema& projection,
                                     unique_ptr<CompactionInput>* out) {
    SchemaPtr schema_ptr = std::make_shared<Schema>(projection);
    vector<shared_ptr<CompactionInput> > merge_inputs;
    for (const shared_ptr<DiskRowSet> &rs : rowsets) {
      unique_ptr<CompactionInput> input;
      RETURN_NOT_OK(CompactionInput::Create(*rs, schema_ptr, merge_snap, nullptr, &input));
      merge_inputs.push_back(shared_ptr<CompactionInput>(input.release()));
    }
    out->reset(CompactionInput::Merge(merge_inputs, schema_ptr));
    return Status::OK();
  }

  // Compacts a set of DRSs.
  // If 'result_rowsets' is not NULL, reopens the resulting rowset(s) and appends
  // them to the vector.
  Status CompactAndReopen(const vector<shared_ptr<DiskRowSet> >& rowsets,
                          const Schema& projection, int64_t roll_threshold,
                          vector<shared_ptr<DiskRowSet> >* result_rowsets) {
    MvccSnapshot merge_snap(mvcc_);
    unique_ptr<CompactionInput> compact_input;
    RETURN_NOT_OK(BuildCompactionInput(merge_snap, rowsets, projection, &compact_input));
    DoFlushAndReopen(compact_input.get(), projection, merge_snap, roll_threshold,
                     result_rowsets);
    return Status::OK();
  }

  // Same as above, but sets a high roll threshold so it only produces a single output.
  void CompactAndReopenNoRoll(const vector<shared_ptr<DiskRowSet> >& input_rowsets,
                              const Schema& projection,
                              shared_ptr<DiskRowSet>* result_rs) {
    vector<shared_ptr<DiskRowSet> > result_rowsets;
    CompactAndReopen(input_rowsets, projection, kLargeRollThreshold, &result_rowsets);
    ASSERT_EQ(1, result_rowsets.size());
    *result_rs = result_rowsets[0];
  }

  // Flush an MRS to disk.
  // If 'result_rowsets' is not NULL, reopens the resulting rowset(s) and appends
  // them to the vector.
  void FlushMRSAndReopen(const MemRowSet& mrs, const Schema& projection,
                         int64_t roll_threshold,
                         vector<shared_ptr<DiskRowSet> >* result_rowsets) {
    SchemaPtr schema_ptr = std::make_shared<Schema>(projection);
    MvccSnapshot snap(mvcc_);
    vector<shared_ptr<RowSetMetadata> > rowset_metas;
    unique_ptr<CompactionInput> input(CompactionInput::Create(mrs, schema_ptr, snap));
    DoFlushAndReopen(input.get(), projection, snap, roll_threshold, result_rowsets);
  }

  // Same as above, but sets a high roll threshold so it only produces a single output.
  void FlushMRSAndReopenNoRoll(const MemRowSet& mrs, const Schema& projection,
                            shared_ptr<DiskRowSet>* result_rs) {
    vector<shared_ptr<DiskRowSet> > rowsets;
    FlushMRSAndReopen(mrs, projection, kLargeRollThreshold, &rowsets);
    ASSERT_EQ(1, rowsets.size());
    *result_rs = rowsets[0];
  }

  // Create an invisible MRS -- one whose inserts and deletes were applied at
  // the same timestamp.
  shared_ptr<MemRowSet> CreateInvisibleMRS() {
    shared_ptr<MemRowSet> mrs;
    CHECK_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                               mem_trackers_.tablet_tracker, &mrs));
    InsertAndDeleteRow(mrs.get(), 0, 0);
    return mrs;
  }

  // Count the number of rows in the given rowsets.
  static uint64_t CountRows(const vector<shared_ptr<DiskRowSet>>& rowsets) {
    uint64_t total_num_rows = 0;
    for (const auto& rs : rowsets) {
      uint64_t rs_live_rows;
      CHECK_OK(rs->CountLiveRows(&rs_live_rows));
      total_num_rows += rs_live_rows;
    }
    return total_num_rows;
  }

  // Test compaction where each of the input rowsets has
  // each of the input schemas. The output rowset will
  // have the 'projection' schema.
  void DoMerge(const Schema& projection, const vector<Schema>& schemas) {
    vector<shared_ptr<DiskRowSet> > rowsets;

    // Create one input rowset for each of the input schemas
    int delta = 0;
    for (const Schema& schema : schemas) {
      // Create a memrowset with a bunch of rows and updates.
      shared_ptr<MemRowSet> mrs;
      SchemaPtr schema_ptr = std::make_shared<Schema>(schema);
      CHECK_OK(MemRowSet::Create(delta, schema_ptr, log_anchor_registry_.get(),
                                 mem_trackers_.tablet_tracker, &mrs));
      InsertRows(mrs.get(), 1000, delta);
      UpdateRows(mrs.get(), 1000, delta, 1);

      // Flush it to disk and re-open it.
      shared_ptr<DiskRowSet> rs;
      FlushMRSAndReopenNoRoll(*mrs, schema, &rs);
      NO_FATALS();
      rowsets.push_back(rs);

      // Perform some updates into DMS
      UpdateRows(rs.get(), 1000, delta, 2);
      delta++;
    }

    // Merge them.
    shared_ptr<DiskRowSet> result_rs;
    NO_FATALS(CompactAndReopenNoRoll(rowsets, projection, &result_rs));

    // Verify the resulting compaction output has the right number
    // of rows.
    rowid_t count = 0;
    ASSERT_OK(result_rs->CountRows(nullptr, &count));
    ASSERT_EQ(1000 * schemas.size(), count);
  }

  template<bool OVERLAP_INPUTS>
  void DoBenchmark() {
    vector<shared_ptr<DiskRowSet> > rowsets;

    if (FLAGS_merge_benchmark_input_dir.empty()) {
      // Create inputs.
      for (int i = 0; i < FLAGS_merge_benchmark_num_rowsets; i++) {
        // Create a memrowset with a bunch of rows and updates.
        shared_ptr<MemRowSet> mrs;
        CHECK_OK(MemRowSet::Create(i, schema_ptr_, log_anchor_registry_.get(),
                                   mem_trackers_.tablet_tracker, &mrs));

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
        FlushMRSAndReopenNoRoll(*mrs, schema_, &rs);
        NO_FATALS();
        rowsets.push_back(rs);
      }
    } else {
      string tablet_id = "KuduCompactionBenchTablet";
      FsManager fs_manager(env_, FsManagerOpts(FLAGS_merge_benchmark_input_dir));
      scoped_refptr<TabletMetadata> input_meta;
      ASSERT_OK(TabletMetadata::Load(&fs_manager, tablet_id, &input_meta));

      for (const shared_ptr<RowSetMetadata>& meta : input_meta->rowsets()) {
        shared_ptr<DiskRowSet> rs;
        CHECK_OK(DiskRowSet::Open(meta, log_anchor_registry_.get(),
                                  mem_trackers_, nullptr, &rs));
        rowsets.push_back(rs);
      }

      CHECK(!rowsets.empty()) << "No rowsets found in " << FLAGS_merge_benchmark_input_dir;
    }
    LOG(INFO) << "Beginning compaction";
    LOG_TIMING(INFO, "compacting " +
               std::string((OVERLAP_INPUTS ? "with overlap" : "without overlap"))) {
      MvccSnapshot merge_snap(mvcc_);
      unique_ptr<CompactionInput> compact_input;
      ASSERT_OK(BuildCompactionInput(merge_snap, rowsets, schema_, &compact_input));
      // Use a low target row size to increase the number of resulting rowsets.
      RollingDiskRowSetWriter rdrsw(tablet()->metadata(), schema_,
                                    Tablet::DefaultBloomSizing(),
                                    1024 * 1024); // 1 MB
      ASSERT_OK(rdrsw.Open());
      ASSERT_OK(FlushCompactionInput(tablet()->metadata()->tablet_id(),
                                     fs_manager()->block_manager()->error_manager(),
                                     compact_input.get(), merge_snap, HistoryGcOpts::Disabled(),
                                     &rdrsw));
      ASSERT_OK(rdrsw.Finish());
    }
  }

  // Helpers for building an expected row history.
  void AddExpectedDelete(Mutation** current_head, Timestamp ts = Timestamp::kInvalidTimestamp);
  void AddExpectedUpdate(Mutation** current_head, int32_t val);
  void AddExpectedReinsert(Mutation** current_head, int32_t val);
  void AddUpdateAndDelete(RowSet* rs, CompactionInputRow* row, int row_id, int32_t val);

 protected:
  OpId op_id_;

  RowBuilder row_builder_;
  char key_buf_[256];
  Arena arena_;
  clock::LogicalClock clock_;
  MvccManager mvcc_;
  SchemaPtr schema_ptr_;

  scoped_refptr<LogAnchorRegistry> log_anchor_registry_;

  TabletMemTrackers mem_trackers_;
};

TEST_F(TestCompaction, TestMemRowSetInput) {
  // Create a memrowset with 10 rows and several updates.
  SchemaPtr schema_ptr = std::make_shared<Schema>(schema_);
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs));
  InsertRows(mrs.get(), 10, 0);
  UpdateRows(mrs.get(), 10, 0, 1);
  UpdateRows(mrs.get(), 10, 0, 2);

  // Ensure that the compaction input yields the expected rows
  // and mutations.
  vector<string> out;
  MvccSnapshot snap(mvcc_);
  unique_ptr<CompactionInput> input(CompactionInput::Create(*mrs, schema_ptr, snap));
  IterateInput(input.get(), &out);
  ASSERT_EQ(10, out.size());
  EXPECT_EQ(R"(RowIdxInBlock: 0; Base: (string key="hello 00000000", int32 val=0, )"
                "int32 nullable_val=0); Undo Mutations: [@1(DELETE)]; Redo Mutations: "
                "[@11(SET val=1, nullable_val=1), @21(SET val=2, nullable_val=NULL)];",
            out[0]);
  EXPECT_EQ(R"(RowIdxInBlock: 9; Base: (string key="hello 00000090", int32 val=9, )"
                "int32 nullable_val=NULL); Undo Mutations: [@10(DELETE)]; Redo Mutations: "
                "[@20(SET val=1, nullable_val=1), @30(SET val=2, nullable_val=NULL)];",
            out[9]);
}

TEST_F(TestCompaction, TestFlushMRSWithRolling) {
  // Create a memrowset with enough rows so that, when we flush with a small
  // roll threshold, we'll end up creating multiple DiskRowSets.
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs));
  InsertRows(mrs.get(), 30000, 0);

  vector<shared_ptr<DiskRowSet> > rowsets;
  FlushMRSAndReopen(*mrs, schema_, kSmallRollThreshold, &rowsets);
  ASSERT_GT(rowsets.size(), 1);

  vector<string> rows;
  rows.reserve(30000 / 2);
  rowsets[0]->DebugDump(&rows);
  EXPECT_EQ(R"(RowIdxInBlock: 0; Base: (string key="hello 00000000", int32 val=0, )"
                "int32 nullable_val=0); Undo Mutations: [@1(DELETE)]; Redo Mutations: [];",
            rows[0]);

  rows.clear();
  rowsets[1]->DebugDump(&rows);
  EXPECT_EQ(R"(RowIdxInBlock: 0; Base: (string key="hello 00017150", int32 val=1715, )"
            "int32 nullable_val=NULL); Undo Mutations: [@1716(DELETE)]; Redo Mutations: [];",
            rows[0]);
  EXPECT_EQ(R"(RowIdxInBlock: 1; Base: (string key="hello 00017160", int32 val=1716, )"
            "int32 nullable_val=1716); Undo Mutations: [@1717(DELETE)]; Redo Mutations: [];",
            rows[1]);
}

TEST_F(TestCompaction, TestRowSetInput) {
  // Create a memrowset with a bunch of rows, flush and reopen.
  shared_ptr<DiskRowSet> rs;
  {
    shared_ptr<MemRowSet> mrs;
    ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                                mem_trackers_.tablet_tracker, &mrs));
    InsertRows(mrs.get(), 10, 0);
    FlushMRSAndReopenNoRoll(*mrs, schema_, &rs);
    NO_FATALS();
  }

  // Update the rows in the rowset.
  UpdateRows(rs.get(), 10, 0, 1);
  UpdateRows(rs.get(), 10, 0, 2);
  // Flush DMS, update some more.
  ASSERT_OK(rs->FlushDeltas(nullptr));
  UpdateRows(rs.get(), 10, 0, 3);
  UpdateRows(rs.get(), 10, 0, 4);

  // Check compaction input
  vector<string> out;
  unique_ptr<CompactionInput> input;
  ASSERT_OK(CompactionInput::Create(*rs, schema_ptr_, MvccSnapshot(mvcc_), nullptr, &input));
  IterateInput(input.get(), &out);
  ASSERT_EQ(10, out.size());
  EXPECT_EQ(R"(RowIdxInBlock: 0; Base: (string key="hello 00000000", int32 val=0, )"
                "int32 nullable_val=0); Undo Mutations: [@1(DELETE)]; Redo Mutations: "
                "[@11(SET val=1, nullable_val=1), @21(SET val=2, nullable_val=NULL), "
                "@31(SET val=3, nullable_val=3), @41(SET val=4, nullable_val=NULL)];",
            out[0]);
  EXPECT_EQ(R"(RowIdxInBlock: 9; Base: (string key="hello 00000090", int32 val=9, )"
                "int32 nullable_val=NULL); Undo Mutations: [@10(DELETE)]; Redo Mutations: "
                "[@20(SET val=1, nullable_val=1), @30(SET val=2, nullable_val=NULL), "
                "@40(SET val=3, nullable_val=3), @50(SET val=4, nullable_val=NULL)];",
            out[9]);
}

// Tests that the same rows, duplicated in three DRSs, ghost in two of them
// appears only once on the compaction output but that the resulting row
// includes reinserts for the ghost and all its mutations.
TEST_F(TestCompaction, TestDuplicatedGhostRowsMerging) {
  shared_ptr<DiskRowSet> rs1;
  {
    shared_ptr<MemRowSet> mrs;
    ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                                mem_trackers_.tablet_tracker, &mrs));
    InsertRows(mrs.get(), 10, 0);
    FlushMRSAndReopenNoRoll(*mrs, schema_, &rs1);
    NO_FATALS();
  }
  // Now delete the rows, this will make the rs report them as deleted and
  // so we would reinsert them into the MRS.
  DeleteRows(rs1.get(), 10);

  shared_ptr<DiskRowSet> rs2;
  {
    shared_ptr<MemRowSet> mrs;
    ASSERT_OK(MemRowSet::Create(1, schema_ptr_, log_anchor_registry_.get(),
                                mem_trackers_.tablet_tracker, &mrs));
    InsertRows(mrs.get(), 10, 0);
    UpdateRows(mrs.get(), 10, 0, 1);
    FlushMRSAndReopenNoRoll(*mrs, schema_, &rs2);
    NO_FATALS();
  }
  DeleteRows(rs2.get(), 10);

  shared_ptr<DiskRowSet> rs3;
  {
    shared_ptr<MemRowSet> mrs;
    ASSERT_OK(MemRowSet::Create(2, schema_ptr_, log_anchor_registry_.get(),
                                mem_trackers_.tablet_tracker, &mrs));
    InsertRows(mrs.get(), 10, 0);
    UpdateRows(mrs.get(), 10, 0, 2);
    FlushMRSAndReopenNoRoll(*mrs, schema_, &rs3);
    NO_FATALS();
  }

  shared_ptr<DiskRowSet> result;
  vector<shared_ptr<DiskRowSet> > all_rss;
  all_rss.push_back(rs3);
  all_rss.push_back(rs1);
  all_rss.push_back(rs2);

  // Shuffle the row sets to make sure we test different orderings
  std::mt19937 gen(SeedRandom());
  std::shuffle(all_rss.begin(), all_rss.end(), gen);

  // Now compact all the drs and make sure we don't get duplicated keys on the output
  CompactAndReopenNoRoll(all_rss, schema_, &result);

  unique_ptr<CompactionInput> input;
  ASSERT_OK(CompactionInput::Create(*result,
                                    schema_ptr_,
                                    MvccSnapshot::CreateSnapshotIncludingAllOps(),
                                    nullptr,
                                    &input));
  vector<string> out;
  IterateInput(input.get(), &out);
  ASSERT_EQ(out.size(), 10);
  EXPECT_EQ(R"(RowIdxInBlock: 0; Base: (string key="hello 00000000", int32 val=2, )"
                "int32 nullable_val=NULL); Undo Mutations: [@61(SET val=0, nullable_val=0), "
                "@51(DELETE), @41(REINSERT val=1, nullable_val=1), @31(SET val=0, nullable_val=0), "
                "@21(DELETE), @11(REINSERT val=0, nullable_val=0), @1(DELETE)]; "
                "Redo Mutations: [];", out[0]);
  EXPECT_EQ(R"(RowIdxInBlock: 9; Base: (string key="hello 00000090", int32 val=2, )"
                "int32 nullable_val=NULL); Undo Mutations: [@70(SET val=9, nullable_val=NULL), "
                "@60(DELETE), @50(REINSERT val=1, nullable_val=1), @40(SET val=9, "
                "nullable_val=NULL), @30(DELETE), @20(REINSERT val=9, nullable_val=NULL), "
                "@10(DELETE)]; Redo Mutations: [];", out[9]);
}

void TestCompaction::AddExpectedDelete(Mutation** current_head, Timestamp ts) {
  faststring buf;
  RowChangeListEncoder enc(&buf);
  enc.SetToDelete();
  if (ts == Timestamp::kInvalidTimestamp) ts = Timestamp(clock_.GetCurrentTime());
  Mutation* mutation = Mutation::CreateInArena(&arena_,
                                               ts,
                                               enc.as_changelist());
  mutation->set_next(*current_head);
  *current_head = mutation;
}

void TestCompaction::AddExpectedUpdate(Mutation** current_head, int32_t val) {
  faststring buf;
  RowChangeListEncoder enc(&buf);
  enc.SetToUpdate();
  enc.AddColumnUpdate(schema_.column(1), schema_.column_id(1), &val);
  if (val % 2 == 0) {
    enc.AddColumnUpdate(schema_.column(2), schema_.column_id(2), &val);
  } else {
    enc.AddColumnUpdate(schema_.column(2), schema_.column_id(2), nullptr);
  }
  Mutation* mutation = Mutation::CreateInArena(&arena_,
                                               Timestamp(clock_.GetCurrentTime()),
                                               enc.as_changelist());
  mutation->set_next(*current_head);
  *current_head = mutation;
}

void TestCompaction::AddExpectedReinsert(Mutation** current_head, int32_t val) {
  faststring buf;
  RowChangeListEncoder enc(&buf);
  enc.SetToReinsert();
  enc.EncodeColumnMutation(schema_.column(1), schema_.column_id(1), &val);
  if (val % 2 == 1) {
    enc.EncodeColumnMutation(schema_.column(2), schema_.column_id(2), &val);
  } else {
    enc.EncodeColumnMutation(schema_.column(2), schema_.column_id(2), nullptr);
  }
  Mutation* mutation = Mutation::CreateInArena(&arena_,
                                               Timestamp(clock_.GetCurrentTime()),
                                               enc.as_changelist());
  mutation->set_next(*current_head);
  *current_head = mutation;
}

void TestCompaction::AddUpdateAndDelete(RowSet* rs, CompactionInputRow* row, int row_id,
                                        int32_t val) {
  UpdateRow(rs, row_id, val);
  // Expect an UNDO update for the update.
  AddExpectedUpdate(&row->undo_head, row_id);

  DeleteRow(rs, row_id);
  // Expect an UNDO reinsert for the delete.
  AddExpectedReinsert(&row->undo_head, val);
}

// Build several layers of overlapping rowsets with many ghost rows.
// Repeatedly merge all the generated RowSets until we are left with a single RowSet, then make
// sure that its history matches our expected history.
//
// There are 'kBaseNumRowSets' layers of overlapping rowsets, each level has one less rowset and
// thus less rows. This is meant to exercise a normal-ish path where there are both duplicated and
// unique rows per merge while at the same time making sure that some of the rows are duplicated
// many times.
//
// The verification is performed against a vector of expected CompactionInputRow that we build
// as we insert/update/delete.
TEST_F(TestCompaction, TestDuplicatedRowsRandomCompaction) {
  // NOTE: this test may generate a lot of rowsets; be mindful that some
  // environments default to a low number of max open files (e.g. 256 on macOS
  // 10.15.4), and that if we're using the file block manager, each rowset will
  // use several files.
  const int kBaseNumRowSets = 5;
  const int kNumRowsPerRowSet = 10;

  int total_num_rows = kBaseNumRowSets * kNumRowsPerRowSet;

  MvccSnapshot all_snap = MvccSnapshot::CreateSnapshotIncludingAllOps();

  vector<CompactionInputRow> expected_rows(total_num_rows);
  vector<shared_ptr<DiskRowSet>> row_sets;

  // Create a vector of ids for rows and fill it for the first layer.
  vector<int> row_ids(total_num_rows);
  std::iota(row_ids.begin(), row_ids.end(), 0);

  SchemaPtr schema_ptr = std::make_shared<Schema>(schema_);
  Schema& schema = *schema_ptr;
  SeedRandom();
  int rs_id = 0;
  for (int i = 0; i < kBaseNumRowSets; ++i) {
    int num_rowsets_in_layer = kBaseNumRowSets - i;
    size_t row_idx = 0;
    for (int j = 0; j < num_rowsets_in_layer; ++j) {
      shared_ptr<MemRowSet> mrs;
      ASSERT_OK(MemRowSet::Create(rs_id, schema_ptr, log_anchor_registry_.get(),
                                  mem_trackers_.tablet_tracker, &mrs));

      // For even rows, insert, update and delete them in the mrs.
      for (int k = 0; k < kNumRowsPerRowSet; ++k) {
        int row_id = row_ids[row_idx + k];
        CompactionInputRow* row = &expected_rows[row_id];
        InsertRow(mrs.get(), row_id, row_id);
        // Expect an UNDO delete for the insert.
        AddExpectedDelete(&row->undo_head);
        if (row_id % 2 == 0) AddUpdateAndDelete(mrs.get(), row, row_id, row_id + i + 1);
      }
      shared_ptr<DiskRowSet> drs;
      FlushMRSAndReopenNoRoll(*mrs, schema, &drs);
      // For odd rows, update them and delete them in the drs.
      for (int k = 0; k < kNumRowsPerRowSet; ++k) {
        int row_id = row_ids[row_idx];
        CompactionInputRow* row = &expected_rows[row_id];
        if (row_id % 2 == 1) AddUpdateAndDelete(drs.get(), row, row_id, row_id + i + 1);
        row_idx++;
      }
      row_sets.push_back(drs);
      rs_id++;
    }
    // For the next layer remove one rowset worth of rows at random.
    for (int j = 0; j < kNumRowsPerRowSet; ++j) {
      int to_remove = rand() % row_ids.size();
      row_ids.erase(row_ids.begin() + to_remove);
    }

  }

  RowBlockMemory mem;
  RowBlock block(&schema, kBaseNumRowSets * kNumRowsPerRowSet, &mem);
  // Go through the expected compaction input rows, flip the last undo into a redo and
  // build the base. This will give us the final version that we'll expect the result
  // of the real compaction to match.
  for (int i = 0; i < expected_rows.size(); ++i) {
    CompactionInputRow* row = &expected_rows[i];
    Mutation* reinsert = row->undo_head;
    row->undo_head = reinsert->next();
    row->row = block.row(i);
    BuildRow(i, i);
    CopyRow(row_builder_.row(), &row->row, &mem.arena);
    RowChangeListDecoder redo_decoder(reinsert->changelist());
    CHECK_OK(redo_decoder.Init());
    faststring buf;
    RowChangeListEncoder dummy(&buf);
    dummy.SetToUpdate();
    redo_decoder.MutateRowAndCaptureChanges(&row->row, &mem.arena, &dummy);
    AddExpectedDelete(&row->redo_head, reinsert->timestamp());
  }

  vector<shared_ptr<CompactionInput>> inputs;
  for (auto& row_set : row_sets) {
    unique_ptr<CompactionInput> ci;
    CHECK_OK(row_set->NewCompactionInput(schema_ptr, all_snap, nullptr, &ci));
    inputs.push_back(shared_ptr<CompactionInput>(ci.release()));
  }

  // Compact the row sets by picking a few at random until we're left with just one.
  while (row_sets.size() > 1) {
    std::mt19937 gen(SeedRandom());
    std::shuffle(row_sets.begin(), row_sets.end(), gen);
    // Merge between 2 and 4 row sets.
    int num_rowsets_to_merge = std::min(rand() % 3 + 2, static_cast<int>(row_sets.size()));
    vector<shared_ptr<DiskRowSet>> to_merge;
    for (int i = 0; i < num_rowsets_to_merge; ++i) {
      to_merge.push_back(row_sets.back());
      row_sets.pop_back();
    }
    shared_ptr<DiskRowSet> result;
    CompactAndReopenNoRoll(to_merge, schema_, &result);
    row_sets.push_back(result);
  }

  vector<string> out;
  unique_ptr<CompactionInput> ci;
  CHECK_OK(row_sets[0]->NewCompactionInput(schema_ptr, all_snap, nullptr, &ci));
  IterateInput(ci.get(), &out);

  // Finally go through the final compaction input and through the expected one and make sure
  // they match.
  ASSERT_EQ(expected_rows.size(), out.size());
  for (int i = 0; i < expected_rows.size(); ++i) {
    EXPECT_EQ(CompactionInputRowToString(expected_rows[i]), out[i]);
  }
}

// Test case that inserts and deletes a row in the same op and makes sure
// the row isn't on the compaction input.
TEST_F(TestCompaction, TestMRSCompactionDoesntOutputUnobservableRows) {
  // Insert a row in the mrs and flush it.
  SchemaPtr schema_ptr = std::make_shared<Schema>(schema_);
  shared_ptr<DiskRowSet> rs1;
  {
    shared_ptr<MemRowSet> mrs;
    ASSERT_OK(MemRowSet::Create(0, schema_ptr, log_anchor_registry_.get(),
                                mem_trackers_.tablet_tracker, &mrs));
    InsertRow(mrs.get(), 1, 1);
    FlushMRSAndReopenNoRoll(*mrs, schema_, &rs1);
    NO_FATALS();
  }

  // Now make the row a ghost in rs1 in the same op as we reinsert it in the mrs then
  // flush it. Also insert another row so that the row set isn't completely empty (otherwise
  // it would disappear on flush).
  shared_ptr<DiskRowSet> rs2;
  {
    shared_ptr<MemRowSet> mrs;
    ASSERT_OK(MemRowSet::Create(1, schema_ptr, log_anchor_registry_.get(),
                                mem_trackers_.tablet_tracker, &mrs));
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();
    DeleteRowInOp(rs1.get(), op, 1);
    InsertRowInOp(mrs.get(), op, 1, 2);
    UpdateRowInOp(mrs.get(), op, 1, 3);
    DeleteRowInOp(mrs.get(), op, 1);

    InsertRowInOp(mrs.get(), op, 2, 0);
    op.FinishApplying();
    FlushMRSAndReopenNoRoll(*mrs, schema_, &rs2);
    NO_FATALS();
  }

  MvccSnapshot all_snap = MvccSnapshot::CreateSnapshotIncludingAllOps();

  unique_ptr<CompactionInput> rs1_input;
  ASSERT_OK(CompactionInput::Create(*rs1, schema_ptr, all_snap, nullptr, &rs1_input));

  unique_ptr<CompactionInput> rs2_input;
  ASSERT_OK(CompactionInput::Create(*rs2, schema_ptr, all_snap, nullptr, &rs2_input));

  vector<shared_ptr<CompactionInput>> to_merge;
  to_merge.push_back(shared_ptr<CompactionInput>(rs1_input.release()));
  to_merge.push_back(shared_ptr<CompactionInput>(rs2_input.release()));

  unique_ptr<CompactionInput> merged(CompactionInput::Merge(to_merge, schema_ptr));

  // Make sure the unobservable version of the row that was inserted and deleted in the MRS
  // in the same op doesn't show up in the compaction input.
  vector<string> out;
  IterateInput(merged.get(), &out);
  EXPECT_EQ(out.size(), 2);
  EXPECT_EQ(R"(RowIdxInBlock: 0; Base: (string key="hello 00000001", int32 val=1, )"
                "int32 nullable_val=NULL); Undo Mutations: [@1(DELETE)]; Redo Mutations: "
                "[@2(DELETE)];", out[0]);
  EXPECT_EQ(R"(RowIdxInBlock: 0; Base: (string key="hello 00000002", int32 val=0, )"
                "int32 nullable_val=0); Undo Mutations: [@2(DELETE)]; Redo Mutations: [];", out[1]);
}

// Test case which doesn't do any merging -- just compacts
// a single input rowset (which may be the memrowset) into a single
// output rowset (on disk).
TEST_F(TestCompaction, TestOneToOne) {
  // Create a memrowset with a bunch of rows and updates.
  SchemaPtr schema_ptr = std::make_shared<Schema>(schema_);
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs));
  InsertRows(mrs.get(), 1000, 0);
  UpdateRows(mrs.get(), 1000, 0, 1);
  MvccSnapshot snap(mvcc_);

  // Flush it to disk and re-open.
  shared_ptr<DiskRowSet> rs;
  FlushMRSAndReopenNoRoll(*mrs, schema_, &rs);
  NO_FATALS();

  // Update the rows with some updates that weren't in the snapshot.
  UpdateRows(mrs.get(), 1000, 0, 2);

  // Catch the updates that came in after the snapshot flush was made.
  MvccSnapshot snap2(mvcc_);
  unique_ptr<CompactionInput> input(CompactionInput::Create(*mrs, schema_ptr, snap2));

  // Add some more updates which come into the new rowset while the "reupdate" is happening.
  UpdateRows(rs.get(), 1000, 0, 3);

  string dummy_name = "";

  ASSERT_OK(ReupdateMissedDeltas(nullptr, input.get(), HistoryGcOpts::Disabled(), snap, snap2,
                                 { rs }));

  // If we look at the contents of the DiskRowSet now, we should see the "re-updated" data.
  vector<string> out;
  ASSERT_OK(CompactionInput::Create(*rs, schema_ptr, MvccSnapshot(mvcc_), nullptr, &input));
  IterateInput(input.get(), &out);
  ASSERT_EQ(1000, out.size());
  EXPECT_EQ(R"(RowIdxInBlock: 0; Base: (string key="hello 00000000", int32 val=1, )"
                "int32 nullable_val=1); Undo Mutations: [@1001(SET val=0, nullable_val=0), "
                "@1(DELETE)]; Redo Mutations: [@2001(SET val=2, nullable_val=NULL), "
                "@3001(SET val=3, nullable_val=3)];", out[0]);

  // And compact (1 input to 1 output)
  MvccSnapshot snap3(mvcc_);
  unique_ptr<CompactionInput> compact_input;
  ASSERT_OK(CompactionInput::Create(*rs, schema_ptr, snap3, nullptr, &compact_input));
  DoFlushAndReopen(compact_input.get(), schema_, snap3, kLargeRollThreshold, nullptr);
}

// Test merging two row sets and the second one has updates, KUDU-102
// We re-create the conditions by providing two DRS that are both the input and the
// output of a compaction, and trying to merge two MRS.
TEST_F(TestCompaction, TestKUDU102) {
  // Create 2 row sets, flush them
  SchemaPtr schema_ptr = std::make_shared<Schema>(schema_);
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs));
  InsertRows(mrs.get(), 10, 0);
  shared_ptr<DiskRowSet> rs;
  FlushMRSAndReopenNoRoll(*mrs, schema_, &rs);
  NO_FATALS();

  shared_ptr<MemRowSet> mrs_b;
  ASSERT_OK(MemRowSet::Create(1, schema_ptr, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs_b));
  InsertRows(mrs_b.get(), 10, 100);
  MvccSnapshot snap(mvcc_);
  shared_ptr<DiskRowSet> rs_b;
  FlushMRSAndReopenNoRoll(*mrs_b, schema_, &rs_b);
  NO_FATALS();

  // Update all the rows in the second row set
  UpdateRows(mrs_b.get(), 10, 100, 2);

  // Catch the updates that came in after the snapshot flush was made.
  // Note that we are merging two MRS, it's a hack
  MvccSnapshot snap2(mvcc_);
  vector<shared_ptr<CompactionInput> > merge_inputs;
  merge_inputs.push_back(
        shared_ptr<CompactionInput>(CompactionInput::Create(*mrs, schema_ptr, snap2)));
  merge_inputs.push_back(
        shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_b, schema_ptr, snap2)));
  unique_ptr<CompactionInput> input(CompactionInput::Merge(merge_inputs, schema_ptr));

  string dummy_name = "";

  // This would fail without KUDU-102
  ASSERT_OK(ReupdateMissedDeltas(nullptr, input.get(), HistoryGcOpts::Disabled(), snap, snap2,
                                 { rs, rs_b }));
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
  int32_t default_c2 = 10;
  CHECK_OK(builder.AddColumn("c2", INT32, false, &default_c2, &default_c2));
  schemas.push_back(builder.Build());

  // add a string column with default
  Slice default_c3("Hello World");
  CHECK_OK(builder.AddColumn("c3", STRING, false, &default_c3, &default_c3));
  schemas.push_back(builder.Build());

  DoMerge(schemas.back(), schemas);
}

// Test MergeCompactionInput against MemRowSets.
TEST_F(TestCompaction, TestMergeMRS) {
  shared_ptr<MemRowSet> mrs_a;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs_a));
  InsertRows(mrs_a.get(), 10, 0);

  shared_ptr<MemRowSet> mrs_b;
  ASSERT_OK(MemRowSet::Create(1, schema_ptr_, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs_b));
  InsertRows(mrs_b.get(), 10, 1);

  // While we're at it, let's strew some rows' histories across both rowsets.
  // This will create ghost rows in the compaction inputs and help validate
  // some of the ghost-row handling applied during compaction.
  DeleteRows(mrs_a.get(), 5, 0);
  InsertRows(mrs_b.get(), 5, 0);
  DeleteRows(mrs_b.get(), 5, 1);
  InsertRows(mrs_a.get(), 5, 1);

  MvccSnapshot snap(mvcc_);
  vector<shared_ptr<CompactionInput> > merge_inputs {
    shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_a, schema_ptr_, snap)),
    shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_b, schema_ptr_, snap))
  };
  unique_ptr<CompactionInput> input(CompactionInput::Merge(merge_inputs, schema_ptr_));
  vector<shared_ptr<DiskRowSet>> result_rs;
  DoFlushAndReopen(input.get(), schema_, snap, kSmallRollThreshold, &result_rs);
  uint64_t total_num_rows = CountRows(result_rs);
  ASSERT_EQ(20, total_num_rows);
}

// Test MergeCompactionInput against MemRowSets, where there are rows that were
// inserted and deleted in the same op, and can thus never be seen.
TEST_F(TestCompaction, TestMergeMRSWithInvisibleRows) {
  shared_ptr<MemRowSet> mrs_a = CreateInvisibleMRS();
  shared_ptr<MemRowSet> mrs_b;
  ASSERT_OK(MemRowSet::Create(1, schema_ptr_, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs_b));
  InsertRows(mrs_b.get(), 10, 0);
  MvccSnapshot snap(mvcc_);
  vector<shared_ptr<CompactionInput> > merge_inputs {
    shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_a, schema_ptr_, snap)),
    shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_b, schema_ptr_, snap))
  };
  unique_ptr<CompactionInput> input(CompactionInput::Merge(merge_inputs, schema_ptr_));
  vector<shared_ptr<DiskRowSet>> result_rs;
  DoFlushAndReopen(input.get(), schema_, snap, kSmallRollThreshold, &result_rs);
  ASSERT_EQ(1, result_rs.size());
  ASSERT_EQ(10, CountRows(result_rs));
}

TEST_F(TestCompaction, TestRandomizeDuplicatedRowsAcrossTransactions) {
  ThreadSafeRandom prng(SeedRandom());
  constexpr int kMaxIters = 32;
  constexpr int kMinIters = 2;
  shared_ptr<MemRowSet> main_mrs;
  int mrs_id = 0;
  ASSERT_OK(MemRowSet::Create(mrs_id++, schema_ptr_, log_anchor_registry_.get(),
            mem_trackers_.tablet_tracker, &main_mrs));

  // Keep track of our transactional MRSs. Since we can only mutate a row in
  // a transactional MRS after committing, we'll treat these MRSs as having
  // committed immediately after being inserted to.
  unordered_set<shared_ptr<MemRowSet>> txn_mrss;

  // Keep track of which MRS has the live row, if any.
  MemRowSet* mrs_with_live_row = nullptr;

  int num_iters = kMinIters + prng.Uniform(kMaxIters - kMinIters);
  for (int i = 0; i < num_iters; i++) {
    const auto one_of_three = prng.Next() % 3;
    if (mrs_with_live_row) {
      switch (one_of_three) {
        case 0:
          LOG(INFO) << Substitute("Deleting row from mrs $0", mrs_with_live_row->mrs_id());
          NO_FATALS(DeleteRow(mrs_with_live_row, 1));
          mrs_with_live_row = nullptr;
          break;
        case 1:
          LOG(INFO) << Substitute("Updating row from mrs $0", mrs_with_live_row->mrs_id());
          NO_FATALS(UpdateRow(mrs_with_live_row, 1, 1337));
          break;
        case 2: {
          // For some added spice, let's also sometimes update the row in the
          // same op.
          bool update = prng.Next() % 2;
          LOG(INFO) << Substitute("Deleting row from mrs $0, inserting $1to mrs $2",
                                  mrs_with_live_row->mrs_id(), update ? "and updating " : "",
                                  main_mrs->mrs_id());
          NO_FATALS(DeleteAndInsertRow(mrs_with_live_row, main_mrs.get(), 1, 0, update));
          mrs_with_live_row = main_mrs.get();
          break;
        }
      }
      continue;
    }
    switch (one_of_three) {
      case 0:
        LOG(INFO) << Substitute("Inserting row into mrs $0", main_mrs->mrs_id());
        NO_FATALS(InsertRow(main_mrs.get(), 1, 0));
        mrs_with_live_row = main_mrs.get();
        break;
      case 1: {
        shared_ptr<MemRowSet> txn_mrs;
        ASSERT_OK(MemRowSet::Create(mrs_id++, schema_ptr_, log_anchor_registry_.get(),
                                    mem_trackers_.tablet_tracker, &txn_mrs));
        LOG(INFO) << Substitute("Inserting into mrs $0 and committing", txn_mrs->mrs_id());
        NO_FATALS(InsertRow(txn_mrs.get(), 1, 0));
        mrs_with_live_row = txn_mrs.get();
        EmplaceOrDie(&txn_mrss, std::move(txn_mrs));
        break;
      }
      case 2:
        LOG(INFO) << Substitute("Inserting row into mrs $0 and deleting", main_mrs->mrs_id());
        NO_FATALS(InsertAndDeleteRow(main_mrs.get(), 1, 0));
        break;
    }
  }
  MvccSnapshot snap(mvcc_);
  vector<shared_ptr<CompactionInput>> merge_inputs;
  merge_inputs.emplace_back(CompactionInput::Create(*main_mrs, schema_ptr_, snap));
  for (auto& mrs : txn_mrss) {
    merge_inputs.emplace_back(CompactionInput::Create(*mrs, schema_ptr_, snap));
  }
  unique_ptr<CompactionInput> input(CompactionInput::Merge(merge_inputs, schema_ptr_));
  vector<shared_ptr<DiskRowSet>> result_rs;
  DoFlushAndReopen(input.get(), schema_, snap, kSmallRollThreshold, &result_rs);
  ASSERT_EQ(1, result_rs.size());
  ASSERT_EQ(mrs_with_live_row ? 1 : 0, CountRows(result_rs));
}

// Test that we can merge rowsets in which we have a row whose liveness jumps
// back and forth between rowsets over time.
TEST_F(TestCompaction, TestRowHistoryJumpsBetweenRowsets) {
  shared_ptr<MemRowSet> mrs_a;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs_a));
  shared_ptr<MemRowSet> mrs_b;
  ASSERT_OK(MemRowSet::Create(1, schema_ptr_, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs_b));
  shared_ptr<MemRowSet> mrs_c;
  ASSERT_OK(MemRowSet::Create(2, schema_ptr_, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs_c));
  // Interleave the history of a row across three MRSs.
  InsertRows(mrs_a.get(), 1, 0);
  DeleteRows(mrs_a.get(), 1, 0);
  InsertRows(mrs_b.get(), 1, 0);
  DeleteRows(mrs_b.get(), 1, 0);
  InsertRows(mrs_a.get(), 1, 0);
  DeleteRows(mrs_a.get(), 1, 0);
  InsertRows(mrs_c.get(), 1, 0);
  DeleteRows(mrs_c.get(), 1, 0);
  InsertRows(mrs_a.get(), 1, 0);
  DeleteRows(mrs_a.get(), 1, 0);

  // At this point, our compaction input rows look like:
  // MRS a:
  // UNDO(del@ts1) <- 0 -> REDO(del@ts2) -> REDO(reins@ts5) -> REDO(del@ts6)
  //                      -> REDO(reins@ts9) -> REDO(del@ts10)
  // UNDO(del@ts3) <- 0 -> REDO(del@ts4)
  // UNDO(del@ts7) <- 0 -> REDO(del@ts8)
  //
  // Despite the overlapping time ranges across these inputs, the compaction
  // should go off without a hitch.
  MvccSnapshot snap(mvcc_);
  vector<shared_ptr<CompactionInput> > merge_inputs {
    shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_a, schema_ptr_, snap)),
    shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_b, schema_ptr_, snap)),
    shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_c, schema_ptr_, snap)),
  };
  unique_ptr<CompactionInput> input(CompactionInput::Merge(merge_inputs, schema_ptr_));
  vector<shared_ptr<DiskRowSet>> result_rs;
  DoFlushAndReopen(input.get(), schema_, snap, kSmallRollThreshold, &result_rs);
  ASSERT_EQ(1, result_rs.size());
  ASSERT_EQ(0, CountRows(result_rs));
}

// Like the above test, but with all invisible rowsets.
TEST_F(TestCompaction, TestMergeMRSWithAllInvisibleRows) {
  shared_ptr<MemRowSet> mrs_a = CreateInvisibleMRS();
  shared_ptr<MemRowSet> mrs_b = CreateInvisibleMRS();
  MvccSnapshot snap(mvcc_);
  vector<shared_ptr<CompactionInput> > merge_inputs {
    shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_a, schema_ptr_, snap)),
    shared_ptr<CompactionInput>(CompactionInput::Create(*mrs_b, schema_ptr_, snap))
  };
  unique_ptr<CompactionInput> input(CompactionInput::Merge(merge_inputs, schema_ptr_));
  vector<shared_ptr<DiskRowSet>> result_rs;
  DoFlushAndReopen(input.get(), schema_, snap, kSmallRollThreshold, &result_rs);
  ASSERT_TRUE(result_rs.empty());
}

#ifdef NDEBUG
// Benchmark for the compaction merge input for the case where the inputs
// contain non-overlapping data. In this case the merge can be optimized
// to be block-wise.
TEST_F(TestCompaction, BenchmarkMergeWithoutOverlap) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  NO_FATALS(DoBenchmark<false>());
}

// Benchmark for the compaction merge input when the inputs are entirely
// overlapping (i.e the inputs become fully interleaved in the output)
TEST_F(TestCompaction, BenchmarkMergeWithOverlap) {
  SKIP_IF_SLOW_NOT_ALLOWED();
  NO_FATALS(DoBenchmark<true>());
}
#endif

// Test for KUDU-2115 to ensure that compaction selection will correctly pick
// rowsets that exist in the rowset tree (i.e. rowsets that are removed by
// concurrent compactions are not considered).
//
// Failure of this test may not necessarily mean that a compaction of the
// single rowset will occur, but rather that a potentially sub-optimal
// compaction may be scheduled.
TEST_F(TestCompaction, TestConcurrentCompactionRowSetPicking) {
  LocalTabletWriter writer(tablet().get(), &client_schema());
  KuduPartialRow row(&client_schema());
  const int kNumRowSets = 3;
  const int kNumRowsPerRowSet = 2;
  const int kExpectedRows = kNumRowSets * kNumRowsPerRowSet;

  // Flush a few overlapping rowsets.
  for (int i = 0; i < kNumRowSets; i++) {
    for (int j = 0; j < kNumRowsPerRowSet; j++) {
      const int val = i + j * 10;
      ASSERT_OK(row.SetStringCopy("key", Substitute("hello $0", val)));
      ASSERT_OK(row.SetInt32("val", val));
      ASSERT_OK(writer.Insert(row));
    }
    ASSERT_OK(tablet()->Flush());
  }
  uint64_t num_rows;
  ASSERT_OK(tablet()->CountRows(&num_rows));
  ASSERT_EQ(kExpectedRows, num_rows);

  // Schedule multiple compactions on the tablet at once. Concurrent
  // compactions should not schedule the same rowsets for compaction, and we
  // should end up with the same number of rows.
  vector<unique_ptr<thread>> threads;
  for (int i = 0; i < 10; i++) {
    threads.emplace_back(new thread([&] {
      ASSERT_OK(tablet()->Compact(Tablet::COMPACT_NO_FLAGS));
    }));
  }
  for (int i = 0; i < 10; i++) {
    threads[i]->join();
  }
  ASSERT_OK(tablet()->CountRows(&num_rows));
  ASSERT_EQ(kExpectedRows, num_rows);
}

TEST_F(TestCompaction, TestCompactionFreesDiskSpace) {
  {
    // We must force the LocalTabletWriter out of scope before measuring
    // disk space usage. Otherwise some deleted blocks are kept open for
    // reading and aren't properly deallocated by the block manager.
    LocalTabletWriter writer(tablet().get(), &client_schema());
    KuduPartialRow row(&client_schema());

    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 10; j++) {
        int val = (i * 10) + j;
        ASSERT_OK(row.SetStringCopy("key", Substitute("hello $0", val)));
        ASSERT_OK(row.SetInt32("val", val));
        ASSERT_OK(writer.Insert(row));
      }
      ASSERT_OK(tablet()->Flush());
    }
  }

  uint64_t bytes_before;
  ASSERT_OK(env_->GetFileSizeOnDiskRecursively(
      fs_manager()->GetDataRootDirs().at(0), &bytes_before));

  ASSERT_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Block deletion may happen asynchronously, so let's loop for a bit until
  // the space becomes free.
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(30);
  while (true) {
    uint64_t bytes_after;
    ASSERT_OK(env_->GetFileSizeOnDiskRecursively(
        fs_manager()->GetDataRootDirs().at(0), &bytes_after));
    LOG(INFO) << Substitute("Data disk space: $0 (before), $1 (after) ",
                            bytes_before, bytes_after);
    if (bytes_after < bytes_before) {
      break;
    } else if (MonoTime::Now() > deadline) {
      FAIL() << "Timed out waiting for compaction to reduce data block disk "
             << "space usage";
    }
    SleepFor(MonoDelta::FromMilliseconds(200));
  }
}

// Regression test for KUDU-1237, a bug in which empty flushes or compactions
// would result in orphaning near-empty cfile blocks on the disk.
TEST_F(TestCompaction, TestEmptyFlushDoesntLeakBlocks) {
  if (FLAGS_block_manager != "log") {
    LOG(WARNING) << "Test requires the log block manager";
    return;
  }

  // Fetch the metric for the number of on-disk blocks, so we can later verify
  // that we actually remove data.
  fs::LogBlockManager* lbm = down_cast<fs::LogBlockManager*>(
      harness_->fs_manager()->block_manager());

  vector<BlockId> before_block_ids;
  ASSERT_OK(lbm->GetAllBlockIds(&before_block_ids));
  ASSERT_OK(tablet()->Flush());
  vector<BlockId> after_block_ids;
  ASSERT_OK(lbm->GetAllBlockIds(&after_block_ids));

  // Sort the two collections before the comparison as GetAllBlockIds() does
  // not guarantee a deterministic order.
  std::sort(before_block_ids.begin(), before_block_ids.end(), BlockIdCompare());
  std::sort(after_block_ids.begin(), after_block_ids.end(), BlockIdCompare());

  ASSERT_EQ(after_block_ids, before_block_ids);
}

TEST_F(TestCompaction, TestCountLiveRowsOfMemRowSetFlush) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              mem_trackers_.tablet_tracker, &mrs));
  NO_FATALS(InsertRows(mrs.get(), 100, 0));
  NO_FATALS(UpdateRows(mrs.get(), 80, 0, 1));
  NO_FATALS(DeleteRows(mrs.get(), 50));
  NO_FATALS(InsertRows(mrs.get(), 10, 0));
  uint64_t count = 0;
  ASSERT_OK(mrs->CountLiveRows(&count));
  ASSERT_EQ(100 - 50 + 10, count);

  shared_ptr<DiskRowSet> rs;
  NO_FATALS(FlushMRSAndReopenNoRoll(*mrs, schema_, &rs));
  ASSERT_OK(rs->CountLiveRows(&count));
  ASSERT_EQ(100 - 50 + 10, count);
}

TEST_F(TestCompaction, TestCountLiveRowsOfDiskRowSetsCompact) {
  shared_ptr<DiskRowSet> rs1;
  {
    shared_ptr<MemRowSet> mrs;
    ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                                mem_trackers_.tablet_tracker, &mrs));
    NO_FATALS(InsertRows(mrs.get(), 100, 0));
    NO_FATALS(UpdateRows(mrs.get(), 80, 0, 1));
    NO_FATALS(DeleteRows(mrs.get(), 50, 0));
    NO_FATALS(InsertRows(mrs.get(), 10, 0));
    NO_FATALS(FlushMRSAndReopenNoRoll(*mrs, schema_, &rs1));
  }
  shared_ptr<DiskRowSet> rs2;
  {
    shared_ptr<MemRowSet> mrs;
    ASSERT_OK(MemRowSet::Create(1, schema_ptr_, log_anchor_registry_.get(),
                                mem_trackers_.tablet_tracker, &mrs));
    NO_FATALS(InsertRows(mrs.get(), 100, 1));
    NO_FATALS(UpdateRows(mrs.get(), 80, 1, 1));
    NO_FATALS(DeleteRows(mrs.get(), 50, 1));
    NO_FATALS(InsertRows(mrs.get(), 10, 1));
    NO_FATALS(FlushMRSAndReopenNoRoll(*mrs, schema_, &rs2));
  }
  shared_ptr<DiskRowSet> rs3;
  {
    shared_ptr<MemRowSet> mrs;
    ASSERT_OK(MemRowSet::Create(2, schema_ptr_, log_anchor_registry_.get(),
                                mem_trackers_.tablet_tracker, &mrs));
    NO_FATALS(InsertRows(mrs.get(), 100, 2));
    NO_FATALS(UpdateRows(mrs.get(), 80, 2, 2));
    NO_FATALS(DeleteRows(mrs.get(), 50, 2));
    NO_FATALS(InsertRows(mrs.get(), 10, 2));
    NO_FATALS(FlushMRSAndReopenNoRoll(*mrs, schema_, &rs3));
  }

  shared_ptr<DiskRowSet> result;
  vector<shared_ptr<DiskRowSet>> all_rss;
  all_rss.emplace_back(std::move(rs3));
  all_rss.emplace_back(std::move(rs1));
  all_rss.emplace_back(std::move(rs2));

  std::mt19937 gen(SeedRandom());
  std::shuffle(all_rss.begin(), all_rss.end(), gen);
  NO_FATALS(CompactAndReopenNoRoll(all_rss, schema_, &result));

  uint64_t count = 0;
  ASSERT_OK(result->CountLiveRows(&count));
  ASSERT_EQ((100 - 50 + 10) * 3, count);
}

} // namespace tablet
} // namespace kudu
