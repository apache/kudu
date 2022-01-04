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
#pragma once

#include <unistd.h>

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/logical_clock.h"
#include "kudu/common/iterator.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_mem_trackers.h"
#include "kudu/util/env.h"
#include "kudu/util/logging.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(roundtrip_num_rows, 10000,
             "Number of rows to use for the round-trip test");
DEFINE_int32(n_read_passes, 10,
             "number of times to read data for perf test");

namespace kudu {
namespace tablet {

class TestRowSet : public KuduRowSetTest {
 public:
  TestRowSet()
      : KuduRowSetTest(CreateTestSchema()),
        n_rows_(FLAGS_roundtrip_num_rows),
        op_id_(consensus::MaximumOpId()),
        clock_(Timestamp::kInitialTimestamp),
        log_anchor_registry_(new log::LogAnchorRegistry()) {
    CHECK_GT(n_rows_, 0);
  }

 protected:
  static Schema CreateTestSchema() {
    SchemaBuilder builder;
    CHECK_OK(builder.AddKeyColumn("key", STRING));
    CHECK_OK(builder.AddColumn("val", UINT32));
    return builder.BuildWithoutIds();
  }

  static Schema CreateProjection(const Schema& schema,
                                 const std::vector<std::string>& cols) {
    std::vector<ColumnSchema> col_schemas;
    std::vector<ColumnId> col_ids;
    for (const std::string& col : cols) {
      int idx = schema.find_column(col);
      CHECK_GE(idx, 0);
      col_schemas.push_back(schema.column(idx));
      col_ids.push_back(schema.column_id(idx));
    }
    return Schema(col_schemas, col_ids, 0);
  }

  void BuildRowKey(RowBuilder *rb, int row_idx) {
    char buf[256];
    FormatKey(row_idx, buf, sizeof(buf));
    rb->AddString(Slice(buf));
  }

  // Write out a test rowset with n_rows_ rows.
  // The data in the rowset looks like:
  //   ("hello <00n>", <n>)
  // ... where n is the index of the row in the rowset
  // or 0 if 'zero_vals' is true.
  // The string values are padded out to 15 digits
  void WriteTestRowSet(int n_rows = 0, bool zero_vals = false) {
    DiskRowSetWriter drsw(rowset_meta_.get(), &schema_,
                          BloomFilterSizing::BySizeAndFPRate(32*1024, 0.01f));
    DoWriteTestRowSet(n_rows, &drsw, zero_vals);
  }

  template<class WriterClass>
  void DoWriteTestRowSet(int n_rows, WriterClass *writer,
                         bool zero_vals = false) {
    if (n_rows == 0) {
      n_rows = n_rows_;
    }

    // Write rows into a new DiskRowSet.
    LOG_TIMING(INFO, "Writing rowset") {
      CHECK_OK(writer->Open());

      char buf[256];
      RowBuilder rb(&schema_);
      for (int i = 0; i < n_rows; i++) {
        CHECK_OK(writer->RollIfNecessary());
        rb.Reset();
        FormatKey(i, buf, sizeof(buf));
        rb.AddString(Slice(buf));
        rb.AddUint32(zero_vals ? 0 : i);
        CHECK_OK(WriteRow(rb.data(), writer));
      }
      CHECK_OK(writer->Finish());
    }
  }

  // Picks some number of rows from the given rowset and updates
  // them. Stores the indexes of the updated rows in *updated.
  void UpdateExistingRows(DiskRowSet *rs, float update_ratio,
                          std::unordered_set<uint32_t> *updated) {
    int to_update = static_cast<int>(n_rows_ * update_ratio);
    faststring update_buf;
    RowChangeListEncoder update(&update_buf);
    for (int i = 0; i < to_update; i++) {
      uint32_t idx_to_update = random() % n_rows_;
      uint32_t new_val = idx_to_update * 5;
      update.Reset();
      update.AddColumnUpdate(schema_.column(1), schema_.column_id(1), &new_val);
      OperationResultPB result;
      CHECK_OK(MutateRow(rs,
                         idx_to_update,
                         RowChangeList(update_buf),
                         &result));
      CHECK_EQ(1, result.mutated_stores_size());
      CHECK_EQ(rs->metadata()->id(), result.mutated_stores(0).rs_id());
      if (updated != NULL) {
        updated->insert(idx_to_update);
      }
    }
  }

  // Delete the row with the given identifier.
  Status DeleteRow(DiskRowSet *rs, uint32_t row_idx, OperationResultPB* result) {
    faststring update_buf;
    RowChangeListEncoder update(&update_buf);
    update.Reset();
    update.SetToDelete();

    return MutateRow(rs, row_idx, RowChangeList(update_buf), result);
  }

  Status UpdateRow(DiskRowSet *rs,
                   uint32_t row_idx,
                   uint32_t new_val,
                   OperationResultPB* result)  {
    faststring update_buf;
    RowChangeListEncoder update(&update_buf);
    update.Reset();
    update.AddColumnUpdate(schema_.column(1), schema_.column_id(1), &new_val);

    return MutateRow(rs, row_idx, RowChangeList(update_buf), result);
  }

  // Mutate the given row.
  Status MutateRow(DiskRowSet *rs,
                   uint32_t row_idx,
                   const RowChangeList &mutation,
                   OperationResultPB* result) {
    Schema proj_key = schema_.CreateKeyProjection();
    RowBuilder rb(&proj_key);
    BuildRowKey(&rb, row_idx);
    Arena arena(64);
    RowSetKeyProbe probe(rb.row(), &arena);

    ProbeStats stats;
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();
    Status s = rs->MutateRow(op.timestamp(), probe, mutation, op_id_, nullptr, &stats, result);
    op.FinishApplying();
    return s;
  }

  Status CheckRowPresent(const DiskRowSet &rs, uint32_t row_idx, bool *present) {
    Schema proj_key = schema_.CreateKeyProjection();
    RowBuilder rb(&proj_key);
    BuildRowKey(&rb, row_idx);
    Arena arena(64);
    RowSetKeyProbe probe(rb.row(), &arena);
    ProbeStats stats;
    return rs.CheckRowPresent(probe, nullptr, present, &stats);
  }

  // Verify the contents of the given rowset.
  // Updated rows (those whose index is present in 'updated') should have
  // a 'val' column equal to idx*5.
  // Other rows should have val column equal to idx.
  void VerifyUpdates(const DiskRowSet &rs, const std::unordered_set<uint32_t> &updated) {
    LOG_TIMING(INFO, "Reading updated rows with row iter") {
      VerifyUpdatesWithRowIter(rs, updated);
    }
  }

  void VerifyUpdatesWithRowIter(const DiskRowSet &rs,
                                const std::unordered_set<uint32_t> &updated) {
    SchemaPtr proj_val_ptr = std::make_shared<Schema>(CreateProjection(schema_, { "val" }));
    Schema& proj_val = *proj_val_ptr;

    RowIteratorOptions opts;
    opts.projection = proj_val_ptr;
    std::unique_ptr<RowwiseIterator> row_iter;
    CHECK_OK(rs.NewRowIterator(opts, &row_iter));
    CHECK_OK(row_iter->Init(nullptr));
    RowBlockMemory mem(1024);
    int batch_size = 10000;
    RowBlock dst(&proj_val, batch_size, &mem);

    int i = 0;
    while (row_iter->HasNext()) {
      mem.Reset();
      CHECK_OK(row_iter->NextBlock(&dst));
      VerifyUpdatedBlock(proj_val.ExtractColumnFromRow<UINT32>(dst.row(0), 0),
                         i, dst.nrows(), updated);
      i += dst.nrows();
    }
  }

  void VerifyUpdatedBlock(const uint32_t *from_file, int start_row, size_t n_rows,
                          const std::unordered_set<uint32_t> &updated) {
      for (int j = 0; j < n_rows; j++) {
        uint32_t idx_in_file = start_row + j;
        int expected;
        if (updated.count(idx_in_file) > 0) {
          expected = idx_in_file * 5;
        } else {
          expected = idx_in_file;
        }

        if (from_file[j] != expected) {
          FAIL() << "Incorrect value at idx " << idx_in_file
                 << ": expected=" << expected << " got=" << from_file[j];
        }
      }
  }

  // Perform a random read of the given row key,
  // asserting that the result matches 'expected_val'.
  void VerifyRandomRead(const DiskRowSet& rs, const Slice& row_key,
                        const std::string& expected_val) {
    Arena arena(256);
    ScanSpec spec;
    auto pred = ColumnPredicate::Equality(schema_.column(0), &row_key);
    spec.AddPredicate(pred);
    spec.OptimizeScan(schema_, &arena, true);

    RowIteratorOptions opts;
    SchemaPtr schema_ptr = std::make_shared<Schema>(schema_);
    opts.projection = schema_ptr;
    std::unique_ptr<RowwiseIterator> row_iter;
    CHECK_OK(rs.NewRowIterator(opts, &row_iter));
    CHECK_OK(row_iter->Init(&spec));
    std::vector<std::string> rows;
    IterateToStringList(row_iter.get(), &rows);
    std::string result = JoinStrings(rows, "\n");
    ASSERT_EQ(expected_val, result);
  }

  // Iterate over a DiskRowSet, dumping occasional rows to the console,
  // using the given schema as a projection.
  static void IterateProjection(const DiskRowSet &rs, const Schema &schema,
                                int expected_rows, bool do_log = true) {
    RowIteratorOptions opts;
    SchemaPtr schema_ptr = std::make_shared<Schema>(schema);
    opts.projection = schema_ptr;
    std::unique_ptr<RowwiseIterator> row_iter;
    CHECK_OK(rs.NewRowIterator(opts, &row_iter));
    CHECK_OK(row_iter->Init(nullptr));

    int batch_size = 1000;
    RowBlockMemory mem(1024);
    RowBlock dst(&schema, batch_size, &mem);

    int i = 0;
    int log_interval = expected_rows/20 / batch_size;
    while (row_iter->HasNext()) {
      mem.Reset();
      CHECK_OK(row_iter->NextBlock(&dst));
      i += dst.nrows();

      if (do_log) {
        KLOG_EVERY_N(INFO, log_interval) << "Got row: " << schema.DebugRow(dst.row(0));
      }
    }

    EXPECT_EQ(expected_rows, i);
  }

  void BenchmarkIterationPerformance(const DiskRowSet &rs,
                                     const std::string &log_message) {
    Schema proj_val = CreateProjection(schema_, { "val" });
    LOG_TIMING(INFO, log_message + " (val column only)") {
      for (int i = 0; i < FLAGS_n_read_passes; i++) {
        IterateProjection(rs, proj_val, n_rows_, false);
      }
    }

    Schema proj_key = CreateProjection(schema_, { "key" });
    LOG_TIMING(INFO, log_message + " (key string column only)") {
      for (int i = 0; i < FLAGS_n_read_passes; i++) {
        IterateProjection(rs, proj_key, n_rows_, false);
      }
    }

    LOG_TIMING(INFO, log_message + " (both columns)") {
      for (int i = 0; i < FLAGS_n_read_passes; i++) {
        IterateProjection(rs, schema_, n_rows_, false);
      }
    }
  }

  Status OpenTestRowSet(std::shared_ptr<DiskRowSet> *rowset) {
    return DiskRowSet::Open(rowset_meta_,
                            log_anchor_registry_.get(),
                            TabletMemTrackers(),
                            nullptr,
                            rowset);
  }

  void FormatKey(int i, char *buf, size_t buf_len) {
    snprintf(buf, buf_len, "hello %015d", i);
  }

  size_t n_rows_;
  consensus::OpId op_id_; // Generally a "fake" OpId for these tests.
  clock::LogicalClock clock_;
  MvccManager mvcc_;
  scoped_refptr<log::LogAnchorRegistry> log_anchor_registry_;
};

} // namespace tablet
} // namespace kudu
