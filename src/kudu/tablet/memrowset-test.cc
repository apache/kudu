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

#include "kudu/tablet/memrowset.h"

#include <cstdint>
#include <cstdio>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/logical_clock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowblock_memory.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tablet/txn_metadata.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/random.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(roundtrip_num_rows, 10000,
             "Number of rows to use for the round-trip test");
DEFINE_int32(num_scan_passes, 1,
             "Number of passes to run the scan portion of the round-trip test");
DEFINE_double(update_ratio, 0.2,
              "Percent of rows to be updated for the update performance test");
DEFINE_int32(times_to_update, 5000,
             "Number of updates for each row for the update performance test");

namespace kudu {
namespace tablet {

using consensus::OpId;
using log::LogAnchorRegistry;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;

class TestMemRowSet : public KuduTest {
 public:
  TestMemRowSet()
    : op_id_(consensus::MaximumOpId()),
      log_anchor_registry_(new LogAnchorRegistry()),
      schema_(CreateSchema()),
      key_schema_(schema_.CreateKeyProjection()),
      schema_ptr_(std::make_shared<Schema>(schema_)),
      key_schema_ptr_(std::make_shared<Schema>(key_schema_)),
      clock_(Timestamp::kInitialTimestamp) {
  }

  static Schema CreateSchema() {
    unique_ptr<SchemaBuilder> sb(CreateSchemaBuilder());
    return sb->Build();
  }

  static unique_ptr<SchemaBuilder> CreateSchemaBuilder() {
    unique_ptr<SchemaBuilder> builder(new SchemaBuilder());
    CHECK_OK(builder->AddKeyColumn("key", STRING));
    CHECK_OK(builder->AddColumn("val", UINT32));
    return builder;
  }

 protected:
  // Check that the given row in the memrowset contains the given data.
  void CheckValue(const shared_ptr<MemRowSet>& mrs, const string& key,
                  const string& expected_row) {
    unique_ptr<MemRowSet::Iterator> iter(mrs->NewIterator());
    ASSERT_OK(iter->Init(nullptr));

    Slice keystr_slice(key);
    Slice key_slice(reinterpret_cast<const char *>(&keystr_slice), sizeof(Slice));

    bool exact;
    ASSERT_OK(iter->SeekAtOrAfter(key_slice, &exact));
    ASSERT_TRUE(exact) << "unable to seek to key " << key;
    ASSERT_TRUE(iter->HasNext());

    vector<string> out;
    ASSERT_OK(IterateToStringList(iter.get(), &out, 1));
    ASSERT_EQ(1, out.size());
    ASSERT_EQ(expected_row, out[0]) << "bad result for key " << key;
  }

  Status CheckRowPresent(const MemRowSet &mrs,
                         const string &key, bool *present) {
    RowBuilder rb(&key_schema_);
    rb.AddString(Slice(key));
    Arena arena(64);
    RowSetKeyProbe probe(rb.row(), &arena);
    ProbeStats stats;

    return mrs.CheckRowPresent(probe, nullptr, present, &stats);
  }

  Status InsertRows(MemRowSet *mrs, int num_rows) {
    RowBuilder rb(&schema_);
    char keybuf[256];
    for (uint32_t i = 0; i < num_rows; i++) {
      rb.Reset();
      snprintf(keybuf, sizeof(keybuf), "hello %d", i);
      rb.AddString(Slice(keybuf));
      rb.AddUint32(i);
      RETURN_NOT_OK(mrs->Insert(Timestamp(i), rb.row(), op_id_));
    }

    return Status::OK();
  }

  Status InsertRow(MemRowSet *mrs, const string &key, uint32_t val) {
    ScopedOp op(&mvcc_, clock_.Now());
    RowBuilder rb(&schema_);
    rb.AddString(key);
    rb.AddUint32(val);
    op.StartApplying();
    Status s = mrs->Insert(op.timestamp(), rb.row(), op_id_);
    op.FinishApplying();
    return s;
  }

  Status UpdateRow(MemRowSet *mrs,
                   const string &key,
                   uint32_t new_val,
                   OperationResultPB* result) {
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();

    mutation_buf_.clear();
    RowChangeListEncoder update(&mutation_buf_);
    update.AddColumnUpdate(schema_.column(1), schema_.column_id(1), &new_val);

    RowBuilder rb(&key_schema_);
    rb.AddString(Slice(key));
    Arena arena(64);
    RowSetKeyProbe probe(rb.row(), &arena);
    ProbeStats stats;
    Status s = mrs->MutateRow(op.timestamp(),
                              probe,
                              RowChangeList(mutation_buf_),
                              op_id_,
                              nullptr,
                              &stats,
                              result);
    op.FinishApplying();
    return s;
  }

  Status DeleteRow(MemRowSet *mrs, const string &key, OperationResultPB* result) {
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();

    mutation_buf_.clear();
    RowChangeListEncoder update(&mutation_buf_);
    update.SetToDelete();

    RowBuilder rb(&key_schema_);
    rb.AddString(Slice(key));
    Arena arena(64);
    RowSetKeyProbe probe(rb.row(), &arena);
    ProbeStats stats;
    Status s = mrs->MutateRow(op.timestamp(),
                              probe,
                              RowChangeList(mutation_buf_),
                              op_id_,
                              nullptr,
                              &stats,
                              result);
    op.FinishApplying();
    return s;
  }

  int ScanAndCount(MemRowSet* mrs, const RowIteratorOptions& opts) {
    unique_ptr<MemRowSet::Iterator> iter(mrs->NewIterator(opts));
    CHECK_OK(iter->Init(nullptr));

    RowBlockMemory mem(1024);
    RowBlock block(&schema_, 100, &mem);
    int fetched = 0;
    while (iter->HasNext()) {
      mem.Reset();
      CHECK_OK(iter->NextBlock(&block));
      fetched += block.selection_vector()->CountSelected();
    }
    return fetched;
  }

  // Checks the number of rows in the range (-Inf, snap].
  bool CheckRowsAtSnapshot(MemRowSet* mrs, const MvccSnapshot& snap, int expected_rows) {
    RowIteratorOptions opts;
    opts.snap_to_include = snap;
    opts.projection = schema_ptr_;
    return expected_rows == ScanAndCount(mrs, opts);
  }

  // Checks the number of mutations in the range (snap_to_exc, snap_to_inc].
  bool CheckRowsBetween(MemRowSet* mrs, const MvccSnapshot& snap_to_exc,
                        const MvccSnapshot& snap_to_inc, int expected_rows) {
    RowIteratorOptions opts;
    opts.snap_to_exclude = snap_to_exc;
    opts.snap_to_include = snap_to_inc;
    opts.projection = schema_ptr_;
    return expected_rows == ScanAndCount(mrs, opts);
  }

  Status GenerateTestData(MemRowSet* mrs) {
    // row 0 - insert
    // row 1 - insert, update
    // row 2 - insert, delete
    // row 3 - insert, update, delete
    // row 4 - insert, update, delete, reinsert
    // row 5 - insert, update, update, delete, reinsert
    // row 6 - insert, delete, reinsert, delete
    RETURN_NOT_OK(InsertRow(mrs, "row 0", 0));
    RETURN_NOT_OK(InsertRow(mrs, "row 1", 0));
    OperationResultPB result;
    RETURN_NOT_OK(UpdateRow(mrs, "row 1", 1, &result));
    RETURN_NOT_OK(InsertRow(mrs, "row 2", 0));
    RETURN_NOT_OK(DeleteRow(mrs, "row 2", &result));
    RETURN_NOT_OK(InsertRow(mrs, "row 3", 0));
    RETURN_NOT_OK(UpdateRow(mrs, "row 3", 1, &result));
    RETURN_NOT_OK(DeleteRow(mrs, "row 3", &result));
    RETURN_NOT_OK(InsertRow(mrs, "row 4", 0));
    RETURN_NOT_OK(UpdateRow(mrs, "row 4", 1, &result));
    RETURN_NOT_OK(DeleteRow(mrs, "row 4", &result));
    RETURN_NOT_OK(InsertRow(mrs, "row 4", 2));
    RETURN_NOT_OK(InsertRow(mrs, "row 5", 0));
    RETURN_NOT_OK(UpdateRow(mrs, "row 5", 1, &result));
    RETURN_NOT_OK(UpdateRow(mrs, "row 5", 2, &result));
    RETURN_NOT_OK(DeleteRow(mrs, "row 5", &result));
    RETURN_NOT_OK(InsertRow(mrs, "row 5", 3));
    RETURN_NOT_OK(InsertRow(mrs, "row 6", 0));
    RETURN_NOT_OK(DeleteRow(mrs, "row 6", &result));
    RETURN_NOT_OK(InsertRow(mrs, "row 6", 1));
    RETURN_NOT_OK(DeleteRow(mrs, "row 6", &result));

    return Status::OK();
  }

  OpId op_id_;
  scoped_refptr<LogAnchorRegistry> log_anchor_registry_;

  faststring mutation_buf_;
  const Schema schema_;
  const Schema key_schema_;
  const SchemaPtr schema_ptr_;
  const SchemaPtr key_schema_ptr_;
  clock::LogicalClock clock_;
  MvccManager mvcc_;
};


TEST_F(TestMemRowSet, TestInsertAndIterate) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  ASSERT_OK(InsertRow(mrs.get(), "hello world", 12345));
  ASSERT_OK(InsertRow(mrs.get(), "goodbye world", 54321));

  ASSERT_EQ(2, mrs->entry_count());

  unique_ptr<MemRowSet::Iterator> iter(mrs->NewIterator());
  ASSERT_OK(iter->Init(nullptr));

  // The first row returned from the iterator should
  // be "goodbye" because 'g' sorts before 'h'
  ASSERT_TRUE(iter->HasNext());
  MRSRow row = iter->GetCurrentRow();
  EXPECT_EQ(R"((string key="goodbye world", uint32 val=54321))", schema_.DebugRow(row));

  // Next row should be 'hello world'
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->HasNext());
  row = iter->GetCurrentRow();
  EXPECT_EQ(R"((string key="hello world", uint32 val=12345))", schema_.DebugRow(row));

  ASSERT_FALSE(iter->Next());
  ASSERT_FALSE(iter->HasNext());
}

TEST_F(TestMemRowSet, TestInsertAndIterateCompoundKey) {

  SchemaBuilder builder;
  ASSERT_OK(builder.AddKeyColumn("key1", STRING));
  ASSERT_OK(builder.AddKeyColumn("key2", INT32));
  ASSERT_OK(builder.AddColumn("val", UINT32));
  SchemaPtr compound_key_schema_ptr = std::make_shared<Schema>(builder.Build());
  Schema& compound_key_schema = *compound_key_schema_ptr.get();

  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, compound_key_schema_ptr, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  RowBuilder rb(&compound_key_schema);
  {
    ScopedOp op(&mvcc_, clock_.Now());
    op.StartApplying();
    rb.AddString(string("hello world"));
    rb.AddInt32(1);
    rb.AddUint32(12345);
    Status row1 = mrs->Insert(op.timestamp(), rb.row(), op_id_);
    ASSERT_OK(row1);
    op.FinishApplying();
  }

  {
    ScopedOp op2(&mvcc_, clock_.Now());
    op2.StartApplying();
    rb.Reset();
    rb.AddString(string("goodbye world"));
    rb.AddInt32(2);
    rb.AddUint32(54321);
    Status row2 = mrs->Insert(op2.timestamp(), rb.row(), op_id_);
    ASSERT_OK(row2);
    op2.FinishApplying();
  }

  {
    ScopedOp op3(&mvcc_, clock_.Now());
    op3.StartApplying();
    rb.Reset();
    rb.AddString(string("goodbye world"));
    rb.AddInt32(1);
    rb.AddUint32(12345);
    Status row3 = mrs->Insert(op3.timestamp(), rb.row(), op_id_);
    ASSERT_OK(row3);
    op3.FinishApplying();
  }

  ASSERT_EQ(3, mrs->entry_count());

  unique_ptr<MemRowSet::Iterator> iter(mrs->NewIterator());
  ASSERT_OK(iter->Init(nullptr));

  // The first row returned from the iterator should
  // be "goodbye" (row3) sorted on the second key
  ASSERT_TRUE(iter->HasNext());
  MRSRow row = iter->GetCurrentRow();
  EXPECT_EQ(R"((string key1="goodbye world", int32 key2=1, uint32 val=12345))",
            compound_key_schema.DebugRow(row));

  // Next row should be "goodbye" (row2)
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->HasNext());
  row = iter->GetCurrentRow();
  EXPECT_EQ(R"((string key1="goodbye world", int32 key2=2, uint32 val=54321))",
            compound_key_schema.DebugRow(row));

  // Next row should be 'hello world' (row1)
  ASSERT_TRUE(iter->Next());
  ASSERT_TRUE(iter->HasNext());
  row = iter->GetCurrentRow();
  EXPECT_EQ(R"((string key1="hello world", int32 key2=1, uint32 val=12345))",
            compound_key_schema.DebugRow(row));

  ASSERT_FALSE(iter->Next());
  ASSERT_FALSE(iter->HasNext());
}

// Test that inserting duplicate key data fails with Status::AlreadyPresent
TEST_F(TestMemRowSet, TestInsertDuplicate) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  ASSERT_OK(InsertRow(mrs.get(), "hello world", 12345));
  Status s = InsertRow(mrs.get(), "hello world", 12345);
  ASSERT_TRUE(s.IsAlreadyPresent()) << "bad status: " << s.ToString();
}

// Test for updating rows in memrowset
TEST_F(TestMemRowSet, TestUpdate) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  ASSERT_OK(InsertRow(mrs.get(), "hello world", 1));

  // Validate insertion
  CheckValue(mrs, "hello world", R"((string key="hello world", uint32 val=1))");

  // Update a key which exists.
  OperationResultPB result;
  ASSERT_OK(UpdateRow(mrs.get(), "hello world", 2, &result));
  ASSERT_EQ(1, result.mutated_stores_size());
  ASSERT_EQ(0L, result.mutated_stores(0).mrs_id());

  // Validate the updated value
  CheckValue(mrs, "hello world", R"((string key="hello world", uint32 val=2))");

  // Try to update a key which doesn't exist - should return NotFound
  result.Clear();
  Status s = UpdateRow(mrs.get(), "does not exist", 3, &result);
  ASSERT_TRUE(s.IsNotFound()) << "bad status: " << s.ToString();
  ASSERT_EQ(0, result.mutated_stores_size());
}

// Test which inserts many rows into memrowset and checks for their
// existence
TEST_F(TestMemRowSet, TestInsertCopiesToArena) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  ASSERT_OK(InsertRows(mrs.get(), 100));
  // Validate insertion
  char keybuf[256];
  for (uint32_t i = 0; i < 100; i++) {
    snprintf(keybuf, sizeof(keybuf), "hello %d", i);
    CheckValue(mrs, keybuf,
               StringPrintf(R"((string key="%s", uint32 val=%d))", keybuf, i));
  }
}

TEST_F(TestMemRowSet, TestDelete) {
  const char kRowKey[] = "hello world";
  bool present;

  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  // Insert row.
  ASSERT_OK(InsertRow(mrs.get(), kRowKey, 1));
  MvccSnapshot snapshot_before_delete(mvcc_);

  // CheckRowPresent should return true
  ASSERT_OK(CheckRowPresent(*mrs, kRowKey, &present));
  EXPECT_TRUE(present);

  // Delete it.
  OperationResultPB result;
  ASSERT_OK(DeleteRow(mrs.get(), kRowKey, &result));
  ASSERT_EQ(1, result.mutated_stores_size());
  ASSERT_EQ(0L, result.mutated_stores(0).mrs_id());

  MvccSnapshot snapshot_after_delete(mvcc_);

  // CheckRowPresent should return false
  ASSERT_OK(CheckRowPresent(*mrs, kRowKey, &present));
  EXPECT_FALSE(present);

  // Trying to Delete again or Update should get an error.
  result.Clear();
  Status s = DeleteRow(mrs.get(), kRowKey, &result);
  ASSERT_TRUE(s.IsNotFound()) << "Unexpected status: " << s.ToString();
  ASSERT_EQ(0, result.mutated_stores_size());

  result.Clear();
  s = UpdateRow(mrs.get(), kRowKey, 12345, &result);
  ASSERT_TRUE(s.IsNotFound()) << "Unexpected status: " << s.ToString();
  ASSERT_EQ(0, result.mutated_stores_size());

  // Re-insert a new row with the same key.
  ASSERT_OK(InsertRow(mrs.get(), kRowKey, 2));
  MvccSnapshot snapshot_after_reinsert(mvcc_);

  // CheckRowPresent should now return true
  ASSERT_OK(CheckRowPresent(*mrs, kRowKey, &present));
  EXPECT_TRUE(present);

  // Verify the MVCC contents of the memrowset.
  // NOTE: the REINSERT has timestamp 4 because of the two failed attempts
  // at mutating the deleted row above -- each of them grabs a timestamp even
  // though it doesn't actually make any successful mutations.
  vector<string> rows;
  ASSERT_OK(mrs->DebugDump(&rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(R"(@1: row (string key="hello world", uint32 val=1) mutations=)"
            "[@2(DELETE), @5(REINSERT val=2)]",
            rows[0]);

  // Verify that iterating the rowset at the first snapshot shows the row.
  RowIteratorOptions opts;
  opts.projection = schema_ptr_;
  opts.snap_to_include = snapshot_before_delete;
  ASSERT_OK(DumpRowSet(*mrs, opts, &rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(R"((string key="hello world", uint32 val=1))", rows[0]);

  // Verify that iterating the rowset at the snapshot where it's deleted
  // doesn't show the row.
  opts.snap_to_include = snapshot_after_delete;
  ASSERT_OK(DumpRowSet(*mrs, opts, &rows));
  ASSERT_EQ(0, rows.size());

  // Verify that iterating the rowset after it's re-inserted shows the row.
  opts.snap_to_include = snapshot_after_reinsert;
  ASSERT_OK(DumpRowSet(*mrs, opts, &rows));
  ASSERT_EQ(1, rows.size());
  EXPECT_EQ(R"((string key="hello world", uint32 val=2))", rows[0]);
}

// Test for basic operations.
// Can operate as a benchmark by setting --roundtrip_num_rows to a high value like 10M
TEST_F(TestMemRowSet, TestMemRowSetInsertCountAndScan) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  LOG_TIMING(INFO, "Inserting rows") {
    ASSERT_OK(InsertRows(mrs.get(), FLAGS_roundtrip_num_rows));
  }

  LOG_TIMING(INFO, "Counting rows") {
    int count = mrs->entry_count();
    ASSERT_EQ(FLAGS_roundtrip_num_rows, count);
  }

  for (int i = 0; i < FLAGS_num_scan_passes; i++) {
    RowIteratorOptions opts;
    opts.projection = schema_ptr_;
    opts.snap_to_include = MvccSnapshot(Timestamp(0));
    LOG_TIMING(INFO, "Scanning rows where none are committed") {
      ASSERT_EQ(0, ScanAndCount(mrs.get(), opts));
    }

    opts.snap_to_include = MvccSnapshot(Timestamp(FLAGS_roundtrip_num_rows + 1));
    LOG_TIMING(INFO, "Scanning rows where all are committed") {
      ASSERT_EQ(FLAGS_roundtrip_num_rows, ScanAndCount(mrs.get(), opts));
    }
  }
}

// Test that scanning at past MVCC snapshots will hide rows which are
// not committed in that snapshot.
TEST_F(TestMemRowSet, TestInsertionMVCC) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));
  vector<MvccSnapshot> snapshots;

  // Insert 5 rows in op 0 through 4
  for (uint32_t i = 0; i < 5; i++) {
    {
      ScopedOp op(&mvcc_, clock_.Now());
      op.StartApplying();
      RowBuilder rb(&schema_);
      char keybuf[256];
      rb.Reset();
      snprintf(keybuf, sizeof(keybuf), "op%d", i);
      rb.AddString(Slice(keybuf));
      rb.AddUint32(i);
      ASSERT_OK_FAST(mrs->Insert(op.timestamp(), rb.row(), op_id_));
      op.FinishApplying();
    }

    // Op is committed. Save the snapshot after this commit.
    snapshots.emplace_back(mvcc_);
  }
  LOG(INFO) << "MemRowSet after inserts:";
  ASSERT_OK(mrs->DebugDump(nullptr));

  ASSERT_EQ(5, snapshots.size());
  RowIteratorOptions opts;
  opts.projection = schema_ptr_;
  for (int i = 0; i < 5; i++) {
    SCOPED_TRACE(i);
    // Each snapshot 'i' is taken after row 'i' was committed.
    opts.snap_to_include = snapshots[i];
    vector<string> rows;
    ASSERT_OK(DumpRowSet(*mrs, opts, &rows));
    ASSERT_EQ(1 + i, rows.size());
    string expected = StringPrintf(R"((string key="op%d", uint32 val=%d))", i, i);
    ASSERT_EQ(expected, rows[i]);
  }
}

// Test that updates respect MVCC -- i.e. that scanning with a past MVCC snapshot
// will yield old versions of a row which has been updated.
TEST_F(TestMemRowSet, TestUpdateMVCC) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  // Insert a row ("myrow", 0)
  ASSERT_OK(InsertRow(mrs.get(), "my row", 0));

  vector<MvccSnapshot> snapshots;
  // First snapshot is after insertion
  snapshots.emplace_back(mvcc_);

  // Update the row 5 times (setting its int column to increasing ints 1-5)
  for (uint32_t i = 1; i <= 5; i++) {
    OperationResultPB result;
    ASSERT_OK(UpdateRow(mrs.get(), "my row", i, &result));
    ASSERT_EQ(1, result.mutated_stores_size());
    ASSERT_EQ(0L, result.mutated_stores(0).mrs_id());

    // Op is committed. Save the snapshot after this commit.
    snapshots.emplace_back(mvcc_);
  }

  LOG(INFO) << "MemRowSet after updates:";
  ASSERT_OK(mrs->DebugDump(nullptr));

  // Validate that each snapshot returns the expected value
  ASSERT_EQ(6, snapshots.size());
  RowIteratorOptions opts;
  opts.projection = schema_ptr_;
  for (int i = 0; i <= 5; i++) {
    SCOPED_TRACE(i);
    vector<string> rows;
    opts.snap_to_include = snapshots[i];
    ASSERT_OK(DumpRowSet(*mrs, opts, &rows));
    ASSERT_EQ(1, rows.size());

    string expected = StringPrintf(R"((string key="my row", uint32 val=%d))", i);
    LOG(INFO) << "Reading with snapshot " << snapshots[i].ToString() << ": "
              << rows[0];
    EXPECT_EQ(expected, rows[0]);
  }
}

class ParameterizedTestMemRowSet : public TestMemRowSet,
                                   public ::testing::WithParamInterface<std::tuple<bool, bool>> {
};


// Tests the Cartesian product of two boolean parameters:
// 1. Whether to include deleted rows in the scan.
// 2. Whether to include the "is deleted" virtual column in the scan's projection.
INSTANTIATE_TEST_SUITE_P(RowIteratorOptionsPermutations, ParameterizedTestMemRowSet,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool()));

TEST_P(ParameterizedTestMemRowSet, TestScanSnapToExclude) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  // Sequence of operations:
  // @1
  // @2 INSERT key=row val=0
  // @3 UPDATE key=row val=1
  // @4 DELETE key=row
  // @5 INSERT key=row val=2
  vector<MvccSnapshot> snaps;
  snaps.emplace_back(mvcc_);
  ASSERT_OK(InsertRow(mrs.get(), "row", 0));
  snaps.emplace_back(mvcc_);
  OperationResultPB result;
  ASSERT_OK(UpdateRow(mrs.get(), "row", 1, &result));
  snaps.emplace_back(mvcc_);
  ASSERT_OK(DeleteRow(mrs.get(), "row", &result));
  snaps.emplace_back(mvcc_);
  ASSERT_OK(InsertRow(mrs.get(), "row", 2));
  snaps.emplace_back(mvcc_);

  bool include_deleted_rows = std::get<0>(GetParam());
  bool add_vc_is_deleted = std::get<1>(GetParam());

  auto DumpAndCheck = [&](const MvccSnapshot& exclude,
                          const MvccSnapshot& include,
                          boost::optional<int> row_value,
                          bool is_deleted = false) {
    // Set up the iterator options.
    unique_ptr<SchemaBuilder> sb = CreateSchemaBuilder();
    if (add_vc_is_deleted) {
      const bool kFalse = false;
      ASSERT_OK(sb->AddColumn("deleted", IS_DELETED,
                              /*is_nullable=*/false,
                              &kFalse, /*write_default=*/nullptr));
    }
    SchemaPtr projection_ptr = std::make_shared<Schema>(sb->Build());
    RowIteratorOptions opts;
    opts.projection = projection_ptr;
    opts.snap_to_include = include;
    opts.snap_to_exclude = exclude;
    opts.include_deleted_rows = include_deleted_rows;

    // Iterate.
    vector<string> rows;
    ASSERT_OK(DumpRowSet(*mrs, opts, &rows));

    // Test the results.
    if (row_value.is_initialized()) {
      ASSERT_EQ(1, rows.size());
      string expected;
      StrAppend(&expected, "(string key=\"row\", uint32 val=");
      StrAppend(&expected, row_value.get());
      if (add_vc_is_deleted) {
        StrAppend(&expected, ", is_deleted deleted=");
        StrAppend(&expected, is_deleted ? "true" : "false");
      }
      StrAppend(&expected, ")");
      ASSERT_EQ(expected, rows[0]);
    } else {
      ASSERT_TRUE(rows.empty());
    }
  };

  // Captures zero rows; a snapshot range [x, x) does not include anything.
  for (const auto& s : snaps) {
    NO_FATALS(DumpAndCheck(s, s, boost::none));
  }

  // If we include deleted rows, the row's value will be 1 due to the UPDATE
  // that preceeded it.
  boost::optional<int> deleted_v = include_deleted_rows ? boost::optional<int>(1) : boost::none;

  {
    NO_FATALS(DumpAndCheck(snaps[0], snaps[1], 0)); // INSERT
    NO_FATALS(DumpAndCheck(snaps[1], snaps[2], 1)); // UPDATE
    NO_FATALS(DumpAndCheck(snaps[2], snaps[3], deleted_v, true)); // DELETE
    NO_FATALS(DumpAndCheck(snaps[3], snaps[4], 2)); // REINSERT
  }

  {
    NO_FATALS(DumpAndCheck(snaps[0], snaps[2], 1)); // INSERT, UPDATE
    NO_FATALS(DumpAndCheck(snaps[1], snaps[3], deleted_v, true)); // UPDATE, DELETE
    NO_FATALS(DumpAndCheck(snaps[2], snaps[4], 2)); // DELETE, REINSERT
  }

  {
    NO_FATALS(DumpAndCheck(snaps[0], snaps[3], boost::none)); // INSERT, UPDATE, DELETE
    NO_FATALS(DumpAndCheck(snaps[1], snaps[4], 2)); // UPDATE, DELETE, REINSERT
  }

  NO_FATALS(DumpAndCheck(snaps[0], snaps[4], 2)); // INSERT, UPDATE, DELETE, REINSERT
}

TEST_F(TestMemRowSet, TestScanIncludeDeletedRows) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));
  ASSERT_OK(GenerateTestData(mrs.get()));

  RowIteratorOptions opts;
  opts.projection = schema_ptr_;
  opts.snap_to_include = MvccSnapshot::CreateSnapshotIncludingAllOps();
  ASSERT_EQ(4, ScanAndCount(mrs.get(), opts));

  opts.include_deleted_rows = true;
  ASSERT_EQ(7, ScanAndCount(mrs.get(), opts));
}

TEST_F(TestMemRowSet, TestScanVirtualColumnIsDeleted) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));
  ASSERT_OK(GenerateTestData(mrs.get()));

  SchemaBuilder sb;
  ASSERT_OK(sb.AddKeyColumn("key", STRING));
  ASSERT_OK(sb.AddColumn("val", UINT32));
  const bool kFalse = false;
  ASSERT_OK(sb.AddColumn("deleted", IS_DELETED,
                         /*is_nullable=*/false,
                         &kFalse, /*write_default=*/nullptr));
  SchemaPtr projection_ptr = std::make_shared<Schema>(sb.Build());

  RowIteratorOptions opts;
  opts.projection = projection_ptr;
  opts.snap_to_include = MvccSnapshot::CreateSnapshotIncludingAllOps();
  opts.include_deleted_rows = true;
  vector<string> rows;
  ASSERT_OK(DumpRowSet(*mrs, opts, &rows));
  ASSERT_EQ(7, rows.size());
  for (const auto& row_idx_present : { 0, 1, 4, 5 }) {
    ASSERT_STR_CONTAINS(rows[row_idx_present], "=false");
  }
  for (const auto& row_idx_deleted : { 2, 3, 6 }) {
    ASSERT_STR_CONTAINS(rows[row_idx_deleted], "=true");
  }
}

// Test for update performance.
// Can simulates zipfian distribution of updates by setting --update_ratio to a small value
// and --times_to_update to a high value.
TEST_F(TestMemRowSet, TestMemRowSetUpdatePerformance) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));
  int num_rows = 1000;
  LOG_TIMING(INFO, "Inserting rows") {
    ASSERT_OK(InsertRows(mrs.get(), num_rows));
  }

  LOG_TIMING(INFO, "Counting rows") {
    int count = mrs->entry_count();
    ASSERT_EQ(num_rows, count);
  }

  int nums_to_update = FLAGS_update_ratio * num_rows;
  unordered_set<int> rows_to_update;
  Random rand(SeedRandom());
  while (rows_to_update.size() < nums_to_update) {
    int next = rand.Uniform(num_rows);
    rows_to_update.insert(next);
  }

  LOG_TIMING(INFO, "Updating rows") {
    for (int i = 0; i < FLAGS_times_to_update; ++i) {
      for (auto row_idx : rows_to_update) {
        OperationResultPB result;
        string key = "hello " + std::to_string(row_idx);
        ASSERT_OK(UpdateRow(mrs.get(), key, i, &result));
        ASSERT_EQ(1, result.mutated_stores_size());
        ASSERT_EQ(0L, result.mutated_stores(0).mrs_id());
      }
    }
  }
}

TEST_F(TestMemRowSet, TestCountLiveRows) {
  shared_ptr<MemRowSet> mrs;
  ASSERT_OK(MemRowSet::Create(0, schema_ptr_, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));

  const auto CheckLiveRowsCount = [&](int64_t expect) {
    uint64_t count = 0;
    ASSERT_OK(mrs->CountLiveRows(&count));
    ASSERT_EQ(expect, count);
  };

  NO_FATALS(CheckLiveRowsCount(0));
  ASSERT_OK(GenerateTestData(mrs.get()));
  NO_FATALS(CheckLiveRowsCount(4));

  ASSERT_OK(InsertRow(mrs.get(), "liverow 0", 0));
  NO_FATALS(CheckLiveRowsCount(5));
  ASSERT_OK(InsertRow(mrs.get(), "liverow 1", 0));
  NO_FATALS(CheckLiveRowsCount(6));

  OperationResultPB result;
  ASSERT_OK(DeleteRow(mrs.get(), "liverow 0", &result));
  NO_FATALS(CheckLiveRowsCount(5));

  ASSERT_OK(InsertRow(mrs.get(), "liverow 0", 0));
  NO_FATALS(CheckLiveRowsCount(6));

  ASSERT_OK(UpdateRow(mrs.get(), "liverow 0", 1, &result));
  NO_FATALS(CheckLiveRowsCount(6));
}

// Test that rows inserted as a part of a transaction only get returned if the
// transaction is committed in the iteration snapshot.
TEST_F(TestMemRowSet, TestCommittedTransactionalRows) {
  const int64_t kTxnId = 0;
  shared_ptr<MemRowSet> mrs;
  scoped_refptr<TxnMetadata> txn_meta(new TxnMetadata);
  ASSERT_OK(MemRowSet::Create(/*mrs_id*/0, schema_ptr_, kTxnId,
                              txn_meta, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));
  MvccSnapshot latest_with_none_applied = MvccSnapshot(mvcc_);
  ASSERT_OK(InsertRow(mrs.get(), "hello world", 12345));
  MvccSnapshot latest_with_one_applied = MvccSnapshot(mvcc_);
  ASSERT_OK(InsertRow(mrs.get(), "goodbye world", 54321));
  MvccSnapshot latest_with_two_applied = MvccSnapshot(mvcc_);

  ASSERT_EQ(2, mrs->entry_count());

  // Iterating through the MRS with a snapshot that doesn't have the
  // transaction committed should yield no rows.
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_with_two_applied, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_with_one_applied, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_with_none_applied, 0));

  // Emulate transaction commit by starting an MVCC op timestamp higher than
  // the transaction so ops with lower timestamps are considered applied.
  //
  // Only once we set the commit timestamp in the txn metadata and finish the
  // op will iteration be able to read any rows.
  auto mvcc_op_ts = clock_.Now();
  ScopedOp commit_op(&mvcc_, mvcc_op_ts);
  txn_meta->set_commit_mvcc_op_timestamp(mvcc_op_ts);
  MvccSnapshot snap_after_begin_commit = MvccSnapshot(Timestamp(mvcc_op_ts.value() + 1));
  MvccSnapshot snap_before_begin_commit = MvccSnapshot(Timestamp(mvcc_op_ts.value()));
  MvccSnapshot latest_after_begin_commit = MvccSnapshot(mvcc_);
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), snap_after_begin_commit, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), snap_before_begin_commit, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_after_begin_commit, 0));

  auto commit_ts = clock_.Now();
  commit_op.StartApplying();
  txn_meta->set_commit_timestamp(commit_ts);
  commit_op.FinishApplying();

  MvccSnapshot snap_after_commit = MvccSnapshot(Timestamp(commit_ts.value() + 1));
  MvccSnapshot snap_before_commit = MvccSnapshot(commit_ts);
  MvccSnapshot latest_after_commit = MvccSnapshot(mvcc_);
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), snap_after_commit, 2));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), snap_before_commit, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_after_commit, 2));

  // Scanning for the rows inserted between the snapshots that didn't have
  // the commit and the snapshot with the commit, we should see the rows.
  NO_FATALS(CheckRowsBetween(mrs.get(), latest_with_two_applied, latest_after_commit, 2));
  NO_FATALS(CheckRowsBetween(mrs.get(), latest_with_one_applied, latest_after_commit, 2));
  NO_FATALS(CheckRowsBetween(mrs.get(), latest_with_none_applied, latest_after_commit, 2));
  NO_FATALS(CheckRowsBetween(mrs.get(), snap_before_commit, latest_after_commit, 2));

  // If we scan for rows in between snapshots with the commit, we shouldn't
  // see anything.
  NO_FATALS(CheckRowsBetween(mrs.get(), snap_after_commit, latest_after_commit, 0));
  NO_FATALS(CheckRowsBetween(mrs.get(), latest_after_commit, latest_after_commit, 0));
}

// Like the above test, but testing the behavior when aborting before beginning
// to commit.
TEST_F(TestMemRowSet, TestAbortBeforeBeginningToCommitTransactionalRows) {
  const int64_t kTxnId = 0;
  shared_ptr<MemRowSet> mrs;
  scoped_refptr<TxnMetadata> txn_meta(new TxnMetadata);
  ASSERT_OK(MemRowSet::Create(/*mrs_id*/0, schema_ptr_, kTxnId,
                              txn_meta, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));
  MvccSnapshot latest_with_none_applied = MvccSnapshot(mvcc_);
  ASSERT_OK(InsertRow(mrs.get(), "hello world", 12345));
  MvccSnapshot latest_with_one_applied = MvccSnapshot(mvcc_);
  ASSERT_OK(InsertRow(mrs.get(), "goodbye world", 54321));
  MvccSnapshot latest_with_two_applied = MvccSnapshot(mvcc_);

  ASSERT_EQ(2, mrs->entry_count());
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_with_two_applied, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_with_one_applied, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_with_none_applied, 0));
  txn_meta->set_aborted();
  MvccSnapshot latest_after_abort = MvccSnapshot(mvcc_);

  // No matter how we iterate, we should see no rows.
  const vector<MvccSnapshot> all_snaps = {
      latest_with_none_applied, latest_with_one_applied, latest_with_two_applied, latest_after_abort
  };
  for (const auto& left_snap : all_snaps) {
    for (const auto& right_snap : all_snaps) {
      NO_FATALS(CheckRowsBetween(mrs.get(), left_snap, right_snap, 0));
    }
  }
}

// Like the above test, but testing the behavior when aborting after beginning
// to commit.
TEST_F(TestMemRowSet, TestAbortAfterBeginningToCommitTransactionalRows) {
  const int64_t kTxnId = 0;
  shared_ptr<MemRowSet> mrs;
  scoped_refptr<TxnMetadata> txn_meta(new TxnMetadata);
  ASSERT_OK(MemRowSet::Create(/*mrs_id*/0, schema_ptr_, kTxnId,
                              txn_meta, log_anchor_registry_.get(),
                              MemTracker::GetRootTracker(), &mrs));
  MvccSnapshot latest_with_none_applied = MvccSnapshot(mvcc_);
  ASSERT_OK(InsertRow(mrs.get(), "hello world", 12345));
  MvccSnapshot latest_with_one_applied = MvccSnapshot(mvcc_);
  ASSERT_OK(InsertRow(mrs.get(), "goodbye world", 54321));
  MvccSnapshot latest_with_two_applied = MvccSnapshot(mvcc_);

  ASSERT_EQ(2, mrs->entry_count());
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_with_two_applied, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_with_one_applied, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_with_none_applied, 0));

  // Start beginning to commit, but abort without finishing the commit.
  auto mvcc_op_ts = clock_.Now();
  ScopedOp commit_op(&mvcc_, mvcc_op_ts);
  txn_meta->set_commit_mvcc_op_timestamp(mvcc_op_ts);
  MvccSnapshot snap_after_begin_commit = MvccSnapshot(Timestamp(mvcc_op_ts.value() + 1));
  MvccSnapshot snap_before_begin_commit = MvccSnapshot(Timestamp(mvcc_op_ts.value()));
  MvccSnapshot latest_after_begin_commit = MvccSnapshot(mvcc_);
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), snap_after_begin_commit, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), snap_before_begin_commit, 0));
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_after_begin_commit, 0));
  txn_meta->set_aborted();
  commit_op.Abort();
  MvccSnapshot latest_after_abort = MvccSnapshot(mvcc_);

  // No matter how we iterate, we should see no rows.
  NO_FATALS(CheckRowsAtSnapshot(mrs.get(), latest_after_abort, 0));
  const vector<MvccSnapshot> all_snaps = {
      latest_with_none_applied, latest_with_one_applied, latest_with_two_applied,
      snap_after_begin_commit, snap_before_begin_commit, latest_after_begin_commit
  };
  for (const auto& left_snap : all_snaps) {
    for (const auto& right_snap : all_snaps) {
      NO_FATALS(CheckRowsBetween(mrs.get(), left_snap, right_snap, 0));
    }
  }
}

} // namespace tablet
} // namespace kudu
