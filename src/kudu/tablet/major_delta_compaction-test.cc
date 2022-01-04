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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/fs/io_context.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_double(cfile_inject_corruption);

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

using strings::Substitute;

class TestMajorDeltaCompaction : public KuduRowSetTest {
 public:
  TestMajorDeltaCompaction() :
      KuduRowSetTest(Schema({ ColumnSchema("key", STRING),
                              ColumnSchema("val1", INT32),
                              ColumnSchema("val2", STRING),
                              ColumnSchema("val3", INT32),
                              ColumnSchema("val4", STRING) }, 1)) {
  }

  struct ExpectedRow {
    string key;
    int32_t val1;
    string val2;
    int32_t val3;
    string val4;

    string Formatted() const {
      return strings::Substitute(
        R"((string key="$0", int32 val1=$1, string val2="$2", int32 val3=$3, string val4="$4"))",
        key, val1, val2, val3, val4);
    }
  };

  virtual void SetUp() OVERRIDE {
    KuduRowSetTest::SetUp();
  }

  // Insert data into tablet_, setting up equivalent state in
  // expected_state_.
  void WriteTestTablet(int nrows) {
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow ins_row(&client_schema_);

    for (int i = 0; i < nrows; i++) {
      ExpectedRow row;
      row.key = StringPrintf("hello %08d", i);
      row.val1 = i * 2;
      row.val2 = StringPrintf("a %08d", i * 2);
      row.val3 = i * 10;
      row.val4 = StringPrintf("b %08d", i * 10);

      int col = 0;
      CHECK_OK(ins_row.SetStringNoCopy(col++, row.key));
      CHECK_OK(ins_row.SetInt32(col++, row.val1));
      CHECK_OK(ins_row.SetStringNoCopy(col++, row.val2));
      CHECK_OK(ins_row.SetInt32(col++, row.val3));
      CHECK_OK(ins_row.SetStringNoCopy(col++, row.val4));
      ASSERT_OK_FAST(writer.Insert(ins_row));
      expected_state_.push_back(row);
    }
  }

  // Delete the data that was inserted and clear the expected state, end to front.
  void DeleteRows(int nrows) {
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow del_row(&client_schema_);

    for (int i = nrows - 1; i >= 0; i--) {
      CHECK_OK(del_row.SetStringNoCopy(0, expected_state_[i].key));
      ASSERT_OK(writer.Delete(del_row));
      expected_state_.pop_back();
    }
    ASSERT_EQ(expected_state_.size(), 0);
  }

  // Update the data, touching only odd or even rows based on the
  // value of 'even'.
  // Makes corresponding updates in expected_state_.
  void UpdateRows(int nrows, bool even) {
    LocalTabletWriter writer(tablet().get(), &client_schema_);
    KuduPartialRow prow(&client_schema_);
    for (int idx = 0; idx < nrows; idx++) {
      ExpectedRow* row = &expected_state_[idx];
      if ((idx % 2 == 0) == even) {
        // Set key
        CHECK_OK(prow.SetStringNoCopy(0, row->key));

        // Update the data
        row->val1 *= 2;
        row->val3 *= 2;
        row->val4.append("[U]");

        // Apply the updates.
        CHECK_OK(prow.SetInt32(1, row->val1));
        CHECK_OK(prow.SetInt32(3, row->val3));
        CHECK_OK(prow.SetStringNoCopy(4, row->val4));
        ASSERT_OK(writer.Update(prow));
      }
    }
  }

  // Verify that the data seen by scanning the tablet matches the data in
  // expected_state_.
  void VerifyData() {
    MvccSnapshot snap(*tablet()->mvcc_manager());
    VerifyDataWithMvccAndExpectedState(snap, expected_state_);
  }

  void VerifyDataWithMvccAndExpectedState(const MvccSnapshot& snap,
                                          const vector<ExpectedRow>& passed_expected_state) {
      RowIteratorOptions opts;
      opts.projection = client_schema_ptr_;
      opts.snap_to_include = snap;
      unique_ptr<RowwiseIterator> row_iter;
      ASSERT_OK(tablet()->NewRowIterator(std::move(opts), &row_iter));
      ASSERT_OK(row_iter->Init(nullptr));

      vector<string> results;
      ASSERT_OK(IterateToStringList(row_iter.get(), &results));
      VLOG(1) << "Results of iterating over the updated materialized rows:";
      ASSERT_EQ(passed_expected_state.size(), results.size());
      for (int i = 0; i < results.size(); i++) {
        SCOPED_TRACE(Substitute("row $0", i));
        const string& str = results[i];
        const ExpectedRow& expected = passed_expected_state[i];
        ASSERT_EQ(expected.Formatted(), str);
      }
    }

  MvccManager mvcc_;
  vector<ExpectedRow> expected_state_;
};

// Regression test for KUDU-2656, wherein a corruption during a major delta
// compaction would lead to a failure in debug mode.
TEST_F(TestMajorDeltaCompaction, TestKudu2656) {
  constexpr int kNumRows = 100;
  NO_FATALS(WriteTestTablet(kNumRows));
  ASSERT_OK(tablet()->Flush());
  vector<shared_ptr<RowSet>> all_rowsets;
  tablet()->GetRowSetsForTests(&all_rowsets);
  ASSERT_FALSE(all_rowsets.empty());
  shared_ptr<RowSet> rs = all_rowsets.front();
  // Create some on-disk deltas.
  NO_FATALS(UpdateRows(kNumRows, /*even=*/false));
  ASSERT_OK(tablet()->FlushBiggestDMS());

  // Major compact some columns.
  vector<ColumnId> col_ids = { schema_.column_id(1),
                               schema_.column_id(3),
                               schema_.column_id(4) };

  // Injecting a failure should result in an error, not a crash.
  FLAGS_cfile_inject_corruption = 1.0;
  fs::IOContext io_context({ "test-tablet" });
  Status s = tablet()->DoMajorDeltaCompaction(col_ids, rs, &io_context);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

// Tests a major delta compaction run.
// Verifies that the output rowset accurately reflects the mutations, but keeps the
// unchanged columns intact.
TEST_F(TestMajorDeltaCompaction, TestCompact) {
  const int kNumRows = 100;
  NO_FATALS(WriteTestTablet(kNumRows));
  ASSERT_OK(tablet()->Flush());

  vector<shared_ptr<RowSet> > all_rowsets;
  tablet()->GetRowSetsForTests(&all_rowsets);

  shared_ptr<RowSet> rs = all_rowsets.front();

  vector<ColumnId> col_ids_to_compact = { schema_.column_id(1),
                                          schema_.column_id(3),
                                          schema_.column_id(4) };

  // We'll run a few rounds of update/compact to make sure
  // that we don't get into some funny state (regression test for
  // an earlier bug).
  // We first compact all the columns, then for each other round we do one less,
  // so that we test a few combinations.
  for (int i = 0; i < 3; i++) {
    SCOPED_TRACE(Substitute("Update/compact round $0", i));
    // Update the even rows and verify.
    NO_FATALS(UpdateRows(kNumRows, false));
    NO_FATALS(VerifyData());

    // Flush the deltas, make sure data stays the same.
    ASSERT_OK(tablet()->FlushBiggestDMS());
    NO_FATALS(VerifyData());

    // Update the odd rows and flush deltas
    NO_FATALS(UpdateRows(kNumRows, true));
    ASSERT_OK(tablet()->FlushBiggestDMS());
    NO_FATALS(VerifyData());

    // Major compact some columns.
    vector<ColumnId> col_ids;
    for (int col_index = 0; col_index < col_ids_to_compact.size() - i; col_index++) {
      col_ids.push_back(col_ids_to_compact[col_index]);
    }
    ASSERT_OK(tablet()->DoMajorDeltaCompaction(col_ids, rs));

    NO_FATALS(VerifyData());
  }
}

// Verify that we do issue UNDO files and that we can read them.
TEST_F(TestMajorDeltaCompaction, TestUndos) {
  const int kNumRows = 100;
  NO_FATALS(WriteTestTablet(kNumRows));
  ASSERT_OK(tablet()->Flush());

  vector<shared_ptr<RowSet> > all_rowsets;
  tablet()->GetRowSetsForTests(&all_rowsets);

  shared_ptr<RowSet> rs = all_rowsets.front();

  MvccSnapshot snap(*tablet()->mvcc_manager());

  // Verify the old data and grab a copy of the old state.
  NO_FATALS(VerifyDataWithMvccAndExpectedState(snap, expected_state_));
  vector<ExpectedRow> old_state(expected_state_.size());
  std::copy(expected_state_.begin(), expected_state_.end(), old_state.begin());

  // Flush the DMS, make sure we still see the old data.
  NO_FATALS(UpdateRows(kNumRows, false));
  ASSERT_OK(tablet()->FlushBiggestDMS());
  NO_FATALS(VerifyDataWithMvccAndExpectedState(snap, old_state));

  // Major compact, check we still have the old data.
  vector<ColumnId> col_ids_to_compact = { schema_.column_id(1),
                                          schema_.column_id(3),
                                          schema_.column_id(4) };
  ASSERT_OK(tablet()->DoMajorDeltaCompaction(col_ids_to_compact, rs));
  NO_FATALS(VerifyDataWithMvccAndExpectedState(snap, old_state));

  // Test adding three updates per row to three REDO files.
  for (int i = 0; i < 3; i++) {
    for (int j = 0; j < 3; j++) {
      NO_FATALS(UpdateRows(kNumRows, false));
    }
    ASSERT_OK(tablet()->FlushBiggestDMS());
  }

  // To complicate things further, only major compact two columns, then verify we can read the old
  // and the new data.
  col_ids_to_compact.pop_back();
  ASSERT_OK(tablet()->DoMajorDeltaCompaction(col_ids_to_compact, rs));
  NO_FATALS(VerifyDataWithMvccAndExpectedState(snap, old_state));
  NO_FATALS(VerifyData());
}

// Test that the delete REDO mutations are written back and not filtered out.
TEST_F(TestMajorDeltaCompaction, TestCarryDeletesOver) {
  const int kNumRows = 100;

  NO_FATALS(WriteTestTablet(kNumRows));
  ASSERT_OK(tablet()->Flush());

  vector<shared_ptr<RowSet> > all_rowsets;
  tablet()->GetRowSetsForTests(&all_rowsets);
  shared_ptr<RowSet> rs = all_rowsets.front();

  NO_FATALS(UpdateRows(kNumRows, false));
  ASSERT_OK(tablet()->FlushBiggestDMS());

  MvccSnapshot updates_snap(*tablet()->mvcc_manager());
  vector<ExpectedRow> old_state(expected_state_.size());
  std::copy(expected_state_.begin(), expected_state_.end(), old_state.begin());

  NO_FATALS(DeleteRows(kNumRows));
  ASSERT_OK(tablet()->FlushBiggestDMS());

  vector<ColumnId> col_ids_to_compact = { schema_.column_id(4) };
  ASSERT_OK(tablet()->DoMajorDeltaCompaction(col_ids_to_compact, rs));

  NO_FATALS(VerifyData());

  NO_FATALS(VerifyDataWithMvccAndExpectedState(updates_snap, old_state));
}

// Verify that reinserts only happen in the MRS and not down into the DRS. This test serves as a
// way to document how things work, and if they change then we'll know that our assumptions have
// changed.
TEST_F(TestMajorDeltaCompaction, TestReinserts) {
  const int kNumRows = 100;

  // Reinsert all the rows directly in the MRS.
  NO_FATALS(WriteTestTablet(kNumRows)); // 1st batch.
  NO_FATALS(DeleteRows(kNumRows)); // Delete 1st batch.
  NO_FATALS(WriteTestTablet(kNumRows)); // 2nd batch.
  ASSERT_OK(tablet()->Flush());

  // Update those rows, we'll try to read them at the end.
  NO_FATALS(UpdateRows(kNumRows, false)); // Update 2nd batch.
  vector<ExpectedRow> old_state(expected_state_.size());
  std::copy(expected_state_.begin(), expected_state_.end(), old_state.begin());
  MvccSnapshot second_batch_inserts(*tablet()->mvcc_manager());

  vector<shared_ptr<RowSet> > all_rowsets;
  tablet()->GetRowSetsForTests(&all_rowsets);
  ASSERT_EQ(1, all_rowsets.size());

  NO_FATALS(VerifyData());

  // Delete the rows (will go into the DMS) then reinsert them (will go in a new MRS), then flush
  // the DMS with the deletes so that we can major compact them.
  NO_FATALS(DeleteRows(kNumRows)); // Delete 2nd batch.
  NO_FATALS(WriteTestTablet(kNumRows)); // 3rd batch.
  ASSERT_OK(tablet()->FlushBiggestDMS());

  // At this point, here's the layout (the 1st batch was discarded during the first flush):
  // MRS: 3rd batch of inserts.
  // RS1: UNDO DF: Deletes for the 2nd batch.
  //      DS: Base data for the 2nd batch.
  //      REDO DF: Updates and deletes for the 2nd.

  // Now we'll push some of the updates down.
  shared_ptr<RowSet> rs = all_rowsets.front();
  vector<ColumnId> col_ids_to_compact = { schema_.column_id(4) };
  ASSERT_OK(tablet()->DoMajorDeltaCompaction(col_ids_to_compact, rs));

  // The data we'll see here is the 3rd batch of inserts, doesn't have updates.
  NO_FATALS(VerifyData());

  // Test that the 3rd batch of inserts goes into a new RS, even though it's the same row keys.
  ASSERT_OK(tablet()->Flush());
  all_rowsets.clear();
  tablet()->GetRowSetsForTests(&all_rowsets);
  ASSERT_EQ(2, all_rowsets.size());

  // Verify the 3rd batch.
  NO_FATALS(VerifyData());

  // Verify the updates in the second batch are still readable, from the first RS.
  NO_FATALS(VerifyDataWithMvccAndExpectedState(second_batch_inserts, old_state));
}

// Verify that we won't schedule a major compaction when files are just composed of deletes.
TEST_F(TestMajorDeltaCompaction, TestJustDeletes) {
  const int kNumRows = 100;

  NO_FATALS(WriteTestTablet(kNumRows));
  ASSERT_OK(tablet()->Flush());
  NO_FATALS(DeleteRows(kNumRows));
  ASSERT_OK(tablet()->FlushBiggestDMS());

  shared_ptr<RowSet> rs;
  ASSERT_EQ(0,
            tablet()->GetPerfImprovementForBestDeltaCompact(RowSet::MAJOR_DELTA_COMPACTION, &rs));
}

} // namespace tablet
} // namespace kudu
