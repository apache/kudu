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
#include <atomic>

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/clock.h"
#include "kudu/clock/hybrid_clock.h"
#include "kudu/clock/mock_ntp.h"
#include "kudu/clock/time_service.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

DECLARE_bool(enable_maintenance_manager);
DECLARE_int32(tablet_history_max_age_sec);
DECLARE_string(time_source);

using kudu::clock::HybridClock;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tablet {

class TabletHistoryGcTest : public TabletTestBase<IntKeyTestSetup<INT64>> {
 public:
  typedef TabletTestBase<IntKeyTestSetup<INT64>> Superclass;

  TabletHistoryGcTest()
      : Superclass(TabletHarness::Options::HYBRID_CLOCK) {
    FLAGS_time_source = "mock";
  }

  void SetMockTime(int64_t micros) {
    auto* hybrid_clock = down_cast<HybridClock*>(clock());
    auto* ntp = down_cast<clock::MockNtp*>(hybrid_clock->time_service());
    ntp->SetMockClockWallTimeForTests(micros);
  }

  virtual void SetUp() OVERRIDE {
    NO_FATALS(TabletTestBase<IntKeyTestSetup<INT64>>::SetUp());
    // Mock clock defaults to 0 and this screws up the AHM calculation which ends up negative.
    SetMockTime(GetCurrentTimeMicros());
  }

 protected:
  enum ToFlush {
    FLUSH,
    NO_FLUSH
  };

  void InsertOriginalRows(int64_t num_rowsets, int64_t rows_per_rowset);
  void UpdateOriginalRows(int64_t num_rowsets, int64_t rows_per_rowset, int32_t val);
  void AddTimeToHybridClock(MonoDelta delta) {
    uint64_t now = HybridClock::GetPhysicalValueMicros(clock()->Now());
    uint64_t new_time = now + delta.ToMicroseconds();
    SetMockTime(new_time);
  }
  // Specify row regex to match on. Empty string means don't match anything.
  void VerifyDebugDumpRowsMatch(const string& pattern) const;

  int64_t TotalNumRows() const { return num_rowsets_ * rows_per_rowset_; }

  TestRowVerifier GenRowsEqualVerifier(int32_t expected_val) {
    return [=](int32_t key, int32_t val) -> bool { return val == expected_val; };
  }

  const TestRowVerifier kRowsEqual0 = GenRowsEqualVerifier(0);
  const TestRowVerifier kRowsEqual1 = GenRowsEqualVerifier(1);
  const TestRowVerifier kRowsEqual2 = GenRowsEqualVerifier(2);

  const int kStartRow = 0;
  int num_rowsets_ = 3;
  int64_t rows_per_rowset_ = 300;
};

void TabletHistoryGcTest::InsertOriginalRows(int64_t num_rowsets, int64_t rows_per_rowset) {
  for (int rowset_id = 0; rowset_id < num_rowsets; rowset_id++) {
    InsertTestRows(rowset_id * rows_per_rowset, rows_per_rowset, 0);
    ASSERT_OK(tablet()->Flush());
  }
  ASSERT_EQ(num_rowsets, tablet()->num_rowsets());
}

void TabletHistoryGcTest::UpdateOriginalRows(int64_t num_rowsets, int64_t rows_per_rowset,
                                             int32_t val) {
  for (int rowset_id = 0; rowset_id < num_rowsets; rowset_id++) {
    UpsertTestRows(rowset_id * rows_per_rowset, rows_per_rowset, val);
    ASSERT_OK(tablet()->FlushAllDMSForTests());
  }
  ASSERT_EQ(num_rowsets, tablet()->num_rowsets());
}

void TabletHistoryGcTest::VerifyDebugDumpRowsMatch(const string& pattern) const {
  vector<string> rows;
  ASSERT_OK(tablet()->DebugDump(&rows));
  // Ignore the non-data (formattting) lines in the output.
  std::string base_pattern = R"(^Dumping|^-|^MRS|^RowSet)";
  if (!pattern.empty()) {
    base_pattern += "|";
  }
  ASSERT_STRINGS_ALL_MATCH(rows, base_pattern + pattern);
}

// Test that we do not generate undos for redo operations that are older than
// the AHM during major delta compaction.
TEST_F(TabletHistoryGcTest, TestNoGenerateUndoOnMajorDeltaCompaction) {
  FLAGS_tablet_history_max_age_sec = 1; // Keep history for 1 second.

  NO_FATALS(InsertOriginalRows(num_rowsets_, rows_per_rowset_));
  NO_FATALS(VerifyTestRowsWithVerifier(kStartRow, TotalNumRows(), kRowsEqual0));
  Timestamp time_after_insert = clock()->Now();

  // Timestamps recorded after each round of updates.
  Timestamp post_update_ts[2];

  // Mutate all of the rows, setting val=1. Then again for val=2.
  LocalTabletWriter writer(tablet().get(), &client_schema_);
  for (int val = 1; val <= 2; val++) {
    for (int row_idx = 0; row_idx < TotalNumRows(); row_idx++) {
      ASSERT_OK(UpdateTestRow(&writer, row_idx, val));
    }
    // We must flush the DMS before major compaction can operate on these REDOs.
    for (int i = 0; i < num_rowsets_; i++) {
      tablet()->FlushBiggestDMS();
    }
    post_update_ts[val - 1] = clock()->Now();
  }

  // Move the AHM beyond our mutations, which are represented as REDOs.
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(2)));

  // Current-time reads should give us 2, but reads from the past should give
  // us 0 or 1.
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, TotalNumRows(),
                                                   time_after_insert, kRowsEqual0));
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, TotalNumRows(),
                                                   post_update_ts[0], kRowsEqual1));
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, TotalNumRows(),
                                                   post_update_ts[1], kRowsEqual2));
  NO_FATALS(VerifyTestRowsWithVerifier(kStartRow, TotalNumRows(), kRowsEqual2));

  // Run major delta compaction.
  for (int i = 0; i < num_rowsets_; i++) {
    ASSERT_OK(tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
  }

  // Now, we should have base data = 2 with no other historical values.
  // Major delta compaction will not remove UNDOs, so we expect a single UNDO DELETE as well.
  NO_FATALS(VerifyDebugDumpRowsMatch(
      R"(int32 val=2\); Undo Mutations: \[@[[:digit:]]+\(DELETE\)\]; )"
      R"(Redo Mutations: \[\];$)"));
}

// Test that major delta compaction works when run on a subset of columns:
// 1. Insert rows and flush to DiskRowSets.
// 2. Mutate two columns.
// 3. Move time forward.
// 4. Run major delta compaction on a single column.
// 5. Make sure we don't lose anything unexpected.
TEST_F(TabletHistoryGcTest, TestMajorDeltaCompactionOnSubsetOfColumns) {
  FLAGS_tablet_history_max_age_sec = 100;

  num_rowsets_ = 3;
  rows_per_rowset_ = 20;

  NO_FATALS(InsertOriginalRows(num_rowsets_, rows_per_rowset_));
  NO_FATALS(VerifyTestRowsWithVerifier(kStartRow, TotalNumRows(), kRowsEqual0));

  LocalTabletWriter writer(tablet().get(), &client_schema_);
  for (int32_t row_key = 0; row_key < TotalNumRows(); row_key++) {
    KuduPartialRow row(&client_schema_);
    setup_.BuildRowKey(&row, row_key);
    ASSERT_OK_FAST(row.SetInt32(1, 1));
    ASSERT_OK_FAST(row.SetInt32(2, 2));
    ASSERT_OK_FAST(writer.Update(row));
  }
  for (int i = 0; i < num_rowsets_; i++) {
    tablet()->FlushBiggestDMS();
  }

  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(200)));

  vector<std::shared_ptr<RowSet>> rowsets;
  tablet()->GetRowSetsForTests(&rowsets);
  for (int i = 0; i < num_rowsets_; i++) {
    DiskRowSet* drs = down_cast<DiskRowSet*>(rowsets[i].get());
    vector<ColumnId> col_ids_to_compact = { schema_.column_id(2) };
    ASSERT_OK(drs->MajorCompactDeltaStoresWithColumnIds(col_ids_to_compact,
                                                        tablet()->GetHistoryGcOpts()));
  }

  NO_FATALS(VerifyDebugDumpRowsMatch(
      R"(int32 val=2\); Undo Mutations: \[@[[:digit:]]+\(DELETE\)\]; )"
      R"(Redo Mutations: \[@[[:digit:]]+\(SET key_idx=1\)\];$)"));

  vector<string> rows;
  ASSERT_OK(IterateToStringList(&rows));
  ASSERT_EQ(TotalNumRows(), rows.size());
}

// Tests the following two MRS flush scenarios:
// 1. Verify that no UNDO is generated after inserting a row into the MRS,
//    waiting for the AHM to pass, then flushing the MRS.
// 2. Same as #1 but delete the inserted row from the MRS before waiting for
//    the AHM to pass.
TEST_F(TabletHistoryGcTest, TestNoGenerateUndoOnMRSFlush) {
  FLAGS_tablet_history_max_age_sec = 100;

  Timestamp time_before_insert = clock()->Now();
  LocalTabletWriter writer(tablet().get(), &client_schema_);
  for (int32_t i = kStartRow; i < TotalNumRows(); i++) {
    ASSERT_OK(InsertTestRow(&writer, i, 0));
  }
  Timestamp time_after_insert = clock()->Now();
  for (int32_t i = kStartRow; i < TotalNumRows(); i++) {
    ASSERT_OK(DeleteTestRow(&writer, i));
  }
  Timestamp time_after_delete = clock()->Now();

  // Move the clock.
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(200)));

  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, 0,
                                                   time_before_insert, boost::none));
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, TotalNumRows(),
                                                   time_after_insert, kRowsEqual0));
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, 0,
                                                   time_after_delete, boost::none));

  // Now flush the MRS. No trace should remain after this.
  ASSERT_OK(tablet()->Flush());
  NO_FATALS(VerifyDebugDumpRowsMatch(""));

  for (const auto& rsmd : tablet()->metadata()->rowsets()) {
    ASSERT_EQ(0, rsmd->undo_delta_blocks().size());
  }
  ASSERT_EQ(0, tablet()->OnDiskDataSize());

  // Now check the same thing (flush not generating an UNDO), but without the
  // delete following the insert. We do it with a single row.

  ASSERT_OK(InsertTestRow(&writer, kStartRow, 0));
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(200)));
  ASSERT_OK(tablet()->Flush());
  // There should be no undo blocks, despite flushing an insert.
  for (const auto& rsmd : tablet()->metadata()->rowsets()) {
    ASSERT_EQ(0, rsmd->undo_delta_blocks().size());
  }
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, 1, Timestamp(0), kRowsEqual0));
  NO_FATALS(VerifyDebugDumpRowsMatch(
      R"(int32 val=0\); Undo Mutations: \[\]; Redo Mutations: \[\];$)"));
}

// Test that undos get GCed on a merge compaction.
// In this test, we GC the UNDO that undoes the insert.
TEST_F(TabletHistoryGcTest, TestUndoGCOnMergeCompaction) {
  FLAGS_tablet_history_max_age_sec = 1; // Keep history for 1 second.

  Timestamp time_before_insert = clock()->Now();
  NO_FATALS(InsertOriginalRows(num_rowsets_, rows_per_rowset_));
  NO_FATALS(VerifyTestRowsWithVerifier(kStartRow, TotalNumRows(), kRowsEqual0));

  // The earliest thing we can see is an empty tablet.
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, 0, time_before_insert, boost::none));

  // Move the clock so the insert is prior to the AHM, then compact.
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(2)));
  ASSERT_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Now the only thing we can see is the base data.
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, TotalNumRows(), time_before_insert,
                                                   kRowsEqual0));
  NO_FATALS(VerifyDebugDumpRowsMatch(
      R"(int32 val=0\); Undo Mutations: \[\]; Redo Mutations: \[\];$)"));
}

// Test that we GC the history and existence of entire deleted rows on a merge compaction.
TEST_F(TabletHistoryGcTest, TestRowRemovalGCOnMergeCompaction) {
  FLAGS_tablet_history_max_age_sec = 100; // Keep history for 100 seconds.

  NO_FATALS(InsertOriginalRows(num_rowsets_, rows_per_rowset_));
  NO_FATALS(VerifyTestRowsWithVerifier(kStartRow, TotalNumRows(), kRowsEqual0));

  Timestamp prev_time = clock()->Now();

  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(200)));

  // Delete all of the rows in the tablet.
  LocalTabletWriter writer(tablet().get(), &client_schema_);
  for (int row_idx = 0; row_idx < TotalNumRows(); row_idx++) {
    ASSERT_OK(DeleteTestRow(&writer, row_idx));
  }
  ASSERT_OK(tablet()->Flush());
  NO_FATALS(VerifyDebugDumpRowsMatch(
      R"(int32 val=0\); Undo Mutations: \[@[[:digit:]]+\(DELETE\)\]; )"
      R"(Redo Mutations: \[@[[:digit:]]+\(DELETE\)\];$)"));

  // Compaction at this time will only remove the initial UNDO records. The
  // DELETE REDOs are too recent.
  ASSERT_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  NO_FATALS(VerifyDebugDumpRowsMatch(
      R"(int32 val=0\); Undo Mutations: \[\]; Redo Mutations: )"
      R"(\[@[[:digit:]]+\(DELETE\)\];$)"));

  // Move the AHM so that the delete is now prior to it.
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(200)));

  // Now that even the deletion is prior to the AHM, all of the on-disk data
  // will be GCed.
  ASSERT_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  NO_FATALS(VerifyDebugDumpRowsMatch(""));
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, 0, prev_time, boost::none));
  ASSERT_EQ(0, tablet()->OnDiskDataSize());
}

// Test that we don't over-aggressively GC history prior to the AHM.
TEST_F(TabletHistoryGcTest, TestNoUndoGCUntilAncientHistoryMark) {
  FLAGS_tablet_history_max_age_sec = 1000; // 1000 seconds before we GC history.

  NO_FATALS(InsertOriginalRows(num_rowsets_, rows_per_rowset_));

  Timestamp prev_time = clock()->Now();
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(2)));

  // Mutate all of the rows.
  LocalTabletWriter writer(tablet().get(), &client_schema_);
  for (int row_idx = 0; row_idx < TotalNumRows(); row_idx++) {
    SCOPED_TRACE(Substitute("Row index: $0", row_idx));
    ASSERT_OK(UpdateTestRow(&writer, row_idx, 1));
  }

  // Current-time reads should give us 1, but reads from the past should give us 0.
  NO_FATALS(VerifyTestRowsWithVerifier(kStartRow, TotalNumRows(), kRowsEqual1));
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, TotalNumRows(), prev_time,
                                                   kRowsEqual0));

  for (int i = 0; i < num_rowsets_; i++) {
    ASSERT_OK(tablet()->CompactWorstDeltas(RowSet::MINOR_DELTA_COMPACTION));
    ASSERT_OK(tablet()->CompactWorstDeltas(RowSet::MAJOR_DELTA_COMPACTION));
  }
  ASSERT_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // Still read 0 from the past.
  NO_FATALS(VerifyTestRowsWithTimestampAndVerifier(kStartRow, TotalNumRows(), prev_time,
                                                   kRowsEqual0));
  NO_FATALS(VerifyDebugDumpRowsMatch(
      R"(int32 val=1\); Undo Mutations: \[@[[:digit:]]+\(SET val=0\), )"
      R"(@[[:digit:]]+\(DELETE\)\]; Redo Mutations: \[\];$)"));
}

// Test that "ghost" rows (deleted on one rowset, reinserted on another) don't
// get revived after history GC.
TEST_F(TabletHistoryGcTest, TestGhostRowsNotRevived) {
  FLAGS_tablet_history_max_age_sec = 100;

  LocalTabletWriter writer(tablet().get(), &client_schema_);
  for (int i = 0; i <= 2; i++) {
    ASSERT_OK(InsertTestRow(&writer, 0, i));
    ASSERT_OK(DeleteTestRow(&writer, 0));
    ASSERT_OK(tablet()->Flush());
  }

  // Create one more rowset on disk which has just an INSERT (ie a non-ghost row).
  ASSERT_OK(InsertTestRow(&writer, 0, 3));
  ASSERT_OK(tablet()->Flush());

  // Move the clock, then compact. This should result in a rowset with just one
  // row in it.
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(200)));
  ASSERT_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  // We should end up with a single row as base data.
  NO_FATALS(VerifyTestRows(0, 1));
  NO_FATALS(VerifyDebugDumpRowsMatch(
      R"(int32 val=3\); Undo Mutations: \[\]; Redo Mutations: \[\];)"));
}

// Test to ensure that nothing bad happens when we partially GC different rows
// in a rowset. We delete alternating keys to end up with a mix of GCed and
// non-GCed rows in each rowset.
TEST_F(TabletHistoryGcTest, TestGcOnAlternatingRows) {
  FLAGS_tablet_history_max_age_sec = 100;
  num_rowsets_ = 3;
  rows_per_rowset_ = 5;

  LocalTabletWriter writer(tablet().get(), &client_schema_);
  for (int rowset_id = 0; rowset_id < num_rowsets_; rowset_id++) {
    for (int i = 0; i < rows_per_rowset_; i++) {
      int32_t row_key = rowset_id * rows_per_rowset_ + i;
      ASSERT_OK(InsertTestRow(&writer, row_key, 0));
    }
    ASSERT_OK(tablet()->Flush());
  }

  // Delete all the odd rows.
  for (int32_t row_key = 1; row_key < TotalNumRows(); row_key += 2) {
    ASSERT_OK(DeleteTestRow(&writer, row_key));
  }

  // Move the clock and compact. We should end up with even rows.
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(200)));
  ASSERT_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));

  vector<string> rows;
  ASSERT_OK(IterateToStringList(&rows));
  ASSERT_EQ(TotalNumRows() / 2 + 1, rows.size());

  // Even row keys are assigned negative values in this test framework and so
  // end up sorted negatively.
  std::reverse(rows.begin(), rows.end());

  int i = 0;
  for (int32_t row_key = 0; row_key < TotalNumRows(); row_key += 2) {
    ASSERT_STR_CONTAINS(rows[i], Substitute("int64 key=$0, int32 key_idx=$1, int32 val=0",
                                            -1 * row_key, row_key));
    i++;
  }
}

// Ensure that ReupdateMissedDeltas() doesn't reupdate the wrong row.
// 1. Insert rows and flush.
// 2. Delete some rows.
// 3. Move time forward.
// 4. Begin merge compaction.
// 5. Insert some of the deleted rows after phase 1 snapshot is written but before phase 2.
// 6. Update some of the rows in-between the deleted rows.
// 7. Ensure that the rows all look right according to what we expect.
//
// This test uses the following pattern. Rows with even keys are deleted, rows
// with odd keys are used in the test. The following takes place:
// - Rows 1 and 5 are inserted with values equaling their keys and are not mutated.
// - Rows 3 and 9 are inserted and then updated with values equaling their keys * 10 + 1.
// - Row 7 is deleted and then reinserted, as well as updated using successive values.
TEST_F(TabletHistoryGcTest, TestGcWithConcurrentCompaction) {
  FLAGS_tablet_history_max_age_sec = 100;

  class MyCommonHooks : public Tablet::FlushCompactCommonHooks {
   public:
    explicit MyCommonHooks(TabletHistoryGcTest* test)
        : test_(test),
          offset_(0) {
    }

    Status PostWriteSnapshot() OVERRIDE {
      LocalTabletWriter writer(test_->tablet().get(), &test_->client_schema());
      int offset = offset_.load(std::memory_order_acquire);
      // Update our reinserted row.
      CHECK_OK(test_->UpdateTestRow(&writer, 7, 73 + offset));

      // Also insert and update other rows after the flush.
      CHECK_OK(test_->UpdateTestRow(&writer, 3, 30 + offset));
      CHECK_OK(test_->UpdateTestRow(&writer, 9, 90 + offset));
      return Status::OK();
    }

    void set_offset(int offset) {
      offset_.store(offset, std::memory_order_release);
    }

   private:
    TabletHistoryGcTest* const test_;
    std::atomic<int> offset_;
  };

  std::shared_ptr<MyCommonHooks> hooks = std::make_shared<MyCommonHooks>(this);
  tablet()->SetFlushCompactCommonHooksForTests(hooks);

  LocalTabletWriter writer(tablet().get(), &client_schema_);
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(InsertTestRow(&writer, i, i));
  }
  // Also generate a reinsert.
  ASSERT_OK(DeleteTestRow(&writer, 7));
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(200)));
  ASSERT_OK(InsertTestRow(&writer, 7, 71));
  CHECK_OK(UpdateTestRow(&writer, 7, 72));

  // Flush the rowset.
  ASSERT_OK(tablet()->Flush());

  // Delete every even row.
  for (int i = 0; i < 10; i += 2) {
    ASSERT_OK(DeleteTestRow(&writer, i));
  }

  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(200)));

  hooks->set_offset(1);
  ASSERT_OK(tablet()->Compact(Tablet::FORCE_COMPACT_ALL));
  tablet()->SetFlushCompactCommonHooksForTests(nullptr);

  vector<string> rows;
  ASSERT_OK(IterateToStringList(&rows));

  if (VLOG_IS_ON(2)) {
    for (const string& r : rows) {
      VLOG(2) << r;
    }
  }

  vector<int32_t> expected_rows = { 1, 3, 5, 7, 9 };
  for (int i = 0; i < expected_rows.size(); i++) {
    int32_t key = expected_rows[i];
    switch (key) {
      case 1:
      case 5:
        ASSERT_STR_CONTAINS(rows[i], Substitute("int32 key_idx=$0, int32 val=$1",
                                                key, key));
        break;
      case 3:
      case 9:
        ASSERT_STR_CONTAINS(rows[i], Substitute("int32 key_idx=$0, int32 val=$1",
                                                key, key * 10 + 1));
        break;
      case 7:
        ASSERT_STR_CONTAINS(rows[i], Substitute("int32 key_idx=$0, int32 val=$1",
                                                key, key * 10 + 4));
        break;
      default:
        break;
    }
  }
}

// A version of the tablet history gc test with the maintenance manager disabled.
class TabletHistoryGcNoMaintMgrTest : public TabletHistoryGcTest {
 public:
  void SetUp() override {
    FLAGS_enable_maintenance_manager = false;
    TabletHistoryGcTest::SetUp();
  }
};

// Test that basic deletion of undo delta blocks prior to the AHM works.
TEST_F(TabletHistoryGcNoMaintMgrTest, TestUndoDeltaBlockGc) {
  FLAGS_tablet_history_max_age_sec = 1000;

  NO_FATALS(InsertOriginalRows(num_rowsets_, rows_per_rowset_));
  ASSERT_EQ(num_rowsets_, tablet()->CountUndoDeltasForTests());

  // Generate a bunch of redo deltas and then compact them into undo deltas.
  constexpr int kNumMutationsPerRow = 5;
  for (int i = 0; i < kNumMutationsPerRow; i++) {
    SCOPED_TRACE(i);
    ASSERT_EQ((i + 1) * num_rowsets_, tablet()->CountUndoDeltasForTests());
    NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(1)));
    NO_FATALS(UpdateOriginalRows(num_rowsets_, rows_per_rowset_, i));
    ASSERT_OK(tablet()->MajorCompactAllDeltaStoresForTests());
    ASSERT_EQ((i + 2) * num_rowsets_, tablet()->CountUndoDeltasForTests());
  }

  ASSERT_EQ(0, tablet()->CountRedoDeltasForTests());
  const int expected_undo_blocks = (kNumMutationsPerRow + 1) * num_rowsets_;
  ASSERT_EQ(expected_undo_blocks, tablet()->CountUndoDeltasForTests());

  // There will be uninitialized undos so we will estimate that there may be
  // undos to GC.
  int64_t bytes;
  ASSERT_OK(tablet()->EstimateBytesInPotentiallyAncientUndoDeltas(&bytes));
  ASSERT_GT(bytes, 0);

  // Now initialize the undos. Our estimates should be back to 0.
  int64_t bytes_in_ancient_undos = 0;
  const MonoDelta kNoTimeLimit = MonoDelta();
  ASSERT_OK(tablet()->InitAncientUndoDeltas(kNoTimeLimit, &bytes_in_ancient_undos));
  ASSERT_EQ(0, bytes_in_ancient_undos);
  ASSERT_OK(tablet()->EstimateBytesInPotentiallyAncientUndoDeltas(&bytes));
  ASSERT_EQ(0, bytes);

  // Move the clock so all deltas should be ancient.
  NO_FATALS(AddTimeToHybridClock(MonoDelta::FromSeconds(FLAGS_tablet_history_max_age_sec + 1)));

  // Initialize and delete undos.
  ASSERT_OK(tablet()->InitAncientUndoDeltas(kNoTimeLimit, &bytes_in_ancient_undos));
  ASSERT_GT(bytes_in_ancient_undos, 0);

  // Check that the estimate and the metrics match.
  ASSERT_OK(tablet()->EstimateBytesInPotentiallyAncientUndoDeltas(&bytes));
  ASSERT_EQ(bytes_in_ancient_undos, bytes);
  ASSERT_EQ(bytes, tablet()->metrics()->undo_delta_block_estimated_retained_bytes->value());

  int64_t blocks_deleted;
  int64_t bytes_deleted;
  ASSERT_OK(tablet()->DeleteAncientUndoDeltas(&blocks_deleted, &bytes_deleted));
  ASSERT_EQ(expected_undo_blocks, blocks_deleted);
  ASSERT_GT(bytes_deleted, 0);
  ASSERT_EQ(0, tablet()->CountUndoDeltasForTests());
  VLOG(1) << "Bytes deleted: " << bytes_deleted;

  // Basic sanity check for our per-tablet metrics. Duration is difficult to
  // verify with a Histogram so simply ensure each Histogram was incremented.
  ASSERT_EQ(bytes_deleted, tablet()->metrics()->undo_delta_block_gc_bytes_deleted->value());
  ASSERT_EQ(2, tablet()->metrics()->undo_delta_block_gc_init_duration->TotalCount());
  ASSERT_EQ(1, tablet()->metrics()->undo_delta_block_gc_delete_duration->TotalCount());
}

} // namespace tablet
} // namespace kudu
