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
#include <cstdlib>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/partial_row.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {
namespace tablet {
class Tablet;
}  // namespace tablet
}  // namespace kudu

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

// Run a diff scan over (snap_to_exclude, snap_to_include] with the requested
// order and row-visibility mode.
static void DoDiffScan(std::shared_ptr<Tablet> tablet,
                       MvccSnapshot snap_to_exclude,
                       MvccSnapshot snap_to_include,
                       OrderMode order,
                       RowVisibility row_visibility,
                       vector<string>* rows) {
  RowIteratorOptions opts;
  opts.snap_to_exclude = std::move(snap_to_exclude);
  opts.snap_to_include = std::move(snap_to_include);
  opts.order = order;
  opts.include_deleted_rows = true;
  opts.row_visibility = row_visibility;
  static constexpr bool kIsDeletedDefault = false;
  SchemaBuilder builder(*tablet->metadata()->schema());
  ASSERT_OK(builder.AddColumn(ColumnSchemaBuilder()
                                  .name("deleted")
                                  .type(IS_DELETED)
                                  .read_default(&kIsDeletedDefault)));
  Schema projection = builder.BuildWithoutIds();
  opts.projection = &projection;
  unique_ptr<RowwiseIterator> row_iterator;
  ASSERT_OK(tablet->NewRowIterator(std::move(opts), &row_iterator));
  ASSERT_TRUE(row_iterator);
  ScanSpec spec;
  ASSERT_OK(row_iterator->Init(&spec));
  ASSERT_OK(tablet::IterateToStringList(row_iterator.get(), rows));
}

class DiffScanTest : public TabletTestBase<IntKeyTestSetup<INT64>>,
                     public ::testing::WithParamInterface<std::tuple<OrderMode, bool>> {
 public:
  DiffScanTest()
      : Superclass(TabletHarness::Options::ClockType::HYBRID_CLOCK) {}

 private:
  using Superclass = TabletTestBase<IntKeyTestSetup<INT64>>;
};

INSTANTIATE_TEST_SUITE_P(DiffScanModes, DiffScanTest,
                         ::testing::Combine(
                            /*order_mode*/ ::testing::Values(UNORDERED, ORDERED),
                            /*include_deleted_rows*/ ::testing::Bool()));

TEST_P(DiffScanTest, DiffScan) {
  OrderMode order_mode = std::get<0>(GetParam());
  bool include_deleted_rows = std::get<1>(GetParam());
  auto tablet = this->tablet();
  auto tablet_id = tablet->tablet_id();

  MvccSnapshot snap1(*tablet->mvcc_manager());

  LocalTabletWriter writer(tablet.get(), &client_schema_);
  constexpr int64_t kRowKey = 1;
  ASSERT_OK(InsertTestRow(&writer, kRowKey, 1));
  ASSERT_OK(tablet->Flush());

  // 2. Delete the row and flush the DMS.
  ASSERT_OK(DeleteTestRow(&writer, kRowKey));
  ASSERT_OK(tablet->FlushAllDMSForTests());

  // 3. Insert the same row key (with another value) and flush the MRS.
  ASSERT_OK(InsertTestRow(&writer, kRowKey, 2));
  ASSERT_OK(tablet->Flush());

  // Ensure there is only 1 live row in the tablet (our reinsert).
  vector<string> rows;
  ASSERT_OK(DumpTablet(*tablet, tablet->schema()->CopyWithoutColumnIds(), &rows));
  ASSERT_EQ(1, rows.size()) << "expected only one live row";
  ASSERT_EQ("(int64 key=1, int32 key_idx=1, int32 val=2)", rows[0]);

  // 4. Do a diff scan from time snap1.
  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  RowIteratorOptions opts;
  opts.snap_to_include = snap2;
  opts.order = order_mode;
  opts.include_deleted_rows = include_deleted_rows;

  static const bool kIsDeletedDefault = false;
  SchemaBuilder builder(*tablet->metadata()->schema());
  if (order_mode == ORDERED) {
    // Define our diff scan to start from snap1.
    // NOTE: it isn't critical to set this given the default is -Inf, but it
    // can't hurt to specify one, given we expect it to be the common case with
    // the backup jobs.
    opts.snap_to_exclude = snap1;

    // The merge iterator requires an IS_DELETED column when including deleted
    // rows in order to support deduplication of the rows.
    ASSERT_OK(builder.AddColumn(ColumnSchemaBuilder()
                                    .name("deleted")
                                    .type(IS_DELETED)
                                    .read_default(&kIsDeletedDefault)));
  }
  Schema projection = builder.BuildWithoutIds();
  opts.projection = &projection;

  unique_ptr<RowwiseIterator> row_iterator;
  ASSERT_OK(tablet->NewRowIterator(std::move(opts),
                                   &row_iterator));
  ASSERT_TRUE(row_iterator);
  ScanSpec spec;
  ASSERT_OK(row_iterator->Init(&spec));

  ASSERT_OK(tablet::IterateToStringList(row_iterator.get(), &rows));

  // In unordered mode, the union iterator will not deduplicate row keys.
  // In ordered mode, the merge iterator will perform deduplication.
  if (order_mode == UNORDERED) {
    if (include_deleted_rows) {
      // No de-dup.
      ASSERT_EQ(2, rows.size());
      // There is no guaranteed order of these results so get them in alpha order.
      std::sort(rows.begin(), rows.end());
      EXPECT_EQ("(int64 key=1, int32 key_idx=1, int32 val=1)", rows[0]);
      EXPECT_EQ("(int64 key=1, int32 key_idx=1, int32 val=2)", rows[1]);
    } else {
      // There will only ever be a single live version of any one row.
      ASSERT_EQ(1, rows.size());
      EXPECT_EQ("(int64 key=1, int32 key_idx=1, int32 val=2)", rows[0]);
    }
  } else {
    // De-dup, regardless of whether deleted rows are included or not.
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ("(int64 key=1, int32 key_idx=1, int32 val=2, is_deleted deleted=false)", rows[0]);
  }
}

class OrderedDiffScanWithDeletesTest : public TabletTestBase<IntKeyTestSetup<INT64>> {
 public:
  OrderedDiffScanWithDeletesTest()
      : Superclass(TabletHarness::Options::ClockType::HYBRID_CLOCK) {}

 private:
  using Superclass = TabletTestBase<IntKeyTestSetup<INT64>>;
};

// Regression test for KUDU-3108, wherein running the merge iterator on
// overlapping rowsets could potentially lead to invalid memory access.
TEST_F(OrderedDiffScanWithDeletesTest, TestKudu3108) {
  auto tablet = this->tablet();
  auto tablet_id = tablet->tablet_id();

  LocalTabletWriter writer(tablet.get(), &client_schema_);
  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(tablet->Flush());

  MvccSnapshot snap1(*tablet->mvcc_manager());
  ASSERT_OK(DeleteTestRow(&writer, 1));
  ASSERT_OK(InsertTestRow(&writer, 3, 1));
  ASSERT_OK(tablet->Flush());

  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(InsertTestRow(&writer, 0, 1));
  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  RowIteratorOptions opts;
  opts.snap_to_exclude = snap1;
  opts.snap_to_include = snap2;
  opts.order = ORDERED;
  opts.include_deleted_rows = true;
  static const bool kIsDeletedDefault = false;
  SchemaBuilder builder(*tablet->metadata()->schema());
  ASSERT_OK(builder.AddColumn(ColumnSchemaBuilder()
                                  .name("deleted")
                                  .type(IS_DELETED)
                                  .read_default(&kIsDeletedDefault)));
  Schema projection = builder.BuildWithoutIds();
  opts.projection = &projection;

  // We should be able to iterate through the rows without issue.
  unique_ptr<RowwiseIterator> row_iterator;
  ASSERT_OK(tablet->NewRowIterator(std::move(opts),
                                   &row_iterator));
  ASSERT_TRUE(row_iterator);
  ScanSpec spec;
  ASSERT_OK(row_iterator->Init(&spec));
  vector<string> rows;
  ASSERT_OK(tablet::IterateToStringList(row_iterator.get(), &rows));
  ASSERT_EQ(3, rows.size());
}

TEST_F(OrderedDiffScanWithDeletesTest, UnobservableRowsMemRowSet) {
  auto tablet = this->tablet();

  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);
  // Insert, update, delete a row which is stored in memrowset.
  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 2));
  ASSERT_OK(DeleteTestRow(&writer, 1));

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  vector<string> rows_with_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE,
                       &rows_with_flag));
  ASSERT_EQ(1, rows_with_flag.size());
  ASSERT_STR_CONTAINS(rows_with_flag[0], "is_deleted deleted=true");

  vector<string> rows_without_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, OBSERVABLE_ONLY,
                       &rows_without_flag));
  ASSERT_EQ(0, rows_without_flag.size());
}

TEST_F(OrderedDiffScanWithDeletesTest, ReinsertAfterDeleteNotReportedDeleted) {
  auto tablet = this->tablet();

  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);

  // Insert, update, delete, then re-insert the same key inside
  // (snap1, snap2]. The row is live at snap2.
  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 2));
  ASSERT_OK(DeleteTestRow(&writer, 1));
  ASSERT_OK(InsertTestRow(&writer, 1, 3));

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  // With the flag ON, the re-inserted row must appear as live, not deleted.
  vector<string> rows_with_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE,
                       &rows_with_flag));
  ASSERT_EQ(1, rows_with_flag.size());
  ASSERT_STR_CONTAINS(rows_with_flag[0], "is_deleted deleted=false");
  ASSERT_STR_CONTAINS(rows_with_flag[0], "val=3");

  // With the flag OFF, the row is still live at end_ts.
  vector<string> rows_without_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, OBSERVABLE_ONLY,
                       &rows_without_flag));
  ASSERT_EQ(1, rows_without_flag.size());
  ASSERT_STR_CONTAINS(rows_without_flag[0], "is_deleted deleted=false");
  ASSERT_STR_CONTAINS(rows_without_flag[0], "val=3");
}

TEST_F(OrderedDiffScanWithDeletesTest, UnobservableRowsDiskRowSet) {
  auto tablet = this->tablet();

  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);
  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(tablet->Flush());
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 2));
  ASSERT_OK(DeleteTestRow(&writer, 1));
  ASSERT_OK(tablet->FlushBiggestDMSForTests());

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  vector<string> rows;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE, &rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_STR_CONTAINS(rows[0], "is_deleted deleted=true");

  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, OBSERVABLE_ONLY, &rows));
  ASSERT_EQ(0, rows.size());
}

TEST_F(OrderedDiffScanWithDeletesTest, UnobservableRowsMultipleTransitions) {
  auto tablet = this->tablet();

  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);

  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 2));
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 3));
  ASSERT_OK(DeleteTestRow(&writer, 1));
  ASSERT_OK(tablet->Flush());
  // Below operations land in delta stores in DRS
  ASSERT_OK(InsertTestRow(&writer, 1, 4));
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 5));
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 6));
  ASSERT_OK(DeleteTestRow(&writer, 1));
  ASSERT_OK(tablet->FlushBiggestDMSForTests());

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  // With the flag ON, the row's lifespan is fully inside the window and
  // hence reported
  vector<string> rows_with_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE,
                       &rows_with_flag));
  ASSERT_EQ(1, rows_with_flag.size());
  ASSERT_STR_CONTAINS(rows_with_flag[0], "is_deleted deleted=true");

  // With the flag OFF, no rows are reported in the scan
  vector<string> rows_without_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, OBSERVABLE_ONLY,
                       &rows_without_flag));
  ASSERT_EQ(0, rows_without_flag.size());
}

TEST_F(OrderedDiffScanWithDeletesTest, ReinsertAcrossFlushNotDeleted) {
  auto tablet = this->tablet();
  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);

  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(DeleteTestRow(&writer, 1));
  ASSERT_OK(tablet->Flush());
  ASSERT_OK(InsertTestRow(&writer, 1, 2));

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  vector<string> rows;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE, &rows));
  // We expect one live row (the re-insert), NOT two rows and NOT a deleted marker.
  ASSERT_EQ(1, rows.size());
  ASSERT_STR_CONTAINS(rows[0], "is_deleted deleted=false");
  ASSERT_STR_CONTAINS(rows[0], "val=2");
}

TEST_F(OrderedDiffScanWithDeletesTest, UnobservableRowsMrsFlushedDmsUnflushed) {
  auto tablet = this->tablet();

  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);

  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(tablet->Flush());
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 2));
  ASSERT_OK(DeleteTestRow(&writer, 1));
  // NOTE: intentionally do NOT flush the DMS here.

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  vector<string> rows_with_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE,
                       &rows_with_flag));
  ASSERT_EQ(1, rows_with_flag.size());
  ASSERT_STR_CONTAINS(rows_with_flag[0], "is_deleted deleted=true");

  vector<string> rows_without_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, OBSERVABLE_ONLY,
                       &rows_without_flag));
  ASSERT_EQ(0, rows_without_flag.size());
}

TEST_F(OrderedDiffScanWithDeletesTest, UnobservableRowsMrsFlushedNoDeltas) {
  auto tablet = this->tablet();

  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);

  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 2));
  ASSERT_OK(DeleteTestRow(&writer, 1));
  ASSERT_OK(tablet->Flush());

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  vector<string> rows_with_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE,
                       &rows_with_flag));
  ASSERT_EQ(1, rows_with_flag.size());
  ASSERT_STR_CONTAINS(rows_with_flag[0], "is_deleted deleted=true");

  vector<string> rows_without_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, OBSERVABLE_ONLY,
                       &rows_without_flag));
  ASSERT_EQ(0, rows_without_flag.size());
}

TEST_F(OrderedDiffScanWithDeletesTest, UnobservableRowsInsertIgnoreDelete) {
  auto tablet = this->tablet();

  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);

  KuduPartialRow row(&client_schema_);
  setup_.BuildRow(&row, /*key_idx=*/1, /*val=*/1);
  ASSERT_OK(writer.InsertIgnore(row));
  ASSERT_OK(DeleteTestRow(&writer, 1));

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  vector<string> rows_with_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE, &rows_with_flag));
  ASSERT_EQ(1, rows_with_flag.size());
  ASSERT_STR_CONTAINS(rows_with_flag[0], "is_deleted deleted=true");

  vector<string> rows_without_flag;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, OBSERVABLE_ONLY, &rows_without_flag));
  ASSERT_EQ(0, rows_without_flag.size());
}

TEST_F(OrderedDiffScanWithDeletesTest,
       InsertDeleteDeleteIgnoreInsertReportedLive) {
  auto tablet = this->tablet();

  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);

  constexpr int64_t kRowKey = 1;
  ASSERT_OK(InsertTestRow(&writer, kRowKey, 1));
  ASSERT_OK(DeleteTestRow(&writer, kRowKey));

  KuduPartialRow key_only(&client_schema_);
  setup_.BuildRowKey(&key_only, kRowKey);
  ASSERT_OK(writer.DeleteIgnore(key_only));
  ASSERT_OK(InsertTestRow(&writer, kRowKey, 2));

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  for (RowVisibility rv : {INCLUDE_UNOBSERVABLE, OBSERVABLE_ONLY}) {
    SCOPED_TRACE(RowVisibility_Name(rv));
    vector<string> rows;
    NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, rv, &rows));
    ASSERT_EQ(1, rows.size());
    ASSERT_STR_CONTAINS(rows[0], "is_deleted deleted=false");
    ASSERT_STR_CONTAINS(rows[0], "val=2");
  }
}

TEST_F(OrderedDiffScanWithDeletesTest,
       InsertBeforeThenInsertIgnoreDeleteSameInBothModes) {
  auto tablet = this->tablet();

  LocalTabletWriter writer(tablet.get(), &client_schema_);
  constexpr int64_t kRowKey = 1;
  ASSERT_OK(InsertTestRow(&writer, kRowKey, 1));
  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap1(*tablet->mvcc_manager());

  KuduPartialRow row(&client_schema_);
  setup_.BuildRow(&row, kRowKey, /*val=*/2);
  ASSERT_OK(writer.InsertIgnore(row));
  ASSERT_OK(DeleteTestRow(&writer, kRowKey));

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  vector<string> rows_a;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE, &rows_a));
  vector<string> rows_b;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, OBSERVABLE_ONLY, &rows_b));

  ASSERT_EQ(1, rows_a.size());
  ASSERT_EQ(rows_a, rows_b);
  ASSERT_STR_CONTAINS(rows_a[0], "is_deleted deleted=true");
}

// Exercise a mix of row lifecycles relative to the (snap_start, snap_end]
// diff-scan window:
// key0: Absent at snap_start. INSERT + DELETE inside the window
//       => unobservable at both endpoints.
// key1: Absent at snap_start. INSERT + DELETE + INSERT inside the window
//       => observable at snap_end with the re-inserted value.
// key2: Present at snap_start. DELETE inside the window
//       => observable delete at snap_end.
// key3: Present at snap_start. no changes inside the window
//       => unchanged; must not appear in either mode.
TEST_F(OrderedDiffScanWithDeletesTest, MixedRowLifecycles) {
  auto tablet = this->tablet();
  LocalTabletWriter writer(tablet.get(), &client_schema_);

  // Rows that must already exist before snap_start.
  constexpr int64_t kKey2 = 2;
  constexpr int64_t kKey3 = 3;
  ASSERT_OK(InsertTestRow(&writer, kKey2, /*val=*/20));
  ASSERT_OK(InsertTestRow(&writer, kKey3, /*val=*/30));
  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap1(*tablet->mvcc_manager());

  // Mutations inside (snap1, snap2].
  constexpr int64_t kKey0 = 0;
  constexpr int64_t kKey1 = 1;
  ASSERT_OK(InsertTestRow(&writer, kKey0, /*val=*/100));
  ASSERT_OK(DeleteTestRow(&writer, kKey0));

  ASSERT_OK(InsertTestRow(&writer, kKey1, /*val=*/101));
  ASSERT_OK(DeleteTestRow(&writer, kKey1));
  ASSERT_OK(InsertTestRow(&writer, kKey1, /*val=*/102));

  ASSERT_OK(DeleteTestRow(&writer, kKey2));
  // key3: intentionally no changes.

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  // Assert on key_idx (the input-index column) rather than on the mangled
  // primary key: IntKeyTestSetup<INT64>::BuildRowKey stores
  // key = i * (i%2==0 ? -1 : 1). So kKey2 ends up with key=-2. Sorting the
  // printed rows lexicographically produces the same order as the primary key
  // order the ORDERED scan already emits (the '-' in "key=-2" sorts before any
  // digit), so we can safely index by position.

  // OBSERVABLE_ONLY: key0 must be absent (unobservable), key3 unchanged and
  // therefore absent. key1 surfaces with its final value (deleted=false), key2
  // surfaces as an observable delete (deleted=true).
  vector<string> rows;
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, OBSERVABLE_ONLY, &rows));
  ASSERT_EQ(2, rows.size()) << JoinStrings(rows, "\n");
  std::sort(rows.begin(), rows.end());
  // PK order: kKey2 stores as -2 -> sorts first; kKey1 stores as 1 -> second.
  ASSERT_STR_CONTAINS(rows[0], "key_idx=2");
  ASSERT_STR_CONTAINS(rows[0], "is_deleted deleted=true");
  ASSERT_STR_CONTAINS(rows[1], "key_idx=1");
  ASSERT_STR_CONTAINS(rows[1], "val=102");
  ASSERT_STR_CONTAINS(rows[1], "is_deleted deleted=false");

  // INCLUDE_UNOBSERVABLE: additionally surfaces key0 as an unobservable delete.
  NO_FATALS(DoDiffScan(tablet, snap1, snap2, ORDERED, INCLUDE_UNOBSERVABLE, &rows));
  ASSERT_EQ(3, rows.size()) << JoinStrings(rows, "\n");
  std::sort(rows.begin(), rows.end());
  // PK order: -2 (kKey2), 0 (kKey0), 1 (kKey1).
  ASSERT_STR_CONTAINS(rows[0], "key_idx=2");
  ASSERT_STR_CONTAINS(rows[0], "is_deleted deleted=true");
  ASSERT_STR_CONTAINS(rows[1], "key_idx=0");
  ASSERT_STR_CONTAINS(rows[1], "is_deleted deleted=true");
  ASSERT_STR_CONTAINS(rows[2], "key_idx=1");
  ASSERT_STR_CONTAINS(rows[2], "val=102");
  ASSERT_STR_CONTAINS(rows[2], "is_deleted deleted=false");
}

// Basic UNORDERED coverage for the INCLUDE_UNOBSERVABLE row-visibility mode.
// The client-facing SetDiffScan() path always forces ORDERED (via
// SetFaultTolerant()), so this exercises the tablet level union scan iterator
// directly.
class UnorderedDiffScanWithDeletesTest : public TabletTestBase<IntKeyTestSetup<INT64>> {
 public:
  UnorderedDiffScanWithDeletesTest()
      : Superclass(TabletHarness::Options::ClockType::HYBRID_CLOCK) {}

 private:
  using Superclass = TabletTestBase<IntKeyTestSetup<INT64>>;
};

TEST_F(UnorderedDiffScanWithDeletesTest, UnobservableRowsMemRowSet) {
  auto tablet = this->tablet();

  MvccSnapshot snap1(*tablet->mvcc_manager());
  LocalTabletWriter writer(tablet.get(), &client_schema_);
  ASSERT_OK(InsertTestRow(&writer, 1, 1));
  ASSERT_OK(UpdateTestRow(&writer, 1, 1, 2));
  ASSERT_OK(DeleteTestRow(&writer, 1));

  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  for (RowVisibility rv : {INCLUDE_UNOBSERVABLE, OBSERVABLE_ONLY}) {
    SCOPED_TRACE(RowVisibility_Name(rv));
    vector<string> rows;
    NO_FATALS(DoDiffScan(tablet, snap1, snap2, UNORDERED, rv, &rows));
    if (rv == INCLUDE_UNOBSERVABLE) {
      ASSERT_EQ(1, rows.size()) << JoinStrings(rows, "\n");
      ASSERT_STR_CONTAINS(rows[0], "is_deleted deleted=true");
    } else {
      ASSERT_EQ(0, rows.size()) << JoinStrings(rows, "\n");
    }
  }
}

// Regression test for KUDU-3291, where doing a diff scan after a delta flush
// raced with a batch update to a single row could result in a crash.
TEST_F(OrderedDiffScanWithDeletesTest, DiffScanAfterDeltaFlushRacesWithBatchUpdate) {
  auto tablet = this->tablet();
  auto tablet_id = tablet->tablet_id();
  LocalTabletWriter writer(tablet.get(), &client_schema_);
  constexpr int64_t kRowKey = 1;
  ASSERT_OK(InsertTestRow(&writer, kRowKey, 1));
  MvccSnapshot snap1(*tablet->mvcc_manager());

  // Start off with a DRS that we can add deltas to.
  ASSERT_OK(tablet->Flush());
  Status s;

  // Update the same row several times, and concurrently delta flush. Inject a
  // short, random sleep to encourage different sizes of delta stores. If
  // implemented incorrectly, the DeltaIteratorMerger could crash, unable to
  // disambiguate between rows of the delta stores being merged.
  const auto& sleep_ms = rand() % 3000;
  std::thread t([&] {
      SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
      s = tablet->FlushAllDMSForTests();
  });
  auto thread_joiner = MakeScopedCleanup([&] {
      t.join();
  });
  ASSERT_OK(UpdateTestRow(&writer, kRowKey, 0, 10000));
  thread_joiner.run();
  ASSERT_OK(s);
  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingOpsToApply());

  // Now perform a diff scan, which is an ordered scan with a start and end
  // timestamp.
  MvccSnapshot snap2(*tablet->mvcc_manager());
  RowIteratorOptions opts;
  opts.snap_to_exclude = snap1;
  opts.snap_to_include = snap2;
  opts.order = ORDERED;;
  SchemaBuilder builder(*tablet->metadata()->schema());
  Schema projection = builder.BuildWithoutIds();
  opts.projection = &projection;

  unique_ptr<RowwiseIterator> row_iterator;
  ASSERT_OK(tablet->NewRowIterator(std::move(opts), &row_iterator));
  ASSERT_TRUE(row_iterator);

  // Regression test for KUDU-3291, iterating through the rows shouldn't result
  // in a crash.
  ScanSpec spec;
  ASSERT_OK(row_iterator->Init(&spec));
  vector<string> rows;
  ASSERT_OK(tablet::IterateToStringList(row_iterator.get(), &rows));
  ASSERT_EQ(1, rows.size());
  ASSERT_STR_CONTAINS(rows[0], "val=9999");
}

} // namespace tablet
} // namespace kudu
