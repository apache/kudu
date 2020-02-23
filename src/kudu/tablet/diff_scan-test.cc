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
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/iterator.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

class DiffScanTest : public TabletTestBase<IntKeyTestSetup<INT64>>,
                     public ::testing::WithParamInterface<std::tuple<OrderMode, bool>> {
 public:
  DiffScanTest()
      : Superclass(TabletHarness::Options::ClockType::HYBRID_CLOCK) {}

 private:
  using Superclass = TabletTestBase<IntKeyTestSetup<INT64>>;
};

INSTANTIATE_TEST_CASE_P(DiffScanModes, DiffScanTest,
                        ::testing::Combine(
                            /*order_mode*/ ::testing::Values(UNORDERED, ORDERED),
                            /*include_deleted_rows*/ ::testing::Bool()));

TEST_P(DiffScanTest, TestDiffScan) {
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
  ASSERT_OK(tablet->mvcc_manager()->WaitForApplyingTransactionsToCommit());
  MvccSnapshot snap2(*tablet->mvcc_manager());

  RowIteratorOptions opts;
  opts.snap_to_include = snap2;
  opts.order = order_mode;
  opts.include_deleted_rows = include_deleted_rows;

  static const bool kIsDeletedDefault = false;
  SchemaBuilder builder(tablet->metadata()->schema());
  if (order_mode == ORDERED) {
    // The merge iterator requires an IS_DELETED column when including deleted
    // rows in order to support deduplication of the rows.
    ASSERT_OK(builder.AddColumn("deleted", IS_DELETED,
                                /*is_nullable=*/ false,
                                /*read_default=*/ &kIsDeletedDefault,
                                /*write_default=*/ nullptr));
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

} // namespace tablet
} // namespace kudu
