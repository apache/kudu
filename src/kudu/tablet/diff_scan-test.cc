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
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/tablet/local_tablet_writer.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet-harness.h"
#include "kudu/tablet/tablet-test-base.h"
#include "kudu/tablet/tablet-test-util.h"
#include "kudu/tablet/tablet.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::vector;

namespace kudu {
namespace tablet {

class DiffScanTest : public TabletTestBase<IntKeyTestSetup<INT64>> {
 public:
  DiffScanTest()
      : Superclass(TabletHarness::Options::ClockType::HYBRID_CLOCK) {}

 private:
  using Superclass = TabletTestBase<IntKeyTestSetup<INT64>>;
};

TEST_F(DiffScanTest, TestDiffScan) {
  auto tablet = this->tablet();
  auto tablet_id = tablet->tablet_id();

  MvccSnapshot snap1;
  tablet->mvcc_manager()->TakeSnapshot(&snap1);

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
  MvccSnapshot snap2;
  tablet->mvcc_manager()->TakeSnapshot(&snap2);

  RowIteratorOptions opts;
  opts.snap_to_include = snap2;
  opts.order = ORDERED;
  opts.include_deleted_rows = true;

  auto projection = tablet->schema()->CopyWithoutColumnIds();
  opts.projection = &projection;

  gscoped_ptr<RowwiseIterator> row_iterator;
  ASSERT_OK(tablet->NewRowIterator(std::move(opts),
                                   &row_iterator));
  ASSERT_TRUE(row_iterator);
  ScanSpec spec;
  ASSERT_OK(row_iterator->Init(&spec));

  // For the time being, we should get two rows back, and they should both be
  // the same key. In reality, one has been deleted.
  // TODO(KUDU-2645): The result of this test should change once we properly
  // implement diff scans and the merge iterator is able to deduplicate ghosts.
  ASSERT_OK(tablet::IterateToStringList(row_iterator.get(), &rows));
  ASSERT_EQ(2, rows.size());
  EXPECT_EQ("(int64 key=1, int32 key_idx=1, int32 val=1)", rows[0]);
  EXPECT_EQ("(int64 key=1, int32 key_idx=1, int32 val=2)", rows[1]);
}

} // namespace tablet
} // namespace kudu
