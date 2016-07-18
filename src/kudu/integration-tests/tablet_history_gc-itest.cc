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

#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"

using kudu::client::KuduScanner;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;

namespace kudu {

class TabletHistoryGcITest : public ExternalMiniClusterITestBase {
};

// Check that attempts to scan prior to the ancient history mark fail.
TEST_F(TabletHistoryGcITest, TestSnapshotScanBeforeAHM) {
  NO_FATALS(StartCluster({ "--tablet_history_max_age_sec=0" }));

  // Create a tablet so we can scan it.
  TestWorkload workload(cluster_.get());
  workload.Setup();

  // When the tablet history max age is set to 0, it's not possible to do a
  // snapshot scan without a timestamp because it's illegal to open a snapshot
  // prior to the AHM. When a snapshot timestamp is not specified, we decide on
  // the timestamp of the snapshot before checking that it's lower than the
  // current AHM. This test verifies that scans prior to the AHM are rejected.
  shared_ptr<KuduTable> table;
  ASSERT_OK(client_->OpenTable(TestWorkload::kDefaultTableName, &table));
  KuduScanner scanner(table.get());
  ASSERT_OK(scanner.SetReadMode(KuduScanner::READ_AT_SNAPSHOT));
  Status s = scanner.Open();
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Snapshot timestamp is earlier than the ancient history mark");
}

} // namespace kudu
