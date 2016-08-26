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
//
// Tests for the kudu-admin command-line tool.

#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/util/path_util.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"

using kudu::itest::TabletServerMap;
using kudu::itest::TServerDetails;
using strings::Split;
using strings::Substitute;

namespace kudu {
namespace tools {

static const char* const kTsCliToolName = "kudu-ts-cli";

class KuduTsCliTest : public ExternalMiniClusterITestBase {
 protected:
  // Figure out where the admin tool is.
  string GetTsCliToolPath() const;
};

string KuduTsCliTest::GetTsCliToolPath() const {
  string exe;
  CHECK_OK(Env::Default()->GetExecutablePath(&exe));
  string binroot = DirName(exe);
  string tool_path = JoinPathSegments(binroot, kTsCliToolName);
  CHECK(Env::Default()->FileExists(tool_path)) << "kudu-admin tool not found at " << tool_path;
  return tool_path;
}

// Test deleting a tablet using kudu-ts-cli tool.
TEST_F(KuduTsCliTest, TestDeleteTablet) {
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  NO_FATALS(StartCluster(
        {"--enable_leader_failure_detection=false"},
        {"--catalog_manager_wait_for_new_tablets_to_elect_leader=false"}));

  TestWorkload workload(cluster_.get());
  workload.Setup(); // Easy way to create a new tablet.

  vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  for (const itest::TabletServerMap::value_type& entry : ts_map_) {
    TServerDetails* ts = entry.second;
    ASSERT_OK(itest::WaitForNumTabletsOnTS(ts, 1, timeout, &tablets));
  }
  string tablet_id = tablets[0].tablet_status().tablet_id();

  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }
  string out;
  ASSERT_OK(Subprocess::Call({
    GetTsCliToolPath(),
    "--server_address",
    cluster_->tablet_server(0)->bound_rpc_addr().ToString(),
    "delete_tablet",
    tablet_id,
    "Deleting for kudu-ts-cli-test"
  }, &out));
  ASSERT_EQ("", out);

  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(0, tablet_id, { tablet::TABLET_DATA_TOMBSTONED }));
  TServerDetails* ts = ts_map_[cluster_->tablet_server(0)->uuid()];
  ASSERT_OK(itest::WaitUntilTabletInState(ts, tablet_id, tablet::SHUTDOWN, timeout));
}

// Test dumping a tablet using kudu-ts-cli tool.
TEST_F(KuduTsCliTest, TestDumpTablet) {
  const int kNumRows = 5;
  MonoDelta timeout = MonoDelta::FromSeconds(30);
  NO_FATALS(StartCluster({}, {}));

  TestWorkload workload(cluster_.get());
  workload.set_write_batch_size(1); // One batch is enough to dump some output.
  workload.Setup();

  vector<tserver::ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  for (const itest::TabletServerMap::value_type& entry : ts_map_) {
    TServerDetails* ts = entry.second;
    ASSERT_OK(itest::WaitForNumTabletsOnTS(ts, 1, timeout, &tablets));
  }
  string tablet_id = tablets[0].tablet_status().tablet_id();

  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    ASSERT_OK(itest::WaitUntilTabletRunning(ts_map_[cluster_->tablet_server(i)->uuid()],
                                            tablet_id, timeout));
  }

  string out;
  // Test for dump_tablet when there is no data in tablet.
  ASSERT_OK(Subprocess::Call({
    GetTsCliToolPath(),
    "--server_address",
    cluster_->tablet_server(0)->bound_rpc_addr().ToString(),
    "dump_tablet",
    tablet_id
  }, &out));
  ASSERT_EQ("", out);

  // Insert very little data and dump_tablet again.
  workload.Start();
  while (workload.rows_inserted() < kNumRows) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();
  ASSERT_OK(WaitForServersToAgree(timeout, ts_map_, tablet_id, workload.batches_completed()));
  ASSERT_OK(Subprocess::Call({
    GetTsCliToolPath(),
    "--server_address",
    cluster_->tablet_server(0)->bound_rpc_addr().ToString(),
    "dump_tablet",
    tablet_id
  }, &out));

  // Split the output into multiple rows and check format of each row,
  // and also check total number of rows are at least kNumRows.
  int nrows = 0;
  vector<string> rows = strings::Split(out, "\n", strings::SkipEmpty());
  for (const auto& row : rows) {
    ASSERT_STR_MATCHES(row, "int32 key=");
    ASSERT_STR_MATCHES(row, "int32 int_val=");
    ASSERT_STR_MATCHES(row, "string string_val=hello world");
    nrows++;
  }
  ASSERT_GE(nrows, kNumRows);
}
} // namespace tools
} // namespace kudu
