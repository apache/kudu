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

#include <memory>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/linked_list-test-util.h"
#include "kudu/integration-tests/log_verifier.h"
#include "kudu/util/test_util.h"

DEFINE_string(test_version_a_bin, "", "Directory to find the binaries for \"Version A\"");
DEFINE_string(test_version_b_bin, "", "Directory to find the binaries for \"Version B\"");

// For linked list testing
DEFINE_int32(seconds_to_run, 5, "Number of seconds for which to run the test");
DEFINE_int32(num_chains, 50, "Number of parallel chains to generate");
DEFINE_int32(num_tablets, 3, "Number of tablets over which to split the data");
DEFINE_bool(enable_mutation, true, "Enable periodic mutation of inserted rows");
DEFINE_int32(num_snapshots, 3, "Number of snapshots to verify across replicas and reboots.");
DEFINE_int32(num_replicas, 3, "Number of replicas per tablet");

using std::string;
using std::unique_ptr;
using std::vector;

const char* kTableName = "test_table";
const int kNumTabletServers = 3;

namespace kudu {
namespace itest {

// A test suite designed to test the ability to migrate between two (or more)
// versions of Kudu. Not invoked by "ctest".
class VersionMigrationTest : public KuduTest {
 protected:
  void SetUp() override {
    if (FLAGS_test_version_a_bin.empty() || FLAGS_test_version_b_bin.empty()) {
      FAIL() << "Must specify --test_version_a_bin and --test_version_b_bin flags";
    }
    KuduTest::SetUp();
  }

  void StartCluster(const vector<string>& extra_ts_flags = {},
                    const vector<string>& extra_master_flags = {},
                    int num_tablet_servers = 3,
                    const string& bin_path = "");

  unique_ptr<ExternalMiniCluster> cluster_;
  client::sp::shared_ptr<client::KuduClient> client_;
  unique_ptr<LinkedListTester> tester_;
  unique_ptr<LogVerifier> verifier_;
};

void VersionMigrationTest::StartCluster(const vector<string>& extra_ts_flags,
                                        const vector<string>& extra_master_flags,
                                        int num_tablet_servers,
                                        const string& bin_path) {
  ExternalMiniClusterOptions opts;
  if (!bin_path.empty()) opts.daemon_bin_path = bin_path;
  opts.num_tablet_servers = num_tablet_servers;
  opts.extra_master_flags = extra_master_flags;
  opts.extra_master_flags.push_back("--undefok=unlock_experimental_flags,unlock_unsafe_flags");
  opts.extra_tserver_flags = extra_ts_flags;
  opts.extra_tserver_flags.push_back("--undefok=unlock_experimental_flags,unlock_unsafe_flags");
  cluster_.reset(new ExternalMiniCluster(std::move(opts)));
  verifier_.reset(new LogVerifier(cluster_.get()));
  ASSERT_OK(cluster_->Start());
  ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  tester_.reset(new LinkedListTester(client_, kTableName,
                                     FLAGS_num_chains,
                                     FLAGS_num_tablets,
                                     FLAGS_num_replicas,
                                     FLAGS_enable_mutation));
}

// Test that a tablet created with "Version A" does not lose data and can be
// read after upgrading to "Version B" of the software.
TEST_F(VersionMigrationTest, TestLinkedListSimpleMigration) {

  LOG(INFO) << "Starting cluster using binaries at " << FLAGS_test_version_a_bin;
  NO_FATALS(StartCluster({}, {}, kNumTabletServers, FLAGS_test_version_a_bin));

  // Create the linked list and verify it with Version A.
  int64_t written = 0;
  ASSERT_OK(tester_->CreateLinkedListTable());
  ASSERT_OK(tester_->LoadLinkedList(MonoDelta::FromSeconds(FLAGS_seconds_to_run),
                                    FLAGS_num_snapshots,
                                    &written));
  ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
  ASSERT_OK(verifier_->VerifyCommittedOpIdsMatch());
  NO_FATALS(cluster_->AssertNoCrashes());

  // Now switch from Version A to Version B.
  cluster_->Shutdown();
  LOG(INFO) << "Restarting cluster using binaries at " << FLAGS_test_version_b_bin;
  cluster_->SetDaemonBinPath(FLAGS_test_version_b_bin);

  // Verify the linked list with Version B.
  // We loop this twice, in case we've introduced a bug where tablet bootstrap
  // might fail on the second restart after migrating between versions of Kudu.
  for (int i = 0; i < 2; i++) {
    cluster_->Restart();
    ASSERT_OK(tester_->WaitAndVerify(FLAGS_seconds_to_run, written));
    ASSERT_OK(verifier_->VerifyCommittedOpIdsMatch());
    NO_FATALS(cluster_->AssertNoCrashes());
    cluster_->Shutdown();
  }
}

} // namespace itest
} // namespace kudu
