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

#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/path_util.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"

DECLARE_string(minidump_path);

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

// Test the creation of minidumps upon process crash.
class MinidumpGenerationITest : public ExternalMiniClusterITestBase {
 protected:
  void WaitForMinidumps(int expected, const string& dir);
};

void MinidumpGenerationITest::WaitForMinidumps(int expected, const string& dir) {
  AssertEventually([&] {
    vector<string> matches;
    ASSERT_OK(env_->Glob(JoinPathSegments(dir, "*.dmp"), &matches));
    ASSERT_EQ(expected, matches.size());
  });
}

TEST_F(MinidumpGenerationITest, TestCreateMinidumpOnCrash) {
  // Minidumps are disabled by default in the ExternalMiniCluster.
  NO_FATALS(StartCluster({"--enable_minidumps"}, {"--enable_minidumps"}, 1));

  // Test kudu-tserver.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  string dir = Substitute("$0/$1/$2", ts->log_dir(), "minidumps", "kudu-tserver");
  ts->process()->Kill(SIGABRT);
  NO_FATALS(WaitForMinidumps(1, dir));

  // Test kudu-master.
  ExternalMaster* master = cluster_->master();
  dir = Substitute("$0/$1/$2", master->log_dir(), "minidumps", "kudu-master");
  master->process()->Kill(SIGABRT);
  NO_FATALS(WaitForMinidumps(1, dir));
}

TEST_F(MinidumpGenerationITest, TestCreateMinidumpOnSIGUSR1) {
  // Minidumps are disabled by default in the ExternalMiniCluster.
  NO_FATALS(StartCluster({"--enable_minidumps"}, {"--enable_minidumps"}, 1));

  // Enable minidumps and ensure SIGUSR1 generates them.
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  string dir = Substitute("$0/$1/$2", ts->log_dir(), "minidumps", "kudu-tserver");
  ts->process()->Kill(SIGUSR1);
  NO_FATALS(WaitForMinidumps(1, dir));
  NO_FATALS(cluster_->AssertNoCrashes());
  cluster_->Shutdown();

  // Disable minidumps and ensure SIGUSR1 remains non-fatal.
  ts->mutable_flags()->push_back("--enable_minidumps=false");
  ASSERT_OK(env_->DeleteRecursively(dir));
  ASSERT_OK(env_->CreateDir(dir));
  ASSERT_OK(cluster_->Restart());
  ts->process()->Kill(SIGUSR1);
  NO_FATALS(cluster_->AssertNoCrashes());
  NO_FATALS(WaitForMinidumps(0, dir)); // There should be no dumps.
}

} // namespace kudu
