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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/external_mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

using cluster::ExternalTabletServer;
using std::map;
using std::string;
using std::vector;

class MultiDirClusterITest : public ExternalMiniClusterITestBase {};

TEST_F(MultiDirClusterITest, TestBasicMultiDirCluster) {
  const uint32_t kNumDataDirs = 3;
  vector<string> ts_flags = {
    // Flush frequently to trigger writes.
    "--flush_threshold_mb=1",
    "--flush_threshold_secs=1",

    // Spread tablet data across all data dirs.
    "--fs_target_data_dirs_per_tablet=0"
  };

  NO_FATALS(StartCluster(ts_flags, {}, /* num_tablet_servers= */ 1, kNumDataDirs));
  ExternalTabletServer* ts = cluster_->tablet_server(0);
  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.Setup();

  // Check that all daemons have the expected number of directories.
  ASSERT_EQ(kNumDataDirs, cluster_->master()->data_dirs().size());
  ASSERT_EQ(kNumDataDirs, ts->data_dirs().size());

  // Take an initial snapshot of the number of files in each directory.
  map<string, int> num_files_in_each_dir;
  for (const string& data_dir : ts->data_dirs()) {
    string data_path = JoinPathSegments(data_dir, "data");
    vector<string> files;
    ASSERT_OK(inspect_->ListFilesInDir(data_path, &files));
    InsertOrDie(&num_files_in_each_dir, data_dir, files.size());
  }

  work.Start();
  ASSERT_EVENTUALLY([&] {
    // Check that files are being written to more than one directory.
    int num_dirs_added_to = 0;
    for (const string& data_dir : ts->data_dirs()) {
      string data_path = JoinPathSegments(data_dir, "data");
      vector<string> files;
      inspect_->ListFilesInDir(data_path, &files);
      int* num_files_before_insert = FindOrNull(num_files_in_each_dir, data_dir);
      ASSERT_NE(nullptr, num_files_before_insert);
      if (*num_files_before_insert < files.size()) {
        num_dirs_added_to++;
      }
    }
    // Block placement should guarantee that more than one data dir will have
    // data written to it.
    ASSERT_GT(num_dirs_added_to, 1);
    vector<string> wal_files;
    ASSERT_OK(inspect_->ListFilesInDir(JoinPathSegments(ts->wal_dir(), "wals"), &wal_files));
    ASSERT_FALSE(wal_files.empty());
  });
  work.StopAndJoin();
}

}  // namespace kudu
