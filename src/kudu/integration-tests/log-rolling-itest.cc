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

// Tests that log rolling and excess logfile cleanup logic works correctly.

#include <algorithm>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/env.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

using cluster::ExternalMiniCluster;
using cluster::ExternalMiniClusterOptions;

class LogRollingITest : public KuduTest {};

static int64_t CountInfoLogs(const string& log_dir) {
    vector<string> logfiles;
    string pattern = Substitute("$0/*.$1.*", log_dir, "INFO");
    CHECK_OK(Env::Default()->Glob(pattern, &logfiles));
    return logfiles.size();
}

// Tests that logs roll on startup, and get cleaned up appropriately.
TEST_F(LogRollingITest, TestLogCleanupOnStartup) {
  ExternalMiniClusterOptions opts;
  opts.num_masters = 1;
  opts.num_tablet_servers = 0;
  opts.extra_master_flags = { "--max_log_files=3", };
  opts.logtostderr = false;
  ExternalMiniCluster cluster(std::move(opts));
  ASSERT_OK(cluster.Start());

  // Explicitly wait for the catalog manager because we've got no tservers in
  // our cluster, which means the usual waiting in ExternalMiniCluster::Start()
  // won't work.
  //
  // This is only needed the first time the master starts so that it can write
  // the catalog out to disk.
  ASSERT_OK(cluster.master()->WaitForCatalogManager());

  for (int i = 1; i <= 10; i++) {
    ASSERT_EVENTUALLY([&] () {
        ASSERT_EQ(std::min(3, i), CountInfoLogs(cluster.master()->log_dir()));
    });
    cluster.master()->Shutdown();
    ASSERT_OK(cluster.master()->Restart());
  }
}
} // namespace kudu
