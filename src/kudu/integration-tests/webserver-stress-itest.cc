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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/webui_checker.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

// Tests that pounding the web UI doesn't cause any crashes.
TEST_F(KuduTest, TestWebUIDoesNotCrashCluster) {
  SKIP_IF_SLOW_NOT_ALLOWED();

#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
  // When using a sanitizer, checkers place a lot of load on the cluster.
  constexpr int kWebUICheckers = 1;
  constexpr int kNumReadThreads = 16;
  constexpr int kNumWriteThreads = 4;
#else
  constexpr int kWebUICheckers = 10;
  constexpr int kNumReadThreads = 64;
  constexpr int kNumWriteThreads = 8;
#endif
  constexpr int kNumTablets = 50;

  ExternalMiniClusterOptions opts;
#ifdef __linux__
  // We can only do explicit webserver ports on Linux, where we use
  // IPs like 127.x.y.z to bind the minicluster servers to different
  // hosts. This might make the test marginally flaky on OSX, but
  // it's easier than adding the ability to pipe separate webserver
  // ports to each server.
  opts.extra_master_flags.emplace_back("-webserver_port=11013");
  opts.extra_tserver_flags.emplace_back("-webserver_port=11014");
#endif
  opts.num_masters = 3;

  ExternalMiniCluster cluster(opts);
  ASSERT_OK(cluster.Start());

  // Start pounding the master and tserver's web UIs.
  vector<unique_ptr<PeriodicWebUIChecker>> checkers;
  checkers.reserve(kWebUICheckers);
  for (int i = 0; i < kWebUICheckers; i++) {
    checkers.emplace_back(new PeriodicWebUIChecker(
        cluster,
        MonoDelta::FromMilliseconds(1)));
  }

  // Create a table and write to it. Write some rows, so that there's something
  // in the tablets' WALs. Also, run many read threads to induce many scan
  // requests, so many threads would be spawned by tablet servers to handle
  // those.
  TestWorkload work(&cluster);
  work.set_timeout_allowed(true);
  work.set_num_replicas(1);
  work.set_num_read_threads(kNumReadThreads);
  work.set_num_write_threads(kNumWriteThreads);
  work.set_num_tablets(kNumTablets);
  work.Setup();
  work.Start();
  while (work.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  SleepFor(MonoDelta::FromSeconds(5));
  work.StopAndJoin();

  // Restart the cluster.
  NO_FATALS(cluster.AssertNoCrashes());
  cluster.Shutdown();
  ASSERT_OK(cluster.Restart());
  ASSERT_OK(cluster.WaitForTabletsRunning(cluster.tablet_server(0),
                                          kNumTablets,
                                          MonoDelta::FromSeconds(180)));
  NO_FATALS(cluster.AssertNoCrashes());
}

} // namespace kudu
