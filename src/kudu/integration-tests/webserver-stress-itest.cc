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

#include <gtest/gtest.h>

#include "kudu/integration-tests/linked_list-test-util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {

using cluster::ExternalMiniCluster;
using cluster::ExternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using std::vector;

// Tests that pounding the web UI doesn't cause any crashes.
TEST_F(KuduTest, TestWebUIDoesNotCrashCluster) {
#if defined(THREAD_SANITIZER) || defined(ADDRESS_SANITIZER)
  // When using a sanitizer, checkers place a lot of load on the cluster.
  const int kWebUICheckers = 1;
#else
  const int kWebUICheckers = 10;
#endif
  const int kNumTablets = 50;

  ExternalMiniClusterOptions opts;
  // Force specific ports so that we can restart and guarantee we
  // can bind the same port. If we use ephemeral ports, it's possible
  // for one of the 'curl' threads to grab one of the ports as the local
  // side of a client TCP connection while the server is down, preventing
  // it from restarting. Choosing ports from the non-ephemeral range
  // prevents this.
  opts.master_rpc_ports = { 11010, 11011, 11012 };
#ifdef __linux__
  // We can only do explicit webserver ports on Linux, where we use
  // IPs like 127.x.y.z to bind the minicluster servers to different
  // hosts. This might make the test marginally flaky on OSX, but
  // it's easier than adding the ability to pipe separate webserver
  // ports to each server.
  opts.extra_master_flags.emplace_back("-webserver_port=11013");
  opts.extra_tserver_flags.emplace_back("-webserver_port=11014");
#endif
  opts.num_masters = opts.master_rpc_ports.size();

  ExternalMiniCluster cluster(opts);
  ASSERT_OK(cluster.Start());

  // Start pounding the master and tserver's web UIs.
  vector<unique_ptr<PeriodicWebUIChecker>> checkers;
  for (int i = 0; i < kWebUICheckers; i++) {
    checkers.emplace_back(new PeriodicWebUIChecker(
        cluster,
        "doesn't matter", // will ping a non-existent page
        MonoDelta::FromMilliseconds(1)));
  }

  // Create a table and write to it. Just a few rows, so that there's something
  // in the tablets' WALs.
  TestWorkload work(&cluster);
  work.set_timeout_allowed(true);
  work.set_num_replicas(1);
  work.set_num_tablets(kNumTablets);
  work.Setup();
  work.Start();
  while (work.rows_inserted() < 100) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
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
