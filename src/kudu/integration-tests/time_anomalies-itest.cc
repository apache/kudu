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

#include <ctime>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/clock/test/mini_chronyd.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/util/monotime.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(max_clock_sync_error_usec);

using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::BuiltinNtpConfigMode;
using kudu::cluster::ClusterNodes;
using std::string;
using std::vector;

namespace kudu {

class TimeAnomaliesITest : public ExternalMiniClusterITestBase {
};

// A scenario to assert on the expected behavior of Kudu servers if true time
// is far behind of HybridClock timestamps recorded in tablet replicas' WALs.
TEST_F(TimeAnomaliesITest, CrashOnBootstrapIfClockFarAhead) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const auto kTServersNum = 3;

  ExternalMiniClusterOptions cluster_opts;
  cluster_opts.num_tablet_servers = kTServersNum;
  cluster_opts.num_ntp_servers = cluster_opts.num_tablet_servers;
  cluster_opts.ntp_config_mode = BuiltinNtpConfigMode::ROUND_ROBIN_SINGLE_SERVER;
  NO_FATALS(StartClusterWithOpts(std::move(cluster_opts)));

  // Set NTP time in the future, so new timestamps are ahead of the ones present
  // in WAL records. Restart tablet servers to be sure their built-in NTP
  // clients immediately have new times.
  cluster_->ShutdownNodes(ClusterNodes::TS_ONLY);
  {
    vector<clock::MiniChronyd*> ntp_servers = cluster_->ntp_servers();
    const time_t t = time(nullptr) +
        5 * FLAGS_max_clock_sync_error_usec / (1000 * 1000);
    for (auto* s : ntp_servers) {
      ASSERT_OK(s->SetTime(t));
    }
  }
  ASSERT_OK(cluster_->Restart());

  // Create a table with RF equal to the number of tablet servers in the
  // cluster to make sure every tablet servers contains a repica of a tablet.
  // Populate the tablet with some data.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(kTServersNum);
  workload.Setup();
  workload.Start();
  while (workload.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  NO_FATALS(cluster_->AssertNoCrashes());
  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckCluster());
  NO_FATALS(v.CheckRowCount(workload.table_name(),
                            ClusterVerifier::EXACTLY,
                            workload.rows_inserted() - workload.rows_deleted()));

  // Return the time back to 'normal': the timestamps of ops in WAL
  // will become far ahead of timestamps output by the built-in NTP client.
  cluster_->ShutdownNodes(ClusterNodes::TS_ONLY);
  {
    vector<clock::MiniChronyd*> ntp_servers = cluster_->ntp_servers();
    const time_t t = time(nullptr);
    for (auto* s : ntp_servers) {
      ASSERT_OK(s->SetTime(t));
    }
  }

  for (auto idx = 0; idx < cluster_->num_tablet_servers(); ++idx) {
    auto* srv = cluster_->tablet_server(idx);
    // Inject a bit of delay before bootstrapping tablets: this is to allow for
    // Status::OK() returned upon starting tablet servers.
    srv->mutable_flags()->emplace_back("--tablet_bootstrap_inject_latency_ms=3000");
    ASSERT_OK(srv->Restart());
  }

  // All tablet servers should crash on attempt to load data timestamped with
  // offset more than FLAGS_max_clock_sync_error_usec in the future from current
  // clock timestamps.
  const MonoDelta kTimeout = MonoDelta::FromSeconds(30);
  for (auto idx = 0; idx < cluster_->num_tablet_servers(); ++idx) {
    auto* srv = cluster_->tablet_server(idx);
    ASSERT_OK(srv->WaitForFatal(kTimeout));
  }
}

} // namespace kudu
