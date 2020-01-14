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
#include <cstdlib>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/mini_cluster_fs_inspector.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

METRIC_DECLARE_gauge_uint64(tablets_num_failed);

using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::ExternalTabletServer;
using kudu::itest::StartTabletCopy;
using kudu::itest::TServerDetails;
using kudu::itest::WaitUntilTabletRunning;
using kudu::tablet::TabletDataState;
using kudu::tserver::ListTabletsResponsePB;
using std::string;
using std::thread;
using std::vector;
using strings::Substitute;

namespace kudu {

// Integration test for StartTabletCopy() and related functionality.
class TabletCopyClientSessionITest : public ExternalMiniClusterITestBase {
 protected:
  // Bring up two tablet servers. Load tablet(s) onto TS 0, while TS 1 is left
  // blank. Prevent the master from tombstoning evicted replicas.
  void PrepareClusterForTabletCopy(ExternalMiniClusterOptions opts = ExternalMiniClusterOptions(),
                                   int num_tablets = kDefaultNumTablets);

  static const int kDefaultNumTablets;
  const MonoDelta kDefaultTimeout = MonoDelta::FromSeconds(30);
};

const int TabletCopyClientSessionITest::kDefaultNumTablets = 1;

void TabletCopyClientSessionITest::PrepareClusterForTabletCopy(
    ExternalMiniClusterOptions opts,
    int num_tablets) {
  const int kNumTabletServers = 2;
  // We don't want the master to interfere when we manually make copies of
  // tablets onto servers it doesn't know about.
  opts.extra_master_flags.emplace_back("--master_tombstone_evicted_tablet_replicas=false");
  opts.num_tablet_servers = kNumTabletServers;
  NO_FATALS(StartClusterWithOpts(std::move(opts)));
  // Shut down the 2nd tablet server; we'll create tablets on the first one.
  cluster_->tablet_server(1)->Shutdown();

  // Restart the Master so it doesn't try to assign tablets to the dead TS.
  cluster_->master()->Shutdown();
  ASSERT_OK(cluster_->master()->Restart());
  ASSERT_OK(cluster_->WaitForTabletServerCount(kNumTabletServers - 1, kDefaultTimeout));

  // Write a bunch of data to the first tablet server.
  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(1);
  workload.set_num_tablets(num_tablets);
  workload.Setup();
  workload.Start();

  // Get some data written to the write ahead log.
  while (workload.rows_inserted() < 10000) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  TServerDetails* ts0 = ts_map_[cluster_->tablet_server(0)->uuid()];
  ASSERT_OK(WaitForNumTabletsOnTS(ts0, num_tablets, kDefaultTimeout, &tablets));

  workload.StopAndJoin();

  // Restart TS 1 so we can use it in our tests.
  ASSERT_OK(cluster_->tablet_server(1)->Restart());
}

// Regression test for KUDU-1785. Ensure that starting a tablet copy session
// while a tablet is bootstrapping will result in a simple failure, not a crash.
TEST_F(TabletCopyClientSessionITest, TestStartTabletCopyWhileSourceBootstrapping) {
  if (!AllowSlowTests()) {
    LOG(INFO) << "Test only runs in slow test mode";
    return;
  }
  const MonoDelta kTimeout = MonoDelta::FromSeconds(90); // Can be very slow on TSAN.
  NO_FATALS(PrepareClusterForTabletCopy());

  TServerDetails* ts0 = ts_map_[cluster_->tablet_server(0)->uuid()];
  TServerDetails* ts1 = ts_map_[cluster_->tablet_server(1)->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts0, kDefaultNumTablets, kTimeout, &tablets));
  ASSERT_EQ(kDefaultNumTablets, tablets.size());
  const string& tablet_id = tablets[0].tablet_status().tablet_id();
  HostPort src_addr;
  ASSERT_OK(HostPortFromPB(ts0->registration.rpc_addresses(0), &src_addr));

  // Repeat the restart process several times.
  for (int i = 0; i < 3; i++) {
    LOG(INFO) << "Starting loop " << i;
    // Shut down the source tablet server (TS 0) so we can catch it with our
    // StartTabletCopy requests during startup.
    cluster_->tablet_server(0)->Shutdown();

    const int kNumStartTabletThreads = 4;
    vector<scoped_refptr<Thread>> threads;
    for (int j = 0; j < kNumStartTabletThreads; j++) {
      scoped_refptr<Thread> t;
      CHECK_OK(Thread::Create("test", "start-tablet-copy", [&]() {
        // Retry until it succeeds (we will intially race against TS 0 startup).
        MonoTime deadline = MonoTime::Now() + kTimeout;
        Status s;
        while (true) {
          if (MonoTime::Now() > deadline) {
            LOG(WARNING) << "Test thread timed out waiting for bootstrap: " << s.ToString();
            return;
          }
          s = StartTabletCopy(ts1, tablet_id, ts0->uuid(), src_addr, 0,
                              deadline - MonoTime::Now());
          if (s.ok()) {
            break;
          }
          s = CheckIfTabletRunning(ts1, tablet_id, deadline - MonoTime::Now());
          if (s.ok()) {
            break;
          }
          SleepFor(MonoDelta::FromMilliseconds(rand() % 50));
          continue;
        }
        // If we got here, we either successfully started a tablet copy or we
        // observed the tablet running.
      }, &t));
      threads.push_back(t);
    }

    // Restart the source tablet server (TS 0).
    ASSERT_OK(cluster_->tablet_server(0)->Restart());

    // Wait for one of the threads to succeed with its tablet copy and for the
    // tablet to be running on TS 1.
    ASSERT_OK(WaitUntilTabletRunning(ts1, tablet_id, kTimeout));

    for (auto& t : threads) {
      t->Join();
    }

    NO_FATALS(cluster_->AssertNoCrashes());

    // There is a delay between the tablet marking itself as RUNNING and the
    // tablet copy operation completing, so we retry tablet deletion attempts.
    // We want a clean deletion so only one thread wins, then we have to
    // restart TS 1 to clear knowledge of the replica from memory.
    ASSERT_OK(DeleteTabletWithRetries(ts1, tablet_id,
                                      TabletDataState::TABLET_DATA_DELETED,
                                      kTimeout));
    cluster_->tablet_server(1)->Shutdown();
    ASSERT_OK(cluster_->tablet_server(1)->Restart());
  }
}

// Test that StartTabletCopy() works in different scenarios.
TEST_F(TabletCopyClientSessionITest, TestStartTabletCopy) {
  NO_FATALS(PrepareClusterForTabletCopy());

  TServerDetails* ts0 = ts_map_[cluster_->tablet_server(0)->uuid()];
  TServerDetails* ts1 = ts_map_[cluster_->tablet_server(1)->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts0, kDefaultNumTablets, kDefaultTimeout, &tablets));
  ASSERT_EQ(kDefaultNumTablets, tablets.size());
  const string& tablet_id = tablets[0].tablet_status().tablet_id();

  // Scenarios to run tablet copy on top of:
  enum Scenarios {
    kPristine,    // No tablets.
    kTombstoned,  // A tombstoned tablet.
    kLast
  };
  for (int scenario = 0; scenario < kLast; scenario++) {
    if (scenario == kTombstoned) {
      ASSERT_OK(DeleteTabletWithRetries(ts1, tablet_id,
                                        TabletDataState::TABLET_DATA_TOMBSTONED,
                                        kDefaultTimeout));
    }

    // Run tablet copy.
    HostPort src_addr;
    ASSERT_OK(HostPortFromPB(ts0->registration.rpc_addresses(0), &src_addr));
    ASSERT_OK(StartTabletCopy(ts1, tablet_id, ts0->uuid(), src_addr,
                              std::numeric_limits<int64_t>::max(), kDefaultTimeout));
    ASSERT_OK(WaitUntilTabletRunning(ts1, tablet_id, kDefaultTimeout));
  }
}

// Test that a tablet copy session will tombstone the tablet if the source
// server crashes in the middle of the tablet copy.
TEST_F(TabletCopyClientSessionITest, TestCopyFromCrashedSource) {
  NO_FATALS(PrepareClusterForTabletCopy());

  TServerDetails* ts0 = ts_map_[cluster_->tablet_server(0)->uuid()];
  TServerDetails* ts1 = ts_map_[cluster_->tablet_server(1)->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts0, kDefaultNumTablets, kDefaultTimeout, &tablets));
  ASSERT_EQ(kDefaultNumTablets, tablets.size());
  const string& tablet_id = tablets[0].tablet_status().tablet_id();

  // Crash when serving tablet copy.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "fault_crash_on_handle_tc_fetch_data",
                              "1.0"));

  HostPort src_addr;
  ASSERT_OK(HostPortFromPB(ts0->registration.rpc_addresses(0), &src_addr));
  ASSERT_OK(StartTabletCopy(ts1, tablet_id, ts0->uuid(), src_addr,
                            std::numeric_limits<int64_t>::max(), kDefaultTimeout));

  ASSERT_OK(inspect_->WaitForTabletDataStateOnTS(1, tablet_id,
                                                 { TabletDataState::TABLET_DATA_TOMBSTONED },
                                                 kDefaultTimeout));

  // The source server will crash.
  ASSERT_OK(cluster_->tablet_server(0)->WaitForInjectedCrash(kDefaultTimeout));
  cluster_->tablet_server(0)->Shutdown();

  // It will restart without the fault flag set.
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  // Attempt the copy again. This time it should succeed.
  ASSERT_OK(WaitUntilTabletRunning(ts0, tablet_id, kDefaultTimeout));
  // KUDU-2444: Even though the copy failed and the tablet replica is
  // tombstoned, the tablet manager may not have transitioned the replica out
  // of the 'copying' phase yet, so we retry here.
  ASSERT_EVENTUALLY([&]{
    ASSERT_OK(StartTabletCopy(ts1, tablet_id, ts0->uuid(), src_addr,
                              std::numeric_limits<int64_t>::max(), kDefaultTimeout));
  });
  ASSERT_OK(WaitUntilTabletRunning(ts1, tablet_id, kDefaultTimeout));
}

// Regression for KUDU-2125: ensure that a heavily loaded source cluster can
// satisfy many concurrent tablet copies.
TEST_F(TabletCopyClientSessionITest, TestTabletCopyWithBusySource) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }
  const int kNumTablets = 20;

  ExternalMiniClusterOptions opts;
  opts.extra_tserver_flags.emplace_back(Substitute("--num_tablets_to_copy_simultaneously=$0",
                                                      kNumTablets));
  NO_FATALS(PrepareClusterForTabletCopy(opts, kNumTablets));

  // Tune down the RPC capacity on the source server to ensure
  // ERROR_SERVER_TOO_BUSY errors occur.
  cluster_->tablet_server(0)->mutable_flags()->emplace_back("--rpc_service_queue_length=1");
  cluster_->tablet_server(0)->mutable_flags()->emplace_back("--rpc_num_service_threads=1");

  // Restart the TS for the new flags to take effect.
  cluster_->tablet_server(0)->Shutdown();
  ASSERT_OK(cluster_->tablet_server(0)->Restart());

  TServerDetails* ts0 = ts_map_[cluster_->tablet_server(0)->uuid()];
  TServerDetails* ts1 = ts_map_[cluster_->tablet_server(1)->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts0, kNumTablets, kDefaultTimeout,
                                  &tablets, tablet::TabletStatePB::RUNNING));
  ASSERT_EQ(kNumTablets, tablets.size());

  HostPort src_addr;
  ASSERT_OK(HostPortFromPB(ts0->registration.rpc_addresses(0), &src_addr));

  vector<thread> threads;
  for (const auto& tablet : tablets) {
    threads.emplace_back(thread([&] {
      const string& tablet_id = tablet.tablet_status().tablet_id();
      // Run tablet copy.
      CHECK_OK(StartTabletCopy(ts1, tablet_id, ts0->uuid(), src_addr,
                               std::numeric_limits<int64_t>::max(), kDefaultTimeout));
      CHECK_OK(WaitUntilTabletRunning(ts1, tablet_id, kDefaultTimeout));
    }));
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

// Test that fails a disk during copies/bootstraps and ensures the tablets are
// appropriately failed.
TEST_F(TabletCopyClientSessionITest, TestStopCopyOnClientDiskFailure) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  const int kNumTablets = 10;
  const MonoDelta kTimeout = MonoDelta::FromSeconds(90);
  ExternalMiniClusterOptions opts;
  opts.extra_tserver_flags = {
    Substitute("--num_tablets_to_copy_simultaneously=$0", kNumTablets),

    // Ensure we get some blocks to copy. This also serves to stress more
    // complex, concurrent codepaths.
    "--flush_threshold_mb=1",
    "--flush_threshold_secs=1",
  };

  // Create a cluster with multiple directories per server so we can fail a
  // directory without crashing.
  opts.num_data_dirs = 2;
  NO_FATALS(PrepareClusterForTabletCopy(opts, kNumTablets));

  ExternalTabletServer* ets_src = cluster_->tablet_server(0);
  ExternalTabletServer* ets_dst = cluster_->tablet_server(1);
  TServerDetails* ts_src = ts_map_[ets_src->uuid()];
  TServerDetails* ts_dst = ts_map_[ets_dst->uuid()];
  vector<ListTabletsResponsePB::StatusAndSchemaPB> tablets;
  ASSERT_OK(WaitForNumTabletsOnTS(ts_src, kNumTablets, kTimeout, &tablets));
  ASSERT_EQ(kNumTablets, tablets.size());

  // Wait a bit for some flushes to occur and blocks to appear.
  ASSERT_EVENTUALLY([&] {
    // Each data dir has '.', '..', and 'block_manager_instance'.
    const string& dir_with_data = JoinPathSegments(ets_src->data_dirs()[0], "data");
    ASSERT_GT(inspect_->CountFilesInDir(dir_with_data), 3);
  });

  // Now kick off the tablet copies.
  HostPort src_addr;
  ASSERT_OK(HostPortFromPB(ts_src->registration.rpc_addresses(0), &src_addr));
  vector<thread> threads;
  auto copy_tablet_with_index = [&] (int i) {
    LOG(INFO) << Substitute("Copying tablet $0 / $1", i + 1, kNumTablets);
    const string& tablet_id = tablets[i].tablet_status().tablet_id();
    // The copy can fail if the tablet in the middle of some maintenance
    // operations, complaining about missing blocks. Just try again.
    while (!StartTabletCopy(ts_dst, tablet_id, ts_src->uuid(), src_addr,
           std::numeric_limits<int64_t>::max(), kDefaultTimeout).ok()) {
      SleepFor(MonoDelta::FromMilliseconds(50));
    }
  };
  for (int i = 0; i < kNumTablets - 1; i++) {
    threads.emplace_back([=] {
      copy_tablet_with_index(i);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // Wait for all copies to start by waiting for the number of tablets on the
  // tserver to reach the number of copies we've started. Note: these copies
  // may still be ongoing.
  ASSERT_OK(WaitForNumTabletsOnTS(ts_dst, kNumTablets - 1, kDefaultTimeout));

  // Inject failures into a directory on the receiving server.
  const string& dir_to_fail = ets_dst->data_dirs()[1];
  LOG(INFO) << "Injecting failures to " << dir_to_fail;
  ASSERT_OK(cluster_->SetFlag(ets_dst, "env_inject_eio_globs",
      JoinPathSegments(dir_to_fail, "**")));

  // Copy over the last tablet and immediately try failing it.
  copy_tablet_with_index(kNumTablets - 1);
  ASSERT_OK(cluster_->SetFlag(ets_dst, "env_inject_eio", "1"));

  // The error will result in all of the tablets on the affected disk failing.
  ASSERT_EVENTUALLY([&] {
    int64_t failed_on_ts = 0;
    ASSERT_OK(itest::GetInt64Metric(ets_dst->bound_http_hostport(),
        &METRIC_ENTITY_server, nullptr, &METRIC_tablets_num_failed, "value", &failed_on_ts));
    LOG(INFO) << Substitute("Waiting for tablets to fail: $0 / $1", failed_on_ts, kNumTablets);
    ASSERT_EQ(failed_on_ts, kNumTablets);
  });
}

} // namespace kudu
