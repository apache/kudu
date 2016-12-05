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

#include <boost/optional.hpp>

#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/macros.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"

using kudu::itest::StartTabletCopy;
using kudu::itest::TServerDetails;
using kudu::itest::WaitUntilTabletRunning;
using kudu::tablet::TabletDataState;
using kudu::tserver::ListTabletsResponsePB;
using std::string;
using std::vector;

namespace kudu {

// Integration test for StartTabletCopy() and related functionality.
class TabletCopyClientSessionITest : public ExternalMiniClusterITestBase {
 protected:
  // Bring up two tablet servers. Load tablet(s) onto TS 0, while TS 1 is left
  // blank. Prevent the master from tombstoning evicted replicas.
  void PrepareClusterForTabletCopy(const vector<string>& extra_tserver_flags = {},
                                   vector<string> extra_master_flags = {},
                                   int num_tablets = kDefaultNumTablets);

  static const int kDefaultNumTablets;
  const MonoDelta kDefaultTimeout = MonoDelta::FromSeconds(30);
};

const int TabletCopyClientSessionITest::kDefaultNumTablets = 1;

void TabletCopyClientSessionITest::PrepareClusterForTabletCopy(
    const vector<string>& extra_tserver_flags,
    vector<string> extra_master_flags,
    int num_tablets) {
  const int kNumTabletServers = 2;
  // We don't want the master to interfere when we manually make copies of
  // tablets onto servers it doesn't know about.
  extra_master_flags.push_back("--master_tombstone_evicted_tablet_replicas=false");
  NO_FATALS(StartCluster(extra_tserver_flags, extra_master_flags, kNumTabletServers));
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
  const MonoDelta kTimeout = MonoDelta::FromSeconds(60);
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
            FAIL() << "Timed out waiting for bootstrap: " << s.ToString();
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
          SleepFor(MonoDelta::FromMilliseconds(10));
          continue;
        }
        LOG(INFO) << "Waiting until tablet running...";
        EXPECT_OK(WaitUntilTabletRunning(ts1, tablet_id, deadline - MonoTime::Now()));
      }, &t));
      threads.push_back(t);
    }

    // Restart the source tablet server (TS 0).
    ASSERT_OK(cluster_->tablet_server(0)->Restart());

    // Wait for one of the threads to succeed with its tablet copy and for the
    // tablet to be running on TS 1.
    EXPECT_OK(WaitUntilTabletRunning(ts1, tablet_id, kTimeout));

    for (auto& t : threads) {
      t->Join();
    }

    NO_FATALS(cluster_->AssertNoCrashes());

    // There is a delay between the tablet marking itself as RUNNING and the
    // tablet copy operation completing, so we retry tablet deletion attempts.
    // We want a clean deletion so only one thread wins, then we have to
    // restart TS 1 to clear knowledge of the replica from memory.
    NO_FATALS(DeleteTabletWithRetries(ts1, tablet_id,
                                      TabletDataState::TABLET_DATA_DELETED,
                                      boost::none, kTimeout));
    cluster_->tablet_server(1)->Shutdown();
    ASSERT_OK(cluster_->tablet_server(1)->Restart());
  }
}

} // namespace kudu
