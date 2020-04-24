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
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/ts_itest-base.h"
#include "kudu/master/master.pb.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(num_replicas);
DECLARE_int32(num_tablet_servers);

METRIC_DECLARE_counter(location_mapping_cache_hits);
METRIC_DECLARE_counter(location_mapping_cache_queries);
METRIC_DECLARE_counter(scans_started);

METRIC_DECLARE_entity(tablet);

using kudu::client::KuduClient;
using kudu::client::KuduScanner;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::LocationInfo;
using kudu::itest::TServerDetails;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {

class ClientLocationAssignmentITest :
    public tserver::TabletServerIntegrationTestBase {
};

TEST_F(ClientLocationAssignmentITest, Basic) {
  // Generate the location mapping and build the cluster. There are three
  // locations. One location has two spots, which may be occupied by two
  // tablet servers or a tablet server and the client.
  LocationInfo info;
  int client_loc_idx = random_.Uniform(FLAGS_num_tablet_servers);
  for (int i = 0; i < FLAGS_num_tablet_servers; i++) {
    EmplaceOrDie(&info, Substitute("/L$0", i), i == client_loc_idx ? 2 : 1);
  }
  FLAGS_num_replicas = FLAGS_num_tablet_servers;
  NO_FATALS(BuildAndStart({}, {}, std::move(info)));

  // Find the tablet server that is colocated with the client, if there is one.
  const auto timeout = MonoDelta::FromSeconds(30);
  vector<master::ListTabletServersResponsePB_Entry> tservers;
  ASSERT_OK(itest::ListTabletServers(cluster_->master_proxy(),
                                     timeout,
                                     &tservers));
  const string client_location = client_->location();
  ASSERT_FALSE(client_location.empty());
  string client_colocated_tserver_uuid;
  for (const auto& tserver : tservers) {
    const auto& ts_location = tserver.location();
    ASSERT_FALSE(ts_location.empty());
    if (ts_location == client_location) {
      client_colocated_tserver_uuid = tserver.instance_id().permanent_uuid();
    }
  }

  // Wait for each replica to have received an op. This should be the NO_OP
  // asserting the leader's leadership in the current term. If we don't wait
  // for a safetime-advancing op to be received, scans might be rejected for
  // correctness reasons (see KUDU-2463). This will blacklist the replica in
  // the client and possibly cause a scan outside of the location on retry.
  ASSERT_OK(WaitForServersToAgree(timeout, tablet_servers_, tablet_id_, 1));

  // Scan the table in CLOSEST_REPLICA mode.
  KuduScanner scanner(table_.get());
  ASSERT_OK(scanner.SetSelection(KuduClient::ReplicaSelection::CLOSEST_REPLICA));
  vector<string> rows;
  ASSERT_OK(ScanToStrings(&scanner, &rows));
  ASSERT_TRUE(rows.empty());

  // The number of scans started is the number of tablets, 1. If on Linux,
  // check that CLOSEST_REPLICA is working as expected.
  int64_t total_scans_started = 0;
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    const auto* tserver = cluster_->tablet_server(i);
    int64_t scans_started;
    ASSERT_OK(itest::GetInt64Metric(tserver->bound_http_hostport(),
                                    &METRIC_ENTITY_tablet,
                                    nullptr,
                                    &METRIC_scans_started,
                                    "value",
                                    &scans_started));
    total_scans_started += scans_started;
    // If there is a tablet server in the same location as the client, it will
    // be the only tablet server scanned. Otherwise, some random tablet server
    // will be scanned.
    if (!client_colocated_tserver_uuid.empty()) {
      if (tserver->uuid() == client_colocated_tserver_uuid) {
        ASSERT_EQ(1, scans_started);
      } else {
        ASSERT_EQ(0, scans_started);
      }
    }
  }
  ASSERT_EQ(1, total_scans_started);
}

class TsLocationAssignmentITest :
    public KuduTest,
    public ::testing::WithParamInterface<std::tuple<int, int>> {
 public:
  TsLocationAssignmentITest()
      : rng_(SeedRandom()) {
    const auto& param = GetParam();
    opts_.num_masters = std::get<0>(param);
    opts_.num_tablet_servers = std::get<1>(param);
  }

  virtual ~TsLocationAssignmentITest() = default;

 protected:
  void StartCluster() {
    // Generate random location mapping.
    LocationInfo info;
    int num_mappings_left = opts_.num_tablet_servers;
    int loc_idx = 0;
    while (true) {
      auto location = Substitute("/L$0", loc_idx);
      if (num_mappings_left <= 1) {
        EmplaceOrDie(&info, std::move(location), 1);
        break;
      }
      const int num = static_cast<int>(rng_.Uniform(num_mappings_left));
      if (num == 0) {
        continue;
      }
      EmplaceOrDie(&info, std::move(location), num);

      num_mappings_left -= num;
      ++loc_idx;
    }

    opts_.location_info = std::move(info);
    cluster_.reset(new ExternalMiniCluster(opts_));
    ASSERT_OK(cluster_->Start());
  }

  void CheckLocationInfo() {
    unordered_map<string, itest::TServerDetails*> ts_map;
    ASSERT_OK(itest::CreateTabletServerMap(cluster_->master_proxy(0),
                                           cluster_->messenger(),
                                           &ts_map));
    ValueDeleter deleter(&ts_map);

    LocationInfo location_info;
    for (const auto& desc : ts_map) {
      ++LookupOrEmplace(&location_info, desc.second->location, 0);
    }
    ASSERT_EQ(opts_.location_info, location_info);
  }

  ThreadSafeRandom rng_;
  ExternalMiniClusterOptions opts_;
  unique_ptr<cluster::ExternalMiniCluster> cluster_;
};

// Verify that the location assignment works as expected for tablet servers
// run as part of ExternalMiniCluster. Also verify that every tablet server
// is assigned the same location after restart once the location assignment
// script is kept the same between restarts.
TEST_P(TsLocationAssignmentITest, Basic) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  NO_FATALS(StartCluster());
  NO_FATALS(CheckLocationInfo());
  NO_FATALS(cluster_->AssertNoCrashes());

  cluster_->Shutdown();

  ASSERT_OK(cluster_->Restart());
  NO_FATALS(CheckLocationInfo());
  NO_FATALS(cluster_->AssertNoCrashes());
}

// Verify the behavior of the location mapping cache upon tablet server
// registrations.
TEST_P(TsLocationAssignmentITest, LocationMappingCacheOnTabletServerRestart) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  NO_FATALS(StartCluster());
  NO_FATALS(CheckLocationInfo());
  NO_FATALS(cluster_->AssertNoCrashes());

  const auto num_tablet_servers = cluster_->num_tablet_servers();

  int64_t hits_before;
  ASSERT_OK(itest::GetInt64Metric(
      cluster_->leader_master()->bound_http_hostport(),
      &METRIC_ENTITY_server,
      nullptr,
      &METRIC_location_mapping_cache_hits,
      "value",
      &hits_before));
  ASSERT_EQ(0, hits_before);

  int64_t queries_before;
  ASSERT_OK(itest::GetInt64Metric(
      cluster_->leader_master()->bound_http_hostport(),
      &METRIC_ENTITY_server,
      nullptr,
      &METRIC_location_mapping_cache_queries,
      "value",
      &queries_before));
  ASSERT_EQ(num_tablet_servers, queries_before);

  for (auto idx = 0; idx < num_tablet_servers; ++idx) {
    auto* ts = cluster_->tablet_server(idx);
    ts->Shutdown();
    ASSERT_OK(ts->Restart());
  }

  NO_FATALS(CheckLocationInfo());
  NO_FATALS(cluster_->AssertNoCrashes());

  ASSERT_EVENTUALLY([&]() {
    int64_t hits_after;
    ASSERT_OK(itest::GetInt64Metric(
        cluster_->leader_master()->bound_http_hostport(),
        &METRIC_ENTITY_server,
        nullptr,
        &METRIC_location_mapping_cache_hits,
        "value",
        &hits_after));
    ASSERT_EQ(hits_before + num_tablet_servers, hits_after);
  });

  ASSERT_EVENTUALLY([&]() {
    int64_t queries_after;
    ASSERT_OK(itest::GetInt64Metric(
        cluster_->leader_master()->bound_http_hostport(),
        &METRIC_ENTITY_server,
        nullptr,
        &METRIC_location_mapping_cache_queries,
        "value",
        &queries_after));
    ASSERT_EQ(queries_before + num_tablet_servers, queries_after);
  });
}

INSTANTIATE_TEST_CASE_P(, TsLocationAssignmentITest,
    ::testing::Combine(::testing::Values(1, 3),
                       ::testing::Values(1, 8, 16, 32)));

} // namespace kudu
