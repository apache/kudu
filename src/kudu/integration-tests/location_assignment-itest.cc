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
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::LocationInfo;
using kudu::itest::TServerDetails;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using strings::Substitute;

namespace kudu {

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

INSTANTIATE_TEST_CASE_P(, TsLocationAssignmentITest,
    ::testing::Combine(::testing::Values(1, 3),
                       ::testing::Values(1, 8, 16, 32)));

} // namespace kudu
