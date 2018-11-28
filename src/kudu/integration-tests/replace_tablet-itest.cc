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
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

using master::MasterServiceProxy;
using master::ReplaceTabletRequestPB;
using master::ReplaceTabletResponsePB;
using tools::LeaderMasterProxy;
using tserver::ListTabletsResponsePB_StatusAndSchemaPB;

class ReplaceTabletITest : public ExternalMiniClusterITestBase {
 public:
  ReplaceTabletITest() :
    rand_(SeedRandom()) {
  }

  Status RandomTabletId(string* tablet_id) {
    // 3 tablets servers and 3 replicas per tablet, so it doesn't matter which TS we choose.
    auto* ts = ts_map_.begin()->second;
    vector<ListTabletsResponsePB_StatusAndSchemaPB> tablets;
    RETURN_NOT_OK(ListTablets(ts, MonoDelta::FromSeconds(30), &tablets));
    if (tablets.empty()) {
      return Status::NotFound("no tablets");
    }
    *tablet_id = tablets[rand_.Uniform(tablets.size())].tablet_status().tablet_id();
    return Status::OK();
  }

  Status ReplaceRandomTablet(LeaderMasterProxy* proxy) {
    string tablet_id;
    RETURN_NOT_OK(RandomTabletId(&tablet_id));
    LOG(INFO) << "Replacing tablet " << tablet_id;
    ReplaceTabletRequestPB req;
    ReplaceTabletResponsePB resp;
    req.set_tablet_id(tablet_id);
    return proxy->SyncRpc<ReplaceTabletRequestPB, ReplaceTabletResponsePB>(
        req, &resp, "ReplaceTablet", &MasterServiceProxy::ReplaceTabletAsync);
  }

 private:
  Random rand_;
};

// TODO(wdberkeley): Enable this test once KUDU-2376 is fixed.
// TODO(wdberkeley): Set the PROCESSOR configuration properly for this test once
//                   it is enabled. See 1c1d3ba.
TEST_F(ReplaceTabletITest, DISABLED_ReplaceTabletsWhileWriting) {
  constexpr int kNumTabletServers = 3;
  constexpr int kNumTablets = 4;
  constexpr int kNumRows = 10000;
  const int kNumReplaceTablets = AllowSlowTests() ? 5 : 1;
  NO_FATALS(StartCluster({}, {}, kNumTabletServers));

  LeaderMasterProxy proxy;
  vector<string> master_addrs;
  for (const auto& hp : cluster_->master_rpc_addrs()) {
    master_addrs.emplace_back(hp.ToString());
  }
  ASSERT_OK(proxy.Init(master_addrs, MonoDelta::FromSeconds(10)));

  TestWorkload workload(cluster_.get());
  workload.set_num_replicas(kNumTabletServers);
  workload.set_num_tablets(kNumTablets);
  workload.Setup();

  // Insert some rows before replacing tablets so the client's cache is warm.
  workload.Start();
  while (workload.rows_inserted() < kNumRows) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }

  // Replace tablets while inserts continue.
  for (int i = 0; i < kNumReplaceTablets; i++) {
    ASSERT_OK(ReplaceRandomTablet(&proxy));
    SleepFor(MonoDelta::FromMilliseconds(100));
  }

  // Make sure we insert a few more rows that hopefully interleave with replaces.
  while (workload.rows_inserted() < 2 * kNumRows) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  workload.StopAndJoin();

  // We lost some indeterminate subset of the rows due to replace tablet ops,
  // but the cluster state should ultimately still be consistent.
  NO_FATALS(ClusterVerifier(cluster_.get()).CheckCluster());
}

} // namespace kudu
