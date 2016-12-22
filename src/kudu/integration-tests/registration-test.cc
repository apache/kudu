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

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master-test-util.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_util.h"
#include "kudu/util/version_info.h"

DECLARE_int32(heartbeat_interval_ms);

namespace kudu {

using std::vector;
using std::shared_ptr;
using master::MiniMaster;
using master::TSDescriptor;
using master::TabletLocationsPB;
using tserver::MiniTabletServer;

// Tests for the Tablet Server registering with the Master,
// and the master maintaining the tablet descriptor.
class RegistrationTest : public KuduTest {
 public:
  RegistrationTest()
    : schema_({ ColumnSchema("c1", UINT32) }, 1) {
  }

  virtual void SetUp() OVERRIDE {
    // Make heartbeats faster to speed test runtime.
    FLAGS_heartbeat_interval_ms = 10;

    KuduTest::SetUp();

    cluster_.reset(new MiniCluster(env_, MiniClusterOptions()));
    ASSERT_OK(cluster_->Start());
  }

  virtual void TearDown() OVERRIDE {
    cluster_->Shutdown();
  }

  void CheckTabletServersPage() {
    EasyCurl c;
    faststring buf;
    string addr = cluster_->mini_master()->bound_http_addr().ToString();
    ASSERT_OK(c.FetchURL(strings::Substitute("http://$0/tablet-servers", addr),
                                &buf));

    // Should include the TS UUID
    string expected_uuid =
      cluster_->mini_tablet_server(0)->server()->instance_pb().permanent_uuid();
    ASSERT_STR_CONTAINS(buf.ToString(), expected_uuid);

    // Should check that the TS software version is included on the page.
    // tserver version should be the same as returned by GetShortVersionString()
    ASSERT_STR_CONTAINS(buf.ToString(), VersionInfo::GetShortVersionString());
  }


  Status WaitForReplicaCount(const string& tablet_id, int expected_count,
                             TabletLocationsPB* locations) {
    while (true) {
      master::CatalogManager* catalog = cluster_->mini_master()->master()->catalog_manager();
      Status s;
      {
        master::CatalogManager::ScopedLeaderSharedLock l(catalog);
        RETURN_NOT_OK(l.first_failed_status());
        s = catalog->GetTabletLocations(tablet_id, locations);
      }
      if (s.ok() && locations->replicas_size() == expected_count) {
        return Status::OK();
      }

      SleepFor(MonoDelta::FromMilliseconds(1));
    }
  }

 protected:
  gscoped_ptr<MiniCluster> cluster_;
  Schema schema_;
};

TEST_F(RegistrationTest, TestTSRegisters) {
  // Wait for the TS to register.
  vector<shared_ptr<TSDescriptor> > descs;
  ASSERT_OK(cluster_->WaitForTabletServerCount(
      1, MiniCluster::MatchMode::MATCH_TSERVERS, &descs));
  ASSERT_EQ(1, descs.size());

  // Verify that the registration is sane.
  ServerRegistrationPB reg;
  descs[0]->GetRegistration(&reg);
  {
    SCOPED_TRACE(SecureShortDebugString(reg));
    ASSERT_EQ(SecureShortDebugString(reg).find("0.0.0.0"), string::npos)
      << "Should not include wildcards in registration";
  }

  ASSERT_NO_FATAL_FAILURE(CheckTabletServersPage());

  // Restart the master, so it loses the descriptor, and ensure that the
  // hearbeater thread handles re-registering.
  ASSERT_OK(cluster_->mini_master()->Restart());

  ASSERT_OK(cluster_->WaitForTabletServerCount(1));

  // TODO: when the instance ID / sequence number stuff is implemented,
  // restart the TS and ensure that it re-registers with the newer sequence
  // number.
}

TEST_F(RegistrationTest, TestMasterSoftwareVersion) {
  // Verify that the master's software version exists.
  ServerRegistrationPB reg;
  cluster_->mini_master()->master()->GetMasterRegistration(&reg);
  {
    SCOPED_TRACE(SecureShortDebugString(reg));
    ASSERT_TRUE(reg.has_software_version());
    ASSERT_STR_CONTAINS(reg.software_version(),
                        VersionInfo::GetShortVersionString());
  }
}

// Test starting multiple tablet servers and ensuring they both register with the master.
TEST_F(RegistrationTest, TestMultipleTS) {
  ASSERT_OK(cluster_->AddTabletServer());
  ASSERT_OK(cluster_->WaitForTabletServerCount(2));
}

// TODO: this doesn't belong under "RegistrationTest" - rename this file
// to something more appropriate - doesn't seem worth having separate
// whole test suites for registration, tablet reports, etc.
TEST_F(RegistrationTest, TestTabletReports) {
  string tablet_id_1;
  string tablet_id_2;

  MiniTabletServer* ts = cluster_->mini_tablet_server(0);
  string ts_root = cluster_->GetTabletServerFsRoot(0);

  // Add a tablet, make sure it reports itself.
  CreateTabletForTesting(cluster_->mini_master(), "fake-table", schema_, &tablet_id_1);

  TabletLocationsPB locs;
  ASSERT_OK(WaitForReplicaCount(tablet_id_1, 1, &locs));
  ASSERT_EQ(1, locs.replicas_size());
  LOG(INFO) << "Tablet successfully reported on " << locs.replicas(0).ts_info().permanent_uuid();

  // Add another tablet, make sure it is reported via incremental.
  CreateTabletForTesting(cluster_->mini_master(), "fake-table2", schema_, &tablet_id_2);
  ASSERT_OK(WaitForReplicaCount(tablet_id_2, 1, &locs));

  // Shut down the whole system, bring it back up, and make sure the tablets
  // are reported.
  ts->Shutdown();
  ASSERT_OK(cluster_->mini_master()->Restart());
  ASSERT_OK(ts->Start());

  ASSERT_OK(WaitForReplicaCount(tablet_id_1, 1, &locs));
  ASSERT_OK(WaitForReplicaCount(tablet_id_2, 1, &locs));

  // TODO: KUDU-870: once the master supports detecting failed/lost replicas,
  // we should add a test case here which removes or corrupts metadata, restarts
  // the TS, and verifies that the master notices the issue.
}

} // namespace kudu
