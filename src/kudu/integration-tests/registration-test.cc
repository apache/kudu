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

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master-test-util.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/mini_master.h"
#include "kudu/master/sys_catalog.h"
#include "kudu/master/ts_descriptor.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/security/tls_context.h"
#include "kudu/security/token_verifier.h"
#include "kudu/server/webserver_options.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/mini_tablet_server.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/tablet_server_options.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/version_info.h"

DECLARE_int32(heartbeat_interval_ms);
METRIC_DECLARE_counter(rows_inserted);
METRIC_DECLARE_counter(rows_updated);

using std::shared_ptr;
using std::string;
using std::vector;

namespace kudu {

using cluster::InternalMiniCluster;
using master::CatalogManager;
using master::CreateTableRequestPB;
using master::CreateTableResponsePB;
using master::GetTableLocationsResponsePB;
using master::IsCreateTableDoneRequestPB;
using master::IsCreateTableDoneResponsePB;
using master::MiniMaster;
using master::TSDescriptor;
using master::TabletLocationsPB;
using kudu::pb_util::SecureShortDebugString;
using tserver::MiniTabletServer;

void CreateTableForTesting(MiniMaster* mini_master,
                           const string& table_name,
                           const Schema& schema,
                           string* tablet_id) {
  {
    CreateTableRequestPB req;
    CreateTableResponsePB resp;

    req.set_name(table_name);
    req.set_num_replicas(1);
    ASSERT_OK(SchemaToPB(schema, req.mutable_schema()));
    CatalogManager* catalog = mini_master->master()->catalog_manager();
    CatalogManager::ScopedLeaderSharedLock l(catalog);
    ASSERT_OK(l.first_failed_status());
    ASSERT_OK(catalog->CreateTable(&req, &resp, nullptr));
  }

  int wait_time = 1000;
  bool is_table_created = false;
  for (int i = 0; i < 80; ++i) {
    IsCreateTableDoneRequestPB req;
    IsCreateTableDoneResponsePB resp;

    req.mutable_table()->set_table_name(table_name);
    CatalogManager* catalog = mini_master->master()->catalog_manager();
    {
      CatalogManager::ScopedLeaderSharedLock l(catalog);
      ASSERT_OK(l.first_failed_status());
      ASSERT_OK(catalog->IsCreateTableDone(&req, &resp));
    }
    if (resp.done()) {
      is_table_created = true;
      break;
    }

    VLOG(1) << "Waiting for table '" << table_name << "'to be created";

    SleepFor(MonoDelta::FromMicroseconds(wait_time));
    wait_time = std::min(wait_time * 5 / 4, 1000000);
  }
  ASSERT_TRUE(is_table_created);

  GetTableLocationsResponsePB resp;
  ASSERT_OK(WaitForRunningTabletCount(mini_master, table_name, 1, &resp));
  *tablet_id = resp.tablet_locations(0).tablet_id();
  LOG(INFO) << "Got tablet " << *tablet_id << " for table " << table_name;
}


// Tests for the Tablet Server registering with the Master,
// and the master maintaining the tablet descriptor.
class RegistrationTest : public KuduTest {
 public:
  RegistrationTest()
      : schema_({ ColumnSchema("c1", UINT32) }, 1) {
  }

  void SetUp() override {
    // Make heartbeats faster to speed test runtime.
    FLAGS_heartbeat_interval_ms = 10;

    KuduTest::SetUp();

    cluster_.reset(new InternalMiniCluster(env_, {}));
    ASSERT_OK(cluster_->Start());
  }

  void TearDown() override {
    cluster_->Shutdown();
  }

  void CheckTabletServersPage(string* contents = nullptr) {
    EasyCurl c;
    faststring buf;
    string addr = cluster_->mini_master()->bound_http_addr().ToString();
    ASSERT_OK(c.FetchURL(strings::Substitute("http://$0/tablet-servers", addr),
                                &buf));
    string buf_str = buf.ToString();

    // Should include the TS UUID
    string expected_uuid =
      cluster_->mini_tablet_server(0)->server()->instance_pb().permanent_uuid();
    ASSERT_STR_CONTAINS(buf_str, expected_uuid);

    // Should check that the TS software version is included on the page.
    // tserver version should be the same as returned by GetVersionInfo()
    ASSERT_STR_CONTAINS(buf_str, VersionInfo::GetVersionInfo());
    if (contents != nullptr) {
      *contents = std::move(buf_str);
    }
  }

  Status WaitForReplicaCount(const string& tablet_id,
                             int expected_count,
                             TabletLocationsPB* locations = nullptr) {
    while (true) {
      master::CatalogManager* catalog =
          cluster_->mini_master()->master()->catalog_manager();
      Status s;
      TabletLocationsPB loc;
      do {
        master::CatalogManager::ScopedLeaderSharedLock l(catalog);
        const Status& ls = l.first_failed_status();
        if (ls.IsServiceUnavailable() || ls.IsIllegalState()) {
          // ServiceUnavailable means catalog manager is not yet ready
          // to serve requests; IllegalState means the catalog is not the
          // leader yet. That's an indication of a transient state where it's
          // it's necessary to try again later.
          break;  // exiting out of the 'do {...} while (false)' scope
        }
        RETURN_NOT_OK(ls);
        s = catalog->GetTabletLocations(tablet_id, master::VOTER_REPLICA, &loc);
      } while (false);
      if (s.ok() && loc.replicas_size() == expected_count) {
        if (locations) {
          *locations = std::move(loc);
        }
        return Status::OK();
      }
      SleepFor(MonoDelta::FromMilliseconds(10));
    }
  }

 protected:
  gscoped_ptr<InternalMiniCluster> cluster_;
  Schema schema_;
};

TEST_F(RegistrationTest, TestTSRegisters) {
  // Wait for the TS to register.
  vector<shared_ptr<TSDescriptor> > descs;
  ASSERT_OK(cluster_->WaitForTabletServerCount(
      1, InternalMiniCluster::MatchMode::MATCH_TSERVERS, &descs));
  ASSERT_EQ(1, descs.size());

  // Verify that the registration is sane.
  ServerRegistrationPB reg;
  descs[0]->GetRegistration(&reg);
  {
    SCOPED_TRACE(SecureShortDebugString(reg));
    ASSERT_EQ(string::npos, SecureShortDebugString(reg).find("0.0.0.0"))
        << "Should not include wildcards in registration";
  }

  ASSERT_NO_FATAL_FAILURE(CheckTabletServersPage());

  // Restart the master, so it loses the descriptor, and ensure that the
  // heartbeater thread handles re-registering.
  cluster_->mini_master()->Shutdown();
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
                        VersionInfo::GetVersionInfo());
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
  auto GetCatalogMetric = [&](CounterPrototype& prototype) {
    auto metrics = cluster_->mini_master()->master()->catalog_manager()->
        sys_catalog()->tablet_replica()->tablet()->GetMetricEntity();
    return prototype.Instantiate(metrics)->value();
  };
  const int startup_rows_inserted = GetCatalogMetric(METRIC_rows_inserted);

  // Add a table, make sure it reports itself.
  string tablet_id_1;
  NO_FATALS(CreateTableForTesting(
      cluster_->mini_master(), "tablet-reports-1", schema_, &tablet_id_1));
  TabletLocationsPB locs_1;
  ASSERT_OK(WaitForReplicaCount(tablet_id_1, 1, &locs_1));
  ASSERT_EQ(1, locs_1.replicas_size());

  // Check that we inserted the right number of rows for the new single-tablet table
  // (one for the table, one for the tablet).
  const int post_create_rows_inserted = GetCatalogMetric(METRIC_rows_inserted);
  EXPECT_EQ(2, post_create_rows_inserted - startup_rows_inserted)
      << "Should have inserted one row each for the table and tablet";

  // Add another table, make sure it is reported via incremental.
  string tablet_id_2;
  NO_FATALS(CreateTableForTesting(
      cluster_->mini_master(), "tablet-reports-2", schema_, &tablet_id_2));
  ASSERT_OK(WaitForReplicaCount(tablet_id_2, 1));

  // Shut down the whole system, bring it back up, and make sure the tablets
  // are reported.
  cluster_->Shutdown();
  ASSERT_OK(cluster_->Start());

  // After restart, check that the tablet reports produced the expected number of
  // writes to the catalog table:
  // - No inserts, because there are no new tablets.
  // - Two updates, since both replicas increase their term on restart.
  //
  // It can take some time for the TS to re-heartbeat. To avoid flakiness, here
  // it's easier to wait for the target non-zero metric value instead of
  // hard-coding an extra delay.
  AssertEventually([&]() {
    ASSERT_EQ(0, GetCatalogMetric(METRIC_rows_inserted));
    ASSERT_EQ(2, GetCatalogMetric(METRIC_rows_updated));
    });

  // If we restart just the master, it should not write any data to the catalog, since the
  // tablets themselves are not changing term, etc.
  cluster_->mini_master()->Shutdown();
  ASSERT_OK(cluster_->mini_master()->Restart());

  // Sleep for a second to make sure the TS has plenty of time to re-heartbeat.
  // The metrics are updated after processing TS heartbeats, and we want to
  // capture updated metric readings.
  SleepFor(MonoDelta::FromSeconds(1));

  EXPECT_EQ(0, GetCatalogMetric(METRIC_rows_inserted));
  EXPECT_EQ(0, GetCatalogMetric(METRIC_rows_updated));
}

// Check that after the tablet server registers, it gets a signed cert
// from the master.
TEST_F(RegistrationTest, TestTSGetsSignedX509Certificate) {
  MiniTabletServer* ts = cluster_->mini_tablet_server(0);
  ASSERT_EVENTUALLY([&](){
      ASSERT_TRUE(ts->server()->tls_context().has_signed_cert());
    });
}

// Check that after the tablet server registers, it gets the list of valid
// public token signing keys.
TEST_F(RegistrationTest, TestTSGetsTskList) {
  MiniTabletServer* ts = cluster_->mini_tablet_server(0);
  ASSERT_EVENTUALLY([&](){
      ASSERT_FALSE(ts->server()->token_verifier().ExportKeys().empty());
    });
}

// Test that, if the tserver has HTTPS enabled, the master links to it
// via https:// URLs and not http://.
TEST_F(RegistrationTest, TestExposeHttpsURLs) {
  MiniTabletServer* ts = cluster_->mini_tablet_server(0);
  string password;
  WebserverOptions* opts = &ts->options()->webserver_opts;
  ASSERT_OK(security::CreateTestSSLCertWithEncryptedKey(GetTestDataDirectory(),
                                                        &opts->certificate_file,
                                                        &opts->private_key_file,
                                                        &password));
  opts->private_key_password_cmd = strings::Substitute("echo $0", password);
  ts->Shutdown();
  ASSERT_OK(ts->Start());

  // The URL displayed on the page uses a hostname. Rather than
  // dealing with figuring out what the hostname should be, just
  // use a more permissive regex which doesn't check the host.
  string expected_url_regex = strings::Substitute(
      "https://[a-zA-Z0-9.-]+:$0/", opts->port);

  // Need "eventually" here because the tserver may take a few seconds
  // to re-register while starting up.
  ASSERT_EVENTUALLY([&](){
      string contents;
      NO_FATALS(CheckTabletServersPage(&contents));
      ASSERT_STR_MATCHES(contents, expected_url_regex);
    });
}

} // namespace kudu
