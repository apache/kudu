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
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/integration-tests/external_mini_cluster.h"
#include "kudu/integration-tests/mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_entity(server);
METRIC_DECLARE_gauge_uint64(threads_running);

namespace kudu {

using std::string;
using strings::Substitute;

enum KerberosMode {
  WITHOUT_KERBEROS, WITH_KERBEROS
};

class ExternalMiniClusterTest : public KuduTest,
                                public testing::WithParamInterface<KerberosMode> {};

INSTANTIATE_TEST_CASE_P(KerberosOnAndOff,
                        ExternalMiniClusterTest,
                        ::testing::Values(WITHOUT_KERBEROS, WITH_KERBEROS));

void SmokeTestKerberizedCluster(ExternalMiniClusterOptions opts) {
  ASSERT_TRUE(opts.enable_kerberos);
  int num_tservers = opts.num_tablet_servers;

  ExternalMiniCluster cluster(std::move(opts));
  ASSERT_OK(cluster.Start());

  // Sleep long enough to ensure that the tserver's ticket would have expired
  // if not for the renewal thread doing its thing.
  SleepFor(MonoDelta::FromSeconds(16));

  // Re-kinit for the client, since the client's ticket would have expired as well
  // since the renewal thread doesn't run for the test client.
  ASSERT_OK(cluster.kdc()->Kinit("test-admin"));

  // Restart the master, and make sure the tserver is still able to reconnect and
  // authenticate.
  cluster.master(0)->Shutdown();
  ASSERT_OK(cluster.master(0)->Restart());
  // Ensure that all of the tablet servers can register with the masters.
  ASSERT_OK(cluster.WaitForTabletServerCount(num_tservers, MonoDelta::FromSeconds(30)));
  cluster.Shutdown();
}

TEST_F(ExternalMiniClusterTest, TestKerberosReacquire) {
  if (!AllowSlowTests()) return;

  ExternalMiniClusterOptions opts;
  opts.enable_kerberos = true;
  // Set the kerberos ticket lifetime as 15 seconds to force ticket reacquisition every 15 seconds.
  // Note that we do not renew tickets but always acquire a new one.
  opts.mini_kdc_options.ticket_lifetime = "15s";
  opts.num_tablet_servers = 1;

  SmokeTestKerberizedCluster(std::move(opts));
}

TEST_P(ExternalMiniClusterTest, TestBasicOperation) {
  ExternalMiniClusterOptions opts;
  opts.enable_kerberos = GetParam() == WITH_KERBEROS;

  // Hard-coded RPC ports for the masters. This is safe, as this unit test
  // runs under a resource lock (see CMakeLists.txt in this directory).
  // TODO we should have a generic method to obtain n free ports.
  opts.master_rpc_ports = { 11010, 11011, 11012 };
  opts.num_masters = opts.master_rpc_ports.size();
  opts.num_tablet_servers = 3;

  ExternalMiniCluster cluster(opts);
  ASSERT_OK(cluster.Start());

  // Verify each of the masters.
  for (int i = 0; i < opts.num_masters; i++) {
    SCOPED_TRACE(i);
    ExternalMaster* master = CHECK_NOTNULL(cluster.master(i));
    HostPort master_rpc = master->bound_rpc_hostport();
    string expected_prefix = Substitute("$0:", cluster.GetBindIpForMaster(i));
    if (cluster.bind_mode() == MiniCluster::UNIQUE_LOOPBACK) {
      EXPECT_NE(expected_prefix, "127.0.0.1:") << "Should bind to unique per-server hosts";
    }
    EXPECT_TRUE(HasPrefixString(master_rpc.ToString(), expected_prefix)) << master_rpc.ToString();

    HostPort master_http = master->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(master_http.ToString(), expected_prefix)) << master_http.ToString();

    // Retrieve a thread metric, which should always be present on any master.
    int64_t value;
    ASSERT_OK(master->GetInt64Metric(&METRIC_ENTITY_server,
                                     "kudu.master",
                                     &METRIC_threads_running,
                                     "value",
                                     &value));
    EXPECT_GT(value, 0);
  }

  // Verify each of the tablet servers.
  for (int i = 0; i < opts.num_tablet_servers; i++) {
    SCOPED_TRACE(i);
    ExternalTabletServer* ts = CHECK_NOTNULL(cluster.tablet_server(i));
    HostPort ts_rpc = ts->bound_rpc_hostport();
    string expected_prefix = Substitute("$0:", cluster.GetBindIpForTabletServer(i));
    if (cluster.bind_mode() == MiniCluster::UNIQUE_LOOPBACK) {
      EXPECT_NE(expected_prefix, "127.0.0.1:") << "Should bind to unique per-server hosts";
    }
    EXPECT_TRUE(HasPrefixString(ts_rpc.ToString(), expected_prefix)) << ts_rpc.ToString();

    HostPort ts_http = ts->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(ts_http.ToString(), expected_prefix)) << ts_http.ToString();

    // Retrieve a thread metric, which should always be present on any TS.
    int64_t value;
    ASSERT_OK(ts->GetInt64Metric(&METRIC_ENTITY_server,
                                 "kudu.tabletserver",
                                 &METRIC_threads_running,
                                 "value",
                                 &value));
    EXPECT_GT(value, 0);
  }

  // Ensure that all of the tablet servers can register with the masters.
  ASSERT_OK(cluster.WaitForTabletServerCount(opts.num_tablet_servers, MonoDelta::FromSeconds(30)));

  // Restart a master and a tablet server. Make sure they come back up with the same ports.
  ExternalMaster* master = cluster.master(0);
  HostPort master_rpc = master->bound_rpc_hostport();
  HostPort master_http = master->bound_http_hostport();

  master->Shutdown();
  ASSERT_OK(master->Restart());

  ASSERT_EQ(master_rpc.ToString(), master->bound_rpc_hostport().ToString());
  ASSERT_EQ(master_http.ToString(), master->bound_http_hostport().ToString());

  ExternalTabletServer* ts = cluster.tablet_server(0);

  HostPort ts_rpc = ts->bound_rpc_hostport();
  HostPort ts_http = ts->bound_http_hostport();

  ts->Shutdown();
  ASSERT_OK(ts->Restart());

  ASSERT_EQ(ts_rpc.ToString(), ts->bound_rpc_hostport().ToString());
  ASSERT_EQ(ts_http.ToString(), ts->bound_http_hostport().ToString());

  // Verify that, in a Kerberized cluster, if we drop our Kerberos environment,
  // we can't make RPCs to a server.
  if (opts.enable_kerberos) {
    ASSERT_OK(cluster.kdc()->Kdestroy());
    Status s = cluster.SetFlag(ts, "foo", "bar");
    // The error differs depending on the version of Kerberos, so we match
    // either message.
    ASSERT_STR_MATCHES(s.ToString(), "Not authorized.*"
                       "(Credentials cache file.*not found|"
                        "No Kerberos credentials|"
                        ".*No such file or directory)");
  }

  // Test that if we inject a fault into a tablet server's boot process
  // ExternalTabletServer::Restart() still returns OK, even if the tablet server crashed.
  ts->Shutdown();
  ts->mutable_flags()->push_back("--fault_before_start=1.0");
  ASSERT_OK(ts->Restart());
  ASSERT_FALSE(ts->IsProcessAlive());
  // Since the process should have already crashed, waiting for an injected crash with no
  // timeout should still return OK.
  ASSERT_OK(ts->WaitForInjectedCrash(MonoDelta::FromSeconds(0)));
  ts->mutable_flags()->pop_back();
  ts->Shutdown();
  ASSERT_OK(ts->Restart());
  ASSERT_TRUE(ts->IsProcessAlive());

  cluster.Shutdown();
}

} // namespace kudu
