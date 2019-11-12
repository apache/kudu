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

#include "kudu/mini-cluster/external_mini_cluster.h"

#include <iosfwd>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h> // IWYU pragma: keep
#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h" // IWYU pragma: keep
#include "kudu/hms/hms_client.h"
#include "kudu/hms/mini_hms.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/thrift/client.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::tuple;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace cluster {

enum class Kerberos {
  DISABLED,
  ENABLED,
};

enum class HiveMetastore {
  DISABLED,
  ENABLED,
};

enum BuiltInNtp {
  DISABLED = 0,
  ENABLED_SINGLE_SERVER = 1,
  ENABLED_MULTIPLE_SERVERS = 5,
};

// Beautifies test output if a test scenario fails.
std::ostream& operator<<(std::ostream& o, Kerberos opt) {
  switch (opt) {
    case Kerberos::ENABLED:
      return o << "Kerberos::ENABLED";
    case Kerberos::DISABLED:
      return o << "Kerberos::DISABLED";
  }
  return o;
}

std::ostream& operator<<(std::ostream& o, HiveMetastore opt) {
  switch (opt) {
    case HiveMetastore::ENABLED:
      return o << "HiveMetastore::ENABLED";
    case HiveMetastore::DISABLED:
      return o << "HiveMetastore::DISABLED";
  }
  return o;
}

std::ostream& operator<<(std::ostream& o, BuiltInNtp opt) {
  switch (opt) {
    case BuiltInNtp::DISABLED:
      return o << "BuiltInNtp::DISABLED";
    case BuiltInNtp::ENABLED_SINGLE_SERVER:
      return o << "BuiltInNtp::ENABLED_SINGLE_SERVER";
    case BuiltInNtp::ENABLED_MULTIPLE_SERVERS:
      return o << "BuiltInNtp::ENABLED_MULTIPLE_SERVERS";
  }
  return o;
}

class ExternalMiniClusterTest :
    public KuduTest,
#if !defined(NO_CHRONY)
    public testing::WithParamInterface<tuple<Kerberos, HiveMetastore, BuiltInNtp>>
#else
    public testing::WithParamInterface<tuple<Kerberos, HiveMetastore>>
#endif
{
};

INSTANTIATE_TEST_CASE_P(,
    ExternalMiniClusterTest,
    ::testing::Combine(
        ::testing::Values(Kerberos::DISABLED, Kerberos::ENABLED),
        ::testing::Values(HiveMetastore::DISABLED, HiveMetastore::ENABLED)
#if !defined(NO_CHRONY)
        ,
        ::testing::Values(BuiltInNtp::DISABLED,
                          BuiltInNtp::ENABLED_SINGLE_SERVER,
                          BuiltInNtp::ENABLED_MULTIPLE_SERVERS,
                          BuiltInNtp::DISABLED)
#endif // #if !defined(NO_CHRONY) ...
                          ));

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

void SmokeExternalMiniCluster(const ExternalMiniClusterOptions& opts,
                              ExternalMiniCluster* cluster,
                              vector<HostPort>* master_rpc_addresses) {
  CHECK(cluster);
  CHECK(master_rpc_addresses);
  master_rpc_addresses->clear();

  ASSERT_OK(cluster->Start());

  // Verify each of the masters.
  for (int i = 0; i < opts.num_masters; i++) {
    SCOPED_TRACE(i);
    ExternalMaster* master = CHECK_NOTNULL(cluster->master(i));
    HostPort master_endpoint = master->bound_rpc_hostport();
    string expected_prefix = Substitute("$0:", cluster->GetBindIpForMaster(i));
    if (cluster->bind_mode() == BindMode::UNIQUE_LOOPBACK) {
      EXPECT_NE(expected_prefix, "127.0.0.1:") << "Should bind to unique per-server hosts";
    }
    EXPECT_TRUE(HasPrefixString(master_endpoint.ToString(), expected_prefix))
        << master_endpoint.ToString();

    HostPort master_http = master->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(master_http.ToString(), expected_prefix)) << master_http.ToString();
    master_rpc_addresses->emplace_back(std::move(master_endpoint));
  }

  // Verify each of the tablet servers.
  for (int i = 0; i < opts.num_tablet_servers; i++) {
    SCOPED_TRACE(i);
    ExternalTabletServer* ts = CHECK_NOTNULL(cluster->tablet_server(i));
    HostPort ts_rpc = ts->bound_rpc_hostport();
    string expected_prefix = Substitute("$0:", cluster->GetBindIpForTabletServer(i));
    if (cluster->bind_mode() == BindMode::UNIQUE_LOOPBACK) {
      EXPECT_NE(expected_prefix, "127.0.0.1:") << "Should bind to unique per-server hosts";
    }
    EXPECT_TRUE(HasPrefixString(ts_rpc.ToString(), expected_prefix)) << ts_rpc.ToString();

    HostPort ts_http = ts->bound_http_hostport();
    EXPECT_TRUE(HasPrefixString(ts_http.ToString(), expected_prefix)) << ts_http.ToString();
  }

  // Ensure that all of the tablet servers can register with the masters.
  ASSERT_OK(cluster->WaitForTabletServerCount(opts.num_tablet_servers, MonoDelta::FromSeconds(30)));

  // Restart a master and a tablet server. Make sure they come back up with the same ports.
  ExternalMaster* master = cluster->master(0);
  HostPort master_rpc = master->bound_rpc_hostport();
  HostPort master_http = master->bound_http_hostport();

  master->Shutdown();
  ASSERT_OK(master->Restart());

  ASSERT_EQ(master_rpc.ToString(), master->bound_rpc_hostport().ToString());
  ASSERT_EQ(master_http.ToString(), master->bound_http_hostport().ToString());

  ExternalTabletServer* ts = cluster->tablet_server(0);

  HostPort ts_rpc = ts->bound_rpc_hostport();
  HostPort ts_http = ts->bound_http_hostport();

  ts->Shutdown();
  ASSERT_OK(ts->Restart());

  ASSERT_EQ(ts_rpc.ToString(), ts->bound_rpc_hostport().ToString());
  ASSERT_EQ(ts_http.ToString(), ts->bound_http_hostport().ToString());

  // Verify that the HMS is reachable.
  if (opts.hms_mode == HmsMode::ENABLE_HIVE_METASTORE) {
    thrift::ClientOptions hms_client_opts;
    hms_client_opts.enable_kerberos = opts.enable_kerberos;
    hms_client_opts.service_principal = "hive";
    hms::HmsClient hms_client(cluster->hms()->address(), hms_client_opts);
    ASSERT_OK(hms_client.Start());
    vector<string> tables;
    ASSERT_OK(hms_client.GetTableNames("default", &tables));
    ASSERT_TRUE(tables.empty()) << "tables: " << tables;
  }

  // Verify that, in a Kerberized cluster, if we drop our Kerberos environment,
  // we can't make RPCs to a server.
  if (opts.enable_kerberos) {
    ASSERT_OK(cluster->kdc()->Kdestroy());
    Status s = cluster->SetFlag(ts, "foo", "bar");
    // The error differs depending on the version of Kerberos, so we match
    // either message.
    ASSERT_STR_CONTAINS(s.ToString(),
                        "server requires authentication, "
                        "but client does not have Kerberos credentials available");
  }

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
}

TEST_F(ExternalMiniClusterTest, TestKerberosReacquire) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ExternalMiniClusterOptions opts;
  opts.enable_kerberos = true;
  // Set the kerberos ticket lifetime as 15 seconds to force ticket reacquisition every 15 seconds.
  // Note that we do not renew tickets but always acquire a new one.
  opts.mini_kdc_options.ticket_lifetime = "15s";
  opts.num_tablet_servers = 1;

  NO_FATALS(SmokeTestKerberizedCluster(std::move(opts)));
}

TEST_P(ExternalMiniClusterTest, TestBasicOperation) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  ExternalMiniClusterOptions opts;
  const auto& param = GetParam();
  opts.enable_kerberos = std::get<0>(param) == Kerberos::ENABLED;
  if (std::get<1>(param) == HiveMetastore::ENABLED) {
    opts.hms_mode = HmsMode::ENABLE_HIVE_METASTORE;
  }
#if !defined(NO_CHRONY)
  opts.num_ntp_servers = std::get<2>(param);
#endif

  opts.num_masters = 3;
  opts.num_tablet_servers = 3;

  unique_ptr<ExternalMiniCluster> cluster(new ExternalMiniCluster(opts));
  vector<HostPort> master_rpc_addresses;
  NO_FATALS(SmokeExternalMiniCluster(opts, cluster.get(), &master_rpc_addresses));

  // Destroy the cluster object, create a new one with the same options,
  // and run the same scenario again at already existing data directory
  // structure.
  // This is to make sure that:
  //   * the cluster's components are shutdown upon the destruction
  //     of the object
  //   * configuration files and other persistent data for cluster components
  //     are either reused or rewritten/recreated in consistent manner
  // The only cluster options to preserve is the masters' RPC addresses from
  // the prior run.
  opts.master_rpc_addresses = master_rpc_addresses;
  cluster.reset(new ExternalMiniCluster(opts));
  NO_FATALS(SmokeExternalMiniCluster(opts, cluster.get(), &master_rpc_addresses));
  ASSERT_EQ(opts.master_rpc_addresses, master_rpc_addresses);

  // Shutdown the cluster explicitly. This is not strictly necessary since
  // the cluster will be shutdown upon the call of ExternalMiniCluster's
  // destructor, but this is done in the context of testing. This is to verify
  // that ExternalMiniCluster object destructor works as expected in the case
  // if the cluster has already been shutdown.
  cluster->Shutdown();
}

} // namespace cluster
} // namespace kudu
