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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(client_use_unix_domain_sockets);
DECLARE_string(dns_addr_resolution_override);
DECLARE_string(host_for_tests);

METRIC_DECLARE_counter(rpc_connections_accepted_unix_domain_socket);
METRIC_DECLARE_entity(server);

using kudu::client::KuduClient;
using kudu::client::KuduTabletServer;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {
namespace itest {

namespace {
constexpr const char* kTServerHostPrefix = "tserver.host";
constexpr const char* kMasterHostPrefix = "master.host";
} // anonymous namespace

class DnsAliasITest : public KuduTest {
 public:
  void SetUp() override {
    SetUpCluster();
  }

  void SetUpCluster(vector<string> extra_master_flags = {},
                    vector<string> extra_tserver_flags = {}) {
    ExternalMiniClusterOptions opts;
    opts.num_masters = 3;
    opts.num_tablet_servers = 3;
    opts.extra_master_flags = std::move(extra_master_flags);
    opts.extra_tserver_flags = std::move(extra_tserver_flags);
    opts.master_alias_prefix = kMasterHostPrefix;
    opts.tserver_alias_prefix = kTServerHostPrefix;
    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());

    FLAGS_dns_addr_resolution_override = cluster_->dns_overrides();
    ASSERT_OK(cluster_->CreateClient(nullptr, &client_));
  }

  void TearDown() override {
    NO_FATALS(cluster_->AssertNoCrashes());
  }
 protected:
  unique_ptr<ExternalMiniCluster> cluster_;
  client::sp::shared_ptr<KuduClient> client_;
};

TEST_F(DnsAliasITest, TestBasic) {
  // Based on the mini-cluster setup, the client should report the aliases.
  auto master_addrs_str = client_->GetMasterAddresses();
  vector<string> master_addrs = Split(master_addrs_str, ",");
  ASSERT_EQ(cluster_->num_masters(), master_addrs.size()) << master_addrs_str;
  for (const auto& master_addr : master_addrs) {
    ASSERT_STR_CONTAINS(master_addr, kMasterHostPrefix);
    // Try resolving a numeric IP. This should fail, since the returned values
    // should be aliased.
    Sockaddr addr;
    Status s = addr.ParseString(master_addr, 0);
    ASSERT_FALSE(s.ok());
  }

  vector<KuduTabletServer*> tservers;
  ElementDeleter deleter(&tservers);
  ASSERT_OK(client_->ListTabletServers(&tservers));
  for (const auto* tserver : tservers) {
    ASSERT_STR_CONTAINS(tserver->hostname(), kTServerHostPrefix);
    // Try resolving a numeric IP. This should fail, since the returned values
    // should be aliased.
    Sockaddr addr;
    Status s = addr.ParseString(tserver->hostname(), 0);
    ASSERT_FALSE(s.ok());
  }

  // Running a test worload should succeed. Have the workload perform both
  // scans and writes to exercise the aliasing codepaths of each.
  TestWorkload w(cluster_.get());
  w.set_num_write_threads(1);
  w.set_num_read_threads(3);
  w.set_num_replicas(3);
  w.Setup();
  w.Start();
  while (w.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  w.StopAndJoin();
}

class DnsAliasWithUnixSocketsITest : public DnsAliasITest {
 public:
  void SetUp() override {
    // Configure --host_for_tests in this process so the test client will think
    // it's local to a tserver.
    FLAGS_host_for_tests = Substitute("$0.$1", kTServerHostPrefix, kTServerIdxWithLocalClient);
    FLAGS_client_use_unix_domain_sockets = true;
    SetUpCluster({ "--rpc_listen_on_unix_domain_socket=true" },
                 { "--rpc_listen_on_unix_domain_socket=true" });
  }
 protected:
  const int kTServerIdxWithLocalClient = 0;
};

TEST_F(DnsAliasWithUnixSocketsITest, TestBasic) {
  TestWorkload w(cluster_.get());
  w.set_num_write_threads(1);
  w.set_num_read_threads(3);
  w.set_num_replicas(3);
  w.Setup();
  w.Start();
  while (w.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  w.StopAndJoin();
  for (int i = 0; i < cluster_->num_tablet_servers(); i++) {
    int64_t unix_connections = 0;

    // Curl doesn't know about our DNS aliasing, so resolve the address and
    // fetch the metric from the proper address.
    vector<Sockaddr> addrs;
    ASSERT_OK(cluster_->tablet_server(i)->bound_http_hostport().ResolveAddresses(&addrs));
    ASSERT_EQ(1, addrs.size());
    const auto& addr = addrs[0];
    ASSERT_OK(GetInt64Metric(HostPort(addr.host(), addr.port()),
                             &METRIC_ENTITY_server, nullptr,
                             &METRIC_rpc_connections_accepted_unix_domain_socket,
                             "value", &unix_connections));
    if (i == kTServerIdxWithLocalClient) {
      ASSERT_LT(0, unix_connections);
    } else {
      ASSERT_EQ(0, unix_connections);
    }
  }
}

} // namespace itest
} // namespace kudu
