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
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/cluster_itest_util.h"
#include "kudu/integration-tests/cluster_verifier.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/pb_util.h"
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
using kudu::cluster::ExternalDaemon;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::pb_util::SecureShortDebugString;
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

  // TODO(awong): more plumbing is needed to allow the server to be restarted
  // bound to a different address with the webserver, so just disable it.
  void SetUpCluster(vector<string> extra_master_flags = { "--webserver_enabled=false" },
                    vector<string> extra_tserver_flags = { "--webserver_enabled=false" }) {
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

  // Get the new DNS override string when restarting the last node of the given
  // daemon type with the given reserved address.
  string GetNewOverridesFlag(ExternalMiniCluster::DaemonType node_type,
                             const Sockaddr& new_addr) {
    int master_end_idx = cluster_->num_masters();
    int tserver_end_idx = cluster_->num_tablet_servers();
    bool is_master = node_type == ExternalMiniCluster::DaemonType::MASTER;
    if (is_master) {
      --master_end_idx;
    } else {
      --tserver_end_idx;
    }
    vector<string> new_overrides;
    new_overrides.reserve(cluster_->num_masters() + cluster_->num_tablet_servers());
    for (int i = 0; i < master_end_idx; i++) {
      new_overrides.emplace_back(Substitute("$0.$1=$2", kMasterHostPrefix, i,
                                            cluster_->master(i)->bound_rpc_addr().ToString()));
    }
    for (int i = 0; i < tserver_end_idx; i++) {
      new_overrides.emplace_back(
          Substitute("$0.$1=$2", kTServerHostPrefix, i,
                     cluster_->tablet_server(i)->bound_rpc_addr().ToString()));
    }
    new_overrides.emplace_back(
        Substitute("$0.$1=$2", is_master ? kMasterHostPrefix : kTServerHostPrefix,
                   is_master ? master_end_idx : tserver_end_idx,
                   new_addr.ToString()));
    return JoinStrings(new_overrides, ",");
  }

  // Adds the appropriate flags for the given daemon to be restarted bound to
  // the given address.
  void SetUpDaemonForNewAddr(const Sockaddr& new_addr, const string& new_overrides_str,
                             ExternalDaemon* daemon) {
    HostPort new_ip_hp(new_addr.host(), new_addr.port());
    daemon->SetRpcBindAddress(new_ip_hp);
    daemon->mutable_flags()->emplace_back("--rpc_reuseport=true");
    daemon->mutable_flags()->emplace_back(
        Substitute("--dns_addr_resolution_override=$0", new_overrides_str));
  }

  // Sets the flags on all nodes in the cluster, except for the last node of
  // the given 'node_type', which is expected to have been restarted with the
  // appropriate flags.
  void SetFlagsOnRemainingCluster(ExternalMiniCluster::DaemonType node_type,
                                  const string& new_overrides_str) {
    int master_end_idx = cluster_->num_masters();
    int tserver_end_idx = cluster_->num_tablet_servers();
    if (node_type == ExternalMiniCluster::DaemonType::MASTER) {
      --master_end_idx;
    } else {
      --tserver_end_idx;
    }
    for (int i = 0; i < master_end_idx; i++) {
      ASSERT_OK(cluster_->SetFlag(
          cluster_->master(i), "dns_addr_resolution_override", new_overrides_str));
    }
    for (int i = 0; i < tserver_end_idx; i++) {
      ASSERT_OK(cluster_->SetFlag(
          cluster_->tablet_server(i), "dns_addr_resolution_override", new_overrides_str));
    }
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

// These tests depend on restarted servers being assigned a new IP address. On
// MacOS, tservers are all assigned the same address, so don't run them there.
#if defined(__linux__)

// Regression test for KUDU-1620, wherein consensus proxies don't eventually
// succeed when the address changes but the host/ports stays the same.
TEST_F(DnsAliasITest, Kudu1620) {
  TestWorkload w(cluster_.get());
  w.set_num_replicas(3);
  w.set_num_write_threads(1);
  w.Setup();
  w.Start();
  while (w.rows_inserted() < 10) {
    SleepFor(MonoDelta::FromMilliseconds(10));
  }
  w.StopAndJoin();

  // Shut down a tablet server and start one up at a different IP.
  auto* tserver = cluster_->tablet_server(cluster_->num_tablet_servers() - 1);
  tserver->Shutdown();
  unique_ptr<Socket> reserved_socket;
  ASSERT_OK(cluster_->ReserveDaemonSocket(cluster::ExternalMiniCluster::DaemonType::TSERVER, 3,
                                          kDefaultBindMode, &reserved_socket,
                                          tserver->bound_rpc_hostport().port()));
  Sockaddr new_addr;
  ASSERT_OK(reserved_socket->GetSocketAddress(&new_addr));

  // Once we start having the other servers communicate with the new tserver,
  // ksck should return healthy.
  auto new_overrides_str = GetNewOverridesFlag(ExternalMiniCluster::DaemonType::TSERVER, new_addr);
  SetUpDaemonForNewAddr(new_addr, new_overrides_str, tserver);
  ASSERT_OK(tserver->Restart());

  // Running ksck should fail because the existing servers are still trying to
  // communicate with the old port.
  ClusterVerifier v(cluster_.get());
  Status s = v.RunKsck();
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();

  SetFlagsOnRemainingCluster(ExternalMiniCluster::DaemonType::TSERVER, new_overrides_str);

  // Our test thread still thinks the old alias is still valid, so our ksck
  // should fail.
  s = v.RunKsck();
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();

  // Once we set the DNS aliases in the test thread, ksck should succeed.
  FLAGS_dns_addr_resolution_override = new_overrides_str;
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(v.RunKsck());
  });
}

// Master-side regression test for KUDU-1620. Masters instantiate consensus
// proxies to get the UUIDs of its peers. With KUDU-1620 resolved, the proxy
// used should be able to re-resolve and retry upon failure, rather than
// retrying at the same address.
TEST_F(DnsAliasITest, TestMasterReresolveOnStartup) {
  const int last_master_idx = cluster_->num_masters() - 1;
  auto* master = cluster_->master(last_master_idx);

  // Shut down and prepare the node that we're going to give a new address.
  master->Shutdown();
  unique_ptr<Socket> reserved_socket;
  ASSERT_OK(cluster_->ReserveDaemonSocket(cluster::ExternalMiniCluster::DaemonType::MASTER, 3,
                                          kDefaultBindMode, &reserved_socket,
                                          master->bound_rpc_hostport().port()));
  Sockaddr new_addr;
  ASSERT_OK(reserved_socket->GetSocketAddress(&new_addr));
  auto new_overrides_str = GetNewOverridesFlag(ExternalMiniCluster::DaemonType::MASTER, new_addr);
  SetUpDaemonForNewAddr(new_addr, new_overrides_str, master);

  // Shut down the other masters so we can test what happens when they come
  // back up.
  for (int i = 0; i < last_master_idx; i++) {
    cluster_->master(i)->Shutdown();
  }
  for (int i = 0; i < last_master_idx; i++) {
    ASSERT_OK(cluster_->master(i)->Restart());
  }
  // Since the rest of the cluster doesn't know about the address, ksck will
  // fail.
  ClusterVerifier v(cluster_.get());
  Status s = v.RunKsck();
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();

  // Even upon setting the DNS overrides on the rest of the nodes, since the
  // master hasn't started, we should still see an error.
  SetFlagsOnRemainingCluster(ExternalMiniCluster::DaemonType::MASTER, new_overrides_str);
  s = v.RunKsck();
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  FLAGS_dns_addr_resolution_override = new_overrides_str;
  s = v.RunKsck();
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();

  // Upon restarting the node, the other masters should be able to resolve and
  // connect to it.
  ASSERT_OK(master->Restart());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(v.RunKsck());
  });
}

// Regression test for KUDU-1885, wherein tserver proxies on the masters don't
// eventually succeed when the tserver's address changes.
TEST_F(DnsAliasITest, Kudu1885) {
  // First, wait for all tablet servers to report to the masters.
  ASSERT_EVENTUALLY([&] {
    vector<KuduTabletServer*> tservers;
    ElementDeleter deleter(&tservers);
    ASSERT_OK(client_->ListTabletServers(&tservers));
    ASSERT_EQ(cluster_->num_tablet_servers(), tservers.size());
  });
  auto* tserver = cluster_->tablet_server(cluster_->num_tablet_servers() - 1);
  // Shut down a tablet server so we can start it up with a different address.
  tserver->Shutdown();
  unique_ptr<Socket> reserved_socket;
  ASSERT_OK(cluster_->ReserveDaemonSocket(cluster::ExternalMiniCluster::DaemonType::TSERVER, 3,
                                          kDefaultBindMode, &reserved_socket,
                                          tserver->bound_rpc_hostport().port()));
  Sockaddr new_addr;
  ASSERT_OK(reserved_socket->GetSocketAddress(&new_addr));
  auto new_overrides_str = GetNewOverridesFlag(ExternalMiniCluster::DaemonType::TSERVER, new_addr);
  SetUpDaemonForNewAddr(new_addr, new_overrides_str, tserver);

  // Create several tables. Based on Kudu's tablet placement algorithm, some
  // should be assigned to the tserver with a new address. This will start some
  // tasks on the master to send requests to tablet servers (some of which will
  // fail because of the down server).
  // NOTE: master's will wait up to --tserver_unresponsive_timeout_ms before
  // stopping replica placement on the down server. By default, this is 60
  // seconds, so we can proceed expecting placement on the down tserver.
  for (int i = 0; i < 10; i++) {
    TestWorkload w(cluster_.get());
    w.set_table_name(Substitute("default.table_$0", i));
    w.set_num_replicas(1);
    // Some tablet creations will initially fail until we restart the down
    // server, so have our client not wait for creation to finish.
    w.set_wait_for_create(false);
    w.Setup();
  }
  ASSERT_OK(tserver->Restart());

  // Allow the rest of the cluster to start seeing the re-addressed server.
  SetFlagsOnRemainingCluster(ExternalMiniCluster::DaemonType::TSERVER, new_overrides_str);
  FLAGS_dns_addr_resolution_override = new_overrides_str;
  ClusterVerifier v(cluster_.get());
  ASSERT_EVENTUALLY([&] {
    ASSERT_OK(v.RunKsck());
  });

  // Ensure there's no funny business with the tserver coming up at a new
  // address -- we should have the same number of tablet servers.
  vector<KuduTabletServer*> tservers;
  ElementDeleter deleter(&tservers);
  ASSERT_OK(client_->ListTabletServers(&tservers));
  ASSERT_EQ(cluster_->num_tablet_servers(), tservers.size());

  // Some tablets should be assigned to the tablet we re-addressed, and the
  // create tablet requests from the masters should have been routed as
  // appropriate.
  auto tserver_proxy = cluster_->tserver_proxy(cluster_->num_tablet_servers() - 1);
  tserver::ListTabletsRequestPB req;
  req.set_need_schema_info(false);
  tserver::ListTabletsResponsePB resp;
  rpc::RpcController controller;
  ASSERT_OK(tserver_proxy->ListTablets(req, &resp, &controller));
  ASSERT_FALSE(resp.has_error()) << SecureShortDebugString(resp.error());
  ASSERT_GT(resp.status_and_schema_size(), 0);
}

#endif

} // namespace itest
} // namespace kudu
