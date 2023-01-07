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
#include <array>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/client/scan_batch.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h" // IWYU pragma: keep
#include "kudu/client/write_op.h"
#include "kudu/common/wire_protocol-test-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/integration-tests/data_gen_util.h"
#include "kudu/integration-tests/test_workload.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/env.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::Fifo;
using kudu::client::KuduClient;
using kudu::client::KuduInsert;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTabletServer;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::array;
using std::function;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Split;
using strings::Substitute;

namespace kudu {

// Class template for test scenarios running against external mini-cluster
// with M masters and T tablet servers.
template<size_t M, size_t T>
class ClientProxiedRpcTest : public KuduTest {
 public:
  void SetUp() override {
    SKIP_IF_SLOW_NOT_ALLOWED();

    auto s = FindExecutable("nc", {"/bin", "/usr/bin", "/usr/local/bin"}, &nc_);
    if (s.IsNotFound()) {
      LOG(WARNING) << "test is skipped: could not find netcat utility (nc)";
      GTEST_SKIP();
    }
    ASSERT_OK(s);

    KuduTest::SetUp();

    ExternalMiniClusterOptions opts;
    opts.num_masters = M;
    opts.num_tablet_servers = T;

    vector<string> master_addrs;
    for (size_t i = 0; i < M; ++i) {
      auto& a_port = m_proxy_advertised_ports_[i];
      ASSERT_OK(GetRandomPort(kIpAddr, &a_port));
      auto& a_addr = m_proxy_advertised_addrs_[i];
      a_addr = HostPort(kIpAddr, a_port);

      auto& p_port = m_proxied_ports_[i];
      ASSERT_OK(GetRandomPort(kIpAddr, &p_port));
      auto& p_addr = m_proxied_addrs_[i];
      p_addr = HostPort(kIpAddr, p_port);

      vector<string> flags = {
        Substitute("--rpc_proxy_advertised_addresses=$0", a_addr.ToString()),
        Substitute("--rpc_proxied_addresses=$0", p_addr.ToString()),
      };
      opts.m_custom_flags.emplace_back(std::move(flags));

      master_addrs.emplace_back(a_addr.ToString());
    }

    if (M > 1) {
      const auto flag = Substitute("--master_rpc_proxy_advertised_addresses=$0",
                                   JoinStrings(master_addrs, ","));
      for (size_t i = 0; i < M; ++i) {
        opts.m_custom_flags[i].push_back(flag);
      }
    }

    for (size_t i = 0; i < T; ++i) {
      auto& a_port = t_proxy_advertised_ports_[i];
      ASSERT_OK(GetRandomPort(kIpAddr, &a_port));
      auto& a_addr = t_proxy_advertised_addrs_[i];
      a_addr = HostPort(kIpAddr, a_port);

      auto& p_port = t_proxied_ports_[i];
      ASSERT_OK(GetRandomPort(kIpAddr, &p_port));
      auto& p_addr = t_proxied_addrs_[i];
      p_addr = HostPort(kIpAddr, p_port);

      vector<string> flags = {
        Substitute("--rpc_proxy_advertised_addresses=$0", a_addr.ToString()),
        Substitute("--rpc_proxied_addresses=$0", p_addr.ToString()),
      };
      opts.t_custom_flags.emplace_back(std::move(flags));
    }

    cluster_.reset(new ExternalMiniCluster(std::move(opts)));

    ASSERT_OK(cluster_->Start());
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

  // Verify basic functionality when RPC connections to Kudu masters and tablet
  // servers are forwarded via a TCP proxy.
  void Run() {
    ASSERT_FALSE(nc_.empty());

    const auto kTimeout = MonoDelta::FromSeconds(5);
    const char* const kTableName = CURRENT_TEST_NAME();
    const auto schema = KuduSchema::FromSchema(GetSimpleTestSchema());
    TestWorkload w(cluster_.get());
    w.set_schema(schema);
    w.set_table_name(kTableName);
    w.set_num_replicas(1);
    w.Setup();

    vector<unique_ptr<Fifo>> m_fifos(M);
    for (auto i = 0; i < M; ++i) {
      const auto fname = Substitute("m.fifo.$0", i);
      ASSERT_OK(env_->NewFifo(JoinPathSegments(test_dir_, fname), &m_fifos[i]));
    }

    vector<unique_ptr<Fifo>> t_fifos(T);
    for (auto i = 0; i < T; ++i) {
      const auto fname = Substitute("t.fifo.$0", i);
      ASSERT_OK(env_->NewFifo(JoinPathSegments(test_dir_, fname), &t_fifos[i]));
    }

    // Run TCP proxies for Kudu masters' connections.
    vector<unique_ptr<Subprocess>> m_proxies;
    m_proxies.reserve(M);
    vector<ScopedCleanup<function<void(void)>>> m_proxy_cleanups;
    m_proxy_cleanups.reserve(M);
    for (auto i = 0; i < M; ++i) {
      const auto proxy_cmd_str = Substitute(
          kProxyCmdPattern,
          nc_,
          kIpAddr,
          m_proxy_advertised_ports_[i],
          m_proxied_ports_[i],
          m_fifos[i]->filename());
      m_proxies.emplace_back(new Subprocess({"/bin/bash", "-c", proxy_cmd_str}));

      auto* proxy = m_proxies.back().get();
      function<void(void)> cleanup = [proxy] {
        if (proxy->IsStarted()) {
          WARN_NOT_OK(proxy->KillAndWait(SIGTERM),
                      Substitute("PID $0: could not stop process", proxy->pid()));
        }
      };

      m_proxy_cleanups.emplace_back(std::move(cleanup));
    }
    for (auto& p : m_proxies) {
      ASSERT_OK(p->Start());
    }

    // Run TCP proxies for Kudu tablet servers' connections.
    vector<unique_ptr<Subprocess>> t_proxies;
    t_proxies.reserve(T);
    vector<ScopedCleanup<function<void(void)>>> t_proxy_cleanups;
    t_proxy_cleanups.reserve(T);
    for (auto i = 0; i < T; ++i) {
      const auto proxy_cmd_str = Substitute(
          kProxyCmdPattern,
          nc_,
          kIpAddr,
          t_proxy_advertised_ports_[i],
          t_proxied_ports_[i],
          t_fifos[i]->filename());
      t_proxies.emplace_back(new Subprocess({"/bin/bash", "-c", proxy_cmd_str}));

      auto* proxy = t_proxies.back().get();
      function<void(void)> cleanup = [proxy] {
        if (proxy->IsStarted()) {
          WARN_NOT_OK(proxy->KillAndWait(SIGTERM),
                      Substitute("PID $0: could not stop process", proxy->pid()));
        }
      };
      t_proxy_cleanups.emplace_back(std::move(cleanup));
    }
    for (auto& p : t_proxies) {
      ASSERT_OK(p->Start());
    }

    // Wait for the TCP proxies to start up.
    for (auto port : m_proxy_advertised_ports_) {
      ASSERT_OK(WaitForTcpBindAtPort({ kIpAddr }, port, kTimeout));
    }
    for (auto port : t_proxy_advertised_ports_) {
      ASSERT_OK(WaitForTcpBindAtPort({ kIpAddr }, port, kTimeout));
    }

    // Build a client to send requests via RPC endpoints advertised by proxy.
    client::sp::shared_ptr<client::KuduClient> client;
    {
      client::KuduClientBuilder b;
      for (auto i = 0; i < M; ++i) {
        b.add_master_server_addr(m_proxy_advertised_addrs_[i].ToString());
      }
      b.default_admin_operation_timeout(kTimeout);
      b.default_rpc_timeout(kTimeout);
      ASSERT_OK(b.Build(&client));
    }

    // Make sure the client receives the addresses advertised by proxy since
    // the request came through the proxied RPC address.
    const vector<string> master_addresses(Split(client->GetMasterAddresses(), ","));
    for (const auto& hp : m_proxy_advertised_addrs_) {
      ASSERT_TRUE(std::any_of(master_addresses.begin(), master_addresses.end(),
                              [&hp](const string& e) { return e == hp.ToString(); }));
    }
    ASSERT_EQ(m_proxy_advertised_addrs_.size(), master_addresses.size());
    if (M > 1) {
      ASSERT_TRUE(client->IsMultiMaster());
    } else {
      ASSERT_FALSE(client->IsMultiMaster());
    }

    // Check that client sees RPC addresses advertised by TCP proxy for the
    // tablet server.
    vector<KuduTabletServer*> tss;
    ElementDeleter deleter(&tss);
    ASSERT_OK(client->ListTabletServers(&tss));
    ASSERT_EQ(T, tss.size());
    for (auto i = 0; i < T; ++i) {
      ASSERT_EQ(kIpAddr, tss[i]->hostname());
    }
    {
      set<uint16_t> ports;
      for (auto i = 0; i < T; ++i) {
        const auto port = tss[i]->port();
        ports.emplace(port);
        ASSERT_TRUE(std::any_of(t_proxy_advertised_ports_.begin(),
                                t_proxy_advertised_ports_.end(),
                                [&port](uint16_t e) { return e == port; }));
      }
      // Make sure all ports are different.
      ASSERT_EQ(T, ports.size());
    }

    client::sp::shared_ptr<KuduTable> table;
    ASSERT_OK(client->OpenTable(kTableName, &table));

    // Create a session and explicitly set the flush mode to AUTO_FLUSH_SYNC
    // to send every operation when calling Apply().
    client::sp::shared_ptr<KuduSession> session(client->NewSession());
    ASSERT_OK(session->SetFlushMode(KuduSession::AUTO_FLUSH_SYNC));
    ThreadSafeRandom rng(SeedRandom());
    for (auto i = 0; i < 10; ++i) {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      auto* row = insert->mutable_row();
      GenerateDataForRow(schema, i, &rng, row);
      ASSERT_OK(session->Apply(insert.release()));
    }
    // Call Flush() just in case, but it's a no-op effectively since the chosen
    // session flush mode.
    ASSERT_OK(session->Flush());

    // Read the data back.
    {
      KuduScanner scanner(table.get());
      ASSERT_OK(scanner.SetTimeoutMillis(kTimeout.ToMilliseconds()));
      ASSERT_OK(scanner.Open());
      ASSERT_TRUE(scanner.HasMoreRows());
      KuduScanBatch batch;

      int32_t idx = 0;
      while (scanner.HasMoreRows()) {
        ASSERT_OK(scanner.NextBatch(&batch));
        for (const auto& row : batch) {
          int32_t value;
          ASSERT_OK(row.GetInt32(0, &value));
          ASSERT_EQ(idx++, value);
        }
      }
      ASSERT_EQ(10, idx);
    }

    // Make sure the client indeed works through the RPC addresses advertised by
    // proxy: stop the proxy and check if client can succeed in writing any data
    // to the table.
    for (auto i = 0; i < T; ++i) {
      t_proxy_cleanups[i].cancel();
      ASSERT_OK(t_proxies[i]->KillAndWait(SIGTERM));
    }
    {
      unique_ptr<KuduInsert> insert(table->NewInsert());
      auto* row = insert->mutable_row();
      GenerateDataForRow(schema, 100, &rng, row);
      const auto s = session->Apply(insert.release());
      ASSERT_TRUE(s.IsIOError()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
    }

    // Try reading the data now: this expected to fail since the client works
    // only through the advertised addresses.
    {
      KuduScanner scanner(table.get());
      ASSERT_OK(scanner.SetTimeoutMillis(kTimeout.ToMilliseconds()));
      const auto s = scanner.Open();
      ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
      ASSERT_STR_MATCHES(s.ToString(),
          "(timed out after deadline expired|exceeded configured scan timeout)");
    }

    // Meanwhile, DDL operations should be still possible: connections to
    // masters are still being proxied as needed, and masters and tablet servers
    // communicate via standard, non-proxied RPC endpoints.
    {
      unique_ptr<KuduTableAlterer> alt(client->NewTableAlterer(kTableName));
      alt->AlterColumn("string_val")->RenameTo("str_val");
      ASSERT_OK(alt->Alter());
    }

    // Make sure the client communicates with masters via the advertised
    // addresses: once the corresponding TCP proxy is shut down, the client
    // should not be able to reach the master to perform a DDL operation.
    for (auto i = 0; i < M; ++i) {
      m_proxy_cleanups[i].cancel();
      ASSERT_OK(m_proxies[i]->KillAndWait(SIGTERM));
    }
    {
      unique_ptr<KuduTableAlterer> alt(client->NewTableAlterer(kTableName));
      alt->AlterColumn("str_val")->RenameTo("string_val");
      const auto s = alt->Alter();
      ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "AlterTable passed its deadline");
      ASSERT_STR_CONTAINS(s.ToString(), "Client connection negotiation failed");
    }
  }

 protected:
  static constexpr const char* const kIpAddr = "127.0.0.1";
  static constexpr const char* const kProxyCmdPattern =
      "trap \"kill %1\" EXIT; $0 -knv -l $1 $2 <$4 | $0 -nv $1 $3 >$4";

  // Full path to the nc/netcat utility (if present).
  string nc_;

  array<uint16_t, M> m_proxied_ports_;
  array<HostPort, M> m_proxied_addrs_;
  array<uint16_t, M> m_proxy_advertised_ports_;
  array<HostPort, M> m_proxy_advertised_addrs_;

  array<uint16_t, T> t_proxied_ports_;
  array<HostPort, T> t_proxied_addrs_;
  array<uint16_t, T> t_proxy_advertised_ports_;
  array<HostPort, T> t_proxy_advertised_addrs_;

  unique_ptr<ExternalMiniCluster> cluster_;
};

typedef ClientProxiedRpcTest<1, 1> ClientProxiedRpc1M1Test;
TEST_F(ClientProxiedRpc1M1Test, Basic) {
  NO_FATALS(Run());
}

typedef ClientProxiedRpcTest<1, 3> ClientProxiedRpc1M3Test;
TEST_F(ClientProxiedRpc1M3Test, Basic) {
  NO_FATALS(Run());
}

typedef ClientProxiedRpcTest<3, 1> ClientProxiedRpc3M1Test;
TEST_F(ClientProxiedRpc3M1Test, Basic) {
  NO_FATALS(Run());
}

typedef ClientProxiedRpcTest<3, 3> ClientProxiedRpc3M3Test;
TEST_F(ClientProxiedRpc3M3Test, Basic) {
  NO_FATALS(Run());
}

} // namespace kudu
