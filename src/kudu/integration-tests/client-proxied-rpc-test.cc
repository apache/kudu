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

#include <csignal>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
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
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class ClientProxiedRpcTest : public KuduTest {
 public:
  void SetUp() override {

    KuduTest::SetUp();

    ASSERT_OK(GetRandomPort(kIpAddr, &m_proxy_advertised_port_));
    m_proxy_advertised_addr_ = HostPort(kIpAddr, m_proxy_advertised_port_);
    ASSERT_OK(GetRandomPort(kIpAddr, &m_proxied_port_));
    m_proxied_addr_ = HostPort(kIpAddr, m_proxied_port_);

    ASSERT_OK(GetRandomPort(kIpAddr, &t_proxy_advertised_port_));
    t_proxy_advertised_addr_ = HostPort(kIpAddr, t_proxy_advertised_port_);
    ASSERT_OK(GetRandomPort(kIpAddr, &t_proxied_port_));
    t_proxied_addr_ = HostPort(kIpAddr, t_proxied_port_);

    ExternalMiniClusterOptions opts;
    opts.extra_master_flags = {
      Substitute("--rpc_proxy_advertised_addresses=$0",
          m_proxy_advertised_addr_.ToString()),
      Substitute("--rpc_proxied_addresses=$0",
          m_proxied_addr_.ToString()),
    };
    opts.extra_tserver_flags = {
      Substitute("--rpc_proxy_advertised_addresses=$0",
          t_proxy_advertised_addr_.ToString()),
      Substitute("--rpc_proxied_addresses=$0",
          t_proxied_addr_.ToString()),
    };

    cluster_.reset(new ExternalMiniCluster(std::move(opts)));
    ASSERT_OK(cluster_->Start());
  }

  void TearDown() override {
    if (cluster_) {
      cluster_->Shutdown();
    }
    KuduTest::TearDown();
  }

 protected:
  static constexpr const char* const kIpAddr = "127.0.0.1";

  uint16_t m_proxied_port_;
  HostPort m_proxied_addr_;
  uint16_t m_proxy_advertised_port_;
  HostPort m_proxy_advertised_addr_;

  uint16_t t_proxied_port_;
  HostPort t_proxied_addr_;
  uint16_t t_proxy_advertised_port_;
  HostPort t_proxy_advertised_addr_;

  unique_ptr<ExternalMiniCluster> cluster_;
};

// Verify basic functionality when RPC connections to Kudu master and tablet
// server are forwarded via a simple TCP proxy.
TEST_F(ClientProxiedRpcTest, Basic) {
  SKIP_IF_SLOW_NOT_ALLOWED();

  string nc;
  {
    auto s = FindExecutable("nc", {"/bin", "/usr/bin", "/usr/local/bin"}, &nc);
    if (s.IsNotFound()) {
      LOG(WARNING) << "test is skipped: could not find netcat utility (nc)";
      GTEST_SKIP();
    }
    ASSERT_OK(s);
  }
  ASSERT_FALSE(nc.empty());

  const auto kTimeout = MonoDelta::FromSeconds(5);
  constexpr const char* const kTableName = "proxied_rpc_test";
  constexpr const char* const kProxyCmdPattern =
      "trap \"kill %1\" EXIT; $0 -knv -l $1 $2 <$4 | $0 -nv $1 $3 >$4";
  const auto schema = KuduSchema::FromSchema(GetSimpleTestSchema());
  TestWorkload w(cluster_.get());
  w.set_schema(schema);
  w.set_table_name(kTableName);
  w.set_num_replicas(1);
  w.Setup();

  unique_ptr<Fifo> m_fifo;
  ASSERT_OK(env_->NewFifo(JoinPathSegments(test_dir_, "m.fifo"), &m_fifo));

  unique_ptr<Fifo> t_fifo;
  ASSERT_OK(env_->NewFifo(JoinPathSegments(test_dir_, "t.fifo"), &t_fifo));

  // Run TCP proxy for Kudu master connections.
  const auto m_proxy_cmd_str = Substitute(
      kProxyCmdPattern, nc,
      kIpAddr, m_proxy_advertised_port_, m_proxied_port_, m_fifo->filename());
  Subprocess m_proxy({"/bin/bash", "-c", m_proxy_cmd_str});
  ASSERT_OK(m_proxy.Start());
  auto m_proxy_cleanup = MakeScopedCleanup([&] {
    m_proxy.KillAndWait(SIGTERM);
  });

  // Run TCP proxy for Kudu tablet server connections.
  const auto t_proxy_cmd_str = Substitute(
      kProxyCmdPattern, nc,
      kIpAddr, t_proxy_advertised_port_, t_proxied_port_, t_fifo->filename());
  Subprocess t_proxy({"/bin/bash", "-c", t_proxy_cmd_str});
  ASSERT_OK(t_proxy.Start());
  auto t_proxy_cleanup = MakeScopedCleanup([&] {
    t_proxy.KillAndWait(SIGTERM);
  });

  // Wait for the TCP proxies to start up.
  ASSERT_OK(WaitForTcpBindAtPort({ kIpAddr }, m_proxy_advertised_port_, kTimeout));
  ASSERT_OK(WaitForTcpBindAtPort({ kIpAddr }, t_proxy_advertised_port_, kTimeout));

  // Build a client to send requests via the proxied RPC endpoint.
  client::sp::shared_ptr<client::KuduClient> client;
  {
    client::KuduClientBuilder b;
    b.add_master_server_addr(m_proxy_advertised_addr_.ToString());
    b.default_admin_operation_timeout(kTimeout);
    b.default_rpc_timeout(kTimeout);
    ASSERT_OK(b.Build(&client));
  }

  // Make sure the client receives proxy advertised addresses since the request
  // came to the proxied RPC address.
  const auto& master_addresses = client->GetMasterAddresses();
  ASSERT_EQ(m_proxy_advertised_addr_.ToString(), master_addresses);
  // Just a sanity check: multiple RPC endpoints shouldn't be treated as
  // a presence of multiple masters in the cluster.
  ASSERT_FALSE(client->IsMultiMaster());

  // Check that client sees RPC addresses advertised by TCP proxy for the
  // tablet server.
  vector<KuduTabletServer*> tss;
  ElementDeleter deleter(&tss);
  ASSERT_OK(client->ListTabletServers(&tss));
  ASSERT_EQ(1, tss.size());
  ASSERT_EQ(kIpAddr, tss[0]->hostname());
  ASSERT_EQ(t_proxy_advertised_port_, tss[0]->port());

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
    ASSERT_OK(scanner.SetFaultTolerant());
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

  // Make sure the client indeed works through the RPC address advertised by
  // proxy: stop the proxy and check if client could write any data to the table.
  t_proxy_cleanup.cancel();
  ASSERT_OK(t_proxy.KillAndWait(SIGTERM));
  {
    unique_ptr<KuduInsert> insert(table->NewInsert());
    auto* row = insert->mutable_row();
    GenerateDataForRow(schema, 100, &rng, row);
    const auto s = session->Apply(insert.release());
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "failed to flush data");
  }

  // Try reading the data now: this expected to fail since the client works only
  // through the advertised addressed.
  {
    KuduScanner scanner(table.get());
    ASSERT_OK(scanner.SetFaultTolerant());
    ASSERT_OK(scanner.SetTimeoutMillis(kTimeout.ToMilliseconds()));
    const auto s = scanner.Open();
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    ASSERT_STR_MATCHES(s.ToString(),
        "(timed out after deadline expired|exceeded configured scan timeout)");
  }

  // Meanwhile, DDL operations should be still possible: connections to master
  // are still being proxied as needed, and master and tablet servers
  // communicate via standard, non-proxied RPC endpoints.
  {
    unique_ptr<KuduTableAlterer> alt(client->NewTableAlterer(kTableName));
    alt->AlterColumn("string_val")->RenameTo("str_val");
    ASSERT_OK(alt->Alter());
  }

  // Make sure the client communicates with master via the advertised addresses:
  // once the TCP proxy is shut down, client should not be able to reach master
  // to perform a DDL operation.
  m_proxy_cleanup.cancel();
  ASSERT_OK(m_proxy.KillAndWait(SIGTERM));
  {
    unique_ptr<KuduTableAlterer> alt(client->NewTableAlterer(kTableName));
    alt->AlterColumn("str_val")->RenameTo("string_val");
    const auto s = alt->Alter();
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "AlterTable passed its deadline");
    ASSERT_STR_CONTAINS(s.ToString(), "Client connection negotiation failed");
  }
}

} // namespace kudu
