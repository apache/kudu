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
#include <cstdint>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/security/test/mini_kdc.h"
#include "kudu/tablet/key_value_test_schema.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(rpc_reopen_outbound_connections);
DECLARE_bool(rpc_trace_negotiation);

using kudu::client::KuduClient;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

// This test exercises the behavior of the system when external security
// components fail (KDC, etc.). It's necessary to check that the system behaves
// consistently in that case and error messages are self-explaining and
// actionable.
class SecurityComponentsFaultsITest : public KuduTest {
 public:
  SecurityComponentsFaultsITest()
      : krb_lifetime_seconds_(120),
        num_masters_(3),
        num_tservers_(3) {

    // Reopen client-->server connections on every RPC. This is to make sure the
    // servers authenticate the client on every RPC call.
    FLAGS_rpc_reopen_outbound_connections = true;

    // Want to run with Kerberos enabled.
    cluster_opts_.enable_kerberos = true;

    cluster_opts_.num_masters = num_masters_;
    cluster_opts_.master_rpc_ports = { 11080, 11081, 11082 };
    cluster_opts_.num_tablet_servers = num_tservers_;

    // KDC-related options
    cluster_opts_.mini_kdc_options.renew_lifetime =
        Substitute("$0s", krb_lifetime_seconds_);
    cluster_opts_.mini_kdc_options.ticket_lifetime =
        Substitute("$0s", krb_lifetime_seconds_);

    // Add common flags for both masters and tservers.
    const vector<string> common_flags = {
      // Enable tracing of successful KRPC negotiations as well.
      "--rpc_trace_negotiation",

      // Speed up Raft elections.
      "--raft_heartbeat_interval_ms=25",
      "--leader_failure_exp_backoff_max_delta_ms=1000",
    };
    std::copy(common_flags.begin(), common_flags.end(),
        std::back_inserter(cluster_opts_.extra_master_flags));
    std::copy(common_flags.begin(), common_flags.end(),
        std::back_inserter(cluster_opts_.extra_tserver_flags));

    const vector<string> tserver_flags = {
      // Decreasing TS->master heartbeat interval speeds up the test.
      "--heartbeat_interval_ms=25",
    };
    std::copy(tserver_flags.begin(), tserver_flags.end(),
        std::back_inserter(cluster_opts_.extra_tserver_flags));
  }

  Status StartCluster() {
    cluster_.reset(new ExternalMiniCluster(cluster_opts_));
    return cluster_->Start();
  }

  Status SmokeTestCluster() {
    using ::kudu::client::sp::shared_ptr;
    static const char* kTableName = "test-table";
    shared_ptr<KuduClient> client;
    RETURN_NOT_OK(cluster_->CreateClient(nullptr, &client));

    // Create a table.
    KuduSchema schema = client::KuduSchemaFromSchema(CreateKeyValueTestSchema());
    gscoped_ptr<KuduTableCreator> table_creator(client->NewTableCreator());

    RETURN_NOT_OK(table_creator->table_name(kTableName)
                  .set_range_partition_columns({ "key" })
                  .schema(&schema)
                  .num_replicas(num_tservers_)
                  .Create());

    // Insert a row.
    shared_ptr<KuduTable> table;
    RETURN_NOT_OK(client->OpenTable(kTableName, &table));
    shared_ptr<KuduSession> session = client->NewSession();
    session->SetTimeoutMillis(30 * 1000);
    unique_ptr<KuduInsert> ins(table->NewInsert());
    RETURN_NOT_OK(ins->mutable_row()->SetInt32(0, 12345));
    RETURN_NOT_OK(ins->mutable_row()->SetInt32(1, 54321));
    RETURN_NOT_OK(session->Apply(ins.release()));
    FlushSessionOrDie(session);

    // Read it back.
    const int64_t num_rows = CountTableRows(table.get());
    if (num_rows != 1) {
      return Status::RuntimeError(
          Substitute("$0: unexpected row count", num_rows));
    }

    // Delete the table.
    return client->DeleteTable(kTableName);
  }

 protected:
  // The ticket lifetime should be long enough to start the cluster and run a
  // single iteration of smoke test workload.
  const int krb_lifetime_seconds_;
  const int num_masters_;
  const int num_tservers_;
  ExternalMiniClusterOptions cluster_opts_;
  std::shared_ptr<ExternalMiniCluster> cluster_;
};

// Check how the system behaves when KDC is not available upon start-up
// of Kudu server-side components.
TEST_F(SecurityComponentsFaultsITest, NoKdcOnStart) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  // Start with the KDC first: let's generate generate keytabs, get initial
  // kerberos tickets, etc.
  ASSERT_OK(StartCluster());
  NO_FATALS(cluster_->Shutdown());

  // Stop the KDC since it's not shutdown on Shutdown() method call: the krb5kdc
  // process is stopped in the destructor of the cluster_ object.
  ASSERT_OK(cluster_->kdc()->Stop());

  // Make sure original Kerberos tickets have expired.
  SleepFor(MonoDelta::FromSeconds(krb_lifetime_seconds_));

  // Try to restart without running KDC (krb5kdc process) -- it should not be
  // possible. The master and tablet servers should crash while starting
  // if kinit() fails.
  {
    auto server = cluster_->master(0);
    ASSERT_NE(nullptr, server);
    const Status s = server->Restart();
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "kudu-master: process exited on signal 6");
  }
  {
    auto server = cluster_->tablet_server(0);
    ASSERT_NE(nullptr, server);
    const Status s = server->Restart();
    ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(),
                        "kudu-tserver: process exited on signal 6");
  }
}

// Check that restarting KDC does not affect running master and tablet servers:
// they are able to operate with no issues past ticket TTL once KDC is back.
TEST_F(SecurityComponentsFaultsITest, KdcRestartsInTheMiddle) {
  if (!AllowSlowTests()) {
    LOG(WARNING) << "test is skipped; set KUDU_ALLOW_SLOW_TESTS=1 to run";
    return;
  }

  // Enable KRPC negotiation tracing for the Kudu client running smoke test
  // workload.
  FLAGS_rpc_trace_negotiation = true;

  ASSERT_OK(StartCluster());
  ASSERT_OK(SmokeTestCluster());

  ASSERT_OK(cluster_->kdc()->Stop());

  // Even if KDC is shut down, if cached Kerberos credentials are still valid
  // when KDC is inactive, everything should work fine.
  ASSERT_OK(SmokeTestCluster());

  // Wait for Kerberos tickets to expire.
  SleepFor(MonoDelta::FromSeconds(krb_lifetime_seconds_));

#ifndef __APPLE__
  // For some reason (may be caching negative responses?), calling this on OS X
  // causes the next call to SmokeTestCluster() fail. Not sure that's expected
  // or correct, so disabling this for OS X until it's clarified.

  // It seems different version of krb5 library handles the error differently:
  // in some cases, the error is about ticket expiration, in other -- failure
  // to contact KDC.
  //
  // Also, different versions of krb5 library have different error messages
  // for the same error.
  const Status s = SmokeTestCluster();
  ASSERT_TRUE(s.IsNotAuthorized()) << s.ToString();
  ASSERT_STR_MATCHES(s.ToString(),
      "Not authorized: Could not connect to the cluster: "
      "Client connection negotiation failed: client connection to .* ("
      "Cannot contact any KDC for realm .*|"
      "Ticket expired.*|"
      "GSSAPI Error:  The context has expire.*|"
      "GSSAPI Error: The referenced context has expired .*|"
      "GSSAPI Error: The referenced credential has expired .*)");
#endif

  ASSERT_OK(cluster_->kdc()->Start());

  // No re-acquire thread is run on the test client, so need to
  // re-acquire tickets explicitly.
  // TODO(aserbin): use constant instead of 'test-admin' string literal here
  ASSERT_OK(cluster_->kdc()->Kinit("test-admin"));

  // After KDC is back and client Kerberos tickets are refreshed/re-acquired,
  // the cluster should be functional again.
  ASSERT_OK(SmokeTestCluster());

  NO_FATALS(cluster_->Shutdown());
}

} // namespace kudu
