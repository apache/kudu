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

#include "kudu/rpc/rpc-test-base.h"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits.h>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include <boost/bind.hpp>
#include <boost/core/ref.hpp>
#include <boost/function.hpp>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/casts.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/acceptor_pool.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/transfer.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/random.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"

METRIC_DECLARE_histogram(handler_latency_kudu_rpc_test_CalculatorService_Sleep);
METRIC_DECLARE_histogram(rpc_incoming_queue_time);

DECLARE_bool(rpc_reopen_outbound_connections);
DECLARE_int32(rpc_negotiation_inject_delay_ms);

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace kudu {
namespace rpc {

class TestRpc : public RpcTestBase, public ::testing::WithParamInterface<bool> {
};

// This is used to run all parameterized tests with and without SSL.
INSTANTIATE_TEST_CASE_P(OptionalSSL, TestRpc, testing::Values(false, true));

TEST_F(TestRpc, TestSockaddr) {
  Sockaddr addr1, addr2;
  addr1.set_port(1000);
  addr2.set_port(2000);
  // port is ignored when comparing Sockaddr objects
  ASSERT_FALSE(addr1 < addr2);
  ASSERT_FALSE(addr2 < addr1);
  ASSERT_EQ(1000, addr1.port());
  ASSERT_EQ(2000, addr2.port());
  ASSERT_EQ(string("0.0.0.0:1000"), addr1.ToString());
  ASSERT_EQ(string("0.0.0.0:2000"), addr2.ToString());
  Sockaddr addr3(addr1);
  ASSERT_EQ(string("0.0.0.0:1000"), addr3.ToString());
}

TEST_P(TestRpc, TestMessengerCreateDestroy) {
  shared_ptr<Messenger> messenger;
  ASSERT_OK(CreateMessenger("TestCreateDestroy", &messenger, 1, GetParam()));
  LOG(INFO) << "started messenger " << messenger->name();
  messenger->Shutdown();
}

// Test starting and stopping a messenger. This is a regression
// test for a segfault seen in early versions of the RPC code,
// in which shutting down the acceptor would trigger an assert,
// making our tests flaky.
TEST_P(TestRpc, TestAcceptorPoolStartStop) {
  int n_iters = AllowSlowTests() ? 100 : 5;
  for (int i = 0; i < n_iters; i++) {
    shared_ptr<Messenger> messenger;
    ASSERT_OK(CreateMessenger("TestAcceptorPoolStartStop", &messenger, 1, GetParam()));
    shared_ptr<AcceptorPool> pool;
    ASSERT_OK(messenger->AddAcceptorPool(Sockaddr(), &pool));
    Sockaddr bound_addr;
    ASSERT_OK(pool->GetBoundAddress(&bound_addr));
    ASSERT_NE(0, bound_addr.port());
    ASSERT_OK(pool->Start(2));
    messenger->Shutdown();
  }
}

TEST_F(TestRpc, TestConnHeaderValidation) {
  MessengerBuilder mb("TestRpc.TestConnHeaderValidation");
  const int conn_hdr_len = kMagicNumberLength + kHeaderFlagsLength;
  uint8_t buf[conn_hdr_len];
  serialization::SerializeConnHeader(buf);
  ASSERT_OK(serialization::ValidateConnHeader(Slice(buf, conn_hdr_len)));
}

// Regression test for KUDU-2041
TEST_P(TestRpc, TestNegotiationDeadlock) {
  bool enable_ssl = GetParam();

  // The deadlock would manifest in cases where the number of concurrent connection
  // requests >= the number of threads. 1 thread and 1 cnxn to ourself is just the easiest
  // way to reproduce the issue, because the server negotiation task must get queued after
  // the client negotiation task if they share the same thread pool.
  MessengerBuilder mb("TestRpc.TestNegotiationDeadlock");
  mb.set_min_negotiation_threads(1)
      .set_max_negotiation_threads(1)
      .set_metric_entity(metric_entity_);
  if (enable_ssl) mb.enable_inbound_tls();

  shared_ptr<Messenger> messenger;
  CHECK_OK(mb.Build(&messenger));

  Sockaddr server_addr;
  ASSERT_OK(StartTestServerWithCustomMessenger(&server_addr, messenger, enable_ssl));

  Proxy p(messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());
  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
}

// Test making successful RPC calls.
TEST_P(TestRpc, TestCall) {
  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());
  ASSERT_STR_CONTAINS(p.ToString(), strings::Substitute("kudu.rpc.GenericCalculatorService@"
                                                            "{remote=$0, user_credentials=",
                                                        server_addr.ToString()));

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
  }
}

TEST_P(TestRpc, TestCallWithChainCerts) {
  bool enable_ssl = GetParam();
  // We're only interested in running this test with TLS enabled.
  if (!enable_ssl) return;

  string rpc_certificate_file;
  string rpc_private_key_file;
  string rpc_ca_certificate_file;
  ASSERT_OK(security::CreateTestSSLCertSignedByChain(GetTestDataDirectory(),
                                                     &rpc_certificate_file,
                                                     &rpc_private_key_file,
                                                     &rpc_ca_certificate_file));
  // Set up server.
  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  SCOPED_TRACE(strings::Substitute("Connecting to $0", server_addr.ToString()));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl,
      rpc_certificate_file, rpc_private_key_file, rpc_ca_certificate_file));

  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());
  ASSERT_STR_CONTAINS(p.ToString(), strings::Substitute("kudu.rpc.GenericCalculatorService@"
                                                            "{remote=$0, user_credentials=",
                                                        server_addr.ToString()));

  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
}

// Test making successful RPC calls while using a TLS certificate with a password protected
// private key.
TEST_P(TestRpc, TestCallWithPasswordProtectedKey) {
  bool enable_ssl = GetParam();
  // We're only interested in running this test with TLS enabled.
  if (!enable_ssl) return;

  string rpc_certificate_file;
  string rpc_private_key_file;
  string rpc_ca_certificate_file;
  string rpc_private_key_password_cmd;
  string passwd;
  ASSERT_OK(security::CreateTestSSLCertWithEncryptedKey(GetTestDataDirectory(),
                                                        &rpc_certificate_file,
                                                        &rpc_private_key_file,
                                                        &passwd));
  rpc_ca_certificate_file = rpc_certificate_file;
  rpc_private_key_password_cmd = strings::Substitute("echo $0", passwd);
  // Set up server.
  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  SCOPED_TRACE(strings::Substitute("Connecting to $0", server_addr.ToString()));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl,
      rpc_certificate_file, rpc_private_key_file, rpc_ca_certificate_file,
      rpc_private_key_password_cmd));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());
  ASSERT_STR_CONTAINS(p.ToString(), strings::Substitute("kudu.rpc.GenericCalculatorService@"
                                                            "{remote=$0, user_credentials=",
                                                        server_addr.ToString()));

  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
}

// Test that using a TLS certificate with a password protected private key and providing
// the wrong password for that private key, causes a server startup failure.
TEST_P(TestRpc, TestCallWithBadPasswordProtectedKey) {
  bool enable_ssl = GetParam();
  // We're only interested in running this test with TLS enabled.
  if (!enable_ssl) return;

  string rpc_certificate_file;
  string rpc_private_key_file;
  string rpc_ca_certificate_file;
  string rpc_private_key_password_cmd;
  string passwd;
  ASSERT_OK(security::CreateTestSSLCertWithEncryptedKey(GetTestDataDirectory(),
                                                        &rpc_certificate_file,
                                                        &rpc_private_key_file,
                                                        &passwd));
  // Overwrite the password with an invalid one.
  passwd = "badpassword";
  rpc_ca_certificate_file = rpc_certificate_file;
  rpc_private_key_password_cmd = strings::Substitute("echo $0", passwd);
  // Verify that the server fails to start up.
  Sockaddr server_addr;
  Status s = StartTestServer(&server_addr, enable_ssl, rpc_certificate_file, rpc_private_key_file,
      rpc_ca_certificate_file, rpc_private_key_password_cmd);
  ASSERT_TRUE(s.IsRuntimeError());
  ASSERT_STR_CONTAINS(s.ToString(), "failed to load private key file");
}

// Test that connecting to an invalid server properly throws an error.
TEST_P(TestRpc, TestCallToBadServer) {
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, GetParam()));
  Sockaddr addr;
  addr.set_port(0);
  Proxy p(client_messenger, addr, addr.host(),
          GenericCalculatorService::static_service_name());

  // Loop a few calls to make sure that we properly set up and tear down
  // the connections.
  for (int i = 0; i < 5; i++) {
    Status s = DoTestSyncCall(p, GenericCalculatorService::kAddMethodName);
    LOG(INFO) << "Status: " << s.ToString();
    ASSERT_TRUE(s.IsNetworkError()) << "unexpected status: " << s.ToString();
  }
}

// Test that RPC calls can be failed with an error status on the server.
TEST_P(TestRpc, TestInvalidMethodCall) {
  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  // Call the method which fails.
  Status s = DoTestSyncCall(p, "ThisMethodDoesNotExist");
  ASSERT_TRUE(s.IsRemoteError()) << "unexpected status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "bad method");
}

// Test that the error message returned when connecting to the wrong service
// is reasonable.
TEST_P(TestRpc, TestWrongService) {
  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client with the wrong service name.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, "localhost", "WrongServiceName");

  // Call the method which fails.
  Status s = DoTestSyncCall(p, "ThisMethodDoesNotExist");
  ASSERT_TRUE(s.IsRemoteError()) << "unexpected status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Service unavailable: service WrongServiceName "
                      "not registered on TestServer");
}

// Test that we can still make RPC connections even if many fds are in use.
// This is a regression test for KUDU-650.
TEST_P(TestRpc, TestHighFDs) {
  // This test can only run if ulimit is set high.
  const int kNumFakeFiles = 3500;
  const int kMinUlimit = kNumFakeFiles + 100;
  if (env_->GetOpenFileLimit() < kMinUlimit) {
    LOG(INFO) << "Test skipped: must increase ulimit -n to at least " << kMinUlimit;
    return;
  }

  // Open a bunch of fds just to increase our fd count.
  vector<RandomAccessFile*> fake_files;
  ElementDeleter d(&fake_files);
  for (int i = 0; i < kNumFakeFiles; i++) {
    unique_ptr<RandomAccessFile> f;
    CHECK_OK(Env::Default()->NewRandomAccessFile("/dev/zero", &f));
    fake_files.push_back(f.release());
  }

  // Set up server and client, and verify we can make a successful call.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());
  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
}

// Test that connections are kept alive between calls.
TEST_P(TestRpc, TestConnectionKeepalive) {
  // Only run one reactor per messenger, so we can grab the metrics from that
  // one without having to check all.
  n_server_reactor_threads_ = 1;
  keepalive_time_ms_ = 500;

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));

  SleepFor(MonoDelta::FromMilliseconds(5));

  ReactorMetrics metrics;
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(1, metrics.num_server_connections_) << "Server should have 1 server connection";
  ASSERT_EQ(0, metrics.num_client_connections_) << "Server should have 0 client connections";

  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_) << "Client should have 0 server connections";
  ASSERT_EQ(1, metrics.num_client_connections_) << "Client should have 1 client connections";

  SleepFor(MonoDelta::FromMilliseconds(2 * keepalive_time_ms_));

  // After sleeping, the keepalive timer should have closed both sides of
  // the connection.
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_) << "Server should have 0 server connections";
  ASSERT_EQ(0, metrics.num_client_connections_) << "Server should have 0 client connections";

  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_) << "Client should have 0 server connections";
  ASSERT_EQ(0, metrics.num_client_connections_) << "Client should have 0 client connections";
}

// Test that idle connection is kept alive when 'keepalive_time_ms_' is set to -1.
TEST_P(TestRpc, TestConnectionAlwaysKeepalive) {
  // Only run one reactor per messenger, so we can grab the metrics from that
  // one without having to check all.
  n_server_reactor_threads_ = 1;
  keepalive_time_ms_ = -1;

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));

  ReactorMetrics metrics;
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(1, metrics.num_server_connections_) << "Server should have 1 server connection";
  ASSERT_EQ(0, metrics.num_client_connections_) << "Server should have 0 client connections";

  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_) << "Client should have 0 server connections";
  ASSERT_EQ(1, metrics.num_client_connections_) << "Client should have 1 client connections";

  SleepFor(MonoDelta::FromSeconds(3));

  // After sleeping, the connection should still be alive.
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(1, metrics.num_server_connections_) << "Server should have 1 server connections";
  ASSERT_EQ(0, metrics.num_client_connections_) << "Server should have 0 client connections";

  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_) << "Client should have 0 server connections";
  ASSERT_EQ(1, metrics.num_client_connections_) << "Client should have 1 client connections";
}

// Test that the metrics on a per connection level work accurately.
TEST_P(TestRpc, TestClientConnectionMetrics) {
  // Only run one reactor per messenger, so we can grab the metrics from that
  // one without having to check all.
  n_server_reactor_threads_ = 1;
  keepalive_time_ms_ = -1;

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  // Cause the reactor thread to be blocked for 2 seconds.
  server_messenger_->ScheduleOnReactor(boost::bind(sleep, 2), MonoDelta::FromSeconds(0));

  RpcController controller;
  DumpRunningRpcsRequestPB dump_req;
  DumpRunningRpcsResponsePB dump_resp;
  dump_req.set_include_traces(false);

  // We'll send several calls asynchronously to force RPC queueing on the sender side.
  int n_calls = 1000;
  AddRequestPB add_req;
  add_req.set_x(rand());
  add_req.set_y(rand());
  AddResponsePB add_resp;

  vector<unique_ptr<RpcController>> controllers;
  CountDownLatch latch(n_calls);
  for (int i = 0; i < n_calls; i++) {
    controllers.emplace_back(new RpcController());
    p.AsyncRequest(GenericCalculatorService::kAddMethodName, add_req, &add_resp,
        controllers.back().get(), boost::bind(&CountDownLatch::CountDown, boost::ref(latch)));
  }

  // Since we blocked the only reactor thread for sometime, we should see RPCs queued on the
  // OutboundTransfer queue, unless the main thread is very slow.
  ASSERT_OK(client_messenger->DumpRunningRpcs(dump_req, &dump_resp));
  ASSERT_EQ(1, dump_resp.outbound_connections_size());
  ASSERT_GT(dump_resp.outbound_connections(0).outbound_queue_size(), 0);

  // Wait for the calls to be marked finished.
  latch.Wait();

  // Verify that all the RPCs have finished.
  for (const auto& controller : controllers) {
    ASSERT_TRUE(controller->finished());
  }
}

// Test that outbound connections to the same server are reopen upon every RPC
// call when the 'rpc_reopen_outbound_connections' flag is set.
TEST_P(TestRpc, TestReopenOutboundConnections) {
  // Set the flag to enable special mode: close and reopen already established
  // outbound connections.
  FLAGS_rpc_reopen_outbound_connections = true;

  // Only run one reactor per messenger, so we can grab the metrics from that
  // one without having to check all.
  n_server_reactor_threads_ = 1;

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  // Verify the initial counters.
  ReactorMetrics metrics;
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);

  // Run several iterations, just in case.
  for (int i = 0; i < 32; ++i) {
    ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
    ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
    ASSERT_EQ(0, metrics.total_client_connections_);
    ASSERT_EQ(i + 1, metrics.total_server_connections_);
    ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
    ASSERT_EQ(i + 1, metrics.total_client_connections_);
    ASSERT_EQ(0, metrics.total_server_connections_);
  }
}

// Test that an outbound connection is closed and a new one is open if going
// from ANY_CREDENTIALS to PRIMARY_CREDENTIALS policy for RPC calls to the same
// destination.
// Test that changing from PRIMARY_CREDENTIALS policy to ANY_CREDENTIALS policy
// re-uses the connection established with PRIMARY_CREDENTIALS policy.
TEST_P(TestRpc, TestCredentialsPolicy) {
  // Only run one reactor per messenger, so we can grab the metrics from that
  // one without having to check all.
  n_server_reactor_threads_ = 1;

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  // Verify the initial counters.
  ReactorMetrics metrics;
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);

  // Make an RPC call with ANY_CREDENTIALS policy.
  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  EXPECT_EQ(0, metrics.total_client_connections_);
  EXPECT_EQ(1, metrics.total_server_connections_);
  EXPECT_EQ(1, metrics.num_server_connections_);
  EXPECT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  EXPECT_EQ(1, metrics.total_client_connections_);
  EXPECT_EQ(0, metrics.total_server_connections_);
  EXPECT_EQ(1, metrics.num_client_connections_);

  // This is to allow all the data to be sent so the connection becomes idle.
  SleepFor(MonoDelta::FromMilliseconds(5));

  // Make an RPC call with PRIMARY_CREDENTIALS policy. Currently open connection
  // with ANY_CREDENTIALS policy should be closed and a new one established
  // with PRIMARY_CREDENTIALS policy.
  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName,
                           CredentialsPolicy::PRIMARY_CREDENTIALS));
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  EXPECT_EQ(0, metrics.total_client_connections_);
  EXPECT_EQ(2, metrics.total_server_connections_);
  EXPECT_EQ(1, metrics.num_server_connections_);
  EXPECT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  EXPECT_EQ(2, metrics.total_client_connections_);
  EXPECT_EQ(0, metrics.total_server_connections_);
  EXPECT_EQ(1, metrics.num_client_connections_);

  // Make another RPC call with ANY_CREDENTIALS policy. The already established
  // connection with PRIMARY_CREDENTIALS policy should be re-used because
  // the ANY_CREDENTIALS policy satisfies the PRIMARY_CREDENTIALS policy which
  // the currently open connection has been established with.
  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  EXPECT_EQ(0, metrics.total_client_connections_);
  EXPECT_EQ(2, metrics.total_server_connections_);
  EXPECT_EQ(1, metrics.num_server_connections_);
  EXPECT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  EXPECT_EQ(2, metrics.total_client_connections_);
  EXPECT_EQ(0, metrics.total_server_connections_);
  EXPECT_EQ(1, metrics.num_client_connections_);
}

// Test that a call which takes longer than the keepalive time
// succeeds -- i.e that we don't consider a connection to be "idle" on the
// server if there is a call outstanding on it.
TEST_P(TestRpc, TestCallLongerThanKeepalive) {
  // Set a short keepalive.
  keepalive_time_ms_ = 1000;

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  // Make a call which sleeps longer than the keepalive.
  RpcController controller;
  SleepRequestPB req;
  req.set_sleep_micros(3 * 1000 * 1000); // 3 seconds.
  req.set_deferred(true);
  SleepResponsePB resp;
  ASSERT_OK(p.SyncRequest(GenericCalculatorService::kSleepMethodName,
                                 req, &resp, &controller));
}

// Test that the RpcSidecar transfers the expected messages.
TEST_P(TestRpc, TestRpcSidecar) {
  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, GetParam()));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  // Test a zero-length sidecar
  DoTestSidecar(p, 0, 0);

  // Test some small sidecars
  DoTestSidecar(p, 123, 456);

  // Test some larger sidecars to verify that we properly handle the case where
  // we can't write the whole response to the socket in a single call.
  DoTestSidecar(p, 3000 * 1024, 2000 * 1024);

  DoTestOutgoingSidecarExpectOK(p, 0, 0);
  DoTestOutgoingSidecarExpectOK(p, 123, 456);
  DoTestOutgoingSidecarExpectOK(p, 3000 * 1024, 2000 * 1024);
}

TEST_P(TestRpc, TestRpcSidecarLimits) {
  {
    // Test that the limits on the number of sidecars is respected.
    RpcController controller;
    string s = "foo";
    int idx;
    for (int i = 0; i < TransferLimits::kMaxSidecars; ++i) {
      ASSERT_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(Slice(s)), &idx));
    }

    ASSERT_TRUE(controller.AddOutboundSidecar(
        RpcSidecar::FromSlice(Slice(s)), &idx).IsRuntimeError());
  }

  {
    // Test that the payload may not exceed --rpc_max_message_size.
    // Set up server.
    Sockaddr server_addr;
    bool enable_ssl = GetParam();
    ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

    // Set up client.
    shared_ptr<Messenger> client_messenger;
    ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, GetParam()));
    Proxy p(client_messenger, server_addr, server_addr.host(),
            GenericCalculatorService::static_service_name());

    RpcController controller;
    // KUDU-2305: Test with a maximal payload to verify that the implementation
    // can handle the limits.
    string s;
    s.resize(INT_MAX, 'a');
    int idx;
    CHECK_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(Slice(s)), &idx));

    PushTwoStringsRequestPB request;
    request.set_sidecar1_idx(idx);
    request.set_sidecar2_idx(idx);
    PushTwoStringsResponsePB resp;
    Status status = p.SyncRequest(GenericCalculatorService::kPushTwoStringsMethodName,
        request, &resp, &controller);
    ASSERT_TRUE(status.IsNetworkError()) << "Unexpected error: " << status.ToString();
    // Remote responds to extra-large payloads by closing the connection.
    ASSERT_STR_MATCHES(status.ToString(),
                       // Linux
                       "Connection reset by peer"
                       // macOS, while reading from socket.
                       "|got EOF from remote"
                       // macOS, while writing to socket.
                       "|Protocol wrong type for socket");
  }
}

// Test that timeouts are properly handled.
TEST_P(TestRpc, TestCallTimeout) {
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  // Test a very short timeout - we expect this will time out while the
  // call is still trying to connect, or in the send queue. This was triggering ASAN failures
  // before.
  ASSERT_NO_FATAL_FAILURE(DoTestExpectTimeout(p, MonoDelta::FromNanoseconds(1)));

  // Test a longer timeout - expect this will time out after we send the request,
  // but shorter than our threshold for two-stage timeout handling.
  ASSERT_NO_FATAL_FAILURE(DoTestExpectTimeout(p, MonoDelta::FromMilliseconds(200)));

  // Test a longer timeout - expect this will trigger the "two-stage timeout"
  // code path.
  ASSERT_NO_FATAL_FAILURE(DoTestExpectTimeout(p, MonoDelta::FromMilliseconds(1500)));
}

// Inject 500ms delay in negotiation, and send a call with a short timeout, followed by
// one with a long timeout. The call with the long timeout should succeed even though
// the previous one failed.
//
// This is a regression test against prior behavior where the connection negotiation
// was assigned the timeout of the first call on that connection. So, if the first
// call had a short timeout, the later call would also inherit the timed-out negotiation.
TEST_P(TestRpc, TestCallTimeoutDoesntAffectNegotiation) {
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  FLAGS_rpc_negotiation_inject_delay_ms = 500;
  ASSERT_NO_FATAL_FAILURE(DoTestExpectTimeout(p, MonoDelta::FromMilliseconds(50)));
  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));

  // Only the second call should have been received by the server, because we
  // don't bother sending an already-timed-out call.
  auto metric_map = server_messenger_->metric_entity()->UnsafeMetricsMapForTests();
  auto* metric = FindOrDie(metric_map, &METRIC_rpc_incoming_queue_time).get();
  ASSERT_EQ(1, down_cast<Histogram*>(metric)->TotalCount());
}

static void AcceptAndReadForever(Socket* listen_sock) {
  // Accept the TCP connection.
  Socket server_sock;
  Sockaddr remote;
  CHECK_OK(listen_sock->Accept(&server_sock, &remote, 0));

  MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(10);

  size_t nread;
  uint8_t buf[1024];
  while (server_sock.BlockingRecv(buf, sizeof(buf), &nread, deadline).ok()) {
  }
}

// Starts a fake listening socket which never actually negotiates.
// Ensures that the client gets a reasonable status code in this case.
TEST_F(TestRpc, TestNegotiationTimeout) {
  // Set up a simple socket server which accepts a connection.
  Sockaddr server_addr;
  Socket listen_sock;
  ASSERT_OK(StartFakeServer(&listen_sock, &server_addr));

  // Create another thread to accept the connection on the fake server.
  scoped_refptr<Thread> acceptor_thread;
  ASSERT_OK(Thread::Create("test", "acceptor",
                           AcceptAndReadForever, &listen_sock,
                           &acceptor_thread));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  bool is_negotiation_error = false;
  ASSERT_NO_FATAL_FAILURE(DoTestExpectTimeout(
      p, MonoDelta::FromMilliseconds(100), &is_negotiation_error));
  EXPECT_TRUE(is_negotiation_error);

  acceptor_thread->Join();
}

// Test that client calls get failed properly when the server they're connected to
// shuts down.
TEST_F(TestRpc, TestServerShutsDown) {
  // Set up a simple socket server which accepts a connection.
  Sockaddr server_addr;
  Socket listen_sock;
  ASSERT_OK(StartFakeServer(&listen_sock, &server_addr));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  // Send a call.
  AddRequestPB req;
  req.set_x(rand());
  req.set_y(rand());
  AddResponsePB resp;

  vector<unique_ptr<RpcController>> controllers;

  // We'll send several calls async, and ensure that they all
  // get the error status when the connection drops.
  int n_calls = 5;

  CountDownLatch latch(n_calls);
  for (int i = 0; i < n_calls; i++) {
    controllers.emplace_back(new RpcController());
    p.AsyncRequest(GenericCalculatorService::kAddMethodName, req, &resp, controllers.back().get(),
                   boost::bind(&CountDownLatch::CountDown, boost::ref(latch)));
  }

  // Accept the TCP connection.
  Socket server_sock;
  Sockaddr remote;
  ASSERT_OK(listen_sock.Accept(&server_sock, &remote, 0));

  // The call is still in progress at this point.
  for (const auto& controller : controllers) {
    ASSERT_FALSE(controller->finished());
  }

  // Shut down the socket.
  ASSERT_OK(listen_sock.Close());
  ASSERT_OK(server_sock.Close());

  // Wait for the call to be marked finished.
  latch.Wait();

  // Should get the appropriate error on the client for all calls;
  for (const auto& controller : controllers) {
    ASSERT_TRUE(controller->finished());
    Status s = controller->status();
    ASSERT_TRUE(s.IsNetworkError()) <<
      "Unexpected status: " << s.ToString();

    // Any of these errors could happen, depending on whether we were
    // in the middle of sending a call while the connection died, or
    // if we were already waiting for responses.
    //
    // ECONNREFUSED is possible because the sending of the calls is async.
    // For example, the following interleaving:
    // - Enqueue 3 calls
    // - Reactor wakes up, creates connection, starts writing calls
    // - Enqueue 2 more calls
    // - Shut down socket
    // - Reactor wakes up, tries to write more of the first 3 calls, gets error
    // - Reactor shuts down connection
    // - Reactor sees the 2 remaining calls, makes a new connection
    // - Because the socket is shut down, gets ECONNREFUSED.
    //
    // EINVAL is possible if the controller socket had already disconnected by
    // the time it trys to set the SO_SNDTIMEO socket option as part of the
    // normal blocking SASL handshake.
    ASSERT_TRUE(s.posix_code() == EPIPE ||
                s.posix_code() == ECONNRESET ||
                s.posix_code() == ESHUTDOWN ||
                s.posix_code() == ECONNREFUSED ||
                s.posix_code() == EINVAL)
      << "Unexpected status: " << s.ToString();
  }
}

// Test handler latency metric.
TEST_P(TestRpc, TestRpcHandlerLatencyMetric) {

  const uint64_t sleep_micros = 20 * 1000;

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          CalculatorService::static_service_name());

  RpcController controller;
  SleepRequestPB req;
  req.set_sleep_micros(sleep_micros);
  req.set_deferred(true);
  SleepResponsePB resp;
  ASSERT_OK(p.SyncRequest("Sleep", req, &resp, &controller));

  const unordered_map<const MetricPrototype*, scoped_refptr<Metric> > metric_map =
    server_messenger_->metric_entity()->UnsafeMetricsMapForTests();

  scoped_refptr<Histogram> latency_histogram = down_cast<Histogram *>(
      FindOrDie(metric_map,
                &METRIC_handler_latency_kudu_rpc_test_CalculatorService_Sleep).get());

  LOG(INFO) << "Sleep() min lat: " << latency_histogram->MinValueForTests();
  LOG(INFO) << "Sleep() mean lat: " << latency_histogram->MeanValueForTests();
  LOG(INFO) << "Sleep() max lat: " << latency_histogram->MaxValueForTests();
  LOG(INFO) << "Sleep() #calls: " << latency_histogram->TotalCount();

  ASSERT_EQ(1, latency_histogram->TotalCount());
  ASSERT_GE(latency_histogram->MaxValueForTests(), sleep_micros);
  ASSERT_TRUE(latency_histogram->MinValueForTests() == latency_histogram->MaxValueForTests());

  // TODO: Implement an incoming queue latency test.
  // For now we just assert that the metric exists.
  ASSERT_TRUE(FindOrDie(metric_map, &METRIC_rpc_incoming_queue_time));
}

static void DestroyMessengerCallback(shared_ptr<Messenger>* messenger,
                                     CountDownLatch* latch) {
  messenger->reset();
  latch->CountDown();
}

TEST_P(TestRpc, TestRpcCallbackDestroysMessenger) {
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, GetParam()));
  Sockaddr bad_addr;
  CountDownLatch latch(1);

  AddRequestPB req;
  req.set_x(rand());
  req.set_y(rand());
  AddResponsePB resp;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(1));
  {
    Proxy p(client_messenger, bad_addr, "xxx-host", "xxx-service");
    p.AsyncRequest("my-fake-method", req, &resp, &controller,
                   boost::bind(&DestroyMessengerCallback, &client_messenger, &latch));
  }
  latch.Wait();
}

// Test that setting the client timeout / deadline gets propagated to RPC
// services.
TEST_P(TestRpc, TestRpcContextClientDeadline) {
  const uint64_t sleep_micros = 20 * 1000;

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          CalculatorService::static_service_name());

  SleepRequestPB req;
  req.set_sleep_micros(sleep_micros);
  req.set_client_timeout_defined(true);
  SleepResponsePB resp;
  RpcController controller;
  Status s = p.SyncRequest("Sleep", req, &resp, &controller);
  ASSERT_TRUE(s.IsRemoteError());
  ASSERT_STR_CONTAINS(s.ToString(), "Missing required timeout");

  controller.Reset();
  controller.set_timeout(MonoDelta::FromMilliseconds(1000));
  ASSERT_OK(p.SyncRequest("Sleep", req, &resp, &controller));
}

// Test that setting an call-level application feature flag to an unknown value
// will make the server reject the call.
TEST_P(TestRpc, TestApplicationFeatureFlag) {
  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          CalculatorService::static_service_name());

  { // Supported flag
    AddRequestPB req;
    req.set_x(1);
    req.set_y(2);
    AddResponsePB resp;
    RpcController controller;
    controller.RequireServerFeature(FeatureFlags::FOO);
    Status s = p.SyncRequest("Add", req, &resp, &controller);
    SCOPED_TRACE(strings::Substitute("supported response: $0", s.ToString()));
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(resp.result(), 3);
  }

  { // Unsupported flag
    AddRequestPB req;
    req.set_x(1);
    req.set_y(2);
    AddResponsePB resp;
    RpcController controller;
    controller.RequireServerFeature(FeatureFlags::FOO);
    controller.RequireServerFeature(99);
    Status s = p.SyncRequest("Add", req, &resp, &controller);
    SCOPED_TRACE(strings::Substitute("unsupported response: $0", s.ToString()));
    ASSERT_TRUE(s.IsRemoteError());
  }
}

TEST_P(TestRpc, TestApplicationFeatureFlagUnsupportedServer) {
  auto savedFlags = kSupportedServerRpcFeatureFlags;
  auto cleanup = MakeScopedCleanup([&] () { kSupportedServerRpcFeatureFlags = savedFlags; });
  kSupportedServerRpcFeatureFlags = {};

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          CalculatorService::static_service_name());

  { // Required flag
    AddRequestPB req;
    req.set_x(1);
    req.set_y(2);
    AddResponsePB resp;
    RpcController controller;
    controller.RequireServerFeature(FeatureFlags::FOO);
    Status s = p.SyncRequest("Add", req, &resp, &controller);
    SCOPED_TRACE(strings::Substitute("supported response: $0", s.ToString()));
    ASSERT_TRUE(s.IsNotSupported());
  }

  { // No required flag
    AddRequestPB req;
    req.set_x(1);
    req.set_y(2);
    AddResponsePB resp;
    RpcController controller;
    Status s = p.SyncRequest("Add", req, &resp, &controller);
    SCOPED_TRACE(strings::Substitute("supported response: $0", s.ToString()));
    ASSERT_TRUE(s.ok());
  }
}

TEST_P(TestRpc, TestCancellation) {
  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  for (int i = OutboundCall::READY; i <= OutboundCall::FINISHED_SUCCESS; ++i) {
    FLAGS_rpc_inject_cancellation_state = i;
    switch (i) {
      case OutboundCall::READY:
      case OutboundCall::ON_OUTBOUND_QUEUE:
      case OutboundCall::SENDING:
      case OutboundCall::SENT:
        ASSERT_TRUE(DoTestOutgoingSidecar(p, 0, 0).IsAborted());
        ASSERT_TRUE(DoTestOutgoingSidecar(p, 123, 456).IsAborted());
        ASSERT_TRUE(DoTestOutgoingSidecar(p, 3000 * 1024, 2000 * 1024).IsAborted());
        break;
      case OutboundCall::NEGOTIATION_TIMED_OUT:
      case OutboundCall::TIMED_OUT:
        DoTestExpectTimeout(p, MonoDelta::FromMilliseconds(1000));
        break;
      case OutboundCall::CANCELLED:
        break;
      case OutboundCall::FINISHED_NEGOTIATION_ERROR:
      case OutboundCall::FINISHED_ERROR: {
        AddRequestPB req;
        req.set_x(1);
        req.set_y(2);
        AddResponsePB resp;
        RpcController controller;
        controller.RequireServerFeature(FeatureFlags::FOO);
        controller.RequireServerFeature(99);
        Status s = p.SyncRequest("Add", req, &resp, &controller);
        ASSERT_TRUE(s.IsRemoteError());
        break;
      }
      case OutboundCall::FINISHED_SUCCESS:
        DoTestOutgoingSidecarExpectOK(p, 0, 0);
        DoTestOutgoingSidecarExpectOK(p, 123, 456);
        DoTestOutgoingSidecarExpectOK(p, 3000 * 1024, 2000 * 1024);
        break;
    }
  }
  client_messenger->Shutdown();
}

#define TEST_PAYLOAD_SIZE  (1 << 23)
#define TEST_SLEEP_TIME_MS (500)

static void SleepCallback(uint8_t* payload, CountDownLatch* latch) {
  // Overwrites the payload which the sidecar is pointing to. The server
  // checks if the payload matches the expected pattern to detect cases
  // in which the payload is overwritten while it's being sent.
  memset(payload, 0, TEST_PAYLOAD_SIZE);
  latch->CountDown();
}

TEST_P(TestRpc, TestCancellationAsync) {
  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  RpcController controller;

  // The payload to be used during the RPC.
  gscoped_array<uint8_t> payload(new uint8_t[TEST_PAYLOAD_SIZE]);

  // Used to generate sleep time between invoking RPC and requesting cancellation.
  Random rand(SeedRandom());

  for (int i = 0; i < 10; ++i) {
    SleepWithSidecarRequestPB req;
    SleepWithSidecarResponsePB resp;

    // Initialize the payload with non-zero pattern.
    memset(payload.get(), 0xff, TEST_PAYLOAD_SIZE);
    req.set_sleep_micros(TEST_SLEEP_TIME_MS);
    req.set_pattern(0xffffffff);
    req.set_num_repetitions(TEST_PAYLOAD_SIZE / sizeof(uint32_t));

    int idx;
    Slice s(payload.get(), TEST_PAYLOAD_SIZE);
    CHECK_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(s), &idx));
    req.set_sidecar_idx(idx);

    CountDownLatch latch(1);
    p.AsyncRequest(GenericCalculatorService::kSleepWithSidecarMethodName,
                   req, &resp, &controller,
                   boost::bind(SleepCallback, payload.get(), &latch));
    // Sleep for a while before cancelling the RPC.
    if (i > 0) SleepFor(MonoDelta::FromMicroseconds(rand.Uniform64(i * 30)));
    controller.Cancel();
    latch.Wait();
    ASSERT_TRUE(controller.status().IsAborted() || controller.status().ok());
    controller.Reset();
  }
  client_messenger->Shutdown();
}

} // namespace rpc
} // namespace kudu
