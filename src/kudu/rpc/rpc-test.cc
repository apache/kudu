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

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/casts.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/reactor.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/rpc/transfer.h"
#include "kudu/security/test/test_certs.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/diagnostic_socket.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/net/socket_info.pb.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace rpc {
class AcceptorPool;
}  // namespace rpc
}  // namespace kudu

METRIC_DECLARE_counter(queue_overflow_rejections_kudu_rpc_test_CalculatorService_Sleep);
METRIC_DECLARE_counter(timed_out_on_response_kudu_rpc_test_CalculatorService_Sleep);
METRIC_DECLARE_gauge_int32(rpc_pending_connections);
METRIC_DECLARE_histogram(acceptor_dispatch_times);
METRIC_DECLARE_histogram(handler_latency_kudu_rpc_test_CalculatorService_Sleep);
METRIC_DECLARE_histogram(rpc_incoming_queue_time);
METRIC_DECLARE_histogram(rpc_listen_socket_rx_queue_size);

DECLARE_bool(rpc_reopen_outbound_connections);
DECLARE_bool(rpc_suppress_negotiation_trace);
DECLARE_int32(rpc_listen_socket_stats_every_log2);
DECLARE_int32(rpc_negotiation_inject_delay_ms);
DECLARE_int32(tcp_keepalive_probe_period_s);
DECLARE_int32(tcp_keepalive_retry_period_s);
DECLARE_int32(tcp_keepalive_retry_count);
DECLARE_string(ip_config_mode);

using std::map;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rpc {

// RPC proxies require a hostname to be passed. In this test we're just connecting to
// the wildcard, so we'll hard-code this hostname instead.
static const char* const kRemoteHostName = "localhost";

enum RpcSocketMode {
  TCP_IPv4_SSL,
  TCP_IPv4_NOSSL,
  TCP_IPv6_SSL,
  TCP_IPv6_NOSSL,
  UNIX_SSL,
  UNIX_NOSSL,
  INVALID_MODE
};

string ModeEnumToString(enum RpcSocketMode mode) {
  string test;
  switch (mode) {
    case TCP_IPv4_SSL:
      test = Substitute("TCP_IPv4_SSL");
      break;
    case TCP_IPv4_NOSSL:
      test = Substitute("TCP_IPv4_NoSSL");
      break;
    case TCP_IPv6_SSL:
      test = Substitute("TCP_IPv6_SSL");
      break;
    case TCP_IPv6_NOSSL:
      test = Substitute("TCP_IPv6_NoSSL");
      break;
    case UNIX_SSL:
      test = Substitute("UnixSocket_SSL");
      break;
    case UNIX_NOSSL:
      test = Substitute("UnixSocket_NoSSL");
      break;
    default:
      test = Substitute("Invalid_Mode");
      break;
  }
  return test;
}

class TestRpc: public RpcTestBase,
               public ::testing::WithParamInterface<RpcSocketMode> {
protected:
  static bool enable_ssl() {
    switch (GetParam()) {
      case TCP_IPv4_SSL:
      case TCP_IPv6_SSL:
      case UNIX_SSL:
        return true;
      default:
        break;
    }
    return false;
  }
  static bool use_unix_socket() {
    switch (GetParam()) {
      case UNIX_SSL:
      case UNIX_NOSSL:
        return true;
      default:
        break;
    }
    return false;
  }
  static sa_family_t ip_family() {
    switch (GetParam()) {
      case TCP_IPv4_SSL:
      case TCP_IPv4_NOSSL:
        return AF_INET;
      case TCP_IPv6_SSL:
      case TCP_IPv6_NOSSL:
        return AF_INET6;
      default:
        break;
    }
    return AF_UNSPEC;
  }
  Sockaddr bind_addr() const {
    if (use_unix_socket()) {
      // Ensure multiple calls to bind_addr work by unlinking the socket file.
      // The only way to reuse a socket file is to remove it with unlink().
      unlink(socket_path_.c_str());
      Sockaddr addr;
      CHECK_OK(addr.ParseUnixDomainPath(socket_path_));
      return addr;
    }
    return Sockaddr::Wildcard(ip_family());
  }
  static string expected_remote_str(const Sockaddr& bound_addr) {
    if (bound_addr.is_ip()) {
      return Substitute("$0 ($1)", bound_addr.ToString(), kRemoteHostName);
    }
    return bound_addr.ToString();
  }
  void TearDown() override {
    RpcTestBase::TearDown();
    // Ensure we cleanup the socket file on teardown.
    unlink(socket_path_.c_str());
  }

  std::string socket_path_ = GetTestSocketPath("rpc-test");
};

// This is used to run all parameterized tests with and without SSL,
// on Unix sockets and TCP.
INSTANTIATE_TEST_SUITE_P(Parameters, TestRpc,
                         testing::Values(TCP_IPv4_SSL, TCP_IPv4_NOSSL,
                                         TCP_IPv6_SSL, TCP_IPv6_NOSSL,
                                         UNIX_SSL, UNIX_NOSSL),
                         [] (const testing::TestParamInfo<enum RpcSocketMode>& info) {
                           return ModeEnumToString(info.param);
                         });


TEST_P(TestRpc, TestMessengerCreateDestroy) {
  shared_ptr<Messenger> messenger;
  ASSERT_OK(CreateMessenger("TestCreateDestroy", &messenger, 1, enable_ssl()));
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
    ASSERT_OK(CreateMessenger("TestAcceptorPoolStartStop", &messenger, 1, enable_ssl()));
    shared_ptr<AcceptorPool> pool;
    ASSERT_OK(messenger->AddAcceptorPool(bind_addr(), &pool));
    Sockaddr bound_addr;
    ASSERT_OK(pool->GetBoundAddress(&bound_addr));
    ASSERT_TRUE(bound_addr.is_initialized());
    if (!use_unix_socket()) {
      ASSERT_NE(0, bound_addr.port());
    }
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

  // The deadlock would manifest in cases where the number of concurrent connection
  // requests >= the number of threads. 1 thread and 1 cnxn to ourself is just the easiest
  // way to reproduce the issue, because the server negotiation task must get queued after
  // the client negotiation task if they share the same thread pool.
  MessengerBuilder mb("TestRpc.TestNegotiationDeadlock");
  mb.set_min_negotiation_threads(1)
      .set_max_negotiation_threads(1)
      .set_metric_entity(metric_entity_);
  if (enable_ssl()) mb.enable_inbound_tls();

  shared_ptr<Messenger> messenger;
  ASSERT_OK(mb.Build(&messenger));

  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServerWithCustomMessenger(&server_addr, messenger, enable_ssl()));

  Proxy p(messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());
  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));
}

// Test making successful RPC calls.
TEST_P(TestRpc, TestCall) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());
  ASSERT_STR_CONTAINS(p.ToString(), Substitute("kudu.rpc.GenericCalculatorService@"
                                               "{remote=$0, user_credentials=",
                                               expected_remote_str(server_addr)));

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));
  }
}

// Test for KUDU-2091 and KUDU-2220.
TEST_P(TestRpc, TestCallWithChainCertAndChainCA) {
  // We're only interested in running this test with TLS enabled.
  if (!enable_ssl()) {
    GTEST_SKIP();
  }

  string rpc_certificate_file;
  string rpc_private_key_file;
  string rpc_ca_certificate_file;
  ASSERT_OK(security::CreateTestSSLCertSignedByChain(GetTestDataDirectory(),
                                                     &rpc_certificate_file,
                                                     &rpc_private_key_file,
                                                     &rpc_ca_certificate_file));
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  SCOPED_TRACE(strings::Substitute("Connecting to $0", server_addr.ToString()));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl(),
      rpc_certificate_file, rpc_private_key_file, rpc_ca_certificate_file));

  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());
  ASSERT_STR_CONTAINS(p.ToString(), strings::Substitute("kudu.rpc.GenericCalculatorService@"
                                                            "{remote=$0, user_credentials=",
                                                        expected_remote_str(server_addr)));

  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));
}

// Test for KUDU-2041.
TEST_P(TestRpc, TestCallWithChainCertAndRootCA) {
  // We're only interested in running this test with TLS enabled.
  if (!enable_ssl()) {
    GTEST_SKIP();
  }

  string rpc_certificate_file;
  string rpc_private_key_file;
  string rpc_ca_certificate_file;
  ASSERT_OK(security::CreateTestSSLCertWithChainSignedByRoot(GetTestDataDirectory(),
                                                             &rpc_certificate_file,
                                                             &rpc_private_key_file,
                                                             &rpc_ca_certificate_file));
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  SCOPED_TRACE(strings::Substitute("Connecting to $0", server_addr.ToString()));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl(),
      rpc_certificate_file, rpc_private_key_file, rpc_ca_certificate_file));

  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());
  ASSERT_STR_CONTAINS(p.ToString(), strings::Substitute("kudu.rpc.GenericCalculatorService@"
                                                            "{remote=$0, user_credentials=",
                                                        expected_remote_str(server_addr)));

  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));
}

// Test making successful RPC calls while using a TLS certificate with a password protected
// private key.
TEST_P(TestRpc, TestCallWithPasswordProtectedKey) {
  // We're only interested in running this test with TLS enabled.
  if (!enable_ssl()) {
    GTEST_SKIP();
  }

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
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  SCOPED_TRACE(strings::Substitute("Connecting to $0", server_addr.ToString()));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl(),
      rpc_certificate_file, rpc_private_key_file, rpc_ca_certificate_file,
      rpc_private_key_password_cmd));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());
  ASSERT_STR_CONTAINS(p.ToString(), strings::Substitute("kudu.rpc.GenericCalculatorService@"
                                                            "{remote=$0, user_credentials=",
                                                        expected_remote_str(server_addr)));

  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));
}

// Test that using a TLS certificate with a password protected private key and providing
// the wrong password for that private key, causes a server startup failure.
TEST_P(TestRpc, TestCallWithBadPasswordProtectedKey) {
  // We're only interested in running this test with TLS enabled.
  if (!enable_ssl()) {
    GTEST_SKIP();
  }

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
  Sockaddr server_addr = bind_addr();
  Status s = StartTestServer(&server_addr, enable_ssl(), rpc_certificate_file, rpc_private_key_file,
      rpc_ca_certificate_file, rpc_private_key_password_cmd);
  ASSERT_TRUE(s.IsRuntimeError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "failed to load private key file");
}

// Test that connecting to an invalid server properly throws an error.
TEST_P(TestRpc, TestCallToBadServer) {
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Sockaddr addr = Sockaddr::Wildcard();
  addr.set_port(0);
  Proxy p(client_messenger, addr, addr.host(),
          GenericCalculatorService::static_service_name());

  // Loop a few calls to make sure that we properly set up and tear down
  // the connections.
  for (int i = 0; i < 5; i++) {
    Status s = DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName);
    LOG(INFO) << "Status: " << s.ToString();
    ASSERT_TRUE(s.IsNetworkError()) << "unexpected status: " << s.ToString();
  }
}

// Test that RPC calls can be failed with an error status on the server.
TEST_P(TestRpc, TestInvalidMethodCall) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  // Call the method which fails.
  Status s = DoTestSyncCall(&p, "ThisMethodDoesNotExist");
  ASSERT_TRUE(s.IsRemoteError()) << "unexpected status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "bad method");
}

// Test that the error message returned when connecting to the wrong service
// is reasonable.
TEST_P(TestRpc, TestWrongService) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client with the wrong service name.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, "localhost", "WrongServiceName");

  // Call the method which fails.
  Status s = DoTestSyncCall(&p, "ThisMethodDoesNotExist");
  ASSERT_TRUE(s.IsRemoteError()) << "unexpected status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Service unavailable: service WrongServiceName "
                      "not registered on TestServer");

  // If the server has been marked as having registered all services, we should
  // expect a "not found" error instead.
  server_messenger_->SetServicesRegistered();
  s = DoTestSyncCall(&p, "ThisMethodDoesNotExist");
  ASSERT_TRUE(s.IsRemoteError()) << "unexpected status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Not found: service WrongServiceName "
                      "not registered on TestServer");
}

// Test that we can still make RPC connections even if many fds are in use.
// This is a regression test for KUDU-650.
TEST_P(TestRpc, TestHighFDs) {
  // This test can only run if ulimit is set high.
  const int kNumFakeFiles = 3500;
  const int kMinUlimit = kNumFakeFiles + 100;
  if (env_->GetResourceLimit(Env::ResourceLimitType::OPEN_FILES_PER_PROCESS) < kMinUlimit) {
    GTEST_SKIP() << "Test skipped: must increase ulimit -n to at least " << kMinUlimit;
  }

  // Open a bunch of fds just to increase our fd count.
  vector<RandomAccessFile*> fake_files;
  ElementDeleter d(&fake_files);
  for (int i = 0; i < kNumFakeFiles; i++) {
    unique_ptr<RandomAccessFile> f;
    ASSERT_OK(Env::Default()->NewRandomAccessFile("/dev/zero", &f));
    fake_files.push_back(f.release());
  }

  // Set up server and client, and verify we can make a successful call.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());
  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));
}

// Test that connections are kept alive between calls.
TEST_P(TestRpc, TestConnectionKeepalive) {
  // Only run one reactor per messenger, so we can grab the metrics from that
  // one without having to check all.
  n_server_reactor_threads_ = 1;
  keepalive_time_ms_ = 500;

  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));

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
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));

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
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client with one reactor so that we can grab the metrics from just
  // that reactor.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  // Here we queue a bunch of calls to the server and test that the sender's
  // OutboundTransfer queue is indeed populated with those calls. Unfortunately,
  // we have no surefire way of controlling the queue directly; a fast client
  // reactor thread or a slow main thread could cause all of the outbound calls
  // to be sent before we test the queue size, even though the server can't yet process them.
  //
  // So we repeat the entire exercise until we get a non-zero queue size.
  ASSERT_EVENTUALLY([&]{
    // We'll send several calls asynchronously to force RPC queueing on the sender side.
    constexpr int n_calls = 1000;
    AddRequestPB add_req;
    add_req.set_x(rand());
    add_req.set_y(rand());
    AddResponsePB add_resp;
    string big_string(8 * 1024 * 1024, 'a');

    // Send the calls.
    vector<unique_ptr<RpcController>> controllers;
    CountDownLatch latch(n_calls);
    for (int i = 0; i < n_calls; i++) {
      unique_ptr<RpcController> rpc(new RpcController());
      // Attach a big sidecar so that we are less likely to be able to send the
      // whole RPC in a single write() call without queueing it.
      int junk;
      ASSERT_OK(rpc->AddOutboundSidecar(RpcSidecar::FromSlice(big_string), &junk));
      controllers.emplace_back(std::move(rpc));
      p.AsyncRequest(GenericCalculatorService::kAddMethodName, add_req, &add_resp,
                     controllers.back().get(), [&latch]() { latch.CountDown(); });
    }
    auto cleanup = MakeScopedCleanup([&](){
      latch.Wait();
    });

    // Test the OutboundTransfer queue.
    DumpConnectionsRequestPB dump_req;
    DumpConnectionsResponsePB dump_resp;
    dump_req.set_include_traces(false);
    ASSERT_OK(client_messenger->DumpConnections(dump_req, &dump_resp));
    ASSERT_EQ(1, dump_resp.outbound_connections_size());
    const auto& conn = dump_resp.outbound_connections(0);
    ASSERT_GT(conn.outbound_queue_size(), 0);

#ifdef __linux__
    if (!use_unix_socket()) {
      // Test that the socket statistics are present. We only assert on those that
      // we know to be present on all kernel versions.
      ASSERT_TRUE(conn.has_socket_stats());
      ASSERT_GT(conn.socket_stats().rtt(), 0);
      ASSERT_GT(conn.socket_stats().rttvar(), 0);
      ASSERT_GT(conn.socket_stats().snd_cwnd(), 0);
      ASSERT_GT(conn.socket_stats().send_bytes_per_sec(), 0);
      ASSERT_TRUE(conn.socket_stats().has_send_queue_bytes());
      ASSERT_TRUE(conn.socket_stats().has_receive_queue_bytes());
    }
#endif

    // Unblock all of the calls and wait for them to finish.
    cleanup.run();

    // Verify that all the RPCs have finished.
    for (const auto& controller : controllers) {
      ASSERT_TRUE(controller->finished());
    }
  });
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
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
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
    ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));
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
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
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
  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(1, metrics.total_server_connections_);
  ASSERT_EQ(1, metrics.num_server_connections_);
  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(1, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_EQ(1, metrics.num_client_connections_);

  // This is to allow all the data to be sent so the connection becomes idle.
  SleepFor(MonoDelta::FromMilliseconds(5));

  // Make an RPC call with PRIMARY_CREDENTIALS policy. Currently open connection
  // with ANY_CREDENTIALS policy should be closed and a new one established
  // with PRIMARY_CREDENTIALS policy.
  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName,
                           CredentialsPolicy::PRIMARY_CREDENTIALS));
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(2, metrics.total_server_connections_);
  ASSERT_EQ(1, metrics.num_server_connections_);
  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(2, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_EQ(1, metrics.num_client_connections_);

  // Make another RPC call with ANY_CREDENTIALS policy. The already established
  // connection with PRIMARY_CREDENTIALS policy should be re-used because
  // the ANY_CREDENTIALS policy satisfies the PRIMARY_CREDENTIALS policy which
  // the currently open connection has been established with.
  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(2, metrics.total_server_connections_);
  ASSERT_EQ(1, metrics.num_server_connections_);
  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(2, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_EQ(1, metrics.num_client_connections_);
}

// Test that proxies with different network planes will open separate connections to server.
TEST_P(TestRpc, TestConnectionNetworkPlane) {
  // Only run one reactor per messenger, so we can grab the metrics from that
  // one without having to check all.
  n_server_reactor_threads_ = 1;

  // Keep the connection alive all the time.
  keepalive_time_ms_ = -1;

  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up clients with default and non-default network planes.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p1(client_messenger, server_addr, kRemoteHostName,
           GenericCalculatorService::static_service_name());
  Proxy p2(client_messenger, server_addr, kRemoteHostName,
           GenericCalculatorService::static_service_name());
  p2.set_network_plane("control-channel");

  // Verify the initial counters.
  ReactorMetrics metrics;
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_EQ(0, metrics.num_server_connections_);
  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_EQ(0, metrics.num_client_connections_);

  // Make an RPC call with the default network plane.
  ASSERT_OK(DoTestSyncCall(&p1, GenericCalculatorService::kAddMethodName));
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(1, metrics.total_server_connections_);
  ASSERT_EQ(1, metrics.num_server_connections_);
  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(1, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_EQ(1, metrics.num_client_connections_);

  // Make an RPC call with the non-default network plane.
  ASSERT_OK(DoTestSyncCall(&p2, GenericCalculatorService::kAddMethodName));
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(2, metrics.total_server_connections_);
  ASSERT_EQ(2, metrics.num_server_connections_);
  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(2, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_EQ(2, metrics.num_client_connections_);

  // Make an RPC call with the default network plane again and verify that
  // there are no new connections.
  ASSERT_OK(DoTestSyncCall(&p1, GenericCalculatorService::kAddMethodName));
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.total_client_connections_);
  ASSERT_EQ(2, metrics.total_server_connections_);
  ASSERT_EQ(2, metrics.num_server_connections_);
  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(2, metrics.total_client_connections_);
  ASSERT_EQ(0, metrics.total_server_connections_);
  ASSERT_EQ(2, metrics.num_client_connections_);
}

// Test that a call which takes longer than the keepalive time
// succeeds -- i.e that we don't consider a connection to be "idle" on the
// server if there is a call outstanding on it.
TEST_P(TestRpc, TestCallLongerThanKeepalive) {
  // Set a short keepalive.
  keepalive_time_ms_ = 1000;

  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
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

// Test a call which leaves the TCP connection idle for extended period of time
// and verifies that the call succeeds (i.e. the connection is not closed).
TEST_P(TestRpc, TestTCPKeepalive) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  FLAGS_tcp_keepalive_probe_period_s = 1;
  FLAGS_tcp_keepalive_retry_period_s = 1;
  FLAGS_tcp_keepalive_retry_count = 1;
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
      GenericCalculatorService::static_service_name());

  // Make a call which sleeps for longer than TCP keepalive probe period,
  // triggering TCP keepalive probes.
  RpcController controller;
  SleepRequestPB req;
  req.set_sleep_micros(8 * 1000 * 1000); // 8 seconds.
  req.set_deferred(true);
  SleepResponsePB resp;
  ASSERT_OK(p.SyncRequest(GenericCalculatorService::kSleepMethodName,
      req, &resp, &controller));
}

// Test that the RpcSidecar transfers the messages within RPC max message
// size limit and errors out when limit is crossed.
TEST_P(TestRpc, TestRpcSidecarWithSizeLimits) {
  int64_t rpc_message_size = 30 * 1024 * 1024; // 30 MB
  map<std::pair<int64_t, int64_t>, string> rpc_max_message_server_and_client;

  // 1. Set the rpc max size to:
  // Server: 50 MB,
  // Client: 70 MB,
  // so that client is able to accommodate the response size of 60 MB.
  EmplaceIfNotPresent(&rpc_max_message_server_and_client,
                      std::make_pair((50 * 1024 * 1024), (70 * 1024 * 1024)),
                      "OK");

  // 2. Set the rpc max size to:
  // Server: 50 MB,
  // Client: 20 MB,
  // so that client rejects the inbound message of size 60 MB.
  EmplaceIfNotPresent(&rpc_max_message_server_and_client,
                      std::make_pair((50 * 1024 * 1024), (20 * 1024 * 1024)),
                      "Network error: RPC frame had a length of");

  for (auto const& rpc_max_message_size : rpc_max_message_server_and_client) {
    // Set rpc_max_message_size.
    int64_t server_rpc_max_size = rpc_max_message_size.first.first;
    int64_t client_rpc_max_size = rpc_max_message_size.first.second;

    // Set up server.
    Sockaddr server_addr = bind_addr();

    MessengerBuilder mb("TestRpc.TestRpcSidecarWithSizeLimits");
    mb.set_rpc_max_message_size(server_rpc_max_size)
      .set_metric_entity(metric_entity_);
    if (enable_ssl()) mb.enable_inbound_tls();

    shared_ptr<Messenger> messenger;
    ASSERT_OK(mb.Build(&messenger));

    ASSERT_OK(StartTestServerWithCustomMessenger(&server_addr, messenger, enable_ssl()));

    // Set up client.
    shared_ptr<Messenger> client_messenger;
    ASSERT_OK(CreateMessenger("Client", &client_messenger,
                              1, enable_ssl(), "", "", "", "", client_rpc_max_size));
    Proxy p(client_messenger, server_addr, kRemoteHostName,
            GenericCalculatorService::static_service_name());

    Status status = DoTestSidecarWithSizeLimits(&p, rpc_message_size, rpc_message_size);

    // OK: If size of payload is within max rpc message size limit.
    // Close connection: If size of payload is beyond max message size limit.
    ASSERT_STR_CONTAINS(status.ToString(), rpc_max_message_size.second);
  }
}

// Test that the RpcSidecar transfers the expected messages.
TEST_P(TestRpc, TestRpcSidecar) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  // Test a zero-length sidecar
  DoTestSidecar(&p, 0, 0);

  // Test some small sidecars
  DoTestSidecar(&p, 123, 456);

  // Test some larger sidecars to verify that we properly handle the case where
  // we can't write the whole response to the socket in a single call.
  DoTestSidecar(&p, 3000 * 1024, 2000 * 1024);

  DoTestOutgoingSidecarExpectOK(&p, 0, 0);
  DoTestOutgoingSidecarExpectOK(&p, 123, 456);
  DoTestOutgoingSidecarExpectOK(&p, 3000 * 1024, 2000 * 1024);
}

// Test sending the maximum number of sidecars, each of them being a single
// character. This makes sure we handle the limit of IOV_MAX iovecs per sendmsg
// call.
TEST_P(TestRpc, TestMaxSmallSidecars) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  Random rng(GetRandomSeed32());
  vector<string> strings(TransferLimits::kMaxSidecars);
  for (auto& s : strings) {
    s = RandomString(2, &rng);
  }
  ASSERT_OK(DoTestOutgoingSidecar(&p, strings));
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

  // Construct a string to use as a maximal payload in following tests
  string max_string(TransferLimits::kMaxTotalSidecarBytes, 'a');

  {
    // Test that limit on the total size of sidecars is respected. The maximal payload
    // reaches the limit exactly.
    RpcController controller;
    int idx;
    ASSERT_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(Slice(max_string)), &idx));

    // Trying to add another byte will fail.
    int dummy = 0;
    string s2(1, 'b');
    Status max_sidecar_status =
        controller.AddOutboundSidecar(RpcSidecar::FromSlice(Slice(s2)), &dummy);
    ASSERT_FALSE(max_sidecar_status.ok());
    ASSERT_STR_MATCHES(max_sidecar_status.ToString(), "Total size of sidecars");
  }

  // Test two cases:
  // 1) The RPC has maximal size and exceeds rpc_max_message_size. This tests the
  //    functionality of rpc_max_message_size. The server will close the connection
  //    immediately.
  // 2) The RPC has maximal size, but rpc_max_message_size has been set to a higher
  //    value. This tests the client's ability to send the maximal message.
  //    The server will reject the message after it has been transferred.
  //    This test is disabled for TSAN due to high memory requirements.
  vector<int64_t> rpc_max_message_values;
  rpc_max_message_values.push_back(FLAGS_rpc_max_message_size);
#ifndef THREAD_SANITIZER
  rpc_max_message_values.push_back(std::numeric_limits<int64_t>::max());
#endif
  for (int64_t rpc_max_message_size_val : rpc_max_message_values) {
    // Set rpc_max_message_size
    FLAGS_rpc_max_message_size = rpc_max_message_size_val;

    // Set up server.
    Sockaddr server_addr = bind_addr();

    ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

    // Set up client.
    shared_ptr<Messenger> client_messenger;
    ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
    Proxy p(client_messenger, server_addr, kRemoteHostName,
            GenericCalculatorService::static_service_name());

    RpcController controller;
    // KUDU-2305: Test with a maximal payload to verify that the implementation
    // can handle the limits.
    int idx;
    ASSERT_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(Slice(max_string)), &idx));

    PushStringsRequestPB request;
    request.add_sidecar_indexes(idx);
    PushStringsResponsePB resp;
    Status status = p.SyncRequest(GenericCalculatorService::kPushStringsMethodName,
        request, &resp, &controller);
    ASSERT_TRUE(status.IsNetworkError()) << "Unexpected error: " << status.ToString();
    // Remote responds to extra-large payloads by closing the connection.
    ASSERT_STR_MATCHES(status.ToString(),
                       // Linux
                       "Connection reset by peer"
                       // Linux domain socket
                       "|Broken pipe"
                       // While reading from socket.
                       "|recv got EOF from"
                       // Linux, SSL enabled
                       "|failed to read from TLS socket"
                       // macOS, while writing to socket.
                       "|Protocol wrong type for socket"
                       // macOS, sendmsg(): the sum of the iov_len values overflows an ssize_t
                       "|sendmsg error: Invalid argument");
  }
}

// Test that timeouts are properly handled.
TEST_P(TestRpc, TestCallTimeout) {
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  // Test a very short timeout - we expect this will time out while the
  // call is still trying to connect, or in the send queue. This was triggering ASAN failures
  // before.
  NO_FATALS(DoTestExpectTimeout(&p, MonoDelta::FromNanoseconds(1)));

  // Test a longer timeout - expect this will time out after we send the request,
  // but shorter than our threshold for two-stage timeout handling.
  NO_FATALS(DoTestExpectTimeout(&p, MonoDelta::FromMilliseconds(200)));

  // Test a longer timeout - expect this will trigger the "two-stage timeout"
  // code path.
  NO_FATALS(DoTestExpectTimeout(&p, MonoDelta::FromMilliseconds(1500)));
}

// Inject 500ms delay in negotiation, and send a call with a short timeout, followed by
// one with a long timeout. The call with the long timeout should succeed even though
// the previous one failed.
//
// This is a regression test against prior behavior where the connection negotiation
// was assigned the timeout of the first call on that connection. So, if the first
// call had a short timeout, the later call would also inherit the timed-out negotiation.
TEST_P(TestRpc, TestCallTimeoutDoesntAffectNegotiation) {
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  FLAGS_rpc_negotiation_inject_delay_ms = 500;
  NO_FATALS(DoTestExpectTimeout(&p, MonoDelta::FromMilliseconds(50)));
  ASSERT_OK(DoTestSyncCall(&p, GenericCalculatorService::kAddMethodName));

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

// Basic test for methods_by_name(). At the time of writing, this isn't used by
// Kudu, but is used in other projects like Apache Impala.
TEST_F(TestRpc, TestMethodsByName) {
  std::unique_ptr<CalculatorService> service(
      new CalculatorService(metric_entity_, result_tracker_));
  const auto& methods = service->methods_by_name();
  ASSERT_EQ(8, methods.size());
}

// Starts a fake listening socket which never actually negotiates.
// Ensures that the client gets a reasonable status code in this case.
TEST_F(TestRpc, TestNegotiationTimeout) {
  // Set up a simple socket server which accepts a connection.
  Sockaddr server_addr = Sockaddr::Wildcard();
  Socket listen_sock;
  ASSERT_OK(StartFakeServer(&listen_sock, &server_addr));

  // Create another thread to accept the connection on the fake server.
  thread acceptor_thread([&listen_sock]() { AcceptAndReadForever(&listen_sock); });
  SCOPED_CLEANUP({ acceptor_thread.join(); });

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  bool is_negotiation_error = false;
  NO_FATALS(DoTestExpectTimeout(
      &p, MonoDelta::FromMilliseconds(100), false, &is_negotiation_error));
  EXPECT_TRUE(is_negotiation_error);
}

// Test that client calls get failed properly when the server they're connected to
// shuts down.
TEST_P(TestRpc, TestServerShutsDown) {
  // Set up a simple socket server which accepts a connection.
  Sockaddr server_addr = bind_addr();
  Socket listen_sock;
  ASSERT_OK(StartFakeServer(&listen_sock, &server_addr));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
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
                   [&latch]() { latch.CountDown(); });
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
    //
    // ENOTCONN is possible simply because the server closes the connection
    // after the connection is established.
    ASSERT_TRUE(s.posix_code() == EPIPE ||
                s.posix_code() == ECONNRESET ||
                s.posix_code() == ESHUTDOWN ||
                s.posix_code() == ECONNREFUSED ||
                s.posix_code() == EINVAL ||
                s.posix_code() == ENOTCONN)
      << "Unexpected status: " << s.ToString();
  }
}

// Test handler latency metric.
TEST_P(TestRpc, TestRpcHandlerLatencyMetric) {
  constexpr uint64_t sleep_micros = 20 * 1000;

  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl()));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          CalculatorService::static_service_name());

  RpcController controller;
  SleepRequestPB req;
  req.set_sleep_micros(sleep_micros);
  req.set_deferred(true);
  SleepResponsePB resp;
  ASSERT_OK(p.SyncRequest("Sleep", req, &resp, &controller));

  const unordered_map<const MetricPrototype*, scoped_refptr<Metric> > metric_map =
    server_messenger_->metric_entity()->UnsafeMetricsMapForTests();

  scoped_refptr<Histogram> latency_histogram = down_cast<Histogram*>(
      FindOrDie(metric_map,
                &METRIC_handler_latency_kudu_rpc_test_CalculatorService_Sleep).get());
  scoped_refptr<Counter> queue_overflow_rejections = down_cast<Counter*>(
      FindOrDie(metric_map,
                &METRIC_queue_overflow_rejections_kudu_rpc_test_CalculatorService_Sleep).get());

  LOG(INFO) << "Sleep() min lat: " << latency_histogram->MinValueForTests();
  LOG(INFO) << "Sleep() mean lat: " << latency_histogram->MeanValueForTests();
  LOG(INFO) << "Sleep() max lat: " << latency_histogram->MaxValueForTests();
  LOG(INFO) << "Sleep() #calls: " << latency_histogram->TotalCount();

  ASSERT_EQ(1, latency_histogram->TotalCount());
  ASSERT_EQ(0, queue_overflow_rejections->value());
  ASSERT_GE(latency_histogram->MaxValueForTests(), sleep_micros);
  ASSERT_TRUE(latency_histogram->MinValueForTests() == latency_histogram->MaxValueForTests());

  // TODO: Implement an incoming queue latency test.
  // For now we just assert that the metric exists.
  ASSERT_TRUE(FindOrDie(metric_map, &METRIC_rpc_incoming_queue_time));
}

// Set of basic test scenarios for the per-RPC 'timed_out_on_response' metric.
TEST_P(TestRpc, TimedOutOnResponseMetric) {
  constexpr uint64_t kSleepMicros = 50 * 1000;
  const string kMethodName = "Sleep";

  // Set RPC connection negotiation timeout to be very high to avoid flakiness
  // if name resolution is very slow.
  rpc_negotiation_timeout_ms_ = 60 * 1000;

  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl()));

  shared_ptr<Messenger> cm;
  ASSERT_OK(CreateMessenger("client", &cm, 1/*n_reactors*/, enable_ssl()));
  Proxy p(cm, server_addr, kRemoteHostName, CalculatorService::static_service_name());

  // Get references to the metrics map and a couple of relevant metrics.
  const auto& mm = server_messenger_->metric_entity()->UnsafeMetricsMapForTests();
  const auto* latency_histogram = down_cast<Histogram*>(FindOrDie(
      mm, &METRIC_handler_latency_kudu_rpc_test_CalculatorService_Sleep).get());
  const auto* timed_out_on_response = down_cast<Counter*>(FindOrDie(
      mm, &METRIC_timed_out_on_response_kudu_rpc_test_CalculatorService_Sleep).get());
  const auto* timed_out_in_queue = service_pool_->RpcsTimedOutInQueueMetricForTests();

  ASSERT_EQ(0, latency_histogram->TotalCount());
  ASSERT_EQ(0, timed_out_on_response->value());
  ASSERT_EQ(0, timed_out_in_queue->value());

  // Make a dry-run call to avoid flakiness in this test scenario if name
  // resolution is slow. This primes the DNS resolver and its cache.
  {
    SleepRequestPB req;
    req.set_sleep_micros(kSleepMicros);
    SleepResponsePB resp;
    RpcController ctl;
    ASSERT_OK(p.SyncRequest(kMethodName, req, &resp, &ctl));
    ASSERT_EQ(1, latency_histogram->TotalCount());
    ASSERT_EQ(0, timed_out_on_response->value());
  }

  // Run the sequence of sub-scenarios where the requests successfully complete
  // at the server side, but the client might mark them as timed out.
  SleepRequestPB req;
  req.set_sleep_micros(kSleepMicros);

  // Client side doesn't set the timeout for the RPC at all.
  {
    RpcController ctl;
    SleepResponsePB resp;
    ASSERT_OK(p.SyncRequest(kMethodName, req, &resp, &ctl));
    ASSERT_EQ(2, latency_histogram->TotalCount());
    ASSERT_EQ(0, timed_out_on_response->value());
  }

  // Set the timeout for the RPC much higher than the request would take
  // to process (assuming scheduler anomalies does not spill over 30 seconds).
  {
    RpcController ctl;
    ctl.set_timeout(MonoDelta::FromSeconds(30));
    SleepResponsePB resp;
    ASSERT_OK(p.SyncRequest(kMethodName, req, &resp, &ctl));
    ASSERT_EQ(3, latency_histogram->TotalCount());
    ASSERT_EQ(0, timed_out_on_response->value());
  }

  // Set the timeout for the RPC very low to make sure the request times out
  // because of the explicitly requested sleep interval but keep it high enough
  // to avoid timing out the request in the RPC queue if a scheduler anomaly
  // hapens.
  {
    RpcController ctl;
    ctl.set_timeout(MonoDelta::FromMicroseconds(kSleepMicros / 2));
    SleepResponsePB resp;
    const auto s = p.SyncRequest(kMethodName, req, &resp, &ctl);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Timed out: Sleep RPC");
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(4, latency_histogram->TotalCount());
    });
    ASSERT_EQ(1, timed_out_on_response->value());
  }

  // Set the timeout for the RPC to be close to the time it takes to process
  // the request.
  {
    RpcController ctl;
    ctl.set_timeout(MonoDelta::FromMicroseconds(kSleepMicros));
    SleepResponsePB resp;
    const auto s = p.SyncRequest(kMethodName, req, &resp, &ctl);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "Timed out: Sleep RPC");
    ASSERT_EVENTUALLY([&] {
      ASSERT_EQ(5, latency_histogram->TotalCount());
    });
    ASSERT_EQ(2, timed_out_on_response->value());
  }

  // Not a single RPC times out in the service queue -- all the RPCs have been
  // successfully processed by the server, just a couple of them were responded
  // after the client-defined deadline.
  ASSERT_EQ(0, timed_out_in_queue->value());
}

// A special scenario for the per-RPC 'timed_out_on_response' metric when an
// RPC times out while waiting in the queue, so it's not actually processed.
TEST_P(TestRpc, TimedOutOnResponseMetricServiceQueue) {
  constexpr uint64_t kSleepMicros = 200 * 1000;
  const string kMethodName = "Sleep";

  // Set RPC connection negotiation timeout to be very high to avoid flakiness
  // if name resolution is very slow.
  rpc_negotiation_timeout_ms_ = 60 * 1000;

  // Limit the capacity of the service's thread pool, so requests are processed
  // sequentially by a single worker thread.
  n_worker_threads_ = 1;

  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl()));

  shared_ptr<Messenger> cm;
  ASSERT_OK(CreateMessenger("client", &cm, 1/*n_reactors*/, enable_ssl()));
  Proxy p(cm, server_addr, kRemoteHostName, CalculatorService::static_service_name());

  // Get the reference to the metrics map and create handles to the needed metrics.
  const auto& mm = server_messenger_->metric_entity()->UnsafeMetricsMapForTests();

  const auto* latency_histogram = down_cast<Histogram*>(FindOrDie(
      mm, &METRIC_handler_latency_kudu_rpc_test_CalculatorService_Sleep).get());
  const auto* timed_out_on_response = down_cast<Counter*>(FindOrDie(
      mm, &METRIC_timed_out_on_response_kudu_rpc_test_CalculatorService_Sleep).get());
  const auto* timed_out_in_queue = service_pool_->RpcsTimedOutInQueueMetricForTests();

  // In the beginning, all the related metrics should be zeroed.
  ASSERT_EQ(0, latency_histogram->TotalCount());
  ASSERT_EQ(0, timed_out_on_response->value());
  ASSERT_EQ(0, timed_out_in_queue->value());

  // Make a dry-run call to avoid flakiness in this test scenario if name
  // resolution is slow. This primes the DNS resolver and its cache.
  {
    SleepRequestPB req;
    req.set_sleep_micros(kSleepMicros);
    SleepResponsePB resp;
    RpcController ctl;
    ASSERT_OK(p.SyncRequest(kMethodName, req, &resp, &ctl));
  }
  ASSERT_EQ(1, latency_histogram->TotalCount());
  ASSERT_EQ(0, timed_out_on_response->value());
  ASSERT_EQ(0, timed_out_in_queue->value());

  CountDownLatch latch(2);

  // The first RPC should be successful: sleep for the specified time.
  SleepRequestPB req0;
  req0.set_sleep_micros(kSleepMicros);
  SleepResponsePB resp0;
  RpcController ctl0;
  p.AsyncRequest(kMethodName, req0, &resp0, &ctl0,
                 [&latch]() { latch.CountDown(); });

  // The second RPC should wait in the RPC queue while the first is being
  // processed by the only thread in the RPC service thread pool.  Eventually,
  // it should time out.
  SleepRequestPB req1;
  req1.set_sleep_micros(0);
  req1.set_return_app_error(true);
  SleepResponsePB resp1;
  RpcController ctl1;
  // Add an extra margin for the timeout setting to avoid flakiness
  // due to scheduler anomalies and off-by-one differences in timestamps.
  ctl1.set_timeout(MonoDelta::FromMicroseconds(kSleepMicros / 2));
  p.AsyncRequest(kMethodName, req1, &resp1, &ctl1,
                 [&latch]() { latch.CountDown(); });

  // Wait for the completion of both requests sent asynchronously above.
  latch.Wait();

  // The Histogram::TotalCount() metric is read in lock-free/no-barrier manner,
  // so ASSERT_EVENTUALLY helps in very rare cases when TotalCount() reads
  // something less than 3 in the very first pass.
  ASSERT_EVENTUALLY([&]{
    // There were three requests in total: the warm-up one and req0, req1.
    ASSERT_EQ(3, latency_histogram->TotalCount());
  });

  // The first RPC should return OK.
  ASSERT_OK(ctl0.status());

  // The second RPC should time out while waiting in the queue
  // and the corresponding metrics should be incremented.
  const auto& s = ctl1.status();
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Timed out: Sleep RPC");
  ASSERT_EQ(1, timed_out_on_response->value());
  ASSERT_EQ(1, timed_out_in_queue->value());
}

// Basic verification for the numbers reported by 'acceptor_dispatch_times'.
TEST_P(TestRpc, AcceptorDispatchingTimesMetric) {
  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  {
    Socket socket;
    ASSERT_OK(socket.Init(server_addr.family(), /*flags=*/0));
    ASSERT_OK(socket.Connect(server_addr));
  }

  scoped_refptr<Histogram> dispatch_times =
      METRIC_acceptor_dispatch_times.Instantiate(server_messenger_->metric_entity());
  // Using ASSERT_EVENTUALLY below because of relaxed memory ordering when
  // fetching metrics' values. Eventually, metrics reports readings that are
  // consistent with the expected numbers.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, dispatch_times->TotalCount());
    ASSERT_GT(dispatch_times->MaxValueForTests(), 0);
  });
}

// Basic verification of the 'rpc_pending_connections' metric.
// The number of pending connections is properly reported on Linux; on other
// platforms that don't support sock_diag() netlink facility (e.g., macOS)
// the metric should report -1.
TEST_P(TestRpc, RpcPendingConnectionsMetric) {
  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  // Get the reference to already registered metric with the proper callback
  // to fetch the necessary information. The { 'return -3'; } fake callback
  // is to make sure the actual gauge returns a proper value,
  // which is verified below.
  auto pending_connections_gauge =
      METRIC_rpc_pending_connections.InstantiateFunctionGauge(
          server_messenger_->metric_entity(), []() { return -3; });

  // No connection attempts have been made yet.
#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
  ASSERT_EQ(0, pending_connections_gauge->value());
#else
  ASSERT_EQ(-3, pending_connections_gauge->value());
#endif // #if defined(KUDU_HAS_DIAGNOSTIC_SOCKET) ...

  {
    Socket socket;
    ASSERT_OK(socket.Init(server_addr.family(), /*flags=*/0));
    ASSERT_OK(socket.Connect(server_addr));
  }

  // At this point, there should be no connection pending: the only received
  // connection request has already been handled above.
#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
  // It's not clear why the sock_diag() netlink facility sometimes reports stale
  // data for a short period of time. That's rare, but it happens. Emprically,
  // it's about 10 to 20 milliseconds when data might be still stale.
  // AssertEventually helps in preventing flakiness of this scenario
  // in such rare cases.
  AssertEventually([&] {
    ASSERT_EQ(0, pending_connections_gauge->value());
  }, MonoDelta::FromMilliseconds(250));
#else
  ASSERT_EQ(-3, pending_connections_gauge->value());
#endif // #if defined(KUDU_HAS_DIAGNOSTIC_SOCKET) ...
}

static void DestroyMessengerCallback(shared_ptr<Messenger>* messenger,
                                     CountDownLatch* latch) {
  messenger->reset();
  latch->CountDown();
}

TEST_P(TestRpc, TestRpcCallbackDestroysMessenger) {
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Sockaddr bad_addr = Sockaddr::Wildcard();
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
                   [&client_messenger, &latch]() {
                     DestroyMessengerCallback(&client_messenger, &latch);
                   });
  }
  latch.Wait();
}

// Test that setting the client timeout / deadline gets propagated to RPC
// services.
TEST_P(TestRpc, TestRpcContextClientDeadline) {
  const uint64_t sleep_micros = 20 * 1000;

  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl()));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          CalculatorService::static_service_name());

  SleepRequestPB req;
  req.set_sleep_micros(sleep_micros);
  req.set_client_timeout_defined(true);
  SleepResponsePB resp;
  RpcController controller;
  Status s = p.SyncRequest("Sleep", req, &resp, &controller);
  ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Missing required timeout");

  controller.Reset();
  controller.set_timeout(MonoDelta::FromMilliseconds(1000));
  ASSERT_OK(p.SyncRequest("Sleep", req, &resp, &controller));
}

// Test that setting an call-level application feature flag to an unknown value
// will make the server reject the call.
TEST_P(TestRpc, TestApplicationFeatureFlag) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl()));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
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
    ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  }
}

TEST_P(TestRpc, TestApplicationFeatureFlagUnsupportedServer) {
  auto savedFlags = kSupportedServerRpcFeatureFlags;
  SCOPED_CLEANUP({ kSupportedServerRpcFeatureFlags = savedFlags; });
  kSupportedServerRpcFeatureFlags = {};

  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, enable_ssl()));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
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
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
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
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  int timeout_ms = 10;
  for (int i = OutboundCall::READY; i <= OutboundCall::FINISHED_SUCCESS; ++i) {
    FLAGS_rpc_inject_cancellation_state = i;
    switch (i) {
      case OutboundCall::READY:
      case OutboundCall::ON_OUTBOUND_QUEUE:
      case OutboundCall::SENDING:
      case OutboundCall::SENT:
        ASSERT_TRUE(DoTestOutgoingSidecar(&p, 0, 0).IsAborted());
        ASSERT_TRUE(DoTestOutgoingSidecar(&p, 123, 456).IsAborted());
        ASSERT_TRUE(DoTestOutgoingSidecar(&p, 3000 * 1024, 2000 * 1024).IsAborted());
        DoTestExpectTimeout(&p, MonoDelta::FromMilliseconds(timeout_ms), true);
        break;
      case OutboundCall::NEGOTIATION_TIMED_OUT:
      case OutboundCall::TIMED_OUT:
        DoTestExpectTimeout(&p, MonoDelta::FromMilliseconds(1000));
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
        ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
        break;
      }
      case OutboundCall::FINISHED_SUCCESS:
        DoTestOutgoingSidecarExpectOK(&p, 0, 0);
        DoTestOutgoingSidecarExpectOK(&p, 123, 456);
        DoTestOutgoingSidecarExpectOK(&p, 3000 * 1024, 2000 * 1024);
        break;
    }
  }
  // Sleep briefly to ensure the timeout tests have a chance for the timeouts to trigger.
  SleepFor(MonoDelta::FromMilliseconds(timeout_ms * 2));
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

// Test to verify that sidecars aren't corrupted when cancelling an async RPC.
TEST_P(TestRpc, TestCancellationAsync) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  RpcController controller;

  // The payload to be used during the RPC.
  unique_ptr<uint8_t[]> payload(new uint8_t[TEST_PAYLOAD_SIZE]);

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
    ASSERT_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(s), &idx));
    req.set_sidecar_idx(idx);

    CountDownLatch latch(1);
    auto* payload_raw = payload.get();
    p.AsyncRequest(GenericCalculatorService::kSleepWithSidecarMethodName,
                   req, &resp, &controller,
                   [payload_raw, &latch]() { SleepCallback(payload_raw, &latch); });
    // Sleep for a while before cancelling the RPC.
    if (i > 0) SleepFor(MonoDelta::FromMicroseconds(rand.Uniform64(i * 30)));
    controller.Cancel();
    latch.Wait();
    ASSERT_TRUE(controller.status().IsAborted() || controller.status().ok());
    controller.Reset();
  }
  client_messenger->Shutdown();
}

// This function loops for 40 iterations and for each iteration, sends an async RPC
// and sleeps for some time between 1 to 100 microseconds before cancelling the RPC.
// This serves as a helper function for TestCancellationMultiThreads() to exercise
// cancellation when there are concurrent RPCs.
static void SendAndCancelRpcs(Proxy* p, const Slice& slice) {
  RpcController controller;

  // Used to generate sleep time between invoking RPC and requesting cancellation.
  Random rand(SeedRandom());

  auto end_time = MonoTime::Now() + MonoDelta::FromSeconds(
    AllowSlowTests() ? 15 : 3);

  int i = 0;
  while (MonoTime::Now() < end_time) {
    controller.Reset();
    PushStringsRequestPB request;
    PushStringsResponsePB resp;
    int idx;
    CHECK_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(slice), &idx));
    request.add_sidecar_indexes(idx);
    CHECK_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(slice), &idx));
    request.add_sidecar_indexes(idx);

    CountDownLatch latch(1);
    p->AsyncRequest(GenericCalculatorService::kPushStringsMethodName,
                    request, &resp, &controller,
                    [&latch]() { latch.CountDown(); });

    if ((i++ % 8) != 0) {
      // Sleep for a while before cancelling the RPC.
      SleepFor(MonoDelta::FromMicroseconds(rand.Uniform64(100)));
      controller.Cancel();
    }
    latch.Wait();
    CHECK(controller.status().IsAborted() || controller.status().IsServiceUnavailable() ||
          controller.status().ok()) << controller.status().ToString();
  }
}

// Test to exercise cancellation when there are multiple concurrent RPCs from the
// same client to the same server.
TEST_P(TestRpc, TestCancellationMultiThreads) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  // Buffer used for sidecars by SendAndCancelRpcs().
  string buf(16 * 1024 * 1024, 'a');
  Slice slice(buf);

  // Start a bunch of threads which invoke async RPC and cancellation.
  constexpr int kNumThreads = 30;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&p, slice]() { SendAndCancelRpcs(&p, slice); });
  }
  // Wait for all threads to complete.
  for (auto& t : threads) {
    t.join();
  }
  client_messenger->Shutdown();
}


// Test performance of Ipv4 vs unix sockets.
TEST_P(TestRpc, TestPerformanceBySocketType) {
  static constexpr int kNumMb = 1024;
  static constexpr int kMbPerRpc = 4;
  static_assert(kNumMb % kMbPerRpc == 0, "total should be a multiple of per-RPC");

  const vector<string> sidecars = { string(kMbPerRpc * 1024 * 1024, 'x') };

  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  for (int i = 0; i < 5; i++) {
    Stopwatch sw(Stopwatch::ALL_THREADS);
    sw.start();
    for (int i = 0; i < kNumMb / kMbPerRpc; i++) {
      ASSERT_OK(DoTestOutgoingSidecar(&p, sidecars));
    }
    sw.stop();
    LOG(INFO) << strings::Substitute(
        "Sending $0MB via $1$2 socket: $3",
        kNumMb,
        enable_ssl() ? "ssl-enabled " : "",
        use_unix_socket() ? "unix" : "tcp",
        sw.elapsed().ToString());
  }
}

// Test that call_id is returned in call response and accessible through RpcController.
TEST_P(TestRpc, TestCallId) {
  // Set up server.
  Sockaddr server_addr = bind_addr();
  ASSERT_OK(StartTestServer(&server_addr, enable_ssl()));

  // Set up client.
  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger, 1, enable_ssl()));
  Proxy p(client_messenger, server_addr, kRemoteHostName,
          GenericCalculatorService::static_service_name());

  for (int i = 0; i < 10; i++) {
    AddRequestPB req;
    req.set_x(rand());
    req.set_y(rand());
    RpcController controller;
    controller.set_timeout(MonoDelta::FromMilliseconds(10000));

    AddResponsePB resp;
    ASSERT_OK(p.SyncRequest(GenericCalculatorService::kAddMethodName,
      req, &resp, &controller));
    ASSERT_EQ(req.x() + req.y(), resp.result());
    ASSERT_EQ(i, controller.call_id());
  }
}

#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
// A test to verify collecting information on the RX queue size of a listening
// socket using the DiagnosticSocket wrapper.
class TestRpcSocketTxRxQueue : public TestRpc {
 protected:
  TestRpcSocketTxRxQueue() = default;

  Status RunAndGetSocketInfo(int listen_backlog,
                             size_t num_clients,
                             DiagnosticSocket::TcpSocketInfo* info) {
    // Limit the backlog for the socket being listened to.
    Sockaddr s_addr = bind_addr();
    Socket s_sock;
    RETURN_NOT_OK(StartFakeServer(&s_sock, &s_addr, listen_backlog));

    vector<shared_ptr<Messenger>> c_messengers(num_clients);
    vector<unique_ptr<Proxy>> proxies(num_clients);
    vector<AddRequestPB> requests(num_clients);
    vector<AddResponsePB> responses(num_clients);
    vector<RpcController> ctls(num_clients);

    for (auto i = 0; i < num_clients; ++i) {
      RETURN_NOT_OK(CreateMessenger("client" + std::to_string(i), &c_messengers[i]));
      proxies[i].reset(new Proxy(c_messengers[i],
                                 s_addr,
                                 kRemoteHostName,
                                 GenericCalculatorService::static_service_name()));
      requests[i].set_x(2 * i);
      requests[i].set_y(2 * i + 1);
      proxies[i]->AsyncRequest(GenericCalculatorService::kAddMethodName,
                               requests[i],
                               &responses[i],
                               &ctls[i],
                               []() {});
    }

    // Let the messengers to send connect() requests.
    // TODO(aserbin): find a more reliable way to track this.
    SleepFor(MonoDelta::FromMilliseconds(250));

    DiagnosticSocket ds;
    RETURN_NOT_OK(ds.Init());

    DiagnosticSocket::TcpSocketInfo result;
    RETURN_NOT_OK(ds.Query(s_sock, &result));
    *info = result;

    // Close the socket explicitly to allow the connecting clients receiving
    // RST on the connection to end up the connection negotiation attempts fast.
    return s_sock.Close();
  }
};
// All the tests run without SSL on TCP sockets: TestRpcSocketTxRxQueue inherits
// from TestRpc, and the latter is parameterized. Running with SSL doesn't make
// much sense since it's the same in this context: all the action happens
// at the TCP level, and RPC connection negotiation doesn't happen.
INSTANTIATE_TEST_SUITE_P(Parameters, TestRpcSocketTxRxQueue,
                         testing::Values(TCP_IPv4_NOSSL,
                                         TCP_IPv6_NOSSL),
                         [] (const testing::TestParamInfo<enum RpcSocketMode>& info) {
                           return ModeEnumToString(info.param);
                         });

// This test scenario verifies the reported socket's stats when it's more than
// enough space in the listening socket's RX queue to accommodate all the
// incoming requests.
TEST_P(TestRpcSocketTxRxQueue, UnderCapacity) {
  constexpr int kListenBacklog = 16;
  constexpr size_t kClientsNum = 5;

  DiagnosticSocket::TcpSocketInfo info;
  ASSERT_OK(RunAndGetSocketInfo(kListenBacklog, kClientsNum, &info));

  // Since the fake server isn't handling incoming requests at all and even not
  // accepting the corresponding TCP connections, all the connetions request
  // end up in the RX queue.
  ASSERT_EQ(kClientsNum, info.rx_queue_size);

  // The TX queue size for a listening socket set to the size of the backlog
  // as specified by the second parameter of the listen() system call, capped
  // by the system-wide limit in /proc/sys/net/core/somaxconn).
  ASSERT_EQ(kListenBacklog, info.tx_queue_size);
}

// This scenario is similar to the TestRpcSocketTxRxQueue.UnderCapacity scenario
// above, but in this case the listening socket's backlog length equals
// to the number of pending client TCP connections.
TEST_P(TestRpcSocketTxRxQueue, AtCapacity) {
  constexpr int kListenBacklog = 8;
  constexpr size_t kClientsNum = 8;

  DiagnosticSocket::TcpSocketInfo info;
  ASSERT_OK(RunAndGetSocketInfo(kListenBacklog, kClientsNum, &info));

  ASSERT_EQ(kClientsNum, info.rx_queue_size);
  ASSERT_EQ(kListenBacklog, info.tx_queue_size);
}

// This scenario is similar to the couple of scenarios above, but it's not
// enough space in the socket's RX queue to accommodate all the pending TCP
// connections.
TEST_P(TestRpcSocketTxRxQueue, OverCapacity) {
  constexpr int kListenBacklog = 5;
  constexpr size_t kClientsNum = 16;

  DiagnosticSocket::TcpSocketInfo info;
  ASSERT_OK(RunAndGetSocketInfo(kListenBacklog, kClientsNum, &info));

  // Even if there are many more connection requests than the backlog of the
  // listening socket can accommodate, the size of the socket's RX queue
  // reflects only the requests that are fit into the backlog plus one extra.
  // The rest of the incoming TCP packets do not affect the size of the RX
  // queue as seen via the sock_diag netlink facility. Same behavior can also be
  // observed via /proc/self/net/tcp, 'netstat', and 'ss' system utilities.
  //
  // On Linux, with the default setting of the net.ipv4.tcp_abort_on_overflow
  // sysctl variable (see [1] for more details), the server's TCP stack just
  // drops overflow packets, so the client's TCP stack retries sending initial
  // SYN packet to re-attempt the TCP connection. That's exactly what the
  // following paragraph from [2] refers to:
  //
  //   The backlog argument defines the maximum length to which the
  //   queue of pending connections for sockfd may grow.  If a
  //   connection request arrives when the queue is full, the client may
  //   receive an error with an indication of ECONNREFUSED or, if the
  //   underlying protocol supports retransmission, the request may be
  //   ignored so that a later reattempt at connection succeeds.
  //
  // [1] https://sysctl-explorer.net/net/ipv4/tcp_abort_on_overflow/
  // [2] https://man7.org/linux/man-pages/man2/listen.2.html
  //
  ASSERT_EQ(kListenBacklog + 1, info.rx_queue_size);
  ASSERT_EQ(kListenBacklog, info.tx_queue_size);
}

// Basic verification for the numbers reported by the
// 'rpc_listen_socket_rx_queue_size' histogram metric.
TEST_P(TestRpcSocketTxRxQueue, AcceptorRxQueueSizeMetric) {
  // Capture listening socket's metrics upon every accepted connection.
  FLAGS_rpc_listen_socket_stats_every_log2 = 0;

  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  {
    Socket socket;
    ASSERT_OK(socket.Init(server_addr.family(), /*flags=*/0));
    ASSERT_OK(socket.Connect(server_addr));
  }

  const auto& metric_entity = server_messenger_->metric_entity();
  scoped_refptr<Histogram> rx_queue_size =
      METRIC_rpc_listen_socket_rx_queue_size.Instantiate(metric_entity);

  // Using ASSERT_EVENTUALLY below because of relaxed memory ordering when
  // fetching metrics' values. Eventually, metrics reports readings that are
  // consistent with the expected numbers.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, rx_queue_size->TotalCount());
    // The metric had been sampled after the only pending connection was
    // accepted, so the maximum metric's value should be 0.
    ASSERT_GE(rx_queue_size->MaxValueForTests(), 0);
  });
}

TEST_P(TestRpcSocketTxRxQueue, DisableAcceptorRxQueueSampling) {
  n_acceptor_pool_threads_ = 1;

  // Disable listening RPC socket's statistics sampling.
  FLAGS_rpc_listen_socket_stats_every_log2 = -1;

  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  for (auto i = 0; i < 100; ++i) {
    Socket socket;
    ASSERT_OK(socket.Init(server_addr.family(), /*flags=*/0));
    ASSERT_OK(socket.Connect(server_addr));
  }

  const auto& metric_entity = server_messenger_->metric_entity();
  scoped_refptr<Histogram> rx_queue_size =
      METRIC_rpc_listen_socket_rx_queue_size.Instantiate(metric_entity);
  ASSERT_EQ(0, rx_queue_size->TotalCount());
}

TEST_P(TestRpcSocketTxRxQueue, CustomAcceptorRxQueueSamplingFrequency) {
  n_acceptor_pool_threads_ = 1;

  // Sampling the listening socket's stats every 8th request.
  FLAGS_rpc_listen_socket_stats_every_log2 = 3;
  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  for (auto i = 0; i < 16; ++i) {
    Socket socket;
    ASSERT_OK(socket.Init(server_addr.family(), /*flags=*/0));
    ASSERT_OK(socket.Connect(server_addr));
  }

  const auto& metric_entity = server_messenger_->metric_entity();
  scoped_refptr<Histogram> rx_queue_size =
      METRIC_rpc_listen_socket_rx_queue_size.Instantiate(metric_entity);
  // Using ASSERT_EVENTUALLY below because of relaxed memory ordering when
  // fetching metrics' values. Eventually, metrics reports readings that are
  // consistent with the expected numbers.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(2, rx_queue_size->TotalCount());
  });
}

class TestRpcWithIpModes: public RpcTestBase,
                          public ::testing::WithParamInterface<string> {
protected:
  void SetUp() override {
    RpcTestBase::SetUp();
    FLAGS_ip_config_mode = GetParam();
    ASSERT_OK(ParseIPModeFlag(FLAGS_ip_config_mode, &mode_));
  }
  Sockaddr bind_ip_addr() const {
    switch (mode_) {
      case IPMode::IPV6:
      case IPMode::DUAL:
        return Sockaddr::Wildcard(AF_INET6);
      default:
        return Sockaddr::Wildcard(AF_INET);
    }
  }

  IPMode mode_;
};

// This is used to run all parameterized tests with every
// possible ip_config_mode options.
INSTANTIATE_TEST_SUITE_P(Parameters, TestRpcWithIpModes,
                         testing::Values("ipv4", "ipv6", "dual"));

TEST_P(TestRpcWithIpModes, TestRpcWithDifferentIpConfigModes) {
  // Set up server with wildcard address.
  Sockaddr server_addr = bind_ip_addr();
  // Request OS to choose port.
  server_addr.set_port(0);

  MessengerBuilder mb("TestRpc.TestRpcWithDifferentIpConfigModes");
  mb.set_metric_entity(metric_entity_);

  shared_ptr<Messenger> messenger;
  ASSERT_OK(mb.Build(&messenger));

  // Start server on IP address based on ip_config_mode flag.
  ASSERT_OK(StartTestServerWithCustomMessenger(&server_addr, messenger));

  // IPv4 socket client tests.
  {
    Socket s4;
    ASSERT_OK(s4.Init(AF_INET, 0));

    // Target address is required mainly for dual mode. This is required to rule out
    // any possibility of 'connect' call failing due to invalid target address.
    Sockaddr target_addr = server_addr;

    // Determine the specific target address based on the server's mode.
    // For DUAL mode, we explicitly target the IPv4 loopback (127.0.0.1) for clarity.
    if (mode_ == IPMode::DUAL) {
      target_addr = Sockaddr::Loopback(AF_INET);
      target_addr.set_port(server_addr.port());
    }

    Status s = s4.Connect(target_addr);

    if (mode_ == IPMode::IPV6) {
      // IPv4 socket's connect call to 'IPv6 only' server should fail.
      ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "Address family not supported by protocol");
    } else {
      ASSERT_OK(s);
    }
  }

  // IPv6 socket client tests.
  {
    Socket s6;
    ASSERT_OK(s6.Init(AF_INET6, 0));

    // Determine the specific target address based on the server's mode.
    // For DUAL mode, we explicitly target the IPv6 loopback (::1) for clarity.
    Sockaddr target_addr = server_addr;
    if (mode_ == IPMode::DUAL) {
        target_addr = Sockaddr::Loopback(AF_INET6);
        target_addr.set_port(server_addr.port());
    }

    Status s = s6.Connect(target_addr);
    if (mode_ == IPMode::IPV4) {
      // IPv6 socket's connect call to 'IPv4 only' server should fail.
      ASSERT_TRUE(s.IsNetworkError()) << s.ToString();
      ASSERT_STR_CONTAINS(s.ToString(), "Invalid argument");
    } else {
      ASSERT_OK(s);
    }
  }
}

#endif // #if defined(KUDU_HAS_DIAGNOSTIC_SOCKET) ...

} // namespace rpc
} // namespace kudu
