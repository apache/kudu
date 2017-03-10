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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/bind.hpp>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/serialization.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/test_util.h"

METRIC_DECLARE_histogram(handler_latency_kudu_rpc_test_CalculatorService_Sleep);
METRIC_DECLARE_histogram(rpc_incoming_queue_time);

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
  shared_ptr<Messenger> messenger(CreateMessenger("TestCreateDestroy", 1, GetParam()));
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
    shared_ptr<Messenger> messenger(CreateMessenger("TestAcceptorPoolStartStop", 1, GetParam()));
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

// Test making successful RPC calls.
TEST_P(TestRpc, TestCall) {
  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  StartTestServer(&server_addr, enable_ssl);

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());
  ASSERT_STR_CONTAINS(p.ToString(), strings::Substitute("kudu.rpc.GenericCalculatorService@"
                                                            "{remote=$0, user_credentials=",
                                                        server_addr.ToString()));

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
  }
}

// Test that connecting to an invalid server properly throws an error.
TEST_P(TestRpc, TestCallToBadServer) {
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, GetParam()));
  Sockaddr addr;
  addr.set_port(0);
  Proxy p(client_messenger, addr, GenericCalculatorService::static_service_name());

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
  StartTestServer(&server_addr, enable_ssl);

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

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
  StartTestServer(&server_addr, enable_ssl);

  // Set up client with the wrong service name.
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, "WrongServiceName");

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
  StartTestServer(&server_addr, enable_ssl);
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());
  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));
}

// Test that connections are kept alive between calls.
TEST_P(TestRpc, TestConnectionKeepalive) {
  // Only run one reactor per messenger, so we can grab the metrics from that
  // one without having to check all.
  n_server_reactor_threads_ = 1;
  keepalive_time_ms_ = 100;

  // Set up server.
  Sockaddr server_addr;
  bool enable_ssl = GetParam();
  StartTestServer(&server_addr, enable_ssl);

  // Set up client.
  LOG(INFO) << "Connecting to " << server_addr.ToString();
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

  ASSERT_OK(DoTestSyncCall(p, GenericCalculatorService::kAddMethodName));

  SleepFor(MonoDelta::FromMilliseconds(5));

  ReactorMetrics metrics;
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(1, metrics.num_server_connections_) << "Server should have 1 server connection";
  ASSERT_EQ(0, metrics.num_client_connections_) << "Server should have 0 client connections";

  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_) << "Client should have 0 server connections";
  ASSERT_EQ(1, metrics.num_client_connections_) << "Client should have 1 client connections";

  SleepFor(MonoDelta::FromMilliseconds(200));

  // After sleeping, the keepalive timer should have closed both sides of
  // the connection.
  ASSERT_OK(server_messenger_->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_) << "Server should have 0 server connections";
  ASSERT_EQ(0, metrics.num_client_connections_) << "Server should have 0 client connections";

  ASSERT_OK(client_messenger->reactors_[0]->GetMetrics(&metrics));
  ASSERT_EQ(0, metrics.num_server_connections_) << "Client should have 0 server connections";
  ASSERT_EQ(0, metrics.num_client_connections_) << "Client should have 0 client connections";
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
  StartTestServer(&server_addr, enable_ssl);

  // Set up client.
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

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
  StartTestServer(&server_addr, enable_ssl);

  // Set up client.
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, GetParam()));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

  // Test a zero-length sidecar
  DoTestSidecar(p, 0, 0);

  // Test some small sidecars
  DoTestSidecar(p, 123, 456);

  // Test some larger sidecars to verify that we properly handle the case where
  // we can't write the whole response to the socket in a single call.
  DoTestSidecar(p, 3000 * 1024, 2000 * 1024);

  DoTestOutgoingSidecar(p, 0, 0);
  DoTestOutgoingSidecar(p, 123, 456);
  DoTestOutgoingSidecar(p, 3000 * 1024, 2000 * 1024);
}

TEST_P(TestRpc, TestRpcSidecarLimits) {
  {
    // Test that the limits on the number of sidecars is respected.
    RpcController controller;
    string s = "foo";
    int idx;
    for (int i = 0; i < TransferLimits::kMaxSidecars; ++i) {
      CHECK_OK(controller.AddOutboundSidecar(RpcSidecar::FromSlice(Slice(s)), &idx));
    }

    CHECK(!controller.AddOutboundSidecar(RpcSidecar::FromSlice(Slice(s)), &idx).ok());
  }

  {
    // Test that the payload may not exceed --rpc_max_message_size.
    // Set up server.
    Sockaddr server_addr;
    bool enable_ssl = GetParam();
    StartTestServer(&server_addr, enable_ssl);

    // Set up client.
    shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, GetParam()));
    Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

    RpcController controller;
    string s(FLAGS_rpc_max_message_size + 1, 'a');
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
  StartTestServer(&server_addr, enable_ssl);
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

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
  StartTestServer(&server_addr, enable_ssl);
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

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
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

  ASSERT_NO_FATAL_FAILURE(DoTestExpectTimeout(p, MonoDelta::FromMilliseconds(100)));

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
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
  Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

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
  StartTestServerWithGeneratedCode(&server_addr, enable_ssl);

  // Set up client.
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, CalculatorService::static_service_name());

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
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, GetParam()));
  Sockaddr bad_addr;
  CountDownLatch latch(1);

  AddRequestPB req;
  req.set_x(rand());
  req.set_y(rand());
  AddResponsePB resp;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(1));
  {
    Proxy p(client_messenger, bad_addr, "xxx");
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
  StartTestServerWithGeneratedCode(&server_addr, enable_ssl);

  // Set up client.
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, CalculatorService::static_service_name());

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
  StartTestServerWithGeneratedCode(&server_addr, enable_ssl);

  // Set up client.
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, CalculatorService::static_service_name());

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
  StartTestServerWithGeneratedCode(&server_addr, enable_ssl);

  // Set up client.
  shared_ptr<Messenger> client_messenger(CreateMessenger("Client", 1, enable_ssl));
  Proxy p(client_messenger, server_addr, CalculatorService::static_service_name());

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

} // namespace rpc
} // namespace kudu
