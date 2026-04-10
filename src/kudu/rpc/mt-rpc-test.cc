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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/proxy.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/service_pool.h"
#include "kudu/util/barrier.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(rpc_connection_collect_io_handler_latency);

METRIC_DECLARE_counter(queue_overflow_rejections_kudu_rpc_test_CalculatorService_Add);
METRIC_DECLARE_counter(queue_overflow_rejections_kudu_rpc_test_CalculatorService_Sleep);
METRIC_DECLARE_counter(rpc_connections_accepted);
METRIC_DECLARE_counter(rpcs_queue_overflow);
METRIC_DECLARE_histogram(reactor_ev_loop_max_read_latency_us);
METRIC_DECLARE_histogram(reactor_ev_loop_max_write_latency_us);

using std::ostringstream;
using std::string;
using std::shared_ptr;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

class AcceptorPool;

class MultiThreadedRpcTest : public RpcTestBase {
 public:
  // Make a single RPC call.
  void SingleCall(Sockaddr server_addr, const string& method_name,
                  Status* result, CountDownLatch* latch) {
    LOG(INFO) << "Connecting to " << server_addr.ToString();
    shared_ptr<Messenger> client_messenger;
    CHECK_OK(CreateMessenger("ClientSC", &client_messenger));
    Proxy p(client_messenger, server_addr, server_addr.host(),
            GenericCalculatorService::static_service_name());
    *result = DoTestSyncCall(&p, method_name);
    latch->CountDown();
  }

  // Make RPC calls until we see a failure.
  void HammerServer(Sockaddr server_addr, const string& method_name,
                    Status* last_result) {
    shared_ptr<Messenger> client_messenger;
    CHECK_OK(CreateMessenger("ClientHS", &client_messenger));
    HammerServerWithMessenger(server_addr, method_name, last_result, client_messenger);
  }

  void HammerServerWithMessenger(
      Sockaddr server_addr, const string& method_name, Status* last_result,
      const shared_ptr<Messenger>& messenger) {
    LOG(INFO) << "Connecting to " << server_addr.ToString();
    Proxy p(messenger, server_addr, server_addr.host(),
            GenericCalculatorService::static_service_name());

    int i = 0;
    while (true) {
      i++;
      Status s = DoTestSyncCall(&p, method_name);
      if (!s.ok()) {
        // Return on first failure.
        LOG(INFO) << "Call failed. Shutting down client thread. Ran " << i << " calls: "
            << s.ToString();
        *last_result = s;
        return;
      }
    }
  }
};

static void AssertShutdown(thread* thread, const Status* status) {
  thread->join();
  string msg = status->ToString();
  ASSERT_TRUE(msg.find("Service unavailable") != string::npos ||
              msg.find("Network error") != string::npos)
              << "Status is actually: " << msg;
}

// Test making several concurrent RPC calls while shutting down.
// Simply verify that we don't hit any CHECK errors.
TEST_F(MultiThreadedRpcTest, TestShutdownDuringService) {
  // Set up server.
  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  constexpr int kNumThreads = 4;
  thread threads[kNumThreads];
  Status statuses[kNumThreads];
  for (int i = 0; i < kNumThreads; i++) {
    auto* my_status = &statuses[i];
    threads[i] = thread([this, server_addr, my_status]() {
      this->HammerServer(server_addr, GenericCalculatorService::kAddMethodName, my_status);
    });
  }

  SleepFor(MonoDelta::FromMilliseconds(50));

  // Shut down server.
  ASSERT_OK(server_messenger_->UnregisterService(service_name_));
  service_pool_->Shutdown();
  server_messenger_->Shutdown();

  for (int i = 0; i < kNumThreads; i++) {
    AssertShutdown(&threads[i], &statuses[i]);
  }
}

// Test shutting down the client messenger exactly as a thread is about to start
// a new connection. This is a regression test for KUDU-104.
TEST_F(MultiThreadedRpcTest, TestShutdownClientWhileCallsPending) {
  // Set up server.
  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("Client", &client_messenger));

  Status status;
  thread thread([this, server_addr, &status, client_messenger]() {
    this->HammerServerWithMessenger(server_addr, GenericCalculatorService::kAddMethodName,
                                    &status, client_messenger);
  });

  // Shut down the messenger after a very brief sleep. This often will race so that the
  // call gets submitted to the messenger before shutdown, but the negotiation won't have
  // started yet. In a debug build this fails about half the time without the bug fix.
  // See KUDU-104.
  SleepFor(MonoDelta::FromMicroseconds(10));
  client_messenger->Shutdown();
  client_messenger.reset();

  thread.join();
  ASSERT_TRUE(status.IsAborted() ||
              status.IsServiceUnavailable());
  string msg = status.ToString();
  SCOPED_TRACE(msg);
  ASSERT_TRUE(msg.find("Client RPC Messenger shutting down") != string::npos ||
              msg.find("reactor is shutting down") != string::npos ||
              msg.find("Unable to start connection negotiation thread") != string::npos)
              << "Status is actually: " << msg;
}

// This bogus service pool leaves the service queue full.
class BogusServicePool : public ServicePool {
 public:
  BogusServicePool(unique_ptr<ServiceIf> service,
                   const scoped_refptr<MetricEntity>& metric_entity,
                   size_t service_queue_length)
    : ServicePool(std::move(service), metric_entity, service_queue_length) {
  }
  Status Init(int /*num_threads*/) override {
    // Do nothing
    return Status::OK();
  }
};

void IncrementBackpressureOrShutdown(const Status* status, int* backpressure, int* shutdown) {
  string msg = status->ToString();
  if (msg.find("service queue is full") != string::npos) {
    ++(*backpressure);
  } else if (msg.find("shutting down") != string::npos) {
    ++(*shutdown);
  } else if (msg.find("recv got EOF from") != string::npos) {
    ++(*shutdown);
  } else {
    FAIL() << "Unexpected status message: " << msg;
  }
}

// Test that we get a Service Unavailable error when we max out the incoming RPC service queue.
TEST_F(MultiThreadedRpcTest, TestBlowOutServiceQueue) {
  const size_t kMaxConcurrency = 2;

  MessengerBuilder bld("messenger1");
  bld.set_num_reactors(kMaxConcurrency);
  bld.set_metric_entity(metric_entity_);
  CHECK_OK(bld.Build(&server_messenger_));

  shared_ptr<AcceptorPool> pool;
  ASSERT_OK(server_messenger_->AddAcceptorPool(Sockaddr::Wildcard(), &pool));
  ASSERT_OK(pool->Start(kMaxConcurrency));
  Sockaddr server_addr = pool->bind_address();

  unique_ptr<ServiceIf> service(new GenericCalculatorService());
  service_name_ = service->service_name();
  service_pool_ = new BogusServicePool(std::move(service),
                                      server_messenger_->metric_entity(),
                                      kMaxConcurrency);
  ASSERT_OK(service_pool_->Init(n_worker_threads_));
  ASSERT_OK(server_messenger_->RegisterService(service_name_, service_pool_));

  constexpr int kNumThreads = 3;
  thread threads[kNumThreads];
  Status status[kNumThreads];
  CountDownLatch latch(1);
  for (int i = 0; i < kNumThreads; i++) {
    auto* my_status = &status[i];
    threads[i] = thread([this, server_addr, my_status, &latch]() {
      this->SingleCall(server_addr, GenericCalculatorService::kAddMethodName,
                       my_status, &latch);
    });
  }

  // One should immediately fail due to backpressure. The latch is only initialized
  // to wait for the first of three threads to finish.
  latch.Wait();

  // The rest would time out after 10 sec, but we help them along.
  ASSERT_OK(server_messenger_->UnregisterService(service_name_));
  service_pool_->Shutdown();
  server_messenger_->Shutdown();

  for (auto& t : threads) {
    t.join();
  }

  // Verify that one error was due to backpressure.
  int errors_backpressure = 0;
  int errors_shutdown = 0;

  for (const auto& s : status) {
    IncrementBackpressureOrShutdown(&s, &errors_backpressure, &errors_shutdown);
  }

  ASSERT_EQ(1, errors_backpressure);
  ASSERT_EQ(2, errors_shutdown);

  // Check that RPC queue overflow metric is 1
  Counter *rpcs_queue_overflow =
    METRIC_rpcs_queue_overflow.Instantiate(server_messenger_->metric_entity()).get();
  ASSERT_EQ(1, rpcs_queue_overflow->value());
}

// This scenario verifies that the histogram-type reactor I/O latency metrics
// for receiving and sending data via server sockets contains no samples:
// unless configured otherwise, collecting those metrics is disabled per
// default setting of the --rpc_connection_collect_io_handler_latency flag.
TEST_F(MultiThreadedRpcTest, ReactorMaxIOLatencyStatsDisabled) {
  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  shared_ptr<Messenger> client_messenger;
  ASSERT_OK(CreateMessenger("ClientSC", &client_messenger));
  Proxy p(client_messenger, server_addr, server_addr.host(),
          GenericCalculatorService::static_service_name());

  DoTestSidecar(&p, 1024, 1024 * 1024);

  // Let the server's reactor close idle RPC connections.
  ASSERT_EVENTUALLY([&] {
    // Make sure all the RPC connections have been closed: if the server metrics
    // have accumulated information on maximum latencies RPC connections,
    // it would be then detectable by the histogram's sample count.
    DumpConnectionsResponsePB srv_con_pb;
    ASSERT_OK(server_messenger_->DumpConnections({}, &srv_con_pb));
    ASSERT_EQ(0, srv_con_pb.outbound_connections_size());
    ASSERT_EQ(0, srv_con_pb.inbound_connections_size());
  });

  const auto& check_histogram = [&](const HdrHistogram& h, const char* ctx) {
    SCOPED_TRACE(ctx);
    ASSERT_GE(h.TotalCount(), 0);
    if (VLOG_IS_ON(1)) {
      ostringstream ostr;
      h.DumpHumanReadable(&ostr);
      VLOG(1) << ostr.str();
    }
  };

  NO_FATALS(check_histogram(
      *METRIC_reactor_ev_loop_max_read_latency_us.Instantiate(
          server_messenger_->metric_entity())->histogram(),
      "read_latency"));

  NO_FATALS(check_histogram(
      *METRIC_reactor_ev_loop_max_write_latency_us.Instantiate(
          server_messenger_->metric_entity())->histogram(),
      "write_latency"));
}

// This scenario verifies the presence and a few basic invariants of the reactor
// I/O latency metrics for receiving and sending data via server sockets, i.e.
// the latency of performing read and write operations since a socket has
// signalled as I/O-ready.
TEST_F(MultiThreadedRpcTest, ReactorMaxIOLatencyStatsEnabled) {
  constexpr const size_t kNumThreads = 5;

  google::FlagSaver flag_saver;
  FLAGS_rpc_connection_collect_io_handler_latency = true;

  // To induce higher reactor I/O latency, use a single reactor thread in the
  // server to handle concurrent RPCs from multiple client connections.
  n_server_reactor_threads_ = 1;
  n_worker_threads_ = kNumThreads;

  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  thread threads[kNumThreads];
  CountDownLatch latch_started(1);
  CountDownLatch latch_completed(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    threads[i] = thread([this, server_addr, &latch_started, &latch_completed]() {
      shared_ptr<Messenger> client_messenger;
      CHECK_OK(CreateMessenger("ClientSC", &client_messenger));
      Proxy p(client_messenger, server_addr, server_addr.host(),
              GenericCalculatorService::static_service_name());

      latch_started.Wait();
      // Larger sidecars make reactor thread spending more time on
      // reading/writing the data.
      constexpr const int kSidecarSize0 = 16 * 1024 * 1024;
      constexpr const int kSidecarSize1 = 32 * 1024 * 1024;
      DoTestSidecar(&p, kSidecarSize0, kSidecarSize1);
      CHECK_OK(DoTestOutgoingSidecar(&p, kSidecarSize0, kSidecarSize1));
      latch_completed.CountDown();
    });
  }

  // Start the client threads.
  latch_started.CountDown();

  // Let the server complete processing the requests.
  latch_completed.Wait();

  for (auto& t : threads) {
    t.join();
  }

  // Let the server's reactor close idle RPC connections.
  ASSERT_EVENTUALLY([&] {
    // Make sure all the RPC connections have been closed, so the server metrics
    // accumulated the information on maximum latencies for all the RPC
    // connections that had been around during the activities above.
    DumpConnectionsResponsePB srv_con_pb;
    ASSERT_OK(server_messenger_->DumpConnections({}, &srv_con_pb));
    ASSERT_EQ(0, srv_con_pb.outbound_connections_size());
    ASSERT_EQ(0, srv_con_pb.inbound_connections_size());
  });

  const auto& check_histogram = [&](const HdrHistogram& h,
                                    const char* ctx,
                                    uint64_t* max) {
    SCOPED_TRACE(ctx);
    ASSERT_GE(h.TotalCount(), 1);
    *max = h.MaxValue();
    if (VLOG_IS_ON(1)) {
      ostringstream ostr;
      h.DumpHumanReadable(&ostr);
      VLOG(1) << ostr.str();
    }
  };

  uint64_t max_rd_latency = 0;
  NO_FATALS(check_histogram(
      *METRIC_reactor_ev_loop_max_read_latency_us.Instantiate(
          server_messenger_->metric_entity())->histogram(),
      "read_latency",
      &max_rd_latency));

  uint64_t max_wr_latency = 0;
  NO_FATALS(check_histogram(
      *METRIC_reactor_ev_loop_max_write_latency_us.Instantiate(
          server_messenger_->metric_entity())->histogram(),
      "write_latency",
      &max_wr_latency));

  // Due to large sidecars, it should take some time to send and receive data
  // in the context of a single RPC call. Since there are many active concurrent
  // connections, the maximum I/O handler latency across all the connections
  // is much higher than one microsecond if the OS-level maximum socket buffer
  // size limit isn't significantly higher than 8MiB. At least, that's how it
  // was on a contemporary hardware at the time of writing.
  ASSERT_GT(max_wr_latency, 1);
  ASSERT_GT(max_rd_latency, 1);
}

TEST_F(MultiThreadedRpcTest, PerMethodQueueOverflowRejectionCounter) {
  n_acceptor_pool_threads_ = 1;
  n_server_reactor_threads_ = 1;
  n_worker_threads_ = 1;
  service_queue_length_ = 1;

  Sockaddr server_addr;
  ASSERT_OK(StartTestServerWithGeneratedCode(&server_addr, false /*enable_ssl*/));

  auto* rpcs_queue_overflow = METRIC_rpcs_queue_overflow.Instantiate(
      server_messenger_->metric_entity()).get();
  ASSERT_EQ(0, rpcs_queue_overflow->value());

  auto* queue_overflow_rejections_add =
      METRIC_queue_overflow_rejections_kudu_rpc_test_CalculatorService_Add.Instantiate(
          server_messenger_->metric_entity()).get();
  ASSERT_EQ(0, queue_overflow_rejections_add->value());

  auto* queue_overflow_rejections_sleep =
      METRIC_queue_overflow_rejections_kudu_rpc_test_CalculatorService_Sleep.Instantiate(
          server_messenger_->metric_entity()).get();
  ASSERT_EQ(0, queue_overflow_rejections_sleep->value());

  // It seems that even in case of scheduling anomalies, 16 concurrent
  // requests should be enough to overflow the RPC service queue of size 1,
  // where the queue is served by a single worker thread and it takes at least
  // 100ms for a request to complete.
  constexpr size_t kNumThreads = 16;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  vector<Status> status(kNumThreads);
  Barrier barrier(kNumThreads);
  for (size_t i = 0; i < kNumThreads; ++i) {
    const size_t idx = i;
    threads.emplace_back([&, idx]() {
      shared_ptr<Messenger> client_messenger;
      CHECK_OK(CreateMessenger("ClientMessenger", &client_messenger));
      Proxy p(client_messenger, server_addr, server_addr.host(),
              CalculatorService::static_service_name());
      SleepRequestPB req;
      req.set_sleep_micros(100 * 1000); // 100ms
      SleepResponsePB resp;
      RpcController controller;
      controller.set_timeout(MonoDelta::FromMilliseconds(10000));
      barrier.Wait();
      status[idx] = p.SyncRequest(
          GenericCalculatorService::kSleepMethodName, req, &resp, &controller);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  size_t num_errors = 0;
  for (const auto& s : status) {
    if (!s.ok()) {
      ++num_errors;
    }
  }

  // Check that there were some RPC queue overflows.
  const auto queue_overflow_num = rpcs_queue_overflow->value();
  ASSERT_GT(num_errors, 0);
  ASSERT_EQ(num_errors, queue_overflow_num);

  // Check corresponding per-method rejection counters.
  ASSERT_EQ(0, queue_overflow_rejections_add->value());
  ASSERT_EQ(queue_overflow_num, queue_overflow_rejections_sleep->value());
}

static void HammerServerWithTCPConns(const Sockaddr& addr) {
  while (true) {
    Socket socket;
    CHECK_OK(socket.Init(addr.family(), 0));
    Status s;
    LOG_SLOW_EXECUTION(INFO, 100, "Connect took long") {
      s = socket.Connect(addr);
    }
    if (!s.ok()) {
      CHECK(s.IsNetworkError()) << "Unexpected error: " << s.ToString();
      return;
    }
    CHECK_OK(socket.Close());
  }
}

// Regression test for KUDU-128.
// Test that shuts down the server while new TCP connections are incoming.
TEST_F(MultiThreadedRpcTest, TestShutdownWithIncomingConnections) {
  // Set up server.
  Sockaddr server_addr;
  ASSERT_OK(StartTestServer(&server_addr));

  // Start a number of threads which just hammer the server with TCP connections.
  constexpr int kNumThreads = 8;
  vector<thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back([server_addr]() { HammerServerWithTCPConns(server_addr); });
  }

  // Sleep until the server has started to actually accept some connections from the
  // test threads.
  scoped_refptr<Counter> conns_accepted =
    METRIC_rpc_connections_accepted.Instantiate(server_messenger_->metric_entity());
  while (conns_accepted->value() == 0) {
    SleepFor(MonoDelta::FromMicroseconds(100));
  }

  // Shutdown while there are still new connections appearing.
  ASSERT_OK(server_messenger_->UnregisterService(service_name_));
  service_pool_->Shutdown();
  server_messenger_->Shutdown();

  for (auto& t : threads) {
    t.join();
  }
}

} // namespace rpc
} // namespace kudu

