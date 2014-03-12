// Copyright (c) 2013, Cloudera, inc

#include "rpc/rpc-test-base.h"

#include <string>

#include <boost/bind.hpp>
#include <gtest/gtest.h>

#include "gutil/stl_util.h"
#include "util/metrics.h"
#include "util/test_util.h"
#include "util/thread_util.h"

METRIC_DECLARE_counter(rpc_connections_accepted);

using std::string;

namespace kudu {
namespace rpc {

class MultiThreadedRpcTest : public RpcTestBase {
 public:
  // Make a single RPC call.
  void SingleCall(Sockaddr server_addr, const char* method_name, Status* result) {
    LOG(INFO) << "Connecting to " << server_addr.ToString();
    shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
    Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());
    *result = DoTestSyncCall(p, method_name);
  }

  // Make RPC calls until we see a failure.
  void HammerServer(Sockaddr server_addr, const char* method_name, Status* last_result) {
    shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
    HammerServerWithMessenger(server_addr, method_name, last_result, client_messenger);
  }

  void HammerServerWithMessenger(
      Sockaddr server_addr, const char* method_name, Status* last_result,
      const shared_ptr<Messenger>& messenger) {
    LOG(INFO) << "Connecting to " << server_addr.ToString();
    Proxy p(messenger, server_addr, GenericCalculatorService::static_service_name());

    int i = 0;
    while (true) {
      i++;
      Status s = DoTestSyncCall(p, method_name);
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

static void AssertShutdown(boost::thread* thread, const string& thread_name, const Status* status) {
  ASSERT_STATUS_OK(ThreadJoiner(thread, thread_name).warn_every_ms(500).Join());
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
  StartTestServer(&server_addr);

  gscoped_ptr<boost::thread> thread1, thread2, thread3, thread4;
  Status status1, status2, status3, status4;
  ASSERT_STATUS_OK(StartThread(boost::bind(&MultiThreadedRpcTest::HammerServer, this, server_addr,
        GenericCalculatorService::kAddMethodName, &status1), &thread1));
  ASSERT_STATUS_OK(StartThread(boost::bind(&MultiThreadedRpcTest::HammerServer, this, server_addr,
        GenericCalculatorService::kAddMethodName, &status2), &thread2));
  ASSERT_STATUS_OK(StartThread(boost::bind(&MultiThreadedRpcTest::HammerServer, this, server_addr,
        GenericCalculatorService::kAddMethodName, &status3), &thread3));
  ASSERT_STATUS_OK(StartThread(boost::bind(&MultiThreadedRpcTest::HammerServer, this, server_addr,
        GenericCalculatorService::kAddMethodName, &status4), &thread4));

  usleep(50000); // 50ms

  // Shut down server.
  server_messenger_->Shutdown();

  AssertShutdown(thread1.get(), "thread1", &status1);
  AssertShutdown(thread2.get(), "thread2", &status2);
  AssertShutdown(thread3.get(), "thread3", &status3);
  AssertShutdown(thread4.get(), "thread4", &status4);
}

// Test shutting down the client messenger exactly as a thread is about to start
// a new connection. This is a regression test for KUDU-104.
TEST_F(MultiThreadedRpcTest, TestShutdownClientWhileCallsPending) {
  // Set up server.
  Sockaddr server_addr;
  StartTestServer(&server_addr);

  shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));

  gscoped_ptr<boost::thread> thread;
  Status status;
  ASSERT_STATUS_OK(
    StartThread(boost::bind(&MultiThreadedRpcTest::HammerServerWithMessenger, this, server_addr,
                            GenericCalculatorService::kAddMethodName, &status, client_messenger),
                &thread));


  // Shut down the messenger after a very brief sleep. This often will race so that the
  // call gets submitted to the messenger before shutdown, but the negotiation won't have
  // started yet. In a debug build this fails about half the time without the bug fix.
  // See KUDU-104.
  usleep(10);
  client_messenger->Shutdown();
  client_messenger.reset();

  ASSERT_STATUS_OK(ThreadJoiner(thread.get(), "client thread").warn_every_ms(500).Join());
  ASSERT_TRUE(status.IsServiceUnavailable());
  string msg = status.ToString();
  SCOPED_TRACE(msg);
  ASSERT_TRUE(msg.find("Client RPC Messenger shutting down") != string::npos ||
              msg.find("reactor is shutting down") != string::npos);
}

// This bogus service pool leaves the service queue full.
class BogusServicePool : public ServicePool {
 public:
  BogusServicePool(const shared_ptr<Messenger>& messenger, gscoped_ptr<ServiceIf> service)
    : ServicePool(messenger, service.Pass()) {
  }
  virtual Status Init(int num_threads) {
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
  } else if (msg.find("got EOF from remote") != string::npos) {
    ++(*shutdown);
  } else {
    FAIL() << "Unexpected status message: " << msg;
  }
}

// Test that we get a Service Unavailable error when we max out the incoming RPC service queue.
TEST_F(MultiThreadedRpcTest, TestBlowOutServiceQueue) {
  MessengerBuilder bld("messenger1");
  bld.set_num_reactors(2);
  bld.set_service_queue_length(2);
  bld.set_metric_context(metric_ctx_);
  CHECK_OK(bld.Build(&server_messenger_));

  shared_ptr<AcceptorPool> pool;
  ASSERT_STATUS_OK(server_messenger_->AddAcceptorPool(Sockaddr(), 2, &pool));
  Sockaddr server_addr = pool->bind_address();

  gscoped_ptr<ServiceIf> impl(new GenericCalculatorService());
  worker_pool_.reset(new BogusServicePool(server_messenger_, impl.Pass()));
  ASSERT_STATUS_OK(worker_pool_->Init(n_worker_threads_));

  gscoped_ptr<boost::thread> thread1, thread2, thread3;
  Status status1, status2, status3;
  ASSERT_STATUS_OK(StartThread(boost::bind(&MultiThreadedRpcTest::SingleCall, this, server_addr,
        GenericCalculatorService::kAddMethodName, &status1), &thread1));
  ASSERT_STATUS_OK(StartThread(boost::bind(&MultiThreadedRpcTest::SingleCall, this, server_addr,
        GenericCalculatorService::kAddMethodName, &status2), &thread2));
  ASSERT_STATUS_OK(StartThread(boost::bind(&MultiThreadedRpcTest::SingleCall, this, server_addr,
        GenericCalculatorService::kAddMethodName, &status3), &thread3));

  // One should immediately fail due to backpressure. We provide a little bit of time for that.
  usleep(100000); // 100ms

  // The rest would time out after 10 sec, but we help them along.
  server_messenger_->Shutdown();

  ASSERT_STATUS_OK(ThreadJoiner(thread1.get(), "thread1").warn_every_ms(500).Join());
  ASSERT_STATUS_OK(ThreadJoiner(thread2.get(), "thread2").warn_every_ms(500).Join());
  ASSERT_STATUS_OK(ThreadJoiner(thread3.get(), "thread3").warn_every_ms(500).Join());

  // Verify that one error was due to backpressure.
  int errors_backpressure = 0;
  int errors_shutdown = 0;

  IncrementBackpressureOrShutdown(&status1, &errors_backpressure, &errors_shutdown);
  IncrementBackpressureOrShutdown(&status2, &errors_backpressure, &errors_shutdown);
  IncrementBackpressureOrShutdown(&status3, &errors_backpressure, &errors_shutdown);

  ASSERT_EQ(1, errors_backpressure);
  ASSERT_EQ(2, errors_shutdown);
}

static void HammerServerWithTCPConns(const Sockaddr& addr) {
  while (true) {
    Socket socket;
    CHECK_OK(socket.Init(0));
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
  StartTestServer(&server_addr);

  // Start a number of threads which just hammer the server with TCP connections.
  vector<boost::thread*> threads;
  ElementDeleter d(&threads);
  for (int i = 0; i < 8; i++) {
    threads.push_back(new boost::thread(&HammerServerWithTCPConns, server_addr));
  }

  // Sleep until the server has started to actually accept some connections from the
  // test threads.
  Counter* conns_accepted =
    METRIC_rpc_connections_accepted.Instantiate(*server_messenger_->metric_context());
  while (conns_accepted->value() == 0) {
    usleep(100);
  }

  // Shutdown while there are still new connections appearing.
  server_messenger_->Shutdown();

  BOOST_FOREACH(boost::thread* t, threads) {
    ASSERT_STATUS_OK(ThreadJoiner(t, "TCP connector thread").warn_every_ms(500).Join());
  }
}

} // namespace rpc
} // namespace kudu

