// Copyright (c) 2013, Cloudera, inc

#include "rpc/rpc-test-base.h"

#include <string>

#include <boost/bind.hpp>
#include <gtest/gtest.h>

#include "util/test_util.h"
#include "util/thread_util.h"

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
    LOG(INFO) << "Connecting to " << server_addr.ToString();
    shared_ptr<Messenger> client_messenger(CreateMessenger("Client"));
    Proxy p(client_messenger, server_addr, GenericCalculatorService::static_service_name());

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
  } else {
    FAIL() << "Unexpected status message: " << msg;
  }
}

// Test that we get a Service Unavailable error when we max out the incoming RPC service queue.
TEST_F(MultiThreadedRpcTest, TestBlowOutServiceQueue) {
  MessengerBuilder bld("messenger1");
  bld.set_num_reactors(2);
  bld.set_service_queue_length(2);
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

} // namespace rpc
} // namespace kudu

