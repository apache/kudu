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

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest.h>
#include <boost/bind.hpp>

#include "kudu/gutil/stl_util.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpcz_store.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/rpc/rtest.service.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/test_util.h"
#include "kudu/util/user.h"

DEFINE_bool(is_panic_test_child, false, "Used by TestRpcPanic");
DECLARE_bool(socket_inject_short_recvs);

using std::shared_ptr;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

class RpcStubTest : public RpcTestBase {
 public:
  virtual void SetUp() OVERRIDE {
    RpcTestBase::SetUp();
    // Use a shorter queue length since some tests below need to start enough
    // threads to saturate the queue.
    service_queue_length_ = 10;
    StartTestServerWithGeneratedCode(&server_addr_);
    client_messenger_ = CreateMessenger("Client");
  }
 protected:
  void SendSimpleCall() {
    CalculatorServiceProxy p(client_messenger_, server_addr_);

    RpcController controller;
    AddRequestPB req;
    req.set_x(10);
    req.set_y(20);
    AddResponsePB resp;
    ASSERT_OK(p.Add(req, &resp, &controller));
    ASSERT_EQ(30, resp.result());
  }

  Sockaddr server_addr_;
  shared_ptr<Messenger> client_messenger_;
};

TEST_F(RpcStubTest, TestSimpleCall) {
  SendSimpleCall();
}

// Regression test for a bug in which we would not properly parse a call
// response when recv() returned a 'short read'. This injects such short
// reads and then makes a number of calls.
TEST_F(RpcStubTest, TestShortRecvs) {
  FLAGS_socket_inject_short_recvs = true;
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  for (int i = 0; i < 100; i++) {
    NO_FATALS(SendSimpleCall());
  }
}

// Test calls which are rather large.
// This test sends many of them at once using the async API and then
// waits for them all to return. This is meant to ensure that the
// IO threads can deal with read/write calls that don't succeed
// in sending the entire data in one go.
TEST_F(RpcStubTest, TestBigCallData) {
  const int kNumSentAtOnce = 20;
  const size_t kMessageSize = 5 * 1024 * 1024;
  string data;
  data.resize(kMessageSize);

  CalculatorServiceProxy p(client_messenger_, server_addr_);

  EchoRequestPB req;
  req.set_data(data);

  vector<unique_ptr<EchoResponsePB>> resps;
  vector<unique_ptr<RpcController>> controllers;

  CountDownLatch latch(kNumSentAtOnce);
  for (int i = 0; i < kNumSentAtOnce; i++) {
    resps.emplace_back(new EchoResponsePB);
    controllers.emplace_back(new RpcController);

    p.EchoAsync(req, resps.back().get(), controllers.back().get(),
                boost::bind(&CountDownLatch::CountDown, boost::ref(latch)));
  }

  latch.Wait();

  for (const auto& c : controllers) {
    ASSERT_OK(c->status());
  }
}

TEST_F(RpcStubTest, TestRespondDeferred) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController controller;
  SleepRequestPB req;
  req.set_sleep_micros(1000);
  req.set_deferred(true);
  SleepResponsePB resp;
  ASSERT_OK(p.Sleep(req, &resp, &controller));
}

// Test that the default user credentials are propagated to the server.
TEST_F(RpcStubTest, TestDefaultCredentialsPropagated) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  string expected;
  ASSERT_OK(GetLoggedInUser(&expected));

  RpcController controller;
  WhoAmIRequestPB req;
  WhoAmIResponsePB resp;
  ASSERT_OK(p.WhoAmI(req, &resp, &controller));
  ASSERT_EQ(expected, resp.credentials().real_user());
  ASSERT_FALSE(resp.credentials().has_effective_user());
}

// Test that the user can specify other credentials.
TEST_F(RpcStubTest, TestCustomCredentialsPropagated) {
  const char* const kFakeUserName = "some fake user";
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  UserCredentials creds;
  creds.set_real_user(kFakeUserName);
  p.set_user_credentials(creds);

  RpcController controller;
  WhoAmIRequestPB req;
  WhoAmIResponsePB resp;
  ASSERT_OK(p.WhoAmI(req, &resp, &controller));
  ASSERT_EQ(kFakeUserName, resp.credentials().real_user());
  ASSERT_FALSE(resp.credentials().has_effective_user());
}

// Test that the user's remote address is accessible to the server.
TEST_F(RpcStubTest, TestRemoteAddress) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController controller;
  WhoAmIRequestPB req;
  WhoAmIResponsePB resp;
  ASSERT_OK(p.WhoAmI(req, &resp, &controller));
  ASSERT_STR_CONTAINS(resp.address(), "127.0.0.1:");
}

////////////////////////////////////////////////////////////
// Tests for error cases
////////////////////////////////////////////////////////////

// Test sending a PB parameter with a missing field, where the client
// thinks it has sent a full PB. (eg due to version mismatch)
TEST_F(RpcStubTest, TestCallWithInvalidParam) {
  Proxy p(client_messenger_, server_addr_, CalculatorService::static_service_name());

  AddRequestPartialPB req;
  req.set_x(rand());
  // AddRequestPartialPB is missing the 'y' field.
  AddResponsePB resp;
  RpcController controller;
  Status s = p.SyncRequest("Add", req, &resp, &controller);
  ASSERT_TRUE(s.IsRemoteError()) << "Bad status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(),
                      "Invalid argument: invalid parameter for call "
                      "kudu.rpc_test.CalculatorService.Add: "
                      "missing fields: y");
}

// Wrapper around AtomicIncrement, since AtomicIncrement returns the 'old'
// value, and our callback needs to be a void function.
static void DoIncrement(Atomic32* count) {
  base::subtle::Barrier_AtomicIncrement(count, 1);
}

// Test sending a PB parameter with a missing field on the client side.
// This also ensures that the async callback is only called once
// (regression test for a previously-encountered bug).
TEST_F(RpcStubTest, TestCallWithMissingPBFieldClientSide) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController controller;
  AddRequestPB req;
  req.set_x(10);
  // Request is missing the 'y' field.
  AddResponsePB resp;
  Atomic32 callback_count = 0;
  p.AddAsync(req, &resp, &controller, boost::bind(&DoIncrement, &callback_count));
  while (NoBarrier_Load(&callback_count) == 0) {
    SleepFor(MonoDelta::FromMicroseconds(10));
  }
  SleepFor(MonoDelta::FromMicroseconds(100));
  ASSERT_EQ(1, NoBarrier_Load(&callback_count));
  ASSERT_STR_CONTAINS(controller.status().ToString(),
                      "Invalid argument: invalid parameter for call "
                      "kudu.rpc_test.CalculatorService.Add: missing fields: y");
}

TEST_F(RpcStubTest, TestResponseWithMissingField) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController rpc;
  TestInvalidResponseRequestPB req;
  TestInvalidResponseResponsePB resp;
  req.set_error_type(rpc_test::TestInvalidResponseRequestPB_ErrorType_MISSING_REQUIRED_FIELD);
  Status s = p.TestInvalidResponse(req, &resp, &rpc);
  ASSERT_STR_CONTAINS(s.ToString(),
                      "invalid RPC response, missing fields: response");
}

// Test case where the server responds with a message which is larger than the maximum
// configured RPC message size. The server should send the response, but the client
// will reject it.
TEST_F(RpcStubTest, TestResponseLargerThanFrameSize) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController rpc;
  TestInvalidResponseRequestPB req;
  TestInvalidResponseResponsePB resp;
  req.set_error_type(rpc_test::TestInvalidResponseRequestPB_ErrorType_RESPONSE_TOO_LARGE);
  Status s = p.TestInvalidResponse(req, &resp, &rpc);
  ASSERT_STR_CONTAINS(s.ToString(), "Network error: RPC frame had a length of");
}

// Test sending a call which isn't implemented by the server.
TEST_F(RpcStubTest, TestCallMissingMethod) {
  Proxy p(client_messenger_, server_addr_, CalculatorService::static_service_name());

  Status s = DoTestSyncCall(p, "DoesNotExist");
  ASSERT_TRUE(s.IsRemoteError()) << "Bad status: " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "with an invalid method name: DoesNotExist");
}

TEST_F(RpcStubTest, TestApplicationError) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  RpcController controller;
  SleepRequestPB req;
  SleepResponsePB resp;
  req.set_sleep_micros(1);
  req.set_return_app_error(true);
  Status s = p.Sleep(req, &resp, &controller);
  ASSERT_TRUE(s.IsRemoteError());
  EXPECT_EQ("Remote error: Got some error", s.ToString());
  EXPECT_EQ("message: \"Got some error\"\n"
            "[kudu.rpc_test.CalculatorError.app_error_ext] {\n"
            "  extra_error_data: \"some application-specific error data\"\n"
            "}\n",
            SecureDebugString(*controller.error_response()));
}

TEST_F(RpcStubTest, TestRpcPanic) {
  if (!FLAGS_is_panic_test_child) {
    // This is a poor man's death test. We call this same
    // test case, but set the above flag, and verify that
    // it aborted. gtest death tests don't work here because
    // there are already threads started up.
    vector<string> argv;
    string executable_path;
    CHECK_OK(env_->GetExecutablePath(&executable_path));
    argv.push_back(executable_path);
    argv.push_back("--is_panic_test_child");
    argv.push_back("--gtest_filter=RpcStubTest.TestRpcPanic");

    Subprocess subp(argv[0], argv);
    subp.ShareParentStderr(false);
    CHECK_OK(subp.Start());
    FILE* in = fdopen(subp.from_child_stderr_fd(), "r");
    PCHECK(in);

    // Search for string "Test method panicking!" somewhere in stderr
    char buf[1024];
    bool found_string = false;
    while (fgets(buf, sizeof(buf), in)) {
      if (strstr(buf, "Test method panicking!")) {
        found_string = true;
        break;
      }
    }
    CHECK(found_string);

    // Check return status
    int wait_status = 0;
    CHECK_OK(subp.Wait(&wait_status));
    CHECK(!WIFEXITED(wait_status)); // should not have been successful
    if (WIFSIGNALED(wait_status)) {
      CHECK_EQ(WTERMSIG(wait_status), SIGABRT);
    } else {
      // On some systems, we get exit status 134 from SIGABRT rather than
      // WIFSIGNALED getting flagged.
      CHECK_EQ(WEXITSTATUS(wait_status), 134);
    }
    return;
  } else {
    // Before forcing the panic, explicitly remove the test directory. This
    // should be safe; this test doesn't generate any data.
    CHECK_OK(env_->DeleteRecursively(test_dir_));

    // Make an RPC which causes the server to abort.
    CalculatorServiceProxy p(client_messenger_, server_addr_);
    RpcController controller;
    PanicRequestPB req;
    PanicResponsePB resp;
    p.Panic(req, &resp, &controller);
  }
}

struct AsyncSleep {
  AsyncSleep() : latch(1) {}

  RpcController rpc;
  SleepRequestPB req;
  SleepResponsePB resp;
  CountDownLatch latch;
};

TEST_F(RpcStubTest, TestDontHandleTimedOutCalls) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);
  vector<AsyncSleep*> sleeps;
  ElementDeleter d(&sleeps);

  // Send enough sleep calls to occupy the worker threads.
  for (int i = 0; i < n_worker_threads_; i++) {
    gscoped_ptr<AsyncSleep> sleep(new AsyncSleep);
    sleep->rpc.set_timeout(MonoDelta::FromSeconds(1));
    sleep->req.set_sleep_micros(100*1000); // 100ms
    p.SleepAsync(sleep->req, &sleep->resp, &sleep->rpc,
                 boost::bind(&CountDownLatch::CountDown, &sleep->latch));
    sleeps.push_back(sleep.release());
  }

  // We asynchronously sent the RPCs above, but the RPCs might still
  // be in the queue. Because the RPC we send next has a lower timeout,
  // it would take priority over the long-timeout RPCs. So, we have to
  // wait until the above RPCs are being processed before we continue
  // the test.
  const Histogram* queue_time_metric = service_pool_->IncomingQueueTimeMetricForTests();
  while (queue_time_metric->TotalCount() < n_worker_threads_) {
    SleepFor(MonoDelta::FromMilliseconds(1));
  }

  // Send another call with a short timeout. This shouldn't get processed, because
  // it'll get stuck in the queue for longer than its timeout.
  RpcController rpc;
  SleepRequestPB req;
  SleepResponsePB resp;
  req.set_sleep_micros(1000);
  rpc.set_timeout(MonoDelta::FromMilliseconds(1));
  Status s = p.Sleep(req, &resp, &rpc);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  for (AsyncSleep* s : sleeps) {
    s->latch.Wait();
  }

  // Verify that the timedout call got short circuited before being processed.
  const Counter* timed_out_in_queue = service_pool_->RpcsTimedOutInQueueMetricForTests();
  ASSERT_EQ(1, timed_out_in_queue->value());
}

// Test which ensures that the RPC queue accepts requests with the earliest
// deadline first (EDF), and upon overflow rejects requests with the latest deadlines.
//
// In particular, this simulates a workload experienced with Impala where the local
// impalad would spawn more scanner threads than the total number of handlers plus queue
// slots, guaranteeing that some of those clients would see SERVER_TOO_BUSY rejections on
// scan requests and be forced to back off and retry.  Without EDF scheduling, we saw that
// the "unlucky" threads that got rejected would likely continue to get rejected upon
// retries, and some would be starved continually until they missed their overall deadline
// and failed the query.
//
// With EDF scheduling, the retries take priority over the original requests (because
// they retain their original deadlines). This prevents starvation of unlucky threads.
TEST_F(RpcStubTest, TestEarliestDeadlineFirstQueue) {
  const int num_client_threads = service_queue_length_ + n_worker_threads_ + 5;
  vector<std::thread> threads;
  vector<int> successes(num_client_threads);
  std::atomic<bool> done(false);
  for (int thread_id = 0; thread_id < num_client_threads; thread_id++) {
    threads.emplace_back([&, thread_id] {
        Random rng(thread_id);
        CalculatorServiceProxy p(client_messenger_, server_addr_);
        while (!done.load()) {
          // Set a deadline in the future. We'll keep using this same deadline
          // on each of our retries.
          MonoTime deadline = MonoTime::Now() + MonoDelta::FromSeconds(8);

          for (int attempt = 1; !done.load(); attempt++) {
            RpcController controller;
            SleepRequestPB req;
            SleepResponsePB resp;
            controller.set_deadline(deadline);
            req.set_sleep_micros(100000);
            Status s = p.Sleep(req, &resp, &controller);
            if (s.ok()) {
              successes[thread_id]++;
              break;
            }
            // We expect to get SERVER_TOO_BUSY errors because we have more clients than the
            // server has handlers and queue slots. No other errors are expected.
            CHECK(s.IsRemoteError() &&
                  controller.error_response()->code() == rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY)
                << "Unexpected RPC failure: " << s.ToString();
            // Randomized exponential backoff (similar to that done by the scanners in the Kudu
            // client.).
            int backoff = (0.5 + rng.NextDoubleFraction() * 0.5) * (std::min(1 << attempt, 1000));
            VLOG(1) << "backoff " << backoff << "ms";
            SleepFor(MonoDelta::FromMilliseconds(backoff));
          }
        }
      });
  }
  // Let the threads run for 5 seconds before stopping them.
  SleepFor(MonoDelta::FromSeconds(5));
  done.store(true);
  for (auto& t : threads) {
    t.join();
  }

  // Before switching to earliest-deadline-first scheduling, the results
  // here would typically look something like:
  //  1 1 0 1 10 17 6 1 12 12 17 10 8 7 12 9 16 15
  // With the fix, we see something like:
  //  9 9 9 8 9 9 9 9 9 9 9 9 9 9 9 9 9
  LOG(INFO) << "thread RPC success counts: " << successes;

  int sum = 0;
  int min = std::numeric_limits<int>::max();
  for (int x : successes) {
    sum += x;
    min = std::min(min, x);
  }
  int avg = sum / successes.size();
  ASSERT_GT(min, avg / 2)
      << "expected the least lucky thread to have at least half as many successes "
      << "as the average thread: min=" << min << " avg=" << avg;
}

TEST_F(RpcStubTest, TestDumpCallsInFlight) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);
  AsyncSleep sleep;
  sleep.req.set_sleep_micros(100 * 1000); // 100ms
  p.SleepAsync(sleep.req, &sleep.resp, &sleep.rpc,
               boost::bind(&CountDownLatch::CountDown, &sleep.latch));

  // Check the running RPC status on the client messenger.
  DumpRunningRpcsRequestPB dump_req;
  DumpRunningRpcsResponsePB dump_resp;
  dump_req.set_include_traces(true);

  ASSERT_OK(client_messenger_->DumpRunningRpcs(dump_req, &dump_resp));
  LOG(INFO) << "client messenger: " << SecureDebugString(dump_resp);
  ASSERT_EQ(1, dump_resp.outbound_connections_size());
  ASSERT_EQ(1, dump_resp.outbound_connections(0).calls_in_flight_size());
  ASSERT_EQ("Sleep", dump_resp.outbound_connections(0).calls_in_flight(0).
                        header().remote_method().method_name());
  ASSERT_GT(dump_resp.outbound_connections(0).calls_in_flight(0).micros_elapsed(), 0);

  // And the server messenger.
  // We have to loop this until we find a result since the actual call is sent
  // asynchronously off of the main thread (ie the server may not be handling it yet)
  for (int i = 0; i < 100; i++) {
    dump_resp.Clear();
    ASSERT_OK(server_messenger_->DumpRunningRpcs(dump_req, &dump_resp));
    if (dump_resp.inbound_connections_size() > 0 &&
        dump_resp.inbound_connections(0).calls_in_flight_size() > 0) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }

  LOG(INFO) << "server messenger: " << SecureDebugString(dump_resp);
  ASSERT_EQ(1, dump_resp.inbound_connections_size());
  ASSERT_EQ(1, dump_resp.inbound_connections(0).calls_in_flight_size());
  ASSERT_EQ("Sleep", dump_resp.inbound_connections(0).calls_in_flight(0).
                        header().remote_method().method_name());
  ASSERT_GT(dump_resp.inbound_connections(0).calls_in_flight(0).micros_elapsed(), 0);
  ASSERT_STR_CONTAINS(dump_resp.inbound_connections(0).calls_in_flight(0).trace_buffer(),
                      "Inserting onto call queue");
  sleep.latch.Wait();
}

TEST_F(RpcStubTest, TestDumpSampledCalls) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  // Issue two calls that fall into different latency buckets.
  AsyncSleep sleeps[2];
  sleeps[0].req.set_sleep_micros(150 * 1000); // 150ms
  sleeps[1].req.set_sleep_micros(1500 * 1000); // 1500ms

  for (auto& sleep : sleeps) {
    p.SleepAsync(sleep.req, &sleep.resp, &sleep.rpc,
                 boost::bind(&CountDownLatch::CountDown, &sleep.latch));
  }
  for (auto& sleep : sleeps) {
    sleep.latch.Wait();
  }

  // Dump the sampled RPCs and expect to see the calls
  // above.

  DumpRpczStoreResponsePB sampled_rpcs;
  server_messenger_->rpcz_store()->DumpPB(DumpRpczStoreRequestPB(), &sampled_rpcs);
  EXPECT_EQ(sampled_rpcs.methods_size(), 1);
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs),
                      "    metrics {\n"
                      "      key: \"test_sleep_us\"\n"
                      "      value: 150000\n"
                      "    }\n");
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs),
                      "    metrics {\n"
                      "      key: \"test_sleep_us\"\n"
                      "      value: 1500000\n"
                      "    }\n");
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs),
                      "    metrics {\n"
                      "      child_path: \"test_child\"\n"
                      "      key: \"related_trace_metric\"\n"
                      "      value: 1\n"
                      "    }");
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs), "SleepRequestPB");
  ASSERT_STR_CONTAINS(SecureDebugString(sampled_rpcs), "duration_ms");
}

namespace {
struct RefCountedTest : public RefCountedThreadSafe<RefCountedTest> {
};

// Test callback which takes a refcounted pointer.
// We don't use this parameter, but it's used to validate that the bound callback
// is cleared in TestCallbackClearedAfterRunning.
void MyTestCallback(CountDownLatch* latch, scoped_refptr<RefCountedTest> my_refptr) {
  latch->CountDown();
}
} // anonymous namespace

// Verify that, after a call has returned, no copy of the call's callback
// is held. This is important when the callback holds a refcounted ptr,
// since we expect to be able to release that pointer when the call is done.
TEST_F(RpcStubTest, TestCallbackClearedAfterRunning) {
  CalculatorServiceProxy p(client_messenger_, server_addr_);

  CountDownLatch latch(1);
  scoped_refptr<RefCountedTest> my_refptr(new RefCountedTest);
  RpcController controller;
  AddRequestPB req;
  req.set_x(10);
  req.set_y(20);
  AddResponsePB resp;
  p.AddAsync(req, &resp, &controller,
             boost::bind(MyTestCallback, &latch, my_refptr));
  latch.Wait();

  // The ref count should go back down to 1. However, we need to loop a little
  // bit, since the deref is happening on another thread. If the other thread gets
  // descheduled directly after calling our callback, we'd fail without these sleeps.
  for (int i = 0; i < 100 && !my_refptr->HasOneRef(); i++) {
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
  ASSERT_TRUE(my_refptr->HasOneRef());
}

// Regression test for KUDU-1409: if the client reactor thread is blocked (e.g due to a
// process-wide pause or a slow callback) then we should not cause RPC calls to time out.
TEST_F(RpcStubTest, DontTimeOutWhenReactorIsBlocked) {
  CHECK_EQ(client_messenger_->num_reactors(), 1)
      << "This test requires only a single reactor. Otherwise the injected sleep might "
      << "be scheduled on a different reactor than the RPC call.";

  CalculatorServiceProxy p(client_messenger_, server_addr_);

  // Schedule a 1-second sleep on the reactor thread.
  //
  // This will cause us the reactor to be blocked while the call response is received, and
  // still be blocked when the timeout would normally occur. Despite this, the call should
  // not time out.
  //
  //  0s         0.5s          1.2s     1.5s
  //  RPC call running
  //  |---------------------|
  //              Reactor blocked in sleep
  //             |----------------------|
  //                            \_ RPC would normally time out

  client_messenger_->ScheduleOnReactor([](const Status& s) {
      ThreadRestrictions::ScopedAllowWait allow_wait;
      SleepFor(MonoDelta::FromSeconds(1));
    }, MonoDelta::FromSeconds(0.5));

  RpcController controller;
  SleepRequestPB req;
  SleepResponsePB resp;
  req.set_sleep_micros(800 * 1000);
  controller.set_timeout(MonoDelta::FromMilliseconds(1200));
  ASSERT_OK(p.Sleep(req, &resp, &controller));
}

} // namespace rpc
} // namespace kudu
