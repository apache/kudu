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

#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/retriable_rpc.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rtest.pb.h"
#include "kudu/rpc/rtest.proxy.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int64(remember_clients_ttl_ms);
DECLARE_int64(remember_responses_ttl_ms);
DECLARE_int64(result_tracker_gc_interval_ms);

using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using std::atomic_int;
using std::shared_ptr;
using std::thread;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace rpc {

class Messenger;

namespace {

const char* kClientId = "test-client";

void AddRequestIdImpl(RpcController* ctl,
                      const std::string& client_id,
                      ResultTracker::SequenceNumber seq_num,
                      ResultTracker::SequenceNumber first_incomplete_seq_num,
                      int64_t attempt_num = 0) {
  unique_ptr<RequestIdPB> request_id(new RequestIdPB);
  request_id->set_client_id(client_id);
  request_id->set_seq_no(seq_num);
  request_id->set_first_incomplete_seq_no(first_incomplete_seq_num);
  request_id->set_attempt_no(attempt_num);
  ctl->SetRequestIdPB(std::move(request_id));
}

void AddRequestId(RpcController* ctl,
                  const std::string& client_id,
                  ResultTracker::SequenceNumber seq_num,
                  int64_t attempt_num = 0) {
  return AddRequestIdImpl(ctl, client_id, seq_num, seq_num, attempt_num);
}

class TestServerPicker : public ServerPicker<CalculatorServiceProxy> {
 public:
  explicit TestServerPicker(CalculatorServiceProxy* proxy) : proxy_(proxy) {}

  void PickLeader(const ServerPickedCallback& callback, const MonoTime& /*deadline*/) override {
    callback(Status::OK(), proxy_);
  }

  void MarkServerFailed(CalculatorServiceProxy* /*server*/, const Status& /*status*/) override {}
  void MarkReplicaNotLeader(CalculatorServiceProxy* /*replica*/) override {}
  void MarkResourceNotFound(CalculatorServiceProxy* /*replica*/) override {}

 private:
  CalculatorServiceProxy* proxy_;
};

} // anonymous namespace

class CalculatorServiceRpc : public RetriableRpc<CalculatorServiceProxy,
                                                 ExactlyOnceRequestPB,
                                                 ExactlyOnceResponsePB> {
 public:
  CalculatorServiceRpc(const scoped_refptr<TestServerPicker>& server_picker,
                       const scoped_refptr<RequestTracker>& request_tracker,
                       const MonoTime& deadline,
                       shared_ptr<Messenger> messenger,
                       int value,
                       CountDownLatch* latch,
                       int server_sleep = 0)
      : RetriableRpc(server_picker, request_tracker, deadline, std::move(messenger)),
        latch_(latch) {
    req_.set_value_to_add(value);
    req_.set_randomly_fail(true);
    req_.set_sleep_for_ms(server_sleep);
  }

  void Try(CalculatorServiceProxy* server, const ResponseCallback& callback) override {
    server->AddExactlyOnceAsync(req_,
                                &resp_,
                                mutable_retrier()->mutable_controller(),
                                callback);
  }

  RetriableRpcStatus AnalyzeResponse(const Status& rpc_cb_status) override {
    // We shouldn't get errors from the server/rpc system since we set a high timeout.
    CHECK_OK(rpc_cb_status);

    if (!mutable_retrier()->controller().status().ok()) {
      CHECK(mutable_retrier()->controller().status().IsRemoteError());
      if (mutable_retrier()->controller().error_response()->code()
          == ErrorStatusPB::ERROR_REQUEST_STALE) {
        return { RetriableRpcStatus::NON_RETRIABLE_ERROR,
              mutable_retrier()->controller().status() };
      }
      return { RetriableRpcStatus::SERVICE_UNAVAILABLE,
               mutable_retrier()->controller().status() };
    }

    // If the controller is not finished we're in the ReplicaFoundCb() callback.
    // Return ok to proceed with the call to the server.
    if (!mutable_retrier()->mutable_controller()->finished()) {
      return { RetriableRpcStatus::OK, Status::OK() };
    }

    // If we've received a response in the past, all following responses must
    // match.
    if (!successful_response_.IsInitialized()) {
      successful_response_.CopyFrom(resp_);
    } else {
      CHECK_EQ(SecureDebugString(successful_response_),
               SecureDebugString(resp_));
    }

    if (sometimes_retry_successful_) {
      // Still report errors, with some probability. This will cause requests to
      // be retried. Since the requests were originally successful we should get
      // the same reply back.
      int random = rand() % 4;
      switch (random) {
        case 0:
          return { RetriableRpcStatus::SERVICE_UNAVAILABLE,
                   Status::RemoteError("") };
        case 1:
          return { RetriableRpcStatus::RESOURCE_NOT_FOUND,
                   Status::RemoteError("") };
        case 2:
          return { RetriableRpcStatus::SERVER_NOT_ACCESSIBLE,
                   Status::RemoteError("") };
        case 3:
          return { RetriableRpcStatus::OK, Status::OK() };
        default: LOG(FATAL) << "Unexpected value";
      }
    }
    return { RetriableRpcStatus::OK, Status::OK() };
  }

  void Finish(const Status& status) override {
    CHECK_OK(status);
    latch_->CountDown();
    delete this;
  }

  std::string ToString() const override { return "test-rpc"; }
  CountDownLatch* latch_;
  ExactlyOnceResponsePB successful_response_;
  bool sometimes_retry_successful_ = true;
};

class ExactlyOnceRpcTest : public RpcTestBase {
 public:
  void SetUp() override {
    RpcTestBase::SetUp();
    SeedRandom();
  }

  Status StartServer() {
    // Set up server.
    RETURN_NOT_OK(StartTestServerWithGeneratedCode(&server_addr_));
    RETURN_NOT_OK(CreateMessenger("Client", &client_messenger_));
    proxy_.reset(new CalculatorServiceProxy(
        client_messenger_, server_addr_, server_addr_.host()));
    test_picker_.reset(new TestServerPicker(proxy_.get()));
    request_tracker_.reset(new RequestTracker(kClientId));
    attempt_nos_ = 0;

    return Status::OK();
  }

  // An exactly once adder that uses RetriableRpc to perform the requests.
  struct RetriableRpcExactlyOnceAdder {
    RetriableRpcExactlyOnceAdder(const scoped_refptr<TestServerPicker>& server_picker,
                     const scoped_refptr<RequestTracker>& request_tracker,
                     shared_ptr<Messenger> messenger,
                     int value,
                     int server_sleep = 0) : latch(1) {
      const auto deadline = MonoTime::Now() + MonoDelta::FromSeconds(10);
      rpc = new CalculatorServiceRpc(server_picker,
                                     request_tracker,
                                     deadline,
                                     std::move(messenger),
                                     value,
                                     &latch,
                                     server_sleep);
    }

    void Start() {
      thr = thread([this]() { this->SleepAndSend(); });
    }

    void SleepAndSend() {
      rpc->SendRpc();
      latch.Wait();
    }

    CountDownLatch latch;
    thread thr;
    CalculatorServiceRpc* rpc;
  };

  // An exactly once adder that sends multiple, simultaneous calls, to the server
  // and makes sure that only one of the calls was successful.
  struct SimultaneousExactlyOnceAdder {
    SimultaneousExactlyOnceAdder(CalculatorServiceProxy* p,
                                 ResultTracker::SequenceNumber sequence_number,
                                 int value,
                                 uint64_t client_sleep,
                                 uint64_t server_sleep,
                                 int64_t attempt_no)
       : proxy(p),
         client_sleep_for_ms(client_sleep) {
      req.set_value_to_add(value);
      req.set_sleep_for_ms(server_sleep);
      AddRequestId(&controller, kClientId, sequence_number, attempt_no);
    }

    void Start() {
      thr = thread([this]() { this->SleepAndSend(); });
    }

    // Sleeps the preset number of msecs before sending the call.
    void SleepAndSend() {
      usleep(client_sleep_for_ms * 1000);
      controller.set_timeout(MonoDelta::FromSeconds(20));
      CHECK_OK(proxy->AddExactlyOnce(req, &resp, &controller));
    }

    CalculatorServiceProxy* const proxy;
    const uint64_t client_sleep_for_ms;
    RpcController controller;
    ExactlyOnceRequestPB req;
    ExactlyOnceResponsePB resp;
    thread thr;
  };


  void CheckValueMatches(int expected_value) {
    RpcController controller;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(0);
    ExactlyOnceResponsePB resp;
    RequestTracker::SequenceNumber seq_no;
    CHECK_OK(request_tracker_->NewSeqNo(&seq_no));
    AddRequestId(&controller, kClientId, seq_no, 0);
    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &controller));
    ASSERT_EQ(resp.current_val(), expected_value);
    request_tracker_->RpcCompleted(seq_no);
  }


  // This continuously issues calls to the server, that often last longer than
  // 'remember_responses_ttl_ms', making sure that we don't get errors back.
  void DoLongWritesThread(MonoDelta run_for) {
    const auto run_until = MonoTime::Now() + run_for;
    int counter = 0;
    while (MonoTime::Now() < run_until) {
      unique_ptr<RetriableRpcExactlyOnceAdder> adder(new RetriableRpcExactlyOnceAdder(
          test_picker_, request_tracker_, client_messenger_, 1,
          rand() % (2 * FLAGS_remember_responses_ttl_ms)));

      // This thread is used in the stress test where we're constantly running GC.
      // So, once we get a "success" response, it's likely that the result will be
      // GCed on the server side, and thus it's not safe to spuriously retry.
      adder->rpc->sometimes_retry_successful_ = false;
      adder->SleepAndSend();
      SleepFor(MonoDelta::FromMilliseconds(rand() % 10));
      counter++;
    }
    ExactlyOnceResponsePB response;
    ResultTracker::SequenceNumber sequence_number;
    CHECK_OK(request_tracker_->NewSeqNo(&sequence_number));
    CHECK_OK(MakeAddCall(sequence_number, 0, &response));
    CHECK_EQ(response.current_val(), counter);
    request_tracker_->RpcCompleted(sequence_number);
  }

  // Stubbornly sends the same request to the server, this should observe three states.
  // The request should be successful at first, then its result should be GCed and the
  // client should be GCed.
  void StubbornlyWriteTheSameRequestThread(ResultTracker::SequenceNumber sequence_number,
                                           MonoDelta run_for) {
    const auto run_until = MonoTime::Now() + run_for;
    // Make an initial request, so that we get a response to compare to.
    ExactlyOnceResponsePB original_response;
    CHECK_OK(MakeAddCall(sequence_number, 0, &original_response));

    // Now repeat the same request. At first we should get the same response, then the result
    // should be GCed and we should get STALE back. Finally the request should succeed again
    // but we should get a new response.
    bool result_gced = false;
    bool client_gced = false;
    while (MonoTime::Now() < run_until) {
      ExactlyOnceResponsePB response;
      Status s = MakeAddCall(sequence_number, 0, &response);
      if (s.ok()) {
        if (!result_gced) {
          CHECK_EQ(SecureDebugString(response), SecureDebugString(original_response));
        } else {
          client_gced = true;
          CHECK_NE(SecureDebugString(response), SecureDebugString(original_response));
        }
        SleepFor(MonoDelta::FromMilliseconds(rand() % 10));
      } else if (s.IsRemoteError()) {
        result_gced = true;
        SleepFor(MonoDelta::FromMilliseconds(FLAGS_remember_clients_ttl_ms * 2));
      }
    }
    CHECK(result_gced);
    CHECK(client_gced);
  }

  Status MakeAddCall(ResultTracker::SequenceNumber sequence_number,
                     int value_to_add,
                     ExactlyOnceResponsePB* response,
                     int attempt_no = -1) {
    RpcController controller;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(value_to_add);
    if (attempt_no == -1) attempt_no = attempt_nos_.fetch_add(1);
    AddRequestId(&controller, kClientId, sequence_number, attempt_no);
    Status s = proxy_->AddExactlyOnce(req, response, &controller);
    return s;
  }

 protected:
  Sockaddr server_addr_;
  atomic_int attempt_nos_;
  shared_ptr<Messenger> client_messenger_;
  std::unique_ptr<CalculatorServiceProxy> proxy_;
  scoped_refptr<TestServerPicker> test_picker_;
  scoped_refptr<RequestTracker> request_tracker_;
};

// Tests that we get exactly once semantics on RPCs when we send a bunch of requests with the
// same sequence number as previous requests.
TEST_F(ExactlyOnceRpcTest, TestExactlyOnceSemanticsAfterRpcCompleted) {
  ASSERT_OK(StartServer());
  ExactlyOnceResponsePB original_resp;
  int mem_consumption = mem_tracker_->consumption();
  {
    RpcController controller;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(1);

    // Assign id 0.
    AddRequestId(&controller, kClientId, 0, 0);

    // Send the request the first time.
    ASSERT_OK(proxy_->AddExactlyOnce(req, &original_resp, &controller));

    // The incremental usage of a new client is the size of the response itself
    // plus some fixed overhead for the client-tracking structure.
    size_t expected_incremental_usage = original_resp.SpaceUsedLong() + 200;

    size_t mem_consumption_after = mem_tracker_->consumption();
    ASSERT_GT(mem_consumption_after - mem_consumption, expected_incremental_usage);
    mem_consumption = mem_consumption_after;
  }

  // Now repeat the rpc 10 times, using the same sequence number, none of these should be executed
  // and they should get the same response back.
  for (int i = 0; i < 10; i++) {
    RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(20));
    ExactlyOnceRequestPB req;
    req.set_value_to_add(1);
    ExactlyOnceResponsePB resp;
    AddRequestId(&controller, kClientId, 0, i + 1);
    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &controller));
    ASSERT_EQ(resp.current_val(), 1);
    ASSERT_EQ(resp.current_time_micros(), original_resp.current_time_micros());
    // Sleep to give the MemTracker time to update -- we don't expect any update,
    // but if we had a bug here, we'd only see it with this sleep.
    SleepFor(MonoDelta::FromMilliseconds(100));
    // We shouldn't have consumed any more memory since the responses were cached.
    ASSERT_EQ(mem_consumption, mem_tracker_->consumption());
  }

  // Making a new request, from a new client, should double the memory consumption.
  {
    RpcController controller;
    ExactlyOnceRequestPB req;
    ExactlyOnceResponsePB resp;
    req.set_value_to_add(1);

    // Assign id 0.
    AddRequestId(&controller, "test-client2", 0, 0);

    // Send the first request for this new client.
    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &controller));
    ASSERT_EQ(mem_tracker_->consumption(), mem_consumption * 2);
  }
}

// Performs a series of requests in which each single request is attempted multiple times, as
// the server side is instructed to spuriously fail attempts.
// In CalculatorServiceRpc we sure that the same response is returned by all retries and,
// after all the rpcs are done, we make sure that final result is the expected one.
TEST_F(ExactlyOnceRpcTest, TestExactlyOnceSemanticsWithReplicatedRpc) {
  ASSERT_OK(StartServer());
  int kNumIterations = 10;
  int kNumRpcs = 10;

  if (AllowSlowTests()) {
    kNumIterations = 100;
    kNumRpcs = 100;
  }

  int count = 0;
  for (int i = 0; i < kNumIterations; i ++) {
    vector<unique_ptr<RetriableRpcExactlyOnceAdder>> adders;
    for (int j = 0; j < kNumRpcs; j++) {
      unique_ptr<RetriableRpcExactlyOnceAdder> adder(
          new RetriableRpcExactlyOnceAdder(test_picker_, request_tracker_, client_messenger_, j));
      adders.push_back(std::move(adder));
      adders[j]->Start();
      count += j;
    }
    for (int j = 0; j < kNumRpcs; j++) {
      adders[j]->thr.join();
    }
    CheckValueMatches(count);
  }
}

// Performs a series of requests in which each single request is attempted by multiple threads.
// On each iteration, after all the threads complete, we expect that the add operation was
// executed exactly once.
TEST_F(ExactlyOnceRpcTest, TestExactlyOnceSemanticsWithConcurrentUpdaters) {
  ASSERT_OK(StartServer());
  int kNumIterations = 10;
  int kNumThreads = 10;

  if (AllowSlowTests()) {
    kNumIterations = 100;
    kNumThreads = 100;
  }

  ResultTracker::SequenceNumber sequence_number = 0;
  int memory_consumption_initial = mem_tracker_->consumption();
  int single_response_size = 0;

  // Measure memory consumption for a single response from the same client.
  ExactlyOnceResponsePB resp;
  ASSERT_OK(MakeAddCall(sequence_number, 1, &resp));

  for (int i = 1; i <= kNumIterations; i ++) {
    vector<unique_ptr<SimultaneousExactlyOnceAdder>> adders;
    for (int j = 0; j < kNumThreads; j++) {
      unique_ptr<SimultaneousExactlyOnceAdder> adder(
          new SimultaneousExactlyOnceAdder(proxy_.get(),
                                           i, // sequence number
                                           1, // value
                                           rand() % 20, // client_sleep
                                           rand() % 10, // server_sleep
                                           attempt_nos_.fetch_add(1))); // attempt number
      adders.push_back(std::move(adder));
      adders[j]->Start();
    }
    uint64_t time_micros = 0;
    for (int j = 0; j < kNumThreads; j++) {
      adders[j]->thr.join();
      ASSERT_EQ(adders[j]->resp.current_val(), i + 1);
      if (time_micros == 0) {
        time_micros = adders[j]->resp.current_time_micros();
      } else {
        ASSERT_EQ(adders[j]->resp.current_time_micros(), time_micros);
      }
    }

    // After all adders finished we should at least the size of one more response.
    // The actual size depends of multiple factors, for instance, how many calls were "attached"
    // (which is timing dependent) so we can't be more precise than this.
    ASSERT_GT(mem_tracker_->consumption(),
              memory_consumption_initial + single_response_size * i);
  }
}

TEST_F(ExactlyOnceRpcTest, TestExactlyOnceSemanticsGarbageCollection) {
  FLAGS_remember_clients_ttl_ms = 500;
  FLAGS_remember_responses_ttl_ms = 100;

  ASSERT_OK(StartServer());

  // Make a request.
  ExactlyOnceResponsePB original;
  ResultTracker::SequenceNumber sequence_number = 0;
  ASSERT_OK(MakeAddCall(sequence_number, 1, &original));

  // Making the same request again, should return the same response.
  ExactlyOnceResponsePB resp;
  ASSERT_OK(MakeAddCall(sequence_number, 1, &resp));
  ASSERT_EQ(SecureShortDebugString(original), SecureShortDebugString(resp));

  // Now sleep for 'remember_responses_ttl_ms' and run GC, we should then
  // get a STALE back.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_remember_responses_ttl_ms));
  int64_t memory_consumption = mem_tracker_->consumption();
  result_tracker_->GCResults();
  ASSERT_LT(mem_tracker_->consumption(), memory_consumption);

  resp.Clear();
  Status s = MakeAddCall(sequence_number, 1, &resp);
  ASSERT_TRUE(s.IsRemoteError()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "is stale");

  // Sleep again, this time for 'remember_clients_ttl_ms' and run GC again.
  // The request should be successful, but its response should be a new one.
  SleepFor(MonoDelta::FromMilliseconds(FLAGS_remember_clients_ttl_ms));
  memory_consumption = mem_tracker_->consumption();
  result_tracker_->GCResults();
  ASSERT_LT(mem_tracker_->consumption(), memory_consumption);

  resp.Clear();
  ASSERT_OK(MakeAddCall(sequence_number, 1, &resp));
  ASSERT_NE(SecureShortDebugString(resp), SecureShortDebugString(original));
}

// This test creates a thread continuously making requests to the server, some lasting longer
// than the GC period, at the same time it runs GC, making sure that the corresponding
// CompletionRecords/ClientStates are not deleted from underneath the ongoing requests.
// This also creates a thread that runs GC very frequently and another thread that sends the
// same request over and over and observes the possible states: request is ok, request is stale
// request is ok again (because the client was forgotten).
TEST_F(ExactlyOnceRpcTest, TestExactlyOnceSemanticsGarbageCollectionStressTest) {
  FLAGS_remember_clients_ttl_ms = 100;
  FLAGS_remember_responses_ttl_ms = 10;
  FLAGS_result_tracker_gc_interval_ms = 10;

  ASSERT_OK(StartServer());

  // The write thread runs for a shorter period to make sure client GC has a
  // chance to run.
  MonoDelta writes_run_for = MonoDelta::FromSeconds(2);
  MonoDelta stubborn_run_for = MonoDelta::FromSeconds(3);
  if (AllowSlowTests()) {
    writes_run_for = MonoDelta::FromSeconds(10);
    stubborn_run_for = MonoDelta::FromSeconds(11);
  }

  result_tracker_->StartGCThread();

  // Assign the first sequence number (0) to the 'stubborn writes' thread.
  // This thread will keep making RPCs with this sequence number while
  // the 'write_thread' will make normal requests with increasing sequence
  // numbers.
  ResultTracker::SequenceNumber stubborn_req_seq_num;
  CHECK_OK(request_tracker_->NewSeqNo(&stubborn_req_seq_num));
  ASSERT_EQ(stubborn_req_seq_num, 0);

  thread stubborn_thread([this, stubborn_req_seq_num, stubborn_run_for]() {
    this->StubbornlyWriteTheSameRequestThread(stubborn_req_seq_num, stubborn_run_for);
  });

  thread write_thread([this, writes_run_for]() {
    this->DoLongWritesThread(writes_run_for);
  });

  write_thread.join();
  stubborn_thread.join();

  // Within a few seconds, the consumption should be back to zero.
  // Really, this should be within 100ms, but we'll give it a bit of
  // time to avoid test flakiness.
  AssertEventually([&]() {
      ASSERT_EQ(0, mem_tracker_->consumption());
    }, MonoDelta::FromSeconds(5));
  NO_PENDING_FATALS();
}

// Requests might be re-ordered due to rare network conditions if sent to
// the server as separate WriteRequest RPCs or due to scheduling anomalies.
// The latter can happen because the requests are dispatched to a multitude
// of worker threads from a queue. This test scenario makes sure that if such
// a situation occurs, the exactly-once logic behaves as expected.
TEST_F(ExactlyOnceRpcTest, ReorderedRequestsOne) {
  ASSERT_OK(StartServer());

  // The request with ID 1 is dispatched first.
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(1);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/1, /*first_incomplete_seq_num=*/0, /*attempt_num=*/0);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(1, resp.current_val());
  }

  // The processing of the request with ID 0 starts a bit later.
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(1);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/0, /*first_incomplete_seq_num=*/0, /*attempt_num=*/0);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(2, resp.current_val());
  }

  // And here comes the retransmission of the request with ID 1
  // (attempt_num = 1). The response should be the same as the server sent for
  // attempt_num == 0, even if the current value of the counter is 2
  // since the request with ID 0 has been already received and processed by
  // the server.
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(1);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/1, /*first_incomplete_seq_num=*/0, /*attempt_num=*/1);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(1, resp.current_val());
  }

  // Now send in a request whose ID is a gap away from the last request ID
  // seen from this client, pretending the client has received responses
  // for requests 0 and 1, and there are some requests in-flight the server
  // hasn't seen yet.
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(10);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/10, /*first_incomplete_seq_num=*/8, /*attempt_num=*/0);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(12, resp.current_val());
  }
}

// This is similar to ReorderRequestsSimple above, but with more variatons.
TEST_F(ExactlyOnceRpcTest, ReorderedRequestsTwo) {
  ASSERT_OK(StartServer());

  // The request with ID 1 is dispatched first.
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(2);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/1, /*first_incomplete_seq_num=*/0, /*attempt_num=*/0);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(2, resp.current_val());
  }

  // The processing of the request with ID 0 starts a bit later.
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(1);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/0, /*first_incomplete_seq_num=*/0, /*attempt_num=*/0);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(3, resp.current_val());
  }

  // However, the request with ID 0 is being responded first even if its
  // processing started later because the worker thread that was processing
  // the request with ID 1 had stuck for a while. So, request with ID 2
  // has first_incomplete_seq_num == 1 on its attempt_num == 0.
  // And with that, the two attempts of the request with ID 2 are re-ordered
  // as well when the exactly-once logic receives them, so the request
  // with attempt_num == 1 has first_incomplete_seq_num == 2.
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(3);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/2, /*first_incomplete_seq_num=*/2, /*attempt_num=*/1);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(6, resp.current_val());
  }
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(3);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/2, /*first_incomplete_seq_num=*/1, /*attempt_num=*/0);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(6, resp.current_val());
  }

  // Send two more requests to make sure that the exactly-once state machine
  // isn't screwed at this point.
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(4);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/3, /*first_incomplete_seq_num=*/2, /*attempt_num=*/0);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(10, resp.current_val());
  }
  {
    RpcController ctl;
    ExactlyOnceResponsePB resp;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(1);

    AddRequestIdImpl(&ctl, kClientId,
        /*seq_num=*/4, /*first_incomplete_seq_num=*/3, /*attempt_num=*/0);

    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &ctl));
    ASSERT_EQ(11, resp.current_val());
  }
}

} // namespace rpc
} // namespace kudu
