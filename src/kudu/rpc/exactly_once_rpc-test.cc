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

#include "kudu/rpc/retriable_rpc.h"
#include "kudu/rpc/rpc-test-base.h"
#include "kudu/rpc/rpc.h"
#include "kudu/util/pb_util.h"

DECLARE_int64(remember_clients_ttl_ms);
DECLARE_int64(remember_responses_ttl_ms);
DECLARE_int64(result_tracker_gc_interval_ms);

using std::atomic_int;
using std::shared_ptr;
using std::unique_ptr;

namespace kudu {
namespace rpc {

namespace {

const char* kClientId = "test-client";

void AddRequestId(RpcController* controller,
                  const std::string& client_id,
                  ResultTracker::SequenceNumber sequence_number,
                  int64_t attempt_no) {
  unique_ptr<RequestIdPB> request_id(new RequestIdPB());
  request_id->set_client_id(client_id);
  request_id->set_seq_no(sequence_number);
  request_id->set_attempt_no(attempt_no);
  request_id->set_first_incomplete_seq_no(sequence_number);
  controller->SetRequestIdPB(std::move(request_id));
}

class TestServerPicker : public ServerPicker<CalculatorServiceProxy> {
 public:
  explicit TestServerPicker(CalculatorServiceProxy* proxy) : proxy_(proxy) {}

  void PickLeader(const ServerPickedCallback& callback, const MonoTime& deadline) override {
    callback.Run(Status::OK(), proxy_);
  }

  void MarkServerFailed(CalculatorServiceProxy*, const Status&) override {}
  void MarkReplicaNotLeader(CalculatorServiceProxy*) override {}
  void MarkResourceNotFound(CalculatorServiceProxy*) override {}

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

  void StartServer() {
    // Set up server.
    StartTestServerWithGeneratedCode(&server_addr_);
    client_messenger_ = CreateMessenger("Client");
    proxy_.reset(new CalculatorServiceProxy(
        client_messenger_, server_addr_, server_addr_.host()));
    test_picker_.reset(new TestServerPicker(proxy_.get()));
    request_tracker_.reset(new RequestTracker(kClientId));
    attempt_nos_ = 0;
  }

  // An exactly once adder that uses RetriableRpc to perform the requests.
  struct RetriableRpcExactlyOnceAdder {
    RetriableRpcExactlyOnceAdder(const scoped_refptr<TestServerPicker>& server_picker,
                     const scoped_refptr<RequestTracker>& request_tracker,
                     shared_ptr<Messenger> messenger,
                     int value,
                     int server_sleep = 0) : latch_(1) {
      MonoTime now = MonoTime::Now();
      now.AddDelta(MonoDelta::FromMilliseconds(10000));
      rpc_ = new CalculatorServiceRpc(server_picker,
                                      request_tracker,
                                      now,
                                      std::move(messenger),
                                      value,
                                      &latch_,
                                      server_sleep);
    }

    void Start() {
      CHECK_OK(kudu::Thread::Create(
                   "test",
                   "test",
                   &RetriableRpcExactlyOnceAdder::SleepAndSend, this, &thread));
    }

    void SleepAndSend() {
      rpc_->SendRpc();
      latch_.Wait();
    }

    CountDownLatch latch_;
    scoped_refptr<kudu::Thread> thread;
    CalculatorServiceRpc* rpc_;
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
      CHECK_OK(kudu::Thread::Create(
          "test",
          "test",
          &SimultaneousExactlyOnceAdder::SleepAndSend, this, &thread));
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
    scoped_refptr<kudu::Thread> thread;
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
    MonoTime run_until = MonoTime::Now();
    run_until.AddDelta(run_for);
    int counter = 0;
    while (MonoTime::Now() < run_until) {
      unique_ptr<RetriableRpcExactlyOnceAdder> adder(new RetriableRpcExactlyOnceAdder(
          test_picker_, request_tracker_, client_messenger_, 1,
          rand() % (2 * FLAGS_remember_responses_ttl_ms)));

      // This thread is used in the stress test where we're constantly running GC.
      // So, once we get a "success" response, it's likely that the result will be
      // GCed on the server side, and thus it's not safe to spuriously retry.
      adder->rpc_->sometimes_retry_successful_ = false;
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
    MonoTime run_until = MonoTime::Now();
    run_until.AddDelta(run_for);
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
  StartServer();
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
    int expected_incremental_usage = original_resp.SpaceUsed() + 200;

    int mem_consumption_after = mem_tracker_->consumption();
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
  StartServer();
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
      CHECK_OK(ThreadJoiner(adders[j]->thread.get()).Join());
    }
    CheckValueMatches(count);
  }
}

// Performs a series of requests in which each single request is attempted by multiple threads.
// On each iteration, after all the threads complete, we expect that the add operation was
// executed exactly once.
TEST_F(ExactlyOnceRpcTest, TestExactlyOnceSemanticsWithConcurrentUpdaters) {
  StartServer();
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
      CHECK_OK(ThreadJoiner(adders[j]->thread.get()).Join());
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

  StartServer();

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
  ASSERT_TRUE(s.IsRemoteError());
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

  StartServer();

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

  scoped_refptr<kudu::Thread> stubborn_thread;
  CHECK_OK(kudu::Thread::Create(
      "stubborn", "stubborn", &ExactlyOnceRpcTest::StubbornlyWriteTheSameRequestThread,
      this, stubborn_req_seq_num, stubborn_run_for, &stubborn_thread));

  scoped_refptr<kudu::Thread> write_thread;
  CHECK_OK(kudu::Thread::Create(
      "write", "write", &ExactlyOnceRpcTest::DoLongWritesThread,
      this, writes_run_for, &write_thread));

  write_thread->Join();
  stubborn_thread->Join();

  // Within a few seconds, the consumption should be back to zero.
  // Really, this should be within 100ms, but we'll give it a bit of
  // time to avoid test flakiness.
  AssertEventually([&]() {
      ASSERT_EQ(0, mem_tracker_->consumption());
    }, MonoDelta::FromSeconds(5));
  NO_PENDING_FATALS();
}


} // namespace rpc
} // namespace kudu
