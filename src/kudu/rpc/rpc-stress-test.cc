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
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc-test-base.h"

using std::atomic_int;
using std::shared_ptr;
using std::unique_ptr;

namespace kudu {
namespace rpc {

namespace {

const char* kClientId = "test-client";

void AddRequestId(RpcController* controller,
                  ResultTracker::SequenceNumber sequence_number,
                  int64_t attempt_no) {
  unique_ptr<RequestIdPB> request_id(new RequestIdPB());
  request_id->set_client_id(kClientId);
  request_id->set_seq_no(sequence_number);
  request_id->set_attempt_no(attempt_no);
  request_id->set_first_incomplete_seq_no(sequence_number);
  controller->SetRequestIdPB(std::move(request_id));
}

} // namespace

class RpcStressTest : public RpcTestBase {
 public:
  void SetUp() override {
    RpcTestBase::SetUp();
    // Set up server.
    StartTestServerWithGeneratedCode(&server_addr_);
    client_messenger_ = CreateMessenger("Client");
    proxy_.reset(new CalculatorServiceProxy(client_messenger_, server_addr_));
    attempt_nos_ = 0;
  }

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
      AddRequestId(&controller, sequence_number, attempt_no);
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

 protected:
  Sockaddr server_addr_;
  atomic_int attempt_nos_;
  shared_ptr<Messenger> client_messenger_;
  std::unique_ptr<CalculatorServiceProxy> proxy_;
};

// Tests that we get exactly once semantics on RPCs when we send a bunch of requests with the
// same sequence number as previous request.
TEST_F(RpcStressTest, TestExactlyOnceSemanticsAfterRpcCompleted) {
  ExactlyOnceResponsePB original_resp;
  {
    RpcController controller;
    ExactlyOnceRequestPB req;
    req.set_value_to_add(1);

    // Assign id 0.
    AddRequestId(&controller, 0, 0);

    // Send the request the first time.
    ASSERT_OK(proxy_->AddExactlyOnce(req, &original_resp, &controller));
  }

  // Now repeat the rpc 10 times, using the same sequence number, none of these should be executed
  // and they should get the same response back.
  for (int i = 0; i < 10; i++) {
    RpcController controller;
    controller.set_timeout(MonoDelta::FromSeconds(20));
    ExactlyOnceRequestPB req;
    req.set_value_to_add(1);
    ExactlyOnceResponsePB resp;
    AddRequestId(&controller, 0, i + 1);
    ASSERT_OK(proxy_->AddExactlyOnce(req, &resp, &controller));
    ASSERT_EQ(resp.current_val(), 1);
    ASSERT_EQ(resp.current_time_micros(), original_resp.current_time_micros());
  }
}

// Performs a series of requests in which each single request is attempted by multiple threads.
// On each iteration, after all the threads complete, we expect that the add operation was
// executed exactly once.
TEST_F(RpcStressTest, TestExactlyOnceSemanticsWithConcurrentUpdaters) {
  int kNumIterations = 10;
  int kNumThreads = 10;

  if (AllowSlowTests()) {
    kNumIterations = 100;
    kNumThreads = 100;
  }

  for (int i = 0; i < kNumIterations; i ++) {
    vector<unique_ptr<SimultaneousExactlyOnceAdder>> adders;
    for (int j = 0; j < kNumThreads; j++) {
      unique_ptr<SimultaneousExactlyOnceAdder> adder(
          new SimultaneousExactlyOnceAdder(proxy_.get(), i, 1,
                                           rand() % 20,
                                           rand() % 10,
                                           attempt_nos_.fetch_add(1)));
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
  }
}

} // namespace rpc
} // namespace kudu
