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

#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(subprocess_request_queue_size_bytes);
DECLARE_int32(subprocess_response_queue_size_bytes);
DECLARE_int32(subprocess_num_responder_threads);
DECLARE_int32(subprocess_timeout_secs);

using google::protobuf::Any;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace subprocess {

namespace {

// Creates a subprocess request with the given payload and ms to sleep.
SubprocessRequestPB CreateEchoSubprocessRequestPB(const string& payload,
                                                  int sleep_ms = 0) {
  SubprocessRequestPB request;
  EchoRequestPB echo_request;
  echo_request.set_data(payload);
  echo_request.set_sleep_ms(sleep_ms);
  unique_ptr<Any> any(new Any);
  any->PackFrom(echo_request);
  request.set_allocated_request(any.release());
  return request;
}

const char* kHello = "hello world";

} // anonymous namespace

class SubprocessServerTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(ResetSubprocessServer());
  }

  // Resets the subprocess server to account for any new configuration.
  Status ResetSubprocessServer(int java_queue_size = 0,
                               int java_parser_threads = 0) {
    // Set up a subprocess server pointing at the kudu-subprocess-echo.jar that
    // contains an echo server.
    string exe;
    RETURN_NOT_OK(env_->GetExecutablePath(&exe));
    const string bin_dir = DirName(exe);
    string java_home;
    RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home));
    vector<string> argv = {
      Substitute("$0/bin/java", java_home),
      "-jar", Substitute("$0/kudu-subprocess-echo.jar", bin_dir)
    };
    if (java_queue_size > 0) {
      argv.emplace_back("q");
      argv.emplace_back(std::to_string(java_queue_size));
    }
    if (java_parser_threads > 0) {
      argv.emplace_back("p");
      argv.emplace_back(std::to_string(java_parser_threads));
    }
    server_ = make_shared<SubprocessServer>(std::move(argv));
    return server_->Init();
  }

 protected:
  shared_ptr<SubprocessServer> server_;
};

TEST_F(SubprocessServerTest, TestBasicCall) {
  SubprocessRequestPB request = CreateEchoSubprocessRequestPB(kHello);
  SubprocessResponsePB response;
  ASSERT_OK(server_->Execute(&request, &response));
  EchoResponsePB echo_response;
  ASSERT_TRUE(response.response().UnpackTo(&echo_response));
  ASSERT_EQ(echo_response.data(), kHello);
}

// Test sending many requests concurrently.
TEST_F(SubprocessServerTest, TestManyConcurrentCalls) {
  constexpr int kNumThreads = 20;
  constexpr int kNumPerThread = 200;
  const string kEchoDataPrefix = "Do the hokey pokey, turn yourself around!";
  vector<vector<SubprocessRequestPB>> requests(kNumThreads,
      vector<SubprocessRequestPB>(kNumPerThread));
  vector<vector<SubprocessResponsePB>> responses(kNumThreads,
      vector<SubprocessResponsePB>(kNumPerThread));
  for (int t = 0; t < kNumThreads; t++) {
    for (int i = 0; i < kNumPerThread; i++) {
      requests[t][i] = CreateEchoSubprocessRequestPB(
          Substitute("$0: thread $1 idx $2", kEchoDataPrefix, t, i));
    }
  }
  Stopwatch sw(Stopwatch::ALL_THREADS);
  sw.start();
  {
    vector<thread> threads;
    SCOPED_CLEANUP({
      for (auto& t : threads) {
        t.join();
      }
    });
    for (int t = 0; t < kNumThreads; t++) {
      threads.emplace_back([&, t] {
        for (int i = 0; i < kNumPerThread; i++) {
          ASSERT_OK(server_->Execute(&requests[t][i], &responses[t][i]));
        }
      });
    }
  }
  sw.stop();
  double reqs_sent = kNumThreads * kNumPerThread;
  double elapsed_seconds = sw.elapsed().wall_seconds();
  LOG(INFO) << Substitute("Sent $0 requests in $1 seconds: $2 req/s",
                          reqs_sent, elapsed_seconds, reqs_sent / elapsed_seconds);
  for (int t = 0; t < kNumThreads; t++) {
    for (int i = 0; i < kNumPerThread; i++) {
      EchoResponsePB echo_resp;
      ASSERT_TRUE(responses[t][i].response().UnpackTo(&echo_resp));
      EchoRequestPB echo_req;
      requests[t][i].request().UnpackTo(&echo_req);
      ASSERT_EQ(echo_resp.data(), echo_req.data());
    }
  }
}

// Test when our timeout occurs before adding the call to the outbound queue.
TEST_F(SubprocessServerTest, TestTimeoutBeforeQueueing) {
  FLAGS_subprocess_timeout_secs = 0;
  ASSERT_OK(ResetSubprocessServer());

  SubprocessRequestPB request = CreateEchoSubprocessRequestPB(kHello);
  SubprocessResponsePB response;
  Status s = server_->Execute(&request, &response);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "timed out before queueing call");
}

// Test when we try sending too many requests at once.
TEST_F(SubprocessServerTest, TestTimeoutWhileQueueingCalls) {
  // Set a relatively low timeout so our calls timeout more easily.
  FLAGS_subprocess_timeout_secs = 1;

  // Set a really low queue size so we can overflow the outbound queue easily.
  FLAGS_subprocess_request_queue_size_bytes = 1;

  // Make the Java subprocess single-threaded so we can overwhelm it more
  // easily.
  ASSERT_OK(ResetSubprocessServer(/*java_queue_size*/1,
                                  /*java_parser_threads*/1));

  // Send a bunch of requests with a sleep that's lower than our timeout.
  // Since we've made the subprocess single-threaded, the sheer number of
  // requests should fill the pipe, and our outbound queue.
  const int kNumRequests = 500;
  vector<thread> threads;
  vector<Status> results(kNumRequests);
  const string kLargeRequest = string(10000, 'x');
  for (int i = 0; i < kNumRequests; i++) {
    threads.emplace_back([&, i] {
      SubprocessRequestPB request =
          CreateEchoSubprocessRequestPB(kLargeRequest, /*sleep_ms*/500);
      SubprocessResponsePB response;
      results[i] = server_->Execute(&request, &response);
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  bool has_timeout_when_queueing = false;
  for (const auto& s : results) {
    if (s.IsTimedOut() &&
        s.ToString().find("timed out trying to queue call") != string::npos) {
      has_timeout_when_queueing = true;
    }
  }
  // We sent a ton of requests and should've overwhelmed the pipe and our
  // outbound queue.
  ASSERT_TRUE(has_timeout_when_queueing) << "expected at least one timeout";
}

// Test when the subprocess takes too long.
TEST_F(SubprocessServerTest, TestSlowSubprocessTimesOut) {
  FLAGS_subprocess_timeout_secs = 1;
  ASSERT_OK(ResetSubprocessServer());
  SubprocessRequestPB request =
      CreateEchoSubprocessRequestPB(kHello, /*sleep_ms*/1500);
  SubprocessResponsePB response;
  Status s = server_->Execute(&request, &response);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "timed out while in flight");
}

// Test calls while shutting down.
TEST_F(SubprocessServerTest, TestCallsReturnWhenShuttingDown) {
  Status s;
  thread t([&] {
    SubprocessRequestPB request =
        CreateEchoSubprocessRequestPB(kHello, /*sleep_ms*/1000);
    SubprocessResponsePB response;
    s = server_->Execute(&request, &response);
  });
  server_->Shutdown();
  t.join();
  // There are many places the error could've happened, so we won't check on
  // the exact message or error type.
  ASSERT_FALSE(s.ok());
}

} // namespace subprocess
} // namespace kudu

