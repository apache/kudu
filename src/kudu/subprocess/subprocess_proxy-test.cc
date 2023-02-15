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

#include "kudu/subprocess/subprocess_proxy.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>

#include "kudu/gutil/casts.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/echo_subprocess.h"
#include "kudu/subprocess/server.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_int32(subprocess_timeout_secs);
DECLARE_uint32(subprocess_max_message_size_bytes);

METRIC_DECLARE_counter(echo_server_dropped_messages);
METRIC_DECLARE_histogram(echo_server_inbound_queue_size_bytes);
METRIC_DECLARE_histogram(echo_server_inbound_queue_time_ms);
METRIC_DECLARE_histogram(echo_server_outbound_queue_size_bytes);
METRIC_DECLARE_histogram(echo_server_outbound_queue_time_ms);
METRIC_DECLARE_histogram(echo_subprocess_execution_time_ms);
METRIC_DECLARE_histogram(echo_subprocess_inbound_queue_length);
METRIC_DECLARE_histogram(echo_subprocess_inbound_queue_time_ms);
METRIC_DECLARE_histogram(echo_subprocess_outbound_queue_length);
METRIC_DECLARE_histogram(echo_subprocess_outbound_queue_time_ms);

using std::unique_ptr;
using std::string;
using std::vector;
using strings::Substitute;


namespace kudu {
namespace subprocess {

class EchoSubprocessTest : public KuduTest {
 public:
  EchoSubprocessTest()
      : metric_entity_(METRIC_ENTITY_server.Instantiate(&metric_registry_,
                                                        "subprocess_proxy-test")),
        test_dir_(GetTestDataDirectory()) {}

  void SetUp() override {
    KuduTest::SetUp();
    ASSERT_OK(ResetEchoSubprocess());
  }

  Status ResetEchoSubprocess() {
    string exe;
    RETURN_NOT_OK(env_->GetExecutablePath(&exe));
    const string bin_dir = DirName(exe);
    string java_home;
    RETURN_NOT_OK(FindHomeDir("java", bin_dir, &java_home));
    const string& pipe_file = SubprocessServer::FifoPath(JoinPathSegments(test_dir_, "echo_pipe"));
    vector<string> argv = {
      Substitute("$0/bin/java", java_home),
      "-cp", Substitute("$0/kudu-subprocess.jar", bin_dir),
      "org.apache.kudu.subprocess.echo.EchoSubprocessMain",
      "-o", pipe_file,
    };
    echo_subprocess_.reset(new EchoSubprocess(env_, pipe_file, std::move(argv),
                                              metric_entity_));
    return echo_subprocess_->Start();
  }

 protected:
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  unique_ptr<EchoSubprocess> echo_subprocess_;
  const string test_dir_;
};

#define GET_COUNTER(metric_entity, metric_name) \
  down_cast<Counter*>((metric_entity)->FindOrNull(METRIC_##metric_name).get())
#define GET_HIST(metric_entity, metric_name) \
  down_cast<Histogram*>((metric_entity)->FindOrNull(METRIC_##metric_name).get())

TEST_F(EchoSubprocessTest, TestBasicSubprocessMetrics) {
  const string kMessage = "don't catch you slippin' now";
  const int64_t kSleepMs = 1000;
  EchoRequestPB req;
  req.set_data(kMessage);
  req.set_sleep_ms(kSleepMs);
  EchoResponsePB resp;
  ASSERT_OK(echo_subprocess_->Execute(req, &resp));
  ASSERT_EQ(kMessage, resp.data());


  // There shouldn't have anything in the subprocess queues.
  Histogram* in_len_hist = GET_HIST(metric_entity_, echo_subprocess_inbound_queue_length);
  ASSERT_EQ(1, in_len_hist->TotalCount());
  ASSERT_EQ(0, in_len_hist->MaxValueForTests());
  Histogram* out_len_hist = GET_HIST(metric_entity_, echo_subprocess_outbound_queue_length);
  ASSERT_EQ(1, out_len_hist->TotalCount());
  ASSERT_EQ(0, out_len_hist->MaxValueForTests());

  // We should have some non-negative queue times.
  Histogram* out_hist = GET_HIST(metric_entity_, echo_subprocess_outbound_queue_time_ms);
  ASSERT_EQ(1, out_hist->TotalCount());
  ASSERT_LE(0, out_hist->MaxValueForTests());
  Histogram* in_hist = GET_HIST(metric_entity_, echo_subprocess_inbound_queue_time_ms);
  ASSERT_EQ(1, in_hist->TotalCount());
  ASSERT_LE(0, in_hist->MaxValueForTests());

  // There shouldn't have anything bytes the server queues when we enqueue.
  Histogram* server_in_size_hist =
      GET_HIST(metric_entity_, echo_server_inbound_queue_size_bytes);
  ASSERT_EQ(1, server_in_size_hist->TotalCount());
  ASSERT_EQ(0, server_in_size_hist->MaxValueForTests());
  Histogram* server_out_size_hist =
      GET_HIST(metric_entity_, echo_server_outbound_queue_size_bytes);
  ASSERT_EQ(1, server_out_size_hist->TotalCount());
  ASSERT_EQ(0, server_out_size_hist->MaxValueForTests());

  // We should have some non-negative queue times on the server side too.
  Histogram* server_out_hist =
    GET_HIST(metric_entity_, echo_server_outbound_queue_time_ms);
  ASSERT_EQ(1, server_out_hist->TotalCount());
  ASSERT_LE(0, server_out_hist->MaxValueForTests());
  Histogram* server_in_hist =
    GET_HIST(metric_entity_, echo_server_inbound_queue_time_ms);
  ASSERT_EQ(1, server_in_hist->TotalCount());
  ASSERT_LE(0, server_in_hist->MaxValueForTests());

  // The execution should've taken at least our sleep time.
  Histogram* exec_hist = GET_HIST(metric_entity_, echo_subprocess_execution_time_ms);
  ASSERT_EQ(1, exec_hist->TotalCount());
  ASSERT_LT(kSleepMs, exec_hist->MaxValueForTests());
}

// Test that we'll still report metrics when we recieve them from the
// subprocess, even if the call itself failed.
TEST_F(EchoSubprocessTest, TestSubprocessMetricsOnError) {
  // Set things up so we'll time out.
  FLAGS_subprocess_timeout_secs = 1;
  const int64_t kSleepMs = 2000;
  ASSERT_OK(ResetEchoSubprocess());

  EchoRequestPB req;
  req.set_data("garbage!");
  req.set_sleep_ms(kSleepMs);
  EchoResponsePB resp;
  Status s = echo_subprocess_->Execute(req, &resp);
  ASSERT_TRUE(s.IsTimedOut()) << s.ToString();

  // Immediately following our call, we won't have any metrics from the subprocess.
  Histogram* exec_hist = GET_HIST(metric_entity_, echo_subprocess_execution_time_ms);
  Histogram* out_len_hist = GET_HIST(metric_entity_, echo_subprocess_outbound_queue_length);
  Histogram* in_len_hist = GET_HIST(metric_entity_, echo_subprocess_inbound_queue_length);
  Histogram* sp_out_hist = GET_HIST(metric_entity_, echo_subprocess_outbound_queue_time_ms);
  Histogram* sp_in_hist = GET_HIST(metric_entity_, echo_subprocess_inbound_queue_time_ms);
  Histogram* server_out_time_hist = GET_HIST(metric_entity_, echo_server_outbound_queue_time_ms);
  Histogram* server_out_size_hist = GET_HIST(metric_entity_, echo_server_outbound_queue_size_bytes);
  Histogram* server_in_time_hist = GET_HIST(metric_entity_, echo_server_inbound_queue_time_ms);
  Histogram* server_in_size_hist = GET_HIST(metric_entity_, echo_server_inbound_queue_size_bytes);
  ASSERT_EQ(0, exec_hist->TotalCount());
  ASSERT_EQ(0, out_len_hist->TotalCount());
  ASSERT_EQ(0, in_len_hist->TotalCount());
  ASSERT_EQ(0, sp_out_hist->TotalCount());
  ASSERT_EQ(0, sp_in_hist->TotalCount());

  // We'll have sent the request from the server and not received the response.
  // Our metrics should reflect that.
  ASSERT_EQ(1, server_out_time_hist->TotalCount());
  ASSERT_EQ(1, server_out_size_hist->TotalCount());
  ASSERT_EQ(0, server_in_time_hist->TotalCount());
  ASSERT_EQ(0, server_in_size_hist->TotalCount());

  // Eventually the subprocess will return our call, and we'll see some
  // metrics.
  ASSERT_EVENTUALLY([&] {
    ASSERT_EQ(1, exec_hist->TotalCount());
    ASSERT_LT(kSleepMs, exec_hist->MaxValueForTests());

    ASSERT_EQ(1, out_len_hist->TotalCount());
    ASSERT_EQ(1, in_len_hist->TotalCount());
    ASSERT_EQ(1, sp_out_hist->TotalCount());
    ASSERT_EQ(1, sp_in_hist->TotalCount());
    ASSERT_EQ(1, server_out_time_hist->TotalCount());
    ASSERT_EQ(1, server_in_time_hist->TotalCount());
    ASSERT_EQ(1, server_out_size_hist->TotalCount());
    ASSERT_EQ(1, server_in_size_hist->TotalCount());
  });
}

TEST_F(EchoSubprocessTest, DroppedMessagesMetric) {
  FLAGS_subprocess_timeout_secs = 1;
  FLAGS_subprocess_max_message_size_bytes = 100;
  ASSERT_OK(ResetEchoSubprocess());

  const auto* counter = GET_COUNTER(metric_entity_, echo_server_dropped_messages);
  ASSERT_EQ(0, counter->value());

  // Send an oversized message -- it should be dropped.
  {
    EchoRequestPB req;
    req.set_data(string(100, 'x'));
    EchoResponsePB resp;
    const auto s = echo_subprocess_->Execute(req, &resp);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  }
  // The dropped message's counter should be increment by one.
  ASSERT_EQ(1, counter->value());

  // Send a non-oversized message.
  {
    EchoRequestPB req;
    req.set_data("x");
    EchoResponsePB resp;
    ASSERT_OK(echo_subprocess_->Execute(req, &resp));
  }
  // The dropped message's counter should stay at its prior value.
  ASSERT_EQ(1, counter->value());

  // Send a few more oversized messages.
  for (size_t i = 0; i < 2; ++i) {
    EchoRequestPB req;
    req.set_data(string(1000 + i, 'x'));
    EchoResponsePB resp;
    const auto s = echo_subprocess_->Execute(req, &resp);
    ASSERT_TRUE(s.IsTimedOut()) << s.ToString();
  }
  // The dropped message's counter should be incremented by the number of
  // oversized messages sent after capturing the prior reading.
  ASSERT_EQ(1 + 2, counter->value());

  // Sent several non-oversized message.
  for (size_t i = 0; i < 5; ++i) {
    EchoRequestPB req;
    req.set_data(string(i, 'x'));
    EchoResponsePB resp;
    ASSERT_OK(echo_subprocess_->Execute(req, &resp));
  }
  // The dropped message's counter should stay at its prior value.
  ASSERT_EQ(3, counter->value());
}

#undef GET_COUNTER
#undef GET_HIST

} // namespace subprocess
} // namespace kudu
