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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/casts.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::make_shared;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

METRIC_DEFINE_histogram(server, echo_subprocess_inbound_queue_length,
    "Echo subprocess inbound queue length",
    kudu::MetricUnit::kMessages,
    "Number of request messages in the Echo subprocess' inbound request queue",
    kudu::MetricLevel::kInfo,
    1000, 1);
METRIC_DEFINE_histogram(server, echo_subprocess_outbound_queue_length,
    "Echo subprocess outbound queue length",
    kudu::MetricUnit::kMessages,
    "Number of request messages in the Echo subprocess' outbound response queue",
    kudu::MetricLevel::kInfo,
    1000, 1);
METRIC_DEFINE_histogram(server, echo_subprocess_inbound_queue_time_ms,
    "Echo subprocess inbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Echo subprocess' inbound request queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, echo_subprocess_outbound_queue_time_ms,
    "Echo subprocess outbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Echo subprocess' outbound response queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, echo_subprocess_execution_time_ms,
    "Echo subprocess execution time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent executing the Echo subprocess request, excluding "
    "time spent spent in the subprocess queues",
    kudu::MetricLevel::kInfo,
    60000LU, 1);


namespace kudu {
namespace subprocess {


#define GINIT(member, x) member = METRIC_##x.Instantiate(entity, 0)
#define HISTINIT(member, x) member = METRIC_##x.Instantiate(entity)
struct EchoSubprocessMetrics : public SubprocessMetrics {
  explicit EchoSubprocessMetrics(const scoped_refptr<MetricEntity>& entity) {
    HISTINIT(inbound_queue_length, echo_subprocess_inbound_queue_length);
    HISTINIT(outbound_queue_length, echo_subprocess_outbound_queue_length);
    HISTINIT(inbound_queue_time_ms, echo_subprocess_inbound_queue_time_ms);
    HISTINIT(outbound_queue_time_ms, echo_subprocess_outbound_queue_time_ms);
    HISTINIT(execution_time_ms, echo_subprocess_execution_time_ms);
  }
};
#undef HISTINIT
#undef MINIT

typedef SubprocessProxy<EchoRequestPB, EchoResponsePB, EchoSubprocessMetrics> EchoSubprocess;

class EchoSubprocessTest : public KuduTest {
 public:
  EchoSubprocessTest()
      : metric_entity_(METRIC_ENTITY_server.Instantiate(&metric_registry_,
                                                        "subprocess_proxy-test")) {}

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
    vector<string> argv = {
      Substitute("$0/bin/java", java_home),
      "-jar", Substitute("$0/kudu-subprocess-echo.jar", bin_dir)
    };
    echo_subprocess_ = make_shared<EchoSubprocess>(std::move(argv), metric_entity_);
    return echo_subprocess_->Start();
  }

 protected:
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
  shared_ptr<EchoSubprocess> echo_subprocess_;
};

TEST_F(EchoSubprocessTest, TestBasicMetrics) {
  const string kMessage = "don't catch you slippin' now";
  const int64_t kSleepMs = 1000;
  EchoRequestPB req;
  req.set_data(kMessage);
  req.set_sleep_ms(kSleepMs);
  EchoResponsePB resp;
  ASSERT_OK(echo_subprocess_->Execute(req, &resp));
  ASSERT_EQ(kMessage, resp.data());

  // There shouldn't have anything in the subprocess queues.
  Histogram* in_len_hist = down_cast<Histogram*>(metric_entity_->FindOrNull(
      METRIC_echo_subprocess_inbound_queue_length).get());
  ASSERT_EQ(1, in_len_hist->TotalCount());
  ASSERT_EQ(0, in_len_hist->MaxValueForTests());
  Histogram* out_len_hist = down_cast<Histogram*>(metric_entity_->FindOrNull(
      METRIC_echo_subprocess_outbound_queue_length).get());
  ASSERT_EQ(1, out_len_hist->TotalCount());
  ASSERT_EQ(0, out_len_hist->MaxValueForTests());

  // We should have some non-negative queue times.
  Histogram* out_hist = down_cast<Histogram*>(metric_entity_->FindOrNull(
      METRIC_echo_subprocess_outbound_queue_time_ms).get());
  ASSERT_EQ(1, out_hist->TotalCount());
  ASSERT_LE(0, out_hist->MaxValueForTests());
  Histogram* in_hist = down_cast<Histogram*>(metric_entity_->FindOrNull(
      METRIC_echo_subprocess_inbound_queue_time_ms).get());
  ASSERT_EQ(1, in_hist->TotalCount());
  ASSERT_LE(0, in_hist->MaxValueForTests());

  // The execution should've taken at least our sleep time.
  Histogram* exec_hist = down_cast<Histogram*>(metric_entity_->FindOrNull(
      METRIC_echo_subprocess_execution_time_ms).get());
  ASSERT_EQ(1, exec_hist->TotalCount());
  ASSERT_LT(kSleepMs, exec_hist->MaxValueForTests());
}

} // namespace subprocess
} // namespace kudu
