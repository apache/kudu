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

#include "kudu/subprocess/echo_subprocess.h"

#include "kudu/util/metrics.h"

METRIC_DEFINE_histogram(server, echo_subprocess_execution_time_ms,
    "Echo subprocess execution time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent executing the Echo subprocess request, excluding "
    "time spent spent in the subprocess queues",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, echo_subprocess_inbound_queue_length,
    "Echo subprocess inbound queue length",
    kudu::MetricUnit::kMessages,
    "Number of request messages in the Echo subprocess' inbound request queue",
    kudu::MetricLevel::kInfo,
    1000, 1);
METRIC_DEFINE_histogram(server, echo_subprocess_inbound_queue_time_ms,
    "Echo subprocess inbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Echo subprocess' inbound request queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, echo_subprocess_outbound_queue_length,
    "Echo subprocess outbound queue length",
    kudu::MetricUnit::kMessages,
    "Number of request messages in the Echo subprocess' outbound response queue",
    kudu::MetricLevel::kInfo,
    1000, 1);
METRIC_DEFINE_histogram(server, echo_subprocess_outbound_queue_time_ms,
    "Echo subprocess outbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Echo subprocess' outbound response queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, echo_server_inbound_queue_size_bytes,
    "Echo server inbound queue size (bytes)",
    kudu::MetricUnit::kBytes,
    "Number of bytes in the inbound response queue of the Echo server, recorded "
    "at the time a new response is read from the pipe and added to the inbound queue",
    kudu::MetricLevel::kInfo,
    4 * 1024 * 1024, 1);
METRIC_DEFINE_histogram(server, echo_server_inbound_queue_time_ms,
    "Echo server inbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Echo server's inbound response queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);
METRIC_DEFINE_histogram(server, echo_server_outbound_queue_size_bytes,
    "Echo server outbound queue size (bytes)",
    kudu::MetricUnit::kBytes,
    "Number of bytes in the outbound request queue of the Echo server, recorded "
    "at the time a new request is added to the outbound request queue",
    kudu::MetricLevel::kInfo,
    4 * 1024 * 1024, 1);
METRIC_DEFINE_histogram(server, echo_server_outbound_queue_time_ms,
    "Echo server outbound queue time (ms)",
    kudu::MetricUnit::kMilliseconds,
    "Duration of time in ms spent in the Echo server's outbound request queue",
    kudu::MetricLevel::kInfo,
    60000LU, 1);

namespace kudu {
namespace subprocess {

#define HISTINIT(member, x) member = METRIC_##x.Instantiate(entity)
EchoSubprocessMetrics::EchoSubprocessMetrics(const scoped_refptr<MetricEntity>& entity) {
  HISTINIT(server_inbound_queue_size_bytes, echo_server_inbound_queue_size_bytes);
  HISTINIT(server_inbound_queue_time_ms, echo_server_inbound_queue_time_ms);
  HISTINIT(server_outbound_queue_size_bytes, echo_server_outbound_queue_size_bytes);
  HISTINIT(server_outbound_queue_time_ms, echo_server_outbound_queue_time_ms);
  HISTINIT(sp_execution_time_ms, echo_subprocess_execution_time_ms);
  HISTINIT(sp_inbound_queue_length, echo_subprocess_inbound_queue_length);
  HISTINIT(sp_inbound_queue_time_ms, echo_subprocess_inbound_queue_time_ms);
  HISTINIT(sp_outbound_queue_length, echo_subprocess_outbound_queue_length);
  HISTINIT(sp_outbound_queue_time_ms, echo_subprocess_outbound_queue_time_ms);
}
#undef HISTINIT

} // namespace subprocess
} // namespace kudu
