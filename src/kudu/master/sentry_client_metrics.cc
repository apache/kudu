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

#include "kudu/master/sentry_client_metrics.h"

#include "kudu/util/metrics.h"

METRIC_DEFINE_counter(server, sentry_client_tasks_successful,
                      "Successful Tasks", kudu::MetricUnit::kTasks,
                      "Number of successfully run tasks");
METRIC_DEFINE_counter(server, sentry_client_tasks_failed_fatal,
                      "Failed tasks (fatal)", kudu::MetricUnit::kTasks,
                      "Number of tasks failed with fatal errors");
METRIC_DEFINE_counter(server, sentry_client_tasks_failed_nonfatal,
                      "Failed Tasks (nonfatal)", kudu::MetricUnit::kTasks,
                      "Number of tasks failed with non-fatal errors");
METRIC_DEFINE_counter(server, sentry_client_reconnections_succeeded,
                      "Successful Reconnections", kudu::MetricUnit::kUnits,
                      "Number of successful reconnections to Sentry");
METRIC_DEFINE_counter(server, sentry_client_reconnections_failed,
                      "Failed Reconnections", kudu::MetricUnit::kUnits,
                      "Number of failed reconnections to Sentry");
METRIC_DEFINE_histogram(server, sentry_client_task_execution_time_us,
                        "Task Execution Time (us)",
                        kudu::MetricUnit::kMicroseconds,
                        "Duration of HaClient::Execute() calls (us)",
                        60000000, 2);

namespace kudu {
namespace master {

#define MINIT(member, x) member = METRIC_##x.Instantiate(e)
SentryClientMetrics::SentryClientMetrics(const scoped_refptr<MetricEntity>& e) {
  MINIT(tasks_successful, sentry_client_tasks_successful);
  MINIT(tasks_failed_fatal, sentry_client_tasks_failed_fatal);
  MINIT(tasks_failed_nonfatal, sentry_client_tasks_failed_nonfatal);
  MINIT(reconnections_succeeded, sentry_client_reconnections_succeeded);
  MINIT(reconnections_failed, sentry_client_reconnections_failed);
  MINIT(task_execution_time_us, sentry_client_task_execution_time_us);
}
#undef MINIT

} // namespace master
} // namespace kudu
