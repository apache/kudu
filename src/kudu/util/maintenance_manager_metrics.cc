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

#include "kudu/util/maintenance_manager_metrics.h"

#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"

// Prepare() for a maintenance operation might fail only due to really rare
// circumstances. For example, if an operation collides with another running
// instance of the same operation.
METRIC_DEFINE_counter(server, maintenance_op_prepare_failed,
                      "Number Of Operations With Failed Prepare()",
                      kudu::MetricUnit::kOperations,
                      "Number of times when calling Prepare() on a maintenance "
                      "operation failed",
                      kudu::MetricLevel::kWarn);

METRIC_DEFINE_histogram(server, maintenance_op_find_best_candidate_duration,
                        "Time Taken To Find Best Maintenance Operation",
                        kudu::MetricUnit::kMicroseconds,
                        "Time spent choosing a maintenance operation with "
                        "highest scores among available candidates",
                        kudu::MetricLevel::kInfo,
                        60000000LU, 2);

namespace kudu {

MaintenanceManagerMetrics::MaintenanceManagerMetrics(
    const scoped_refptr<MetricEntity>& metric_entity)
    : op_find_duration(
          METRIC_maintenance_op_find_best_candidate_duration.Instantiate(metric_entity)),
      op_prepare_failed(METRIC_maintenance_op_prepare_failed.Instantiate(metric_entity)) {
}

void MaintenanceManagerMetrics::SubmitOpFindDuration(const MonoDelta& duration) {
  op_find_duration->Increment(duration.ToMicroseconds());
}

void MaintenanceManagerMetrics::SubmitOpPrepareFailed() {
  op_prepare_failed->Increment();
}

} // namespace kudu
