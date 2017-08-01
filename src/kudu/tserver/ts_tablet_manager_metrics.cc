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

#include "kudu/tserver/ts_tablet_manager_metrics.h"

#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/tserver/ts_tablet_manager.h"

METRIC_DEFINE_gauge_int32(server, tablets_num_not_initialized,
                          "Number of Not Initialized Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently not initialized");

METRIC_DEFINE_gauge_int32(server, tablets_num_initialized,
                          "Number of Initialized Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently initialized");

METRIC_DEFINE_gauge_int32(server, tablets_num_bootstrapping,
                          "Number of Bootstrapping Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently bootstrapping");

METRIC_DEFINE_gauge_int32(server, tablets_num_running,
                          "Number of Running Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently running");

METRIC_DEFINE_gauge_int32(server, tablets_num_failed,
                          "Number of Failed Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of failed tablets");

METRIC_DEFINE_gauge_int32(server, tablets_num_stopping,
                          "Number of Stopping Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently stopping");

METRIC_DEFINE_gauge_int32(server, tablets_num_stopped,
                          "Number of Stopped Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently stopped");

METRIC_DEFINE_gauge_int32(server, tablets_num_shutdown,
                          "Number of Shut Down Tablets",
                          kudu::MetricUnit::kTablets,
                          "Number of tablets currently shut down");

namespace kudu {
namespace tserver {

TSTabletManagerMetrics::TSTabletManagerMetrics(const scoped_refptr<MetricEntity>& metric_entity,
                                               TSTabletManager* ts_tablet_manager) :
    ts_tablet_manager_(ts_tablet_manager) {
  METRIC_tablets_num_not_initialized.InstantiateFunctionGauge(
          metric_entity,
          Bind(&TSTabletManagerMetrics::NumNotInitializedTablets, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_initialized.InstantiateFunctionGauge(
          metric_entity,
          Bind(&TSTabletManagerMetrics::NumInitializedTablets, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_bootstrapping.InstantiateFunctionGauge(
          metric_entity,
          Bind(&TSTabletManagerMetrics::NumBootstrappingTablets, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_running.InstantiateFunctionGauge(
          metric_entity,
          Bind(&TSTabletManagerMetrics::NumRunningTablets, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_failed.InstantiateFunctionGauge(
          metric_entity,
          Bind(&TSTabletManagerMetrics::NumFailedTablets, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_stopping.InstantiateFunctionGauge(
          metric_entity,
          Bind(&TSTabletManagerMetrics::NumStoppingTablets, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_stopped.InstantiateFunctionGauge(
          metric_entity,
          Bind(&TSTabletManagerMetrics::NumStoppedTablets, Unretained(this)))
      ->AutoDetach(&metric_detacher_);
  METRIC_tablets_num_shutdown.InstantiateFunctionGauge(
          metric_entity,
          Bind(&TSTabletManagerMetrics::NumShutdownTablets, Unretained(this)))
      ->AutoDetach(&metric_detacher_);

  ResetTabletStateCounts();
}

int TSTabletManagerMetrics::NumNotInitializedTablets() {
  ts_tablet_manager_->RefreshTabletStateCache();
  return num_not_initialized;
}

int TSTabletManagerMetrics::NumInitializedTablets() {
  ts_tablet_manager_->RefreshTabletStateCache();
  return num_initialized;
}

int TSTabletManagerMetrics::NumBootstrappingTablets() {
  ts_tablet_manager_->RefreshTabletStateCache();
  return num_bootstrapping;
}

int TSTabletManagerMetrics::NumRunningTablets() {
  ts_tablet_manager_->RefreshTabletStateCache();
  return num_running;
}

int TSTabletManagerMetrics::NumFailedTablets() {
  ts_tablet_manager_->RefreshTabletStateCache();
  return num_failed;
}

int TSTabletManagerMetrics::NumStoppingTablets() {
  ts_tablet_manager_->RefreshTabletStateCache();
  return num_stopping;
}

int TSTabletManagerMetrics::NumStoppedTablets() {
  ts_tablet_manager_->RefreshTabletStateCache();
  return num_stopped;
}

int TSTabletManagerMetrics::NumShutdownTablets() {
  ts_tablet_manager_->RefreshTabletStateCache();
  return num_shutdown;
}

void TSTabletManagerMetrics::ResetTabletStateCounts() {
  num_not_initialized = 0;
  num_initialized = 0;
  num_bootstrapping = 0;
  num_running = 0;
  num_failed = 0;
  num_stopping = 0;
  num_stopped = 0;
  num_shutdown = 0;
}

} // namespace tserver
} // namespace kudu
