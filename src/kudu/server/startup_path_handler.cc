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

#include "kudu/server/startup_path_handler.h"

#include <functional>
#include <iosfwd>
#include <string>

#include "kudu/gutil/strings/human_readable.h"
#include "kudu/server/webserver.h"
#include "kudu/util/easy_json.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/timer.h"
#include "kudu/util/web_callback_registry.h"

METRIC_DEFINE_gauge_int32(server, startup_progress_steps_remaining,
                          "Server Startup Steps Remaining",
                          kudu::MetricUnit::kUnits,
                          "Server startup progress steps remaining ",
                          kudu::MetricLevel::kWarn);

METRIC_DEFINE_gauge_int64(server, startup_progress_time_elapsed,
                          "Server Startup Progress Time Elapsed",
                          kudu::MetricUnit::kMilliseconds,
                          "Time taken by the server to complete the startup or"
                          "time elapsed so far for the server to startup",
                          kudu::MetricLevel::kInfo);

using std::ifstream;
using std::ostringstream;
using std::string;

namespace kudu {

namespace server {

void SetWebResponse(EasyJson* output, const string& step,
                    const Timer& startup_step, const int percent = -1) {
  output->Set(step + "_status", percent == -1 ? (startup_step.IsStopped() ? 100 : 0) : percent);
  output->Set(step + "_time", HumanReadableElapsedTime::ToShortString(
                  (startup_step.TimeElapsed()).ToSeconds()));
}

StartupPathHandler::StartupPathHandler(const scoped_refptr<MetricEntity>& entity):
  tablets_processed_(0),
  tablets_total_(0),
  containers_processed_(0),
  containers_total_(0),
  is_tablet_server_(false),
  is_using_lbm_(true) {
  METRIC_startup_progress_steps_remaining.InstantiateFunctionGauge(entity,
      [this]() {return StartupProgressStepsRemainingMetric();})
      ->AutoDetachToLastValue(&metric_detacher_);
  METRIC_startup_progress_time_elapsed.InstantiateFunctionGauge(entity,
      [this]() {return StartupProgressTimeElapsedMetric().ToMilliseconds();})
      ->AutoDetachToLastValue(&metric_detacher_);
}

void StartupPathHandler::Startup(const Webserver::WebRequest& /*req*/,
                                 Webserver::WebResponse* resp) {

  auto* output = &resp->output;

  output->Set("is_tablet_server", is_tablet_server_);
  output->Set("is_master_server", !is_tablet_server_);
  output->Set("is_log_block_manager", is_using_lbm_);

  // Populate the different startup steps with their progress
  SetWebResponse(output, "init", init_progress_);
  SetWebResponse(output, "read_filesystem", read_filesystem_progress_);
  SetWebResponse(output, "read_instance_metadatafiles", read_instance_metadata_files_progress_);

  // Populate the progress percentage of opening of container files in case of lbm and non-lbm
  if (is_using_lbm_) {
    if (containers_total_ == 0) {
      SetWebResponse(output, "read_data_directories", read_data_directories_progress_);
    } else {
      SetWebResponse(output, "read_data_directories", read_data_directories_progress_,
                      containers_processed_ * 100 / containers_total_);
    }
    output->Set("containers_processed", containers_processed_.load());
    output->Set("containers_total", containers_total_.load());
  } else {
    SetWebResponse(output, "read_data_directories", read_data_directories_progress_);
  }

  // Set the bootstrapping and opening tablets step and handle the case of zero tablets
  // present in the server
  if (tablets_total_ == 0) {
    SetWebResponse(output, "start_tablets", start_tablets_progress_);
  } else {
    SetWebResponse(output, "start_tablets", start_tablets_progress_,
                    tablets_processed_ * 100 / tablets_total_);
  }

  if (is_tablet_server_) {
    output->Set("tablets_processed", tablets_processed_.load());
    output->Set("tablets_total", tablets_total_.load());
  }

  SetWebResponse(output, "initialize_master_catalog", initialize_master_catalog_progress_);
  SetWebResponse(output, "start_rpc_server", start_rpc_server_progress_);
}

void StartupPathHandler::RegisterStartupPathHandler(Webserver *webserver) {
  bool on_nav_bar = true;
  webserver->RegisterPathHandler("/startup", "Startup",
                                 [this](const Webserver::WebRequest& req,
                                        Webserver::WebResponse* resp) {
                                          this->Startup(req, resp);
                                        },
                                 StyleMode::STYLED, on_nav_bar);
}

void StartupPathHandler::set_is_tablet_server(bool is_tablet_server) {
  is_tablet_server_ = is_tablet_server;
}

void StartupPathHandler::set_is_using_lbm(bool is_using_lbm) {
  is_using_lbm_ = is_using_lbm;
}

int StartupPathHandler::StartupProgressStepsRemainingMetric() {
  int counter = 0;
  counter += (init_progress_.IsStopped() ? 0 : 1);
  counter += (read_filesystem_progress_.IsStopped() ? 0 : 1);
  if (is_tablet_server_) {
    counter += start_tablets_progress_.IsStopped() ? 0 : 1;
  } else {
    counter += initialize_master_catalog_progress_.IsStopped() ? 0 : 1;
  }
  counter += (start_rpc_server_progress_.IsStopped() ? 0 : 1);
  return counter;
}

MonoDelta StartupPathHandler::StartupProgressTimeElapsedMetric() {
  MonoDelta time_elapsed;
  time_elapsed = init_progress_.TimeElapsed();
  time_elapsed += read_filesystem_progress_.TimeElapsed();
  if (is_tablet_server_) {
    time_elapsed += start_tablets_progress_.TimeElapsed();
  } else {
    time_elapsed += initialize_master_catalog_progress_.TimeElapsed();
  }
  time_elapsed += start_rpc_server_progress_.TimeElapsed();
  return time_elapsed;
}

} // namespace server
} // namespace kudu
