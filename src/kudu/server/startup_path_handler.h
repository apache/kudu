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
#pragma once

#include <atomic>

#include "kudu/gutil/ref_counted.h"
#include "kudu/server/webserver.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/timer.h"

namespace kudu {

namespace server {

class StartupPathHandler {
public:

  explicit StartupPathHandler(const scoped_refptr<MetricEntity>& entity);

  // Populate the response output with the current information
  void Startup(const Webserver::WebRequest &req, Webserver::WebResponse *resp);

  // Startup page path handler
  void RegisterStartupPathHandler(Webserver *webserver);

  Timer* init_progress() {return &init_progress_;}
  Timer* read_filesystem_progress() {return &read_filesystem_progress_;}
  Timer* read_instance_metadata_files_progress() {return &read_instance_metadata_files_progress_;}
  Timer* read_data_directories_progress() {return &read_data_directories_progress_;}
  Timer* start_tablets_progress() {return &start_tablets_progress_;}
  Timer* initialize_master_catalog_progress() {return &initialize_master_catalog_progress_;}
  Timer* start_rpc_server_progress() {return &start_rpc_server_progress_;}
  std::atomic<int>* tablets_processed() {return &tablets_processed_;}
  std::atomic<int>* tablets_total() {return &tablets_total_;}
  std::atomic<int>* containers_processed() {return &containers_processed_;}
  std::atomic<int>* containers_total() {return &containers_total_;}
  void set_is_tablet_server(bool is_tablet_server);
  void set_is_using_lbm(bool is_using_lbm);

  // Call back functions for aggregate percentage and time elapsed
  int StartupProgressStepsRemainingMetric();
  MonoDelta StartupProgressTimeElapsedMetric();

private:
  // Hold the initialization step progress information like the status, start and end time.
  Timer init_progress_;

  // Hold the read filesystem step progress information.
  Timer read_filesystem_progress_;

  // Hold the reading of instance metadata files progress in all the configured directories.
  Timer read_instance_metadata_files_progress_;

  // Hold the reading of data directories progress.
  Timer read_data_directories_progress_;

  // Hold the progress of bootstrapping and starting the tablets.
  Timer start_tablets_progress_;

  // Hold the progress of initializing the master catalog.
  Timer initialize_master_catalog_progress_;

  // Hold the progress of starting of the rpc server.
  Timer start_rpc_server_progress_;

  // Hold the number of tablets which were attempted to be bootstrapped and started.
  std::atomic<int> tablets_processed_;

  // Hold the total tablets present on the server.
  std::atomic<int> tablets_total_;

  // Hold the number of containers which were opened and processed
  std::atomic<int> containers_processed_;

  // Hold the total containers present on the server.
  std::atomic<int> containers_total_;

  // To display different web page contents for masters and tablet servers, we need to know
  // if the particular instance is a master of tablet server.
  bool is_tablet_server_;

  // We do not open containers if file block manager is being used and hence display different
  // webpage contents if file block manager is being used.
  bool is_using_lbm_;

  FunctionGaugeDetacher metric_detacher_;
};

} // namespace server
} // namespace kudu
