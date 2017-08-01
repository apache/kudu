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

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/metrics.h"

namespace kudu {
namespace tserver {

class TSTabletManager;

// A container for tablet manager metrics.
struct TSTabletManagerMetrics {
  TSTabletManagerMetrics(const scoped_refptr<MetricEntity>& metric_entity,
                         TSTabletManager* ts_tablet_manager);

  // Functions that return their respective tablet state count and potentially
  // refresh the counts from ts_tablet_manager_.
  int NumNotInitializedTablets();
  int NumInitializedTablets();
  int NumBootstrappingTablets();
  int NumRunningTablets();
  int NumFailedTablets();
  int NumStoppingTablets();
  int NumStoppedTablets();
  int NumShutdownTablets();

  void ResetTabletStateCounts();

  // Cached tablet state counts computed from ts_tablet_manager_.
  int num_not_initialized;
  int num_initialized;
  int num_bootstrapping;
  int num_running;
  int num_failed;
  int num_stopping;
  int num_stopped;
  int num_shutdown;

 private:
  TSTabletManager* ts_tablet_manager_;

  FunctionGaugeDetacher metric_detacher_;
};

} // namespace tserver
} // namespace kudu
