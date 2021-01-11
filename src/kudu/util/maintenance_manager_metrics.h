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
class MonoDelta;

// An utility class to help with keeping track of the MaintenanceManager-related
// metrics.
struct MaintenanceManagerMetrics {
  explicit MaintenanceManagerMetrics(const scoped_refptr<MetricEntity>& entity);

  // Submit stats on the time spent choosing the best available maintenance
  // operation among available candidates. The 'duration' parameter reflects
  // the amount of time spent on finding the best candidate.
  void SubmitOpFindDuration(const MonoDelta& duration);

  // Increment the 'op_prepare_failed' counter.
  void SubmitOpPrepareFailed();

  // Time spent on choosing the "best" maintenance operation among candidates.
  scoped_refptr<Histogram> op_find_duration;

  // Number of times when running Prepare() failed on a maintenance operation.
  scoped_refptr<Counter> op_prepare_failed;
};

} // namespace kudu
