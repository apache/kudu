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

#include <cstdint>
#include <memory>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"

namespace kudu {

class MetricRegistry;
class RollingLog;
class Thread;
class Status;

namespace server {

class DiagnosticsLog {
 public:
  DiagnosticsLog(std::string log_dir, MetricRegistry* metric_registry);
  ~DiagnosticsLog();

  void SetMetricsLogInterval(MonoDelta interval);

  Status Start();
  void Stop();

 private:
  void RunThread();
  Status LogMetrics();

  const std::string log_dir_;
  const MetricRegistry* metric_registry_;

  scoped_refptr<Thread> thread_;
  std::unique_ptr<RollingLog> log_;

  Mutex lock_;
  ConditionVariable wake_;
  bool stop_ = false;

  MonoDelta metrics_log_interval_;

  int64_t metrics_epoch_ = 0;

  DISALLOW_COPY_AND_ASSIGN(DiagnosticsLog);
};

} // namespace server
} // namespace kudu
