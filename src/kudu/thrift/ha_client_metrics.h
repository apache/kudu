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

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/metrics.h"

namespace kudu {
namespace thrift {

struct HaClientMetrics {
  virtual ~HaClientMetrics() = default;

  scoped_refptr<Counter> tasks_successful;
  scoped_refptr<Counter> tasks_failed_fatal;
  scoped_refptr<Counter> tasks_failed_nonfatal;
  scoped_refptr<Counter> reconnections_succeeded;
  scoped_refptr<Counter> reconnections_failed;

  scoped_refptr<Histogram> task_execution_time_us;
};

} // namespace thrift
} // namespace kudu
