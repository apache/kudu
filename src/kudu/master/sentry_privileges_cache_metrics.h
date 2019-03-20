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
#include "kudu/util/ttl_cache_metrics.h"

namespace kudu {

class MetricEntity;

namespace master {

struct SentryPrivilegesCacheMetrics : public TTLCacheMetrics {
  explicit SentryPrivilegesCacheMetrics(
      const scoped_refptr<MetricEntity>& metric_entity);
};

} // namespace master
} // namespace kudu
