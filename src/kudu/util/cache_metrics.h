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

struct CacheMetrics {
  virtual ~CacheMetrics() = default;

  scoped_refptr<Counter> inserts;
  scoped_refptr<Counter> lookups;
  scoped_refptr<Counter> evictions;
  scoped_refptr<Counter> cache_hits;
  scoped_refptr<Counter> cache_hits_caching;
  scoped_refptr<Counter> cache_misses;
  scoped_refptr<Counter> cache_misses_caching;

  scoped_refptr<AtomicGauge<uint64_t>> cache_usage;
};

// TODO(mreddy): Calculate high-level cache metrics by using
//  information from segment-level metrics.
struct SLRUCacheMetrics : public CacheMetrics {
  explicit SLRUCacheMetrics(const scoped_refptr<MetricEntity>& entity);

  scoped_refptr<Histogram> upgrades_stats;
  scoped_refptr<Histogram> downgrades_stats;

  scoped_refptr<Counter> probationary_segment_inserts;
  scoped_refptr<Counter> probationary_segment_evictions;
  scoped_refptr<AtomicGauge<uint64_t>> probationary_segment_cache_usage;

  scoped_refptr<Counter> protected_segment_inserts;
  scoped_refptr<Counter> protected_segment_evictions;
  scoped_refptr<AtomicGauge<uint64_t>> protected_segment_cache_usage;
};

} // namespace kudu
