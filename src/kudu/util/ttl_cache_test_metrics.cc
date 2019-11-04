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

#include "kudu/util/ttl_cache_test_metrics.h"

#include "kudu/util/metrics.h"

METRIC_DEFINE_counter(server, test_ttl_cache_inserts,
                      "TTL Cache Inserts",
                      kudu::MetricUnit::kEntries,
                      "Number of entries inserted in the cache",
                      kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(server, test_ttl_cache_lookups,
                      "TTL Cache Lookups",
                      kudu::MetricUnit::kEntries,
                      "Number of entries looked up from the cache",
                      kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(server, test_ttl_cache_evictions,
                      "TTL Cache Evictions",
                      kudu::MetricUnit::kEntries,
                      "Number of entries evicted from the cache",
                      kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(server, test_ttl_cache_evictions_expired,
                      "TTL Cache Evictions of Expired Entries",
                      kudu::MetricUnit::kEntries,
                      "Number of entries that had already expired upon "
                      "eviction from the cache",
                      kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(server, test_ttl_cache_misses,
                      "TTL Cache Misses",
                      kudu::MetricUnit::kEntries,
                      "Number of lookups that didn't find a cached entry",
                      kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(server, test_ttl_cache_hits,
                      "TTL Cache Hits",
                      kudu::MetricUnit::kEntries,
                      "Number of lookups that found a cached entry",
                      kudu::MetricLevel::kInfo);
METRIC_DEFINE_counter(server, test_ttl_cache_hits_expired,
                      "TTL Cache Hits of Expired Entries",
                      kudu::MetricUnit::kEntries,
                      "Number of lookups that found an entry, but the entry "
                      "had already expired at the time of lookup",
                      kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_uint64(server, test_ttl_cache_memory_usage,
                           "TTL Cache Memory Usage",
                           kudu::MetricUnit::kBytes,
                           "Memory consumed by the cache",
                           kudu::MetricLevel::kInfo);

namespace kudu {

#define MINIT(member, x) member = METRIC_##x.Instantiate(entity)
#define GINIT(member, x) member = METRIC_##x.Instantiate(entity, 0)
TTLCacheTestMetrics::TTLCacheTestMetrics(
    const scoped_refptr<MetricEntity>& entity) {
  MINIT(inserts, test_ttl_cache_inserts);
  MINIT(lookups, test_ttl_cache_lookups);
  MINIT(evictions, test_ttl_cache_evictions);
  MINIT(evictions_expired, test_ttl_cache_evictions_expired);
  MINIT(cache_hits_caching, test_ttl_cache_hits);
  MINIT(cache_hits_expired, test_ttl_cache_hits_expired);
  MINIT(cache_misses_caching, test_ttl_cache_misses);
  GINIT(cache_usage, test_ttl_cache_memory_usage);
}
#undef MINIT
#undef GINIT

} // namespace kudu
