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

#include "kudu/master/sentry_privileges_cache_metrics.h"

#include "kudu/util/metrics.h"

METRIC_DEFINE_counter(server, sentry_privileges_cache_inserts,
                      "Sentry Privileges Cache Inserts",
                      kudu::MetricUnit::kEntries,
                      "Number of entries inserted in the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, sentry_privileges_cache_lookups,
                      "Sentry Privileges Cache Lookups",
                      kudu::MetricUnit::kEntries,
                      "Number of entries looked up from the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, sentry_privileges_cache_evictions,
                      "Sentry Privileges Cache Evictions",
                      kudu::MetricUnit::kEntries,
                      "Number of entries evicted from the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, sentry_privileges_cache_evictions_expired,
                      "Sentry Privileges Cache Evictions of Expired Entries",
                      kudu::MetricUnit::kEntries,
                      "Number of entries that had already expired upon "
                      "eviction from the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, sentry_privileges_cache_misses,
                      "Sentry Privileges Cache Misses",
                      kudu::MetricUnit::kEntries,
                      "Number of lookups that didn't find a cached entry",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, sentry_privileges_cache_hits,
                      "Sentry Privileges Cache Hits",
                      kudu::MetricUnit::kEntries,
                      "Number of lookups that found a cached entry",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, sentry_privileges_cache_hits_expired,
                      "Sentry Privileges Cache Hits of Expired Entries",
                      kudu::MetricUnit::kEntries,
                      "Number of lookups that found an entry, but the entry "
                      "had already expired at the time of lookup",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_gauge_uint64(server, sentry_privileges_cache_memory_usage,
                           "Sentry Privileges Cache Memory Usage",
                           kudu::MetricUnit::kBytes,
                           "Memory consumed by the cache",
                           kudu::MetricLevel::kDebug);

namespace kudu {
namespace master {

#define MINIT(member, x) member = METRIC_##x.Instantiate(metric_entity)
#define GINIT(member, x) member = METRIC_##x.Instantiate(metric_entity, 0)
SentryPrivilegesCacheMetrics::SentryPrivilegesCacheMetrics(
    const scoped_refptr<MetricEntity>& metric_entity) {
  MINIT(inserts, sentry_privileges_cache_inserts);
  MINIT(lookups, sentry_privileges_cache_lookups);
  MINIT(evictions, sentry_privileges_cache_evictions);
  MINIT(evictions_expired, sentry_privileges_cache_evictions_expired);
  MINIT(cache_hits_caching, sentry_privileges_cache_hits);
  MINIT(cache_hits_expired, sentry_privileges_cache_hits_expired);
  MINIT(cache_misses_caching, sentry_privileges_cache_misses);
  GINIT(cache_usage, sentry_privileges_cache_memory_usage);
}
#undef MINIT
#undef GINIT

} // namespace master
} // namespace kudu
