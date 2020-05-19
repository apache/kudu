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

#include "kudu/master/table_locations_cache_metrics.h"

#include "kudu/util/metrics.h"

METRIC_DEFINE_counter(server, table_locations_cache_inserts,
                      "Table Locations Cache Inserts",
                      kudu::MetricUnit::kEntries,
                      "Number of entries inserted in the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, table_locations_cache_lookups,
                      "Table Locations Cache Lookups",
                      kudu::MetricUnit::kEntries,
                      "Number of entries looked up from the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, table_locations_cache_evictions,
                      "Table Locations Cache Evictions",
                      kudu::MetricUnit::kEntries,
                      "Number of entries evicted from the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, table_locations_cache_misses,
                      "Table Locations Cache Misses",
                      kudu::MetricUnit::kEntries,
                      "Number of lookups that didn't find a cached entry",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, table_locations_cache_hits,
                      "Table Locations Cache Hits",
                      kudu::MetricUnit::kEntries,
                      "Number of lookups that found a cached entry",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_gauge_uint64(server, table_locations_cache_memory_usage,
                           "Table Locations Cache Memory Usage",
                           kudu::MetricUnit::kBytes,
                           "Memory consumed by the cache",
                           kudu::MetricLevel::kDebug);

namespace kudu {
namespace master {

#define GINIT(member, x) (member) = METRIC_##x.Instantiate(metric_entity, 0)

#define MINIT(member, x) \
  (member) = METRIC_##x.Instantiate(metric_entity); \
  (member)->Reset()

TableLocationsCacheMetrics::TableLocationsCacheMetrics(
    const scoped_refptr<MetricEntity>& metric_entity) {
  MINIT(inserts, table_locations_cache_inserts);
  MINIT(lookups, table_locations_cache_lookups);
  MINIT(evictions, table_locations_cache_evictions);
  MINIT(cache_hits_caching, table_locations_cache_hits);
  MINIT(cache_misses_caching, table_locations_cache_misses);
  GINIT(cache_usage, table_locations_cache_memory_usage);
}
#undef MINIT
#undef GINIT

} // namespace master
} // namespace kudu
