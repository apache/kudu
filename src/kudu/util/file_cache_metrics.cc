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

#include "kudu/util/file_cache_metrics.h"

#include "kudu/util/metrics.h"

METRIC_DEFINE_counter(server, file_cache_inserts,
                      "File Cache Inserts", kudu::MetricUnit::kEntries,
                      "Number of file descriptors inserted in the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, file_cache_lookups,
                      "File Cache Lookups", kudu::MetricUnit::kEntries,
                      "Number of file descriptors looked up from the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, file_cache_evictions,
                      "File Cache Evictions", kudu::MetricUnit::kEntries,
                      "Number of file descriptors evicted from the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, file_cache_misses,
                      "File Cache Misses", kudu::MetricUnit::kEntries,
                      "Number of lookups that didn't yield a file descriptor",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, file_cache_misses_caching,
                      "File Cache Misses (Caching)", kudu::MetricUnit::kEntries,
                      "Number of lookups that were expecting a file descriptor "
                      "that didn't yield one. Use this number instead of "
                      "cache_misses when trying to determine how "
                      "efficient the cache is",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, file_cache_hits,
                      "File Cache Hits", kudu::MetricUnit::kEntries,
                      "Number of lookups that found a file descriptor",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, file_cache_hits_caching,
                      "File Cache Hits (Caching)", kudu::MetricUnit::kEntries,
                      "Number of lookups that were expecting a file descriptor "
                      "that found one. Use this number instead of cache_hits "
                      "when trying to determine how efficient the cache is",
                      kudu::MetricLevel::kDebug);

METRIC_DEFINE_gauge_uint64(server, file_cache_usage, "File Cache Usage",
                           kudu::MetricUnit::kEntries,
                           "Number of entries in the file cache",
                           kudu::MetricLevel::kInfo);

namespace kudu {

#define MINIT(member, x) member = METRIC_##x.Instantiate(entity)
#define GINIT(member, x) member = METRIC_##x.Instantiate(entity, 0)
FileCacheMetrics::FileCacheMetrics(const scoped_refptr<MetricEntity>& entity) {
  MINIT(inserts, file_cache_inserts);
  MINIT(lookups, file_cache_lookups);
  MINIT(evictions, file_cache_evictions);
  MINIT(cache_hits, file_cache_hits);
  MINIT(cache_hits_caching, file_cache_hits_caching);
  MINIT(cache_misses, file_cache_misses);
  MINIT(cache_misses_caching, file_cache_misses_caching);
  GINIT(cache_usage, file_cache_usage);
}
#undef MINIT
#undef GINIT

} // namespace kudu
