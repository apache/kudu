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

#include "kudu/util/block_cache_metrics.h"

#include <cstdint>

#include "kudu/util/metrics.h"

METRIC_DEFINE_counter(server, block_cache_inserts,
                      "Block Cache Inserts", kudu::MetricUnit::kBlocks,
                      "Number of blocks inserted in the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, block_cache_lookups,
                      "Block Cache Lookups", kudu::MetricUnit::kBlocks,
                      "Number of blocks looked up from the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, block_cache_evictions,
                      "Block Cache Evictions", kudu::MetricUnit::kBlocks,
                      "Number of blocks evicted from the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, block_cache_misses,
                      "Block Cache Misses", kudu::MetricUnit::kBlocks,
                      "Number of lookups that didn't yield a block",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, block_cache_misses_caching,
                      "Block Cache Misses (Caching)", kudu::MetricUnit::kBlocks,
                      "Number of lookups that were expecting a block that didn't yield one. "
                      "Use this number instead of cache_misses when trying to determine how "
                      "efficient the cache is",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, block_cache_hits,
                      "Block Cache Hits", kudu::MetricUnit::kBlocks,
                      "Number of lookups that found a block",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, block_cache_hits_caching,
                      "Block Cache Hits (Caching)", kudu::MetricUnit::kBlocks,
                      "Number of lookups that were expecting a block that found one. "
                      "Use this number instead of cache_hits when trying to determine how "
                      "efficient the cache is",
                      kudu::MetricLevel::kDebug);

METRIC_DEFINE_gauge_uint64(server, block_cache_usage, "Block Cache Memory Usage",
                           kudu::MetricUnit::kBytes,
                           "Memory consumed by the block cache",
                           kudu::MetricLevel::kInfo);

METRIC_DEFINE_histogram(server, block_cache_upgrades_stats,
                        "Block Cache Upgrades Stats", kudu::MetricUnit::kBlocks,
                        "Histogram of the number of times an entry has been upgraded",
                        kudu::MetricLevel::kDebug, 1000000, 2);
METRIC_DEFINE_histogram(server, block_cache_downgrades_stats,
                        "Block Cache Downgrades Stats", kudu::MetricUnit::kBlocks,
                        "Histogram of the number of times an entry has been downgraded",
                        kudu::MetricLevel::kDebug, 1000000, 2);

METRIC_DEFINE_counter(server, block_cache_probationary_segment_inserts,
                      "Block Cache Probationary Segment Inserts", kudu::MetricUnit::kBlocks,
                      "Number of blocks inserted in the probationary segment of the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, block_cache_probationary_segment_evictions,
                      "Block Cache Probationary Segment Evictions", kudu::MetricUnit::kBlocks,
                      "Number of blocks evicted from the probationary segment of the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_gauge_uint64(server, block_cache_probationary_segment_usage,
                           "Block Cache Probationary Segment Memory Usage",
                           kudu::MetricUnit::kBytes,
                           "Memory consumed by the probationary segment of the block cache",
                           kudu::MetricLevel::kInfo);

METRIC_DEFINE_counter(server, block_cache_protected_segment_inserts,
                      "Block Cache Protected Segment Inserts", kudu::MetricUnit::kBlocks,
                      "Number of blocks inserted in the protected segment of the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_counter(server, block_cache_protected_segment_evictions,
                      "Block Cache Protected Segment Evictions", kudu::MetricUnit::kBlocks,
                      "Number of blocks evicted from the protected segment of the cache",
                      kudu::MetricLevel::kDebug);
METRIC_DEFINE_gauge_uint64(server, block_cache_protected_segment_usage,
                           "Block Cache Protected Segment Memory Usage", kudu::MetricUnit::kBytes,
                           "Memory consumed by the protected segment of the block cache",
                           kudu::MetricLevel::kInfo);

namespace kudu {

#define MINIT(member, x) member = METRIC_##x.Instantiate(entity)
#define GINIT(member, x) member = METRIC_##x.Instantiate(entity, 0)
BlockCacheMetrics::BlockCacheMetrics(const scoped_refptr<MetricEntity>& entity) {
  MINIT(inserts, block_cache_inserts);
  MINIT(lookups, block_cache_lookups);
  MINIT(evictions, block_cache_evictions);
  MINIT(cache_hits, block_cache_hits);
  MINIT(cache_hits_caching, block_cache_hits_caching);
  MINIT(cache_misses, block_cache_misses);
  MINIT(cache_misses_caching, block_cache_misses_caching);
  GINIT(cache_usage, block_cache_usage);
}

SLRUCacheMetrics::SLRUCacheMetrics(const scoped_refptr<MetricEntity>& entity) {
  MINIT(inserts, block_cache_inserts);
  MINIT(lookups, block_cache_lookups);
  MINIT(evictions, block_cache_evictions);
  MINIT(cache_hits, block_cache_hits);
  MINIT(cache_hits_caching, block_cache_hits_caching);
  MINIT(cache_misses, block_cache_misses);
  MINIT(cache_misses_caching, block_cache_misses_caching);
  GINIT(cache_usage, block_cache_usage);

  MINIT(upgrades_stats, block_cache_upgrades_stats);
  MINIT(downgrades_stats, block_cache_downgrades_stats);

  MINIT(probationary_segment_inserts, block_cache_probationary_segment_inserts);
  MINIT(probationary_segment_evictions, block_cache_probationary_segment_evictions);
  GINIT(probationary_segment_cache_usage, block_cache_probationary_segment_usage);
  MINIT(protected_segment_inserts, block_cache_protected_segment_inserts);
  MINIT(protected_segment_evictions, block_cache_protected_segment_evictions);
  GINIT(protected_segment_cache_usage, block_cache_protected_segment_usage);
}
#undef MINIT
#undef GINIT

} // namespace kudu
