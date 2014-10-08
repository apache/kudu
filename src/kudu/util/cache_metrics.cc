// Copyright (c) 2014, Cloudera, inc.

#include "kudu/util/cache_metrics.h"

#include "kudu/util/metrics.h"

METRIC_DEFINE_counter(inserts, kudu::MetricUnit::kBlocks,
                      "Number of blocks inserted in the cache");
METRIC_DEFINE_counter(lookups, kudu::MetricUnit::kBlocks,
                      "Number of blocks looked up from the cache");
METRIC_DEFINE_counter(evictions, kudu::MetricUnit::kBlocks,
                      "Number of block evicted from the cache");
METRIC_DEFINE_counter(cache_misses, kudu::MetricUnit::kBlocks,
                      "Number of lookups that didn't yield a block");
METRIC_DEFINE_counter(cache_hits, kudu::MetricUnit::kBlocks,
                      "Number of lookups that found a block");

METRIC_DEFINE_gauge_uint64(cache_usage, kudu::MetricUnit::kBytes,
                           "Number of bytes of memory consumed by the cache");

namespace kudu {

#define MINIT(x) x(METRIC_##x.Instantiate(metric_ctx))
#define GINIT(x) x(AtomicGauge<uint64_t>::Instantiate(METRIC_##x, metric_ctx))
CacheMetrics::CacheMetrics(const MetricContext& metric_ctx)
  : MINIT(inserts),
    MINIT(lookups),
    MINIT(evictions),
    MINIT(cache_hits),
    MINIT(cache_misses),
    GINIT(cache_usage) {
}
#undef MINIT
#undef GINIT

} // namespace kudu
