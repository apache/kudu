// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_CACHE_METRICS_H
#define KUDU_UTIL_CACHE_METRICS_H

#include <stdint.h>

#include "kudu/gutil/macros.h"

namespace kudu {

template<class T>
class AtomicGauge;
class Counter;
class MetricContext;

struct CacheMetrics {
  explicit CacheMetrics(const MetricContext& metric_ctx);

  Counter* inserts;
  Counter* lookups;
  Counter* evictions;
  Counter* cache_hits;
  Counter* cache_misses;

  AtomicGauge<uint64_t>* cache_usage;
};

} // namespace kudu
#endif /* KUDU_UTIL_CACHE_METRICS_H */
