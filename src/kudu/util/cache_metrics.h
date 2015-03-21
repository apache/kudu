// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_CACHE_METRICS_H
#define KUDU_UTIL_CACHE_METRICS_H

#include <stdint.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"

namespace kudu {

template<class T>
class AtomicGauge;
class Counter;
class MetricContext;

struct CacheMetrics {
  explicit CacheMetrics(const MetricContext& metric_ctx);

  scoped_refptr<Counter> inserts;
  scoped_refptr<Counter> lookups;
  scoped_refptr<Counter> evictions;
  scoped_refptr<Counter> cache_hits;
  scoped_refptr<Counter> cache_hits_caching;
  scoped_refptr<Counter> cache_misses;
  scoped_refptr<Counter> cache_misses_caching;

  scoped_refptr<AtomicGauge<uint64_t> > cache_usage;
};

} // namespace kudu
#endif /* KUDU_UTIL_CACHE_METRICS_H */
