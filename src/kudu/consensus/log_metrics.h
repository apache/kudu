// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CONSENSUS_LOG_METRICS_H
#define KUDU_CONSENSUS_LOG_METRICS_H

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/monotime.h"

namespace kudu {

class Counter;
class Histogram;
class MetricEntity;

namespace log {

struct LogMetrics {
  explicit LogMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  // Global stats
  scoped_refptr<Counter> bytes_logged;

  // Per-group group commit stats
  scoped_refptr<Histogram> sync_latency;
  scoped_refptr<Histogram> append_latency;
  scoped_refptr<Histogram> group_commit_latency;
  scoped_refptr<Histogram> roll_latency;
  scoped_refptr<Histogram> entry_batches_per_group;
};

// TODO extract and generalize this for all histogram metrics
#define SCOPED_LATENCY_METRIC(_mtx, _h) \
  ScopedLatencyMetric _h##_metric(_mtx ? _mtx->_h.get() : NULL)

} // namespace log
} // namespace kudu

#endif // KUDU_CONSENSUS_LOG_METRICS_H
