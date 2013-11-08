// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_METRICS_H
#define KUDU_TABLET_TABLET_METRICS_H

#include "gutil/macros.h"

namespace kudu {

class Counter;
class MetricContext;

namespace tablet {

struct ProbeStats;

// Container for all metrics specific to a single tablet.
struct TabletMetrics {
  explicit TabletMetrics(const MetricContext& metric_ctx);

  void AddProbeStats(const ProbeStats& stats);

  // Operation rates
  Counter* rows_inserted;
  Counter* rows_updated;
  Counter* insertions_failed_dup_key;

  // Probe stats
  Counter* blooms_consulted;
  Counter* keys_consulted;
  Counter* deltas_consulted;
  Counter* mrs_consulted;
};

class ProbeStatsSubmitter {
 public:
  ProbeStatsSubmitter(const ProbeStats& stats, TabletMetrics* metrics)
    : stats_(stats),
      metrics_(metrics) {
  }

  ~ProbeStatsSubmitter() {
    if (metrics_) {
      metrics_->AddProbeStats(stats_);
    }
  }

 private:
  const ProbeStats& stats_;
  TabletMetrics* const metrics_;

  DISALLOW_COPY_AND_ASSIGN(ProbeStatsSubmitter);
};

} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_TABLET_METRICS_H */
