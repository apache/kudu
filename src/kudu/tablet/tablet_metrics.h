// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_METRICS_H
#define KUDU_TABLET_TABLET_METRICS_H

#include "kudu/gutil/macros.h"

namespace kudu {

class Counter;
class Histogram;
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
  Counter* scans_started;

  // Probe stats
  Counter* blooms_consulted;
  Counter* keys_consulted;
  Counter* deltas_consulted;
  Counter* mrs_consulted;
  Counter* bytes_flushed;

  Histogram* commit_wait_duration;
  Histogram* snapshot_scan_inflight_wait_duration;
  Histogram* write_op_duration_no_consistency;
  Histogram* write_op_duration_client_propagated_consistency;
  Histogram* write_op_duration_commit_wait_consistency;
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
