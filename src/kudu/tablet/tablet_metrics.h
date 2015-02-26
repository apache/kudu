// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TABLET_TABLET_METRICS_H
#define KUDU_TABLET_TABLET_METRICS_H

#include "kudu/gutil/macros.h"
#include "kudu/tablet/rowset.h"

namespace kudu {

class Counter;
template<class T>
class AtomicGauge;
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
  Counter* rows_deleted;
  Counter* insertions_failed_dup_key;
  Counter* scans_started;

  // Probe stats
  Counter* blooms_consulted;
  Counter* keys_consulted;
  Counter* deltas_consulted;
  Counter* mrs_consulted;
  Counter* bytes_flushed;

  Histogram* blooms_consulted_per_op;
  Histogram* keys_consulted_per_op;
  Histogram* deltas_consulted_per_op;

  Histogram* commit_wait_duration;
  Histogram* snapshot_scan_inflight_wait_duration;
  Histogram* write_op_duration_no_consistency;
  Histogram* write_op_duration_client_propagated_consistency;
  Histogram* write_op_duration_commit_wait_consistency;

  AtomicGauge<uint32_t>* flush_dms_running;
  AtomicGauge<uint32_t>* flush_mrs_running;
  AtomicGauge<uint32_t>* compact_rs_running;
  AtomicGauge<uint32_t>* delta_minor_compact_rs_running;

  Histogram* flush_dms_duration;
  Histogram* flush_mrs_duration;
  Histogram* compact_rs_duration;
  Histogram* delta_minor_compact_rs_duration;
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
