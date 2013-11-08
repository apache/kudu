// Copyright (c) 2013, Cloudera, inc.

#include "tablet/rowset.h"
#include "tablet/tablet_metrics.h"

#include "util/metrics.h"

// Tablet-specific metrics.
METRIC_DEFINE_counter(rows_inserted, kudu::MetricUnit::kRows,
    "Number of rows inserted into this tablet since service start");
METRIC_DEFINE_counter(rows_updated, kudu::MetricUnit::kRows,
    "Number of row update operations performed on this tablet since service start");
METRIC_DEFINE_counter(insertions_failed_dup_key, kudu::MetricUnit::kRows,
                      "Number of inserts which failed because the key already existed");

METRIC_DEFINE_counter(blooms_consulted, kudu::MetricUnit::kProbes,
                      "Number of times a bloom filter was consulted");
METRIC_DEFINE_counter(keys_consulted, kudu::MetricUnit::kProbes,
                      "Number of times a key cfile was consulted");
METRIC_DEFINE_counter(deltas_consulted, kudu::MetricUnit::kProbes,
                      "Number of times a delta file was consulted");
METRIC_DEFINE_counter(mrs_consulted, kudu::MetricUnit::kProbes,
                      "Number of times a MemRowSet was consulted.");

namespace kudu {
namespace tablet {

#define MINIT(x) x(METRIC_##x.Instantiate(metric_ctx))
TabletMetrics::TabletMetrics(const MetricContext& metric_ctx)
  : MINIT(rows_inserted),
    MINIT(rows_updated),
    MINIT(insertions_failed_dup_key),
    MINIT(blooms_consulted),
    MINIT(keys_consulted),
    MINIT(deltas_consulted),
    MINIT(mrs_consulted) {
}
#undef MINIT

void TabletMetrics::AddProbeStats(const ProbeStats& stats) {
  blooms_consulted->IncrementBy(stats.blooms_consulted);
  keys_consulted->IncrementBy(stats.keys_consulted);
  deltas_consulted->IncrementBy(stats.deltas_consulted);
  mrs_consulted->IncrementBy(stats.mrs_consulted);
}

} // namespace tablet
} // namespace kudu
