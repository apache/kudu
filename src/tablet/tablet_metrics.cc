// Copyright (c) 2013, Cloudera, inc.

#include "tablet/tablet_metrics.h"

#include "util/metrics.h"

// Tablet-specific metrics.
METRIC_DEFINE_counter(rows_inserted, kudu::MetricUnit::kRows,
    "Number of rows inserted into this tablet since service start");
METRIC_DEFINE_counter(rows_updated, kudu::MetricUnit::kRows,
    "Number of row update operations performed on this tablet since service start");

namespace kudu {
namespace tablet {

TabletMetrics::TabletMetrics(const MetricContext& metric_ctx)
  : rows_inserted(FindOrCreateCounter(metric_ctx, METRIC_rows_inserted)),
    rows_updated(FindOrCreateCounter(metric_ctx, METRIC_rows_updated)) {
}

} // namespace tablet
} // namespace kudu
