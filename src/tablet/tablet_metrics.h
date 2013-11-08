// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_METRICS_H
#define KUDU_TABLET_TABLET_METRICS_H

#include "gutil/macros.h"

namespace kudu {

class Counter;
class MetricContext;

namespace tablet {

// Container for all metrics specific to a single tablet.
struct TabletMetrics {
  explicit TabletMetrics(const MetricContext& metric_ctx);
  Counter* rows_inserted;
  Counter* rows_updated;
};

} // namespace tablet
} // namespace kudu
#endif /* KUDU_TABLET_TABLET_METRICS_H */
