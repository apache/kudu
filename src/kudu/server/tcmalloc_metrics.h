// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_SERVER_TCMALLOC_METRICS_H_
#define KUDU_SERVER_TCMALLOC_METRICS_H_

namespace kudu {
class MetricRegistry;
namespace tcmalloc {

// Registers tcmalloc-related status etrics.
// Takes a registry instead of a context because these are all process-wide metrics.
void RegisterMetrics(MetricRegistry* registry);

} // namespace tcmalloc
} // namespace kudu

#endif // KUDU_SERVER_TCMALLOC_METRICS_H_
