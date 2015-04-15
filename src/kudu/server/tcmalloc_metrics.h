// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_SERVER_TCMALLOC_METRICS_H_
#define KUDU_SERVER_TCMALLOC_METRICS_H_

#include "kudu/gutil/ref_counted.h"

namespace kudu {
class MetricEntity;
namespace tcmalloc {

// Registers tcmalloc-related status etrics.
// This can be called multiple times on different entities, though the resulting
// metrics will be identical, since the tcmalloc tracking is process-wide.
void RegisterMetrics(const scoped_refptr<MetricEntity>& entity);

} // namespace tcmalloc
} // namespace kudu

#endif // KUDU_SERVER_TCMALLOC_METRICS_H_
