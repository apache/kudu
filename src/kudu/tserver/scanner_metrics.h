// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_SCANNER_METRICS_H
#define KUDU_TSERVER_SCANNER_METRICS_H

#include "kudu/gutil/ref_counted.h"

namespace kudu {

class MetricEntity;
class Counter;
class Histogram;
class MonoTime;

namespace tserver {

// Keeps track of scanner related metrics for a given ScannerManager
// instance.
struct ScannerMetrics {
  explicit ScannerMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  // Adds the the number of microseconds that have passed since
  // 'time_started' to 'scanner_duration' histogram.
  void SubmitScannerDuration(const MonoTime& time_started);

  // Keeps track of the total number of scanners that have been
  // expired since the start of service.
  scoped_refptr<Counter> scanners_expired;

  // Keeps track of the duration of scanners.
  scoped_refptr<Histogram> scanner_duration;
};

} // namespace tserver
} // namespace kudu

#endif // KUDU_TSERVER_SCANNER_METRICS_H
