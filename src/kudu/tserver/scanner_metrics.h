// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_SCANNER_METRICS_H
#define KUDU_TSERVER_SCANNER_METRICS_H

namespace kudu {

class MetricContext;
class Counter;
class Histogram;
class MonoTime;

namespace tserver {

// Keeps track of scanner related metrics for a given ScannerManager
// instance.
struct ScannerMetrics {
  explicit ScannerMetrics(const MetricContext& metric_ctx);

  // Adds the the number of microseconds that have passed since
  // 'time_started' to 'scanner_duration' histogram.
  void SubmitScannerDuration(const MonoTime& time_started);

  // Keeps track of the total number of scanners that have been
  // expired since the start of service.
  Counter* scanners_expired_since_start;

  // Keeps track of the duration of scanners.
  Histogram* scanner_duration;
};

} // namespace tserver
} // namespace kudu

#endif // KUDU_TSERVER_SCANNER_METRICS_H
