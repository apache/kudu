// Copyright (c) 2014, Cloudera, inc.
#include "tserver/scanner_metrics.h"

#include "util/metrics.h"
#include "util/monotime.h"

METRIC_DEFINE_counter(scanners_expired_since_start,
                      kudu::MetricUnit::kScanners,
                      "Number of scanners that have expired since service start");

METRIC_DEFINE_histogram(scanner_duration, kudu::MetricUnit::kMicroseconds,
                        "Scanner duration in microseconds",
                        60000000LU, 2);

namespace kudu {

namespace tserver {

ScannerMetrics::ScannerMetrics(const MetricContext& metric_ctx)
    : scanners_expired_since_start(
          METRIC_scanners_expired_since_start.Instantiate(metric_ctx)),
      scanner_duration(METRIC_scanner_duration.Instantiate(metric_ctx)) {
}

void ScannerMetrics::SubmitScannerDuration(const MonoTime& time_started) {
  scanner_duration->Increment(
      MonoTime::Now(MonoTime::COARSE).GetDeltaSince(time_started).ToMicroseconds());
}

} // namespace tserver
} // namespace kudu
