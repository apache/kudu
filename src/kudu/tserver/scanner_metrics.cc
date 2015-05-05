// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/tserver/scanner_metrics.h"

#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"

METRIC_DEFINE_counter(server, scanners_expired,
                      "Scanners Expired",
                      kudu::MetricUnit::kScanners,
                      "Number of scanners that have expired since service start");

METRIC_DEFINE_histogram(server, scanner_duration,
                        "Scanner Duration",
                        kudu::MetricUnit::kMicroseconds,
                        "Histogram of the duration of active scanners on this tablet.",
                        60000000LU, 2);

namespace kudu {

namespace tserver {

ScannerMetrics::ScannerMetrics(const scoped_refptr<MetricEntity>& metric_entity)
    : scanners_expired(
          METRIC_scanners_expired.Instantiate(metric_entity)),
      scanner_duration(METRIC_scanner_duration.Instantiate(metric_entity)) {
}

void ScannerMetrics::SubmitScannerDuration(const MonoTime& time_started) {
  scanner_duration->Increment(
      MonoTime::Now(MonoTime::COARSE).GetDeltaSince(time_started).ToMicroseconds());
}

} // namespace tserver
} // namespace kudu
