// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/consensus/log_metrics.h"

#include "kudu/util/metrics.h"

METRIC_DEFINE_counter(tablet, log_bytes_logged, "Bytes Written to WAL",
                      kudu::MetricUnit::kBytes,
                      "Number of bytes logged since service start");

METRIC_DEFINE_histogram(tablet, log_sync_latency, "Log Sync Latency",
                        kudu::MetricUnit::kMicroseconds,
                        "Microseconds spent on synchronizing the log segment file",
                        60000000LU, 2);

METRIC_DEFINE_histogram(tablet, log_append_latency, "Log Append Latency",
                        kudu::MetricUnit::kMicroseconds,
                        "Microseconds spent on appending to the log segment file",
                        60000000LU, 2);

METRIC_DEFINE_histogram(tablet, log_group_commit_latency, "Log Group Commit Latency",
                        kudu::MetricUnit::kMicroseconds,
                        "Microseconds spent on committing an entire group",
                        60000000LU, 2);

METRIC_DEFINE_histogram(tablet, log_roll_latency, "Log Roll Latency",
                        kudu::MetricUnit::kMicroseconds,
                        "Microseconds spent on rolling over to a new log segment file",
                        60000000LU, 2);

METRIC_DEFINE_histogram(tablet, log_entry_batches_per_group, "Log Group Commit Batch Size",
                        kudu::MetricUnit::kRequests,
                        "Number of log entry batches in a group commit group",
                        1024, 2);

namespace kudu {
namespace log {

#define MINIT(x) x(METRIC_log_##x.Instantiate(metric_entity))
LogMetrics::LogMetrics(const scoped_refptr<MetricEntity>& metric_entity)
    : MINIT(bytes_logged),
      MINIT(sync_latency),
      MINIT(append_latency),
      MINIT(group_commit_latency),
      MINIT(roll_latency),
      MINIT(entry_batches_per_group) {
}
#undef MINIT

} // namespace log
} // namespace kudu
