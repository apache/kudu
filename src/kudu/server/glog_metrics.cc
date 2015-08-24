// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/server/glog_metrics.h"

#include <glog/logging.h>

#include "kudu/gutil/once.h"
#include "kudu/util/metrics.h"

METRIC_DEFINE_counter(server, glog_info_messages,
                      "INFO-level Log Messages", kudu::MetricUnit::kMessages,
                      "Number of INFO-level log messages emitted by the application.");

METRIC_DEFINE_counter(server, glog_warning_messages,
                      "WARNING-level Log Messages", kudu::MetricUnit::kMessages,
                      "Number of WARNING-level log messages emitted by the application.");

METRIC_DEFINE_counter(server, glog_error_messages,
                      "ERROR-level Log Messages", kudu::MetricUnit::kMessages,
                      "Number of ERROR-level log messages emitted by the application.");

namespace kudu {

class MetricsSink : public google::LogSink {
 public:
  explicit MetricsSink(const scoped_refptr<MetricEntity>& entity) :
    info_counter_(METRIC_glog_info_messages.Instantiate(entity)),
    warning_counter_(METRIC_glog_warning_messages.Instantiate(entity)),
    error_counter_(METRIC_glog_error_messages.Instantiate(entity)) {
  }

  virtual void send(google::LogSeverity severity, const char* full_filename,
                    const char* base_filename, int line,
                    const struct ::tm* tm_time,
                    const char* message, size_t message_len) OVERRIDE {

    Counter* c;
    switch (severity) {
      case google::INFO:
        c = info_counter_.get();
        break;
      case google::WARNING:
        c = warning_counter_.get();
        break;
      case google::ERROR:
        c = error_counter_.get();
        break;
      default:
        return;
    }

    c->Increment();
  }

 private:
  scoped_refptr<Counter> info_counter_;
  scoped_refptr<Counter> warning_counter_;
  scoped_refptr<Counter> error_counter_;
};

ScopedGLogMetrics::ScopedGLogMetrics(const scoped_refptr<MetricEntity>& entity)
  : sink_(new MetricsSink(entity)) {
  google::AddLogSink(sink_.get());
}

ScopedGLogMetrics::~ScopedGLogMetrics() {
  google::RemoveLogSink(sink_.get());
}



} // namespace kudu
