// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/server/diagnostics_log.h"

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/condition_variable.h"
#include "kudu/util/env.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/rolling_log.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace server {

DiagnosticsLog::DiagnosticsLog(string log_dir,
                               MetricRegistry* metric_registry) :
    log_dir_(std::move(log_dir)),
    metric_registry_(metric_registry),
    wake_(&lock_),
    metrics_log_interval_(MonoDelta::FromSeconds(60)) {
}
DiagnosticsLog::~DiagnosticsLog() {
  Stop();
}

void DiagnosticsLog::SetMetricsLogInterval(MonoDelta interval) {
  MutexLock l(lock_);
  metrics_log_interval_ = interval;
}


Status DiagnosticsLog::Start() {
  unique_ptr<RollingLog> l(new RollingLog(Env::Default(), log_dir_, "diagnostics"));
  RETURN_NOT_OK_PREPEND(l->Open(), "unable to open diagnostics log");
  log_ = std::move(l);
  Status s = Thread::Create("server", "diag-logger",
                            &DiagnosticsLog::RunThread,
                            this, &thread_);
  if (!s.ok()) {
    // Don't leave the log open if we failed to start our thread.
    log_.reset();
  }
  return s;
}

void DiagnosticsLog::Stop() {
  if (!thread_) return;

  {
    MutexLock l(lock_);
    stop_ = true;
    wake_.Signal();
  }
  thread_->Join();
  thread_.reset();
  stop_ = false;
  WARN_NOT_OK(log_->Close(), "Unable to close diagnostics log");
}

void DiagnosticsLog::RunThread() {
  // How long to wait before trying again if we experience a failure
  // logging metrics.
  const MonoDelta kWaitBetweenFailures = MonoDelta::FromSeconds(60);

  MutexLock l(lock_);

  MonoTime next_log = MonoTime::Now();
  while (!stop_) {
    wake_.TimedWait(next_log - MonoTime::Now());
    if (MonoTime::Now() > next_log) {
      Status s = LogMetrics();
      if (!s.ok()) {
        WARN_NOT_OK(s, Substitute(
            "Unable to collect metrics to diagnostics log. Will try again in $0",
            kWaitBetweenFailures.ToString()));
        next_log = MonoTime::Now() + kWaitBetweenFailures;
      } else {
        next_log = MonoTime::Now() + metrics_log_interval_;
      }
    }
  }
}

Status DiagnosticsLog::LogMetrics() {
  MetricJsonOptions opts;
  opts.include_raw_histograms = true;

  opts.only_modified_in_or_after_epoch = metrics_epoch_;

  // We don't output any metrics which have never been incremented. Though
  // this seems redundant with the "only include changed metrics" above, it
  // also ensures that we don't dump a bunch of zero data on startup.
  opts.include_untouched_metrics = false;

  // Entity attributes aren't that useful in the context of this log. We can
  // always grab the entity attributes separately if necessary.
  opts.include_entity_attributes = false;

  std::ostringstream buf;
  MicrosecondsInt64 now = GetCurrentTimeMicros();
  buf << "I" << FormatTimestampForLog(now)
      << " metrics " << now << " ";

  // Collect the metrics JSON string.
  int64_t this_log_epoch = Metric::current_epoch();
  Metric::IncrementEpoch();
  JsonWriter writer(&buf, JsonWriter::COMPACT);
  RETURN_NOT_OK(metric_registry_->WriteAsJson(&writer, {"*"}, opts));
  buf << "\n";

  RETURN_NOT_OK(log_->Append(buf.str()));

  // Next time we fetch, only show those that changed after the epoch
  // we just logged.
  //
  // NOTE: we only bump this in the successful log case so that if we failed to
  // write above, we wouldn't skip any changes.
  metrics_epoch_ = this_log_epoch + 1;
  return Status::OK();
}


} // namespace server
} // namespace kudu
