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

#include "kudu/clock/system_ntp.h"

#include <sys/time.h>
#include <sys/timex.h>

#include <cerrno>
#include <functional>
#include <limits>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/errno.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

DECLARE_bool(inject_unsync_time_errors);

using std::string;
using std::vector;
using strings::Substitute;

METRIC_DEFINE_gauge_string(server, clock_ntp_status, "Clock NTP Status String",
                           kudu::MetricUnit::kState,
                           "Output of ntp_adjtime()/ntp_gettime() kernel API call",
                           kudu::MetricLevel::kDebug);

namespace kudu {
namespace clock {

namespace {

// Convert the clock state returned by ntp_gettime() into Status.
Status NtpStateToStatus(int rc) {
  // According to http://man7.org/linux/man-pages/man3/ntp_gettime.3.html,
  // ntp_gettime() never fails if given correct pointer to the output argument.
  PCHECK(rc >= 0);
  switch (rc) {
    case TIME_OK:
      return Status::OK();
    case TIME_ERROR:
      return Status::ServiceUnavailable(
          PREDICT_FALSE(FLAGS_inject_unsync_time_errors)
          ? "Injected clock unsync error"
          : "Error reading clock. Clock considered unsynchronized");
    default:
      // TODO(dralves): what to do about leap seconds? see KUDU-146
      KLOG_FIRST_N(ERROR, 1)
          << "Server undergoing leap second. This may cause consistency issues "
          << "(rc=" << rc << ")";
      return Status::OK();
  }
}

// Check if the specified code corresponds to an error returned by ntp_adjtime()
// and convert it into Status.
Status CheckForNtpAdjtimeError(int rc) {
  // From http://man7.org/linux/man-pages/man2/adjtimex.2.html,
  // the failure implies invalid pointer for its output parameter of the 'timex'
  // type. It cannot be the case in the code below. However, there were reports
  // that with old version of Docker it might return EPERM even if 'tx.mode'
  // is set to 0; see KUDU-2000 for details.
  if (PREDICT_FALSE(rc == -1)) {
    int err = errno;
    return Status::RuntimeError("ntp_adjtime() failed", ErrnoToString(err));
  }
  return Status::OK();
}

void TryRun(vector<string> cmd, vector<string>* log) {
  string exe;
  Status s = FindExecutable(cmd[0], {"/sbin", "/usr/sbin/"}, &exe);
  if (!s.ok()) {
    LOG_STRING(WARNING, log) << "could not find executable: " << cmd[0];
    return;
  }

  cmd[0] = exe;
  string out;
  string err;
  s = Subprocess::Call(cmd, "", &out, &err);
  // Subprocess::Call() returns RuntimeError in the case that the process returns
  // a non-zero exit code, but that might still generate useful err.
  if (s.ok() || (s.IsRuntimeError() && (!out.empty() || !err.empty()))) {
    LOG_STRING(ERROR, log)
        << JoinStrings(cmd, " ")
        << "\n------------------------------------------"
        << (!out.empty() ? Substitute("\nstdout:\n$0", out) : "")
        << (!err.empty() ? Substitute("\nstderr:\n$0", err) : "")
        << "\n";
  } else {
    LOG_STRING(WARNING, log) << "failed to run executable: " << cmd[0];
  }
}

} // anonymous namespace

SystemNtp::SystemNtp(const scoped_refptr<MetricEntity>& metric_entity)
    : skew_ppm_(std::numeric_limits<int64_t>::max()),
      metric_entity_(metric_entity) {
}

Status SystemNtp::Init() {
  timex t;
  t.modes = 0; // set mode to 0 for read-only query
  const int rc = ntp_adjtime(&t);
  RETURN_NOT_OK(CheckForNtpAdjtimeError(rc));
  RETURN_NOT_OK(NtpStateToStatus(rc));

  // The unit of the reported tolerance is ppm with 16-bit fractional part:
  // 65536 is 1 ppm (see http://man7.org/linux/man-pages/man3/ntp_adjtime.3.html
  // for details).
  skew_ppm_ = t.tolerance >> 16;
  VLOG(1) << "ntp_adjtime(): tolerance is " << t.tolerance;

  METRIC_clock_ntp_status.InstantiateFunctionGauge(
      metric_entity_, []() { return ClockNtpStatusForMetrics(); })->
          AutoDetachToLastValue(&metric_detacher_);

  return Status::OK();
}

Status SystemNtp::WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) {
  if (PREDICT_FALSE(FLAGS_inject_unsync_time_errors)) {
    return NtpStateToStatus(TIME_ERROR);
  }
  // Read the clock and convert its state into status. This will return an error
  // if the clock is not synchronized.
#ifdef __APPLE__
  ntptimeval t;
  RETURN_NOT_OK(NtpStateToStatus(ntp_gettime(&t)));
  uint64_t now = static_cast<uint64_t>(t.time.tv_sec) * 1000000 +
      t.time.tv_nsec / 1000;
#else
  timex t;
  t.modes = 0; // set mode to 0 for read-only query
  const int rc = ntp_adjtime(&t);
  RETURN_NOT_OK(CheckForNtpAdjtimeError(rc));
  RETURN_NOT_OK(NtpStateToStatus(rc));
  if (t.status & STA_NANO) {
    t.time.tv_usec /= 1000;
  }
  uint64_t now = static_cast<uint64_t>(t.time.tv_sec) * 1000000 +
      t.time.tv_usec;
#endif
  *error_usec = t.maxerror;
  *now_usec = now;
  return Status::OK();
}

void SystemNtp::DumpDiagnostics(vector<string>* log) const {
  LOG_STRING(ERROR, log) << "Dumping NTP diagnostics";

  TryRun({"ntpstat"}, log);
  TryRun({"ntptime"}, log);
  // Gather as much info as possible from both ntpq and ntpdc, even
  // though some of it might be redundant. Different versions of ntp
  // expose different sets of commands through these two tools.
  // The tools will happily ignore commmands they don't understand.
  TryRun({"ntpq", "-n",
          "-c", "timeout 1000",
          "-c", "readvar",
          "-c", "sysinfo",
          "-c", "lpeers",
          "-c", "opeers",
          "-c", "version"}, log);
  TryRun({"ntpdc", "-n",
          "-c", "timeout 1000",
          "-c", "peers",
          "-c", "sysinfo",
          "-c", "sysstats",
          "-c", "version"}, log);

  TryRun({"chronyc", "-n", "tracking"}, log);
  TryRun({"chronyc", "-n", "sources"}, log);
  TryRun({"chronyc", "-n", "activity"}, log);

  MetricJsonOptions opts;
  // There are relevant histogram metrics such as 'hybrid_clock_max_errors' and
  // 'hybrid_clock_extrapolation_intervals' to output.
  opts.include_raw_histograms = true;
  // No need to output attributes -- that information could be retrieved from
  // elsewhere, if needed.
  opts.include_entity_attributes = false;
  opts.filters.entity_metrics.emplace_back("clock_ntp_status");
  opts.filters.entity_metrics.emplace_back("hybrid_clock_extrapolating");
  opts.filters.entity_metrics.emplace_back("hybrid_clock_error");
  opts.filters.entity_metrics.emplace_back("hybrid_clock_timestamp");
  opts.filters.entity_metrics.emplace_back("hybrid_clock_max_errors");
  opts.filters.entity_metrics.emplace_back("hybrid_clock_extrapolation_intervals");

  // Collect the metrics in JSON pretty-printed format.
  std::ostringstream buf;
  JsonWriter writer(&buf, JsonWriter::PRETTY);
  if (auto s = metric_entity_->WriteAsJson(&writer, opts); !s.ok()) {
    LOG_STRING(WARNING, log) << "failed to dump relevant clock metrics: " << s.ToString();
  } else {
    LOG_STRING(ERROR, log) << buf.str();
  }
}

string SystemNtp::ClockNtpStatusForMetrics() {
  static constexpr const char* const kFmt = "now:$0 maxerror:$1 status:$2";

#ifdef __APPLE__
  ntptimeval t;
  if (const auto s = NtpStateToStatus(ntp_gettime(&t)); !s.ok()) {
    return Substitute(kFmt, "n/a", "n/a", s.ToString());
  }
  const uint64_t now_usec = static_cast<uint64_t>(t.time.tv_sec) * 1000000 +
      t.time.tv_nsec / 1000;
#else
  timex t;
  t.modes = 0; // set mode to 0 for read-only query
  const int rc = ntp_adjtime(&t);

  if (const auto s = CheckForNtpAdjtimeError(rc); !s.ok()) {
    return Substitute(kFmt, "n/a", "n/a", s.ToString());
  }
  if (const auto s = NtpStateToStatus(rc); !s.ok()) {
    return Substitute(kFmt, "n/a", "n/a", s.ToString());
  }

  if (t.status & STA_NANO) {
    t.time.tv_usec /= 1000;
  }
  const uint64_t now_usec = static_cast<uint64_t>(t.time.tv_sec) * 1000000 +
      t.time.tv_usec;
#endif // #ifdef __APPLE__ ... #else ...

  return Substitute(kFmt, now_usec, t.maxerror, "ok");
}

} // namespace clock
} // namespace kudu
