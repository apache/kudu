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
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/errno.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"

DECLARE_bool(inject_unsync_time_errors);

DEFINE_int32(ntp_initial_sync_wait_secs, 60,
             "Amount of time in seconds to wait for NTP to synchronize the "
             "clock at startup. A value of zero means Kudu will fail to start "
             "if the clock is unsynchronized. This flag can prevent Kudu from "
             "crashing if it starts before NTP can synchronize the clock.");
TAG_FLAG(ntp_initial_sync_wait_secs, evolving);
TAG_FLAG(ntp_initial_sync_wait_secs, advanced);

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace clock {

const double SystemNtp::kAdjtimexScalingFactor = 65536;
const uint64_t SystemNtp::kMicrosPerSec = 1000000;

namespace {

// Returns the current time/max error and checks if the clock is synchronized.
Status CallAdjTime(timex* tx) {
  // Set mode to 0 to query the current time.
  tx->modes = 0;
  int rc = ntp_adjtime(tx);
  if (PREDICT_FALSE(FLAGS_inject_unsync_time_errors)) {
    rc = TIME_ERROR;
  }
  switch (rc) {
    case TIME_OK:
      return Status::OK();
    case -1: // generic error
      // From 'man 2 adjtimex', ntp_adjtime failure implies an improper 'tx'.
      return Status::InvalidArgument("Error reading clock. ntp_adjtime() failed",
                                     ErrnoToString(errno));
    case TIME_ERROR:
      return Status::ServiceUnavailable(
          PREDICT_FALSE(FLAGS_inject_unsync_time_errors) ?
          "Injected clock unsync error" :
          "Error reading clock. Clock considered unsynchronized");
    default:
      // TODO what to do about leap seconds? see KUDU-146
      KLOG_FIRST_N(ERROR, 1) << "Server undergoing leap second. This may cause consistency issues "
        << "(rc=" << rc << ")";
      return Status::OK();
  }
}

void TryRun(vector<string> cmd, vector<string>* log) {
  string exe, out, err;
  Status s = FindExecutable(cmd[0], {"/sbin", "/usr/sbin/"}, &exe);
  if (!s.ok()) {
    LOG_STRING(WARNING, log) << "could not find executable: " << cmd[0];
    return;
  }

  cmd[0] = exe;
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

Status WaitForNtp() {
  int32_t wait_secs = FLAGS_ntp_initial_sync_wait_secs;
  if (wait_secs <= 0) {
    LOG(INFO) << Substitute("Not waiting for clock synchronization: "
                            "--ntp_initial_sync_wait_secs=$0 is nonpositive",
                            wait_secs);
    return Status::OK();
  }
  LOG(INFO) << Substitute("Waiting up to --ntp_initial_sync_wait_secs=$0 "
                          "seconds for the clock to synchronize", wait_secs);

  // We previously relied on ntpd/chrony support tools to wait, but that
  // approach doesn't work in environments where ntpd is unreachable but the
  // clock is still synchronized (i.e. running inside a Linux container).
  //
  // Now we just interrogate the kernel directly.
  Status s;
  for (int i = 0; i < wait_secs; i++) {
    timex timex;
    s = CallAdjTime(&timex);
    if (s.ok() || !s.IsServiceUnavailable()) {
      return s;
    }
    SleepFor(MonoDelta::FromSeconds(1));
  }

  // Return the last failure.
  return s.CloneAndPrepend("Timed out waiting for clock sync");
}

} // anonymous namespace

void SystemNtp::DumpDiagnostics(vector<string>* log) const {
  LOG_STRING(ERROR, log) << "Dumping NTP diagnostics";
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
}


Status SystemNtp::Init() {
  timex timex;
  Status s = CallAdjTime(&timex);
  if (s.IsServiceUnavailable()) {
    s = WaitForNtp().AndThen([&timex]() {
          return CallAdjTime(&timex);
        });
  }
  if (!s.ok()) {
    DumpDiagnostics(/* log= */nullptr);
    return s;
  }

  // Calculate the sleep skew adjustment according to the max tolerance of the clock.
  // Tolerance comes in parts per million but needs to be applied a scaling factor.
  skew_ppm_ = timex.tolerance / kAdjtimexScalingFactor;

  LOG(INFO) << "NTP initialized."
            << " Skew: " << skew_ppm_ << "ppm"
            << " Current error: " << timex.maxerror <<  "us";

  return Status::OK();
}


Status SystemNtp::WalltimeWithError(uint64_t *now_usec,
                                    uint64_t *error_usec) {
  // Read the time. This will return an error if the clock is not synchronized.
  timex tx;
  RETURN_NOT_OK(CallAdjTime(&tx));

  if (tx.status & STA_NANO) {
    tx.time.tv_usec /= 1000;
  }
  DCHECK_LE(tx.time.tv_usec, 1e6);

  *now_usec = tx.time.tv_sec * kMicrosPerSec + tx.time.tv_usec;
  *error_usec = tx.maxerror;
  return Status::OK();
}

} // namespace clock
} // namespace kudu
