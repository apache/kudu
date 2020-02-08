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

#include "kudu/clock/hybrid_clock.h"

#include <algorithm>
#include <mutex>
#include <ostream>
#include <string>

#include <boost/algorithm/string/predicate.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/clock/builtin_ntp.h"
#include "kudu/clock/mock_ntp.h"
#include "kudu/clock/system_ntp.h"
#include "kudu/clock/system_unsync_time.h"
#include "kudu/gutil/bind.h"
#include "kudu/gutil/bind_helpers.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

using kudu::Status;
using std::string;
using strings::Substitute;

DEFINE_int32(max_clock_sync_error_usec, 10 * 1000 * 1000, // 10 secs
             "Maximum allowed clock synchronization error as reported by NTP "
             "before the server will abort.");
TAG_FLAG(max_clock_sync_error_usec, advanced);
TAG_FLAG(max_clock_sync_error_usec, runtime);

DEFINE_bool(use_hybrid_clock, true,
            "Whether HybridClock should be used as the default clock "
            "implementation. This should be disabled for testing purposes only.");
TAG_FLAG(use_hybrid_clock, hidden);

// Use the 'system' time source by default in standard (non-test) environment.
// This requires local machine clock to be NTP-synchronized.
DEFINE_string(time_source,
#if defined(KUDU_HAS_SYSTEM_TIME_SOURCE)
              "system",
#else
              "system_unsync",
#endif
              "The time source that HybridClock should use. Must be one of "
              "'builtin', "
#if defined(KUDU_HAS_SYSTEM_TIME_SOURCE)
              "'system', "
#endif
              "'system_unsync', or 'mock' "
              "('system_unsync' and 'mock' are for tests only)");
TAG_FLAG(time_source, evolving);
DEFINE_validator(time_source, [](const char* flag_name, const string& value) {
  if (boost::iequals(value, "builtin") ||
#if defined(KUDU_HAS_SYSTEM_TIME_SOURCE)
      boost::iequals(value, "system") ||
#endif
      boost::iequals(value, "system_unsync") ||
      boost::iequals(value, "mock")) {
    return true;
  }
  LOG(ERROR) << Substitute("unknown value for --$0 flag: '$1' "
                           "(expected one of 'system', 'builtin', or 'mock')",
                           flag_name, value);
  return false;
});

DEFINE_int32(ntp_initial_sync_wait_secs, 60,
             "Amount of time in seconds to wait for clock synchronisation at "
             "startup. A value of zero means Kudu will fail to start "
             "if the clock is unsynchronized. This flag can prevent Kudu from "
             "crashing if it starts before NTP can synchronize the clock.");
TAG_FLAG(ntp_initial_sync_wait_secs, advanced);
TAG_FLAG(ntp_initial_sync_wait_secs, evolving);

DECLARE_bool(unlock_unsafe_flags);

// This group flag validator is a guardrail to help using proper time source
// in production.
//
// The validator makes it necessary to explicitly enable unsafe flags
// (i.e. set the --unlock_unsafe_flags flag to 'true') if configuring
// --time_source to be 'system_unsync' or 'mock': these timesources are for
// experimental and test clusters only.
bool ValidateTimeSource() {
  if ((FLAGS_time_source == "system_unsync" ||
       FLAGS_time_source == "mock") && !FLAGS_unlock_unsafe_flags) {
    LOG(ERROR) << "--unlock_unsafe_flags should be set if configuring "
                  "--time_source to be 'system_unsync' or 'mock'";
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(time_source_guardrail, ValidateTimeSource);

METRIC_DEFINE_gauge_uint64(server, hybrid_clock_timestamp,
                           "Hybrid Clock Timestamp",
                           kudu::MetricUnit::kMicroseconds,
                           "Hybrid clock timestamp.",
                           kudu::MetricLevel::kInfo);
METRIC_DEFINE_gauge_uint64(server, hybrid_clock_error,
                           "Hybrid Clock Error",
                           kudu::MetricUnit::kMicroseconds,
                           "Server clock maximum error.",
                           kudu::MetricLevel::kInfo);

namespace kudu {
namespace clock {

namespace {

Status CheckDeadlineNotWithinMicros(const MonoTime& deadline, int64_t wait_for_usec) {
  if (!deadline.Initialized()) {
    // No deadline.
    return Status::OK();
  }
  int64_t us_until_deadline = (deadline - MonoTime::Now()).ToMicroseconds();
  if (us_until_deadline <= wait_for_usec) {
    return Status::TimedOut(Substitute(
        "specified time is $0us in the future, but deadline expires in $1us",
        wait_for_usec, us_until_deadline));
  }
  return Status::OK();
}

}  // anonymous namespace

// Left shifting 12 bits gives us 12 bits for the logical value
// and should still keep accurate microseconds time until 2100+
const int HybridClock::kBitsToShift = 12;
// This mask gives us back the logical bits.
const uint64_t HybridClock::kLogicalBitMask = (1 << kBitsToShift) - 1;


HybridClock::HybridClock()
    : next_timestamp_(0),
      state_(kNotInitialized) {
}

Status HybridClock::Init() {
  if (boost::iequals(FLAGS_time_source, "mock")) {
    time_service_.reset(new clock::MockNtp);
#if defined(KUDU_HAS_SYSTEM_TIME_SOURCE)
  } else if (boost::iequals(FLAGS_time_source, "system")) {
    time_service_.reset(new clock::SystemNtp);
#endif
  } else if (boost::iequals(FLAGS_time_source, "system_unsync")) {
    time_service_.reset(new clock::SystemUnsyncTime);
  } else if (boost::iequals(FLAGS_time_source, "builtin")) {
    time_service_.reset(new clock::BuiltInNtp);
  } else {
    return Status::InvalidArgument("invalid NTP source", FLAGS_time_source);
  }
  RETURN_NOT_OK(time_service_->Init());

  // Make sure the underlying clock service is available (e.g., for NTP-based
  // clock make sure it's synchronized with its NTP source). If requested, wait
  // up to the specified timeout for the clock to become ready to use.
  const auto wait_s = FLAGS_ntp_initial_sync_wait_secs;
  const auto deadline = MonoTime::Now() + MonoDelta::FromSeconds(wait_s);
  bool need_log = true;
  Status s;
  uint64_t now_usec;
  uint64_t error_usec;
  int poll_backoff_ms = 1;
  do {
    s = time_service_->WalltimeWithError(&now_usec, &error_usec);
    if (!s.IsServiceUnavailable()) {
      break;
    }
    if (need_log) {
      // Log about what's going on, just once.
      if (wait_s > 0) {
        LOG(INFO) << Substitute("waiting up to --ntp_initial_sync_wait_secs=$0 "
                                "seconds for the clock to synchronize", wait_s);
      } else {
        LOG(INFO) << Substitute("not waiting for clock synchronization: "
                                "--ntp_initial_sync_wait_secs=$0 is nonpositive",
                                wait_s);
      }
      need_log = false;
    }
    SleepFor(MonoDelta::FromMilliseconds(poll_backoff_ms));
    poll_backoff_ms = std::min(2 * poll_backoff_ms, 1000);
  } while (MonoTime::Now() < deadline);

  if (!s.ok()) {
    time_service_->DumpDiagnostics(/* log= */nullptr);
    return s.CloneAndPrepend("timed out waiting for clock synchronisation");
  }

  LOG(INFO) << Substitute("HybridClock initialized: "
                          "now $0 us; error $1 us; skew $2 ppm",
                          now_usec, error_usec, time_service_->skew_ppm());
  state_ = kInitialized;
  return Status::OK();
}

Timestamp HybridClock::Now() {
  Timestamp now;
  uint64_t error;

  std::lock_guard<simple_spinlock> lock(lock_);
  NowWithError(&now, &error);
  return now;
}

Timestamp HybridClock::NowLatest() {
  Timestamp now;
  uint64_t error;

  {
    std::lock_guard<simple_spinlock> lock(lock_);
    NowWithError(&now, &error);
  }

  uint64_t now_latest = GetPhysicalValueMicros(now) + error;
  uint64_t now_logical = GetLogicalValue(now);

  return TimestampFromMicrosecondsAndLogicalValue(now_latest, now_logical);
}

Status HybridClock::GetGlobalLatest(Timestamp* t) {
  Timestamp now = Now();
  uint64_t now_latest = GetPhysicalValueMicros(now) + FLAGS_max_clock_sync_error_usec;
  uint64_t now_logical = GetLogicalValue(now);
  *t = TimestampFromMicrosecondsAndLogicalValue(now_latest, now_logical);
  return Status::OK();
}

void HybridClock::NowWithError(Timestamp* timestamp, uint64_t* max_error_usec) {

  DCHECK_EQ(state_, kInitialized) << "Clock not initialized. Must call Init() first.";

  uint64_t now_usec;
  uint64_t error_usec;
  WalltimeWithErrorOrDie(&now_usec, &error_usec);

  // If the physical time from the system clock is higher than our last-returned
  // time, we should use the physical timestamp.
  uint64_t candidate_phys_timestamp = now_usec << kBitsToShift;
  if (PREDICT_TRUE(candidate_phys_timestamp > next_timestamp_)) {
    next_timestamp_ = candidate_phys_timestamp;
    *timestamp = Timestamp(next_timestamp_++);
    *max_error_usec = error_usec;
    VLOG(2) << Substitute("Current clock is higher than the last one. "
                          "Resetting logical values. Physical Value: $0 usec "
                          "Logical Value: 0  Error: $1",
                          now_usec, error_usec);
    return;
  }

  // We don't have the last time read max error since it might have originated
  // in another machine, but we can put a bound on the maximum error of the
  // timestamp we are providing.
  // In particular we know that the "true" time falls within the interval
  // now_usec +- now.maxerror so we get the following situations:
  //
  // 1)
  // --------|----------|----|---------|--------------------------> time
  //     now - e       now  last   now + e
  // 2)
  // --------|----------|--------------|------|-------------------> time
  //     now - e       now         now + e   last
  //
  // Assuming, in the worst case, that the "true" time is now - error we need to
  // always return: last - (now - e) as the new maximum error.
  // This broadens the error interval for both cases but always returns
  // a correct error interval.

  *max_error_usec = (next_timestamp_ >> kBitsToShift) - (now_usec - error_usec);
  *timestamp = Timestamp(next_timestamp_++);
  VLOG(2) << Substitute("Current clock is lower than the last one. Returning "
                        "last read and incrementing logical values. "
                        "Clock: $0 Error: $1",
                        Stringify(*timestamp), *max_error_usec);
}

Status HybridClock::Update(const Timestamp& to_update) {
  std::lock_guard<simple_spinlock> lock(lock_);
  Timestamp now;
  uint64_t error_ignored;
  NowWithError(&now, &error_ignored);

  // If the incoming message is in the past relative to our current
  // physical clock, there's nothing to do.
  if (PREDICT_TRUE(now > to_update)) {
    return Status::OK();
  }

  uint64_t to_update_physical = GetPhysicalValueMicros(to_update);
  uint64_t now_physical = GetPhysicalValueMicros(now);
  DCHECK_GE(to_update_physical, now_physical);

  // Don't update our clock if 'to_update' is more than
  // '--max_clock_sync_error_usec' into the future as it might have been
  // corrupted or originated from an out-of-sync server.
  if (to_update_physical - now_physical > FLAGS_max_clock_sync_error_usec) {
    return Status::InvalidArgument(Substitute(
        "tried to update clock beyond the error threshold of $0us: "
        "now $1, to_update $2 (now_physical $3, to_update_physical $4)",
        FLAGS_max_clock_sync_error_usec,
        now.ToUint64(), to_update.ToUint64(), now_physical, to_update_physical));
  }

  // Our next timestamp must be higher than the one that we are updating from.
  next_timestamp_ = to_update.value() + 1;
  return Status::OK();
}

bool HybridClock::SupportsExternalConsistencyMode(ExternalConsistencyMode mode) {
  return true;
}

bool HybridClock::HasPhysicalComponent() const {
  return true;
}

MonoDelta HybridClock::GetPhysicalComponentDifference(Timestamp lhs, Timestamp rhs) const {
  return MonoDelta::FromMicroseconds(static_cast<int64_t>(GetPhysicalValueMicros(lhs)) -
                                     static_cast<int64_t>(GetPhysicalValueMicros(rhs)));
}

Status HybridClock::WaitUntilAfter(const Timestamp& then,
                                   const MonoTime& deadline) {
  TRACE_EVENT0("clock", "HybridClock::WaitUntilAfter");
  Timestamp now;
  uint64_t error;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    NowWithError(&now, &error);
  }

  // "unshift" the timestamps so that we can measure actual time
  uint64_t now_usec = GetPhysicalValueMicros(now);
  uint64_t then_latest_usec = GetPhysicalValueMicros(then);

  uint64_t now_earliest_usec = now_usec - error;

  // Case 1, event happened definitely in the past, return
  if (PREDICT_TRUE(then_latest_usec < now_earliest_usec)) {
    return Status::OK();
  }

  // Case 2 wait out until we are sure that then has passed

  // We'll sleep then_latest_usec - now_earliest_usec so that the new
  // nw.earliest is higher than then.latest.
  uint64_t wait_for_usec = (then_latest_usec - now_earliest_usec);

  // Additionally adjust the sleep time with the max tolerance adjustment
  // to account for the worst case clock skew while we're sleeping.
  wait_for_usec *= (1 + (time_service_->skew_ppm() / 1000000.0));

  // Check that sleeping wouldn't sleep longer than our deadline.
  RETURN_NOT_OK(CheckDeadlineNotWithinMicros(deadline, wait_for_usec));

  SleepFor(MonoDelta::FromMicroseconds(wait_for_usec));

  VLOG(1) << "WaitUntilAfter(): Incoming time(latest): " << then_latest_usec
          << " Now(earliest): " << now_earliest_usec << " error: " << error
          << " Waiting for: " << wait_for_usec;
  return Status::OK();
}

Status HybridClock::WaitUntilAfterLocally(const Timestamp& then,
                                          const MonoTime& deadline) {
  Timestamp now;
  uint64_t error;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    NowWithError(&now, &error);
  }
  if (now > then) {
    return Status::OK();
  }
  uint64_t wait_for_usec = GetPhysicalValueMicros(then) - GetPhysicalValueMicros(now);

  // Check that sleeping wouldn't sleep longer than our deadline.
  RETURN_NOT_OK(CheckDeadlineNotWithinMicros(deadline, wait_for_usec));

  SleepFor(MonoDelta::FromMicroseconds(wait_for_usec));

  return Status::OK();
}

bool HybridClock::IsAfter(Timestamp t) {
  // Manually get the time, rather than using Now(), so we don't end up causing
  // a time update.
  uint64_t now_usec;
  uint64_t error_usec;
  WalltimeWithErrorOrDie(&now_usec, &error_usec);

  Timestamp now;
  {
    std::lock_guard<simple_spinlock> lock(lock_);
    now = Timestamp(std::max(next_timestamp_, now_usec << kBitsToShift));
  }
  return t.value() < now.value();
}

void HybridClock::WalltimeWithErrorOrDie(uint64_t* now_usec, uint64_t* error_usec) {
  Status s = WalltimeWithError(now_usec, error_usec);
  if (PREDICT_FALSE(!s.ok())) {
    time_service_->DumpDiagnostics(/*log=*/nullptr);
    CHECK_OK_PREPEND(s, "unable to get current time with error bound");
  }
}

Status HybridClock::WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec) {
  bool is_extrapolated = false;
  auto read_time_before = MonoTime::Now();
  Status s = time_service_->WalltimeWithError(now_usec, error_usec);
  auto read_time_after = MonoTime::Now();

  if (PREDICT_TRUE(s.ok())) {
    // We got a good clock read. Remember this in case the clock later becomes
    // unsynchronized and we need to extrapolate from here.
    //
    // Note that the actual act of reading the clock could have taken some time
    // (eg if we context-switched out) so we need to account for that by adding
    // some extra error.
    //
    //  A         B          C
    //  |---------|----------|
    //
    //  A = read_time_before (monotime)
    //  B = now_usec (walltime reading)
    //  C = read_time_after (monotime)
    //
    // We don't know whether 'B' was halfway in between 'A' and 'C' or elsewhere.
    // The max likelihood estimate is that 'B' corresponds to the average of 'A'
    // and 'C'. Then we need to add in this uncertainty (half of C - A) into any
    // future clock readings that we extrapolate from this estimate.
    int64_t read_duration_us = (read_time_after - read_time_before).ToMicroseconds();
    int64_t read_time_error_us = read_duration_us / 2;
    MonoTime read_time_max_likelihood = read_time_before +
        MonoDelta::FromMicroseconds(read_time_error_us);

    std::unique_lock<simple_spinlock> l(last_clock_read_lock_);
    if (!last_clock_read_time_.Initialized() ||
        last_clock_read_time_ < read_time_max_likelihood) {
      last_clock_read_time_ = read_time_max_likelihood;
      last_clock_read_physical_ = *now_usec;
      last_clock_read_error_ = *error_usec + read_time_error_us;
    }
  } else {
    // We failed to read the clock. Extrapolate the new time based on our
    // last successful read.
    std::unique_lock<simple_spinlock> l(last_clock_read_lock_);
    if (!last_clock_read_time_.Initialized()) {
      RETURN_NOT_OK_PREPEND(s, "could not read system time source");
    }
    MonoDelta time_since_last_read = read_time_after - last_clock_read_time_;
    int64_t micros_since_last_read = time_since_last_read.ToMicroseconds();
    int64_t accum_error_us = (micros_since_last_read * time_service_->skew_ppm()) / 1000000;
    *now_usec = last_clock_read_physical_ + micros_since_last_read;
    *error_usec = last_clock_read_error_ + accum_error_us;
    is_extrapolated = true;
    l.unlock();
    // Log after unlocking to minimize the lock hold time.
    KLOG_EVERY_N_SECS(ERROR, 1) << "Unable to read clock for last "
                                << time_since_last_read.ToString() << ": " << s.ToString();

  }

  // If the clock is synchronized but has max_error beyond max_clock_sync_error_usec
  // we also return a non-ok status.
  if (*error_usec > FLAGS_max_clock_sync_error_usec) {
    return Status::ServiceUnavailable(Substitute(
        "clock error estimate ($0us) too high (clock considered $1 by the kernel)",
        *error_usec,
        is_extrapolated ? "unsynchronized" : "synchronized"));
  }
  return kudu::Status::OK();
}


// Used to get the timestamp for metrics.
uint64_t HybridClock::NowForMetrics() {
  return Now().ToUint64();
}

// Used to get the current error, for metrics.
uint64_t HybridClock::ErrorForMetrics() {
  Timestamp now;
  uint64_t error;

  std::lock_guard<simple_spinlock> lock(lock_);
  NowWithError(&now, &error);
  return error;
}

void HybridClock::RegisterMetrics(const scoped_refptr<MetricEntity>& metric_entity) {
  METRIC_hybrid_clock_timestamp.InstantiateFunctionGauge(
      metric_entity,
      Bind(&HybridClock::NowForMetrics, Unretained(this)))
    ->AutoDetachToLastValue(&metric_detacher_);
  METRIC_hybrid_clock_error.InstantiateFunctionGauge(
      metric_entity,
      Bind(&HybridClock::ErrorForMetrics, Unretained(this)))
    ->AutoDetachToLastValue(&metric_detacher_);
}

string HybridClock::Stringify(Timestamp timestamp) {
  return StringifyTimestamp(timestamp);
}

uint64_t HybridClock::GetLogicalValue(const Timestamp& timestamp) {
  return timestamp.value() & kLogicalBitMask;
}

uint64_t HybridClock::GetPhysicalValueMicros(const Timestamp& timestamp) {
  return timestamp.value() >> kBitsToShift;
}

Timestamp HybridClock::TimestampFromMicroseconds(uint64_t micros) {
  return Timestamp(micros << kBitsToShift);
}

Timestamp HybridClock::TimestampFromMicrosecondsAndLogicalValue(
    uint64_t micros,
    uint64_t logical_value) {
  return Timestamp((micros << kBitsToShift) + logical_value);
}

Timestamp HybridClock::AddPhysicalTimeToTimestamp(const Timestamp& original,
                                                  const MonoDelta& to_add) {
  int64_t new_physical = static_cast<int64_t>(GetPhysicalValueMicros(original))
                       + to_add.ToMicroseconds();
  int64_t old_logical = GetLogicalValue(original);
  return TimestampFromMicrosecondsAndLogicalValue(new_physical, old_logical);
}

string HybridClock::StringifyTimestamp(const Timestamp& timestamp) {
  return Substitute("P: $0 usec, L: $1",
                    GetPhysicalValueMicros(timestamp),
                    GetLogicalValue(timestamp));
}

}  // namespace clock
}  // namespace kudu
