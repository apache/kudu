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
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kudu/clock/clock.h"
#include "kudu/clock/time_service.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {

class HostPort;

namespace clock {

// The HybridTime clock.
//
// HybridTime should not be used on a distributed cluster running on OS X hosts,
// since NTP clock error is not available.
class HybridClock : public Clock {
 public:
  // Create an instance, registering HybridClock's metrics with the specified
  // metric entity.
  explicit HybridClock(const scoped_refptr<MetricEntity>& metric_entity);

  // Should be called only once.
  Status Init() override;

  // Obtains the timestamp corresponding to the current time.
  Timestamp Now() override;

  // Obtains the timestamp corresponding to latest possible current
  // time.
  Timestamp NowLatest() override;

  // Obtain a timestamp which is guaranteed to be later than the current time
  // on any machine in the cluster.
  //
  // NOTE: this is not a very tight bound.
  Status GetGlobalLatest(Timestamp* t) override;

  // Updates the clock with a timestamp originating on another machine.
  Status Update(const Timestamp& to_update) override;

  // HybridClock supports all external consistency modes.
  bool SupportsExternalConsistencyMode(
      ExternalConsistencyMode /* mode */) const override {
    return true;
  }

  bool HasPhysicalComponent() const override {
    return true;
  }

  MonoDelta GetPhysicalComponentDifference(Timestamp lhs, Timestamp rhs) const override;

  // Blocks the caller thread until the true time is after 'then'.
  // In other words, waits until the HybridClock::Now() on _all_ nodes
  // will return a value greater than 'then'.
  //
  // The incoming time 'then' is assumed to be the latest time possible
  // at the time the read was performed, i.e. 'then' = now + max_error.
  //
  // This method can be used to make Kudu behave like Spanner/TrueTime.
  // This is implemented by possibly making the caller thread wait for a
  // a certain period of time.
  //
  // As an example, the following cases might happen:
  //
  // 1 - 'then' is lower than now.earliest() -> Definitely in
  // the past, no wait necessary.
  //
  // 2 - 'then' is greater than > now.earliest(): need to wait until
  // 'then' <= now.earliest()
  //
  // Returns OK if it waited long enough or if no wait was necessary.
  //
  // Returns Status::ServiceUnavailable if the system clock was not
  // synchronized and therefore it couldn't wait out the error.
  //
  // Returns Status::TimedOut() if 'deadline' will pass before the specified
  // timestamp. NOTE: unlike most "wait" methods, this may return _immediately_
  // with a timeout, rather than actually waiting for the timeout to expire.
  // This is because, by looking at the current clock, we can know how long
  // we'll have to wait, in contrast to most Wait() methods which are waiting
  // on some external condition to become true.
  Status WaitUntilAfter(const Timestamp& then,
                        const MonoTime& deadline) override;

  // Blocks the caller thread until the local time is after 'then'.
  // This is in contrast to the above method, which waits until the time
  // on _all_ machines is past the given time.
  //
  // Returns Status::TimedOut() if 'deadline' will pass before the specified
  // timestamp. NOTE: unlike most "wait" methods, this may return _immediately_
  // with a timeout. See WaitUntilAfter() for details.
  Status WaitUntilAfterLocally(const Timestamp& then,
                               const MonoTime& deadline) override;

  // Return true if the given time has passed (i.e any future call
  // to Now() would return a higher value than t).
  //
  // NOTE: this only refers to the _local_ clock, and is not a guarantee
  // that other nodes' clocks have definitely passed this timestamp.
  // This is in contrast to WaitUntilAfter() above.
  bool IsAfter(Timestamp t) override;

  std::string Stringify(Timestamp timestamp) override;

  // Obtains the timestamp corresponding to the current time and the associated
  // error in micros. This may fail if the clock is unsynchronized or synchronized
  // but the error is too high and, since we can't do anything about it,
  // LOG(FATAL)'s in that case.
  void NowWithError(Timestamp* timestamp, uint64_t* max_error_usec);

  // Static encoding/decoding methods for timestamps. Public mostly
  // for testing/debugging purposes.

  // Returns the logical value embedded in 'timestamp'
  static uint64_t GetLogicalValue(const Timestamp& timestamp);

  // Returns the physical value embedded in 'timestamp', in microseconds.
  static uint64_t GetPhysicalValueMicros(const Timestamp& timestamp);

  // Obtains a new Timestamp with the logical value zeroed out.
  static Timestamp TimestampFromMicroseconds(uint64_t micros);

  // Obtains a new Timestamp that embeds both the physical and logical values.
  static Timestamp TimestampFromMicrosecondsAndLogicalValue(uint64_t micros,
                                                            uint64_t logical_value);

  // Creates a new timestamp whose physical time is GetPhysicalValue(original) +
  // 'to_add' and which retains the same logical value.
  static Timestamp AddPhysicalTimeToTimestamp(const Timestamp& original,
                                              const MonoDelta& to_add);

  // Outputs a string containing the physical and logical values of the timestamp,
  // separated.
  static std::string StringifyTimestamp(const Timestamp& timestamp);

  clock::TimeService* time_service() const {
    return time_service_.get();
  }

 private:
  enum class TimeSource {
    // Internal Kudu clock synchronized by built-in NTP client.
    NTP_SYNC_BUILTIN,

    // Local machine clock synchronized by NTP.
    NTP_SYNC_SYSTEM,

    // Local machine clock with no requirement of NTP synchronization.
    UNSYNC_SYSTEM,

    // Mock clock (used for tests only).
    MOCK,

    // Unknown/invalid time source.
    UNKNOWN,
  };

  // How many bits to left shift a microseconds clock read. The remainder
  // of the timestamp will be reserved for logical values. Left shifting 12 bits
  // gives us 12 bits for the logical value and should still keep accurate
  // microseconds time until 2100+
  static constexpr const int kBitsToShift = 12;

  // Mask to extract the pure logical bits.
  static constexpr const uint64_t kLogicalBitMask = (1 << kBitsToShift) - 1;

  // Convert time source to string representation.
  static const char* TimeSourceToString(HybridClock::TimeSource time_source);

  // Select particular time source for the hybrid clock given the
  // 'time_source_str' parameter which can be a pseudo-source such as 'auto'.
  // On success, the 'time_source' output parameter contains time source that
  // determines particular time service to use, and the 'builtin_ntp_servers'
  // contains NTP servers for the built-in NTP client if the 'builtin' time
  // source is selected.
  static Status SelectTimeSource(const std::string& time_source_str,
                                 TimeSource* time_source,
                                 std::vector<HostPort>* builtin_ntp_servers);

  // Initialize hybrid clock with the specified time source.
  // The 'builtin_ntp_servers' is used in case of TimeSource::BUILTIN_NTP_SYNC.
  Status InitWithTimeSource(TimeSource time_source,
                            std::vector<HostPort> builtin_ntp_servers);

  // Variant of NowWithError() that requires 'lock_' to be held already.
  void NowWithErrorUnlocked(Timestamp* timestamp, uint64_t* max_error_usec);

  // Obtains the current wallclock time and maximum error in microseconds,
  // and checks if the clock is synchronized.
  //
  // On OS X, the error will always be 0.
  Status WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec);

  // Same as above, but exits with a FATAL if there is an error.
  void WalltimeWithErrorOrDie(uint64_t* now_usec, uint64_t* error_usec);

  // Used to get the timestamp for metrics.
  uint64_t NowForMetrics();

  // Used to get the current error, for metrics.
  uint64_t ErrorForMetrics();

  // Used to fetch the current time and error bound from the system or NTP
  // service.
  std::unique_ptr<clock::TimeService> time_service_;

  // Guards access to 'state_' and 'next_timestamp_'.
  simple_spinlock lock_;

  // The next timestamp to be generated from this clock, assuming that
  // the physical clock hasn't advanced beyond the value stored here.
  // Protected by 'lock_'.
  uint64_t next_timestamp_;

  // The last valid clock reading we got from the time source, along
  // with the monotime that we took that reading. The 'is_extrapolating' field
  // tracks whether extrapolated or real readings of the underlying clock are
  // used to generate hybrid timestamps.
  simple_spinlock last_clock_read_lock_;  // protects four fields below
  MonoTime last_clock_read_time_;
  uint64_t last_clock_read_physical_;
  uint64_t last_clock_read_error_;
  bool is_extrapolating_ = false;

  // The state of the object. Guarded by 'lock_'.
  enum State {
    kNotInitialized,
    kInitialized
  };
  State state_;

  // Metric entity.
  scoped_refptr<MetricEntity> metric_entity_;

  // Whether the hybrid clock is extrapolating the readings of the underlying
  // clock instead of using the real ones. It's important to know whether
  // the extrapolation is happening, but 'extrapolation_intervals_histogram_'
  // metric doesn't allow for exposing this fact prior to the end of current
  // extrapolation interval.
  scoped_refptr<AtomicGauge<bool>> extrapolating_;

  // Stats on the underlying clock's 'maximum error' metric sampled every
  // NowWithError() call (essentially, every call when requesting a hybrid clock
  // timestamp).
  scoped_refptr<Histogram> max_errors_histogram_;

  // Stats on time intervals when the underlying clock was extrapolated
  // instead of using the actual readings. Extrapolation happens when an attempt
  // to read the clock yields an error (clock might be unsynchronized, etc.).
  scoped_refptr<Histogram> extrapolation_intervals_histogram_;

  // Clock metrics are set to detach to their last value. This means
  // that, during our destructor, we'll need to access other class members
  // declared above this. Hence, this member must be declared last.
  FunctionGaugeDetacher metric_detacher_;
};

}  // namespace clock
}  // namespace kudu
