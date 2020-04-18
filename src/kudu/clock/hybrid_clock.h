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

#include "kudu/clock/clock.h"
#include "kudu/clock/time_service.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"

namespace kudu {
namespace clock {

// The HybridTime clock.
//
// HybridTime should not be used on a distributed cluster running on OS X hosts,
// since NTP clock error is not available.
class HybridClock : public Clock {
 public:
  HybridClock();

  virtual Status Init() override;

  // Obtains the timestamp corresponding to the current time.
  virtual Timestamp Now() override;

  // Obtains the timestamp corresponding to latest possible current
  // time.
  virtual Timestamp NowLatest() override;

  // Obtain a timestamp which is guaranteed to be later than the current time
  // on any machine in the cluster.
  //
  // NOTE: this is not a very tight bound.
  virtual Status GetGlobalLatest(Timestamp* t) override;

  // Updates the clock with a timestamp originating on another machine.
  virtual Status Update(const Timestamp& to_update) override;

  virtual void RegisterMetrics(const scoped_refptr<MetricEntity>& metric_entity) override;

  // HybridClock supports all external consistency modes.
  virtual bool SupportsExternalConsistencyMode(ExternalConsistencyMode mode) override;

  virtual bool HasPhysicalComponent() const override;

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
  virtual Status WaitUntilAfter(const Timestamp& then,
                                const MonoTime& deadline) override;

  // Blocks the caller thread until the local time is after 'then'.
  // This is in contrast to the above method, which waits until the time
  // on _all_ machines is past the given time.
  //
  // Returns Status::TimedOut() if 'deadline' will pass before the specified
  // timestamp. NOTE: unlike most "wait" methods, this may return _immediately_
  // with a timeout. See WaitUntilAfter() for details.
  virtual Status WaitUntilAfterLocally(const Timestamp& then,
                                       const MonoTime& deadline) override;

  // Return true if the given time has passed (i.e any future call
  // to Now() would return a higher value than t).
  //
  // NOTE: this only refers to the _local_ clock, and is not a guarantee
  // that other nodes' clocks have definitely passed this timestamp.
  // This is in contrast to WaitUntilAfter() above.
  virtual bool IsAfter(Timestamp t) override;

  // Obtains the timestamp corresponding to the current time and the associated
  // error in micros. This may fail if the clock is unsynchronized or synchronized
  // but the error is too high and, since we can't do anything about it,
  // LOG(FATAL)'s in that case.
  void NowWithError(Timestamp* timestamp, uint64_t* max_error_usec);

  virtual std::string Stringify(Timestamp timestamp) override;

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

  clock::TimeService* time_service() {
    return time_service_.get();
  }

 private:
  friend class TestNtp;

  // Obtains the current wallclock time and maximum error in microseconds,
  // and checks if the clock is synchronized.
  //
  // On OS X, the error will always be 0.
  kudu::Status WalltimeWithError(uint64_t* now_usec, uint64_t* error_usec);

  // Same as above, but exits with a FATAL if there is an error.
  void WalltimeWithErrorOrDie(uint64_t* now_usec, uint64_t* error_usec);

  // Used to get the timestamp for metrics.
  uint64_t NowForMetrics();

  // Used to get the current error, for metrics.
  uint64_t ErrorForMetrics();

  // Used to fetch the current time and error bound from the system or NTP
  // service.
  std::unique_ptr<clock::TimeService> time_service_;

  mutable simple_spinlock lock_;

  // The next timestamp to be generated from this clock, assuming that
  // the physical clock hasn't advanced beyond the value stored here.
  uint64_t next_timestamp_;

  // The last valid clock reading we got from the time source, along
  // with the monotime that we took that reading.
  mutable simple_spinlock last_clock_read_lock_;
  MonoTime last_clock_read_time_;
  uint64_t last_clock_read_physical_;
  uint64_t last_clock_read_error_;

  // How many bits to left shift a microseconds clock read. The remainder
  // of the timestamp will be reserved for logical values.
  static const int kBitsToShift;

  // Mask to extract the pure logical bits.
  static const uint64_t kLogicalBitMask;

  enum State {
    kNotInitialized,
    kInitialized
  };

  State state_;

  // Clock metrics are set to detach to their last value. This means
  // that, during our destructor, we'll need to access other class members
  // declared above this. Hence, this member must be declared last.
  FunctionGaugeDetacher metric_detacher_;
};

}  // namespace clock
}  // namespace kudu
