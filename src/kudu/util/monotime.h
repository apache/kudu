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
#ifndef KUDU_UTIL_MONOTIME_H
#define KUDU_UTIL_MONOTIME_H

#include <stdint.h>
#include <string>

#ifdef KUDU_HEADERS_NO_STUBS
#include <gtest/gtest_prod.h>
#else
// This is a poor module interdependency, but the stubs are header-only and
// it's only for exported header builds, so we'll make an exception.
#include "kudu/client/stubs.h"
#endif

#include "kudu/util/kudu_export.h"

struct timeval;
struct timespec;

namespace kudu {
class MonoTime;

/// @brief A representation of a time interval.
///
/// The MonoDelta class represents an elapsed duration of time -- i.e.
/// the delta between two MonoTime instances.
class KUDU_EXPORT MonoDelta {
 public:
  /// @name Converters from seconds representation (and ubiquitous SI prefixes).
  ///
  /// @param [in] seconds/ms/us/ns
  ///   Time interval representation in seconds (with ubiquitous SI prefixes).
  /// @return The resulting MonoDelta object initialized in accordance with
  ///   the specified parameter.
  ///
  ///@{
  static MonoDelta FromSeconds(double seconds);
  static MonoDelta FromMilliseconds(int64_t ms);
  static MonoDelta FromMicroseconds(int64_t us);
  static MonoDelta FromNanoseconds(int64_t ns);
  ///@}

  /// Build a MonoDelta object.
  ///
  /// @note A MonoDelta instance built with the this default constructor is
  ///   "uninitialized" and may not be used for any operation.
  MonoDelta();

  /// @return @c true iff this object is initialized.
  bool Initialized() const;

  /// Check whether this time interval is shorter than the specified one.
  ///
  /// @param [in] rhs
  ///   A time interval for comparison.
  /// @return @c true iff this time interval is strictly shorter
  ///   than the specified one.
  bool LessThan(const MonoDelta &rhs) const;

  /// Check whether this time interval is longer than the specified one.
  ///
  /// @param [in] rhs
  ///   A time interval for comparison.
  /// @return @c true iff this time interval is strictly longer
  ///   than the specified one.
  bool MoreThan(const MonoDelta &rhs) const;

  /// Check whether this time interval has the same duration
  ///  as the specified one.
  ///
  /// @param [in] rhs
  ///   A time interval for comparison.
  /// @return @c true iff this time interval has the same duration as the
  ///   the specified one.
  bool Equals(const MonoDelta &rhs) const;

  /// @return String representation of this interval's duration (in seconds).
  std::string ToString() const;

  /// @name Converters into seconds representation (and ubiquitous SI prefixes).
  ///
  /// @return Representation of the time interval in appropriate SI units.
  ///
  ///@{
  double ToSeconds() const;
  int64_t ToMilliseconds() const;
  int64_t ToMicroseconds() const;
  int64_t ToNanoseconds() const;
  ///@}

  /// Represent this time interval as a timeval structure, with microsecond
  /// accuracy.
  ///
  /// @param [out] tv
  ///   Placeholder for the result value.
  void ToTimeVal(struct timeval *tv) const;

  /// Represent this time interval as a timespec structure, with nanosecond
  /// accuracy.
  ///
  /// @param [out] ts
  ///   Placeholder for the result value.
  void ToTimeSpec(struct timespec *ts) const;

  /// Convert a nanosecond value to a timespec.
  ///
  /// @param [in] nanos
  ///   Representation of a relative point in time in nanoseconds.
  /// @param [out] ts
  ///   Placeholder for the resulting timespec representation.
  static void NanosToTimeSpec(int64_t nanos, struct timespec* ts);

 private:
  static const int64_t kUninitialized;

  friend class MonoTime;
  FRIEND_TEST(TestMonoTime, TestDeltaConversions);
  explicit MonoDelta(int64_t delta);
  int64_t nano_delta_;
};

/// @brief Representation of a particular point in time.
///
/// The MonoTime class represents a particular point in time,
/// relative to some fixed but unspecified reference point.
///
/// This time is monotonic, meaning that if the user changes his or her system
/// clock, the monotime does not change.
class KUDU_EXPORT MonoTime {
 public:
  /// @brief The granularity of the time specification
  ///
  /// The coarse monotonic time is faster to retrieve, but "only"
  /// accurate to within a millisecond or two. The speed difference will
  /// depend on your timer hardware.
  enum Granularity {
    COARSE,
    FINE
  };

  /// @name Conversion constants for ubiquitous time units.
  ///
  ///@{
  static const int64_t kNanosecondsPerSecond = 1000000000L;
  static const int64_t kNanosecondsPerMillisecond = 1000000L;
  static const int64_t kNanosecondsPerMicrosecond = 1000L;

  static const int64_t kMicrosecondsPerSecond = 1000000L;
  ///@}

  /// Get current time in MonoTime representation.
  ///
  /// @param [in] granularity
  ///   Granularity for the resulting time specification.
  /// @return Time specification for the moment of the method's invocation.
  static MonoTime Now(enum Granularity granularity);

  /// @return MonoTime equal to farthest possible time into the future.
  static MonoTime Max();

  /// @return MonoTime equal to farthest possible time into the past.
  static MonoTime Min();

  /// Select the earliest between the specified time points.
  ///
  /// @param [in] a
  ///   The first MonoTime object to select from.
  /// @param [in] b
  ///   The second MonoTime object to select from.
  /// @return The earliest (minimum) of the two monotimes.
  static const MonoTime& Earliest(const MonoTime& a, const MonoTime& b);

  /// Build a MonoTime object. The resulting object is not initialized
  /// and not ready to use.
  MonoTime();

  /// @return @c true iff the object is initialized.
  bool Initialized() const;

  /// Compute time interval between the point in time specified by this
  /// and the specified object.
  ///
  /// @param [in] rhs
  ///   The object that corresponds to the left boundary of the time interval,
  ///   where this object corresponds to the right boundary of the interval.
  /// @return The resulting time interval represented as a MonoDelta object.
  MonoDelta GetDeltaSince(const MonoTime &rhs) const;

  /// Advance this object's time specification by the specified interval.
  ///
  /// @param [in] delta
  ///   The time interval to add.
  void AddDelta(const MonoDelta &delta);

  /// Check whether the point in time specified by this object is earlier
  /// than the specified one.
  ///
  /// @param [in] rhs
  ///   The other MonoTime object to compare with.
  /// @return @c true iff the point in time represented by this MonoTime object
  ///   is earlier then the point in time represented by the parameter.
  bool ComesBefore(const MonoTime &rhs) const;

  /// @return String representation of the object (in seconds).
  std::string ToString() const;

  /// Check whether this object represents the same point in time as the other.
  ///
  /// @param [in] other
  ///   The other MonoTime object to compare.
  /// @return @c true iff the point in time represented by this MonoTime object
  ///   is the same as the one represented by the other.
  bool Equals(const MonoTime& other) const;

 private:
  friend class MonoDelta;
  FRIEND_TEST(TestMonoTime, TestTimeSpec);
  FRIEND_TEST(TestMonoTime, TestDeltaConversions);

  explicit MonoTime(const struct timespec &ts);
  explicit MonoTime(int64_t nanos);
  double ToSeconds() const;
  int64_t nanos_;
};

/// Sleep for an interval specified by a MonoDelta instance.
///
/// This is preferred over sleep(3), usleep(3), and nanosleep(3).
/// It's less prone to mixups with units since it uses the MonoDelta for
/// interval specification.
/// Besides, it ignores signals/EINTR, so will reliably sleep at least for the
/// MonoDelta duration.
///
/// @param [in] delta
///   The time interval to sleep for.
void KUDU_EXPORT SleepFor(const MonoDelta& delta);

} // namespace kudu

#endif
