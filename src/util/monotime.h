// Copyright (c) 2013, Cloudera, inc
#ifndef KUDU_UTIL_MONOTIME_H
#define KUDU_UTIL_MONOTIME_H

#include <stdint.h>
#include <string>

namespace kudu {
class MonoTime;

// Represent an elapsed duration of time -- i.e the delta between
// two MonoTime instances.
class MonoDelta {
 public:
  static MonoDelta FromSeconds(double seconds);
  static MonoDelta FromMilliseconds(int ms);
  static MonoDelta FromNanoseconds(int ns);
  explicit MonoDelta();
  bool LessThan(const MonoDelta &rhs) const;
  bool MoreThan(const MonoDelta &rhs) const;
  std::string ToString() const;
  double ToSeconds() const;

 private:
  friend class MonoTime;
  explicit MonoDelta(int64_t delta);
  int64_t nano_delta_;
};

// Represent a particular point in time, relative to some fixed but unspecified
// reference point.
//
// This time is monotonic, meaning that if the user changes his or her system
// clock, the monotime does not change.
class MonoTime {
 public:
  enum Granularity {
    COARSE,
    FINE
  };

  // The coarse monotonic time is faster to retrieve, but "only"
  // accurate to within a millisecond or two.  The speed difference will
  // depend on your timer hardware.
  static MonoTime Now(enum Granularity granularity);

  explicit MonoTime();
  bool Initialized() const;
  MonoDelta GetDeltaSince(const MonoTime &rhs) const;
  void AddDelta(const MonoDelta &delta);
  bool ComesBefore(const MonoTime &rhs) const;
  std::string ToString() const;
 private:
  explicit MonoTime(const struct timespec &ts);
  double ToSeconds() const;
  uint64_t nanos_;
};
}

#endif
