// Copyright (c) 2013, Cloudera, inc.

#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include <glog/logging.h>

#include "gutil/stringprintf.h"
#include "util/monotime.h"

namespace kudu {

static const long kNanosecondsPerSecond = 1000000000L;
static const long kNanosecondsPerMillisecond = 1000000L;

#define MAX_MONOTONIC_SECONDS \
  (((1ULL<<63) - 1ULL) /(int64_t)kNanosecondsPerSecond)

MonoDelta MonoDelta::FromSeconds(double seconds) {
  int64_t delta = seconds * kNanosecondsPerSecond;
  return MonoDelta(delta);
}

MonoDelta MonoDelta::FromMilliseconds(int ms) {
  int64_t delta(ms);
  delta *= kNanosecondsPerMillisecond;
  return MonoDelta(delta);
}

MonoDelta MonoDelta::FromNanoseconds(int ns) {
  return MonoDelta(ns);
}

MonoDelta::MonoDelta()
  : nano_delta_(0)
{
}

bool MonoDelta::LessThan(const MonoDelta &rhs) const {
  return nano_delta_ < rhs.nano_delta_;
}

bool MonoDelta::MoreThan(const MonoDelta &rhs) const {
  return nano_delta_ > rhs.nano_delta_;
}

std::string MonoDelta::ToString() const {
  return StringPrintf("%.3fs", ToSeconds());
}

MonoDelta::MonoDelta(int64_t delta)
  : nano_delta_(delta)
{
}

double MonoDelta::ToSeconds() const {
  double d(nano_delta_);
  d /= kNanosecondsPerSecond;
  return d;
}

MonoTime MonoTime::Now(enum Granularity granularity) {
  struct timespec ts;
  CHECK_EQ(0, clock_gettime((granularity == COARSE) ?
                    CLOCK_MONOTONIC_COARSE : CLOCK_MONOTONIC, &ts));
  return MonoTime(ts);
}

MonoTime::MonoTime()
  : nanos_(0)
{
}

bool MonoTime::Initialized() const {
  return nanos_ != 0;
}

MonoDelta MonoTime::GetDeltaSince(const MonoTime &rhs) const {
  DCHECK(Initialized());
  DCHECK(rhs.Initialized());
  int64_t delta(nanos_);
  delta -= rhs.nanos_;
  return MonoDelta(delta);
}

void MonoTime::AddDelta(const MonoDelta &delta) {
  DCHECK(Initialized());
  nanos_ += delta.nano_delta_;
}

bool MonoTime::ComesBefore(const MonoTime &rhs) const {
  DCHECK(Initialized());
  DCHECK(rhs.Initialized());
  return nanos_ < rhs.nanos_;
}

std::string MonoTime::ToString() const {
  return StringPrintf("%.3fs", ToSeconds());
}

MonoTime::MonoTime(const struct timespec &ts) {
  // Monotonic time resets when the machine reboots.  The 64-bit limitation
  // means that we can't represent times larger than 292 years, which should be
  // adequate.
  CHECK_LT(ts.tv_sec, MAX_MONOTONIC_SECONDS);
  nanos_ = ts.tv_sec;
  nanos_ *= kNanosecondsPerSecond;
  nanos_ += ts.tv_nsec;
}

double MonoTime::ToSeconds() const {
  double d(nanos_);
  d /= kNanosecondsPerSecond;
  return d;
}

} // namespace kudu
