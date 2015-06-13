// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_SPINLOCK_PROFILING_H
#define KUDU_UTIL_SPINLOCK_PROFILING_H

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"

namespace kudu {

class MetricEntity;

// Enable instrumentation of spinlock contention.
//
// Calling this method currently does nothing, except for ensuring
// that the spinlock_profiling.cc object file gets linked into your
// executable. It needs to be somewhere reachable in your code,
// just so that gcc doesn't omit the underlying module from the binary.
void InitSpinLockContentionProfiling();

// Return the total number of microseconds spent in spinlock contention
// since the server started.
uint64_t GetSpinLockContentionMicros();

// Register metrics in the given server entity which measure the amount of
// spinlock contention.
void RegisterSpinLockContentionMetrics(const scoped_refptr<MetricEntity>& entity);

} // namespace kudu
#endif /* KUDU_UTIL_SPINLOCK_PROFILING_H */
