// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_SPINLOCK_PROFILING_H
#define KUDU_UTIL_SPINLOCK_PROFILING_H

#include "kudu/gutil/macros.h"

namespace kudu {

// Enable instrumentation of spinlock contention.
//
// Calling this method currently does nothing, except for ensuring
// that the spinlock_profiling.cc object file gets linked into your
// executable. It needs to be somewhere reachable in your code,
// just so that gcc doesn't omit the underlying module from the binary.
void InitSpinLockContentionProfiling();

} // namespace kudu
#endif /* KUDU_UTIL_SPINLOCK_PROFILING_H */
