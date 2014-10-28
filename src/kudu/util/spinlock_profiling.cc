// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/spinlock_profiling.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/trace.h"

DEFINE_int32(lock_contention_trace_threshold_cycles,
             2000000, // 2M cycles should be about 1ms
             "If acquiring a spinlock takes more than this number of "
             "cycles, and a Trace is currently active, then the current "
             "stack trace is logged to the trace buffer.");

namespace kudu {

static const double kNanosPerSecond = 1000000000.0;

namespace {

void SubmitSpinLockProfileData(const void *contendedlock, int64 wait_cycles) {
  if (wait_cycles > FLAGS_lock_contention_trace_threshold_cycles) {
    double nanos = static_cast<double>(wait_cycles) / base::CyclesPerSecond()
      * kNanosPerSecond;
    Trace* t = Trace::CurrentTrace();
    if (t) {
      char backtrace_buffer[1024];
      HexStackTraceToString(backtrace_buffer, arraysize(backtrace_buffer));
      TRACE_TO(t, "Waited $0ns on lock $1. stack: $2",
               nanos, contendedlock,
               backtrace_buffer);
    }
  }
}
} // anonymous namespace

void InitSpinLockContentionProfiling() {
  // This function currently doesn't do anything, but it's important to call
  // it so that this object actually gets linked into the resulting executable.
  // Otherwise, the weak symbol from gutil/ gets included and no spinlock
  // profiling is done.
}
} // namespace kudu

// The hook expected by gutil is in the gutil namespace. Simply forward into the
// kudu namespace so we don't need to qualify everything.
namespace gutil {
void SubmitSpinLockProfileData(const void *contendedlock, int64 wait_cycles) {
  kudu::SubmitSpinLockProfileData(contendedlock, wait_cycles);
}
} // namespace gutil
