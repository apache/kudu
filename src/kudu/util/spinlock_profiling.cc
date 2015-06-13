// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/spinlock_profiling.h"

#include <glog/logging.h>
#include <gflags/gflags.h>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/debug-util.h"
#include "kudu/util/metrics.h"
#include "kudu/util/striped64.h"
#include "kudu/util/trace.h"

DEFINE_int32(lock_contention_trace_threshold_cycles,
             2000000, // 2M cycles should be about 1ms
             "If acquiring a spinlock takes more than this number of "
             "cycles, and a Trace is currently active, then the current "
             "stack trace is logged to the trace buffer.");

METRIC_DEFINE_gauge_uint64(server, spinlock_contention_time,
    "Spinlock Contention Time", kudu::MetricUnit::kMicroseconds,
    "Amount of time consumed by contention on internal spinlocks since the server "
    "started. If this increases rapidly, it may indicate a performance issue in Kudu "
    "internals triggered by a particular workload and warrant investigation.");

namespace kudu {

static const double kNanosPerSecond = 1000000000.0;
static const double kMicrosPerSecond = 1000000.0;

static LongAdder* g_contended_cycles = NULL;

namespace {

void SubmitSpinLockProfileData(const void *contendedlock, int64 wait_cycles) {
  if (wait_cycles > FLAGS_lock_contention_trace_threshold_cycles) {
    Trace* t = Trace::CurrentTrace();
    if (t) {
      double nanos = static_cast<double>(wait_cycles) / base::CyclesPerSecond()
        * kNanosPerSecond;
      char backtrace_buffer[1024];
      HexStackTraceToString(backtrace_buffer, arraysize(backtrace_buffer));
      TRACE_TO(t, "Waited $0ns on lock $1. stack: $2",
               nanos, contendedlock,
               backtrace_buffer);
    }
  }

  LongAdder* la = reinterpret_cast<LongAdder*>(
      base::subtle::Acquire_Load(reinterpret_cast<AtomicWord*>(&g_contended_cycles)));
  if (la) {
    la->IncrementBy(wait_cycles);
  }
}

} // anonymous namespace

void InitSpinLockContentionProfiling() {
  if (g_contended_cycles) {
    // Already initialized.
    return;
  }
  base::subtle::Release_Store(reinterpret_cast<AtomicWord*>(&g_contended_cycles),
                              reinterpret_cast<uintptr_t>(new LongAdder()));
}

void RegisterSpinLockContentionMetrics(const scoped_refptr<MetricEntity>& entity) {
  InitSpinLockContentionProfiling();
  entity->NeverRetire(
      METRIC_spinlock_contention_time.InstantiateFunctionGauge(
          entity, Bind(&GetSpinLockContentionMicros)));

}

uint64_t GetSpinLockContentionMicros() {
  int64_t wait_cycles = DCHECK_NOTNULL(g_contended_cycles)->Value();
  double micros = static_cast<double>(wait_cycles) / base::CyclesPerSecond()
    * kMicrosPerSecond;
  return implicit_cast<int64_t>(micros);
}


} // namespace kudu

// The hook expected by gutil is in the gutil namespace. Simply forward into the
// kudu namespace so we don't need to qualify everything.
namespace gutil {
void SubmitSpinLockProfileData(const void *contendedlock, int64 wait_cycles) {
  kudu::SubmitSpinLockProfileData(contendedlock, wait_cycles);
}
} // namespace gutil
