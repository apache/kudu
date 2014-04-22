// Copyright (c) 2013, Cloudera, inc.

#include <boost/bind.hpp>

#include "server/logical_clock.h"

#include "gutil/atomicops.h"
#include "util/metrics.h"
#include "util/status.h"

namespace kudu {
namespace server {

METRIC_DEFINE_gauge_uint64(clock_timestamp, kudu::MetricUnit::kCount,
                           "Logical clock timestamp.");

using base::subtle::Atomic64;
using base::subtle::Barrier_AtomicIncrement;
using base::subtle::NoBarrier_CompareAndSwap;

Timestamp LogicalClock::Now() {
  return Timestamp(Barrier_AtomicIncrement(&now_, 1));
}

Timestamp LogicalClock::NowLatest() {
  return Now();
}

Status LogicalClock::Update(const Timestamp& to_update) {
  DCHECK_NE(to_update.value(), Timestamp::kInvalidTimestamp.value())
      << "Updating the clock with an invalid timestamp";
  Atomic64 new_value = to_update.value();

  while (true) {
    Atomic64 current_value = NoBarrier_Load(&now_);
    // if the incoming value is less than the current one, or we've failed the
    // CAS because the current clock increased to higher than the incoming value,
    // we can stop the loop now.
    if (new_value <= current_value) return Status::OK();
    // otherwise try a CAS
    if (PREDICT_TRUE(NoBarrier_CompareAndSwap(&now_, current_value, new_value)
        == current_value))
      break;
  }
  return Status::OK();
}
Status LogicalClock::WaitUntilAfter(const Timestamp& then) {
  return Status::ServiceUnavailable(
      "Logical clock does not support WaitUntilAfter()");
}

LogicalClock* LogicalClock::CreateStartingAt(const Timestamp& timestamp) {
  // initialize at 'timestamp' - 1 so that the  first output value is 'timestamp'.
  return new LogicalClock(timestamp.value() - 1);
}

uint64_t LogicalClock::NowForMetrics() {
  return Now().ToUint64();
}


void LogicalClock::RegisterMetrics(MetricRegistry* registry) {
  MetricContext ctx(registry, "clock");
  METRIC_clock_timestamp.InstantiateFunctionGauge(
      ctx,
      boost::bind(&LogicalClock::NowForMetrics, this));
}

}  // namespace server
}  // namespace kudu

