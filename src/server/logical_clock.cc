// Copyright (c) 2013, Cloudera, inc.

#include "server/logical_clock.h"

#include "gutil/atomicops.h"
#include "util/status.h"

namespace kudu {
namespace server {

using base::subtle::Atomic64;
using base::subtle::Barrier_AtomicIncrement;
using base::subtle::NoBarrier_CompareAndSwap;

Timestamp LogicalClock::Now() {
  return Timestamp(Barrier_AtomicIncrement(&now_, 1));
}

Status LogicalClock::Update(const Timestamp& to_update) {
  Atomic64 new_value = to_update.value();
  // if the incoming value is less than the current one there's nothing to do
  if (new_value <= now_) return Status::OK();
  while (true) {
    Atomic64 current_value = now_;
    // if we failed the CAS before maybe (though unlikely) the local time
    // got updated past the incoming time and we can just return.
    if (PREDICT_FALSE(new_value <= current_value)) return Status::OK();
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

Status LogicalClock::TimedWaitUntilAfter(const Timestamp& then, const MonoDelta& max) {
  return Status::ServiceUnavailable(
      "Logical clock does not support WaitUntilAfter()");
}

}  // namespace server
}  // namespace kudu

