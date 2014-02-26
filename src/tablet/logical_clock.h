// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_TABLET_LOGICAL_CLOCK_H_
#define KUDU_TABLET_LOGICAL_CLOCK_H_

#include "tablet/clock.h"
#include "tablet/mvcc.h"

namespace kudu {
class MonoDelta;
namespace tablet {

// An implementation of Clock that behaves as a plain Lamport Clock.
// In a single node, single tablet, setting this generates exactly the
// same Timestamp sequence as the original MvccManager did, but it can be
// updated to make sure replicas generate new timestamps on becoming leader.
//
// This can be used as a deterministic timestamp generator that has the same
// consistency properties as a HybridTime clock.
//
// The Wait* methods are unavailable in this implementation and will
// return Status::ServiceUnavailable().
//
// NOTE: this class is thread safe.
class LogicalClock : public Clock {
 public:
  explicit LogicalClock(Timestamp::val_type initial_time) : now_(initial_time) {}
  Timestamp Now();
  Status Update(const Timestamp& to_update);

  // Below methods are unavailable for this clock.
  Status WaitUntilAfter(const Timestamp& then);
  Status TimedWaitUntilAfter(const Timestamp& then, const MonoDelta& max);
 private:
  base::subtle::Atomic64 now_;
};

}  // namespace tablet
}  // namespace kudu

#endif /* KUDU_TABLET_LOGICAL_CLOCK_H_ */

