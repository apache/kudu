// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_SERVER_CLOCK_H_
#define KUDU_SERVER_CLOCK_H_

#include "common/timestamp.h"
#include "gutil/ref_counted.h"

namespace kudu {
class faststring;
class MonoDelta;
class Slice;
class Status;
namespace server {

// An interface for a clock that can be used to assign timestamps to
// operations.
// Implementations must respect the following assumptions:
// 1 - Now() must return monotonically increasing numbers
//     i.e. for any two calls, i.e. Now returns timestamp1 and timestamp2, it must
//     hold that timestamp1 < timestamp2.
// 2 - Update() must never set the clock backwards (corollary of 1)
class Clock : public base::RefCountedThreadSafe<Clock> {
 public:
  // Obtains a new transaction timestamp corresponding to the current instant.
  virtual Timestamp Now() = 0;

  // Update the clock with a transaction timestamp originating from
  // another server. For instance replicas can call this so that,
  // if elected leader, they are guaranteed to generate timestamps
  // higher than the timestamp of the last transaction accepted from the
  // leader.
  virtual Status Update(const Timestamp& to_update) = 0;

  // Waits until the clock advances to 'then'.
  // Can also be used to implement 'external consistency' in the same sense as
  // Google's Spanner.
  virtual Status WaitUntilAfter(const Timestamp& then) = 0;

  // Behaves like the above method but waits until the clock advances to 'then'
  // for a maximum time of 'max'. If the clock has not reached the requested
  // timestamp by max returns Status::TimedOut.
  virtual Status TimedWaitUntilAfter(const Timestamp& then, const MonoDelta& max) = 0;

  virtual ~Clock() {}
};

} // namespace server
} // namespace kudu

#endif /* KUDU_SERVER_CLOCK_H_ */
