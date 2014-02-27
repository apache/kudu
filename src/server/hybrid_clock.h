// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_SERVER_HYBRID_CLOCK_H_
#define KUDU_SERVER_HYBRID_CLOCK_H_

#include <gtest/gtest.h>

#include "server/clock.h"
#include "gutil/ref_counted.h"
#include "util/locks.h"
#include "util/status.h"

struct ntptimeval;

namespace kudu {
namespace server {

// The HybridTime clock.
class HybridClock : public Clock {
 public:
  HybridClock();

  virtual Status Init() OVERRIDE;

  // Obtains the timestamp corresponding to the current time.
  virtual Timestamp Now() OVERRIDE;

  // Obtains the timestamp corresponding to latest possible current
  // time.
  virtual Timestamp NowLatest() OVERRIDE;

  // Updates the clock with a timestamp originating on another machine.
  virtual Status Update(const Timestamp& to_update) OVERRIDE;

  // Blocks the caller thread until the current time is after 'then'.
  //
  // The incoming time 'then' is assumed to be the latest time possible
  // at the time the read was performed, i.e. 'then' = now + max_error.
  //
  // This method can be used to make Kudu behave like Spanner/TrueTime.
  // This is implemented by possibly making the caller thread wait for a
  // a certain period of time.
  //
  // As an example, the following cases might happen:
  //
  // 1 - 'then' is lower than now.earliest() -> Definitely in
  // the past, no wait necessary.
  //
  // 2 - 'then' is greater than > now.earliest(): need to wait until
  // 'then' - now.earliest()
  //
  // Note that if 'then' > now.latest() we trim it to now.latest()
  // as we know the event cannot have happened in the future.
  //
  // Returns OK if it waited long enough or if no wait was necessary.
  // Returns Status::ServiceUnavailable if the system clock was not
  // synchronized and therefore it couldn't wait out the error.
  virtual Status WaitUntilAfter(const Timestamp& then) OVERRIDE;

  // Static encoding/decoding methods for timestamps. Public mostly
  // for testing/debugging purposes.

  // Returns the logical value embedded in 'timestamp'
  static uint64_t GetLogicalValue(const Timestamp& timestamp);

  // Returns the physical value embedded in 'timestamp'
  static uint64_t GetPhysicalValue(const Timestamp& timestamp);

  // Obtains a new Timestamp with the logical value zeroed out.
  static Timestamp TimestampFromMicroseconds(uint64_t micros);

  // Obtains a new Timestamp that embeds both the physical and logical values.
  static Timestamp TimestampFromMicrosecondsAndLogicalValue(uint64_t micros,
                                                            uint64_t logical_value);

 private:
  FRIEND_TEST(HybridClockTest, TestWaitUntilAfter_TestCase1);
  FRIEND_TEST(HybridClockTest, TestWaitUntilAfter_TestCase2);
  FRIEND_TEST(HybridClockTest, TestWaitUntilAfter_TestIncomingEventIsInTheFuture);

  // Obtains the timestamp corresponding to the current time and the associated
  // error in micros. This may fail if the clock is unsynchronized or synchronized
  // but the error is too high and, since we can't do anything about it,
  // LOG(FATAL)'s in that case.
  void NowWithError(Timestamp* timestamp, uint64_t* max_error_usec);

  uint64_t GetTimeUsecs(ntptimeval* timeval);

  uint64_t divisor_;

  double tolerance_adjustment_;

  mutable simple_spinlock lock_;

  // the last clock read/update, in microseconds.
  uint64_t last_usec_;
  // the next logical value to be assigned to a timestamp
  uint64_t next_logical_;

  // How many bits to left shift a microseconds clock read. The remainder
  // of the timestamp will be reserved for logical values.
  static const int kBitsToShift;

  // Mask to extract the pure logical bits.
  static const uint64_t kLogicalBitMask;

  static const uint64_t kNanosPerSec;

  // The scaling factor used to obtain ppms. From the adjtimex source:
  // "scale factor used by adjtimex freq param.  1 ppm = 65536"
  static const double kAdjtimexScalingFactor;

  enum State {
    kNotInitialized,
    kInitialized
  };

  State state_;
};

}  // namespace server
}  // namespace kudu

#endif /* KUDU_SERVER_HYBRID_CLOCK_H_ */
