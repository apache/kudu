// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_UTIL_HIGH_WATER_MARK_H
#define KUDU_UTIL_HIGH_WATER_MARK_H

#include "kudu/gutil/macros.h"
#include "kudu/util/atomic.h"

namespace kudu {

// Lock-free integer that keeps track of the highest value seen.
// Similar to Impala's RuntimeProfile::HighWaterMarkCounter.
// HighWaterMark::max_value() returns the highest value seen;
// HighWaterMark::current_value() returns the current value.
class HighWaterMark {
 public:
  explicit HighWaterMark(int64_t initial_value)
    : current_value_(initial_value),
      max_value_(initial_value) {
  }

  // Return the current value.
  int64_t current_value() const {
    return current_value_.Load(kMemOrderNoBarrier);
  }

  // Return the max value.
  int64_t max_value() const {
    return max_value_.Load(kMemOrderNoBarrier);
  }

  // If current value + 'delta' is <= 'max', increment current value
  // by 'delta' and return true; return false otherwise.
  bool TryIncrementBy(int64_t delta, int64_t max) {
    while (true) {
      int64_t old_val = current_value();
      int64_t new_val = old_val + delta;
      if (new_val > max) {
        return false;
      }
      if (PREDICT_TRUE(current_value_.CompareAndSet(old_val,
                                                    new_val,
                                                    kMemOrderNoBarrier))) {
        UpdateMax(new_val);
        return true;
      }
    }
  }

  void IncrementBy(int64_t amount) {
    UpdateMax(current_value_.IncrementBy(amount, kMemOrderNoBarrier));
  }

  void set_value(int64_t v) {
    current_value_.Store(v, kMemOrderNoBarrier);
    UpdateMax(v);
  }

 private:
  void UpdateMax(int64_t value) {
    max_value_.StoreMax(value, kMemOrderNoBarrier);
  }

  AtomicInt<int64_t> current_value_;
  AtomicInt<int64_t> max_value_;
};

} // namespace kudu
#endif /* KUDU_UTIL_HIGH_WATER_MARK_H */


