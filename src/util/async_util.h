// Copyright (c) 2013, Cloudera, inc.
//
// Utility functions which are handy when doing async/callback-based programming.
#ifndef KUDU_UTIL_ASYNC_UTIL_H
#define KUDU_UTIL_ASYNC_UTIL_H

#include <boost/function.hpp>

#include "gutil/macros.h"
#include "util/status.h"
#include "util/countdown_latch.h"

namespace kudu {

// A callback which takes a Status. This is typically used for functions which
// produce asynchronous results and may fail.
typedef boost::function<void(const Status& status)> StatusCallback;

// StatusCallback implementation which, upon completion, assigns the
// result status to a variable and triggers a latch. Useful to convert
// async functions which take StatusCallbacks into synchronous calls.
class AssignStatusAndTriggerLatch {
 public:
  AssignStatusAndTriggerLatch(Status* result_status, CountDownLatch* latch)
    : result_status_(result_status),
      latch_(latch) {
  }

  inline void operator()(const Status& status) {
    *result_status_ = status;
    latch_->CountDown();
  }

 private:
  Status* result_status_;
  CountDownLatch* latch_;
};

// Simple class which can be used to make async methods synchronous.
// For example:
//   Synchronizer s;
//   SomeAsyncMethod(s.callback());
//   CHECK_OK(s.Wait());
class Synchronizer {
 public:
  Synchronizer() : l(1) {}
  inline StatusCallback callback() {
    return AssignStatusAndTriggerLatch(&s, &l);
  }
  Status Wait() {
    l.Wait();
    return s;
  }
  void Reset() {
    l.Reset(1);
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(Synchronizer);
  Status s;
  CountDownLatch l;
};

} // namespace kudu
#endif /* KUDU_UTIL_ASYNC_UTIL_H */
