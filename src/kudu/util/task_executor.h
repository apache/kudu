// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
#ifndef KUDU_UTIL_TASK_EXECUTOR_H
#define KUDU_UTIL_TASK_EXECUTOR_H

#include <boost/bind.hpp>
#include <boost/function.hpp>

#include "kudu/gutil/macros.h"
#include "kudu/util/async_util.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"

namespace kudu {

// A Callback for a Task. Added to a Future through AddListener() the
// FutureCallback gets called when the Task the future is attached to
// finished execution.
//
// Future Callbacks are executed in the same thread has the task, therefore
// potentially consuming Threadpool resources. It is the users responsibility
// deferring execution to another Threadpool for long-running Callbacks.
//
// DEPRECATED: please use kudu::Callback instead!
class FutureCallback {
 public:
  // Called when the FutureTask completes successfully.
  virtual void OnSuccess() = 0;

  // Called when the FutureTask fails or is aborted.
  // The status describes the reason for failure.
  virtual void OnFailure(const Status& status) = 0;

  // Adapter which lets FutureCallbacks be used in the context of a StatusCallback.
  // This simply translates from one API to another.
  //
  // The FutureCallback must remain in scope for as long as any generated
  // StatusCallbacks.
  StatusCallback AsStatusCallback() {
    return Bind(&FutureCallback::StatusCB, Unretained(this));
  }

  virtual ~FutureCallback() {
  }

 private:
  void StatusCB(const Status& s) {
    if (PREDICT_TRUE(s.ok())) {
      OnSuccess();
    } else {
      OnFailure(s);
    }
  }
};

// FutureCallback implementation that can be waited on.
// Helper to make async methods with callback args, sync.
class LatchCallback : public FutureCallback {
 public:
  LatchCallback() : latch_(1) {}

  virtual void OnSuccess() OVERRIDE {
    latch_.CountDown();
  }

  virtual void OnFailure(const Status& status) OVERRIDE {
    status_ = status;
    latch_.CountDown();
  }

  Status Wait() {
    latch_.Wait();
    return status_;
  }

  Status WaitFor(const MonoDelta& delta) {
    bool done = latch_.WaitFor(delta);
    if (!done) {
      return Status::TimedOut("Timeout waiting on LatchCallback.");
    }
    return status_;
  }

  Status status() const {
    return status_;
  }

 private:
  Status status_;
  CountDownLatch latch_;

  DISALLOW_COPY_AND_ASSIGN(LatchCallback);
};

// A callback that does nothing. To be used when methods require a callback but
// we have nothing to do there, e.g. tests/mocks/reduced func. impl.
class NullCallback : public FutureCallback {
 public:
  NullCallback() {}

  virtual void OnSuccess() OVERRIDE { delete this; }

  virtual void OnFailure(const Status&) OVERRIDE { delete this; }
};

class BoundFunctionCallback : public FutureCallback {
 public:
  explicit BoundFunctionCallback(const boost::function<void()>& on_success)
      : on_success_(on_success),
        on_failure_(boost::bind(&BoundFunctionCallback::DefaultOnFailure, _1)) {
  }

  BoundFunctionCallback(const boost::function<void()>& on_success,
                        const boost::function<void(const Status&)>& on_failure)
      : on_success_(on_success),
        on_failure_(on_failure) {
  }

  void OnSuccess() OVERRIDE {
    on_success_();
  }
  void OnFailure(const Status& status) OVERRIDE {
    on_failure_(status);
  }

 private:
  static void DefaultOnFailure(const Status& status) {
    DLOG(WARNING) << "Task failed silently with status: " << status.ToString();
  }

  boost::function<void()> on_success_;
  boost::function<void(const Status&)> on_failure_;

  DISALLOW_COPY_AND_ASSIGN(BoundFunctionCallback);
};


} // namespace kudu
#endif
