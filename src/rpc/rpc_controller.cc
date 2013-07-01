// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/thread/mutex.hpp>
#include <glog/logging.h>

#include "rpc/rpc_controller.h"

namespace kudu { namespace rpc {

RpcController::RpcController()
  : state_(READY) {
}

bool RpcController::finished() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  switch (state_) {
    case READY:
    case ON_OUTBOUND_QUEUE:
    case SENT:
      return false;
    case TIMED_OUT:
    case FINISHED_ERROR:
    case FINISHED_SUCCESS:
      return true;
    default:
      LOG(FATAL) << "Unknown call state: " << state_;
      return false;
  }
}

Status RpcController::status() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return call_status_;
}

void RpcController::set_timeout(const MonoDelta &timeout) {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(state_, READY);
  timeout_ = timeout;
}

void RpcController::SetFailed(const Status &status) {
  boost::lock_guard<simple_spinlock> l(lock_);
  call_status_ = status;
  set_state_unlocked(FINISHED_ERROR);
}

void RpcController::SetTimedOut() {
  boost::lock_guard<simple_spinlock> l(lock_);
  // TODO: have a better error message which includes the call timeout,
  // remote host, how long it spent in the queue, other useful stuff.
  call_status_ = Status::RuntimeError("Call timed out");
  set_state_unlocked(TIMED_OUT);
}

void RpcController::set_state(State new_state) {
  boost::lock_guard<simple_spinlock> l(lock_);
  set_state_unlocked(new_state);
}

void RpcController::set_state_unlocked(State new_state) {
  // Sanity check state transitions.
  switch (new_state) {
    case ON_OUTBOUND_QUEUE:
      DCHECK_EQ(state_, READY);
      break;
    case SENT:
      DCHECK_EQ(state_, ON_OUTBOUND_QUEUE);
      break;
    case TIMED_OUT:
    case FINISHED_SUCCESS:
      DCHECK_EQ(state_, SENT);
      break;
    default:
      // No sanity checks for others.
      break;
  }

  state_ = new_state;
}

} // namespace rpc
} // namespace kudu
