// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <boost/thread/locks.hpp>
#include <glog/logging.h>

#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/outbound_call.h"

namespace kudu { namespace rpc {

RpcController::RpcController() {
  DVLOG(4) << "RpcController " << this << " constructed";
}

RpcController::~RpcController() {
  DVLOG(4) << "RpcController " << this << " destroyed";
}

void RpcController::Reset() {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (call_) {
    CHECK(finished());
  }
  call_.reset();
}

bool RpcController::finished() const {
  if (call_) {
    return call_->IsFinished();
  }
  return false;
}

Status RpcController::status() const {
  if (call_) {
    return call_->status();
  }
  return Status::OK();
}

const ErrorStatusPB* RpcController::error_response() const {
  if (call_) {
    return call_->error_pb();
  }
  return NULL;
}

void RpcController::set_timeout(const MonoDelta& timeout) {
  boost::lock_guard<simple_spinlock> l(lock_);
  DCHECK(!call_ || call_->state() == OutboundCall::READY);
  timeout_ = timeout;
}

void RpcController::set_deadline(const MonoTime& deadline) {
  set_timeout(deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE)));
}

const MonoDelta &RpcController::timeout() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return timeout_;
}

} // namespace rpc
} // namespace kudu
