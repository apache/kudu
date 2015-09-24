// Copyright 2013 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
  lock_guard<simple_spinlock> l(&lock_);
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

Status RpcController::GetSidecar(int idx, Slice* sidecar) const {
  return call_->call_response_->GetSidecar(idx, sidecar);
}

void RpcController::set_timeout(const MonoDelta& timeout) {
  lock_guard<simple_spinlock> l(&lock_);
  DCHECK(!call_ || call_->state() == OutboundCall::READY);
  timeout_ = timeout;
}

void RpcController::set_deadline(const MonoTime& deadline) {
  set_timeout(deadline.GetDeltaSince(MonoTime::Now(MonoTime::FINE)));
}

MonoDelta RpcController::timeout() const {
  lock_guard<simple_spinlock> l(&lock_);
  return timeout_;
}

} // namespace rpc
} // namespace kudu
