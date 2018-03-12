// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/rpc/rpc_controller.h"

#include <memory>
#include <mutex>
#include <ostream>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/slice.h"


using std::unique_ptr;
using strings::Substitute;
namespace kudu {

namespace rpc {

RpcController::RpcController()
    : credentials_policy_(CredentialsPolicy::ANY_CREDENTIALS), messenger_(nullptr) {
  DVLOG(4) << "RpcController " << this << " constructed";
}

RpcController::~RpcController() {
  DVLOG(4) << "RpcController " << this << " destroyed";
}

void RpcController::Swap(RpcController* other) {
  // Cannot swap RPC controllers while they are in-flight.
  if (call_) {
    CHECK(finished());
  }
  if (other->call_) {
    CHECK(other->finished());
  }

  std::swap(outbound_sidecars_, other->outbound_sidecars_);
  std::swap(outbound_sidecars_total_bytes_, other->outbound_sidecars_total_bytes_);
  std::swap(timeout_, other->timeout_);
  std::swap(credentials_policy_, other->credentials_policy_);
  std::swap(call_, other->call_);
}

void RpcController::Reset() {
  std::lock_guard<simple_spinlock> l(lock_);
  if (call_) {
    CHECK(finished());
  }
  call_.reset();
  required_server_features_.clear();
  credentials_policy_ = CredentialsPolicy::ANY_CREDENTIALS;
  messenger_ = nullptr;
  outbound_sidecars_total_bytes_ = 0;
}

bool RpcController::finished() const {
  if (call_) {
    return call_->IsFinished();
  }
  return false;
}

bool RpcController::negotiation_failed() const {
  if (call_) {
    DCHECK(finished());
    return call_->IsNegotiationError();
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
  return nullptr;
}

Status RpcController::GetInboundSidecar(int idx, Slice* sidecar) const {
  return call_->call_response_->GetSidecar(idx, sidecar);
}

void RpcController::set_timeout(const MonoDelta& timeout) {
  std::lock_guard<simple_spinlock> l(lock_);
  DCHECK(!call_ || call_->state() == OutboundCall::READY);
  timeout_ = timeout;
}

void RpcController::set_deadline(const MonoTime& deadline) {
  set_timeout(deadline - MonoTime::Now());
}

void RpcController::SetRequestIdPB(std::unique_ptr<RequestIdPB> request_id) {
  request_id_ = std::move(request_id);
}

bool RpcController::has_request_id() const {
  return request_id_ != nullptr;
}

const RequestIdPB& RpcController::request_id() const {
  DCHECK(has_request_id());
  return *request_id_;
}

void RpcController::RequireServerFeature(uint32_t feature) {
  DCHECK(!call_ || call_->state() == OutboundCall::READY);
  required_server_features_.insert(feature);
}

MonoDelta RpcController::timeout() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return timeout_;
}

Status RpcController::AddOutboundSidecar(unique_ptr<RpcSidecar> car, int* idx) {
  if (outbound_sidecars_.size() >= TransferLimits::kMaxSidecars) {
    return Status::RuntimeError("All available sidecars already used");
  }
  int64_t sidecar_bytes = car->AsSlice().size();
  if (outbound_sidecars_total_bytes_ >
      TransferLimits::kMaxTotalSidecarBytes - sidecar_bytes) {
    return Status::RuntimeError(Substitute("Total size of sidecars $0 would exceed limit $1",
        static_cast<int64_t>(outbound_sidecars_total_bytes_) + sidecar_bytes,
        TransferLimits::kMaxTotalSidecarBytes));
  }

  outbound_sidecars_.emplace_back(std::move(car));
  outbound_sidecars_total_bytes_ += sidecar_bytes;
  DCHECK_GE(outbound_sidecars_total_bytes_, 0);
  *idx = outbound_sidecars_.size() - 1;
  return Status::OK();
}

void RpcController::SetRequestParam(const google::protobuf::Message& req) {
  DCHECK(call_ != nullptr);
  call_->SetRequestPayload(req, std::move(outbound_sidecars_));
}

void RpcController::Cancel() {
  DCHECK(call_);
  DCHECK(messenger_);
  messenger_->QueueCancellation(call_);
}

} // namespace rpc
} // namespace kudu
