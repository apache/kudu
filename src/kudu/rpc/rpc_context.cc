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

#include "kudu/rpc/rpc_context.h"

#include <memory>
#include <ostream>
#include <utility>

#include <glog/logging.h>
#include <google/protobuf/message.h>

#include "kudu/rpc/connection.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/result_tracker.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/trace.h"

using google::protobuf::Message;
using kudu::pb_util::SecureDebugString;
using std::string;
using std::unique_ptr;

namespace kudu {

class Slice;

namespace rpc {

RpcContext::RpcContext(InboundCall *call,
                       const google::protobuf::Message *request_pb,
                       google::protobuf::Message *response_pb,
                       scoped_refptr<ResultTracker> result_tracker)
  : call_(CHECK_NOTNULL(call)),
    request_pb_(request_pb),
    response_pb_(response_pb),
    result_tracker_(std::move(result_tracker)) {
  VLOG(4) << call_->remote_method().service_name() << ": Received RPC request for "
          << call_->ToString() << ":" << std::endl << SecureDebugString(*request_pb_);
  TRACE_EVENT_ASYNC_BEGIN2("rpc_call", "RPC", this,
                           "call", call_->ToString(),
                           "request", pb_util::PbTracer::TracePb(*request_pb_));
}

RpcContext::~RpcContext() {
}

void RpcContext::RespondSuccess() {
  if (AreResultsTracked()) {
    result_tracker_->RecordCompletionAndRespond(call_->header().request_id(),
                                                response_pb_.get());
  } else {
    VLOG(4) << call_->remote_method().service_name() << ": Sending RPC success response for "
        << call_->ToString() << ":" << std::endl << SecureDebugString(*response_pb_);
    TRACE_EVENT_ASYNC_END2("rpc_call", "RPC", this,
                           "response", pb_util::PbTracer::TracePb(*response_pb_),
                           "trace", trace()->DumpToString());
    call_->RespondSuccess(*response_pb_);
    delete this;
  }
}

void RpcContext::RespondNoCache() {
  if (AreResultsTracked()) {
    result_tracker_->FailAndRespond(call_->header().request_id(),
                                    response_pb_.get());
  } else {
    VLOG(4) << call_->remote_method().service_name() << ": Sending RPC failure response for "
        << call_->ToString() << ": " << SecureDebugString(*response_pb_);
    TRACE_EVENT_ASYNC_END2("rpc_call", "RPC", this,
                           "response", pb_util::PbTracer::TracePb(*response_pb_),
                           "trace", trace()->DumpToString());
    // This is a bit counter intuitive, but when we get the failure but set the error on the
    // call's response we call RespondSuccess() instead of RespondFailure().
    call_->RespondSuccess(*response_pb_);
    delete this;
  }
}

void RpcContext::RespondFailure(const Status &status) {
  return RespondRpcFailure(ErrorStatusPB::ERROR_APPLICATION, status);
}

void RpcContext::RespondRpcFailure(ErrorStatusPB_RpcErrorCodePB err, const Status& status) {
  if (AreResultsTracked()) {
    result_tracker_->FailAndRespond(call_->header().request_id(),
                                    err, status);
  } else {
    VLOG(4) << call_->remote_method().service_name() << ": Sending RPC failure response for "
        << call_->ToString() << ": " << status.ToString();
    TRACE_EVENT_ASYNC_END2("rpc_call", "RPC", this,
                           "status", status.ToString(),
                           "trace", trace()->DumpToString());
    call_->RespondFailure(err, status);
    delete this;
  }
}

void RpcContext::RespondApplicationError(int error_ext_id, const std::string& message,
                                         const Message& app_error_pb) {
  if (AreResultsTracked()) {
    result_tracker_->FailAndRespond(call_->header().request_id(),
                                    error_ext_id, message, app_error_pb);
  } else {
    if (VLOG_IS_ON(4)) {
      ErrorStatusPB err;
      InboundCall::ApplicationErrorToPB(error_ext_id, message, app_error_pb, &err);
      VLOG(4) << call_->remote_method().service_name()
          << ": Sending application error response for " << call_->ToString()
          << ":" << std::endl << SecureDebugString(err);
    }
    TRACE_EVENT_ASYNC_END2("rpc_call", "RPC", this,
                           "response", pb_util::PbTracer::TracePb(app_error_pb),
                           "trace", trace()->DumpToString());
    call_->RespondApplicationError(error_ext_id, message, app_error_pb);
    delete this;
  }
}

const rpc::RequestIdPB* RpcContext::request_id() const {
  return call_->header().has_request_id() ? &call_->header().request_id() : nullptr;
}

size_t RpcContext::GetTransferSize() const {
  return call_->GetTransferSize();
}

Status RpcContext::AddOutboundSidecar(unique_ptr<RpcSidecar> car, int* idx) {
  return call_->AddOutboundSidecar(std::move(car), idx);
}

Status RpcContext::GetInboundSidecar(int idx, Slice* slice) const {
  return call_->GetInboundSidecar(idx, slice);
}

const RemoteUser& RpcContext::remote_user() const {
  return call_->remote_user();
}

bool RpcContext::is_confidential() const {
  return call_->connection()->is_confidential();
}

void RpcContext::DiscardTransfer() {
  call_->DiscardTransfer();
}

const Sockaddr& RpcContext::remote_address() const {
  return call_->remote_address();
}

std::string RpcContext::requestor_string() const {
  return call_->remote_user().ToString() + " at " +
    call_->remote_address().ToString();
}

std::string RpcContext::method_name() const {
  return call_->remote_method().method_name();
}

std::string RpcContext::service_name() const {
  return call_->remote_method().service_name();
}

MonoTime RpcContext::GetClientDeadline() const {
  return call_->GetClientDeadline();
}

MonoTime RpcContext::GetTimeReceived() const {
  return call_->GetTimeReceived();
}

Trace* RpcContext::trace() {
  return call_->trace();
}

void RpcContext::Panic(const char* filepath, int line_number, const string& message) {
  // Use the LogMessage class directly so that the log messages appear to come from
  // the line of code which caused the panic, not this code.
#define MY_ERROR google::LogMessage(filepath, line_number, google::GLOG_ERROR).stream()
#define MY_FATAL google::LogMessageFatal(filepath, line_number).stream()

  MY_ERROR << "Panic handling " << call_->ToString() << ": " << message;
  MY_ERROR << "Request:\n" << SecureDebugString(*request_pb_);
  Trace* t = trace();
  if (t) {
    MY_ERROR << "RPC trace:";
    t->Dump(&MY_ERROR, true);
  }
  MY_FATAL << "Exiting due to panic.";

#undef MY_ERROR
#undef MY_FATAL
}


} // namespace rpc
} // namespace kudu
