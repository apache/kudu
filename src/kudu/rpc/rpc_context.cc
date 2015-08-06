// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/rpc/rpc_context.h"

#include <ostream>

#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/service_if.h"
#include "kudu/util/hdr_histogram.h"
#include "kudu/util/metrics.h"
#include "kudu/util/trace.h"

using google::protobuf::MessageLite;

namespace kudu {
namespace rpc {

RpcContext::RpcContext(InboundCall *call,
                       const google::protobuf::Message *request_pb,
                       google::protobuf::Message *response_pb,
                       RpcMethodMetrics metrics)
  : call_(CHECK_NOTNULL(call)),
    request_pb_(request_pb),
    response_pb_(response_pb),
    metrics_(metrics) {
  VLOG(4) << call_->remote_method().service_name() << ": Received RPC request for "
          << call_->ToString() << ":" << std::endl << request_pb_->DebugString();
}

RpcContext::~RpcContext() {
}

void RpcContext::RespondSuccess() {
  call_->RecordHandlingCompleted(metrics_.handler_latency);
  VLOG(4) << call_->remote_method().service_name() << ": Sending RPC success response for "
          << call_->ToString() << ":" << std::endl << response_pb_->DebugString();
  call_->RespondSuccess(*response_pb_);
  delete this;
}

void RpcContext::RespondFailure(const Status &status) {
  call_->RecordHandlingCompleted(metrics_.handler_latency);
  VLOG(4) << call_->remote_method().service_name() << ": Sending RPC failure response for "
          << call_->ToString() << ": " << status.ToString();
  call_->RespondFailure(ErrorStatusPB::ERROR_APPLICATION,
                        status);
  delete this;
}

void RpcContext::RespondRpcFailure(ErrorStatusPB_RpcErrorCodePB err, const Status& status) {
  call_->RecordHandlingCompleted(metrics_.handler_latency);
  VLOG(4) << call_->remote_method().service_name() << ": Sending RPC failure response for "
          << call_->ToString() << ": " << status.ToString();
  call_->RespondFailure(err, status);
  delete this;
}

void RpcContext::RespondApplicationError(int error_ext_id, const std::string& message,
                                         const MessageLite& app_error_pb) {
  call_->RecordHandlingCompleted(metrics_.handler_latency);
  if (VLOG_IS_ON(4)) {
    ErrorStatusPB err;
    InboundCall::ApplicationErrorToPB(error_ext_id, message, app_error_pb, &err);
    VLOG(4) << call_->remote_method().service_name() << ": Sending application error response for "
            << call_->ToString() << ":" << std::endl << err.DebugString();
  }
  call_->RespondApplicationError(error_ext_id, message, app_error_pb);
  delete this;
}

Status RpcContext::AddRpcSidecar(gscoped_ptr<RpcSidecar> car, int* idx) {
  return call_->AddRpcSidecar(car.Pass(), idx);
}

const UserCredentials& RpcContext::user_credentials() const {
  return call_->user_credentials();
}

const Sockaddr& RpcContext::remote_address() const {
  return call_->remote_address();
}

std::string RpcContext::requestor_string() const {
  return call_->user_credentials().ToString() + " at " +
    call_->remote_address().ToString();
}

MonoTime RpcContext::GetClientDeadline() const {
  return call_->GetClientDeadline();
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
  MY_ERROR << "Request:\n" << request_pb_->DebugString();
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
