// Copyright (c) 2013, Cloudera, inc.

#include "rpc/rpc_context.h"

#include "rpc/outbound_call.h"
#include "rpc/inbound_call.h"

namespace kudu {
namespace rpc {

RpcContext::RpcContext(InboundCall *call,
                       const google::protobuf::Message *request_pb,
                       google::protobuf::Message *response_pb)
  : call_(call),
    request_pb_(request_pb),
    response_pb_(response_pb) {
}

RpcContext::~RpcContext() {
}

void RpcContext::RespondSuccess() {
  call_->RespondSuccess(*response_pb_);
  delete this;
}

void RpcContext::RespondFailure(const Status &status) {
  call_->RespondFailure(status);
  delete this;
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

Trace* RpcContext::trace() {
  return call_->trace();
}

} // namespace rpc
} // namespace kudu
