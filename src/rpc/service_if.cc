// Copyright (c) 2013, Cloudera, inc.

#include "rpc/server_call.h"
#include "rpc/service_if.h"

namespace kudu {
namespace rpc {

ServiceIf::~ServiceIf() {
}

bool ServiceIf::ParseParam(InboundCall *call, google::protobuf::Message *message) {
  Slice param(call->serialized_request());
  if (PREDICT_FALSE(!message->ParseFromArray(param.data(), param.size()))) {
    string err = StringPrintf("Invalid parameter for call %s: %s",
                              call->method_name().c_str(),
                              message->InitializationErrorString().c_str());
    LOG(WARNING) << err;
    call->RespondFailure(Status::InvalidArgument(err));
    return false;
  }
  return true;
}

void ServiceIf::RespondBadMethod(InboundCall *call) {
  string err = StringPrintf("Invalid method: %s",
                            call->method_name().c_str());
  LOG(WARNING) << err;
  call->RespondFailure(Status::InvalidArgument(err));
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

  template<class RequestPB, class ResponsePB>
  void DoCall(InboundCall *call);

} // namespace rpc
} // namespace kudu
