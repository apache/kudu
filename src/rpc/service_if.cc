// Copyright (c) 2013, Cloudera, inc.

#include "rpc/service_if.h"

#include <string>

#include "rpc/inbound_call.h"
#include "rpc/rpc_header.pb.h"

using std::string;

namespace kudu {
namespace rpc {

ServiceIf::~ServiceIf() {
}

void ServiceIf::Shutdown() {
}

bool ServiceIf::ParseParam(InboundCall *call, google::protobuf::Message *message) {
  Slice param(call->serialized_request());
  if (PREDICT_FALSE(!message->ParseFromArray(param.data(), param.size()))) {
    string err = StringPrintf("Invalid parameter for call %s: %s",
                              call->method_name().c_str(),
                              message->InitializationErrorString().c_str());
    LOG(WARNING) << err;
    call->RespondFailure(ErrorStatusPB::ERROR_INVALID_REQUEST,
                         Status::InvalidArgument(err));
    return false;
  }
  return true;
}

void ServiceIf::RespondBadMethod(InboundCall *call) {
  string err = StringPrintf("Invalid method: %s",
                            call->method_name().c_str());
  LOG(WARNING) << err << " from " << call->remote_address().ToString();
  call->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_METHOD,
                       Status::InvalidArgument(err));
}

} // namespace rpc
} // namespace kudu
