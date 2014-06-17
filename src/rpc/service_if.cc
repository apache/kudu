// Copyright (c) 2013, Cloudera, inc.

#include "rpc/service_if.h"

#include <string>

#include "gutil/strings/substitute.h"

#include "rpc/connection.h"
#include "rpc/inbound_call.h"
#include "rpc/rpc_header.pb.h"

using std::string;
using strings::Substitute;

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
  Sockaddr local_addr, remote_addr;

  CHECK_OK(call->connection()->socket()->GetSocketAddress(&local_addr));
  CHECK_OK(call->connection()->socket()->GetPeerAddress(&remote_addr));
  string err = Substitute("Call on service $0 received at $1 from $2 with an "
                          "invalid method name: $3",
                          call->connection()->service_name(),
                          local_addr.ToString(),
                          remote_addr.ToString(),
                          call->method_name());
  LOG(WARNING) << err;
  call->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_METHOD,
                       Status::InvalidArgument(err));
}

} // namespace rpc
} // namespace kudu
