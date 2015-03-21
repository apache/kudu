// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/rpc/service_if.h"

#include <string>

#include "kudu/gutil/strings/substitute.h"

#include "kudu/rpc/connection.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/rpc_header.pb.h"

using std::string;
using strings::Substitute;

namespace kudu {
namespace rpc {

RpcMethodMetrics::RpcMethodMetrics()
  : handler_latency(NULL) {
}

RpcMethodMetrics::~RpcMethodMetrics() {
}

ServiceIf::~ServiceIf() {
}

void ServiceIf::Shutdown() {
}

bool ServiceIf::ParseParam(InboundCall *call, google::protobuf::Message *message) {
  Slice param(call->serialized_request());
  if (PREDICT_FALSE(!message->ParseFromArray(param.data(), param.size()))) {
    string err = Substitute("Invalid parameter for call $0: $1",
                            call->remote_method().ToString(),
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
                          call->remote_method().service_name(),
                          local_addr.ToString(),
                          remote_addr.ToString(),
                          call->remote_method().method_name());
  LOG(WARNING) << err;
  call->RespondFailure(ErrorStatusPB::ERROR_NO_SUCH_METHOD,
                       Status::InvalidArgument(err));
}

} // namespace rpc
} // namespace kudu
