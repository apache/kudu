// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_RPC_SERVICE_IF_H
#define KUDU_RPC_SERVICE_IF_H

#include <string>

#include "gutil/gscoped_ptr.h"
#include "util/net/sockaddr.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {

class Histogram;

namespace rpc {

class InboundCall;

struct RpcMethodMetrics {
  RpcMethodMetrics()
    : handler_latency(NULL) {
  }
  Histogram* handler_latency;
};

// Handles incoming messages that initiate an RPC.
class ServiceIf {
 public:
  virtual ~ServiceIf();
  virtual void Handle(InboundCall* incoming) = 0;
  virtual void Shutdown();
  virtual std::string service_name() const = 0;

 protected:
  bool ParseParam(InboundCall* call, google::protobuf::Message* message);
  void RespondBadMethod(InboundCall* call);

};

} // namespace rpc
} // namespace kudu
#endif
