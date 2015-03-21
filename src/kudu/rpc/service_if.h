// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_RPC_SERVICE_IF_H
#define KUDU_RPC_SERVICE_IF_H

#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"

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
  RpcMethodMetrics();
  ~RpcMethodMetrics();

  scoped_refptr<Histogram> handler_latency;
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
