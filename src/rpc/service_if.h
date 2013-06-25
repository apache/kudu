// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_RPC_SERVICE_IF_H
#define KUDU_RPC_SERVICE_IF_H

#include "gutil/gscoped_ptr.h"
#include "rpc/sockaddr.h"

namespace kudu {
namespace rpc {

class InboundCall;

// Handles incoming messages that initiate an RPC.
class ServiceIf {
 public:
  virtual ~ServiceIf();
  virtual void Handle(InboundCall *incoming) = 0;
};

} // namespace rpc
} // namespace kudu
#endif
