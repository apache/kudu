// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_RPC_SERVICE_H_
#define KUDU_RPC_SERVICE_H_

#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {
namespace rpc {

class InboundCall;

class RpcService : public RefCountedThreadSafe<RpcService> {
 public:
  virtual ~RpcService() {}

  // Enqueue a call for processing.
  // On failure, the RpcService::QueueInboundCall() implementation is
  // responsible for responding to the client with a failure message.
  virtual Status QueueInboundCall(gscoped_ptr<InboundCall> call) = 0;
};

} // namespace rpc
} // namespace kudu

#endif // KUDU_RPC_SERVICE_H_
