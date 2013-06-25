// Copyright (c) 2013, Cloudera, inc.

#include "rpc/proxy.h"

#include <boost/bind.hpp>
#include <boost/thread/mutex.hpp>
#include <glog/logging.h>
#include <inttypes.h>
#include <iostream>
#include <sstream>
#include <stdint.h>
#include <tr1/memory>
#include <vector>

#include "rpc/client_call.h"
#include "rpc/messenger.h"
#include "rpc/response_callback.h"
#include "rpc/rpc_header.pb.h"
#include "rpc/sockaddr.h"
#include "rpc/socket.h"
#include "util/countdown_latch.h"
#include "util/status.h"

using google::protobuf::Message;
using std::tr1::shared_ptr;

namespace kudu {
namespace rpc {

Proxy::Proxy(const std::tr1::shared_ptr<Messenger> &messenger,
             const Sockaddr &remote)
  : messenger_(messenger),
    remote_(remote)
{
}

Proxy::~Proxy() {
}

void Proxy::AsyncRequest(const string &method,
                         const google::protobuf::Message &req,
                         google::protobuf::Message *response,
                         RpcController *controller,
                         const ResponseCallback &callback) const {
  shared_ptr<OutboundCall> call(
    new OutboundCall(remote_, method, response, controller, callback));
  Status s = call->SetRequestParam(req);
  if (PREDICT_FALSE(!s.ok())) {
    // Failed to serialize request: likely the request is missing a required
    // field.
    controller->SetFailed(s);
    callback();
    return;
  }

  // If this fails to queue, the callback will get called immediately
  // and the controller will be in an ERROR state.
  messenger_->QueueOutboundCall(call);
}


Status Proxy::SyncRequest(const string &method,
                          const google::protobuf::Message &req,
                          google::protobuf::Message *resp,
                          RpcController *controller) const {
  CountDownLatch latch(1);
  AsyncRequest(method, req, DCHECK_NOTNULL(resp), controller,
               boost::bind(&CountDownLatch::CountDown, boost::ref(latch)));

  latch.Wait();
  return controller->status();
}

const Sockaddr &Proxy::remote() const {
  return remote_;
}

}
}
