// Copyright (c) 2013, Cloudera, inc.

#include "rpc/proxy.h"

#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <glog/logging.h>
#include <inttypes.h>
#include <stdint.h>
#include <tr1/memory>

#include <iostream>
#include <sstream>
#include <vector>

#include "rpc/client_call.h"
#include "rpc/messenger.h"
#include "rpc/response_callback.h"
#include "rpc/rpc_header.pb.h"
#include "util/net/sockaddr.h"
#include "util/net/socket.h"
#include "util/countdown_latch.h"
#include "util/status.h"
#include "util/user.h"

using google::protobuf::Message;
using std::tr1::shared_ptr;

namespace kudu {
namespace rpc {

Proxy::Proxy(const std::tr1::shared_ptr<Messenger>& messenger,
             const Sockaddr& remote,
             const string& service_name)
  : messenger_(messenger),
    is_started_(false) {
  DCHECK(!service_name.empty()) << "Proxy service name must not be blank";

  // By default, we set the real user to the currently logged-in user.
  // Effective user and password remain blank.
  string real_user;
  Status s = GetLoggedInUser(&real_user);
  if (!s.ok()) {
    LOG(WARNING) << "Proxy for " << service_name << ": Unable to get logged-in user name: "
        << s.ToString() << " before connecting to remote: " << remote.ToString();
  }

  conn_id_.set_remote(remote);
  conn_id_.set_service_name(service_name);
  conn_id_.mutable_user_cred()->set_real_user(real_user);
}

Proxy::~Proxy() {
}

void Proxy::AsyncRequest(const string& method,
                         const google::protobuf::Message& req,
                         google::protobuf::Message* response,
                         RpcController* controller,
                         const ResponseCallback& callback) const {
  CHECK(controller->call_.get() == NULL) << "Controller should be reset";
  base::subtle::NoBarrier_Store(&is_started_, true);
  OutboundCall* call = new OutboundCall(conn_id_, method, response, controller, callback);
  controller->call_.reset(call);
  Status s = call->SetRequestParam(req);
  if (PREDICT_FALSE(!s.ok())) {
    // Failed to serialize request: likely the request is missing a required
    // field.
    call->SetFailed(s);
    callback();
    return;
  }

  // If this fails to queue, the callback will get called immediately
  // and the controller will be in an ERROR state.
  messenger_->QueueOutboundCall(controller->call_);
}


Status Proxy::SyncRequest(const string& method,
                          const google::protobuf::Message& req,
                          google::protobuf::Message* resp,
                          RpcController* controller) const {
  CountDownLatch latch(1);
  AsyncRequest(method, req, DCHECK_NOTNULL(resp), controller,
               boost::bind(&CountDownLatch::CountDown, boost::ref(latch)));

  latch.Wait();
  return controller->status();
}

void Proxy::set_user_cred(const UserCredentials& user_cred) {
  CHECK(base::subtle::Release_Load(&is_started_) == false) << "It is illegal to call set_user_cred() after request processing has started";
  conn_id_.set_user_cred(user_cred);
}

} // namespace rpc
} // namespace kudu
