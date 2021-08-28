// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/rpc/proxy.h"

#include <functional>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/user_credentials.h"
#include "kudu/util/net/dns_resolver.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/notification.h"
#include "kudu/util/status.h"
#include "kudu/util/user.h"

using google::protobuf::Message;
using std::string;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rpc {

Proxy::Proxy(std::shared_ptr<Messenger> messenger,
             const Sockaddr& remote,
             string hostname,
             string service_name)
    : service_name_(std::move(service_name)),
      dns_resolver_(nullptr),
      messenger_(std::move(messenger)),
      is_started_(false) {
  CHECK(messenger_ != nullptr);
  DCHECK(!service_name_.empty()) << "Proxy service name must not be blank";
  DCHECK(remote.is_initialized());
  // By default, we set the real user to the currently logged-in user.
  // Effective user and password remain blank.
  string real_user;
  Status s = GetLoggedInUser(&real_user);
  if (!s.ok()) {
    LOG(WARNING) << "Proxy for " << service_name_ << ": Unable to get logged-in user name: "
        << s.ToString() << " before connecting to remote: " << remote.ToString();
  }

  UserCredentials creds;
  creds.set_real_user(std::move(real_user));
  conn_id_ = ConnectionId(remote, std::move(hostname), std::move(creds));
}

Proxy::Proxy(std::shared_ptr<Messenger> messenger,
             HostPort hp,
             DnsResolver* dns_resolver,
             string service_name)
    : service_name_(std::move(service_name)),
      hp_(std::move(hp)),
      dns_resolver_(dns_resolver),
      messenger_(std::move(messenger)),
      is_started_(false) {
  CHECK(messenger_ != nullptr);
  DCHECK(!service_name_.empty()) << "Proxy service name must not be blank";
  DCHECK(hp_.Initialized());
}

Sockaddr* Proxy::GetSingleSockaddr(std::vector<Sockaddr>* addrs) const {
  DCHECK(!addrs->empty());
  if (PREDICT_FALSE(addrs->size() > 1)) {
    LOG(WARNING) << Substitute(
        "$0 proxy host/port $1 resolves to $2 different addresses. Using $3",
        service_name_, hp_.ToString(), addrs->size(), (*addrs)[0].ToString());
  }
  return &(*addrs)[0];
}

void Proxy::Init(Sockaddr addr) {
  if (!dns_resolver_) {
    return;
  }
  // By default, we set the real user to the currently logged-in user.
  // Effective user and password remain blank.
  string real_user;
  Status s = GetLoggedInUser(&real_user);
  if (!s.ok()) {
    LOG(WARNING) << "Proxy for " << service_name_ << ": Unable to get logged-in user name: "
        << s.ToString() << " before connecting to host/port: " << hp_.ToString();
  }
  vector<Sockaddr> addrs;
  if (!addr.is_initialized()) {
    s = dns_resolver_->ResolveAddresses(hp_, &addrs);
    if (PREDICT_TRUE(s.ok() && !addrs.empty())) {
      addr = *GetSingleSockaddr(&addrs);
      DCHECK(addr.is_initialized());
      addr.set_port(hp_.port());
      // NOTE: it's ok to proceed on failure -- the address will remain
      // uninitialized and be re-resolved when sending the next request.
    }
  }

  UserCredentials creds;
  creds.set_real_user(std::move(real_user));
  conn_id_ = ConnectionId(addr, hp_.host(), std::move(creds));
}

Proxy::~Proxy() {
}

void Proxy::EnqueueRequest(const string& method,
                           const google::protobuf::Message& req,
                           google::protobuf::Message* response,
                           RpcController* controller,
                           const ResponseCallback& callback) const {
  ConnectionId connection = conn_id();
  DCHECK(connection.remote().is_initialized());
  RemoteMethod remote_method(service_name_, method);
  controller->call_.reset(
      new OutboundCall(connection, remote_method, response, controller, callback));
  controller->SetRequestParam(req);
  controller->SetMessenger(messenger_.get());

  // If this fails to queue, the callback will get called immediately
  // and the controller will be in an ERROR state.
  messenger_->QueueOutboundCall(controller->call_);
}

void Proxy::RefreshDnsAndEnqueueRequest(const std::string& method,
                                        const google::protobuf::Message& req,
                                        google::protobuf::Message* response,
                                        RpcController* controller,
                                        const ResponseCallback& callback) {
  DCHECK(!controller->call_);
  vector<Sockaddr>* addrs = new vector<Sockaddr>();
  DCHECK_NOTNULL(dns_resolver_)->RefreshAddressesAsync(hp_, addrs,
      [this, &req, &method, callback, response, controller, addrs] (const Status& s) {
    unique_ptr<vector<Sockaddr>> unique_addrs(addrs);
    // If we fail to resolve the address, treat the call as failed.
    if (!s.ok() || addrs->empty()) {
      DCHECK(!controller->call_);
      // NOTE: we need to keep a reference here because the callback may end up
      // destructing the controller and the outbound call, _while_ the callback
      // is running from within the call!
      auto shared_call = std::make_shared<OutboundCall>(
          conn_id(), RemoteMethod{service_name_, method}, response, controller, callback);
      controller->call_ = shared_call;
      controller->call_->SetFailed(s.CloneAndPrepend("failed to refresh physical address"));
      return;
    }
    auto* addr = GetSingleSockaddr(addrs);
    DCHECK(addr->is_initialized());
    addr->set_port(hp_.port());
    {
      std::lock_guard<simple_spinlock> l(lock_);
      conn_id_.set_remote(*addr);
    }
    EnqueueRequest(method, req, response, controller, callback);
  });
}

void Proxy::AsyncRequest(const string& method,
                         const google::protobuf::Message& req,
                         google::protobuf::Message* response,
                         RpcController* controller,
                         const ResponseCallback& callback) {
  CHECK(!controller->call_) << "Controller should be reset";
  base::subtle::NoBarrier_Store(&is_started_, true);
  if (!dns_resolver_) {
    EnqueueRequest(method, req, response, controller, callback);
    return;
  }

  // If we haven't successfully initialized the remote, e.g. because the DNS
  // lookup failed, refresh the DNS entry and enqueue the request.
  bool remote_initialized;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    remote_initialized = conn_id_.remote().is_initialized();
  }
  if (!remote_initialized) {
    RefreshDnsAndEnqueueRequest(method, req, response, controller, callback);
    return;
  }

  // Otherwise, just enqueue the request, but retry if there's a network error,
  // since it's possible the physical address of the host was changed. We only
  // retry once more before calling the callback.
  auto refresh_dns_and_cb = [this, &req, &method,
                             callback, response, controller] () {
    // TODO(awong): we should be more specific here -- consider having the RPC
    // layer set a flag in the controller that warrants a retry.
    if (PREDICT_FALSE(!controller->status().ok())) {
      controller->Reset();
      RefreshDnsAndEnqueueRequest(method, req, response, controller, callback);
      return;
    }
    // For any other status, OK or otherwise, just run the callback.
    callback();
  };
  EnqueueRequest(method, req, response, controller, refresh_dns_and_cb);
}


Status Proxy::SyncRequest(const string& method,
                          const google::protobuf::Message& req,
                          google::protobuf::Message* resp,
                          RpcController* controller) {
  Notification note;
  AsyncRequest(method, req, DCHECK_NOTNULL(resp), controller,
               [&note]() { note.Notify(); });
  note.WaitForNotification();
  return controller->status();
}

void Proxy::set_user_credentials(UserCredentials user_credentials) {
  CHECK(base::subtle::NoBarrier_Load(&is_started_) == false)
    << "It is illegal to call set_user_credentials() after request processing has started";
  conn_id_.set_user_credentials(std::move(user_credentials));
}

void Proxy::set_network_plane(string network_plane) {
  CHECK(base::subtle::NoBarrier_Load(&is_started_) == false)
    << "It is illegal to call set_network_plane() after request processing has started";
  conn_id_.set_network_plane(std::move(network_plane));
}

std::string Proxy::ToString() const {
  return Substitute("$0@$1", service_name_, conn_id_.ToString());
}

} // namespace rpc
} // namespace kudu
