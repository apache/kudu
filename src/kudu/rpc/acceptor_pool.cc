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

#include "kudu/rpc/acceptor_pool.h"

#include <functional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"


METRIC_DEFINE_counter(server, rpc_connections_accepted,
                      "RPC Connections Accepted",
                      kudu::MetricUnit::kConnections,
                      "Number of incoming TCP connections made to the RPC server",
                      kudu::MetricLevel::kInfo);

METRIC_DEFINE_counter(server, rpc_connections_accepted_unix_domain_socket,
                      "RPC Connections Accepted via UNIX Domain Socket",
                      kudu::MetricUnit::kConnections,
                      "Number of incoming UNIX Domain Socket connections made to the RPC server",
                      kudu::MetricLevel::kInfo);

DEFINE_int32(rpc_acceptor_listen_backlog, 128,
             "Socket backlog parameter used when listening for RPC connections. "
             "This defines the maximum length to which the queue of pending "
             "TCP connections inbound to the RPC server may grow. If a connection "
             "request arrives when the queue is full, the client may receive "
             "an error. Higher values may help the server ride over bursts of "
             "new inbound connection requests.");
TAG_FLAG(rpc_acceptor_listen_backlog, advanced);

using std::string;
using strings::Substitute;

namespace kudu {
namespace rpc {

AcceptorPool::AcceptorPool(Messenger* messenger,
                           Socket* socket,
                           const Sockaddr& bind_address)
    : messenger_(messenger),
      socket_(socket->Release()),
      bind_address_(bind_address),
      closing_(false) {
  auto& accept_metric = bind_address.is_ip() ?
      METRIC_rpc_connections_accepted :
      METRIC_rpc_connections_accepted_unix_domain_socket;
  rpc_connections_accepted_ = accept_metric.Instantiate(messenger->metric_entity());
}

AcceptorPool::~AcceptorPool() {
  Shutdown();
}

Status AcceptorPool::Start(int num_threads) {
  RETURN_NOT_OK(socket_.Listen(FLAGS_rpc_acceptor_listen_backlog));

  for (int i = 0; i < num_threads; i++) {
    scoped_refptr<Thread> new_thread;
    Status s = Thread::Create("acceptor pool", "acceptor",
                              [this]() { this->RunThread(); }, &new_thread);
    if (PREDICT_FALSE(!s.ok())) {
      Shutdown();
      return s;
    }
    threads_.emplace_back(std::move(new_thread));
  }
  return Status::OK();
}

void AcceptorPool::Shutdown() {
  bool is_shut_down = false;
  if (!closing_.compare_exchange_strong(is_shut_down, true)) {
    VLOG(2) << Substitute("AcceptorPool on $0 already shut down",
                          bind_address_.ToString());
    return;
  }

#if defined(__linux__)
  // Closing the socket will break us out of accept() if we're in it, and
  // prevent future accepts.
  WARN_NOT_OK(socket_.Shutdown(true, true),
              Substitute("Could not shut down acceptor socket on $0",
                         bind_address_.ToString()));
#else
  // Calling shutdown on an accepting (non-connected) socket is illegal on most
  // platforms (but not Linux). Instead, the accepting threads are interrupted
  // forcefully.
  for (const auto& thread : threads_) {
    pthread_cancel(thread.get()->pthread_id());
  }
#endif

  for (const auto& thread : threads_) {
    CHECK_OK(ThreadJoiner(thread.get()).Join());
  }
  threads_.clear();

  // Close the socket: keeping the descriptor open and, possibly, receiving late
  // not-to-be-read messages from the peer does not make much sense. The
  // Socket::Close() method is called upon destruction of the aggregated socket_
  // object as well. However, the typical ownership pattern of an AcceptorPool
  // object includes two references wrapped via a shared_ptr smart pointer: one
  // is held by Messenger, another by RpcServer. If not calling Socket::Close()
  // here, it would  necessary to wait until Messenger::Shutdown() is called for
  // the corresponding messenger object to close this socket.
  ignore_result(socket_.Close());
}

Sockaddr AcceptorPool::bind_address() const {
  return bind_address_;
}

Status AcceptorPool::GetBoundAddress(Sockaddr* addr) const {
  return socket_.GetSocketAddress(addr);
}

int64_t AcceptorPool::num_rpc_connections_accepted() const {
  return rpc_connections_accepted_->value();
}

void AcceptorPool::RunThread() {
  while (true) {
    Socket new_sock;
    Sockaddr remote;
    VLOG(2) << Substitute("calling accept() on socket $0 listening on $1",
                          socket_.GetFd(), bind_address_.ToString());
    const auto s = socket_.Accept(&new_sock, &remote, Socket::FLAG_NONBLOCKING);
    if (PREDICT_FALSE(!s.ok())) {
      if (closing_) {
        break;
      }
      KLOG_EVERY_N_SECS(WARNING, 1)
          << Substitute("AcceptorPool: accept() failed: $0", s.ToString())
          << THROTTLE_MSG;
      continue;
    }
    if (remote.is_ip()) {
      if (auto s = new_sock.SetNoDelay(true); PREDICT_FALSE(!s.ok())) {
        KLOG_EVERY_N_SECS(WARNING, 1)
            << Substitute("unable to set TCP_NODELAY on newly accepted "
                          "connection from $0: $1",
                          remote.ToString(), s.ToString())
            << THROTTLE_MSG;
        continue;
      }
    }
    rpc_connections_accepted_->Increment();
    messenger_->RegisterInboundSocket(&new_sock, remote);
  }
  VLOG(1) << "AcceptorPool shutting down";
}

} // namespace rpc
} // namespace kudu
