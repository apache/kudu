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
#include "kudu/gutil/sysinfo.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
#include "kudu/util/net/diagnostic_socket.h"
#endif

using std::string;
using strings::Substitute;

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

METRIC_DEFINE_histogram(server, acceptor_dispatch_times,
                        "Acceptor Dispatch Times",
                        kudu::MetricUnit::kMicroseconds,
                        "A histogram of dispatching timings for accepted "
                        "connections. Outliers in this histogram contribute "
                        "to the latency of handling incoming connection "
                        "requests and growing the backlog of pending TCP "
                        "connections to the server.",
                        kudu::MetricLevel::kInfo,
                        1000000, 2);

METRIC_DEFINE_histogram(server, rpc_listen_socket_rx_queue_size,
                        "Listening RPC Socket Backlog",
                        kudu::MetricUnit::kEntries,
                        "A histogram of the pending connections queue size for "
                        "the listening RPC socket that this acceptor pool serves.",
                        kudu::MetricLevel::kInfo,
                        1000000, 2);

DEFINE_int32(rpc_acceptor_listen_backlog,
             kudu::rpc::AcceptorPool::kDefaultListenBacklog,
             "Socket backlog parameter used when listening for RPC connections. "
             "This defines the maximum length to which the queue of pending "
             "TCP connections inbound to the RPC server may grow. The value "
             "might be silently capped by the system-level limit on the listened "
             "socket's backlog. The value of -1 has the semantics of the "
             "longest possible queue with the length up to the system-level "
             "limit. If a connection request arrives when the queue is full, "
             "the client may receive an error. Higher values may help "
             "the server ride over bursts of new inbound connection requests.");
TAG_FLAG(rpc_acceptor_listen_backlog, advanced);

DEFINE_int32(rpc_listen_socket_stats_every_log2,
#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
             3,
#else
             -1,
#endif
             "Listening RPC socket's statistics sampling frequency. With "
             "--rpc_listen_socket_stats_every_log2=N, the statistics are "
             "sampled every 2^N connection request by each acceptor thread. "
             "Set this flag to -1 to disable statistics collection on "
             "the listening RPC socket.");
TAG_FLAG(rpc_listen_socket_stats_every_log2, advanced);

namespace {

bool ValidateListenBacklog(const char* flagname, int value) {
  if (value >= -1) {
    return true;
  }
  LOG(ERROR) << Substitute(
      "$0: invalid setting for $1; regular setting must be at least 0, and -1 "
      "is a special value with the semantics of maximum possible setting "
      "capped at the system-wide limit", value, flagname);
  return false;
}

bool ValidateStatsCollectionFrequency(const char* flagname, int value) {
  if (value < 64) {
    return true;
  }
  LOG(ERROR) << Substitute("$0: invalid setting for $1; must be less than 64",
                           value, flagname);
  return false;
}

} // anonymous namespace

DEFINE_validator(rpc_acceptor_listen_backlog, &ValidateListenBacklog);
DEFINE_validator(rpc_listen_socket_stats_every_log2,
                 &ValidateStatsCollectionFrequency);


namespace kudu {
namespace rpc {

AcceptorPool::AcceptorPool(Messenger* messenger,
                           Socket* socket,
                           const Sockaddr& bind_address,
                           int listen_backlog)
    : messenger_(messenger),
      socket_(socket->Release()),
      bind_address_(bind_address),
      listen_backlog_(listen_backlog),
      closing_(false) {
  const auto& metric_entity = messenger->metric_entity();
  auto& connections_accepted = bind_address.is_ip()
      ? METRIC_rpc_connections_accepted
      : METRIC_rpc_connections_accepted_unix_domain_socket;
  rpc_connections_accepted_ = connections_accepted.Instantiate(metric_entity);
  dispatch_times_ = METRIC_acceptor_dispatch_times.Instantiate(metric_entity);
  listen_socket_queue_size_ =
      METRIC_rpc_listen_socket_rx_queue_size.Instantiate(metric_entity);
}

AcceptorPool::~AcceptorPool() {
  Shutdown();
}

Status AcceptorPool::Start(int num_threads) {
  RETURN_NOT_OK(socket_.Listen(listen_backlog_));
#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
  WARN_NOT_OK(diag_socket_.Init(), "could not initialize diagnostic socket");
#endif

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

#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
  WARN_NOT_OK(diag_socket_.Close(), "error closing diagnostic socket");
#endif

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

Status AcceptorPool::GetPendingConnectionsNum(uint32_t* result) const {
#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
  DiagnosticSocket::TcpSocketInfo info;
  RETURN_NOT_OK(diag_socket_.Query(socket_, &info));
  *result = info.rx_queue_size;

  return Status::OK();
#else // #if defined(KUDU_HAS_DIAGNOSTIC_SOCKET) ...
  return Status::NotSupported(
      "pending connections metric is not available for this platform");
#endif // #if defined(KUDU_HAS_DIAGNOSTIC_SOCKET) ... else ...
}

void AcceptorPool::RunThread() {
  const int64_t kCyclesPerSecond = static_cast<int64_t>(base::CyclesPerSecond());

  // Fetch and keep the information on the listening socket's address to avoid
  // re-fetching it every time when accepting a new connection.
  // The diagnostic socket is needed to fetch information on the RX queue size.
  Sockaddr cur_addr;
  WARN_NOT_OK(socket_.GetSocketAddress(&cur_addr),
              "unable to get address info on RPC socket");
  const auto& cur_addr_str = cur_addr.ToString();

  const int ds_query_freq_log2 = FLAGS_rpc_listen_socket_stats_every_log2;
  const bool ds_query_enabled = (ds_query_freq_log2 >= 0);

#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
  const uint64_t ds_query_freq_mask =
      ds_query_enabled ? (1ULL << ds_query_freq_log2) - 1 : 0;
  DiagnosticSocket ds;
  uint64_t counter = 0;
  if (ds_query_enabled) {
    if (const auto s = ds.Init(); s.ok()) {
      LOG(INFO) << Substitute(
          "collecting diagnostics on the listening RPC socket $0 "
          "every $1 connection(s)", cur_addr_str, ds_query_freq_mask + 1);
    } else {
      WARN_NOT_OK(s, "unable to open diagnostic socket");
    }
  } else {
    LOG(INFO) << Substitute(
        "collecting diagnostics on the listening RPC socket $0 is disabled",
        cur_addr_str);
  }
#else
  if (ds_query_enabled) {
    LOG(WARNING) << Substitute(
        "--rpc_listen_socket_stats_every_log2 is set to $0, but collecting "
        "stats on listening RPC sockets is not supported on this platform",
        ds_query_freq_log2);
  }
#endif // #if defined(KUDU_HAS_DIAGNOSTIC_SOCKET) ... else ...

  while (true) {
    Socket new_sock;
    Sockaddr remote;
    VLOG(2) << Substitute("calling accept() on socket $0 listening on $1",
                          socket_.GetFd(), bind_address_.ToString());
    const auto s = socket_.Accept(&new_sock, &remote, Socket::FLAG_NONBLOCKING);
    const auto accepted_at = CycleClock::Now();

#if defined(KUDU_HAS_DIAGNOSTIC_SOCKET)
    if (ds_query_enabled && ds.IsInitialized() &&
        (counter & ds_query_freq_mask) == ds_query_freq_mask) {
      VLOG(2) << "getting stats on the listening socket";
      // Once removing an element from the pending connections queue
      // (a.k.a. listen backlog), collect information on the number
      // of connections in the queue still waiting to be accepted.
      DiagnosticSocket::TcpSocketInfo info;
      if (auto s = ds.Query(socket_, &info); PREDICT_TRUE(s.ok())) {
        listen_socket_queue_size_->Increment(info.rx_queue_size);
      } else if (!closing_) {
        KLOG_EVERY_N_SECS(WARNING, 60)
            << Substitute("unable to collect diagnostics on RPC socket $0: $1",
                          cur_addr_str, s.ToString());
      }
    }
    ++counter;
#endif // #if defined(KUDU_HAS_DIAGNOSTIC_SOCKET) ...

    const auto dispatch_times_recorder = MakeScopedCleanup([&]() {
      // The timings are captured for both success and failure paths, so the
      // 'dispatch_times_' histogram accounts for all the connection attempts
      // that lead to successfully extracting an item from the queue of pending
      // connections for the listening RPC socket. Meanwhile, the
      // 'rpc_connection_accepted_' counter accounts only for connections that
      // were successfully dispatched to the messenger for further processing.
      dispatch_times_->Increment(
          (CycleClock::Now() - accepted_at) * 1000000 / kCyclesPerSecond);
    });

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
    messenger_->RegisterInboundSocket(&new_sock, remote);
    rpc_connections_accepted_->Increment();
  }
  VLOG(1) << "AcceptorPool shutting down";
}

} // namespace rpc
} // namespace kudu
