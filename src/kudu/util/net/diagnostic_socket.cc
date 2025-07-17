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

#include "kudu/util/net/diagnostic_socket.h"

#include <linux/inet_diag.h>
#include <linux/netlink.h>
#include <linux/sock_diag.h>
#include <linux/types.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/errno.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

const DiagnosticSocket::SocketStates& DiagnosticSocket::SocketStateWildcard() {
  static constexpr const SocketStates kSocketStateWildcard {
    SS_ESTABLISHED,
    SS_SYN_SENT,
    SS_SYN_RECV,
    SS_FIN_WAIT1,
    SS_FIN_WAIT2,
    SS_TIME_WAIT,
    SS_CLOSE,
    SS_CLOSE_WAIT,
    SS_LAST_ACK,
    SS_LISTEN,
    SS_CLOSING,
  };
  static_assert(kSocketStateWildcard.size() == SocketState::SS_MAX);
  return kSocketStateWildcard;
}

DiagnosticSocket::DiagnosticSocket()
    : fd_(-1) {
}

DiagnosticSocket::~DiagnosticSocket() {
  WARN_NOT_OK(Close(), "errors on closing diagnostic socket");
}

Status DiagnosticSocket::Init() {
  auto fd = ::socket(AF_NETLINK, SOCK_RAW | SOCK_CLOEXEC, NETLINK_SOCK_DIAG);
  if (fd < 0) {
    int err = errno;
    return Status::RuntimeError("unable to open diagnostic socket",
                                ErrnoToString(err), err);
  }
  fd_ = fd;

  return Status::OK();
}

Status DiagnosticSocket::Close() {
  if (PREDICT_FALSE(fd_ < 0)) {
    return Status::OK();
  }
  const int fd = fd_;
  fd_ = -1;

  Status s;
  if (PREDICT_FALSE(::close(fd) != 0)) {
    const int err = errno;
    s = Status::IOError("error closing diagnostic socket", ErrnoToString(err), err);
  }
  return s;
}

Status DiagnosticSocket::Query(const Sockaddr& socket_src_addr,
                               const Sockaddr& socket_dst_addr,
                               const SocketStates& matching_socket_states,
                               vector<TcpSocketInfo>* info) const {
  DCHECK_GE(fd_, 0) << "requires calling Init() first";
  DCHECK(info);

  uint32_t socket_states_bitmask = 0;
  for (auto state : matching_socket_states) {
    DCHECK_LT(state, 8 * sizeof(decltype(socket_states_bitmask)));
    socket_states_bitmask |= (1U << state);
  }

  RETURN_NOT_OK(SendRequest(
      socket_src_addr, socket_dst_addr, socket_states_bitmask));
  vector<TcpSocketInfo> result;
  RETURN_NOT_OK(ReceiveResponse(&result));
  *info = std::move(result);
  return Status::OK();
}

Status DiagnosticSocket::Query(const Socket& socket,
                               TcpSocketInfo* info) const {
  DCHECK_GE(fd_, 0) << "requires calling Init() first";
  DCHECK(info);

  RETURN_NOT_OK(SendRequest(socket));
  vector<TcpSocketInfo> result;
  RETURN_NOT_OK(ReceiveResponse(&result));
  if (result.empty()) {
    return Status::NotFound("no matching IPv4 TCP socket found");
  }
  if (PREDICT_FALSE(result.size() > 1)) {
    return Status::InvalidArgument("socket address is ambiguous");
  }

  *info = result.front();
  return Status::OK();
}

// Send query about the specified socket.
Status DiagnosticSocket::SendRequest(const Socket& socket) const {
  DCHECK_GE(fd_, 0);
  static constexpr const char* const kNonIpErrMsg =
      "netlink diagnostics is currently supported only on IPv4 TCP sockets";

  Sockaddr src_addr;
  RETURN_NOT_OK(socket.GetSocketAddress(&src_addr));
  if (PREDICT_FALSE(!src_addr.is_ip())) {
    return Status::NotSupported(kNonIpErrMsg);
  }

  Sockaddr dst_addr;
  auto s = socket.GetPeerAddress(&dst_addr);
  if (s.ok()) {
    if (PREDICT_FALSE(!dst_addr.is_ip())) {
      return Status::NotSupported(kNonIpErrMsg);
    }
  } else {
    if (PREDICT_TRUE(s.IsNetworkError() && s.posix_code() == ENOTCONN)) {
      // Assuming it's a listened socket if there isn't a peer at the other side.
      dst_addr = Sockaddr::Wildcard();
    } else {
      return s;
    }
  }

  const uint32_t socket_state_bitmask =
      dst_addr.IsWildcard() ? (1U << SS_LISTEN) : (1U << SS_ESTABLISHED);
  return SendRequest(src_addr, dst_addr, socket_state_bitmask);
}

Status DiagnosticSocket::SendRequest(const Sockaddr& socket_src_addr,
                                     const Sockaddr& socket_dst_addr,
                                     uint32_t socket_states_bitmask) const {
  DCHECK_GE(fd_, 0);
  const in_addr& src_ipv4 = socket_src_addr.ipv4_addr().sin_addr;
  const auto src_port = socket_src_addr.port();
  const in_addr& dst_ipv4 = socket_dst_addr.ipv4_addr().sin_addr;
  const auto dst_port = socket_dst_addr.port();

  constexpr uint32_t kWildcard = static_cast<uint32_t>(-1);
  // All values in inet_diag_sockid are in network byte order.
  const struct inet_diag_sockid sock_id = {
    .idiag_sport = htons(src_port),
    .idiag_dport = htons(dst_port),
    .idiag_src = { src_ipv4.s_addr, 0, 0, 0, },
    .idiag_dst = { dst_ipv4.s_addr, 0, 0, 0, },
    .idiag_if = kWildcard,
    .idiag_cookie = { kWildcard, kWildcard },
  };

  struct TcpSocketRequest {
    struct nlmsghdr nlh;
    struct inet_diag_req_v2 idr;
  } req = {
    .nlh = {
      .nlmsg_len = sizeof(req),
      .nlmsg_type = SOCK_DIAG_BY_FAMILY,
      .nlmsg_flags = NLM_F_REQUEST | NLM_F_MATCH,
    },
    .idr = {
      .sdiag_family = AF_INET,
      .sdiag_protocol = IPPROTO_TCP,
      .idiag_ext = INET_DIAG_MEMINFO,
      .pad = 0,
      .idiag_states = socket_states_bitmask,
      .id = sock_id,
    }
  };

  struct iovec iov = {
    .iov_base = &req,
    .iov_len = sizeof(req),
  };
  struct sockaddr_nl nladdr = {
    .nl_family = AF_NETLINK
  };
  struct msghdr msg = {
    .msg_name = &nladdr,
    .msg_namelen = sizeof(nladdr),
    .msg_iov = &iov,
    .msg_iovlen = 1,
  };

  int rc = -1;
  RETRY_ON_EINTR(rc, ::sendmsg(fd_, &msg, 0));
  if (rc < 0) {
    int err = errno;
    return Status::NetworkError("sendmsg() failed", ErrnoToString(err), err);
  }
  return Status::OK();
}

Status DiagnosticSocket::ReceiveResponse(vector<TcpSocketInfo>* result) const {
  DCHECK_GE(fd_, 0);
  uint8_t buf[8192];
  struct iovec iov = {
    .iov_base = buf,
    .iov_len = sizeof(buf)
  };

  while (true) {
    struct sockaddr_nl nladdr = {};
    struct msghdr msg = {
      .msg_name = &nladdr,
      .msg_namelen = sizeof(nladdr),
      .msg_iov = &iov,
      .msg_iovlen = 1
    };

    ssize_t ret = -1;
    RETRY_ON_EINTR(ret, ::recvmsg(fd_, &msg, 0));
    if (PREDICT_FALSE(ret < 0)) {
      int err = errno;
      return Status::IOError("recvmsg()", ErrnoToString(err), err);
    }
    if (ret == 0) {
      // End of stream.
      return Status::OK();
    }

    const struct nlmsghdr* h = reinterpret_cast<const struct nlmsghdr*>(buf);
    if (PREDICT_FALSE(!NLMSG_OK(h, ret))) {
      return Status::Corruption(
          Substitute("unexpected netlink response size $0", ret));
    }

    if (PREDICT_FALSE(nladdr.nl_family != AF_NETLINK)) {
      return Status::Corruption(Substitute(
          "$0: unexpected address family", static_cast<uint32_t>(nladdr.nl_family)));
    }

    for (; NLMSG_OK(h, ret); h = NLMSG_NEXT(h, ret)) {
      if (h->nlmsg_type == NLMSG_DONE) {
        return Status::OK();
      }
      if (PREDICT_FALSE(h->nlmsg_type == NLMSG_ERROR)) {
        // Below, the NLMSG_DATA(h) macro is expanded and C-style casts replaced
        // with reinterpret_cast<>.
        const struct nlmsgerr* errdata = reinterpret_cast<const struct nlmsgerr*>(
            reinterpret_cast<const uint8_t*>(h) + NLMSG_LENGTH(0));
        if (PREDICT_FALSE(h->nlmsg_len < NLMSG_LENGTH(sizeof(*errdata)))) {
          return Status::Corruption("NLMSG error message is too short");
        }
        const int err = -errdata->error;
        return Status::RuntimeError("netlink error", ErrnoToString(err), err);
      }

      if (PREDICT_FALSE(h->nlmsg_type != SOCK_DIAG_BY_FAMILY)) {
        return Status::Corruption(Substitute("$0: unexpected netlink message type",
                                             static_cast<uint32_t>(h->nlmsg_type)));
      }

      // Below, the NLMSG_DATA(h) macro is expanded and C-style casts replaced
      // with reinterpret_cast<>.
      const struct inet_diag_msg* msg_data = reinterpret_cast<const struct inet_diag_msg*>(
          reinterpret_cast<const uint8_t*>(h) + NLMSG_LENGTH(0));
      const uint32_t msg_size = h->nlmsg_len;
      if (PREDICT_FALSE(msg_size < NLMSG_LENGTH(sizeof(*msg_data)))) {
        return Status::Corruption(Substitute(
            "$0: netlink response is too short", msg_size));
      }
      // Only IPv4 addresses are expected due to the query pattern.
      if (PREDICT_FALSE(msg_data->idiag_family != AF_INET)) {
        return Status::Corruption(Substitute(
            "$0: unexpected address family in netlink response",
            static_cast<uint32_t>(msg_data->idiag_family)));
      }

      DCHECK_LE(SocketState::SS_ESTABLISHED, msg_data->idiag_state);
      DCHECK_GE(SocketState::SS_CLOSING, msg_data->idiag_state);

      TcpSocketInfo info;
      info.state = static_cast<SocketState>(msg_data->idiag_state);
      info.src_addr = msg_data->id.idiag_src[0];  // IPv4 address, network byte order
      info.dst_addr = msg_data->id.idiag_dst[0];  // IPv4 address, network byte order
      info.src_port = msg_data->id.idiag_sport;
      info.dst_port = msg_data->id.idiag_dport;
      info.rx_queue_size = msg_data->idiag_rqueue;
      info.tx_queue_size = msg_data->idiag_wqueue;
      result->emplace_back(info);
    }
  }
  return Status::OK();
}

} // namespace kudu
