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

#pragma once

#include <cstdint>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

namespace kudu {

class Sockaddr;
class Socket;

// A wrapper around Linux-specific sock_diag() API [1] based on the
// netlink facility [2] to fetch information on IPv4 TCP sockets.
//
// [1] https://man7.org/linux/man-pages/man7/sock_diag.7.html
// [2] https://man7.org/linux/man-pages/man7/netlink.7.html
class DiagnosticSocket final {
 public:
  // Enum for the socket state. This is modeled after the corresponding
  // TCP_-prefixed enum in /usr/include/netinet/tcp.h with exact value mapping.
  // This enum is introduced to decouple the netinet/tcp.h header and the API
  // of this class.
  enum SocketState {
    SS_UNKNOWN = 0,
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
    SS_MAX,
  };

  // Diagnostic information on a TCP IPv4 socket. That's a subset of the
  // information available via the netlink data structures.
  //
  // TODO(aserbin): if using this API more broadly than fetching information on
  //                a single socket, consider replacing { addr, port } pairs for
  //                the source and the destination with Sockaddr class fields.
  struct TcpSocketInfo {
    SocketState state;      // current state of the socket
    uint32_t src_addr;      // IPv4 source address (network byte order)
    uint32_t dst_addr;      // IPv4 destination address (network byte order)
    uint16_t src_port;      // source port number (network byte order)
    uint16_t dst_port;      // destination port number (network byte order)
    uint32_t rx_queue_size; // RX queue size
    uint32_t tx_queue_size; // TX queue size
  };

  // Return wildcard for all the available socket states.
  static const std::vector<SocketState>& SocketStateWildcard();

  // Construct an object.
  DiagnosticSocket();

  // Close the diagnostic socket. Errors will be logged, but ignored.
  ~DiagnosticSocket();

  // Open the diagnostic socket of the NETLINK_SOCK_DIAG protocol in the
  // AF_NETLINK domain, so it's possible to fetch the requested information
  // from the kernel using the netlink facility via the API of this class.
  Status Init() WARN_UNUSED_RESULT;

  // Whether this wrapper has been initialized: the underlying netlink socket
  // successfully opened, etc.
  bool IsInitialized() const { return fd_ >= 0; }

  // Close the Socket, checking for errors.
  Status Close();

  // Get diagnostic information on IPv4 TCP sockets of the specified states
  // having the specified source and the destination address. Wildcard addresses
  // are supported.
  Status Query(const Sockaddr& socket_src_addr,
               const Sockaddr& socket_dst_addr,
               const std::vector<SocketState>& socket_states,
               std::vector<TcpSocketInfo>* info) const;

  // Get diagnostic information on the specified socket. This is a handy
  // shortcut to the Query() method above for a single active socket in the
  // SS_ESTABLISHED or SS_LISTEN.
  Status Query(const Socket& socket, TcpSocketInfo* info) const;

 private:
  // Build and send netlink request, writing it into the diagnostic socket.
  Status SendRequest(const Socket& socket) const;
  Status SendRequest(const Sockaddr& socket_src_addr,
                     const Sockaddr& socket_dst_addr,
                     uint32_t socket_states_bitmask) const;

  // Receive response for a request sent by a method above.
  Status ReceiveResponse(std::vector<TcpSocketInfo>* result) const;

  // File descriptor of the diagnostic socket (AF_NETLINK domain).
  int fd_;

  DISALLOW_COPY_AND_ASSIGN(DiagnosticSocket);
};

} // namespace kudu
