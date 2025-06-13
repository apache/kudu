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

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

class DiagnosticSocketTest : public KuduTest {
 protected:
  Socket listener_;
  Sockaddr listen_addr_;

  Status BindAndListen(const string& addr_str, uint16_t port, int listen_backlog = 1) {
    Sockaddr address;
    RETURN_NOT_OK(address.ParseString(addr_str, port));
    return BindAndListen(address, listen_backlog);
  }

  Status BindAndListen(const Sockaddr& address, int listen_backlog) {
    RETURN_NOT_OK(listener_.Init(address.family(), 0));
    RETURN_NOT_OK(listener_.BindAndListen(address, listen_backlog));
    return listener_.GetSocketAddress(&listen_addr_);
  }

  void GetListeningSocketInfo(const string& ip_addr, DiagnosticSocket::TcpSocketInfo* info) {
    constexpr uint16_t kPort = 56789;
    constexpr int kListenBacklog = 8;

    ASSERT_OK(BindAndListen(ip_addr, kPort, kListenBacklog));

    DiagnosticSocket ds;
    ASSERT_OK(ds.Init());
    ASSERT_OK(ds.Query(listener_, info));

    // Make sure the result matches the input parameters.
    ASSERT_EQ(kPort, ntohs(info->src_port));
    ASSERT_EQ(0, ntohs(info->dst_port));
    ASSERT_EQ(DiagnosticSocket::SS_LISTEN, info->state);

    // TX queue size for a listening socket is the size of the backlog queue.
    ASSERT_EQ(kListenBacklog, info->tx_queue_size);

    // Nothing is connecting to the listen port: no pending connections expected.
    ASSERT_EQ(0, info->rx_queue_size);
  }

  void MatchSimplePattern(const string& ip_addr, sa_family_t family) {
    // Open a socket, bind and listen, and then close it. This is just to make
    // sure the socket has valid address, but there is no open socket at the
    // specified address.
    DCHECK(family == AF_INET || family == AF_INET6);
    constexpr uint16_t kPort = 56789;
    constexpr int kListenBacklog = 5;
    ASSERT_OK(BindAndListen(ip_addr, kPort, kListenBacklog));

    const auto& src_addr = listen_addr_;
    const auto& dst_addr = Sockaddr::Wildcard(family);
    const DiagnosticSocket::SocketStates socket_states{ DiagnosticSocket::SS_LISTEN };

    DiagnosticSocket ds;
    ASSERT_OK(ds.Init());

    // Use a pattern to match only the listened server socket.
    {
      vector<DiagnosticSocket::TcpSocketInfo> info;
      // The query should return success.
      ASSERT_OK(ds.Query(src_addr, dst_addr, socket_states, &info));
      ASSERT_EQ(1, info.size());
      const auto& entry = info.front();

      // Make sure the result matches the input parameters.
      if (family == AF_INET) {
        ASSERT_EQ(src_addr.ipv4_addr().sin_addr.s_addr, entry.src_addr[0]);
        ASSERT_EQ(INADDR_ANY, entry.dst_addr[0]);
      } else {
        ASSERT_FALSE(memcmp(src_addr.ipv6_addr().sin6_addr.s6_addr,
                            entry.src_addr, sizeof(entry.src_addr)));
        ASSERT_FALSE(memcmp(&in6addr_any, entry.dst_addr, sizeof(entry.dst_addr)));
      }
      ASSERT_EQ(kPort, ntohs(entry.src_port));
      ASSERT_EQ(0, ntohs(entry.dst_port));
      ASSERT_EQ(DiagnosticSocket::SS_LISTEN, entry.state);

      // Verify the expected statistics on the server socket.
      ASSERT_EQ(0, entry.rx_queue_size); // no pending connections
      ASSERT_EQ(kListenBacklog, entry.tx_queue_size);
    }

    // Use a pattern to match any IPv4 or IPv6 TCP socket.
    {
      const auto& addr_wildcard = Sockaddr::Wildcard(family);
      const auto& state_wildcard = DiagnosticSocket::SocketStateWildcard();
      vector<DiagnosticSocket::TcpSocketInfo> info;
      // The query should return success.
      ASSERT_OK(ds.Query(addr_wildcard, addr_wildcard, state_wildcard, &info));
      ASSERT_GE(info.size(), 1);

      const auto compare_addresses = [&](DiagnosticSocket::TcpSocketInfo entry) {
        if (family == AF_INET) {
          // IPv4 comparison
          return (src_addr.ipv4_addr().sin_addr.s_addr == entry.src_addr[0] &&
                  INADDR_ANY == entry.dst_addr[0]);
        }
        // IPv6 comparison
        return (memcmp(src_addr.ipv6_addr().sin6_addr.s6_addr,
                       entry.src_addr, sizeof(entry.src_addr)) == 0 &&
                memcmp(&in6addr_any, entry.dst_addr,
                       sizeof(entry.dst_addr)) == 0);
      };

      // Make sure the server's socket is one of the reported ones.
      size_t matched_entries = 0;
      for (const auto& entry : info) {
        if (!compare_addresses(entry) ||
            kPort != ntohs(entry.src_port) ||
            0 != ntohs(entry.dst_port) ||
            DiagnosticSocket::SS_LISTEN != entry.state) {
          continue;
        }
        ++matched_entries;
      }
      ASSERT_EQ(1, matched_entries);
    }

    // Close the socket; the socket's address in listen_addr_ still valid.
    ASSERT_OK(listener_.Close());

    {
      vector<DiagnosticSocket::TcpSocketInfo> info;
      // The query should return success.
      ASSERT_OK(ds.Query(src_addr, dst_addr, socket_states, &info));
      // However, the list of matching sockets should be empty since the socket
      // that could match the pattern has been just closed.
      ASSERT_TRUE(info.empty());
    }
  }
};

TEST_F(DiagnosticSocketTest, Basic) {
  DiagnosticSocket ds;
  // Make sure it's possible to create a netlink socket.
  ASSERT_OK(ds.Init());
  // Call Close() on the socket explicitly.
  ASSERT_OK(ds.Close());
}

TEST_F(DiagnosticSocketTest, ListeningSocketIpV4) {
  DiagnosticSocket::TcpSocketInfo info;

  NO_FATALS(GetListeningSocketInfo("127.254.254.254", &info));

  // Make sure the result matches the input parameters.
  ASSERT_EQ(listen_addr_.ipv4_addr().sin_addr.s_addr, info.src_addr[0]);
  ASSERT_EQ(INADDR_ANY, info.dst_addr[0]);
}

TEST_F(DiagnosticSocketTest, SimplePatternIpV4) {
  MatchSimplePattern("127.254.254.254", AF_INET);
}

TEST_F(DiagnosticSocketTest, ListeningSocketIpV6) {
  DiagnosticSocket::TcpSocketInfo info;

  NO_FATALS(GetListeningSocketInfo("::1", &info));

  // Make sure the result matches the input parameters.
  ASSERT_EQ(0, memcmp(listen_addr_.ipv6_addr().sin6_addr.s6_addr,
                      info.src_addr, sizeof(info.src_addr)));
  ASSERT_EQ(0, memcmp(in6addr_any.s6_addr, info.dst_addr,
                      sizeof(info.dst_addr)));
}

TEST_F(DiagnosticSocketTest, SimplePatternIpV6) {
  MatchSimplePattern("::1", AF_INET6);
}

} // namespace kudu
