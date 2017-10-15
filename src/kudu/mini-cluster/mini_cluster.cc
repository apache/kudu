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

#include "kudu/mini-cluster/mini_cluster.h"

#include <unistd.h>

#include <cstdint>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"

using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace cluster {

string MiniCluster::GetBindIpForDaemon(DaemonType type, int index, BindMode bind_mode) {
  static const int kPidBits = 18;
  static const int kServerIdxBits = 24 - kPidBits;
  static const int kServersMaxNum = (1 << kServerIdxBits) - 2;
  CHECK(0 <= index && index < kServersMaxNum) << Substitute(
      "server index $0 is not in range [$1, $2)", index, 0, kServersMaxNum);

  switch (bind_mode) {
    case UNIQUE_LOOPBACK: {
      uint32_t pid = getpid();
      CHECK_LT(pid, 1 << kPidBits) << Substitute(
          "PID $0 is more than $1 bits wide", pid, kPidBits);
      int idx = (type == TSERVER) ? index + 1 : kServersMaxNum - index;
      uint32_t ip = (pid << kServerIdxBits) | static_cast<uint32_t>(idx);
      uint8_t octets[] = {
          static_cast<uint8_t>((ip >> 16) & 0xff),
          static_cast<uint8_t>((ip >>  8) & 0xff),
          static_cast<uint8_t>((ip >>  0) & 0xff),
      };
      // Range for the last octet of a valid unicast IP address is (0, 255).
      CHECK(0 < octets[2] && octets[2] < UINT8_MAX) << Substitute(
          "last IP octet $0 is not in range ($1, $2)", octets[2], 0, UINT8_MAX);
      return Substitute("127.$0.$1.$2", octets[0], octets[1], octets[2]);
    }
    case WILDCARD:
      return kWildcardIpAddr;
    case LOOPBACK:
      return kLoopbackIpAddr;
    default:
      LOG(FATAL) << bind_mode;
  }
}

Status MiniCluster::ReserveDaemonSocket(DaemonType type,
                                        int index,
                                        BindMode bind_mode,
                                        unique_ptr<Socket>* socket) {
  string ip = GetBindIpForDaemon(type, index, bind_mode);
  Sockaddr sock_addr;
  RETURN_NOT_OK(sock_addr.ParseString(ip, 0));

  unique_ptr<Socket> sock(new Socket());
  RETURN_NOT_OK(sock->Init(0));
  RETURN_NOT_OK(sock->SetReuseAddr(true));
  RETURN_NOT_OK(sock->SetReusePort(true));
  RETURN_NOT_OK(sock->Bind(sock_addr));
  *socket = std::move(sock);
  return Status::OK();
}

} // namespace cluster
} // namespace kudu
