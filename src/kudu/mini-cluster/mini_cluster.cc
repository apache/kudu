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

string MiniCluster::GetBindIpForDaemonWithType(DaemonType type,
                                               int index,
                                               BindMode bind_mode) {
  int idx;
  // Partition the index space 'kServersMaxNum' into three portions, one for each
  // daemon type, to get unique address. If a daemon index spans over the portion
  // reserved for another type, then duplicate address can be generated. Though this
  // should be enough for our current tests.
  switch (type) {
    case MASTER:
      idx = kServersMaxNum - index;
      break;
    case TSERVER:
      idx = index + 1;
      break;
    case EXTERNAL_SERVER:
      idx = kServersMaxNum / 3 + index;
      break;
    default:
      LOG(FATAL) << type;
  }
  return GetBindIpForDaemon(idx, bind_mode);
}

Status MiniCluster::ReserveDaemonSocket(DaemonType type,
                                        int index,
                                        BindMode bind_mode,
                                        unique_ptr<Socket>* socket) {
  string ip = GetBindIpForDaemonWithType(type, index, bind_mode);
  Sockaddr sock_addr;
  RETURN_NOT_OK(sock_addr.ParseString(ip, 0));

  unique_ptr<Socket> sock(new Socket());
  RETURN_NOT_OK(sock->Init(sock_addr.family(), 0));
  RETURN_NOT_OK(sock->SetReuseAddr(true));
  RETURN_NOT_OK(sock->SetReusePort(true));
  RETURN_NOT_OK(sock->Bind(sock_addr));
  *socket = std::move(sock);
  return Status::OK();
}

} // namespace cluster
} // namespace kudu
