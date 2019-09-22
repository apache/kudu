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

#include "kudu/clock/test/mini_chronyd_test_util.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "kudu/clock/test/mini_chronyd.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"

using kudu::cluster::MiniCluster;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace clock {

namespace {

// Reserve and bind port for NTP server socket, returning the bound address
// and port. The 'socket' object has outer lifecycle: it's is necessary
// to keep the port reserved.
Status ReservePort(int index, unique_ptr<Socket>* socket,
                   string* address, uint16_t* port) {
  RETURN_NOT_OK(MiniCluster::ReserveDaemonSocket(
      MiniCluster::EXTERNAL_SERVER, index, kDefaultBindMode, socket));
  Sockaddr addr;
  RETURN_NOT_OK((*socket)->GetSocketAddress(&addr));
  *address = addr.host();
  *port = static_cast<uint16_t>(addr.port());
  return Status::OK();
}

} // anonymous namespace

Status StartChronydAtAutoReservedPort(unique_ptr<MiniChronyd>* chronyd,
                                      MiniChronydOptions* options) {
  MiniChronydOptions opts;
  if (options) {
    opts = *options;
  }
  unique_ptr<Socket> socket;
  RETURN_NOT_OK(ReservePort(opts.index, &socket, &opts.bindaddress, &opts.port));
  chronyd->reset(new MiniChronyd(opts));
  RETURN_NOT_OK((*chronyd)->Start());
  if (options) {
    *options = std::move(opts);
  }
  return Status::OK();
}

} // namespace clock
} // namespace kudu
