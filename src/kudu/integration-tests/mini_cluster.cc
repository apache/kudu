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

#include "kudu/integration-tests/mini_cluster.h"

#include <unistd.h>

#include <cstdint>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"

using std::string;
using strings::Substitute;

namespace kudu {

string MiniCluster::GetBindIpForDaemon(DaemonType type, int index, BindMode bind_mode) {
  switch (bind_mode) {
    case UNIQUE_LOOPBACK: {
      // IP address last octet range: [1 - 254].
      uint8_t last_octet = (type == TSERVER) ? index + 1 : UINT8_MAX - 1 - index;
      CHECK_GE(last_octet, 1);
      CHECK_LE(last_octet, 254);
      pid_t p = getpid();
      CHECK_LE(p, UINT16_MAX) << "Cannot run on systems with >16-bit pid";
      return Substitute("127.$0.$1.$2", p >> 8, p & 0xff, last_octet);
    }
    case WILDCARD:
      return kWildcardIpAddr;
    case LOOPBACK:
      return kLoopbackIpAddr;
    default:
      LOG(FATAL) << bind_mode;
  }
}

} // namespace kudu
