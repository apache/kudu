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

#include "kudu/util/net/sockaddr.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <string>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/hash/builtin_type_hash.h"
#include "kudu/gutil/hash/hash128to64.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/stopwatch.h"

using std::string;
using strings::Substitute;

namespace kudu {

///
/// Sockaddr
///
Sockaddr::Sockaddr() {
  memset(&addr_, 0, sizeof(addr_));
  addr_.sin6_family = AF_INET6;
  addr_.sin6_addr = in6addr_any;
}

Sockaddr::Sockaddr(const struct sockaddr_in6& addr) {
  memcpy(&addr_, &addr, sizeof(struct sockaddr_in6));
}

Status Sockaddr::ParseString(const std::string& s, uint16_t default_port) {
  HostPort hp;
  RETURN_NOT_OK(hp.ParseString(s, default_port));

  if (inet_pton(AF_INET6, hp.host().c_str(), &addr_.sin6_addr) != 1) {
    return Status::InvalidArgument("Invalid IP address", hp.host());
  }
  set_port(hp.port());
  return Status::OK();
}

Sockaddr& Sockaddr::operator=(const struct sockaddr_in6 &addr) {
  memcpy(&addr_, &addr, sizeof(struct sockaddr_in6));
  return *this;
}

bool Sockaddr::operator==(const Sockaddr& other) const {
  return memcmp(&other.addr_, &addr_, sizeof(addr_)) == 0;
}

bool Sockaddr::operator<(const Sockaddr &rhs) const {
  return NetworkByteOrder::Load128(addr_.sin6_addr.s6_addr) <
      NetworkByteOrder::Load128(rhs.addr_.sin6_addr.s6_addr);
}

uint64 Sockaddr::HashCode() const {
  // Note: IPv6 addresses are 128 bits.
  uint64 hash = Hash128to64(
      NetworkByteOrder::Load128(addr_.sin6_addr.s6_addr));
  hash = Hash64NumWithSeed(addr_.sin6_port, hash);
  return hash;
}

void Sockaddr::set_port(int port) {
  addr_.sin6_port = htons(port);
}

int Sockaddr::port() const {
  return ntohs(addr_.sin6_port);
}

std::string Sockaddr::host() const {
  char str[INET6_ADDRSTRLEN];
  ::inet_ntop(AF_INET6, &addr_.sin6_addr, str, INET6_ADDRSTRLEN);
  return str;
}

const struct sockaddr_in6& Sockaddr::addr() const {
  return addr_;
}

std::string Sockaddr::ToString() const {
  return Substitute("$0:$1", host(), port());
}

bool Sockaddr::IsWildcard() const {
  return addr_.sin6_addr.s6_addr == 0;
}

bool Sockaddr::IsAnyLocalAddress() const {
  return (NetworkByteOrder::Load128(addr_.sin6_addr.s6_addr) == 1);
}

Status Sockaddr::LookupHostname(string* hostname) const {
  char host[NI_MAXHOST];
  int flags = 0;

  int rc;
  LOG_SLOW_EXECUTION(WARNING, 200,
                     Substitute("DNS reverse-lookup for $0", ToString())) {
    rc = getnameinfo((struct sockaddr *) &addr_, sizeof(sockaddr_in6),
                     host, NI_MAXHOST,
                     nullptr, 0, flags);
  }
  if (PREDICT_FALSE(rc != 0)) {
    if (rc == EAI_SYSTEM) {
      int errno_saved = errno;
      return Status::NetworkError(Substitute("getnameinfo: $0", gai_strerror(rc)),
                                  strerror(errno_saved), errno_saved);
    }
    return Status::NetworkError("getnameinfo", gai_strerror(rc), rc);
  }
  *hostname = host;
  return Status::OK();
}

} // namespace kudu
