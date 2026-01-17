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
#include <sys/un.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <limits>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/hash/string_hash.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/int128.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

///
/// Sockaddr
///
Sockaddr::Sockaddr() {
  set_length(0);
}

Sockaddr::Sockaddr(const Sockaddr& other) noexcept {
  *this = other;
}

Sockaddr::~Sockaddr() {
  ASAN_UNPOISON_MEMORY_REGION(&storage_, sizeof(storage_));
}

Sockaddr Sockaddr::Wildcard(sa_family_t family) {
  // IPv4.
  if (family == AF_INET) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    return Sockaddr(addr);
  }

  // IPv6.
  DCHECK(family == AF_INET6);
  struct sockaddr_in6 addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin6_family = AF_INET6;
  addr.sin6_addr = IN6ADDR_ANY_INIT;
  return Sockaddr(addr);
}

Sockaddr Sockaddr::Loopback(sa_family_t family) {
  // IPv4.
  if (family == AF_INET) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    return Sockaddr(addr);
  }

  // IPv6.
  DCHECK(family == AF_INET6);
  struct sockaddr_in6 addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin6_family = AF_INET6;
  inet_pton(AF_INET6, "::1", &addr.sin6_addr);
  return Sockaddr(addr);
}

Sockaddr& Sockaddr::operator=(const Sockaddr& other) noexcept {
  if (&other == this) {
    return *this;
  }
  set_length(other.len_);
  memcpy(&storage_, &other.storage_, len_);
  return *this;
}

Sockaddr::Sockaddr(const struct sockaddr& addr, socklen_t len) {
  set_length(len);
  memcpy(&storage_, &addr, len);
  if (family() == AF_INET6) {
    storage_.in6.sin6_flowinfo = 0; // Flow-label & traffic class
    storage_.in6.sin6_scope_id = 0; // Scope identifier
  }
}

// When storing sockaddr_in, do not count the padding sin_zero field in the
// length of Sockaddr::storage_ since the padding might contain irrelevant
// non-zero bytes that should not be passed along with the rest of the valid
// and properly initialized sockaddr_in's fields to BytewiseCompare() and
// HashCode().
Sockaddr::Sockaddr(const struct sockaddr_in& addr) :
    Sockaddr(reinterpret_cast<const struct sockaddr&>(addr),
             offsetof(struct sockaddr_in, sin_zero)) {
  DCHECK_EQ(AF_INET, addr.sin_family);
}

Sockaddr::Sockaddr(const struct sockaddr_in6& addr) :
    Sockaddr(reinterpret_cast<const struct sockaddr&>(addr), sizeof(addr)) {
  DCHECK_EQ(AF_INET6, addr.sin6_family);
  static_assert(sizeof(struct sockaddr_storage) >= sizeof(struct sockaddr_in6),
      "Structure not large enough to hold IPv6 socket address");
}

Status Sockaddr::ParseString(const string& s, uint16_t default_port) {
  HostPort hp;
  RETURN_NOT_OK(hp.ParseString(s, default_port));
  return ParseFromNumericHostPort(hp);
}

Status Sockaddr::ParseFromNumericHostPort(const HostPort& hp) {
  sa_family_t fam = hp.host().find(':') == string::npos ? AF_INET : AF_INET6;
  if (AF_INET == fam) {
    struct in_addr in_addr_local;
    if (inet_pton(AF_INET, hp.host().c_str(), &in_addr_local) != 1) {
      return Status::InvalidArgument(Substitute("invalid IPv4 address $0", hp.host()));
    }
    // Do not count the padding sin_zero field in the length of Sockaddr::storage_
    // since the padding might contain irrelevant non-zero bytes that should not
    // be passed along with the rest of the valid and properly initialized
    // sockaddr_in's fields to BytewiseCompare() and HashCode().
    constexpr auto len = offsetof(struct sockaddr_in, sin_zero);

    // Fields inside storage_ need to be set after length is set. This is to avoid any
    // ASAN failures that can arise due to assginment of storage_ fields still residing
    // in poisoned memory. set_length() adjusts the poisoned region and avoids such case.
    set_length(len);
#ifndef __linux__
    static_assert(len > offsetof(struct sockaddr_in, sin_len));
    storage_.in.sin_len = sizeof(struct sockaddr_in);
#endif
    static_assert(len > offsetof(struct sockaddr_in, sin_family));
    storage_.in.sin_family = fam;
    set_port(hp.port());
    static_assert(len > offsetof(struct sockaddr_in, sin_addr));
    storage_.in.sin_addr = in_addr_local;
    return Status::OK();
  }

  DCHECK(AF_INET6 == fam);
  struct in6_addr sin6_addr_local;
  if (inet_pton(AF_INET6, hp.host().c_str(), &sin6_addr_local) != 1) {
    return Status::InvalidArgument(Substitute("invalid IPv6 address $0", hp.host()));
  }
  // Fields inside storage_ need to be set after length is set. This is to avoid any
  // ASAN failures that can arise due to assginment of storage_ fields still residing
  // in poisoned memory. set_length() adjusts the poisoned region and avoids such case.
  set_length(sizeof(struct sockaddr_in6));
#ifndef __linux__
  storage_.in6.sin6_len = sizeof(struct sockaddr_in6);
#endif
  storage_.in6.sin6_family = fam;
  set_port(hp.port());
  storage_.in6.sin6_flowinfo = 0; // Flow-label & traffic class
  storage_.in6.sin6_addr = sin6_addr_local;
  storage_.in6.sin6_scope_id = 0; // Scope identifier
  return Status::OK();
}

Status Sockaddr::ParseUnixDomainPath(const string& s) {
  constexpr auto kMaxPathSize = SIZEOF_MEMBER(struct sockaddr_un, sun_path);
  if (s[0] == '@') {
    // Specify a path in the abstract namespace.
    if (s.size() > kMaxPathSize) {
      return Status::InvalidArgument(Substitute(
          "UNIX domain socket path exceeds maximum length $0", kMaxPathSize));
    }
    set_length(offsetof(struct sockaddr_un, sun_path) + s.size());
    storage_.un.sun_family = AF_UNIX;
    memcpy(storage_.un.sun_path, s.data(), s.size());
    storage_.un.sun_path[0] = '\0';
  } else {
    // Path names must be null-terminated. The null-terminated length
    // may not match the length s.size().
    int c_len = strlen(s.c_str());
    if (c_len != s.size()) {
      return Status::InvalidArgument("UNIX domain socket path must not contain null bytes");
    }
    // Per unix(7) the length of the path name, including the terminating null
    // byte, should not exceed the size of sun_path.
    if (c_len + 1 > kMaxPathSize) {
      return Status::InvalidArgument(Substitute(
          "UNIX domain socket path exceeds maximum length $0", kMaxPathSize));
    }
    // unix(7) says the addrlen can be specified as the full length of the
    // structure.
    set_length(sizeof(struct sockaddr_un));
    storage_.un.sun_family = AF_UNIX;
    memcpy(storage_.un.sun_path, s.c_str(), c_len + 1);
  }
  return Status::OK();
}

string Sockaddr::UnixDomainPath() const {
  CHECK_EQ(family(), AF_UNIX);
  switch  (unix_address_type()) {
    case UnixAddressType::kUnnamed:
      return "<unnamed>";
    case UnixAddressType::kPath:
      return string(storage_.un.sun_path);
    case UnixAddressType::kAbstractNamespace:
      size_t len = len_ - offsetof(struct sockaddr_un, sun_path) - 1;
      return "@" + string(storage_.un.sun_path + 1, len);
  }
  LOG(FATAL) << "unknown unix address type";
  return "";
}

Sockaddr::UnixAddressType Sockaddr::unix_address_type() const {
  CHECK_EQ(family(), AF_UNIX);
  if (len_ == sizeof(sa_family_t)) {
    return UnixAddressType::kUnnamed;
  }
  if (storage_.un.sun_path[0] == '\0') {
    return UnixAddressType::kAbstractNamespace;
  }
  return UnixAddressType::kPath;
}

Sockaddr& Sockaddr::operator=(const struct sockaddr_in &addr) {
  // Do not count the padding sin_zero field since it contains irrelevant bytes
  // that should not be passed along with the rest of the valid and properly
  // initialized fields into storage_.
  constexpr auto len = offsetof(struct sockaddr_in, sin_zero);
  set_length(len);
  memcpy(&storage_, &addr, len);
  DCHECK_EQ(family(), AF_INET);
  return *this;
}

Sockaddr& Sockaddr::operator=(const struct sockaddr_in6& addr) {
  set_length(sizeof(addr));
  memcpy(&storage_, &addr, len_);
  storage_.in6.sin6_flowinfo = 0; // Flow-label & traffic class
  storage_.in6.sin6_scope_id = 0; // Scope identifier
  DCHECK_EQ(family(), AF_INET6);
  return *this;
}

bool Sockaddr::operator==(const Sockaddr& other) const {
  return BytewiseCompare(*this, other) == 0;
}

int Sockaddr::BytewiseCompare(const Sockaddr& a, const Sockaddr& b) {
  Slice a_slice(reinterpret_cast<const uint8_t*>(&a.storage_), a.len_);
  Slice b_slice(reinterpret_cast<const uint8_t*>(&b.storage_), b.len_);
  return a_slice.compare(b_slice);
}

void Sockaddr::set_length(socklen_t len) {
  DCHECK(len == 0 || len >= sizeof(sa_family_t));
  DCHECK_LE(len, sizeof(storage_));
  len_ = len;
  ASAN_UNPOISON_MEMORY_REGION(&storage_, len_);
  ASAN_POISON_MEMORY_REGION(reinterpret_cast<uint8_t*>(&storage_) + len_,
                            sizeof(storage_) - len_);
}

size_t Sockaddr::HashCode() const {
  return HashStringThoroughly(reinterpret_cast<const char*>(&storage_), len_);
}

void Sockaddr::set_port(uint16_t port) {
  DCHECK(is_ip());
  DCHECK_GE(port, 0);
  DCHECK_LE(port, std::numeric_limits<uint16_t>::max());
  if (AF_INET == family()) {
    storage_.in.sin_port = htons(port);
  } else {
    storage_.in6.sin6_port = htons(port);
  }
}

uint16_t Sockaddr::port() const {
  DCHECK(is_ip());
  if (AF_INET == family()) {
    return ntohs(storage_.in.sin_port);
  }
  return ntohs(storage_.in6.sin6_port);
}

std::string Sockaddr::host() const {
  switch (family()) {
    case AF_INET:
      return HostPort::AddrToString(&(storage_.in.sin_addr.s_addr), AF_INET);
    case AF_INET6:
      return HostPort::AddrToString(&(storage_.in6.sin6_addr.s6_addr), AF_INET6);
    case AF_UNIX:
      DCHECK(false) << "unexpected host() call on unix socket";
      // In case we missed a host() call somewhere in a vlog or error message not
      // covered by tests, better to at least return some string here.
      return "<unix socket>";
    default:
      DCHECK(false) << "unexpected host() call on socket with family " << family();
      return "<unknown socket type>";
  }
}

const struct sockaddr_in& Sockaddr::ipv4_addr() const {
  DCHECK_EQ(family(), AF_INET);
  return storage_.in;
}

const struct sockaddr_in6& Sockaddr::ipv6_addr() const {
  DCHECK_EQ(family(), AF_INET6);
  return storage_.in6;
}

std::string Sockaddr::ToString() const {
  if (!is_initialized()) {
    return "<uninitialized>";
  }
  switch (family()) {
    case AF_INET:
      return Substitute("$0:$1", host(), port());
    case AF_INET6:
      return Substitute("[$0]:$1", host(), port());
    case AF_UNIX:
      return Substitute("unix:$0", UnixDomainPath());
    default:
      return "<invalid sockaddr>";
  }
}

bool Sockaddr::IsWildcard() const {
  DCHECK(is_ip());
  if (AF_INET == family()) {
    return 0 == storage_.in.sin_addr.s_addr;
  }
  return IN6_IS_ADDR_UNSPECIFIED(&storage_.in6.sin6_addr);
}

bool Sockaddr::IsAnyLocalAddress() const {
  if (AF_UNIX == family()) {
    return true;
  }

  uint128_t addr = 0;
  if (AF_INET == family()) {
    addr = storage_.in.sin_addr.s_addr;
  } else {
    memcpy(&addr, storage_.in6.sin6_addr.s6_addr, sizeof(addr));
  }
  return HostPort::IsLoopback(addr, family());
}

Status Sockaddr::LookupHostname(string* hostname) const {
  char host[NI_MAXHOST];
  int flags = 0;

  auto* addr = reinterpret_cast<const sockaddr*>(&storage_);
  int rc = 0;
  LOG_SLOW_EXECUTION(WARNING, 200,
                     Substitute("DNS reverse-lookup for $0", ToString())) {
    rc = getnameinfo(addr, addrlen(), host, NI_MAXHOST, nullptr, 0, flags);
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

string Sockaddr::ToCommaSeparatedString(const std::vector<Sockaddr>& addrs) {
  vector<string> addrs_str;
  addrs_str.reserve(addrs.size());
  for (const Sockaddr& addr : addrs) {
    addrs_str.push_back(addr.ToString());
  }
  return JoinStrings(addrs_str, ",");
}

} // namespace kudu
