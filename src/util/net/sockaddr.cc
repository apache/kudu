// Copyright (c) 2013, Cloudera, inc.

#include "util/net/sockaddr.h"

#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>

#include "gutil/stringprintf.h"

namespace kudu {

///
/// Sockaddr
///
Sockaddr::Sockaddr() {
  memset(&addr_, 0, sizeof(addr_));
  addr_.sin_family = AF_INET;
  addr_.sin_addr.s_addr = INADDR_ANY;
}

Sockaddr::Sockaddr(const struct sockaddr_in *addr) {
  memcpy(&addr_, addr, sizeof(struct sockaddr_in));
}

Sockaddr& Sockaddr::operator=(const struct sockaddr_in &addr) {
  memcpy(&addr_, &addr, sizeof(struct sockaddr_in));
  return *this;
}

bool Sockaddr::operator==(const Sockaddr& other) const {
  return memcmp(&other.addr_, &addr_, sizeof(addr_)) == 0;
}

bool Sockaddr::operator<(const Sockaddr &rhs) const {
  return addr_.sin_addr.s_addr < rhs.addr_.sin_addr.s_addr;
}

uint32_t Sockaddr::HashCode() const {
  uint32_t ret = addr_.sin_addr.s_addr;
  ret ^= (addr_.sin_port * 7919);
  return ret;
}

void Sockaddr::set_port(int port) {
  addr_.sin_port = htons(port);
}

int Sockaddr::port() const {
  return ntohs(addr_.sin_port);
}

const struct sockaddr_in& Sockaddr::addr() const {
  return addr_;
}

std::string Sockaddr::ToString() const {
  char str[INET_ADDRSTRLEN];
  ::inet_ntop(AF_INET, &addr_.sin_addr, str, INET_ADDRSTRLEN);
  return StringPrintf("%s:%d", str, port());
}

} // namespace kudu
