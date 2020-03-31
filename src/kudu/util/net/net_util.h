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

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {

class Sockaddr;

extern const int kServersMaxNum;

static constexpr const char* const kWildcardIpAddr = "0.0.0.0";
static constexpr const char* const kLoopbackIpAddr = "127.0.0.1";

// A container for a host:port pair.
class HostPort {
 public:
  HostPort();
  HostPort(std::string host, uint16_t port);
  explicit HostPort(const Sockaddr& addr);

  bool Initialized() const {
    return !host_.empty();
  }

  // Parse a <host>:<port> pair into this object.
  // If there is no port specified in the string, then 'default_port' is used.
  //
  // Note that <host> cannot be in IPv6 address notation.
  Status ParseString(const std::string& str, uint16_t default_port);

  // Similar to above but allow the address to have scheme and path, e.g.
  //   <host>
  //   <host>:<port>
  //   <fs>://<host>:<port>/<path>
  //
  // Note that both scheme and path are ignored.
  Status ParseStringWithScheme(const std::string& str, uint16_t default_port);

  // Resolve any addresses corresponding to this host:port pair.
  // Note that a host may resolve to more than one IP address.
  //
  // 'addresses' may be NULL, in which case this function simply checks that
  // the host/port pair can be resolved, without returning anything.
  Status ResolveAddresses(std::vector<Sockaddr>* addresses) const;

  std::string ToString() const;

  const std::string& host() const { return host_; }
  void set_host(const std::string& host) { host_ = host; }

  uint16_t port() const { return port_; }
  void set_port(uint16_t port) { port_ = port; }

  size_t HashCode() const;

  // Parse a comma separated list of "host:port" pairs into a vector
  // HostPort objects. If no port is specified for an entry in the
  // comma separated list, 'default_port' is used for that entry's
  // pair.
  static Status ParseStrings(
      const std::string& comma_sep_addrs, uint16_t default_port, std::vector<HostPort>* res);

  // Similar to above but allow the addresses to have scheme and path,
  // which are ignored.
  static Status ParseStringsWithScheme(
      const std::string& comma_sep_addrs, uint16_t default_port, std::vector<HostPort>* res);

  // Takes a vector of HostPort objects and returns a comma separated
  // string containing of "host:port" pairs. This method is the
  // "inverse" of ParseStrings().
  static std::string ToCommaSeparatedString(const std::vector<HostPort>& host_ports);

  // Returns true if addr is within 127.0.0.0/8 range.
  static bool IsLoopback(uint32_t addr);

  // Returns dotted-decimal ('1.2.3.4') representation of IP address in addr.
  static std::string AddrToString(uint32_t addr);

 private:
  std::string host_;
  uint16_t port_;
};

bool operator==(const HostPort& hp1, const HostPort& hp2);

// Hasher of HostPort objects for UnorderedAssociativeContainers.
struct HostPortHasher {
  size_t operator()(const HostPort& hp) const {
    return hp.HashCode();
  }
};

// Equality BinaryPredicate of HostPort objects for UnorderedAssociativeContainers.
struct HostPortEqualityPredicate {
  bool operator()(const HostPort& hp1, const HostPort& hp2) const {
    return hp1 == hp2;
  }
};

typedef std::unordered_set<HostPort, HostPortHasher, HostPortEqualityPredicate>
    UnorderedHostPortSet;

// A container for addr:mask pair.
// Both addr and netmask are in big-endian byte order
// (same as network byte order).
class Network {
 public:
  Network();
  Network(uint32_t addr, uint32_t netmask);

  uint32_t addr() const { return addr_; }

  uint32_t netmask() const { return netmask_; }

  // Returns true if the address is within network.
  bool WithinNetwork(const Sockaddr& addr) const;

  // Returns true if the network is within 127.0.0.0/8 range.
  bool IsLoopback() const;

  // Returns addr part of addr:mask pair as string.
  std::string GetAddrAsString() const;

  // Parses a "addr/netmask" (CIDR notation) pair into this object.
  Status ParseCIDRString(const std::string& addr);

  // Parses a comma separated list of "addr/netmask" (CIDR notation)
  // pairs into a vector of Network objects.
  static Status ParseCIDRStrings(
      const std::string& comma_sep_addrs, std::vector<Network>* res);
 private:
  uint32_t addr_;
  uint32_t netmask_;
};

// Parse and resolve the given comma-separated list of addresses.
//
// The resulting addresses will be resolved, made unique, and added to
// the 'addresses' vector.
//
// Any elements which do not include a port will be assigned 'default_port'.
Status ParseAddressList(const std::string& addr_list,
                        uint16_t default_port,
                        std::vector<Sockaddr>* addresses);

// Return true if the given port is likely to need root privileges to bind to.
bool IsPrivilegedPort(uint16_t port);

// Return the local machine's hostname.
Status GetHostname(std::string* hostname);

// Returns local subnets of all local network interfaces.
Status GetLocalNetworks(std::vector<Network>* net);

// Return the local machine's FQDN.
Status GetFQDN(std::string* hostname);

// Returns a single socket address from a HostPort.
// If the hostname resolves to multiple addresses, returns the first in the
// list and logs a message in verbose mode.
Status SockaddrFromHostPort(const HostPort& host_port, Sockaddr* addr);

// Converts the given Sockaddr into a HostPort, substituting the FQDN
// in the case that the provided address is the wildcard.
//
// In the case of other addresses, the returned HostPort will contain just the
// stringified form of the IP.
Status HostPortFromSockaddrReplaceWildcard(const Sockaddr& addr, HostPort* hp);

// Try to run 'lsof' to determine which process is preventing binding to
// the given 'addr'. If pids can be determined, outputs full 'ps' and 'pstree'
// output for that process.
//
// Output is issued to the log at WARNING level, or appended to 'log' if it
// is non-NULL (mostly useful for testing).
void TryRunLsof(const Sockaddr& addr, std::vector<std::string>* log = nullptr);

// BindMode lets you specify the socket binding mode for RPC and/or HTTP server.
// A) LOOPBACK binds each server to loopback ip address "127.0.0.1".
//
// B) WILDCARD specifies "0.0.0.0" as the ip to bind to, which means sockets
// can be bound to any interface on the local host.
// For example, if a host has two interfaces with addresses
// 192.168.0.10 and 192.168.0.11, the server process can accept connection
// requests addressed to 192.168.0.10 or 192.168.0.11.
//
// C) UNIQUE_LOOPBACK binds each tablet server to a different loopback address.
// This affects the server's RPC server, and also forces the server to
// only use this IP address for outgoing socket connections as well.
// This allows the use of iptables on the localhost to simulate network
// partitions.
//
// The addresses used are 127.<A>.<B>.<C> where:
// - <A,B> are the high and low bytes of the pid of the process running the
//   test (not the daemon itself).
// - <C> is the index of the server within the started test.
//
// This requires that the system is set up such that processes may bind
// to any IP address in the localhost netblock (127.0.0.0/8). This seems
// to be the case on common Linux distributions. You can verify by running
// 'ip addr | grep 127.0.0.1' and checking that the address is listed as
// '127.0.0.1/8'.
//
// Note: UNIQUE_LOOPBACK is not supported on macOS.
//
// Default: UNIQUE_LOOPBACK on Linux, LOOPBACK on macOS.
enum class BindMode {
  UNIQUE_LOOPBACK,
  WILDCARD,
  LOOPBACK
};

// Gets a random port from the ephemeral range by binding to port 0 on address
// 'address' and letting the kernel choose an unused one from the ephemeral port
// range. The socket is then immediately closed and it remains in TIME_WAIT for
// 2*tcp_fin_timeout (by default 2*60=120 seconds). The kernel won't assign this
// port until it's in TIME_WAIT but it can still be used by binding it
// explicitly.
Status GetRandomPort(const std::string& address, uint16_t* port);

#if defined(__APPLE__)
  static constexpr const BindMode kDefaultBindMode = BindMode::LOOPBACK;
#else
  static constexpr const BindMode kDefaultBindMode = BindMode::UNIQUE_LOOPBACK;
#endif

// Return the IP address that the daemon will bind to. If bind_mode is LOOPBACK,
// this will be 127.0.0.1 and if it is WILDCARD it will be 0.0.0.0. Otherwise,
// it is another IP in the local netblock indicated by the given index (which
// should range from (0, 62]). In this UNIQUE_LOOPBACK mode, if the same index
// is given twice, then the same IP address could return when the caller is from
// the same process.
std::string GetBindIpForDaemon(int index, BindMode bind_mode);

} // namespace kudu
