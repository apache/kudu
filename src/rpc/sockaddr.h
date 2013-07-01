// Copyright (c) 2013, Cloudera, inc.

#ifndef KUDU_RPC_SOCKADDR_H
#define KUDU_RPC_SOCKADDR_H

#include <netinet/in.h>
#include <iosfwd>
#include <string>

namespace kudu {
namespace rpc {

///
/// Represents a sockaddr.
///
/// Currently only IPv4 is implemented.  When IPv6 and UNIX domain are
/// implemented, this should become an abstract base class and those should be
/// multiple implementations.
///
class Sockaddr {
 public:
  explicit Sockaddr();
  explicit Sockaddr(const struct sockaddr_in *addr);

  Sockaddr& operator=(const struct sockaddr_in &addr);

  // Compare the endpoints of two sockaddrs.
  // The port number is ignored in this comparison.
  bool operator<(const Sockaddr &rhs) const;

  uint32_t HashCode() const;

  void set_port(int port);
  int port() const;
  const struct sockaddr_in& addr() const;
  std::string ToString() const;

  // the default auto-generated copy constructor is fine here
 private:
  struct sockaddr_in addr_;
};

}
}

#endif  
