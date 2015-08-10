// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <tr1/unordered_set>
#include <utility>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/errno.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/subprocess.h"

using std::tr1::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {
struct AddrinfoDeleter {
  void operator()(struct addrinfo* info) {
    freeaddrinfo(info);
  }
};
}

HostPort::HostPort()
  : host_(""),
    port_(0) {
}

HostPort::HostPort(const std::string& host, uint16_t port)
  : host_(host),
    port_(port) {
}

HostPort::HostPort(const Sockaddr& addr)
  : host_(addr.host()),
    port_(addr.port()) {
}

Status HostPort::ParseString(const string& str, uint16_t default_port) {
  std::pair<string, string> p = strings::Split(str, strings::delimiter::Limit(":", 1));

  // Strip any whitespace from the host.
  StripWhiteSpace(&p.first);

  // Parse the port.
  uint32_t port;
  if (p.second.empty() && strcount(str, ':') == 0) {
    // No port specified.
    port = default_port;
  } else if (!SimpleAtoi(p.second, &port) ||
             port > 65535) {
    return Status::InvalidArgument("Invalid port", str);
  }

  host_.swap(p.first);
  port_ = port;
  return Status::OK();
}

Status HostPort::ResolveAddresses(vector<Sockaddr>* addresses) const {
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* res = NULL;
  int rc;
  LOG_SLOW_EXECUTION(WARNING, 200,
                     Substitute("resolving address for $0", host_)) {
    rc = getaddrinfo(host_.c_str(), NULL, &hints, &res);
  }
  if (rc != 0) {
    return Status::NetworkError(
      StringPrintf("Unable to resolve address '%s'", host_.c_str()),
      gai_strerror(rc));
  }
  gscoped_ptr<addrinfo, AddrinfoDeleter> scoped_res(res);
  for (; res != NULL; res = res->ai_next) {
    CHECK_EQ(res->ai_family, AF_INET);
    struct sockaddr_in* addr = reinterpret_cast<struct sockaddr_in*>(res->ai_addr);
    addr->sin_port = htons(port_);
    Sockaddr sockaddr(*addr);
    if (addresses) {
      addresses->push_back(sockaddr);
    }
    VLOG(2) << "Resolved address " << sockaddr.ToString()
            << " for host/port " << ToString();
  }
  return Status::OK();
}

Status HostPort::ParseStrings(const string& comma_sep_addrs,
                              uint16_t default_port,
                              vector<HostPort>* res) {
  vector<string> addr_strings = strings::Split(comma_sep_addrs, ",", strings::SkipEmpty());
  BOOST_FOREACH(const string& addr_string, addr_strings) {
    HostPort host_port;
    RETURN_NOT_OK(host_port.ParseString(addr_string, default_port));
    res->push_back(host_port);
  }
  return Status::OK();
}

string HostPort::ToString() const {
  return Substitute("$0:$1", host_, port_);
}

string HostPort::ToCommaSeparatedString(const vector<HostPort>& hostports) {
  vector<string> hostport_strs;
  BOOST_FOREACH(const HostPort& hostport, hostports) {
    hostport_strs.push_back(hostport.ToString());
  }
  return JoinStrings(hostport_strs, ",");
}

bool IsPrivilegedPort(uint16_t port) {
  return port <= 1024 && port != 0;
}

Status ParseAddressList(const std::string& addr_list,
                        uint16_t default_port,
                        std::vector<Sockaddr>* addresses) {
  vector<HostPort> host_ports;
  RETURN_NOT_OK(HostPort::ParseStrings(addr_list, default_port, &host_ports));
  unordered_set<Sockaddr> uniqued;

  BOOST_FOREACH(const HostPort& host_port, host_ports) {
    vector<Sockaddr> this_addresses;
    RETURN_NOT_OK(host_port.ResolveAddresses(&this_addresses));

    // Only add the unique ones -- the user may have specified
    // some IP addresses in multiple ways
    BOOST_FOREACH(const Sockaddr& addr, this_addresses) {
      if (!InsertIfNotPresent(&uniqued, addr)) {
        LOG(INFO) << "Address " << addr.ToString() << " for " << host_port.ToString()
                  << " duplicates an earlier resolved entry.";
      }
    }
  }

  std::copy(uniqued.begin(), uniqued.end(), std::back_inserter(*addresses));
  return Status::OK();
}

Status GetHostname(string* hostname) {
  char name[HOST_NAME_MAX];
  int ret = gethostname(name, HOST_NAME_MAX);
  if (ret != 0) {
    return Status::NetworkError("Unable to determine local hostname",
                                ErrnoToString(errno),
                                errno);
  }
  *hostname = name;
  return Status::OK();
}

Status GetFQDN(string* hostname) {
  // Start with the non-qualified hostname
  RETURN_NOT_OK(GetHostname(hostname));

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_CANONNAME;

  struct addrinfo* result;
  int rc = getaddrinfo(hostname->c_str(), NULL, &hints, &result);
  if (rc != 0) {
    return Status::NetworkError("Unable to lookup FQDN", ErrnoToString(errno), errno);
  }

  *hostname = result->ai_canonname;
  freeaddrinfo(result);
  return Status::OK();
}

Status SockaddrFromHostPort(const HostPort& host_port, Sockaddr* addr) {
  vector<Sockaddr> addrs;
  RETURN_NOT_OK(host_port.ResolveAddresses(&addrs));
  if (addrs.empty()) {
    return Status::NetworkError("Unable to resolve address", host_port.ToString());
  }
  *addr = addrs[0];
  if (addrs.size() > 1) {
    VLOG(1) << "Hostname " << host_port.host() << " resolved to more than one address. "
            << "Using address: " << addr->ToString();
  }
  return Status::OK();
}

Status HostPortFromSockaddrReplaceWildcard(const Sockaddr& addr, HostPort* hp) {
  string host;
  if (addr.IsWildcard()) {
    RETURN_NOT_OK(GetFQDN(&host));
  } else {
    host = addr.host();
  }
  hp->set_host(host);
  hp->set_port(addr.port());
  return Status::OK();
}

void TryRunLsof(const Sockaddr& addr, vector<string>* log) {
  // Little inline bash script prints the full ancestry of any pid listening
  // on the same port as 'addr'. We could use 'pstree -s', but that option
  // doesn't exist on el6.
  string cmd = strings::Substitute(
      "export PATH=$$PATH:/usr/sbin ; "
      "lsof -n -i 'TCP:$0' -sTCP:LISTEN ; "
      "for pid in $$(lsof -F p -n -i 'TCP:$0' -sTCP:LISTEN | cut -f 2 -dp) ; do"
      "  while [ $$pid -gt 1 ] ; do"
      "    ps h -fp $$pid ;"
      "    stat=($$(</proc/$$pid/stat)) ;"
      "    pid=$${stat[3]} ;"
      "  done ; "
      "done",
      addr.port());

  LOG_STRING(WARNING, log) << "Failed to bind to " << addr.ToString() << ". "
                           << "Trying to use lsof to find any processes listening "
                           << "on the same port:";
  LOG_STRING(INFO, log) << "$ " << cmd;
  Subprocess p("/bin/bash",
               boost::assign::list_of<string>("bash")("-c")(cmd));
  p.ShareParentStdout(false);
  Status s = p.Start();
  if (!s.ok()) {
    LOG_STRING(WARNING, log) << "Unable to fork bash: " << s.ToString();
    return;
  }

  close(p.ReleaseChildStdinFd());

  faststring results;
  char buf[1024];
  while (true) {
    ssize_t n = read(p.from_child_stdout_fd(), buf, arraysize(buf));
    if (n == 0) {
      // EOF
      break;
    }
    if (n < 0) {
      if (errno == EINTR) continue;
      LOG_STRING(WARNING, log) << "IO error reading from bash: " <<
        ErrnoToString(errno);
      close(p.ReleaseChildStdoutFd());
      break;
    }

    results.append(buf, n);
  }

  int rc;
  s = p.Wait(&rc);
  if (!s.ok()) {
    LOG_STRING(WARNING, log) << "Unable to wait for lsof: " << s.ToString();
    return;
  }
  if (rc != 0) {
    LOG_STRING(WARNING, log) << "lsof failed";
  }

  LOG_STRING(WARNING, log) << results.ToString();
}

} // namespace kudu
