// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/net/dns_resolver.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <vector>

#include "kudu/util/flag_tags.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"

DEFINE_int32(dns_num_resolver_threads, 1, "The number of threads to use for DNS resolution");
TAG_FLAG(dns_num_resolver_threads, advanced);

using std::vector;

namespace kudu {

DnsResolver::DnsResolver() {
  CHECK_OK(ThreadPoolBuilder("dns-resolver")
           .set_max_threads(FLAGS_dns_num_resolver_threads)
           .Build(&pool_));
}

DnsResolver::~DnsResolver() {
  pool_->Shutdown();
}

namespace {
static void DoResolution(const HostPort &hostport, vector<Sockaddr>* addresses,
                         StatusCallback cb) {
  cb.Run(hostport.ResolveAddresses(addresses));
}
} // anonymous namespace

void DnsResolver::ResolveAddresses(const HostPort& hostport,
                                   vector<Sockaddr>* addresses,
                                   const StatusCallback& cb) {
  Status s = pool_->SubmitFunc(boost::bind(&DoResolution, hostport, addresses, cb));
  if (!s.ok()) {
    cb.Run(s);
  }
}

} // namespace kudu
