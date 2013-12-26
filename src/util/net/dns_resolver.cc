// Copyright (c) 2013, Cloudera, inc.

#include "util/net/dns_resolver.h"

#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <vector>

#include "util/threadpool.h"
#include "util/net/net_util.h"
#include "util/net/sockaddr.h"

DEFINE_int32(num_dns_threads, 1, "The number of threads to use for DNS resolution");

using std::vector;

namespace kudu {

DnsResolver::DnsResolver()
  : pool_(new ThreadPool("DNS resolver")) {
  CHECK_OK(pool_->Init(FLAGS_num_dns_threads));
}

DnsResolver::~DnsResolver() {
  pool_->Shutdown();
}

namespace {
static void DoResolution(const HostPort &hostport, vector<Sockaddr>* addresses,
                         StatusCallback cb) {
  cb(hostport.ResolveAddresses(addresses));
}
} // anonymous namespace

void DnsResolver::ResolveAddresses(const HostPort& hostport,
                                   vector<Sockaddr>* addresses,
                                   const StatusCallback& cb) {
  Status s = pool_->SubmitFunc(boost::bind(&DoResolution, hostport, addresses, cb));
  if (!s.ok()) {
    cb(s);
  }
}

} // namespace kudu
