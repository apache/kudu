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

#include "kudu/util/net/dns_resolver.h"

#include <memory>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/malloc.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/ttl_cache.h"

DEFINE_int32(dns_resolver_max_threads_num, 1,
             "The maximum number of threads to use for async DNS resolution");
TAG_FLAG(dns_resolver_max_threads_num, advanced);

DEFINE_uint32(dns_resolver_cache_capacity_mb, 1,
              "Capacity of DNS resolver cache, in MiBytes. For each key, the "
              "cache stores records returned by getaddrinfo(). A value of 0 "
              "means the results of DNS name resolution are not cached.");
TAG_FLAG(dns_resolver_cache_capacity_mb, advanced);

DEFINE_uint32(dns_resolver_cache_ttl_sec, 15,
              "TTL of records in the DNS resolver cache, in seconds.");
TAG_FLAG(dns_resolver_cache_ttl_sec, advanced);

using std::unique_ptr;
using std::vector;

namespace kudu {

DnsResolver::DnsResolver(int max_threads_num,
                         size_t cache_capacity_bytes,
                         MonoDelta cache_ttl) {
  CHECK_OK(ThreadPoolBuilder("dns-resolver")
           .set_max_threads(max_threads_num)
           .Build(&pool_));
  if (cache_capacity_bytes > 0) {
    // Cache TTL should be a valid time interval if cache is enabled.
    CHECK(cache_ttl.Initialized() && cache_ttl.ToNanoseconds() > 0);
    cache_.reset(new HostRecordCache(cache_capacity_bytes, cache_ttl));
  }
}

DnsResolver::~DnsResolver() {
  pool_->Shutdown();
}

Status DnsResolver::ResolveAddresses(const HostPort& hostport,
                                     vector<Sockaddr>* addresses) {
  if (GetCachedAddresses(hostport, addresses)) {
    return Status::OK();
  }
  return DoResolution(hostport, addresses);
}

void DnsResolver::ResolveAddressesAsync(const HostPort& hostport,
                                        vector<Sockaddr>* addresses,
                                        const StatusCallback& cb) {
  if (GetCachedAddresses(hostport, addresses)) {
    return cb.Run(Status::OK());
  }
  const auto s = pool_->Submit([=]() {
    this->DoResolutionCb(hostport, addresses, cb);
  });
  if (!s.ok()) {
    cb.Run(s);
  }
}

Status DnsResolver::DoResolution(const HostPort& hostport,
                                 vector<Sockaddr>* addresses) {
  vector<Sockaddr> resolved_addresses;
  RETURN_NOT_OK(hostport.ResolveAddresses(&resolved_addresses));

  if (PREDICT_TRUE(cache_)) {
    unique_ptr<vector<Sockaddr>> cached_addresses(
        new vector<Sockaddr>(resolved_addresses));
    const auto& entry_key = hostport.host();
    const auto entry_charge = kudu_malloc_usable_size(cached_addresses.get()) +
        cached_addresses->capacity() > 0
        ? kudu_malloc_usable_size(cached_addresses->data()) : 0;
#ifndef NDEBUG
    // Clear the port number.
    for (auto& addr : *cached_addresses) {
      addr.set_port(0);
    }
#endif
    cache_->Put(entry_key, std::move(cached_addresses), entry_charge);
  }

  if (addresses) {
    *addresses = std::move(resolved_addresses);
  }
  return Status::OK();
}

void DnsResolver::DoResolutionCb(const HostPort& hostport,
                                 vector<Sockaddr>* addresses,
                                 const StatusCallback& cb) {
  cb.Run(DoResolution(hostport, addresses));
}

bool DnsResolver::GetCachedAddresses(const HostPort& hostport,
                                     vector<Sockaddr>* addresses) {
  if (PREDICT_TRUE(cache_)) {
    auto handle = cache_->Get(hostport.host());
    if (handle) {
      if (addresses) {
        vector<Sockaddr> result_addresses(handle.value());
        for (auto& addr : result_addresses) {
          addr.set_port(hostport.port());
        }
        *addresses = std::move(result_addresses);
      }
      return true;
    }
  }
  return false;
}

} // namespace kudu
