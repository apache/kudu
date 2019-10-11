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

#include <stddef.h>

#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"
#include "kudu/util/ttl_cache.h"

namespace kudu {

class HostPort;
class ThreadPool;

// A utility class for DNS resolution. The resolver supports both synchronous
// and asynchronous address resolution. Optionally, the resolved entries are
// cached and re-used when not yet expired. The constructor's arguments define
// parameters for the TTL cache containing the results of prior DNS lookups.
// The cache doesn't store negative results, i.e. nothing is stored in the cache
// when DNS resolution fails for the specified HostPort.
class DnsResolver {
 public:
  // The 'max_threads_num' parameter specifies the maximum number of threads in
  // the pool used for asynchronous DNS resolution. The 'cache_capacity_bytes'
  // parameter defines the capacity of the underlying TTL cache for resolved
  // DNS records. If set to 0, resolved DNS entries are not cached. The
  // 'cache_ttl' parameter defines the TTL for cache's entries.
  explicit DnsResolver(int max_threads_num = 1,
                       size_t cache_capacity_bytes = 0,
                       MonoDelta cache_ttl = MonoDelta::FromSeconds(60));
  ~DnsResolver();

  // Synchronously resolve addresses corresponding to the specified host:port
  // pair in 'hostport'. Note that a host may resolve to more than one IP
  // address.
  //
  // The 'addresses' output parameter may be nullptr, in which case this method
  // simply checks that the host/port pair can be resolved, without returning
  // the actual results.
  Status ResolveAddresses(const HostPort& hostport,
                          std::vector<Sockaddr>* addresses);

  // The asynchronous version of ResolveAddresses() method.
  // See ResolveAddresses() for information on 'hostport' and 'addresses'
  // parameters.
  //
  // When the result is available, or an error occurred, 'cb' is called with
  // the result Status.
  //
  // NOTE: the callback should be fast since it is called by the DNS
  // resolution thread.
  // NOTE: in some rare cases, the callback may also be called inline
  // from this function call, on the caller's thread.
  void ResolveAddressesAsync(const HostPort& hostport,
                             std::vector<Sockaddr>* addresses,
                             const StatusCallback& cb);

 private:
  // The cache is keyed by the host part of the HostPort structure, and the
  // entry stores a vector of all Sockaddr structures produced by DNS resolution
  // of the key. The port number is stored as a part of Sockaddr structure:
  // it's not relevant for any lookup and re-written upon retrieval
  // of the corresponding entry for the specified key.
  typedef TTLCache<std::string, std::vector<Sockaddr>> HostRecordCache;

  Status DoResolution(const HostPort& hostport,
                      std::vector<Sockaddr>* addresses);

  void DoResolutionCb(const HostPort& hostport,
                      std::vector<Sockaddr>* addresses,
                      const StatusCallback& cb);

  bool GetCachedAddresses(const HostPort& hostport,
                          std::vector<Sockaddr>* addresses);

  std::unique_ptr<ThreadPool> pool_;
  std::unique_ptr<HostRecordCache> cache_;

  DISALLOW_COPY_AND_ASSIGN(DnsResolver);
};

} // namespace kudu
