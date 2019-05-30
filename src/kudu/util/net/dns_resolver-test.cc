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

#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <string>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/async_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/test_macros.h"

DECLARE_uint32(dns_resolver_cache_capacity_mb);

using std::vector;
using strings::Substitute;

namespace kudu {

TEST(DnsResolverTest, AsyncResolution) {
  vector<Sockaddr> addrs;
  // Non-caching asynchronous DNS resolver.
  DnsResolver resolver(1/* max_threads_num */);
  Synchronizer s;
  resolver.ResolveAddressesAsync(HostPort("localhost", 12345), &addrs,
                                 s.AsStatusCallback());
  ASSERT_OK(s.Wait());
  ASSERT_TRUE(!addrs.empty());
  for (const Sockaddr& addr : addrs) {
    LOG(INFO) << "Address: " << addr.ToString();
    EXPECT_TRUE(HasPrefixString(addr.ToString(), "127."));
    EXPECT_TRUE(HasSuffixString(addr.ToString(), ":12345"));
  }
}

TEST(DnsResolverTest, CachingVsNonCachingResolver) {
  constexpr const auto kNumIterations = 1000;
  constexpr const auto kIdxNonCached = 0;
  constexpr const auto kIdxCached = 1;
  constexpr const char* const kHost = "localhost";

  MonoDelta timings[2];
  for (auto idx = 0; idx < ARRAYSIZE(timings); ++idx) {
    // DNS resolver's cache capacity of 0 means the results are not cached.
    size_t capacity_mb = idx * 1024 * 1024;
    DnsResolver resolver(1, capacity_mb, MonoDelta::FromSeconds(10));
    const auto start_time = MonoTime::Now();
    for (auto i = 0; i < kNumIterations; ++i) {
      vector<Sockaddr> addrs;
      uint16_t port = rand() % kNumIterations + kNumIterations;
      {
        HostPort hp(kHost, port);
        ASSERT_OK(resolver.ResolveAddresses(hp, &addrs));
      }
      ASSERT_TRUE(!addrs.empty());
      for (const Sockaddr& addr : addrs) {
        EXPECT_TRUE(HasSuffixString(addr.ToString(), Substitute(":$0", port)));
      }
    }
    timings[idx] = MonoTime::Now() - start_time;
  }
  LOG(INFO) << Substitute("$0 non-cached resolutions of '$1' took $2",
                          kNumIterations, kHost,
                          timings[kIdxNonCached].ToString());
  LOG(INFO) << Substitute("$0     cached resolutions of '$1' took $2",
                          kNumIterations, kHost,
                          timings[kIdxCached].ToString());
  ASSERT_GT(timings[kIdxNonCached], timings[kIdxCached]);
}

} // namespace kudu
