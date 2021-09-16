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
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/util/async_util.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/test_macros.h"

DECLARE_string(dns_addr_resolution_override);
DECLARE_uint32(dns_resolver_cache_capacity_mb);

using std::thread;
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

TEST(DnsResolverTest, RefreshCachedEntry) {
  gflags::FlagSaver saver;
  vector<Sockaddr> addrs;
  DnsResolver resolver(1/* max_threads_num */, 1024 * 1024/* cache_capacity_bytes */);
  ASSERT_OK(resolver.ResolveAddresses(HostPort("localhost", 12345), &addrs));
  ASSERT_TRUE(!addrs.empty());
  for (const Sockaddr& addr : addrs) {
    LOG(INFO) << "Address: " << addr.ToString();
    EXPECT_TRUE(HasPrefixString(addr.ToString(), "127."));
    EXPECT_TRUE(HasSuffixString(addr.ToString(), ":12345"));
  }
  // If we override the DNS lookup address, when we refresh the address, the
  // cached entry gets reset.
  constexpr const char* kFakeAddr = "1.1.1.1";
  FLAGS_dns_addr_resolution_override = Substitute("localhost=$0", kFakeAddr);
  Synchronizer s;
  resolver.RefreshAddressesAsync(HostPort("localhost", 1111), &addrs,
                                 s.AsStatusCallback());
  ASSERT_OK(s.Wait());
  ASSERT_EQ(1, addrs.size());
  ASSERT_EQ(Substitute("$0:1111", kFakeAddr), addrs[0].ToString());
  ASSERT_EQ(1111, addrs[0].port());

  // Once we stop overriding DNS lookups, simply getting the address from the
  // resolver will read from the cache.
  FLAGS_dns_addr_resolution_override = "";
  ASSERT_OK(resolver.ResolveAddresses(HostPort("localhost", 12345), &addrs));
  ASSERT_EQ(1, addrs.size());
  ASSERT_EQ(Substitute("$0:12345", kFakeAddr), addrs[0].ToString());
  ASSERT_EQ(12345, addrs[0].port());

  // But a refresh should return the original address.
  Synchronizer s2;
  resolver.RefreshAddressesAsync(HostPort("localhost", 12345), &addrs,
                                 s2.AsStatusCallback());
  ASSERT_OK(s2.Wait());
  EXPECT_FALSE(addrs.empty());
  for (const Sockaddr& addr : addrs) {
    LOG(INFO) << "Address: " << addr.ToString();
    EXPECT_TRUE(HasPrefixString(addr.ToString(), "127."));
    EXPECT_TRUE(HasSuffixString(addr.ToString(), ":12345"));
  }
}

TEST(DnsResolverTest, ConcurrentRefreshesAndResolutions) {
  constexpr int kNumThreads = 3;
  constexpr int kNumResolutionsPerThread = 10;
  DnsResolver resolver(1/* max_threads_num */, 1024 * 1024/* cache_capacity_bytes */);
  vector<thread> threads;
  auto cancel_threads = MakeScopedCleanup([&] {
    for (auto& t : threads) {
      t.join();
    }
  });
  const auto validate_addrs = [] (const vector<Sockaddr>& addrs) {
    ASSERT_FALSE(addrs.empty());
    for (const Sockaddr& addr : addrs) {
      EXPECT_TRUE(HasPrefixString(addr.ToString(), "127."));
      EXPECT_TRUE(HasSuffixString(addr.ToString(), ":12345"));
    }
  };
  for (int i = 0; i < kNumThreads - 1; i++) {
    threads.emplace_back([&] {
      for (int r = 0; r < kNumResolutionsPerThread; r++) {
        vector<Sockaddr> addrs;
        Synchronizer s;
        resolver.RefreshAddressesAsync(HostPort("localhost", 12345), &addrs,
                                       s.AsStatusCallback());
        ASSERT_OK(s.Wait());
        NO_FATALS(validate_addrs(addrs));
      }
    });
  }
  threads.emplace_back([&] {
    for (int r = 0; r < kNumResolutionsPerThread; r++) {
      vector<Sockaddr> addrs;
      ASSERT_OK(resolver.ResolveAddresses(HostPort("localhost", 12345), &addrs));
      NO_FATALS(validate_addrs(addrs));
    }
  });
  for (auto& t : threads) {
    t.join();
  }
  cancel_threads.cancel();
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
        EXPECT_TRUE(HasSuffixString(addr.ToString(), Substitute(":$0", port))) << addr.ToString();
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
