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

#include "kudu/util/ttl_cache.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/integral_types.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/cache.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/ttl_cache_metrics.h"
#include "kudu/util/ttl_cache_test_metrics.h"

DECLARE_bool(cache_force_single_shard);
DECLARE_double(cache_memtracker_approximation_ratio);

METRIC_DECLARE_counter(test_ttl_cache_evictions);
METRIC_DECLARE_counter(test_ttl_cache_evictions_expired);
METRIC_DECLARE_counter(test_ttl_cache_hits);
METRIC_DECLARE_counter(test_ttl_cache_hits_expired);
METRIC_DECLARE_counter(test_ttl_cache_inserts);
METRIC_DECLARE_counter(test_ttl_cache_lookups);
METRIC_DECLARE_counter(test_ttl_cache_misses);
METRIC_DECLARE_gauge_uint64(test_ttl_cache_memory_usage);

using std::atomic;
using std::set;
using std::string;
using std::thread;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

// A class to represent values with constant memory footprint of 1.
class TestValue {
 public:
  explicit TestValue(int value = 0)
      : value(value) {
  }

  // The 'implicit cast to int' operator for ASSERT_EQ()-like comparisions.
  // NOLINTNEXTLINE(google-explicit-constructor)
  operator int() const {
    return value;
  }

  const int value;
};

typedef TTLCache<string, TestValue> TTLTestCache;
typedef TTLTestCache::EntryHandle Handle;

class TTLCacheTest : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();
    // To simplify the logic of the test related to the capacity of the TTL
    // cache and the eviction of entries:
    //   * make the cache consisting of a single shard
    //   * stay clear of any approximations while tracking memory consumption
    FLAGS_cache_force_single_shard = true;
    FLAGS_cache_memtracker_approximation_ratio = 0;

    metric_entity_ = METRIC_ENTITY_server.Instantiate(&metric_registry_,
                                                      "ttl_cache-test");
  }

  // Verify that the cache's entry referenced by 'handle' is non-null and
  // has the expected value.
  void VerifyEntryValue(const Handle& handle, int expected_value) {
    ASSERT_TRUE(handle);
    ASSERT_EQ(expected_value, handle.value());
  }

#define GET_GAUGE_READINGS(func_name, counter_name_suffix) \
  int64_t func_name() { \
    scoped_refptr<Counter> gauge(metric_entity_->FindOrCreateCounter( \
        &METRIC_test_ttl_##counter_name_suffix)); \
    CHECK(gauge); \
    return gauge->value(); \
  }
  GET_GAUGE_READINGS(GetCacheEvictions, cache_evictions)
  GET_GAUGE_READINGS(GetCacheEvictionsExpired, cache_evictions_expired)
  GET_GAUGE_READINGS(GetCacheHitsExpired, cache_hits_expired)
  GET_GAUGE_READINGS(GetCacheHits, cache_hits)
  GET_GAUGE_READINGS(GetCacheInserts, cache_inserts)
  GET_GAUGE_READINGS(GetCacheLookups, cache_lookups)
  GET_GAUGE_READINGS(GetCacheMisses, cache_misses)
#undef GET_GAUGE_READINGS

  int64_t GetCacheUsage() {
    scoped_refptr<AtomicGauge<uint64>> gauge(metric_entity_->FindOrCreateGauge(
        &METRIC_test_ttl_cache_memory_usage, static_cast<uint64>(0)));
    CHECK(gauge);
    return gauge->value();
  }

 protected:
  MetricRegistry metric_registry_;
  scoped_refptr<MetricEntity> metric_entity_;
};

// Basic verification on the cache behavior for entries which don't expire
// during the test.
TEST_F(TTLCacheTest, BasicNoExpiration) {
  const auto entry_ttl = MonoDelta::FromSeconds(300);

  TTLTestCache cache(1, entry_ttl);
  {
    unique_ptr<TTLCacheTestMetrics> metrics(
        new TTLCacheTestMetrics(metric_entity_));
    cache.SetMetrics(std::move(metrics));
  }

  // Try to fetch an entry from an empty cache.
  ASSERT_FALSE(cache.Get("key0"));
  ASSERT_EQ(0, GetCacheHits());
  ASSERT_EQ(1, GetCacheLookups());
  ASSERT_EQ(1, GetCacheMisses());
  ASSERT_EQ(0, GetCacheUsage());

  {
    // Verify how Put() works.
    auto put_h(cache.Put("key0", unique_ptr<TestValue>(new TestValue), 1));
    NO_FATALS(VerifyEntryValue(put_h, 0));

    {
      // Once put in the cache and not yet evicted, the entry must be available
      // for retrieval from the cache.
      auto h(cache.Get("key0"));
      NO_FATALS(VerifyEntryValue(h, 0););
      ASSERT_EQ(2, GetCacheLookups());
      ASSERT_EQ(1, GetCacheHits());
      // That's the same entry.
      ASSERT_EQ(&put_h.value(), &h.value());
    }

    // One more entry is added, but since the cache was at capacity ...
    auto h1 = cache.Put("key1", unique_ptr<TestValue>(new TestValue(1)), 1);
    // However, since the 'put_h' handle still references the entry, it should
    // not be freed yet.
    ASSERT_EQ(0, GetCacheEvictions());
    NO_FATALS(VerifyEntryValue(h1, 1));
    // .. the formerly cached entry should no longer be available.
    ASSERT_FALSE(cache.Get("key0"));
    // One extra miss should be registered.
    ASSERT_EQ(2, GetCacheMisses());
    // No new hits.
    ASSERT_EQ(1, GetCacheHits());

    // However, since the reference to the entry is still present in the scope,
    // it's still available via that reference (but not from the cache as is).
    NO_FATALS(VerifyEntryValue(put_h, 0));
    ASSERT_EQ(2, GetCacheUsage());
  }
  // Once 'put_h' handle is out of scope, the earlier removed entry should be
  // freed.
  ASSERT_EQ(1, GetCacheEvictions());
  ASSERT_EQ(1, GetCacheUsage());

  // Have multiple handles for the same entry in the cache in one scope
  // and make sure the reference counting in the underlying cache works
  // as expected.
  {
    auto put_h = cache.Put("k", unique_ptr<TestValue>(new TestValue(5)), 1);
    ASSERT_EQ(3, GetCacheInserts());
    // The entry keyed with 'key1' should be evicted and freed after new
    // insertion.
    ASSERT_EQ(2, GetCacheEvictions());
    // Nothing except for this newly inserted entry is references elsewhere.
    ASSERT_EQ(1, GetCacheUsage());

    NO_FATALS(VerifyEntryValue(put_h, 5));

    auto get_h0 = cache.Get("k");
    // That's the same entry.
    ASSERT_EQ(&put_h.value(), &get_h0.value());
    NO_FATALS(VerifyEntryValue(get_h0, 5));

    auto get_h1 = cache.Get("k");
    // That's the same entry.
    ASSERT_EQ(&get_h0.value(), &get_h1.value());
    NO_FATALS(VerifyEntryValue(get_h1, 5));
  }

  // No hits of expired entries.
  ASSERT_EQ(0, GetCacheHitsExpired());
  // No evictions of expired entries: all the evicted entries were valid.
  ASSERT_EQ(0, GetCacheEvictionsExpired());
  // The cache evicts entries in a lazy manner.
  ASSERT_EQ(2, GetCacheEvictions());
  ASSERT_EQ(1, GetCacheUsage());
}

// Verify the cache's eviction policy. After their expiration time, entries
// should not be available from the cache. Also, the earliest added, even if
// not-yet-expired, entries are evicted to accomodate new ones if the cache is
// at capacity.
TEST_F(TTLCacheTest, Basic) {
  constexpr size_t cache_capacity = 5;
  const auto entry_ttl = MonoDelta::FromMilliseconds(100);

  TTLTestCache cache(cache_capacity, entry_ttl);
  {
    unique_ptr<TTLCacheTestMetrics> metrics(
        new TTLCacheTestMetrics(metric_entity_));
    cache.SetMetrics(std::move(metrics));
  }

  // At an empty cache, all gauges should read zero.
  ASSERT_EQ(0, GetCacheEvictions());
  ASSERT_EQ(0, GetCacheEvictionsExpired());
  ASSERT_EQ(0, GetCacheHits());
  ASSERT_EQ(0, GetCacheHitsExpired());
  ASSERT_EQ(0, GetCacheInserts());
  ASSERT_EQ(0, GetCacheLookups());
  ASSERT_EQ(0, GetCacheMisses());
  ASSERT_EQ(0, GetCacheUsage());

  auto put_h = cache.Put("key0", unique_ptr<TestValue>(new TestValue(100)), 1);
  ASSERT_EQ(1, GetCacheInserts());
  ASSERT_EQ(1, GetCacheUsage());
  NO_FATALS(VerifyEntryValue(put_h, 100));
  // Using the handle from the Put() operation, no lookups so far.
  ASSERT_EQ(0, GetCacheLookups());
  ASSERT_EQ(0, GetCacheMisses());

  SleepFor(entry_ttl);
  {
    // The entry is expired and must not be available from the cache, but
    // it should not yet be freed since there is a handle that refers to it.
    ASSERT_FALSE(cache.Get("key0"));
    ASSERT_EQ(1, GetCacheLookups());
    // The expired entry was in the cache, even if it was not returned as
    // the result.
    ASSERT_EQ(0, GetCacheMisses());
    ASSERT_EQ(1, GetCacheHits());
    // The cache_hits_expired counter reflects the fact that the expired
    // entry was not returned from the cache as the result of the query.
    ASSERT_EQ(1, GetCacheHitsExpired());
    ASSERT_EQ(0, GetCacheEvictionsExpired());

    // However, the entry should be still available via the reference
    // that is kept in the scope.
    NO_FATALS(VerifyEntryValue(put_h, 100));
  }

  {
    // Re-insert the entry with the same key, but different value.
    auto h = cache.Put("key0", unique_ptr<TestValue>(new TestValue(200)), 1);
    NO_FATALS(VerifyEntryValue(h, 200));
    ASSERT_EQ(2, GetCacheInserts());
    ASSERT_EQ(1, GetCacheLookups());
    ASSERT_EQ(2, GetCacheUsage());

    // That operation should not affect the reference to the entry inserted
    // previously.
    NO_FATALS(VerifyEntryValue(put_h, 100));
  }

  for (auto i = 0; i < cache_capacity; ++i) {
    const auto key = Substitute("1key$0", i);
    unique_ptr<TestValue> val(new TestValue(i));
    cache.Put(key, std::move(val), 1);

    auto h = cache.Get(key);
    NO_FATALS(VerifyEntryValue(h, i));
  }
  ASSERT_EQ(cache_capacity + 2, GetCacheInserts());
  ASSERT_EQ(cache_capacity + 1, GetCacheLookups());
  // All recent lookups should result in cache hits.
  ASSERT_EQ(cache_capacity + 1, GetCacheHits());
  ASSERT_EQ(0, GetCacheMisses());
  // The re-inserted entry for 'key0' should be gone: nothing is referencing it.
  ASSERT_EQ(1, GetCacheEvictions());
  ASSERT_EQ(0, GetCacheEvictionsExpired());
  // The cache is at capacity: all fresh inserted entries plus the entry
  // removed from the cache, but still referenced by 'put_h' handle.
  ASSERT_EQ(cache_capacity + 1, GetCacheUsage());

  // The earliest added entry should be removed from the cache at this point.
  ASSERT_FALSE(cache.Get("key0"));
  ASSERT_EQ(cache_capacity + 2, GetCacheLookups());
  ASSERT_EQ(1, GetCacheMisses());

  // Lately inserted entries should be still around ...
  for (auto i = 0; i < cache_capacity; ++i) {
    const auto key = Substitute("1key$0", i);
    auto h = cache.Get(key);
    NO_FATALS(VerifyEntryValue(h, i));
  }
  ASSERT_EQ(cache_capacity * 2 + 2, GetCacheLookups());
  // No new misses so far.
  ASSERT_EQ(1, GetCacheMisses());

  SleepFor(entry_ttl);
  // ... and after expiration they should be gone.
  for (auto i = 0; i < cache_capacity + 1; ++i) {
    const auto key = Substitute("1key$0", i);
    ASSERT_FALSE(cache.Get(key));
  }
  ASSERT_EQ(cache_capacity + 1, GetCacheHitsExpired());
  // All the entries are expired but should not be freed yet: the very first one
  // is still referenced by the 'put_h' handle, and the rest haven't spilled
  // over the cache's capacity.
  ASSERT_EQ(0, GetCacheEvictionsExpired());
  // One new miss for the absent key Substitute("1key$0", capacity)
  ASSERT_EQ(2, GetCacheMisses());

  // Insert double of the capacity of the queue.
  for (auto i = 0; i < 2 * cache_capacity; ++i) {
    const auto key = Substitute("2key$0", i);
    unique_ptr<TestValue> val(new TestValue(i));
    cache.Put(key, std::move(val), 1);
  }
  // (capacity * 2) new evictions should have happened.
  ASSERT_EQ(cache_capacity * 2 + 1, GetCacheEvictions());
  // Only one extra entry is still referenced.
  ASSERT_EQ(cache_capacity + 1, GetCacheUsage());

  // The first half of the fresnly inserted entries should not be available.
  for (auto i = 0; i < cache_capacity; ++i) {
    const auto key = Substitute("2key$0", i);
    ASSERT_FALSE(cache.Get(key));
  }
  // All recent lookups should result in misses.
  ASSERT_EQ(cache_capacity + 2, GetCacheMisses());
  ASSERT_EQ(cache_capacity + 1, GetCacheHitsExpired());
  // All the expired entries except for the very first inserted must be freed:
  // the very first is still referenced by the 'put_h' handle,
  // but others don't have any references.
  ASSERT_EQ(cache_capacity, GetCacheEvictionsExpired());

  // The second half of the entries should be still present.
  for (auto i = cache_capacity; i < 2 * cache_capacity; ++i) {
    const auto key = Substitute("2key$0", i);
    auto h = cache.Get(key);
    NO_FATALS(VerifyEntryValue(h, i));
  }
  // Correspondingly, there should be no new misses.
  ASSERT_EQ(cache_capacity + 2, GetCacheMisses());

  // After all the shenanigans, an entry should be still available
  // via the reference kept in the scope.
  NO_FATALS(VerifyEntryValue(put_h, 100));
  ASSERT_EQ(cache_capacity + 1, GetCacheUsage());
}

// Test the invalidation of expired entries in the underlying cache.
TEST_F(TTLCacheTest, InvalidationOfExpiredEntries) {
  constexpr size_t cache_capacity = 512;
  const auto entry_ttl = MonoDelta::FromMilliseconds(250);

  TTLTestCache cache(cache_capacity, entry_ttl);
  {
    unique_ptr<TTLCacheTestMetrics> metrics(
        new TTLCacheTestMetrics(metric_entity_));
    cache.SetMetrics(std::move(metrics));
  }

  const Cache::InvalidationControl ctl = {
    [&](Slice /* key */, Slice value) {
      CHECK_EQ(sizeof(TTLTestCache::Entry), value.size());
      const auto* entry = reinterpret_cast<const TTLTestCache::Entry*>(
          value.data());
      return (entry->exp_time > MonoTime::Now());
    },
    [&](size_t valid_entry_count, size_t /* invalid_entry_count */) {
      return valid_entry_count == 0;
    }
  };
  ASSERT_EQ(0, cache.cache_->Invalidate(ctl));

  for (auto i = 0; i < cache_capacity / 2; ++i) {
    cache.Put(Substitute("0$0", i), unique_ptr<TestValue>(new TestValue), 1);
  }
  ASSERT_EQ(0, cache.cache_->Invalidate(ctl));
  ASSERT_EQ(0, GetCacheEvictionsExpired());

  SleepFor(entry_ttl);

  ASSERT_EQ(cache_capacity / 2, cache.cache_->Invalidate(ctl));
  ASSERT_EQ(cache_capacity / 2, GetCacheEvictionsExpired());
  ASSERT_EQ(0, GetCacheUsage());

  for (auto i = 0; i < cache_capacity / 2; ++i) {
    cache.Put(Substitute("1$0", i), unique_ptr<TestValue>(new TestValue), 1);
  }
  SleepFor(entry_ttl);
  for (auto i = 0; i < cache_capacity / 2; ++i) {
    cache.Put(Substitute("2$0", i), unique_ptr<TestValue>(new TestValue), 1);
  }
  ASSERT_EQ(cache_capacity / 2, cache.cache_->Invalidate(ctl));
  ASSERT_EQ(cache_capacity, GetCacheEvictionsExpired());
  ASSERT_EQ(cache_capacity / 2, GetCacheUsage());
}

// Verify the auto-invalidation of expired entries in TTLCache work as expected:
// expired entries are removed with the expecting timing and valid entries stay.
TEST_F(TTLCacheTest, AutoInvalidationOfEntries) {
  constexpr size_t kCacheCapacity = 128;
  constexpr size_t kHalfCacheCapacity = kCacheCapacity / 2;
  const auto kEntryTtl = MonoDelta::FromMilliseconds(256);
  const auto kScrubInterval = MonoDelta::FromMilliseconds(
      kEntryTtl.ToMilliseconds() / 8);
  const auto kEntryTtlThreeQuarters = MonoDelta::FromMilliseconds(
      kEntryTtl.ToMilliseconds() * 3 / 4);

  TTLTestCache cache(kCacheCapacity, kEntryTtl, kScrubInterval);
  {
    unique_ptr<TTLCacheTestMetrics> metrics(
        new TTLCacheTestMetrics(metric_entity_));
    cache.SetMetrics(std::move(metrics));
  }

  // Make sure the scrubber doesn't conflate non-expired entries with expired
  // ones while scrubbing the cache.
  for (auto i = 0; i < kHalfCacheCapacity; ++i) {
    cache.Put(Substitute("1$0", i), unique_ptr<TestValue>(new TestValue(i)), 1);
  }
  SleepFor(kEntryTtlThreeQuarters);
  for (auto i = 0; i < kHalfCacheCapacity; ++i) {
    cache.Put(Substitute("2$0", i), unique_ptr<TestValue>(new TestValue(i)), 1);
  }
  ASSERT_EQ(kCacheCapacity, GetCacheUsage());
  SleepFor(kEntryTtlThreeQuarters);

  // Apart from the OS scheduler's anomalies, the scrubbing thread should run
  // at least once at this point. Given the timing of adding the entries into
  // the cache, half of the entries in the cache should have been invalidated,
  // while the other half should stay.
  ASSERT_EQ(kHalfCacheCapacity, GetCacheEvictionsExpired());
  ASSERT_EQ(kHalfCacheCapacity, GetCacheUsage());

  // Eventually, nothing should be in the cache.
  ASSERT_EVENTUALLY([&]{
    ASSERT_EQ(0, GetCacheUsage());
    ASSERT_EQ(kCacheCapacity, GetCacheEvictionsExpired());
  });
}

// Verify the auto-invalidation of expired entries in TTLCache work as
// expected in the presence of oustanding handles to the entries which
// are subject to invalidation by the scrubbing thread.
TEST_F(TTLCacheTest, AutoInvalidationOfEntriesWithOutstandingReferences) {
  constexpr size_t kCacheCapacity = 512;
  constexpr size_t kHalfCacheCapacity = kCacheCapacity / 2;
  const auto kEntryTtl = MonoDelta::FromMilliseconds(50);
  const auto kScrubInterval = MonoDelta::FromMilliseconds(
      kEntryTtl.ToMilliseconds() / 2);

  // A TTL cache with auto-scrubbing thread that gets rid of expired entries
  // with the frequency corresponding to a half of the TTL interval.
  TTLTestCache cache(kCacheCapacity, kEntryTtl, kScrubInterval);
  {
    unique_ptr<TTLCacheTestMetrics> metrics(
        new TTLCacheTestMetrics(metric_entity_));
    cache.SetMetrics(std::move(metrics));
  }

  {
    const set<string> selected_keys = {
        "0", "2", "9", "99", "127", "128", "252", "255", };
    vector<Handle> selected_handles;
    selected_handles.reserve(selected_keys.size());
    for (auto i = 0; i < kHalfCacheCapacity; ++i) {
      const auto key = Substitute("$0", i);
      auto h = cache.Put(key, unique_ptr<TestValue>(new TestValue), 1);
      if (ContainsKey(selected_keys, key)) {
        selected_handles.emplace_back(std::move(h));
      }
    }
    ASSERT_EQ(selected_keys.size(), selected_handles.size());

    // All the entries except for the ones selected should be gone:
    // the background thread should get rid all the expired entries.
    // The 'selected_handles' still keep references to the selected entries,
    // so those should not be deallocated yet.
    ASSERT_EVENTUALLY([&]{
      ASSERT_EQ(kHalfCacheCapacity - selected_keys.size(),
                GetCacheEvictionsExpired());
      ASSERT_EQ(selected_keys.size(), GetCacheUsage());
    });

    // However, even if the selected entries are not yet deallocated,
    // they must have been removed from the cache's lookup table
    // once the scrubbing thread invalidated them.
    for (const auto& key : selected_keys) {
      ASSERT_FALSE(cache.Get(key));
    }
  }
  // Now, once the handles for the selected entries are gone out of scope,
  // the cache should be empty.
  ASSERT_EQ(kHalfCacheCapacity, GetCacheEvictionsExpired());
  ASSERT_EQ(0, GetCacheUsage());
}

// Verify how the auto-invalidation of expired entries work in the presence
// of concurrent read access to the entries. Also, verify that the limit on the
// number of maximum number of processed entries per one run of the scrubbing
// thread applies as expected.
TEST_F(TTLCacheTest, AutoInvalidationOfEntriesLimitPerPass) {
  constexpr size_t kCacheCapacity = 1024;
  constexpr size_t kScrubMaxEntriesPerPass = 64;
  const auto kEntryTtl = MonoDelta::FromMilliseconds(100);
  const auto kScrubInterval = kEntryTtl;
  const auto kNumRunnerThreads = 64;

  // A TTL cache with auto-scrubbing thread that gets rid of expired entries.
  // The amount of scrubbed entries per pass is limited.
  TTLTestCache cache(kCacheCapacity, kEntryTtl,
                     kScrubInterval, kScrubMaxEntriesPerPass);
  {
    unique_ptr<TTLCacheTestMetrics> metrics(
        new TTLCacheTestMetrics(metric_entity_));
    cache.SetMetrics(std::move(metrics));
  }

  atomic<bool> stop(false);
  vector<thread> threads;
  SCOPED_CLEANUP({
    stop = true;
    for (auto& thread : threads) {
      thread.join();
    }
  });

  for (auto i = 0; i < kNumRunnerThreads; ++i) {
    threads.emplace_back([&cache, &stop] () {
      while (!stop) {
        const auto key = std::to_string(rand() % kCacheCapacity);
        auto h = cache.Get(key);
        // Keep the handle around for some time.
        SleepFor(MonoDelta::FromNanoseconds(rand() % 5));
      }
    });
  }

  const auto start_time = MonoTime::Now();
  for (auto i = 0; i < kCacheCapacity; ++i) {
    cache.Put(std::to_string(i), unique_ptr<TestValue>(new TestValue(i)), 1);
  }

  const auto scrub_interval_ms = kScrubInterval.ToMilliseconds();
  for (auto i = 0; i < kCacheCapacity / kScrubMaxEntriesPerPass; ++i) {
    const auto evictions_expired_num = GetCacheEvictionsExpired();
    const auto duration_ms = (MonoTime::Now() - start_time).ToMilliseconds();
    const auto runs_num = 1 +
        (duration_ms + scrub_interval_ms - 1) / scrub_interval_ms;
    // No more than the specified number of entries should be evicted per one
    // pass of the scrubbing thread.
    ASSERT_LE(evictions_expired_num, runs_num * kScrubMaxEntriesPerPass);
    SleepFor(kScrubInterval);
  }
  stop = true;

  ASSERT_EVENTUALLY([&]{
    ASSERT_EQ(kCacheCapacity, GetCacheEvictionsExpired());
    ASSERT_EQ(0, GetCacheUsage());
  });
}

} // namespace kudu
