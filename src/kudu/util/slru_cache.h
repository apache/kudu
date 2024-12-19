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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/alignment.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/locks.h"
#include "kudu/util/slice.h"

using Handle = kudu::Cache::Handle;
using EvictionCallback = kudu::Cache::EvictionCallback;

namespace kudu {
// An SLRU cache contains two internal caches, the probationary and protected segments both with
// their own separate configurable capacities. An SLRU cache is scan resistant as it protects the
// cache from infrequent long scans larger than the cache or a long series of small infrequent scans
// close to the scan's capacity. It does this by promoting entries from the probationary segment to
// the protected segment after a configurable amount of lookups. Any random scan would then only
// evict entries from the probationary segment. Any entries evicted from the protected segment as a
// result of upgrade of entries would then be inserted into the MRU end of the probationary segment.

class MemTracker;

struct SLRUHandle {
  EvictionCallback* eviction_callback;
  SLRUHandle* next_hash;
  SLRUHandle* next;
  SLRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  uint32_t key_length;
  uint32_t val_length;
  std::atomic<int32_t> refs;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons

  // Number of lookups for this entry. Used for upgrading to protected segment in SLRU cache.
  // Resets to 0 when moved between probationary and protected segments in both directions.
  uint32_t lookups;

  // True if entry is in protected segment, false if not.
  // Used for releasing from the right shard in the SLRU cache implementation.
  std::atomic<bool> in_protected_segment;

  // Number of times an entry has been upgraded or downgraded. These fields are tied to the key,
  // not the entry itself. For example, when an entry is updated, a new handle is inserted, but it
  // retains the values of these fields from the previous entry with the same key.
  int32_t upgrades;
  int32_t downgrades;

  // The storage for the key/value pair itself. The data is stored as:
  //   [key bytes ...] [padding up to 8-byte boundary] [value bytes ...]
  uint8_t kv_data[1];   // Beginning of key/value pair

  Slice key() const {
    return Slice(kv_data, key_length);
  }

  uint8_t* mutable_val_ptr() {
    int val_offset = KUDU_ALIGN_UP(key_length, sizeof(void*));
    return &kv_data[val_offset];
  }

  const uint8_t* val_ptr() const {
    return const_cast<SLRUHandle*>(this)->mutable_val_ptr();
  }

  Slice value() const {
    return Slice(val_ptr(), val_length);
  }

  // Clears fields when moving between probationary and protected segments.
  // Used in SLRU cache implementation.
  void Sanitize() {
    lookups = 0;
    prev = nullptr;
    next = nullptr;
    next_hash = nullptr;
  }
};

enum class Segment {
  kProbationary,
  kProtected,
};

// A single shard of sharded SLRU cache.
template <Segment segment>
class SLRUCacheShard {
 public:
  SLRUCacheShard(MemTracker* tracker, size_t capacity);
  ~SLRUCacheShard();

  size_t capacity() const {
    return capacity_;
  }

  void SetMetrics(SLRUCacheMetrics* metrics) { metrics_ = metrics; }

  // Inserts handle into the appropriate shard and returns the inserted handle.
  // Returns entries to be freed outside the lock with parameter 'free_entries'.
  // See comments on template specialization for each function for more details.
  Handle* Insert(SLRUHandle* handle,
                 EvictionCallback* eviction_callback,
                 SLRUHandle** free_entries);
  // Inserts handle into the appropriate shard when upgrading or downgrading entry.
  // Returns entries to be freed outside the lock with parameter 'free_entries'.
  // See comments on template specialization for each function for more details.
  void ReInsert(SLRUHandle* handle, SLRUHandle** free_entries);
  // Like SLRUCache::Lookup, but with an extra "hash" parameter.
  Handle* Lookup(const Slice& key, uint32_t hash);
  // Reduces the entry's ref by one, frees the entry if no refs are remaining.
  void Release(Handle* handle);
  // Removes entry from shard, returns it to be freed if no refs are remaining.
  // Returns a bool indicating whether the entry was erased.
  bool Erase(const Slice& key, uint32_t hash, SLRUHandle** free_entry);
  // Like Erase, but underlying entry is not freed.
  // Necessary when upgrading entry to protected segment.
  void SoftErase(const Slice& key, uint32_t hash);
  // Returns true if shard contains entry, false if not.
  bool Contains(const Slice& key, uint32_t hash);
  // Update the high-level metrics for a lookup operation.
  void UpdateMetricsLookup(bool was_hit, bool caching);

 private:
  friend class SLRUCacheShardPair;
  void RL_Remove(SLRUHandle* e);
  void RL_Append(SLRUHandle* e);
  // Update the recency list after a lookup operation.
  void RL_UpdateAfterLookup(SLRUHandle* e);
  // Just reduce the reference count by 1.
  // Return true if last reference
  static bool Unref(SLRUHandle* e);
  // Call the user's eviction callback, if it exists, and free the entry.
  void FreeEntry(SLRUHandle* e);
  // Updates eviction related metrics.
  void UpdateMetricsEviction(size_t charge);
  // Removes any entries past capacity of the probationary shard.
  // Those entries are returned through parameter 'free_entries' to be freed outside the lock.
  void RemoveEntriesPastCapacity(SLRUHandle** free_entries);

  // Update the memtracker's consumption by the given amount.
  //
  // This "buffers" the updates locally in 'deferred_consumption_' until the amount
  // of accumulated delta is more than ~1% of the cache capacity. This improves
  // performance under workloads with high eviction rates for a few reasons:
  //
  // 1) once the cache reaches its full capacity, we expect it to remain there
  // in steady state. Each insertion is usually matched by an eviction, and unless
  // the total size of the evicted item(s) is much different than the size of the
  // inserted item, each eviction event is unlikely to change the total cache usage
  // much. So, we expect that the accumulated error will mostly remain around 0
  // and we can avoid propagating changes to the MemTracker at all.
  //
  // 2) because the cache implementation is sharded, we do this tracking in a bunch
  // of different locations, avoiding bouncing cache-lines between cores. By contrast
  // the MemTracker is a simple integer, so it doesn't scale as well under concurrency.
  //
  // Positive delta indicates an increased memory consumption.
  void UpdateMemTracker(int64_t delta);

  // Initialized before use.
  const size_t capacity_;

  size_t usage_;

  // Dummy head of recency list.
  // rl.prev is newest entry, rl.next is oldest entry.
  SLRUHandle rl_;

  Cache::HandleTable<SLRUHandle> table_;

  MemTracker* mem_tracker_;
  std::atomic<int64_t> deferred_consumption_ { 0 };

  SLRUCacheMetrics* metrics_;

  // Initialized based on capacity_ to ensure an upper bound on the error on the
  // MemTracker consumption.
  int64_t max_deferred_consumption_;

  DISALLOW_COPY_AND_ASSIGN(SLRUCacheShard);
};

// Wrapper class for SLRUCacheShard.
class SLRUCacheShardPair {
 public:
  SLRUCacheShardPair(MemTracker* mem_tracker,
                     size_t probationary_capacity,
                     size_t protected_capacity,
                     uint32_t lookups);

  void SetMetrics(SLRUCacheMetrics* metrics);

  Handle* Insert(SLRUHandle* handle, EvictionCallback* eviction_callback);
  // Like Cache::Lookup, but with an extra "hash" parameter.
  Handle* Lookup(const Slice& key, uint32_t hash, bool caching);
  void Release(Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  bool ProbationaryContains(const Slice& key, uint32_t hash);
  bool ProtectedContains(const Slice& key, uint32_t hash);

 private:
  // Remove any entries past capacity in the protected shard and insert them into the probationary
  // shard. As a result of inserting them into the probationary shard, the LRU entries of the
  // probationary shard will be evicted if the probationary shard's usage exceeds its capacity.
  // Any entries evicted from the probationary shard are returned through the parameter
  // 'free_entries' to be freed outside the lock.
  void DowngradeEntries(SLRUHandle** free_entries);
  SLRUCacheShard<Segment::kProbationary> probationary_shard_;
  SLRUCacheShard<Segment::kProtected> protected_shard_;

  // If an entry is looked up at least 'lookups_threshold_' times,
  // it's upgraded to the protected segment.
  const uint32_t lookups_threshold_;

  // mutex_ protects the state of the shard pair from other threads reading/modifying the data.
  simple_spinlock mutex_;

  DISALLOW_COPY_AND_ASSIGN(SLRUCacheShardPair);
};

// TODO(mreddy): Remove duplicated code between ShardedSLRUCache and ShardedCache in cache.cc
class ShardedSLRUCache : public Cache {
 public:
  explicit ShardedSLRUCache(size_t probationary_capacity, size_t protected_capacity,
                            const std::string& id, const uint32_t lookups);

  ~ShardedSLRUCache() override = default;

  UniquePendingHandle Allocate(Slice key, int val_len, int charge) override;

  UniqueHandle Lookup(const Slice& key, CacheBehavior caching) override;

  void Erase(const Slice& key) override;

  Slice Value(const UniqueHandle& handle) const override;

  uint8_t* MutableValue(UniquePendingHandle* handle) override;

  UniqueHandle Insert(UniquePendingHandle handle,
                      EvictionCallback* eviction_callback) override;

  void SetMetrics(std::unique_ptr<CacheMetrics> metrics,
                  ExistingMetricsPolicy metrics_policy) override;

  size_t Invalidate(const InvalidationControl& ctl) override { return 0; }

 protected:
  void Release(Handle* handle) override;

  void Free(PendingHandle* h) override;

 private:
  friend class SLRUCacheBaseTest;
  friend class CacheBench;
  FRIEND_TEST(SLRUCacheTest, EntriesArePinned);
  static int DetermineShardBits();

  static uint32_t HashSlice(const Slice& s);

  uint32_t Shard(uint32_t hash) const;

  // Needs to be declared before 'shards_' so the destructor for 'shards_' is called first.
  // The destructor for 'mem_tracker_' checks that all the memory consumed has been released.
  // The destructor for 'shards_' must be called first since it releases all the memory consumed.
  std::shared_ptr<MemTracker> mem_tracker_;

  std::unique_ptr<CacheMetrics> metrics_;

  // The first shard in the pair belongs to the probationary segment, second one to the protected.
  std::vector<std::unique_ptr<SLRUCacheShardPair>> shards_;

  // Number of bits of hash used to determine the shard.
  const int shard_bits_;

  // Used only when metrics are set to ensure that they are set only once in test environments.
  simple_spinlock metrics_lock_;
};

// Creates a new SLRU cache with 'probationary_capacity' being the capacity of
// the probationary segment in bytes, 'protected_capacity' being the capacity of the
// protected segment in bytes, 'id' specifying the identifier, and 'lookups' specifying
// the number of times an entry can be looked up in the probationary segment
// before being upgraded to the protected segment.
template <Cache::MemoryType memory_type = Cache::MemoryType::DRAM>
ShardedSLRUCache* NewSLRUCache(size_t probationary_capacity, size_t protected_capacity,
                               const std::string& id, uint32_t lookups);

} // namespace kudu
