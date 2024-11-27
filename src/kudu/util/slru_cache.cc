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

#include "kudu/util/slru_cache.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/gutil/bits.h"
#include "kudu/gutil/hash/city.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/alignment.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/malloc.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util_prod.h"

DECLARE_bool(cache_force_single_shard);
DECLARE_double(cache_memtracker_approximation_ratio);

using std::vector;
using Handle = kudu::Cache::Handle;
using EvictionCallback = kudu::Cache::EvictionCallback;

namespace kudu {

template<Segment segment>
SLRUCacheShard<segment>::SLRUCacheShard(MemTracker* tracker, size_t capacity)
    : capacity_(capacity),
      usage_(0),
      mem_tracker_(tracker),
      metrics_(nullptr) {
  max_deferred_consumption_ = capacity * FLAGS_cache_memtracker_approximation_ratio;
  // Make empty circular linked list.
  rl_.next = &rl_;
  rl_.prev = &rl_;
}

template<Segment segment>
SLRUCacheShard<segment>::~SLRUCacheShard() {
  for (SLRUHandle* e = rl_.next; e != &rl_; ) {
    SLRUHandle* next = e->next;
    DCHECK_EQ(e->refs.load(std::memory_order_relaxed), 1)
      << "caller has an unreleased handle";
    if (Unref(e)) {
      FreeEntry(e);
    }
    e = next;
  }
  mem_tracker_->Consume(deferred_consumption_);
}

template<Segment segment>
bool SLRUCacheShard<segment>::Unref(SLRUHandle* e) {
  DCHECK_GT(e->refs.load(std::memory_order_relaxed), 0);
  return e->refs.fetch_sub(1) == 1;
}

template<Segment segment>
void SLRUCacheShard<segment>::FreeEntry(SLRUHandle* e) {
  DCHECK_EQ(e->refs.load(std::memory_order_relaxed), 0);
  if (e->eviction_callback) {
    e->eviction_callback->EvictedEntry(e->key(), e->value());
  }
  UpdateMemTracker(-static_cast<int64_t>(e->charge));
  if (PREDICT_TRUE(metrics_)) {
    metrics_->cache_usage->DecrementBy(e->charge);
    metrics_->evictions->Increment();
    UpdateMetricsEviction(e->charge);
  }
  delete [] e;
}

template<>
void SLRUCacheShard<Segment::kProbationary>::UpdateMetricsEviction(size_t charge) {
  metrics_->probationary_segment_cache_usage->DecrementBy(charge);
  metrics_->probationary_segment_evictions->Increment();
}

template<>
void SLRUCacheShard<Segment::kProtected>::UpdateMetricsEviction(size_t charge) {
  metrics_->protected_segment_cache_usage->DecrementBy(charge);
  metrics_->protected_segment_evictions->Increment();
}

template<Segment segment>
void SLRUCacheShard<segment>::UpdateMemTracker(int64_t delta) {
  int64_t old_deferred = deferred_consumption_.fetch_add(delta);
  int64_t new_deferred = old_deferred + delta;

  if (new_deferred > max_deferred_consumption_ ||
      new_deferred < -max_deferred_consumption_) {
    int64_t to_propagate = deferred_consumption_.exchange(0, std::memory_order_relaxed);
    mem_tracker_->Consume(to_propagate);
  }
}

template<Segment segment>
void SLRUCacheShard<segment>::UpdateMetricsLookup(bool was_hit, bool caching) {
  if (PREDICT_TRUE(metrics_)) {
    metrics_->lookups->Increment();
    if (was_hit) {
      if (caching) {
        metrics_->cache_hits_caching->Increment();
      } else {
        metrics_->cache_hits->Increment();
      }
    } else {
      if (caching) {
        metrics_->cache_misses_caching->Increment();
      } else {
        metrics_->cache_misses->Increment();
      }
    }
  }
}

template<>
void SLRUCacheShard<Segment::kProbationary>::UpdateSegmentMetricsLookup(bool was_hit,
                                                                        bool caching) {
  if (PREDICT_TRUE(metrics_)) {
    metrics_->probationary_segment_lookups->Increment();
    if (was_hit) {
      if (caching) {
        metrics_->probationary_segment_cache_hits_caching->Increment();
      } else {
        metrics_->probationary_segment_cache_hits->Increment();
      }
    } else {
      if (caching) {
        metrics_->probationary_segment_cache_misses_caching->Increment();
      } else {
        metrics_->probationary_segment_cache_misses->Increment();
      }
    }
  }
}

template<>
void SLRUCacheShard<Segment::kProtected>::UpdateSegmentMetricsLookup(bool was_hit, bool caching) {
  if (PREDICT_TRUE(metrics_)) {
    metrics_->protected_segment_lookups->Increment();
    if (was_hit) {
      if (caching) {
        metrics_->protected_segment_cache_hits_caching->Increment();
      } else {
        metrics_->protected_segment_cache_hits->Increment();
      }
    } else {
      if (caching) {
        metrics_->protected_segment_cache_misses_caching->Increment();
      } else {
        metrics_->protected_segment_cache_misses->Increment();
      }
    }
  }
}

template<Segment segment>
void SLRUCacheShard<segment>::RL_Remove(SLRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
  DCHECK_GE(usage_, e->charge);
  usage_ -= e->charge;
}

template<Segment segment>
void SLRUCacheShard<segment>::RL_Append(SLRUHandle* e) {
  // Make "e" newest entry by inserting just before rl_.
  e->next = &rl_;
  e->prev = rl_.prev;
  e->prev->next = e;
  e->next->prev = e;
  usage_ += e->charge;
}

template<Segment segment>
void SLRUCacheShard<segment>::RL_UpdateAfterLookup(SLRUHandle* e) {
  RL_Remove(e);
  RL_Append(e);
}

// No mutex is needed here since all the SLRUCacheShardPair methods that access the underlying
// shards and its tables are protected by mutexes. Same logic applies to the all the below
// methods in SLRUCacheShard.
template<Segment segment>
Handle* SLRUCacheShard<segment>::Lookup(const Slice& key, uint32_t hash, bool caching) {
  SLRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    e->refs.fetch_add(1, std::memory_order_relaxed);
    e->lookups++;
    RL_UpdateAfterLookup(e);
  }
  UpdateSegmentMetricsLookup(e != nullptr, caching);

  return reinterpret_cast<Handle*>(e);
}

template<Segment segment>
bool SLRUCacheShard<segment>::Contains(const Slice& key, uint32_t hash) {
  return table_.Lookup(key, hash) != nullptr;
}

template<Segment segment>
void SLRUCacheShard<segment>::Release(Handle* handle) {
  SLRUHandle* e = reinterpret_cast<SLRUHandle*>(handle);
  // If this is the last reference of the handle, the entry will be freed.
  if (Unref(e)) {
    FreeEntry(e);
  }
}

template<Segment segment>
void SLRUCacheShard<segment>::RemoveEntriesPastCapacity() {
  while (usage_ > capacity_ && rl_.next != &rl_) {
    SLRUHandle* old = rl_.next;
    RL_Remove(old);
    table_.Remove(old->key(), old->hash);
    if (Unref(old)) {
      FreeEntry(old);
    }
  }
}

template<Segment segment>
void SLRUCacheShard<segment>::SoftRemoveEntriesPastCapacity(vector<SLRUHandle*>* evicted_entries) {
  while (usage_ > capacity_ && rl_.next != &rl_) {
    SLRUHandle* old = rl_.next;
    RL_Remove(old);
    table_.Remove(old->key(), old->hash);
    if (PREDICT_TRUE(metrics_)) {
      UpdateMetricsEviction(old->charge);
    }
    evicted_entries->emplace_back(old);
  }
}

template<>
Handle* SLRUCacheShard<Segment::kProbationary>::Insert(SLRUHandle* handle,
                                                       EvictionCallback* eviction_callback) {
  // Set the remaining SLRUHandle members which were not already allocated during Allocate().
  handle->eviction_callback = eviction_callback;
  // Two refs for the handle: one from SLRUCacheShard, one for the returned handle.
  handle->refs.store(2, std::memory_order_relaxed);
  UpdateMemTracker(handle->charge);
  if (PREDICT_TRUE(metrics_)) {
    metrics_->cache_usage->IncrementBy(handle->charge);
    metrics_->inserts->Increment();
    metrics_->probationary_segment_cache_usage->IncrementBy(handle->charge);
    metrics_->probationary_segment_inserts->Increment();
  }
  RL_Append(handle);

  SLRUHandle* old_entry = table_.Insert(handle);
  // If entry with key already exists, remove it.
  if (old_entry != nullptr) {
    RL_Remove(old_entry);
    if (Unref(old_entry)) {
      FreeEntry(old_entry);
    }
  }
  RemoveEntriesPastCapacity();

  return reinterpret_cast<Handle*>(handle);
}

template<>
vector<SLRUHandle*> SLRUCacheShard<Segment::kProtected>::InsertAndReturnEvicted(
    SLRUHandle* handle) {
  handle->in_protected_segment.store(true, std::memory_order_relaxed);
  if (PREDICT_TRUE(metrics_)) {
    metrics_->upgrades->Increment();
    metrics_->protected_segment_cache_usage->IncrementBy(handle->charge);
    metrics_->protected_segment_inserts->Increment();
  }

  handle->Sanitize();
  RL_Append(handle);

  // No entries should exist with same key in protected segment when upgrading.
  SLRUHandle* old_entry = table_.Insert(handle);
  DCHECK(old_entry == nullptr);

  vector<SLRUHandle*> evicted_entries;
  SoftRemoveEntriesPastCapacity(&evicted_entries);
  return evicted_entries;
}

template<>
Handle* SLRUCacheShard<Segment::kProtected>::ProtectedInsert(SLRUHandle* handle,
                                                             EvictionCallback* eviction_callback,
                                                             vector<SLRUHandle*>* evictions) {
  handle->eviction_callback = eviction_callback;
  // Two refs for the handle: one from SLRUCacheShard, one for the returned handle.
  // Even though this function is for updates in the protected segment, it's treated similarly
  // to Insert() in the probationary segment.
  handle->refs.store(2, std::memory_order_relaxed);
  handle->in_protected_segment.store(true, std::memory_order_relaxed);
  UpdateMemTracker(handle->charge);
  if (PREDICT_TRUE(metrics_)) {
    metrics_->cache_usage->IncrementBy(handle->charge);
    metrics_->inserts->Increment();
    metrics_->protected_segment_cache_usage->IncrementBy(handle->charge);
    metrics_->protected_segment_inserts->Increment();
  }

  RL_Append(handle);

  // Update case so Insert should return a non-null entry.
  SLRUHandle* old_entry = table_.Insert(handle);
  DCHECK(old_entry != nullptr);
  RL_Remove(old_entry);
  if (Unref(old_entry)) {
    FreeEntry(old_entry);
  }

  SoftRemoveEntriesPastCapacity(evictions);
  return reinterpret_cast<Handle*>(handle);
}

template<>
void SLRUCacheShard<Segment::kProbationary>::ReInsert(SLRUHandle* handle) {
  handle->in_protected_segment.store(false, std::memory_order_relaxed);
  if (PREDICT_TRUE(metrics_)) {
    metrics_->downgrades->Increment();
    metrics_->probationary_segment_cache_usage->IncrementBy(handle->charge);
    metrics_->probationary_segment_inserts->Increment();
  }
  handle->Sanitize();
  RL_Append(handle);

  // No entries should exist with same key in probationary segment when downgrading.
  SLRUHandle* old_entry = table_.Insert(handle);
  DCHECK(old_entry == nullptr);
  RemoveEntriesPastCapacity();
}

template<Segment segment>
void SLRUCacheShard<segment>::Erase(const Slice& key, uint32_t hash) {
  SLRUHandle* e = table_.Remove(key, hash);
  if (e != nullptr) {
    RL_Remove(e);
    // Free entry if this is the last reference.
    if (Unref(e)) {
      FreeEntry(e);
    }
  }
}

template<Segment segment>
void SLRUCacheShard<segment>::SoftErase(const Slice& key, uint32_t hash) {
  SLRUHandle* e = table_.Remove(key, hash);
  if (e != nullptr) {
    RL_Remove(e);
    if (PREDICT_TRUE(metrics_)) {
      UpdateMetricsEviction(e->charge);
    }
  }
}

template class SLRUCacheShard<Segment::kProtected>;
template class SLRUCacheShard<Segment::kProbationary>;

SLRUCacheShardPair::SLRUCacheShardPair(MemTracker* mem_tracker,
                                       size_t probationary_capacity,
                                       size_t protected_capacity,
                                       uint32_t lookups) :
    probationary_shard_(SLRUCacheShard<Segment::kProbationary>(mem_tracker, probationary_capacity)),
    protected_shard_(SLRUCacheShard<Segment::kProtected>(mem_tracker, protected_capacity)),
    lookups_threshold_(lookups) {
}

void SLRUCacheShardPair::SetMetrics(SLRUCacheMetrics* metrics) {
  std::lock_guard l(mutex_);
  probationary_shard_.SetMetrics(metrics);
  protected_shard_.SetMetrics(metrics);
}

// Commit a prepared entry into the probationary segment if entry does not exist or if it
// exists in the probationary segment (upsert case).
// If entry exists in protected segment, entry will be updated and any evicted entries will
// be properly downgraded to the probationary segment.
// Look at Cache::Insert() for more details.
Handle* SLRUCacheShardPair::Insert(SLRUHandle* handle,
                                   EvictionCallback* eviction_callback) {
  std::lock_guard l(mutex_);
  if (!ProtectedContains(handle->key(), handle->hash)) {
    return probationary_shard_.Insert(handle, eviction_callback);
  }
  // If newly inserted entry has greater charge than previous one,
  // possible that entries can be evicted if at capacity.
  vector<SLRUHandle*> evictions;
  auto* inserted = protected_shard_.ProtectedInsert(handle, eviction_callback, &evictions);
  for (auto it = evictions.begin(); it != evictions.end(); ++it) {
    SLRUHandle* evicted_entry = *it;
    probationary_shard_.ReInsert(evicted_entry);
  }
  return inserted;
}

Handle* SLRUCacheShardPair::Lookup(const Slice& key, uint32_t hash, bool caching) {
  // Lookup protected segment:
  //  - Hit: Return handle.
  //  - Miss: Lookup probationary segment:
  //      - Hit: If the number of lookups is < than 'lookups_threshold_', return the lookup handle.
  //             If the number of lookups is >= than 'lookups_threshold_', upgrade the entry:
  //                Erase entry from the probationary segment and insert entry into the protected
  //                segment. Return the lookup handle. If any entries are evicted from the
  //                protected segment, insert them into the probationary segment.
  //      - Miss: Return the handle.
  //
  // Lookup metrics for both segments and the high-level cache are updated with each lookup.
  std::lock_guard l(mutex_);
  Handle* protected_handle = protected_shard_.Lookup(key, hash, caching);

  // If the key exists in the protected segment, return the result from the lookup of the
  // protected segment.
  if (protected_handle) {
    protected_shard_.UpdateMetricsLookup(true, caching);
    probationary_shard_.UpdateSegmentMetricsLookup(false, caching);
    return protected_handle;
  }
  Handle* probationary_handle = probationary_shard_.Lookup(key, hash, caching);

  // Return null handle if handle is not found in either the probationary or protected segment.
  if (!probationary_handle) {
    protected_shard_.UpdateMetricsLookup(false, caching);
    return probationary_handle;
  }
  protected_shard_.UpdateMetricsLookup(true, caching);
  auto* val_handle = reinterpret_cast<SLRUHandle*>(probationary_handle);
  // If the number of lookups for entry isn't at the minimum number required before
  // upgrading to the protected segment, return the entry found in probationary segment.
  // If the entry's charge is larger than the protected segment's capacity, return entry found
  // in probationary segment to avoid evicting any entries in the protected segment.
  if (val_handle->lookups < lookups_threshold_ ||
      val_handle->charge > protected_shard_.capacity()) {
    return probationary_handle;
  }

  // Upgrade from the probationary segment.
  // Erase entry from the probationary segment then add entry to the protected segment.
  probationary_shard_.SoftErase(key, hash);
  vector<SLRUHandle*> evictions = protected_shard_.InsertAndReturnEvicted(val_handle);

  // Go through all evicted entries from the protected segment and insert them into
  // the probationary segment. Insert the LRU entries from the protected segment first.
  for (auto it = evictions.begin(); it != evictions.end(); ++it) {
    SLRUHandle* evicted_entry = *it;
    probationary_shard_.ReInsert(evicted_entry);
  }
  return probationary_handle;
}

void SLRUCacheShardPair::Release(Handle* handle) {
  SLRUHandle* e = reinterpret_cast<SLRUHandle*>(handle);

  // Release from either the probationary or the protected shard.
  if (!e->in_protected_segment.load(std::memory_order_relaxed)) {
    probationary_shard_.Release(handle);
  } else {
    protected_shard_.Release(handle);
  }
}

void SLRUCacheShardPair::Erase(const Slice& key, uint32_t hash) {
  std::lock_guard l(mutex_);
  probationary_shard_.Erase(key, hash);
  protected_shard_.Erase(key, hash);
}

bool SLRUCacheShardPair::ProbationaryContains(const Slice& key, uint32_t hash) {
  return probationary_shard_.Contains(key, hash);
}

bool SLRUCacheShardPair::ProtectedContains(const Slice& key, uint32_t hash) {
  return protected_shard_.Contains(key, hash);
}

ShardedSLRUCache::ShardedSLRUCache(size_t probationary_capacity, size_t protected_capacity,
                                   const std::string& id, const uint32_t lookups)
    : shard_bits_(DetermineShardBits()) {
  // A cache is often a singleton, so:
  // 1. We reuse its MemTracker if one already exists, and
  // 2. It is directly parented to the root MemTracker.
  mem_tracker_ = MemTracker::FindOrCreateGlobalTracker(
      -1, strings::Substitute("$0-sharded_slru_cache", id));

  CHECK_GT(lookups, 0);
  int num_shards = 1 << shard_bits_;
  const size_t per_probationary_shard = (probationary_capacity + (num_shards - 1)) / num_shards;
  const size_t per_protected_shard = (protected_capacity + (num_shards - 1)) / num_shards;
  for (auto s = 0; s < num_shards; ++s) {
    shards_.emplace_back(new SLRUCacheShardPair(mem_tracker_.get(), per_probationary_shard,
                                                per_protected_shard, lookups));
  }
}

Cache::UniquePendingHandle ShardedSLRUCache::Allocate(Slice key, int val_len, int charge) {
  int key_len = key.size();
  DCHECK_GE(key_len, 0);
  DCHECK_GE(val_len, 0);
  int key_len_padded = KUDU_ALIGN_UP(key_len, sizeof(void*));
  UniquePendingHandle h(reinterpret_cast<PendingHandle*>(
                            new uint8_t[sizeof(SLRUHandle)
                                + key_len_padded + val_len // the kv_data VLA data
                                - 1 // (the VLA has a 1-byte placeholder)
                            ]),
                        PendingHandleDeleter(this));
  SLRUHandle* handle = reinterpret_cast<SLRUHandle*>(h.get());
  handle->lookups = 0;
  handle->in_protected_segment.store(false, std::memory_order_relaxed);
  handle->key_length = key_len;
  handle->val_length = val_len;
  // TODO(KUDU-1091): account for the footprint of structures used by Cache's
  //                  internal housekeeping (RL handles, etc.) in case of
  //                  non-automatic charge.
  handle->charge = (charge == kAutomaticCharge) ? kudu_malloc_usable_size(h.get()) : charge;
  handle->hash = HashSlice(key);
  memcpy(handle->kv_data, key.data(), key_len);

  return h;
}

Cache::UniqueHandle ShardedSLRUCache::Lookup(const Slice& key, CacheBehavior caching) {
  const uint32_t hash = HashSlice(key);
  return UniqueHandle(
      shards_[Shard(hash)]->Lookup(key, hash, caching == EXPECT_IN_CACHE),
      HandleDeleter(this));
}

void ShardedSLRUCache::Erase(const Slice& key)  {
  const uint32_t hash = HashSlice(key);
  shards_[Shard(hash)]->Erase(key, hash);
}

Slice ShardedSLRUCache::Value(const UniqueHandle& handle) const {
  return reinterpret_cast<const SLRUHandle*>(handle.get())->value();
}

uint8_t* ShardedSLRUCache::MutableValue(UniquePendingHandle* handle) {
  return reinterpret_cast<SLRUHandle*>(handle->get())->mutable_val_ptr();
}

Cache::UniqueHandle ShardedSLRUCache::Insert(UniquePendingHandle handle,
                                             EvictionCallback* eviction_callback)  {
  SLRUHandle* h = reinterpret_cast<SLRUHandle*>(DCHECK_NOTNULL(handle.release()));
  return UniqueHandle(shards_[Shard(h->hash)]->Insert(h, eviction_callback),
                      HandleDeleter(this));
}

void ShardedSLRUCache::SetMetrics(std::unique_ptr<CacheMetrics> metrics,
                                  ExistingMetricsPolicy metrics_policy) {
  std::lock_guard l(metrics_lock_);
  if (metrics_ && metrics_policy == ExistingMetricsPolicy::kKeep) {
    CHECK(IsGTest()) << "Metrics should only be set once per Cache";
    return;
  }
  metrics_ = std::move(metrics);
  auto* metrics_ptr = dynamic_cast<SLRUCacheMetrics*>(metrics_.get());
  DCHECK(metrics_ptr != nullptr);
  for (auto& shard_pair : shards_) {
    shard_pair->SetMetrics(metrics_ptr);
  }
}

void ShardedSLRUCache::Release(Handle* handle) {
  SLRUHandle* h = reinterpret_cast<SLRUHandle*>(handle);
  shards_[Shard(h->hash)]->Release(handle);
}

void ShardedSLRUCache::Free(PendingHandle* h) {
  delete [] reinterpret_cast<uint8_t*>(h);
}

// Determine the number of bits of the hash that should be used to determine
// the cache shard. This, in turn, determines the number of shards.
int ShardedSLRUCache::DetermineShardBits() {
  int bits = PREDICT_FALSE(FLAGS_cache_force_single_shard) ?
             0 : Bits::Log2Ceiling(base::NumCPUs());
  VLOG(1) << "Will use " << (1 << bits) << " shards for SLRU cache.";
  return bits;
}

uint32_t ShardedSLRUCache::HashSlice(const Slice& s) {
  return util_hash::CityHash64(reinterpret_cast<const char *>(s.data()), s.size());
}

uint32_t ShardedSLRUCache::Shard(uint32_t hash) const {
  // Widen to uint64 before shifting, or else on a single CPU,
  // we would try to shift a uint32_t by 32 bits, which is undefined.
  return static_cast<uint64_t>(hash) >> (32 - shard_bits_);
}

template<>
ShardedSLRUCache* NewSLRUCache<Cache::MemoryType::DRAM>(size_t probationary_capacity,
                                                        size_t protected_capacity,
                                                        const std::string& id,
                                                        const uint32_t lookups) {
  return new ShardedSLRUCache(probationary_capacity, protected_capacity, id, lookups);
}

} // namespace kudu
