// Some portions copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "kudu/util/cache.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/bits.h"
#include "kudu/gutil/hash/city.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/alignment.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/malloc.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/metrics.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util_prod.h"

// Useful in tests that require accurate cache capacity accounting.
DEFINE_bool(cache_force_single_shard, false,
            "Override all cache implementations to use just one shard");
TAG_FLAG(cache_force_single_shard, hidden);

DEFINE_double(cache_memtracker_approximation_ratio, 0.01,
              "The MemTracker associated with a cache can accumulate error up to "
              "this ratio to improve performance. For tests.");
TAG_FLAG(cache_memtracker_approximation_ratio, hidden);

using RLHandle = kudu::Cache::RLHandle;
using std::atomic;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

Cache::~Cache() {
}

const Cache::ValidityFunc Cache::kInvalidateAllEntriesFunc = [](
    Slice /* key */, Slice /* value */) {
  return false;
};

const Cache::IterationFunc Cache::kIterateOverAllEntriesFunc = [](
    size_t /* valid_entries_num */, size_t /* invalid_entries_num */) {
  return true;
};

namespace {

string ToString(Cache::EvictionPolicy p) {
  switch (p) {
    case Cache::EvictionPolicy::FIFO:
      return "fifo";
    case Cache::EvictionPolicy::LRU:
      return "lru";
    case Cache::EvictionPolicy::SLRU:
      return "slru";
    default:
      LOG(FATAL) << "unexpected cache eviction policy: " << static_cast<int>(p);
      break;
  }
  return "unknown";
}

// A single shard of sharded cache.
template<Cache::EvictionPolicy policy>
class CacheShard {
 public:
  explicit CacheShard(MemTracker* tracker);
  ~CacheShard();

  // Separate from constructor so caller can easily make an array of CacheShard.
  void SetCapacity(size_t capacity) {
    capacity_ = capacity;
    max_deferred_consumption_ = capacity * FLAGS_cache_memtracker_approximation_ratio;
  }

  void SetMetrics(CacheMetrics* metrics) { metrics_ = metrics; }

  Cache::Handle* Insert(RLHandle* handle, Cache::EvictionCallback* eviction_callback);
  // Like Cache::Lookup, but with an extra "hash" parameter.
  Cache::Handle* Lookup(const Slice& key, uint32_t hash, bool caching);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  size_t Invalidate(const Cache::InvalidationControl& ctl);

 private:
  void RL_Remove(RLHandle* e);
  void RL_Append(RLHandle* e);
  // Update the recency list after a lookup operation.
  void RL_UpdateAfterLookup(RLHandle* e);
  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(RLHandle* e);
  // Call the user's eviction callback, if it exists, and free the entry.
  void FreeEntry(RLHandle* e);


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

  // Update the metrics for a lookup operation in the cache.
  void UpdateMetricsLookup(bool was_hit, bool caching);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  simple_spinlock mutex_;
  size_t usage_;

  // Dummy head of recency list.
  // rl.prev is the newest entry, rl.next is the oldest entry.
  RLHandle rl_;

  Cache::HandleTable<RLHandle> table_;

  MemTracker* mem_tracker_;
  atomic<int64_t> deferred_consumption_ { 0 };

  // Initialized based on capacity_ to ensure an upper bound on the error on the
  // MemTracker consumption.
  int64_t max_deferred_consumption_;

  CacheMetrics* metrics_;
};

template<Cache::EvictionPolicy policy>
CacheShard<policy>::CacheShard(MemTracker* tracker)
    : usage_(0),
      mem_tracker_(tracker),
      metrics_(nullptr) {
  // Make empty circular linked list.
  rl_.next = &rl_;
  rl_.prev = &rl_;
}

template<Cache::EvictionPolicy policy>
CacheShard<policy>::~CacheShard() {
  for (RLHandle* e = rl_.next; e != &rl_; ) {
    RLHandle* next = e->next;
    DCHECK_EQ(e->refs.load(std::memory_order_relaxed), 1)
        << "caller has an unreleased handle";
    if (Unref(e)) {
      FreeEntry(e);
    }
    e = next;
  }
  mem_tracker_->Consume(deferred_consumption_);
}

template<Cache::EvictionPolicy policy>
bool CacheShard<policy>::Unref(RLHandle* e) {
  DCHECK_GT(e->refs.load(std::memory_order_relaxed), 0);
  return e->refs.fetch_sub(1) == 1;
}

template<Cache::EvictionPolicy policy>
void CacheShard<policy>::FreeEntry(RLHandle* e) {
  DCHECK_EQ(e->refs.load(std::memory_order_relaxed), 0);
  if (e->eviction_callback) {
    e->eviction_callback->EvictedEntry(e->key(), e->value());
  }
  UpdateMemTracker(-static_cast<int64_t>(e->charge));
  if (PREDICT_TRUE(metrics_)) {
    metrics_->cache_usage->DecrementBy(e->charge);
    metrics_->evictions->Increment();
  }
  delete [] e;
}

template<Cache::EvictionPolicy policy>
void CacheShard<policy>::UpdateMemTracker(int64_t delta) {
  int64_t old_deferred = deferred_consumption_.fetch_add(delta);
  int64_t new_deferred = old_deferred + delta;

  if (new_deferred > max_deferred_consumption_ ||
      new_deferred < -max_deferred_consumption_) {
    int64_t to_propagate = deferred_consumption_.exchange(0, std::memory_order_relaxed);
    mem_tracker_->Consume(to_propagate);
  }
}

template<Cache::EvictionPolicy policy>
void CacheShard<policy>::UpdateMetricsLookup(bool was_hit, bool caching) {
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

template<Cache::EvictionPolicy policy>
void CacheShard<policy>::RL_Remove(RLHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
  DCHECK_GE(usage_, e->charge);
  usage_ -= e->charge;
}

template<Cache::EvictionPolicy policy>
void CacheShard<policy>::RL_Append(RLHandle* e) {
  // Make "e" newest entry by inserting just before rl_.
  e->next = &rl_;
  e->prev = rl_.prev;
  e->prev->next = e;
  e->next->prev = e;
  usage_ += e->charge;
}

template<>
void CacheShard<Cache::EvictionPolicy::FIFO>::RL_UpdateAfterLookup(RLHandle* /* e */) {
}

template<>
void CacheShard<Cache::EvictionPolicy::LRU>::RL_UpdateAfterLookup(RLHandle* e) {
  RL_Remove(e);
  RL_Append(e);
}

template<Cache::EvictionPolicy policy>
Cache::Handle* CacheShard<policy>::Lookup(const Slice& key,
                                          uint32_t hash,
                                          bool caching) {
  RLHandle* e;
  {
    std::lock_guard l(mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      e->refs.fetch_add(1, std::memory_order_relaxed);
      RL_UpdateAfterLookup(e);
    }
  }

  // Do the metrics outside the lock.
  UpdateMetricsLookup(e != nullptr, caching);

  return reinterpret_cast<Cache::Handle*>(e);
}

template<Cache::EvictionPolicy policy>
void CacheShard<policy>::Release(Cache::Handle* handle) {
  RLHandle* e = reinterpret_cast<RLHandle*>(handle);
  bool last_reference = Unref(e);
  if (last_reference) {
    FreeEntry(e);
  }
}

template<Cache::EvictionPolicy policy>
Cache::Handle* CacheShard<policy>::Insert(
    RLHandle* handle,
    Cache::EvictionCallback* eviction_callback) {
  // Set the remaining RLHandle members which were not already allocated during Allocate().
  handle->eviction_callback = eviction_callback;
  // Two refs for the handle: one from CacheShard, one for the returned handle.
  handle->refs.store(2, std::memory_order_relaxed);
  UpdateMemTracker(handle->charge);
  if (PREDICT_TRUE(metrics_)) {
    metrics_->cache_usage->IncrementBy(handle->charge);
    metrics_->inserts->Increment();
  }

  RLHandle* to_remove_head = nullptr;
  {
    std::lock_guard l(mutex_);

    RL_Append(handle);

    RLHandle* old = table_.Insert(handle);
    if (old != nullptr) {
      RL_Remove(old);
      if (Unref(old)) {
        old->next = to_remove_head;
        to_remove_head = old;
      }
    }

    while (usage_ > capacity_ && rl_.next != &rl_) {
      RLHandle* old = rl_.next;
      RL_Remove(old);
      table_.Remove(old->key(), old->hash);
      if (Unref(old)) {
        old->next = to_remove_head;
        to_remove_head = old;
      }
    }
  }

  // We free the entries here outside the mutex for performance reasons.
  while (to_remove_head != nullptr) {
    RLHandle* next = to_remove_head->next;
    FreeEntry(to_remove_head);
    to_remove_head = next;
  }

  return reinterpret_cast<Cache::Handle*>(handle);
}

template<Cache::EvictionPolicy policy>
void CacheShard<policy>::Erase(const Slice& key, uint32_t hash) {
  RLHandle* e;
  bool last_reference = false;
  {
    std::lock_guard l(mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      RL_Remove(e);
      last_reference = Unref(e);
    }
  }
  // Mutex not held here.
  // last_reference will only be true if e != NULL.
  if (last_reference) {
    FreeEntry(e);
  }
}

template<Cache::EvictionPolicy policy>
size_t CacheShard<policy>::Invalidate(const Cache::InvalidationControl& ctl) {
  size_t invalid_entry_count = 0;
  size_t valid_entry_count = 0;
  RLHandle* to_remove_head = nullptr;

  {
    std::lock_guard l(mutex_);

    // rl_.next is the oldest (a.k.a. least relevant) entry in the recency list.
    RLHandle* h = rl_.next;
    while (h != nullptr && h != &rl_ &&
           ctl.iteration_func(valid_entry_count, invalid_entry_count)) {
      if (ctl.validity_func(h->key(), h->value())) {
        // Continue iterating over the list.
        h = h->next;
        ++valid_entry_count;
        continue;
      }
      // Copy the handle slated for removal.
      RLHandle* h_to_remove = h;
      // Prepare for next iteration of the cycle.
      h = h->next;

      RL_Remove(h_to_remove);
      table_.Remove(h_to_remove->key(), h_to_remove->hash);
      if (Unref(h_to_remove)) {
        h_to_remove->next = to_remove_head;
        to_remove_head = h_to_remove;
      }
      ++invalid_entry_count;
    }
  }
  // Once removed from the lookup table and the recency list, the entries
  // with no references left must be deallocated because Cache::Release()
  // won't be called for them from elsewhere.
  while (to_remove_head != nullptr) {
    RLHandle* next = to_remove_head->next;
    FreeEntry(to_remove_head);
    to_remove_head = next;
  }
  return invalid_entry_count;
}

// Determine the number of bits of the hash that should be used to determine
// the cache shard. This, in turn, determines the number of shards.
int DetermineShardBits() {
  int bits = PREDICT_FALSE(FLAGS_cache_force_single_shard) ?
      0 : Bits::Log2Ceiling(base::NumCPUs());
  VLOG(1) << "Will use " << (1 << bits) << " shards for recency list cache.";
  return bits;
}

template<Cache::EvictionPolicy policy>
class ShardedCache : public Cache {
 public:
  explicit ShardedCache(size_t capacity, const string& id)
        : shard_bits_(DetermineShardBits()) {
    // A cache is often a singleton, so:
    // 1. We reuse its MemTracker if one already exists, and
    // 2. It is directly parented to the root MemTracker.
    mem_tracker_ = MemTracker::FindOrCreateGlobalTracker(
        -1, strings::Substitute("$0-sharded_$1_cache", id, ToString(policy)));

    int num_shards = 1 << shard_bits_;
    const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
    for (int s = 0; s < num_shards; s++) {
      unique_ptr<CacheShard<policy>> shard(
          new CacheShard<policy>(mem_tracker_.get()));
      shard->SetCapacity(per_shard);
      shards_.push_back(shard.release());
    }
  }

  ~ShardedCache() override {
    STLDeleteElements(&shards_);
  }

  void SetMetrics(std::unique_ptr<CacheMetrics> metrics,
                  ExistingMetricsPolicy metrics_policy) override {
    std::lock_guard l(metrics_lock_);
    if (metrics_ && metrics_policy == ExistingMetricsPolicy::kKeep) {
      // KUDU-2165: reuse of the Cache singleton across multiple InternalMiniCluster
      // servers causes TSAN errors. So, we'll ensure that metrics only get
      // attached once, from whichever server starts first. This has the downside
      // that, in test builds, we won't get accurate cache metrics, but that's
      // probably better than spurious failures.
      CHECK(IsGTest()) << "Metrics should only be set once per Cache";
      return;
    }
    metrics_ = std::move(metrics);
    for (auto* cache : shards_) {
      cache->SetMetrics(metrics_.get());
    }
  }

  UniqueHandle Lookup(const Slice& key, CacheBehavior caching) override {
    const uint32_t hash = HashSlice(key);
    return UniqueHandle(
        shards_[Shard(hash)]->Lookup(key, hash, caching == EXPECT_IN_CACHE),
        Cache::HandleDeleter(this));
  }

  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shards_[Shard(hash)]->Erase(key, hash);
  }

  Slice Value(const UniqueHandle& handle) const override {
    return reinterpret_cast<const RLHandle*>(handle.get())->value();
  }

  UniqueHandle Insert(UniquePendingHandle handle,
                      EvictionCallback* eviction_callback) override {
    RLHandle* h = reinterpret_cast<RLHandle*>(DCHECK_NOTNULL(handle.release()));
    return UniqueHandle(
        shards_[Shard(h->hash)]->Insert(h, eviction_callback),
        Cache::HandleDeleter(this));
  }

  UniquePendingHandle Allocate(Slice key, int val_len, int charge) override {
    int key_len = key.size();
    DCHECK_GE(key_len, 0);
    DCHECK_GE(val_len, 0);
    int key_len_padded = KUDU_ALIGN_UP(key_len, sizeof(void*));
    UniquePendingHandle h(reinterpret_cast<PendingHandle*>(
        new uint8_t[sizeof(RLHandle)
                    + key_len_padded + val_len // the kv_data VLA data
                    - 1 // (the VLA has a 1-byte placeholder)
                   ]),
        PendingHandleDeleter(this));
    RLHandle* handle = reinterpret_cast<RLHandle*>(h.get());
    handle->key_length = key_len;
    handle->val_length = val_len;
    // TODO(KUDU-1091): account for the footprint of structures used by Cache's
    //                  internal housekeeping (RL handles, etc.) in case of
    //                  non-automatic charge.
    handle->charge = (charge == kAutomaticCharge) ? kudu_malloc_usable_size(h.get())
                                                  : charge;
    handle->hash = HashSlice(key);
    memcpy(handle->kv_data, key.data(), key_len);

    return h;
  }

  uint8_t* MutableValue(UniquePendingHandle* handle) override {
    return reinterpret_cast<RLHandle*>(handle->get())->mutable_val_ptr();
  }

  size_t Invalidate(const InvalidationControl& ctl) override {
    size_t invalidated_count = 0;
    for (auto& shard: shards_) {
      invalidated_count += shard->Invalidate(ctl);
    }
    return invalidated_count;
  }

 protected:
  void Release(Handle* handle) override {
    RLHandle* h = reinterpret_cast<RLHandle*>(handle);
    shards_[Shard(h->hash)]->Release(handle);
  }

  void Free(PendingHandle* h) override {
    uint8_t* data = reinterpret_cast<uint8_t*>(h);
    delete [] data;
  }

 private:
  static inline uint32_t HashSlice(const Slice& s) {
    return util_hash::CityHash64(
      reinterpret_cast<const char *>(s.data()), s.size());
  }

  uint32_t Shard(uint32_t hash) {
    // Widen to uint64 before shifting, or else on a single CPU,
    // we would try to shift a uint32_t by 32 bits, which is undefined.
    return static_cast<uint64_t>(hash) >> (32 - shard_bits_);
  }

  shared_ptr<MemTracker> mem_tracker_;
  unique_ptr<CacheMetrics> metrics_;
  vector<CacheShard<policy>*> shards_;

  // Number of bits of hash used to determine the shard.
  const int shard_bits_;

  // Protects 'metrics_'. Used only when metrics are set, to ensure
  // that they are set only once in test environments.
  simple_spinlock metrics_lock_;
};

}  // end anonymous namespace

template<>
Cache* NewCache<Cache::EvictionPolicy::FIFO,
                Cache::MemoryType::DRAM>(size_t capacity, const std::string& id) {
  return new ShardedCache<Cache::EvictionPolicy::FIFO>(capacity, id);
}

template<>
Cache* NewCache<Cache::EvictionPolicy::LRU,
                Cache::MemoryType::DRAM>(size_t capacity, const std::string& id) {
  return new ShardedCache<Cache::EvictionPolicy::LRU>(capacity, id);
}

std::ostream& operator<<(std::ostream& os, Cache::MemoryType mem_type) {
  switch (mem_type) {
    case Cache::MemoryType::DRAM:
      os << "DRAM";
      break;
    case Cache::MemoryType::NVM:
      os << "NVM";
      break;
    default:
      os << "unknown (" << static_cast<int>(mem_type) << ")";
      break;
  }
  return os;
}

}  // namespace kudu
