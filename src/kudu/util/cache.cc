// Some portions copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "kudu/util/cache.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
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

#if !defined(__APPLE__)
#include "kudu/util/nvm_cache.h"
#endif

// Useful in tests that require accurate cache capacity accounting.
DEFINE_bool(cache_force_single_shard, false,
            "Override all cache implementations to use just one shard");
TAG_FLAG(cache_force_single_shard, hidden);

DEFINE_double(cache_memtracker_approximation_ratio, 0.01,
              "The MemTracker associated with a cache can accumulate error up to "
              "this ratio to improve performance. For tests.");
TAG_FLAG(cache_memtracker_approximation_ratio, hidden);

using std::atomic;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace kudu {

Cache::~Cache() {
}

namespace {

typedef simple_spinlock MutexType;

// Recency list cache implementations (FIFO, LRU, etc.)

// Recency list handle. An entry is a variable length heap-allocated structure.
// Entries are kept in a circular doubly linked list ordered by some recency
// criterion (e.g., access time for LRU policy, insertion time for FIFO policy).
struct RLHandle {
  Cache::EvictionCallback* eviction_callback;
  RLHandle* next_hash;
  RLHandle* next;
  RLHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  uint32_t key_length;
  uint32_t val_length;
  std::atomic<int32_t> refs;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons

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
    return const_cast<RLHandle*>(this)->mutable_val_ptr();
  }

  Slice value() const {
    return Slice(val_ptr(), val_length);
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  RLHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  RLHandle* Insert(RLHandle* h) {
    RLHandle** ptr = FindPointer(h->key(), h->hash);
    RLHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  RLHandle* Remove(const Slice& key, uint32_t hash) {
    RLHandle** ptr = FindPointer(key, hash);
    RLHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  RLHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  RLHandle** FindPointer(const Slice& key, uint32_t hash) {
    RLHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 16;
    while (new_length < elems_ * 1.5) {
      new_length *= 2;
    }
    auto new_list = new RLHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      RLHandle* h = list_[i];
      while (h != nullptr) {
        RLHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        RLHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    DCHECK_EQ(elems_, count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

string ToString(Cache::EvictionPolicy p) {
  switch (p) {
    case Cache::EvictionPolicy::FIFO:
      return "fifo";
    case Cache::EvictionPolicy::LRU:
      return "lru";
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

  // Separate from constructor so caller can easily make an array of CacheShard
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
  MutexType mutex_;
  size_t usage_;

  // Dummy head of recency list.
  // rl.prev is newest entry, rl.next is oldest entry.
  RLHandle rl_;

  HandleTable table_;

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
    std::lock_guard<MutexType> l(mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      e->refs.fetch_add(1, std::memory_order_relaxed);
      RL_UpdateAfterLookup(e);
    }
  }

  // Do the metrics outside of the lock.
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
  // Set the remaining RLHandle members which were not already allocated during
  // Allocate().
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
    std::lock_guard<MutexType> l(mutex_);

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

  // we free the entries here outside of mutex for
  // performance reasons
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
    std::lock_guard<MutexType> l(mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      RL_Remove(e);
      last_reference = Unref(e);
    }
  }
  // mutex not held here
  // last_reference will only be true if e != NULL
  if (last_reference) {
    FreeEntry(e);
  }
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
 private:
  shared_ptr<MemTracker> mem_tracker_;
  unique_ptr<CacheMetrics> metrics_;
  vector<CacheShard<policy>*> shards_;

  // Number of bits of hash used to determine the shard.
  const int shard_bits_;

  // Protects 'metrics_'. Used only when metrics are set, to ensure
  // that they are set only once in test environments.
  MutexType metrics_lock_;

  static inline uint32_t HashSlice(const Slice& s) {
    return util_hash::CityHash64(
      reinterpret_cast<const char *>(s.data()), s.size());
  }

  uint32_t Shard(uint32_t hash) {
    // Widen to uint64 before shifting, or else on a single CPU,
    // we would try to shift a uint32_t by 32 bits, which is undefined.
    return static_cast<uint64_t>(hash) >> (32 - shard_bits_);
  }

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

  virtual ~ShardedCache() {
    STLDeleteElements(&shards_);
  }

  Handle* Insert(PendingHandle* handle,
                 Cache::EvictionCallback* eviction_callback) override {
    RLHandle* h = reinterpret_cast<RLHandle*>(DCHECK_NOTNULL(handle));
    return shards_[Shard(h->hash)]->Insert(h, eviction_callback);
  }
  Handle* Lookup(const Slice& key, CacheBehavior caching) override {
    const uint32_t hash = HashSlice(key);
    return shards_[Shard(hash)]->Lookup(key, hash, caching == EXPECT_IN_CACHE);
  }
  void Release(Handle* handle) override {
    RLHandle* h = reinterpret_cast<RLHandle*>(handle);
    shards_[Shard(h->hash)]->Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shards_[Shard(hash)]->Erase(key, hash);
  }
  Slice Value(Handle* handle) override {
    return reinterpret_cast<RLHandle*>(handle)->value();
  }
  void SetMetrics(std::unique_ptr<CacheMetrics> metrics) override {
    // TODO(KUDU-2165): reuse of the Cache singleton across multiple MiniCluster servers
    // causes TSAN errors. So, we'll ensure that metrics only get attached once, from
    // whichever server starts first. This has the downside that, in test builds, we won't
    // get accurate cache metrics, but that's probably better than spurious failures.
    std::lock_guard<simple_spinlock> l(metrics_lock_);
    if (metrics_) {
      CHECK(IsGTest()) << "Metrics should only be set once per Cache singleton";
      return;
    }
    metrics_ = std::move(metrics);
    for (auto* cache : shards_) {
      cache->SetMetrics(metrics_.get());
    }
  }

  PendingHandle* Allocate(Slice key, int val_len, int charge) override {
    int key_len = key.size();
    DCHECK_GE(key_len, 0);
    DCHECK_GE(val_len, 0);
    int key_len_padded = KUDU_ALIGN_UP(key_len, sizeof(void*));
    uint8_t* buf = new uint8_t[sizeof(RLHandle)
                               + key_len_padded + val_len // the kv_data VLA data
                               - 1 // (the VLA has a 1-byte placeholder)
                               ];
    RLHandle* handle = reinterpret_cast<RLHandle*>(buf);
    handle->key_length = key_len;
    handle->val_length = val_len;
    handle->charge = (charge == kAutomaticCharge) ? kudu_malloc_usable_size(buf) : charge;
    handle->hash = HashSlice(key);
    memcpy(handle->kv_data, key.data(), key_len);

    return reinterpret_cast<PendingHandle*>(handle);
  }

  void Free(PendingHandle* h) override {
    uint8_t* data = reinterpret_cast<uint8_t*>(h);
    delete [] data;
  }

  uint8_t* MutableValue(PendingHandle* h) override {
    return reinterpret_cast<RLHandle*>(h)->mutable_val_ptr();
  }
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

#if defined(HAVE_LIB_VMEM)
template<>
Cache* NewCache<Cache::EvictionPolicy::LRU,
                Cache::MemoryType::NVM>(size_t capacity, const std::string& id) {
  return NewLRUNvmCache(capacity, id);
}
#endif

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
