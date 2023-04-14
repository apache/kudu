// This file is derived from cache.cc in the LevelDB project:
//
//   Some portions copyright (c) 2011 The LevelDB Authors. All rights reserved.
//   Use of this source code is governed by a BSD-style license that can be
//   found in the LICENSE file.
//
// ------------------------------------------------------------
// This file implements a cache based on the MEMKIND library (http://memkind.github.io/memkind/)
// This library makes it easy to program against persistent memory hardware by exposing an API
// which parallels malloc/free, but allocates from persistent memory instead of DRAM.
//
// We use this API to implement a cache which treats persistent memory or
// non-volatile memory as if it were a larger cheaper bank of volatile memory. We
// currently make no use of its persistence properties.
//
// Currently, we only store key/value in NVM. All other data structures such as the
// ShardedLRUCache instances, hash table, etc are in DRAM. The assumption is that
// the ratio of data stored vs overhead is quite high.

#include "kudu/util/nvm_cache.h"

#include <dlfcn.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/atomic_refcount.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bits.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/hash/city.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util_prod.h"

#ifndef MEMKIND_PMEM_MIN_SIZE
#define MEMKIND_PMEM_MIN_SIZE (1024 * 1024 * 16) // Taken from memkind 1.9.0.
#endif

struct memkind;

// Useful in tests that require accurate cache capacity accounting.
DECLARE_bool(cache_force_single_shard);

DEFINE_string(nvm_cache_path, "/pmem",
              "The path at which the NVM cache will try to allocate its memory. "
              "This can be a tmpfs or ramfs for testing purposes.");

DEFINE_bool(nvm_cache_simulate_allocation_failure, false,
            "If true, the NVM cache will inject failures in calls to memkind_malloc "
            "for testing.");
TAG_FLAG(nvm_cache_simulate_allocation_failure, unsafe);

DEFINE_double(nvm_cache_usage_ratio, 1.25,
             "A ratio to set the usage of nvm cache. The charge of an item in the nvm "
             "cache is equal to the results of memkind_malloc_usable_size multiplied by "
             "the ratio.");
TAG_FLAG(nvm_cache_usage_ratio, advanced);

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

static bool ValidateNVMCacheUsageRatio(const char* flagname, double value) {
  if (value < 1.0) {
    LOG(ERROR) << Substitute("$0 must be greater than or equal to 1.0, value $1 is invalid.",
                             flagname, value);
    return false;
  }
  if (value < 1.25) {
    LOG(WARNING) << Substitute("The value of $0 is $1, it is less than recommended "
                               "value (1.25). Due to memkind fragmentation, an improper "
                               "ratio will cause allocation failures while capacity-based "
                               "evictions are not triggered. Raise --nvm_cache_usage_ratio.",
                               flagname, value);
  }
  return true;
}
DEFINE_validator(nvm_cache_usage_ratio, &ValidateNVMCacheUsageRatio);

namespace kudu {

namespace {

// Taken together, these typedefs and this macro make it easy to call a
// memkind function:
//
//  CALL_MEMKIND(memkind_malloc, vmp_, size);
typedef int (*memkind_create_pmem)(const char*, size_t, memkind**);
typedef int (*memkind_destroy_kind)(memkind*);
typedef void* (*memkind_malloc)(memkind*, size_t);
typedef size_t (*memkind_malloc_usable_size)(memkind*, void*);
typedef void (*memkind_free)(memkind*, void*);
#define CALL_MEMKIND(func_name, ...) ((func_name)g_##func_name)(__VA_ARGS__)

// Function pointers into memkind; set by InitMemkindOps().
void* g_memkind_create_pmem;
void* g_memkind_destroy_kind;
void* g_memkind_malloc;
void* g_memkind_malloc_usable_size;
void* g_memkind_free;

// After InitMemkindOps() is called, true if memkind is available and safe
// to use, false otherwise.
bool g_memkind_available;

std::once_flag g_memkind_ops_flag;

// Try to dlsym() a particular symbol from 'handle', storing the result in 'ptr'
// if successful.
Status TryDlsym(void* handle, const char* sym, void** ptr) {
  dlerror(); // Need to clear any existing error first.
  void* ret = dlsym(handle, sym);
  char* error = dlerror();
  if (error) {
    return Status::NotSupported(Substitute("could not dlsym $0", sym), error);
  }
  *ptr = ret;
  return Status::OK();
}

// Try to dlopen() memkind and set up all the function pointers we need from it.
//
// Note: in terms of protecting ourselves against changes in memkind, we'll
// notice (and fail) if a symbol is missing, but not if it's signature has
// changed or if there's some subtle behavioral change. A scan of the memkind
// repo suggests that backwards compatibility is enforced: symbols are only
// added and behavioral changes are effected via the introduction of new symbols.
void InitMemkindOps() {
  g_memkind_available = false;

  // Use RTLD_NOW so that if any of memkind's dependencies aren't satisfied
  // (e.g. libnuma is too old and is missing symbols), we'll know up front
  // instead of during cache operations.
  void* memkind_lib = dlopen("libmemkind.so.0", RTLD_NOW);
  if (!memkind_lib) {
    LOG(WARNING) << "could not dlopen: " << dlerror();
    return;
  }
  auto cleanup = MakeScopedCleanup([&]() {
    dlclose(memkind_lib);
  });

#define DLSYM_OR_RETURN(func_name, handle) do { \
    const Status _s = TryDlsym(memkind_lib, func_name, handle); \
    if (!_s.ok()) { \
      LOG(WARNING) << _s.ToString(); \
      return; \
    } \
  } while (0)

  DLSYM_OR_RETURN("memkind_create_pmem", &g_memkind_create_pmem);
  DLSYM_OR_RETURN("memkind_destroy_kind", &g_memkind_destroy_kind);
  DLSYM_OR_RETURN("memkind_malloc", &g_memkind_malloc);
  DLSYM_OR_RETURN("memkind_malloc_usable_size", &g_memkind_malloc_usable_size);
  DLSYM_OR_RETURN("memkind_free", &g_memkind_free);
#undef DLSYM_OR_RETURN

  g_memkind_available = true;

  // Need to keep the memkind library handle open so our function pointers
  // remain loaded in memory.
  cleanup.cancel();
}

typedef simple_spinlock MutexType;

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  Cache::EvictionCallback* eviction_callback;
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  uint32_t key_length;
  uint32_t val_length;
  Atomic32 refs;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  uint8_t* kv_data;

  Slice key() const {
    return Slice(kv_data, key_length);
  }

  Slice value() const {
    return Slice(&kv_data[key_length], val_length);
  }

  uint8_t* val_ptr() {
    return &kv_data[key_length];
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

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
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

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
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
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
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
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
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

// A single shard of sharded cache.
class NvmLRUCache {
 public:
  explicit NvmLRUCache(memkind *vmp);
  ~NvmLRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  void SetMetrics(CacheMetrics* metrics) { metrics_ = metrics; }

  Cache::Handle* Insert(LRUHandle* e, Cache::EvictionCallback* eviction_callback);

  // Like Cache::Lookup, but with an extra "hash" parameter.
  Cache::Handle* Lookup(const Slice& key, uint32_t hash, bool caching);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  size_t Invalidate(const Cache::InvalidationControl& ctl);
  void* Allocate(size_t size);

 private:
  void NvmLRU_Remove(LRUHandle* e);
  void NvmLRU_Append(LRUHandle* e);
  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(LRUHandle* e);
  void FreeEntry(LRUHandle* e);

  // Evict the LRU item in the cache, adding it to the linked list
  // pointed to by 'to_remove_head'.
  void EvictOldestUnlocked(LRUHandle** to_remove_head);

  // Free all of the entries in the linked list that has to_free_head
  // as its head.
  void FreeLRUEntries(LRUHandle* to_free_head);

  // Wrapper around memkind_malloc which injects failures based on a flag.
  void* MemkindMalloc(size_t size);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  MutexType mutex_;
  size_t usage_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_;

  HandleTable table_;

  memkind* vmp_;

  CacheMetrics* metrics_;
};

NvmLRUCache::NvmLRUCache(memkind* vmp)
  : usage_(0),
  vmp_(vmp),
  metrics_(nullptr) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

NvmLRUCache::~NvmLRUCache() {
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    DCHECK_EQ(e->refs, 1);  // Error if caller has an unreleased handle
    if (Unref(e)) {
      FreeEntry(e);
    }
    e = next;
  }
}

void* NvmLRUCache::MemkindMalloc(size_t size) {
  if (PREDICT_FALSE(FLAGS_nvm_cache_simulate_allocation_failure)) {
    return nullptr;
  }
  return CALL_MEMKIND(memkind_malloc, vmp_, size);
}

bool NvmLRUCache::Unref(LRUHandle* e) {
  DCHECK_GT(ANNOTATE_UNPROTECTED_READ(e->refs), 0);
  return !base::RefCountDec(&e->refs);
}

void NvmLRUCache::FreeEntry(LRUHandle* e) {
  DCHECK_EQ(ANNOTATE_UNPROTECTED_READ(e->refs), 0);
  if (e->eviction_callback) {
    e->eviction_callback->EvictedEntry(e->key(), e->value());
  }
  if (PREDICT_TRUE(metrics_)) {
    metrics_->cache_usage->DecrementBy(e->charge);
    metrics_->evictions->Increment();
  }
  CALL_MEMKIND(memkind_free, vmp_, e);
}

// Allocate NVM memory.
void* NvmLRUCache::Allocate(size_t size) {
  return MemkindMalloc(size);
}

void NvmLRUCache::NvmLRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
  usage_ -= e->charge;
}

void NvmLRUCache::NvmLRU_Append(LRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
  usage_ += e->charge;
}

Cache::Handle* NvmLRUCache::Lookup(const Slice& key, uint32_t hash, bool caching) {
 LRUHandle* e;
  {
    std::lock_guard<MutexType> l(mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      // If an entry exists, remove the old entry from the cache
      // and re-add to the end of the linked list.
      base::RefCountInc(&e->refs);
      NvmLRU_Remove(e);
      NvmLRU_Append(e);
    }
  }

  // Do the metrics outside of the lock.
  if (metrics_) {
    metrics_->lookups->Increment();
    bool was_hit = (e != nullptr);
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

  return reinterpret_cast<Cache::Handle*>(e);
}

void NvmLRUCache::Release(Cache::Handle* handle) {
  LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
  bool last_reference = Unref(e);
  if (last_reference) {
    FreeEntry(e);
  }
}

void NvmLRUCache::EvictOldestUnlocked(LRUHandle** to_remove_head) {
  LRUHandle* old = lru_.next;
  NvmLRU_Remove(old);
  table_.Remove(old->key(), old->hash);
  if (Unref(old)) {
    old->next = *to_remove_head;
    *to_remove_head = old;
  }
}

void NvmLRUCache::FreeLRUEntries(LRUHandle* to_free_head) {
  while (to_free_head != nullptr) {
    LRUHandle* next = to_free_head->next;
    FreeEntry(to_free_head);
    to_free_head = next;
  }
}

Cache::Handle* NvmLRUCache::Insert(LRUHandle* e,
                                   Cache::EvictionCallback* eviction_callback) {
  DCHECK(e);
  LRUHandle* to_remove_head = nullptr;

  e->refs = 2;  // One from LRUCache, one for the returned handle
  e->eviction_callback = eviction_callback;
  if (PREDICT_TRUE(metrics_)) {
    metrics_->cache_usage->IncrementBy(e->charge);
    metrics_->inserts->Increment();
  }

  {
    std::lock_guard<MutexType> l(mutex_);

    NvmLRU_Append(e);

    LRUHandle* old = table_.Insert(e);
    if (old != nullptr) {
      NvmLRU_Remove(old);
      if (Unref(old)) {
        old->next = to_remove_head;
        to_remove_head = old;
      }
    }

    while (usage_ > capacity_ && lru_.next != &lru_) {
      EvictOldestUnlocked(&to_remove_head);
    }
  }

  // we free the entries here outside of mutex for
  // performance reasons
  FreeLRUEntries(to_remove_head);

  return reinterpret_cast<Cache::Handle*>(e);
}

void NvmLRUCache::Erase(const Slice& key, uint32_t hash) {
  LRUHandle* e;
  bool last_reference = false;
  {
    std::lock_guard<MutexType> l(mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      NvmLRU_Remove(e);
      last_reference = Unref(e);
    }
  }
  // mutex not held here
  // last_reference will only be true if e != nullptr
  if (last_reference) {
    FreeEntry(e);
  }
}

size_t NvmLRUCache::Invalidate(const Cache::InvalidationControl& ctl) {
  size_t invalid_entry_count = 0;
  size_t valid_entry_count = 0;
  LRUHandle* to_remove_head = nullptr;

  {
    std::lock_guard<MutexType> l(mutex_);

    // rl_.next is the oldest entry in the recency list.
    LRUHandle* h = lru_.next;
    while (h != nullptr && h != &lru_ &&
           ctl.iteration_func(valid_entry_count, invalid_entry_count)) {
      if (ctl.validity_func(h->key(), h->value())) {
        // Continue iterating over the list.
        h = h->next;
        ++valid_entry_count;
        continue;
      }
      // Copy the handle slated for removal.
      LRUHandle* h_to_remove = h;
      // Prepare for next iteration of the cycle.
      h = h->next;

      NvmLRU_Remove(h_to_remove);
      table_.Remove(h_to_remove->key(), h_to_remove->hash);
      if (Unref(h_to_remove)) {
        h_to_remove->next = to_remove_head;
        to_remove_head = h_to_remove;
      }
      ++invalid_entry_count;
    }
  }

  FreeLRUEntries(to_remove_head);

  return invalid_entry_count;
}

// Determine the number of bits of the hash that should be used to determine
// the cache shard. This, in turn, determines the number of shards.
int DetermineShardBits() {
  int bits = PREDICT_FALSE(FLAGS_cache_force_single_shard) ?
      0 : Bits::Log2Ceiling(base::NumCPUs());
  VLOG(1) << "Will use " << (1 << bits) << " shards for LRU cache.";
  return bits;
}

class ShardedLRUCache : public Cache {
 private:
  unique_ptr<CacheMetrics> metrics_;
  vector<unique_ptr<NvmLRUCache>> shards_;

  // Number of bits of hash used to determine the shard.
  const int shard_bits_;

  memkind* vmp_;

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
  explicit ShardedLRUCache(size_t capacity, const string& /*id*/, memkind* vmp)
      : shard_bits_(DetermineShardBits()),
        vmp_(vmp) {
    int num_shards = 1 << shard_bits_;
    const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
    for (int s = 0; s < num_shards; s++) {
      unique_ptr<NvmLRUCache> shard(new NvmLRUCache(vmp_));
      shard->SetCapacity(per_shard);
      shards_.emplace_back(std::move(shard));
    }
  }

  virtual ~ShardedLRUCache() {
    shards_.clear();
    // Per the note at the top of this file, our cache is entirely volatile.
    // Hence, when the cache is destructed, we delete the underlying
    // memkind pool.
    CALL_MEMKIND(memkind_destroy_kind, vmp_);
  }

  virtual UniqueHandle Insert(UniquePendingHandle handle,
                              Cache::EvictionCallback* eviction_callback) OVERRIDE {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(DCHECK_NOTNULL(handle.release()));
    return UniqueHandle(
        shards_[Shard(h->hash)]->Insert(h, eviction_callback),
        Cache::HandleDeleter(this));
  }
  virtual UniqueHandle Lookup(const Slice& key, CacheBehavior caching) OVERRIDE {
    const uint32_t hash = HashSlice(key);
    return UniqueHandle(
        shards_[Shard(hash)]->Lookup(key, hash, caching == EXPECT_IN_CACHE),
        Cache::HandleDeleter(this));
  }
  virtual void Release(Handle* handle) OVERRIDE {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shards_[Shard(h->hash)]->Release(handle);
  }
  virtual void Erase(const Slice& key) OVERRIDE {
    const uint32_t hash = HashSlice(key);
    shards_[Shard(hash)]->Erase(key, hash);
  }
  virtual Slice Value(const UniqueHandle& handle) const OVERRIDE {
    return reinterpret_cast<const LRUHandle*>(handle.get())->value();
  }
  virtual uint8_t* MutableValue(UniquePendingHandle* handle) OVERRIDE {
    return reinterpret_cast<LRUHandle*>(handle->get())->val_ptr();
  }

  virtual void SetMetrics(unique_ptr<CacheMetrics> metrics,
                          Cache::ExistingMetricsPolicy metrics_policy) OVERRIDE {
    if (metrics_ && metrics_policy == Cache::ExistingMetricsPolicy::kKeep) {
      CHECK(IsGTest()) << "Metrics should only be set once per Cache";
      return;
    }
    metrics_ = std::move(metrics);
    for (const auto& shard : shards_) {
      shard->SetMetrics(metrics_.get());
    }
  }
  virtual UniquePendingHandle Allocate(Slice key, int val_len, int charge) OVERRIDE {
    int key_len = key.size();
    DCHECK_GE(key_len, 0);
    DCHECK_GE(val_len, 0);

    // Try allocating from each of the shards -- if memkind is tight,
    // this can cause eviction, so we might have better luck in different
    // shards.
    for (const auto& shard : shards_) {
      UniquePendingHandle ph(static_cast<PendingHandle*>(
          shard->Allocate(sizeof(LRUHandle) + key_len + val_len)),
          Cache::PendingHandleDeleter(this));
      if (ph) {
        LRUHandle* handle = reinterpret_cast<LRUHandle*>(ph.get());
        uint8_t* buf = reinterpret_cast<uint8_t*>(ph.get());
        handle->kv_data = &buf[sizeof(LRUHandle)];
        handle->val_length = val_len;
        handle->key_length = key_len;
        // Multiply the results of memkind_malloc_usable_size by a ratio
        // due to the fragmentation is not counted in.
        handle->charge = (charge == kAutomaticCharge) ?
            CALL_MEMKIND(memkind_malloc_usable_size, vmp_, buf) * FLAGS_nvm_cache_usage_ratio :
            charge;
        handle->hash = HashSlice(key);
        memcpy(handle->kv_data, key.data(), key.size());
        return ph;
      }
    }
    // TODO(unknown): increment a metric here on allocation failure.
    return UniquePendingHandle(nullptr, Cache::PendingHandleDeleter(this));
  }

  virtual void Free(PendingHandle* ph) OVERRIDE {
    CALL_MEMKIND(memkind_free, vmp_, ph);
  }

  size_t Invalidate(const InvalidationControl& ctl) override {
    size_t invalidated_count = 0;
    for (const auto& shard: shards_) {
      invalidated_count += shard->Invalidate(ctl);
    }
    return invalidated_count;
  }
};

} // end anonymous namespace

template<>
Cache* NewCache<Cache::EvictionPolicy::LRU,
                Cache::MemoryType::NVM>(size_t capacity, const std::string& id) {
  std::call_once(g_memkind_ops_flag, InitMemkindOps);

  // TODO(adar): we should plumb the failure up the call stack, but at the time
  // of writing the NVM cache is only usable by the block cache, and its use of
  // the singleton pattern prevents the surfacing of errors.
  CHECK(g_memkind_available) << "Memkind not available!";

  // memkind_create_pmem() will fail if the capacity is too small, but with
  // an inscrutable error. So, we'll check ourselves.
  CHECK_GE(capacity, MEMKIND_PMEM_MIN_SIZE)
    << "configured capacity " << capacity << " bytes is less than "
    << "the minimum capacity for an NVM cache: " << MEMKIND_PMEM_MIN_SIZE;

  LOG(INFO) << 1 / FLAGS_nvm_cache_usage_ratio * 100 << "% capacity "
            << "from nvm block cache is available due to memkind fragmentation.";

  memkind* vmp;
  int err = CALL_MEMKIND(memkind_create_pmem, FLAGS_nvm_cache_path.c_str(), capacity, &vmp);
  // If we cannot create the cache pool we should not retry.
  PLOG_IF(FATAL, err) << "Could not initialize NVM cache library in path "
                           << FLAGS_nvm_cache_path.c_str();

  return new ShardedLRUCache(capacity, id, vmp);
}

bool CanUseNVMCacheForTests() {
  std::call_once(g_memkind_ops_flag, InitMemkindOps);
  return g_memkind_available;
}

} // namespace kudu
