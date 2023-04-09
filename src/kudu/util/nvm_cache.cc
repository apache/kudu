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

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
//#include <vector>

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
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/memkind_cache.h"
#include "kudu/util/metrics.h"
#include "kudu/util/slice.h"
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
//using std::vector;
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

// A single shard of sharded cache.
class NvmLRUCache: public MemkindCacheShard {
 public:
  explicit NvmLRUCache(memkind *vmp);
  ~NvmLRUCache() OVERRIDE;

 private:
  void FreeEntry(LRUHandle* e);

  // Wrapper around memkind_malloc which injects failures based on a flag.
  void* MemkindMalloc(size_t size);

  memkind* vmp_;
};

NvmLRUCache::NvmLRUCache(memkind* vmp)
  : MemkindCacheShard(),
  vmp_(vmp) {}

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

// Determine the number of bits of the hash that should be used to determine
// the cache shard. This, in turn, determines the number of shards.
int DetermineShardBits() {
  int bits = PREDICT_FALSE(FLAGS_cache_force_single_shard) ?
      0 : Bits::Log2Ceiling(base::NumCPUs());
  VLOG(1) << "Will use " << (1 << bits) << " shards for LRU cache.";
  return bits;
}

class ShardedLRUCache : public ShardedMemkindCache {
 private:
  // Number of bits of hash used to determine the shard.
  const int shard_bits_;

  memkind* vmp_;

  uint32_t Shard(uint32_t hash) {
    // Widen to uint64 before shifting, or else on a single CPU,
    // we would try to shift a uint32_t by 32 bits, which is undefined.
    return static_cast<uint64_t>(hash) >> (32 - shard_bits_);
  }

 public:
  explicit ShardedLRUCache(size_t capacity, const string& /*id*/, memkind* vmp)
      : ShardedMemkindCache(),
        shard_bits_(DetermineShardBits()),
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
