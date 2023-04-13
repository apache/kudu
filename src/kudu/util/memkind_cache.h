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
// ShardedMemkindCache instances, hash table, etc are in DRAM. The assumption is that
// the ratio of data stored vs overhead is quite high.

#pragma once

#include <dlfcn.h>

#include <cstdint>
//#include <cstring>
//#include <iostream>
//#include <memory>
#include <mutex>
//#include <string>
//#include <utility>
//#include <vector>
//
#include <gflags/gflags.h>
#include <glog/logging.h>
//
#include "kudu/gutil/atomic_refcount.h"
//#include "kudu/gutil/atomicops.h"
//#include "kudu/gutil/bits.h"
//#include "kudu/gutil/dynamic_annotations.h"
//#include "kudu/gutil/hash/city.h"
//#include "kudu/gutil/macros.h"
//#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
//#include "kudu/gutil/sysinfo.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
//#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
//#include "kudu/util/metrics.h"
#include "kudu/util/scoped_cleanup.h"
//#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util_prod.h"

struct memkind;

//using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

// Try to dlsym() a particular symbol from 'handle', storing the result in 'ptr'
// if successful.
Status TryDlsym(void* handle, const char* sym, void** ptr);

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

  Slice key() const;

  Slice value() const;

  uint8_t* val_ptr();
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable();
  ~HandleTable();

  LRUHandle* Lookup(const Slice& key, uint32_t hash);

  LRUHandle* Insert(LRUHandle* h);

  LRUHandle* Remove(const Slice& key, uint32_t hash);

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash);

  void Resize();
};

// A single shard of sharded cache.
class MemkindCacheShard {
 public:
  explicit MemkindCacheShard();
  virtual ~MemkindCacheShard() = default;

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity);

  void SetMetrics(CacheMetrics* metrics);

  Cache::Handle* Insert(LRUHandle* e, Cache::EvictionCallback* eviction_callback);

  // Like Cache::Lookup, but with an extra "hash" parameter.
  Cache::Handle* Lookup(const Slice& key, uint32_t hash, bool caching);
  virtual void Release(Cache::Handle* handle);
  virtual void Erase(const Slice& key, uint32_t hash);
  size_t Invalidate(const Cache::InvalidationControl& ctl);
  virtual void* Allocate(size_t size);

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  virtual void FreeEntry(LRUHandle* e) = 0;

  // Evict the LRU item in the cache, adding it to the linked list
  // pointed to by 'to_remove_head'.
  void EvictOldestUnlocked(LRUHandle** to_remove_head);

  // Free all of the entries in the linked list that has to_free_head
  // as its head.
  virtual void FreeLRUEntries(LRUHandle* to_free_head);

  // Wrapper around memkind_malloc which injects failures based on a flag.
  virtual void* MemkindMalloc(size_t size) = 0;

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  MutexType mutex_;
  size_t usage_;

  HandleTable table_;

 protected:
  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(LRUHandle* e);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_;

  CacheMetrics* metrics_;
};

class ShardedMemkindCache : public Cache {
 private:
  unique_ptr<CacheMetrics> metrics_;

 protected:
  vector<unique_ptr<MemkindCacheShard>> shards_;

  static inline uint32_t HashSlice(const Slice& s) {
    return util_hash::CityHash64(
      reinterpret_cast<const char *>(s.data()), s.size());
  }

  virtual uint32_t Shard(uint32_t hash) = 0;

 public:
  explicit ShardedMemkindCache() = default;

  virtual ~ShardedMemkindCache() OVERRIDE = default;

  virtual UniqueHandle Insert(UniquePendingHandle handle,
                              Cache::EvictionCallback* eviction_callback) OVERRIDE;
  virtual UniqueHandle Lookup(const Slice& key, CacheBehavior caching) OVERRIDE;
  virtual void Release(Handle* handle) OVERRIDE;
  virtual void Erase(const Slice& key) OVERRIDE;
  virtual Slice Value(const UniqueHandle& handle) const OVERRIDE;
  virtual uint8_t* MutableValue(UniquePendingHandle* handle) OVERRIDE;

  virtual void SetMetrics(unique_ptr<CacheMetrics> metrics,
                          Cache::ExistingMetricsPolicy metrics_policy) OVERRIDE;

  size_t Invalidate(const InvalidationControl& ctl) OVERRIDE;
};

} // namespace kudu
