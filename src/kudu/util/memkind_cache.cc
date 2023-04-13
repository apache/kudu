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

#include "kudu/util/memkind_cache.h"

#include <dlfcn.h>

#include <cstdint>
//#include <cstring>
//#include <iostream>
//#include <memory>
//#include <mutex>
//#include <string>
//#include <utility>
//#include <vector>
//
#include <gflags/gflags.h>
#include <glog/logging.h>
//
//#include "kudu/gutil/atomic_refcount.h"
//#include "kudu/gutil/atomicops.h"
//#include "kudu/gutil/bits.h"
//#include "kudu/gutil/dynamic_annotations.h"
//#include "kudu/gutil/hash/city.h"
//#include "kudu/gutil/macros.h"
//#include "kudu/gutil/port.h"
//#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
//#include "kudu/gutil/sysinfo.h"
#include "kudu/util/cache.h"
//#include "kudu/util/cache_metrics.h"
//#include "kudu/util/flag_tags.h"
//#include "kudu/util/locks.h"
//#include "kudu/util/metrics.h"
#include "kudu/util/scoped_cleanup.h"
//#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util_prod.h"

//using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

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

Slice LRUHandle::key() const {
  return Slice(kv_data, key_length);
}

Slice LRUHandle::value() const {
  return Slice(&kv_data[key_length], val_length);
}

uint8_t* LRUHandle::val_ptr() {
  return &kv_data[key_length];
}

HandleTable::HandleTable()
    : length_(0),
      elems_(0),
      list_(nullptr) {
  Resize();
}

HandleTable::~HandleTable() {
  delete[] list_;
}

LRUHandle* HandleTable::Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

LRUHandle* HandleTable::Insert(LRUHandle* h) {
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

LRUHandle* HandleTable::Remove(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = FindPointer(key, hash);
  LRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

LRUHandle** HandleTable::FindPointer(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = &list_[hash & (length_ - 1)];
  while (*ptr != nullptr &&
         ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void HandleTable::Resize() {
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

void MemkindCacheShard::SetCapacity(size_t capacity) {
  capacity_ = capacity;
}

void MemkindCacheShard::SetMetrics(CacheMetrics* metrics) {
  metrics_ = metrics;
}

MemkindCacheShard::MemkindCacheShard()
  : usage_(0),
  metrics_(nullptr) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

bool MemkindCacheShard::Unref(LRUHandle* e) {
  DCHECK_GT(ANNOTATE_UNPROTECTED_READ(e->refs), 0);
  return !base::RefCountDec(&e->refs);
}

// Allocate NVM memory.
void* MemkindCacheShard::Allocate(size_t size) {
  return MemkindMalloc(size);
}

void MemkindCacheShard::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
  usage_ -= e->charge;
}

void MemkindCacheShard::LRU_Append(LRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
  usage_ += e->charge;
}

Cache::Handle* MemkindCacheShard::Lookup(const Slice& key, uint32_t hash, bool caching) {
 LRUHandle* e;
  {
    std::lock_guard<MutexType> l(mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      // If an entry exists, remove the old entry from the cache
      // and re-add to the end of the linked list.
      base::RefCountInc(&e->refs);
      LRU_Remove(e);
      LRU_Append(e);
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

void MemkindCacheShard::Release(Cache::Handle* handle) {
  LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
  bool last_reference = Unref(e);
  if (last_reference) {
    FreeEntry(e);
  }
}

void MemkindCacheShard::EvictOldestUnlocked(LRUHandle** to_remove_head) {
  LRUHandle* old = lru_.next;
  LRU_Remove(old);
  table_.Remove(old->key(), old->hash);
  if (Unref(old)) {
    old->next = *to_remove_head;
    *to_remove_head = old;
  }
}

void MemkindCacheShard::FreeLRUEntries(LRUHandle* to_free_head) {
  while (to_free_head != nullptr) {
    LRUHandle* next = to_free_head->next;
    FreeEntry(to_free_head);
    to_free_head = next;
  }
}

Cache::Handle* MemkindCacheShard::Insert(LRUHandle* e,
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

    LRU_Append(e);

    LRUHandle* old = table_.Insert(e);
    if (old != nullptr) {
      LRU_Remove(old);
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

void MemkindCacheShard::Erase(const Slice& key, uint32_t hash) {
  LRUHandle* e;
  bool last_reference = false;
  {
    std::lock_guard<MutexType> l(mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      LRU_Remove(e);
      last_reference = Unref(e);
    }
  }
  // mutex not held here
  // last_reference will only be true if e != nullptr
  if (last_reference) {
    FreeEntry(e);
  }
}

size_t MemkindCacheShard::Invalidate(const Cache::InvalidationControl& ctl) {
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

      LRU_Remove(h_to_remove);
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

Cache::UniqueHandle ShardedMemkindCache::Insert(UniquePendingHandle handle,
                                         Cache::EvictionCallback* eviction_callback) {
  LRUHandle* h = reinterpret_cast<LRUHandle*>(DCHECK_NOTNULL(handle.release()));
  return Cache::UniqueHandle(
      shards_[Shard(h->hash)]->Insert(h, eviction_callback),
      Cache::HandleDeleter(this));
}
Cache::UniqueHandle ShardedMemkindCache::Lookup(const Slice& key, CacheBehavior caching) {
  const uint32_t hash = HashSlice(key);
  return Cache::UniqueHandle(
      shards_[Shard(hash)]->Lookup(key, hash, caching == EXPECT_IN_CACHE),
      Cache::HandleDeleter(this));
}
void ShardedMemkindCache::Release(Handle* handle) {
  LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
  shards_[Shard(h->hash)]->Release(handle);
}
void ShardedMemkindCache::Erase(const Slice& key) {
  const uint32_t hash = HashSlice(key);
  shards_[Shard(hash)]->Erase(key, hash);
}
Slice ShardedMemkindCache::Value(const Cache::UniqueHandle& handle) const {
  return reinterpret_cast<const LRUHandle*>(handle.get())->value();
}
uint8_t* ShardedMemkindCache::MutableValue(UniquePendingHandle* handle) {
  return reinterpret_cast<LRUHandle*>(handle->get())->val_ptr();
}

void ShardedMemkindCache::SetMetrics(unique_ptr<CacheMetrics> metrics,
                                     Cache::ExistingMetricsPolicy metrics_policy) {
  if (metrics_ && metrics_policy == Cache::ExistingMetricsPolicy::kKeep) {
    CHECK(IsGTest()) << "Metrics should only be set once per Cache";
    return;
  }
  metrics_ = std::move(metrics);
  for (const auto& shard : shards_) {
    shard->SetMetrics(metrics_.get());
  }
}

size_t ShardedMemkindCache::Invalidate(const InvalidationControl& ctl) {
  size_t invalidated_count = 0;
  for (const auto& shard: shards_) {
    invalidated_count += shard->Invalidate(ctl);
  }
  return invalidated_count;
}

} // namespace kudu
