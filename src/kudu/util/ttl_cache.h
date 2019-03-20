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

#include <cstddef>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/slice.h"
#include "kudu/util/ttl_cache_metrics.h"

namespace kudu {

// Limited capacity cache with FIFO eviction policy, where the validity
// of cached entries is based on their expiration time. The expiration time for
// an entry is set upon adding it into the cache, where all entries have
// the same static TTL specified at cache's creation time.
//
// When TTLCache is at capacity and needs to accommodate a new entry, the
// underlying cache evicts already existing entries regardless of
// their expiration status using the FIFO approach: the earliest added entry
// is evicted first. Since the TTL setting is static and applies to every entry
// in the cache, the FIFO eviction policy naturally evicts entries with earlier
// expiration times prior to evicting entries with later expiration times.
// The eviction of entries in the underlying cache continues until
// there is enough space to accommodate the newly added entry
// or no entries are left in the cache.
//
// TODO(aserbin): add an option to evict expired entries on a periodic timer
// TODO(aserbin): add the 'move semantics option' for the key in Get()/Put()
template<typename K, typename V>
class TTLCache {
 public:

  // The handle for an entry in the cache.
  class EntryHandle {
   public:
    // Construct an empty/nullptr handle.
    EntryHandle()
        : ptr_(nullptr),
          handle_(Cache::UniqueHandle(nullptr,
                                      Cache::HandleDeleter(nullptr))) {
    }

    ~EntryHandle() = default;

    EntryHandle(EntryHandle&& other) noexcept
        : EntryHandle() {
      std::swap(ptr_, other.ptr_);
      handle_ = std::move(other.handle_);
    }

    EntryHandle& operator=(EntryHandle&& other) noexcept {
      ptr_ = other.ptr_;
      other.ptr_ = nullptr;
      handle_ = std::move(other.handle_);
      return *this;
    }

    // Copying of entry handles is explicitly prohibited.
    EntryHandle(const EntryHandle&) = delete;
    EntryHandle& operator=(const EntryHandle&) = delete;

    explicit operator bool() const noexcept {
      return ptr_ != nullptr;
    }

    // No modifications of the value are allowed through EntryHandle.
    const V& value() const {
      DCHECK(ptr_);
      return *ptr_;
    }

   private:
    friend class TTLCache;

    EntryHandle(V* ptr, Cache::UniqueHandle handle)
        : ptr_(ptr),
          handle_(std::move(handle)) {
      DCHECK((ptr_ != nullptr && handle_) ||
             (ptr_ == nullptr && !handle_));
    }

    V* ptr_;
    Cache::UniqueHandle handle_;
  };

  // Create a new TTL cache with the specified capacity and name. The cache's
  // metric gauges are attached to the metric entity specified via the 'entity'
  // parameter if the parameter is non-null.
  TTLCache(size_t capacity_bytes,
           MonoDelta entry_ttl,
           const std::string& cache_name = "")
      : entry_ttl_(entry_ttl),
        metrics_(nullptr),
        eviction_cb_(new EvictionCallback(this)),
        cache_(NewCache<Cache::EvictionPolicy::FIFO>(capacity_bytes,
                                                     cache_name)) {
    VLOG(1) << strings::Substitute(
        "constructed TTL cache '$0' with capacity of $1",
        cache_name, capacity_bytes);
  }

  // This class is not intended to be inherited from.
  ~TTLCache() = default;

  // Retrieve an entry from the cache. If a non-expired entry exists
  // for the specified key, this method returns corresponding handle.
  // If there is no entry for the specified key or the entry has expired
  // at the time of the query, nullptr handle is returned.
  //
  // Cached key/value pairs may still be evicted even with an outstanding
  // handle, but a cached value won't be destroyed until the handle goes
  // out of scope.
  EntryHandle Get(const K& key) {
    Cache::UniqueHandle h(cache_->Lookup(key, Cache::EXPECT_IN_CACHE),
                          Cache::HandleDeleter(cache_.get()));
    if (!h) {
      return EntryHandle();
    }
    auto* entry_ptr = reinterpret_cast<Entry*>(
        cache_->Value(h.get()).mutable_data());
    DCHECK(entry_ptr);
    if (entry_ptr->exp_time < MonoTime::Now()) {
      // If the entry has expired already, return null handle. The underlying
      // cache will purge the expired entry when necessary upon accommodating
      // new entries being added into the cache, if any.
      if (PREDICT_TRUE(metrics_ != nullptr)) {
        metrics_->cache_hits_expired->Increment();
      }
      return EntryHandle();
    }
    return EntryHandle(DCHECK_NOTNULL(entry_ptr->val_ptr), std::move(h));
  }

  // For the specified key, add an entry into the cache or replace already
  // existing one. The 'charge' parameter specifies the charge to associate
  // with the entry with regard to the cache's capacity. This method returns
  // a smart pointer for the entry's handle: the same return type as for the
  // Get() method is used to support similar pattern of using cache's handles
  // while both retrieving and storing entries in the cache.
  //
  // Put() evicts already existing entry with the same key (if any), effectively
  // replacing corresponding entry in the cache as the result.
  EntryHandle Put(const K& key,
                  std::unique_ptr<V> val,
                  int charge = Cache::kAutomaticCharge) {
    auto pending(cache_->Allocate(key, sizeof(Entry), charge));
    CHECK(pending);
    Entry* entry = reinterpret_cast<Entry*>(cache_->MutableValue(pending.get()));
    entry->val_ptr = val.get();
    entry->exp_time = MonoTime::Now() + entry_ttl_;
    // Insert() evicts already existing entry with the same key, if any.
    Cache::UniqueHandle h(cache_->Insert(std::move(pending), eviction_cb_.get()),
                          Cache::HandleDeleter(cache_.get()));
    // The cache takes care of the entry from this point: deallocation of the
    // resources passed via 'val' parameter is performed by the eviction callback.
    return EntryHandle(DCHECK_NOTNULL(val.release()), std::move(h));
  }

  // Set metrics for the cache. This method mimics the signature of
  // Cache::SetMetrics() but consumes TTL-specific metrics.
  void SetMetrics(std::unique_ptr<TTLCacheMetrics> metrics) {
    // Keep a copy of the pointer to the metrics: the FIFO cache is the owner.
    metrics_ = metrics.get();
    cache_->SetMetrics(std::move(metrics));
  }

 private:
  friend class EvictionCallback;

  // An entry to store in the underlying FIFO cache.
  struct Entry {
    // Raw pointer to the 'value'. The cache owns the memory allocated and takes
    // care of deallocating it upon call of EvictionCallback::EvictedEntry()
    // by the underlying FIFO cache instance.
    V* val_ptr;
    // Expiration time of the entry.
    MonoTime exp_time;
  };

  // Callback invoked by the underlying FIFO cache when a cached entry's
  // reference count reaches zero and is evicted.
  class EvictionCallback : public Cache::EvictionCallback {
   public:
    explicit EvictionCallback(TTLCache* cache)
        : cache_(cache) {
      DCHECK(cache_);
    }

    void EvictedEntry(Slice key, Slice val) override {
      VLOG(2) << strings::Substitute("EvictedEntry callback for key '$0'",
                                     key.ToString());
      auto* entry_ptr = reinterpret_cast<Entry*>(val.mutable_data());
      DCHECK(entry_ptr);
      if (PREDICT_TRUE(cache_->metrics_ != nullptr) &&
          entry_ptr->exp_time < MonoTime::Now()) {
        cache_->metrics_->evictions_expired->Increment();
      }
      delete entry_ptr->val_ptr;
    }

   private:
    TTLCache* cache_;
    DISALLOW_COPY_AND_ASSIGN(EvictionCallback);
  };

  // The validity interval for cached entries (see 'struct Entry' above).
  const MonoDelta entry_ttl_;

  // A pointer to metrics specific to the TTL cache represented by this class.
  // The raw pointer is a duplicate of the pointer owned by the underlying FIFO
  // cache (see cache_ below).
  TTLCacheMetrics* metrics_;

  // Invoked whenever a cached entry reaches zero references, i.e. it was
  // removed from the cache and there aren't any other references
  // floating around.
  std::unique_ptr<Cache::EvictionCallback> eviction_cb_;

  // The underlying FIFO cache instance.
  std::unique_ptr<Cache> cache_;

  DISALLOW_COPY_AND_ASSIGN(TTLCache);
};

} // namespace kudu
