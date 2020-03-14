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
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
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

  // Create a new TTL cache with the specified capacity and name. All the
  // entries put into the cache have the same TTL specified by the 'entry_ttl'
  // parameter. If the 'scrubbing_period' parameter is provided with valid
  // time interval, the cache starts a periodic scrubbing thread that evicts
  // expired entries from the cache.
  TTLCache(size_t capacity_bytes,
           MonoDelta entry_ttl,
           MonoDelta scrubbing_period = {},
           size_t max_scrubbed_entries_per_pass_num = 0,
           const std::string& cache_name = "ttl-cache")
      : entry_ttl_(entry_ttl),
        scrubbing_period_(scrubbing_period),
        max_scrubbed_entries_per_pass_num_(max_scrubbed_entries_per_pass_num),
        metrics_(nullptr),
        eviction_cb_(new EvictionCallback(this)),
        cache_(NewCache<Cache::EvictionPolicy::FIFO>(capacity_bytes,
                                                     cache_name)),
        scrubbing_thread_running_(0) {
    VLOG(1) << strings::Substitute(
        "constructed TTL cache '$0' with capacity of $1",
        cache_name, capacity_bytes);
    if (scrubbing_period_.Initialized()) {
      scrubbing_thread_running_.Reset(1);
      CHECK_OK(Thread::Create(
          "cache", strings::Substitute("$0-scrubbing", cache_name),
          [this]() { this->ScrubExpiredEntries(); }, &scrubbing_thread_));
      VLOG(1) << strings::Substitute(
          "started scrubbing thread for TTL cache '$0' with period of $1",
          cache_name, scrubbing_period_.ToString());
    }
  }

  // This class is not intended to be inherited from.
  ~TTLCache() {
    if (scrubbing_period_.Initialized()) {
      scrubbing_thread_running_.CountDown();
      scrubbing_thread_->Join();
    }
  }

  // Retrieve an entry from the cache. If a non-expired entry exists
  // for the specified key, this method returns corresponding handle.
  // If there is no entry for the specified key or the entry has expired
  // at the time of the query, nullptr handle is returned.
  //
  // Cached key/value pairs may still be evicted even with an outstanding
  // handle, but a cached value won't be destroyed until the handle goes
  // out of scope.
  EntryHandle Get(const K& key) {
    auto h(cache_->Lookup(key, Cache::EXPECT_IN_CACHE));
    if (!h) {
      return EntryHandle();
    }
    auto* entry_ptr = reinterpret_cast<Entry*>(cache_->Value(h).mutable_data());
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
    Entry* entry = reinterpret_cast<Entry*>(cache_->MutableValue(&pending));
    entry->val_ptr = val.get();
    entry->exp_time = MonoTime::Now() + entry_ttl_;
    // Insert() evicts already existing entry with the same key, if any.
    auto h(cache_->Insert(std::move(pending), eviction_cb_.get()));
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
  FRIEND_TEST(TTLCacheTest, InvalidationOfExpiredEntries);

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

  // Periodically search for expired entries in the cache and remove them.
  // This method is called from a separate thread 'scrubbing_thread_'.
  void ScrubExpiredEntries() {
    MonoTime time_now;

    // TODO(aserbin): clarify why making this 'static const' behaves
    //                incorrectly when compiled with LLVM 6.0.0 at CentOS 6.6
    //                with devtoolset-3. However, it works as intended when
    //                compiled with LLVM 6.0.0 at Mac OS X 10.11.6.
    const Cache::InvalidationControl ctl = {
      [&time_now](Slice /* key */, Slice value) {
        DCHECK_EQ(sizeof(Entry), value.size());
        const auto* entry = reinterpret_cast<const Entry*>(value.data());
        // The entry expiration time is recorded in the entry's data. An entry
        // is expired once current time passed the expiration time milestone.
        return entry->exp_time > time_now;
      },
      [this](size_t valid_entry_count, size_t invalid_entry_count) {
        // The TTL cache arranges its recency list in a FIFO manner: the
        // oldest entries are in the very beginning of the list. All entries
        // have the same TTL. All the entries in the recency list past
        // a non-expired one must be not yet expired as well. So, when
        // searching for expired entries, it doesn't make sense to continue
        // iterating over the recency list once a non-expired entry
        // has been encountered.
        return valid_entry_count == 0 &&
            (max_scrubbed_entries_per_pass_num_ == 0 ||
             invalid_entry_count < max_scrubbed_entries_per_pass_num_);
      }
    };

    while (!scrubbing_thread_running_.WaitFor(scrubbing_period_)) {
      // Capture current time once, so the validity functor wouldn't need
      // to call MonoTime::Now() for every entry being processed. That also
      // makes the invalidation logic consistent with the FIFO-ordered
      // recency list of the underlying cache: once a non-expired entry is
      // encountered, it's guaranteed all the entries past it are non-expired
      // as well. With advancing 'now' the latter contract would be broken.
      time_now = MonoTime::Now();

      const auto count = cache_->Invalidate(ctl);
      VLOG(2) << strings::Substitute("invalidated $0 expired entries", count);
    }
  }

  // The validity interval for cached entries (see 'struct Entry' above).
  const MonoDelta entry_ttl_;

  // The interval to run the 'scrubbing_thread_': that's the thread to scrub
  // the cache of expired entries.
  const MonoDelta scrubbing_period_;

  // The maximum number of processed entries per one pass of the scrubbing.
  // The scrubbing of the underlying cache assumes locking access to the cache's
  // recency list, increasing contention with the concurrent usage of the cache.
  // Limiting the number of entries to invalidate at once while holding the
  // lock helps to reduce the contention.
  const size_t max_scrubbed_entries_per_pass_num_;

  // A pointer to metrics specific to the TTL cache represented by this class.
  // The raw pointer is a duplicate of the pointer owned by the underlying FIFO
  // cache (see cache_ below).
  TTLCacheMetrics* metrics_;

  // Invoked whenever a cached entry reaches zero reference count, i.e. it was
  // removed from the cache and there aren't any other references
  // floating around.
  std::unique_ptr<Cache::EvictionCallback> eviction_cb_;

  // The underlying FIFO cache instance.
  std::unique_ptr<Cache> cache_;

  // A synchronization primitive to communicate on running conditions between
  // the 'scrubbing_thread_' thread and the main one.
  CountDownLatch scrubbing_thread_running_;

  // If 'scrubbing_period_' is set to a valid time interval, this thread
  // periodically calls ScrubExpiredEntries() method until
  // 'scrubbing_thread_running_' reaches 0.
  scoped_refptr<Thread> scrubbing_thread_;

  DISALLOW_COPY_AND_ASSIGN(TTLCache);
};

} // namespace kudu
