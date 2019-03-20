// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// This is taken from LevelDB and evolved to fit the kudu codebase.
//
// TODO(unknown): this is pretty lock-heavy. Would be good to sub out something
// a little more concurrent.

#pragma once

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>

#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"

namespace kudu {

struct CacheMetrics;

class Cache {
 public:

  // Type of memory backing the cache's storage.
  enum class MemoryType {
    DRAM,
    NVM,
  };

  // Supported eviction policies for the cache. Eviction policy determines what
  // items to evict if the cache is at capacity when trying to accommodate
  // an extra item.
  enum class EvictionPolicy {
    // The earliest added items are evicted (a.k.a. queue).
    FIFO,

    // The least-recently-used items are evicted.
    LRU,
  };

  // Callback interface which is called when an entry is evicted from the
  // cache.
  class EvictionCallback {
   public:
    virtual void EvictedEntry(Slice key, Slice value) = 0;
    virtual ~EvictionCallback() {}
  };

  Cache() { }

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  struct Handle { };

  // Custom handle "deleter", primarily intended for use with std::unique_ptr.
  //
  // Sample usage:
  //
  //   Cache* cache = NewLRUCache(...);
  //   ...
  //   {
  //     unique_ptr<Cache::Handle, Cache::HandleDeleter> h(
  //       cache->Lookup(...), Cache::HandleDeleter(cache));
  //     ...
  //   } // 'h' is automatically released here
  //
  // Or:
  //
  //   Cache* cache = NewLRUCache(...);
  //   ...
  //   {
  //     Cache::UniqueHandle h(cache->Lookup(...), Cache::HandleDeleter(cache));
  //     ...
  //   } // 'h' is automatically released here
  //
  class HandleDeleter {
   public:
    explicit HandleDeleter(Cache* c)
        : c_(c) {
    }

    void operator()(Cache::Handle* h) const {
      if (h != nullptr) {
        c_->Release(h);
      }
    }

   private:
    Cache* c_;
  };
  typedef std::unique_ptr<Handle, HandleDeleter> UniqueHandle;

  // Opaque handle to an entry which is being prepared to be added to the cache.
  struct PendingHandle { };

  class PendingHandleDeleter {
   public:
    explicit PendingHandleDeleter(Cache* c)
        : c_(c) {
    }

    void operator()(Cache::PendingHandle* h) const {
      c_->Free(h);
    }

   private:
    Cache* c_;
  };
  typedef std::unique_ptr<PendingHandle, PendingHandleDeleter> UniquePendingHandle;

  // Passing EXPECT_IN_CACHE will increment the hit/miss metrics that track the number of times
  // blocks were requested that the users were hoping to get the block from the cache, along with
  // with the basic metrics.
  // Passing NO_EXPECT_IN_CACHE will only increment the basic metrics.
  // This helps in determining if we are effectively caching the blocks that matter the most.
  enum CacheBehavior {
    EXPECT_IN_CACHE,
    NO_EXPECT_IN_CACHE
  };

  // If the cache has no mapping for "key", returns NULL.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key, CacheBehavior caching) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual Slice Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // Set the cache metrics to update corresponding counters accordingly.
  virtual void SetMetrics(std::unique_ptr<CacheMetrics> metrics) = 0;

  // ------------------------------------------------------------
  // Insertion path
  // ------------------------------------------------------------
  //
  // Because some cache implementations (eg NVM) manage their own memory, and because we'd
  // like to read blocks directly into cache-managed memory rather than causing an extra
  // memcpy, the insertion of a new element into the cache requires two phases. First, a
  // PendingHandle is allocated with space for the value, and then it is later inserted.
  //
  // For example:
  //
  //   auto ph(cache_->Allocate("my entry", value_size, charge));
  //   if (!ReadDataFromDisk(cache_->MutableValue(ph.get())).ok()) {
  //     ... error handling ...
  //     return;
  //   }
  //   Handle* h = cache_->Insert(std::move(ph), my_eviction_callback);
  //   ...
  //   cache_->Release(h);

  // Indicates that the charge of an item in the cache should be calculated
  // based on its memory consumption.
  static constexpr int kAutomaticCharge = -1;

  // Allocate space for a new entry to be inserted into the cache.
  //
  // The provided 'key' is copied into the resulting handle object.
  // The allocated handle has enough space such that the value can
  // be written into cache_->MutableValue(handle).
  //
  // If 'charge' is not 'kAutomaticCharge', then the cache capacity will be charged
  // the explicit amount. This is useful when caching items that are small but need to
  // maintain a bounded count (eg file descriptors) rather than caring about their actual
  // memory usage. It is also useful when caching items for whom calculating
  // memory usage is a complex affair (i.e. items containing pointers to
  // additional heap allocations).
  //
  // Note that this does not mutate the cache itself: lookups will
  // not be able to find the provided key until it is inserted.
  //
  // It is possible that this will return a nullptr wrapped in a std::unique_ptr
  // if the cache is above its capacity and eviction fails to free up enough
  // space for the requested allocation.
  //
  // The returned handle owns the allocated memory.
  virtual UniquePendingHandle Allocate(Slice key, int val_len, int charge) = 0;

  // Default 'charge' should be kAutomaticCharge
  // (default arguments on virtual functions are prohibited).
  UniquePendingHandle Allocate(Slice key, int val_len) {
    return Allocate(key, val_len, kAutomaticCharge);
  }

  virtual uint8_t* MutableValue(PendingHandle* handle) = 0;

  // Commit a prepared entry into the cache.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed. This method always succeeds and returns a non-null
  // entry, since the space was reserved above.
  //
  // The 'pending' entry passed here should have been allocated using
  // Cache::Allocate() above.
  //
  // If 'eviction_callback' is non-NULL, then it will be called when the
  // entry is later evicted or when the cache shuts down.
  virtual Handle* Insert(UniquePendingHandle pending,
                         EvictionCallback* eviction_callback) = 0;

  // Free 'ptr', which must have been previously allocated using 'Allocate'.
  virtual void Free(PendingHandle* ptr) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(Cache);
};

// A template helper function to instantiate a cache of particular
// 'eviction_policy' flavor, backed by the given storage 'mem_type',
// where 'capacity' specifies the capacity of the result cache,
// and 'id' specifies its identifier.
template<Cache::EvictionPolicy eviction_policy = Cache::EvictionPolicy::LRU,
         Cache::MemoryType mem_type = Cache::MemoryType::DRAM>
Cache* NewCache(size_t capacity, const std::string& id);

// Create a new FIFO cache with a fixed size capacity. This implementation
// of Cache uses the first-in-first-out eviction policy and stored in DRAM.
template<>
Cache* NewCache<Cache::EvictionPolicy::FIFO,
                Cache::MemoryType::DRAM>(size_t capacity, const std::string& id);

// Create a new LRU cache with a fixed size capacity. This implementation
// of Cache uses the least-recently-used eviction policy and stored in DRAM.
template<>
Cache* NewCache<Cache::EvictionPolicy::LRU,
                Cache::MemoryType::DRAM>(size_t capacity, const std::string& id);

#if defined(HAVE_LIB_VMEM)
// Create a new LRU cache with a fixed size capacity. This implementation
// of Cache uses the least-recently-used eviction policy and stored in NVM.
template<>
Cache* NewCache<Cache::EvictionPolicy::LRU,
                Cache::MemoryType::NVM>(size_t capacity, const std::string& id);
#endif

// A helper method to output cache memory type into ostream.
std::ostream& operator<<(std::ostream& os, Cache::MemoryType mem_type);

} // namespace kudu
