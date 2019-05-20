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
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <utility>

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
    virtual ~EvictionCallback() = default;
  };

  Cache() = default;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Set the cache metrics to update corresponding counters accordingly.
  virtual void SetMetrics(std::unique_ptr<CacheMetrics> metrics) = 0;

  // Opaque handle to an entry stored in the cache.
  struct Handle { };

  // Custom deleter: intended for use with std::unique_ptr<Handle>.
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

    Cache* cache() const {
      return c_;
    }

   private:
    Cache* c_;
  };

  // UniqueHandle -- a wrapper around opaque Handle structure to facilitate
  // automatic reference counting of cache's handles.
  typedef std::unique_ptr<Handle, HandleDeleter> UniqueHandle;

  // Opaque handle to an entry which is being prepared to be added to the cache.
  struct PendingHandle { };

  // Custom deleter: intended for use with std::unique_ptr<PendingHandle>.
  class PendingHandleDeleter {
   public:
    explicit PendingHandleDeleter(Cache* c)
        : c_(c) {
    }

    void operator()(Cache::PendingHandle* h) const {
      if (h != nullptr) {
        c_->Free(h);
      }
    }

    Cache* cache() const {
      return c_;
    }

   private:
    Cache* c_;
  };

  // UniquePendingHandle -- a wrapper around opaque PendingHandle structure
  // to facilitate automatic reference counting newly allocated cache's handles.
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
  // Else return a handle that corresponds to the mapping.
  //
  // Sample usage:
  //
  //   unique_ptr<Cache> cache(NewCache(...));
  //   ...
  //   {
  //     Cache::UniqueHandle h(cache->Lookup(...)));
  //     ...
  //   } // 'h' is automatically released here
  //
  // Or:
  //
  //   unique_ptr<Cache> cache(NewCache(...));
  //   ...
  //   {
  //     auto h(cache->Lookup(...)));
  //     ...
  //   } // 'h' is automatically released here
  //
  virtual UniqueHandle Lookup(const Slice& key, CacheBehavior caching) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // Return the value encapsulated in a raw handle returned by a successful
  // Lookup().
  virtual Slice Value(const UniqueHandle& handle) const = 0;


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
  //   if (!ReadDataFromDisk(cache_->MutableValue(&ph)).ok()) {
  //     ... error handling ...
  //     return;
  //   }
  //   UniqueHandle h(cache_->Insert(std::move(ph), my_eviction_callback));
  //   ...
  //   // 'h' is automatically released.

  // Indicates that the charge of an item in the cache should be calculated
  // based on its memory consumption.
  static constexpr int kAutomaticCharge = -1;

  // Allocate space for a new entry to be inserted into the cache.
  //
  // The provided 'key' is copied into the resulting handle object.
  // The allocated handle has enough space such that the value can
  // be written into cache_->MutableValue(&handle).
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

  virtual uint8_t* MutableValue(UniquePendingHandle* handle) = 0;

  // Commit a prepared entry into the cache.
  //
  // Returns a handle that corresponds to the mapping. This method always
  // succeeds and returns a non-null entry, since the space was reserved above.
  //
  // The 'pending' entry passed here should have been allocated using
  // Cache::Allocate() above.
  //
  // If 'eviction_callback' is non-NULL, then it will be called when the
  // entry is later evicted or when the cache shuts down.
  virtual UniqueHandle Insert(UniquePendingHandle pending,
                              EvictionCallback* eviction_callback) = 0;

  // Forward declaration to simplify the layout of types/typedefs needed for the
  // Invalidate() method while trying to adhere to the code style guide.
  struct InvalidationControl;

  // Invalidate cache's entries, effectively evicting non-valid ones from the
  // cache. The invalidation process iterates over the cache's recency list(s),
  // from best candidate for eviction to the worst.
  //
  // The provided control structure 'ctl' is responsible for the following:
  //   * determine whether an entry is valid or not
  //   * determine how to iterate over the entries in the cache's recency list
  //
  // NOTE: The invalidation process might hold a lock while iterating over
  //       the cache's entries. Using proper IterationFunc might help to reduce
  //       contention with the concurrent request for the cache's contents.
  //       See the in-line documentation for IterationFunc for more details.
  virtual size_t Invalidate(const InvalidationControl& ctl) = 0;

  // Functor to define a criterion on a cache entry's validity. Upon call
  // of Cache::Invalidate() method, if the functor returns 'false' for the
  // specified key and value, the cache evicts the entry, otherwise the entry
  // stays in the cache.
  typedef std::function<bool(Slice /* key */,
                             Slice /* value */)>
      ValidityFunc;

  // Functor to define whether to continue or stop iterating over the cache's
  // entries based on the number of encountered invalid and valid entries
  // during the Cache::Invalidate() call. If a cache contains multiple
  // sub-caches (e.g., shards), those parameters are per sub-cache. For example,
  // in case of multi-shard cache, when the 'iteration_func' returns 'false',
  // the invalidation at current shard stops and switches to the next
  // non-yet-processed shard, if any is present.
  //
  // The choice of the signature for the iteration functor is to allow for
  // effective purging of non-valid (e.g., expired) entries in caches with
  // the FIFO eviction policy (e.g., TTL caches).
  //
  // The first parameter of the functor is useful for short-circuiting
  // the invalidation process once some valid entries have been encountered.
  // For example, that's useful in case if the recency list has its entries
  // ordered in FIFO-like order (e.g., TTL cache with FIFO eviction policy),
  // so most-likely-invalid entries are in the very beginning of the list.
  // In the latter case, once a valid (e.g., not yet expired) entry is
  // encountered, there is no need to iterate any further: all the entries past
  // the first valid one in the recency list should be valid as well.
  //
  // The second parameter is useful when the validity criterion is fuzzy,
  // but there is a target number of entries to invalidate during each
  // invocation of the Invalidate() method or there is some logic that reads
  // the cache's metric(s) once the given number of entries have been evicted:
  // e.g., compare the result memory footprint of the cache against a threshold
  // to decide whether to continue invalidation of entries.
  //
  // Summing both parameters of the functor is useful when it's necessary to
  // limit the number of entries processed per one invocation of the
  // Invalidate() method. It makes sense in cases when a 'lazy' invalidation
  // process is run by a periodic task along with a significant amount of
  // concurrent requests to the cache, and the number of entries in the cache
  // is huge. Given the fact that in most cases it's necessary to guard
  // the access to the cache's recency list while iterating over it entries,
  // limiting the number of entries to process at once allows for better control
  // over the duration of the guarded/locked sections.
  typedef std::function<bool(size_t /* valid_entries_num */,
                             size_t /* invalid_entries_num */)>
      IterationFunc;

  // A helper function for 'validity_func' of the Invalidate() method:
  // invalidate all entries.
  static const ValidityFunc kInvalidateAllEntriesFunc;

  // A helper function for 'iteration_func' of the Invalidate() method:
  // examine all entries.
  static const IterationFunc kIterateOverAllEntriesFunc;

  // Control structure for the Invalidate() method. Combines the validity
  // and the iteration functors.
  struct InvalidationControl {
    // NOLINTNEXTLINE(google-explicit-constructor)
    InvalidationControl(ValidityFunc vfunctor = kInvalidateAllEntriesFunc,
                        IterationFunc ifunctor = kIterateOverAllEntriesFunc)
        : validity_func(std::move(vfunctor)),
          iteration_func(std::move(ifunctor)) {
    }
    const ValidityFunc validity_func;
    const IterationFunc iteration_func;
  };

 protected:
  // Release a mapping returned by a previous Lookup(), using raw handle.
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

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

#if defined(HAVE_LIB_MEMKIND)
// Create a new LRU cache with a fixed size capacity. This implementation
// of Cache uses the least-recently-used eviction policy and stored in NVM.
template<>
Cache* NewCache<Cache::EvictionPolicy::LRU,
                Cache::MemoryType::NVM>(size_t capacity, const std::string& id);
#endif

// A helper method to output cache memory type into ostream.
std::ostream& operator<<(std::ostream& os, Cache::MemoryType mem_type);

} // namespace kudu
