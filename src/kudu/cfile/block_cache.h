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
#ifndef KUDU_CFILE_BLOCK_CACHE_H
#define KUDU_CFILE_BLOCK_CACHE_H

#include <cstddef>
#include <cstdint>
#include <utility>

#include "kudu/fs/block_id.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/singleton.h"
#include "kudu/util/cache.h"
#include "kudu/util/slice.h"

template <class T> class scoped_refptr;

namespace kudu {

class MetricEntity;

namespace cfile {

class BlockCacheHandle;

// Wrapper around kudu::Cache specifically for caching blocks of CFiles.
// Provides a singleton and LRU cache for CFile blocks.
class BlockCache {
 public:
  // Parse the gflag which configures the block cache. FATALs if the flag is
  // invalid.
  static Cache::MemoryType GetConfiguredCacheMemoryTypeOrDie();

  // BlockId refers to the unique identifier for a Kudu block, that is, for an
  // entire CFile. This is different than the block cache's notion of a block,
  // which is just a portion of a CFile.
  typedef BlockId FileId;

  // The unique key identifying entries in the block cache.
  // Each cached block corresponds to a specific offset within
  // a file (called a "block" in other parts of Kudu).
  //
  // This structure's in-memory representation is internally memcpyed
  // and treated as a string. It may also be persisted across restarts
  // and upgrades of Kudu in persistent cache implementations. So, it's
  // important that the layout be fixed and kept compatible for all
  // future releases.
  struct CacheKey {
    CacheKey(BlockCache::FileId file_id, uint64_t offset) :
      file_id_(file_id.id()),
      offset_(offset)
    {}

    uint64_t file_id_;
    uint64_t offset_;
  } PACKED;

  // An entry that is in the process of being inserted into the block
  // cache. See the documentation above 'Allocate' below on the block
  // cache insertion path.
  class PendingEntry {
   public:
    PendingEntry()
        : handle_(Cache::UniquePendingHandle(nullptr,
                                             Cache::PendingHandleDeleter(nullptr))) {
    }
    explicit PendingEntry(Cache::UniquePendingHandle handle)
        : handle_(std::move(handle)) {
    }
    PendingEntry(PendingEntry&& other) noexcept : PendingEntry() {
      *this = std::move(other);
    }

    ~PendingEntry() {
      reset();
    }

    PendingEntry& operator=(PendingEntry&& other) noexcept;
    PendingEntry& operator=(const PendingEntry& other) = delete;

    // Free the pending entry back to the block cache.
    // This is safe to call multiple times.
    void reset();

    // Return true if this is a valid pending entry.
    bool valid() const {
      return static_cast<bool>(handle_);
    }

    // Return the pointer into which the value should be written.
    uint8_t* val_ptr() {
      return handle_.get_deleter().cache()->MutableValue(&handle_);
    }

   private:
    friend class BlockCache;

    Cache::UniquePendingHandle handle_;
  };

  static BlockCache* GetSingleton() {
    return Singleton<BlockCache>::get();
  }

  explicit BlockCache(size_t capacity);

  // Lookup the given block in the cache.
  //
  // If the entry is found, then sets *handle to refer to the entry.
  // This object's destructor will release the cache entry so it may be freed again.
  // Alternatively,  handle->Release() may be used to explicitly release it.
  //
  // Returns true to indicate that the entry was found, false otherwise.
  bool Lookup(const CacheKey& key, Cache::CacheBehavior behavior,
              BlockCacheHandle* handle);

  // Pass a metric entity to the cache to start recording metrics.
  // This should be called before the block cache starts serving blocks.
  // Not calling StartInstrumentation will simply result in no block cache-related metrics.
  // Calling StartInstrumentation multiple times will reset the metrics each time.
  void StartInstrumentation(const scoped_refptr<MetricEntity>& metric_entity);

  // Insertion path
  // --------------------
  // Block cache entries are written in two phases. First, a pending entry must be
  // constructed. The data to be cached then must be copied directly into
  // this pending entry before being inserted. For example:
  //
  //   // Allocate space in the cache for a block of 'data_size' bytes.
  //   PendingEntry entry = cache->Allocate(my_cache_key, data_size);
  //   // Check for allocation failure.
  //   if (!entry.valid()) {
  //     // if there is no space left in the cache, handle the error.
  //   }
  //   // Read the actual block into the allocated buffer.
  //   RETURN_NOT_OK(ReadDataFromDiskIntoBuffer(entry.val_ptr()));
  //   // "Commit" the entry to the cache
  //   BlockCacheHandle bch;
  //   cache->Insert(&entry, &bch);

  // Allocate a new entry to be inserted into the cache.
  PendingEntry Allocate(const CacheKey& key, size_t block_size);

  // Insert the given block into the cache. 'inserted' is set to refer to the
  // entry in the cache.
  void Insert(PendingEntry* entry, BlockCacheHandle* inserted);

 private:
  friend class Singleton<BlockCache>;
  BlockCache();

  DISALLOW_COPY_AND_ASSIGN(BlockCache);

  gscoped_ptr<Cache> cache_;
};

// Scoped reference to a block from the block cache.
class BlockCacheHandle {
 public:
  BlockCacheHandle()
      : handle_(Cache::UniqueHandle(nullptr, Cache::HandleDeleter(nullptr))) {
  }

  ~BlockCacheHandle() = default;

  // Swap this handle with another handle.
  // This can be useful to transfer ownership of a handle by swapping
  // with an empty BlockCacheHandle.
  void swap(BlockCacheHandle* dst) {
    std::swap(handle_, dst->handle_);
  }

  // Return the data in the cached block.
  //
  // NOTE: this slice is only valid until the block cache handle is
  // destructed or explicitly Released().
  Slice data() const {
    return handle_.get_deleter().cache()->Value(handle_);
  }

  bool valid() const {
    return static_cast<bool>(handle_);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(BlockCacheHandle);
  friend class BlockCache;

  void SetHandle(Cache::UniqueHandle handle) {
    handle_ = std::move(handle);
  }

  Cache::UniqueHandle handle_;
};


inline BlockCache::PendingEntry& BlockCache::PendingEntry::operator=(
    BlockCache::PendingEntry&& other) noexcept {
  reset();
  handle_ = std::move(other.handle_);
  return *this;
}

inline void BlockCache::PendingEntry::reset() {
  handle_.reset();
}

} // namespace cfile
} // namespace kudu

#endif
