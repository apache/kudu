// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CFILE_BLOCK_CACHE_H
#define KUDU_CFILE_BLOCK_CACHE_H

#include <glog/logging.h>

#include "gutil/gscoped_ptr.h"
#include "util/cache.h"

namespace kudu {
namespace cfile {

class BlockCacheHandle;

// Wrapper around kudu::Cache specifically for caching blocks of CFiles.
// Provides a singleton and LRU cache for CFile blocks.
class BlockCache : boost::noncopyable {
public:
  typedef uint64_t FileId;

  static BlockCache *GetSingleton();

  explicit BlockCache(size_t capacity);
  BlockCache();

  // Return a unique ID suitable for use as part of a cache key.
  FileId GenerateFileId();

  // Lookup the given block in the cache.
  //
  // If the entry is found, then sets *handle to refer to the entry.
  // This object's destructor will release the cache entry so it may be freed again.
  // Alternatively,  handle->Release() may be used to explicitly release it.
  //
  // Returns true to indicate that the entry was found, false otherwise.
  bool Lookup(FileId file_id, uint64_t offset, BlockCacheHandle *handle);

  // Insert the given block into the cache.
  //
  // The data pointed to by Slice should have been allocated with malloc.
  // After insertion, the block cache owns this pointer and will free it upon
  // eviction.
  //
  // The inserted entry is returned in *inserted.
  void Insert(FileId file_id, uint64_t offset, const Slice &block_data,
              BlockCacheHandle *inserted);

private:

  static void ValueDeleter(const Slice &key, void *value);

  gscoped_ptr<Cache> cache_;
};

// Scoped reference to a block from the block cache.
class BlockCacheHandle : boost::noncopyable {
public:
  BlockCacheHandle() :
    handle_(NULL)
  {}

  ~BlockCacheHandle() {
    if (handle_ != NULL) {
      Release();
    }
  }

  void Release() {
    CHECK_NOTNULL(cache_)->Release(CHECK_NOTNULL(handle_));
    handle_ = NULL;
  }

  // Swap this handle with another handle.
  // This can be useful to transfer ownership of a handle by swapping
  // with an empty BlockCacheHandle.
  void swap(BlockCacheHandle *dst) {
    uint8_t buf[sizeof(*this)];
    memcpy(buf, dst, sizeof(*this));
    memcpy(dst, this, sizeof(*this));
    memcpy(this, buf, sizeof(*this));
  }

  // Return the data in the cached block.
  //
  // NOTE: this slice is only valid until the block cache handle is
  // destructed or explicitly Released().
  const Slice &data() const {
    const Slice *slice = reinterpret_cast<const Slice *>(cache_->Value(handle_));
    return *slice;
  }

  bool valid() const {
    return handle_ != NULL;
  }

private:
  friend class BlockCache;

  void SetHandle(Cache *cache, Cache::Handle *handle) {
    if (handle_ != NULL) Release();

    cache_ = cache;
    handle_ = handle;
  }

  Cache::Handle *handle_;
  Cache *cache_;
};


} // namespace cfile
} // namespace kudu

#endif
