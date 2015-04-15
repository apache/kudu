// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gflags/gflags.h>

#include "kudu/cfile/block_cache.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/singleton.h"
#include "kudu/util/cache.h"
#include "kudu/util/metrics.h"
#include "kudu/util/slice.h"


DEFINE_int64(block_cache_capacity_mb, 512, "block cache capacity in MB");

namespace kudu {

class MetricEntity;

namespace cfile {

struct CacheKey {
  CacheKey(BlockCache::FileId file_id, uint64_t offset) :
    file_id_(file_id),
    offset_(offset)
  {}

  const Slice slice() const {
    return Slice(reinterpret_cast<const uint8_t *>(this), sizeof(*this));
  }

  BlockCache::FileId file_id_;
  uint64_t offset_;
} PACKED;

BlockCache::BlockCache()
  : cache_(CHECK_NOTNULL(NewLRUCache(FLAGS_block_cache_capacity_mb * 1024 * 1024,
                                     "block_cache"))) {
}

BlockCache::BlockCache(size_t capacity)
  : cache_(CHECK_NOTNULL(NewLRUCache(capacity, "block_cache"))) {
}

BlockCache *BlockCache::GetSingleton() {
  return Singleton<BlockCache>::get();
}

BlockCache::FileId BlockCache::GenerateFileId() {
  return cache_->NewId();
}

bool BlockCache::Lookup(FileId file_id, uint64_t offset, Cache::CacheBehavior behavior,
                        BlockCacheHandle *handle) {
  CacheKey key(file_id, offset);
  Cache::Handle *h = cache_->Lookup(key.slice(), behavior);
  if (h != NULL) {
    handle->SetHandle(cache_.get(), h);
  }
  return h != NULL;
}

void BlockCache::Insert(FileId file_id, uint64_t offset, const Slice &block_data,
                        BlockCacheHandle *inserted) {
  CacheKey key(file_id, offset);

  // Allocate a copy of the value Slice (not the referred-to-data!)
  // for insertion in the cache.
  Slice *value = new Slice(block_data);

  Cache::Handle *h = cache_->Insert(key.slice(), value, value->size(),
                             BlockCache::ValueDeleter);
  inserted->SetHandle(cache_.get(), h);
}

void BlockCache::ValueDeleter(const Slice &key, void *value) {
  Slice *value_slice = reinterpret_cast<Slice *>(value);

  delete [] value_slice->data();
  delete value_slice;
}

void BlockCache::StartInstrumentation(const scoped_refptr<MetricEntity>& metric_entity) {
  cache_->SetMetrics(metric_entity);
}

} // namespace cfile
} // namespace kudu
