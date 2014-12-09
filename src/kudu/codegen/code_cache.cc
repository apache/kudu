// Copyright 2014 Cloudera inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/codegen/code_cache.h"

#include "kudu/codegen/jit_owner.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/cache.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace codegen {

CodeCache::CodeCache(size_t capacity)
  : cache_(NewLRUCache(capacity)) {}

CodeCache::~CodeCache() {}

namespace {

void CodeCacheDeleter(const Slice& key, void* value) {
  // The Cache from cache.h deletes the memory that it allocates for its
  // own copy of key, but it expects its users to delete their own
  // void* values. To delete, we just release our shared ownership.
  static_cast<JITCodeOwner*>(value)->Release();
}

} // anonymous namespace

void CodeCache::AddEntry(const Slice& key,
                         const scoped_refptr<JITCodeOwner>& payload) {
  // Because Cache only accepts void* values, we store just the JITPayload*
  // and increase its ref count.
  payload->AddRef();
  void* value = payload.get();

  // Insert into cache and release the handle (we have a local copy of a refptr)
  Cache::Handle* inserted = cache_->Insert(key, value, 1, CodeCacheDeleter);
  cache_->Release(inserted);
}

scoped_refptr<JITCodeOwner> CodeCache::Lookup(const Slice& key) {
  // Look up in Cache after generating key, returning NULL if not found.
  Cache::Handle* found = cache_->Lookup(key, Cache::EXPECT_IN_CACHE);
  if (!found) return scoped_refptr<JITCodeOwner>();

  // Retrieve the value
  scoped_refptr<JITCodeOwner> value =
    static_cast<JITCodeOwner*>(cache_->Value(found));

  // No need to hold on to handle after we have our copy
  cache_->Release(found);

  return value;
}

} // namespace codegen
} // namespace kudu
