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

#include "kudu/codegen/code_cache.h"

#include <cstring>
#include <memory>
#include <utility>

#include <glog/logging.h>

#include "kudu/codegen/jit_wrapper.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/cache.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
class faststring;

namespace codegen {

namespace {

// The values we store in cache are actually just pointers to JITWrapper
// objects. This returns the 'unwrapped' pointer from a cache value.
JITWrapper* CacheValueToJITWrapper(Slice val) {
  JITWrapper* jw;
  DCHECK_EQ(val.size(), sizeof(jw));
  memcpy(&jw, val.data(), sizeof(jw));
  return jw;
}

} // anonymous namespace

// When an entry is evicted from the cache, we need to also
// decrement the ref-count on the wrapper, since the cache
// itself isn't managing all of the associated objects.
class CodeCache::EvictionCallback : public Cache::EvictionCallback {
 public:
  EvictionCallback() {}
  void EvictedEntry(Slice /*key*/, Slice value) override {
    CacheValueToJITWrapper(value)->Release();
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(EvictionCallback);
};

CodeCache::CodeCache(size_t capacity)
    : cache_(NewCache(capacity, "code_cache")) {
  eviction_callback_.reset(new EvictionCallback);
}

CodeCache::~CodeCache() {}

Status CodeCache::AddEntry(const faststring& key,
                           const scoped_refptr<JITWrapper>& value) {
  const JITWrapper* val = value.get();
  constexpr size_t kValLen = sizeof(val); // NOLINT(bugprone-sizeof-expression)
  // Using CHECK because this is always a DRAM-based cache, and if allocation
  // failed, we'd just crash the process.
  auto pending(cache_->Allocate(key, kValLen, /*charge = */1));
  CHECK(pending);
  memcpy(cache_->MutableValue(&pending), &val, kValLen);

  // Because Cache only accepts void* values, we store just the JITWrapper*
  // and increase its ref count.
  value->AddRef();

  // Insert into cache and release the handle (we have a local copy of a refptr).
  auto inserted(cache_->Insert(std::move(pending), eviction_callback_.get()));
  DCHECK(inserted);
  return Status::OK();
}

scoped_refptr<JITWrapper> CodeCache::Lookup(const faststring& key) {
  // Look up in Cache after generating key, returning NULL if not found.
  auto found(cache_->Lookup(key, Cache::EXPECT_IN_CACHE));
  if (!found) {
    return scoped_refptr<JITWrapper>();
  }

  // Retrieve the value
  return CacheValueToJITWrapper(cache_->Value(found));
}

} // namespace codegen
} // namespace kudu
