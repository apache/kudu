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
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/cache.h"
#include "kudu/util/locks.h"
#include "kudu/util/slice.h"

namespace kudu {
struct CacheMetrics;

namespace master {

class GetTableLocationsRequestPB;
class GetTableLocationsResponsePB;

class TableLocationsCache final {
 public:

  // The handle for an entry in the cache.
  class EntryHandle {
   public:
    // Construct an empty/nullptr handle.
    EntryHandle();
    ~EntryHandle() = default;

    EntryHandle(EntryHandle&& other) noexcept;
    EntryHandle& operator=(EntryHandle&& other) noexcept;

    explicit operator bool() const noexcept {
      return ptr_ != nullptr;
    }

    // No modifications of the value are allowed through EntryHandle.
    const GetTableLocationsResponsePB& value() const {
      DCHECK(ptr_);
      return *ptr_;
    }

   private:
    friend class TableLocationsCache;
    DISALLOW_COPY_AND_ASSIGN(EntryHandle);

    EntryHandle(GetTableLocationsResponsePB* ptr, Cache::UniqueHandle handle);

    GetTableLocationsResponsePB* ptr_;
    Cache::UniqueHandle handle_;
  };

  // Create a new cache (LRU eviction policy) with the specified capacity.
  explicit TableLocationsCache(size_t capacity_bytes);

  ~TableLocationsCache() = default;

  // Retrieve an entry from the cache for the specified table identifier
  // and parameters of GetTableLocationsRequest.
  //
  // Cached key/value pairs may still be evicted even with an outstanding
  // handle, but a cached value won't be destroyed until the handle goes
  // out of scope.
  EntryHandle Get(const std::string& table_id,
                  size_t num_tablets,
                  const std::string& first_tablet_id,
                  const GetTableLocationsRequestPB& req);

  // For the specified key, add an entry into the cache or replace already
  // existing one. This method returns a smart pointer for the entry's handle.
  //
  // Put() evicts already existing entry with the same key (if any), effectively
  // replacing corresponding entry in the cache as the result.
  EntryHandle Put(const std::string& table_id,
                  size_t num_tablets,
                  const std::string& first_tablet_id,
                  const GetTableLocationsRequestPB& req,
                  std::unique_ptr<GetTableLocationsResponsePB> val);

  // Remove all entries for the specified table identifier.
  void Remove(const std::string& table_id);

  // Set metrics for the cache.
  void SetMetrics(std::unique_ptr<CacheMetrics> metrics);

 private:
  friend class EvictionCallback;

  // An entry to store in the underlying LRU cache.
  struct Entry {
    // Raw pointer to the 'value'. The cache owns the memory allocated and takes
    // care of deallocating it upon call of EvictionCallback::EvictedEntry()
    // by the underlying cache instance.
    GetTableLocationsResponsePB* val_ptr;
  };

  // Callback invoked by the underlying LRU cache when a cached entry's
  // reference count reaches zero and is evicted.
  class EvictionCallback : public Cache::EvictionCallback {
   public:
    explicit EvictionCallback(TableLocationsCache* cache);

    // This method called upon evicting an entry with the specified key ('key')
    // and value ('val').
    void EvictedEntry(Slice key, Slice val) override;

   private:
    TableLocationsCache* cache_;
    DISALLOW_COPY_AND_ASSIGN(EvictionCallback);
  };

  // Build a key for the item in the cache given table identifier
  // and the information in the request. The key is a composite one and includes
  // request parameters because the response depends on them, so there might be
  // multiple cached items in the cache for the same table identifier.
  static std::string BuildKey(const std::string& table_id,
                              size_t num_tablets,
                              const std::string& first_tablet_id,
                              const GetTableLocationsRequestPB& req);

  // Invoked whenever a cached entry reaches zero reference count, i.e. it was
  // removed from the cache and there aren't any other references
  // floating around.
  EvictionCallback eviction_cb_;

  // The underlying LRU cache instance.
  std::unique_ptr<Cache> cache_;

  // A convenience dictionary to keep correspondence between a table's
  // identifier and cached locations for the table. This map is used when
  // invalidating element in the cache given table identifier.
  std::unordered_map<std::string, std::unordered_set<std::string>> keys_by_table_id_;

  // A synchronisation primitive to guard access to keys_by_table_id_.
  simple_spinlock keys_by_table_id_lock_;

  DISALLOW_COPY_AND_ASSIGN(TableLocationsCache);
};

} // namespace master
} // namespace kudu
