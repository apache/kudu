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

#include "kudu/master/table_locations_cache.h"

#include <cstddef>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/slice.h"

using std::string;
using strings::Substitute;

namespace kudu {
namespace master {

TableLocationsCache::TableLocationsCache(size_t capacity_bytes)
    : eviction_cb_(this),
      cache_(NewCache<Cache::EvictionPolicy::LRU>(capacity_bytes,
                                                  "table-locations-cache")) {
}

string TableLocationsCache::BuildKey(
    const string& table_id,
    size_t num_tablets,
    const string& first_tablet_id,
    const GetTableLocationsRequestPB& req) {
  return Substitute("$0:$1:$2:$3:$4",
      table_id,
      num_tablets,
      first_tablet_id,
      req.has_intern_ts_infos_in_response() ? 1 : 0,
      req.has_replica_type_filter() ? static_cast<int>(req.replica_type_filter())
                                    : -1);
}

TableLocationsCache::EntryHandle TableLocationsCache::Get(
    const string& table_id,
    size_t num_tablets,
    const string& first_tablet_id,
    const GetTableLocationsRequestPB& req) {
  const string key = BuildKey(table_id, num_tablets, first_tablet_id, req);
  auto h(cache_->Lookup(key, Cache::EXPECT_IN_CACHE));
  if (!h) {
    VLOG(2) << Substitute("key '$0': entry absent", key);
    return EntryHandle();
  }
  VLOG(2) << Substitute("key '$0': entry present", key);
  auto* entry_ptr = reinterpret_cast<Entry*>(cache_->Value(h).mutable_data());
  DCHECK(entry_ptr);
  return EntryHandle(DCHECK_NOTNULL(entry_ptr->val_ptr), std::move(h));
}

TableLocationsCache::EntryHandle TableLocationsCache::Put(
    const string& table_id,
    size_t num_tablets,
    const string& first_tablet_id,
    const GetTableLocationsRequestPB& req,
    std::unique_ptr<GetTableLocationsResponsePB> val) {
  const auto key = BuildKey(table_id, num_tablets, first_tablet_id, req);
  const auto charge = key.size() + val->ByteSizeLong();
  auto pending(cache_->Allocate(key, sizeof(Entry), charge));
  CHECK(pending);
  Entry* entry = reinterpret_cast<Entry*>(cache_->MutableValue(&pending));
  entry->val_ptr = val.get();
  // Insert() evicts already existing entry with the same key, if any.
  auto h(cache_->Insert(std::move(pending), &eviction_cb_));
  {
    std::lock_guard<simple_spinlock> l(keys_by_table_id_lock_);
    keys_by_table_id_[table_id].emplace(key);
  }
  VLOG(2) << Substitute("key '$0': added entry for table '$1'", key, table_id);
  // The cache takes care of the entry from this point: deallocation of the
  // resources passed via 'val' parameter is performed by the eviction callback.
  return EntryHandle(DCHECK_NOTNULL(val.release()), std::move(h));
}

void TableLocationsCache::Remove(const std::string& table_id) {
  std::lock_guard<simple_spinlock> l(keys_by_table_id_lock_);
  const auto it = keys_by_table_id_.find(table_id);
  if (it != keys_by_table_id_.end()) {
    VLOG(2) << Substitute("removing cached locations for table '$0'", table_id);
    const auto& keys = it->second;
    for (const auto& key : keys) {
      VLOG(2) << Substitute("removing key '$0' from table location cache", key);
      cache_->Erase(key);
    }
    keys_by_table_id_.erase(it);
  }
}

// Set metrics for the cache.
void TableLocationsCache::SetMetrics(std::unique_ptr<CacheMetrics> metrics) {
  cache_->SetMetrics(std::move(metrics), Cache::ExistingMetricsPolicy::kKeep);
}

TableLocationsCache::EntryHandle::EntryHandle()
    : ptr_(nullptr),
      handle_(Cache::UniqueHandle(nullptr,
                                  Cache::HandleDeleter(nullptr))) {
}

TableLocationsCache::EntryHandle::EntryHandle(EntryHandle&& other) noexcept
    : EntryHandle() {
  std::swap(ptr_, other.ptr_);
  handle_ = std::move(other.handle_);
}

TableLocationsCache::EntryHandle& TableLocationsCache::EntryHandle::operator=(
    TableLocationsCache::EntryHandle&& other) noexcept {
  ptr_ = other.ptr_;
  other.ptr_ = nullptr;
  handle_ = std::move(other.handle_);
  return *this;
}

TableLocationsCache::EntryHandle::EntryHandle(GetTableLocationsResponsePB* ptr,
                                              Cache::UniqueHandle handle)
    : ptr_(ptr),
      handle_(std::move(handle)) {
  DCHECK((ptr_ != nullptr && handle_) ||
         (ptr_ == nullptr && !handle_));
}

TableLocationsCache::EvictionCallback::EvictionCallback(TableLocationsCache* cache)
    : cache_(cache) {
  DCHECK(cache_);
}

void TableLocationsCache::EvictionCallback::EvictedEntry(Slice key, Slice val) {
  VLOG(2) << Substitute("EvictedEntry callback for key '$0'", key.ToString());
  auto* entry_ptr = reinterpret_cast<Entry*>(val.mutable_data());
  DCHECK(entry_ptr);
  delete entry_ptr->val_ptr;
}

} // namespace master
} // namespace kudu
