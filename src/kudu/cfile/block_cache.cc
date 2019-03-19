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

#include "kudu/cfile/block_cache.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/block_cache_metrics.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/slice.h"
#include "kudu/util/string_case.h"

DEFINE_int64(block_cache_capacity_mb, 512, "block cache capacity in MB");
TAG_FLAG(block_cache_capacity_mb, stable);

// Yes, it's strange: the default value is 'true' but that's intentional.
// The idea is to avoid the corresponding group flag validator striking
// while running anything but master and tserver. As for the master and tserver,
// those have the default value for this flag set to 'false'.
DEFINE_bool(force_block_cache_capacity, true,
            "Force Kudu to accept the block cache size, even if it is unsafe.");
TAG_FLAG(force_block_cache_capacity, unsafe);
TAG_FLAG(force_block_cache_capacity, hidden);

DEFINE_string(block_cache_type, "DRAM",
              "Which type of block cache to use for caching data. "
              "Valid choices are 'DRAM' or 'NVM'. DRAM, the default, "
              "caches data in regular memory. 'NVM' caches data "
              "in a memory-mapped file using the NVML library.");
TAG_FLAG(block_cache_type, experimental);

using strings::Substitute;

template <class T> class scoped_refptr;

namespace kudu {

class MetricEntity;

namespace cfile {

namespace {

Cache* CreateCache(int64_t capacity) {
  auto mem_type = BlockCache::GetConfiguredCacheMemoryTypeOrDie();
  return NewLRUCache(mem_type, capacity, "block_cache");
}

// Validates the block cache capacity won't permit the cache to grow large enough
// to cause pernicious flushing behavior. See KUDU-2318.
bool ValidateBlockCacheCapacity() {
  if (FLAGS_force_block_cache_capacity) {
    return true;
  }
  int64_t capacity = FLAGS_block_cache_capacity_mb * 1024 * 1024;
  int64_t mpt = process_memory::MemoryPressureThreshold();
  if (capacity > mpt) {
    LOG(ERROR) << Substitute("Block cache capacity exceeds the memory pressure "
                             "threshold ($0 bytes vs. $1 bytes). This will "
                             "cause instability and harmful flushing behavior. "
                             "Lower --block_cache_capacity_mb or raise "
                             "--memory_limit_hard_bytes.",
                             capacity, mpt);
    return false;
  }
  if (capacity > mpt / 2) {
    LOG(WARNING) << Substitute("Block cache capacity exceeds 50% of the memory "
                               "pressure threshold ($0 bytes vs. 50% of $1 bytes). "
                               "This may cause performance problems. Consider "
                               "lowering --block_cache_capacity_mb or raising "
                               "--memory_limit_hard_bytes.",
                               capacity, mpt);
  }
  return true;
}

} // anonymous namespace

GROUP_FLAG_VALIDATOR(block_cache_capacity_mb, ValidateBlockCacheCapacity);

Cache::MemoryType BlockCache::GetConfiguredCacheMemoryTypeOrDie() {
    ToUpperCase(FLAGS_block_cache_type, &FLAGS_block_cache_type);
  if (FLAGS_block_cache_type == "NVM") {
    return Cache::MemoryType::NVM;
  }
  if (FLAGS_block_cache_type == "DRAM") {
    return Cache::MemoryType::DRAM;
  }

  LOG(FATAL) << "Unknown block cache type: '" << FLAGS_block_cache_type
             << "' (expected 'DRAM' or 'NVM')";
  __builtin_unreachable();
}

BlockCache::BlockCache()
  : BlockCache(FLAGS_block_cache_capacity_mb * 1024 * 1024) {
}

BlockCache::BlockCache(size_t capacity)
  : cache_(CreateCache(capacity)) {
}

BlockCache::PendingEntry BlockCache::Allocate(const CacheKey& key, size_t block_size) {
  Slice key_slice(reinterpret_cast<const uint8_t*>(&key), sizeof(key));
  return PendingEntry(cache_.get(), cache_->Allocate(key_slice, block_size));
}

bool BlockCache::Lookup(const CacheKey& key, Cache::CacheBehavior behavior,
                        BlockCacheHandle *handle) {
  Cache::Handle *h = cache_->Lookup(Slice(reinterpret_cast<const uint8_t*>(&key),
                                          sizeof(key)), behavior);
  if (h != nullptr) {
    handle->SetHandle(cache_.get(), h);
  }
  return h != nullptr;
}

void BlockCache::Insert(BlockCache::PendingEntry* entry, BlockCacheHandle* inserted) {
  Cache::Handle *h = cache_->Insert(entry->handle_, /* eviction_callback= */ nullptr);
  entry->handle_ = nullptr;
  inserted->SetHandle(cache_.get(), h);
}

void BlockCache::StartInstrumentation(const scoped_refptr<MetricEntity>& metric_entity) {
  std::unique_ptr<BlockCacheMetrics> metrics(new BlockCacheMetrics(metric_entity));
  cache_->SetMetrics(std::move(metrics));
}

} // namespace cfile
} // namespace kudu
