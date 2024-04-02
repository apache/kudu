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

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/block_cache_metrics.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/flag_validators.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/slice.h"
#include "kudu/util/slru_cache.h"
#include "kudu/util/string_case.h"

DEFINE_int64(block_cache_capacity_mb, 512, "block cache capacity in MB");
TAG_FLAG(block_cache_capacity_mb, stable);

DEFINE_string(block_cache_eviction_policy, "LRU",
              "Eviction policy used for block cache. "
              "Either 'LRU' (default) or 'SLRU' (experimental).");
TAG_FLAG(block_cache_eviction_policy, advanced);
TAG_FLAG(block_cache_eviction_policy, experimental);

DEFINE_double(block_cache_protected_segment_percentage, 0.8,
              "Percentage of 'block_cache_capacity_mb' that's the protected segment's capacity. "
              "Ex. For the default parameters of 0.8 for this flag and 512 MB for "
              "'block_cache_capacity_mb', the protected segment will be 0.8 * 512 = 410 MB."
              "Must be >= 0 and <= 1.");
TAG_FLAG(block_cache_protected_segment_percentage, experimental);

DEFINE_uint32(block_cache_lookups_before_upgrade, 2,
             "Number of lookups before entry is upgraded from probationary to protected segment. "
             "Must be > 0.");
TAG_FLAG(block_cache_lookups_before_upgrade, experimental);


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
              "in a memory-mapped file using the memkind library. To use 'NVM', "
              "libmemkind 1.8.0 or newer must be available on the system; "
              "otherwise Kudu will crash.");

#if !defined(NO_ROCKSDB)
DECLARE_uint32(log_container_rdb_block_cache_capacity_mb);
#endif

using std::string;
using std::unique_ptr;
using strings::Substitute;

template <class T> class scoped_refptr;

namespace kudu {

class MetricEntity;

namespace cfile {

namespace {

string ToUpper(const string& input) {
  string temp;
  ToUpperCase(input, &temp);
  return temp;
}

} // anonymous namespace

bool ValidateBlockCacheCapacity() {
  if (FLAGS_force_block_cache_capacity || ToUpper(FLAGS_block_cache_type) != "DRAM") {
    return true;
  }

  int64_t capacity = FLAGS_block_cache_capacity_mb * 1024 * 1024;
  if (capacity < 0) {
    LOG(ERROR) << Substitute("Block cache capacity must be > 0. It's $0 bytes.", capacity);
  }

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
#if !defined(NO_ROCKSDB)
  if (FLAGS_log_container_rdb_block_cache_capacity_mb >= FLAGS_block_cache_capacity_mb) {
    LOG(WARNING) << Substitute("Block cache capacity for RocksDB which is used only for metadata "
                               "is larger than that for data ($0 MB vs. $1 MB). This may cause "
                               "performance problems. Consider lowering "
                               "--log_container_rdb_block_cache_capacity_mb or raising "
                               "--block_cache_capacity_mb.",
                               FLAGS_log_container_rdb_block_cache_capacity_mb,
                               FLAGS_block_cache_capacity_mb);
    return true;
  }
#endif
  return true;
}
GROUP_FLAG_VALIDATOR(block_cache_capacity_mb, ValidateBlockCacheCapacity);

bool ValidateBlockCacheSegmentCapacity() {
  if (FLAGS_force_block_cache_capacity ||
      ToUpper(FLAGS_block_cache_type) != "DRAM" ||
      ToUpper(FLAGS_block_cache_eviction_policy) == "LRU") {
    return true;
  }

  const auto& percentage = FLAGS_block_cache_protected_segment_percentage;
  if (percentage < 0 || percentage > 1) {
    LOG(ERROR) << Substitute("FLAGS_block_cache_protected_segment_percentage must be >= 0 "
                             "and <= 1. It's $0.", percentage);
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(block_cache_protected_segment_percentage, ValidateBlockCacheSegmentCapacity);

bool ValidateLookups() {
  if (ToUpper(FLAGS_block_cache_eviction_policy) == "LRU") {
    return true;
  }

  uint32_t lookups = FLAGS_block_cache_lookups_before_upgrade;
  if (lookups == 0) {
    LOG(ERROR) << Substitute("Number of lookups for SLRU cache must be > 0");
    return false;
  }
  return true;
}
GROUP_FLAG_VALIDATOR(block_cache_lookups_before_upgrade, ValidateLookups);

bool ValidateEvictionPolicy() {
  const auto& eviction_policy = ToUpper(FLAGS_block_cache_eviction_policy);
  return eviction_policy == "LRU" || eviction_policy == "SLRU";
}
GROUP_FLAG_VALIDATOR(block_cache_eviction_policy, ValidateEvictionPolicy);

Cache* BlockCache::CreateCache(int64_t capacity) {
  const auto& mem_type = BlockCache::GetConfiguredCacheMemoryTypeOrDie();
  switch (mem_type) {
    case Cache::MemoryType::DRAM:
      return NewCache<Cache::EvictionPolicy::LRU, Cache::MemoryType::DRAM>(
          capacity, "block_cache");
    case Cache::MemoryType::NVM:
      return NewCache<Cache::EvictionPolicy::LRU, Cache::MemoryType::NVM>(
          capacity, "block_cache");
    default:
      LOG(FATAL) << "unsupported LRU cache memory type: " << mem_type;
      return nullptr;
  }
}

Cache* BlockCache::CreateCache(int64_t probationary_segment_capacity,
                   int64_t protected_segment_capacity,
                   uint32_t lookups) {
  const auto& mem_type = BlockCache::GetConfiguredCacheMemoryTypeOrDie();
  switch (mem_type) {
    case Cache::MemoryType::DRAM:
      return NewSLRUCache<Cache::MemoryType::DRAM>(
          probationary_segment_capacity, protected_segment_capacity, "block_cache", lookups);
    default:
      LOG(FATAL) << "unsupported SLRU cache memory type: " << mem_type;
      return nullptr;
  }
}

Cache::MemoryType BlockCache::GetConfiguredCacheMemoryTypeOrDie() {
  const auto& memory_type = ToUpper(FLAGS_block_cache_type);
  if (memory_type == "NVM") {
    return Cache::MemoryType::NVM;
  }
  if (memory_type == "DRAM") {
    return Cache::MemoryType::DRAM;
  }

  LOG(FATAL) << "Unknown block cache type: '" << memory_type
             << "' (expected 'DRAM' or 'NVM')";
  __builtin_unreachable();
}

Cache::EvictionPolicy BlockCache::GetCacheEvictionPolicyOrDie() {
  const auto& eviction_policy = ToUpper(FLAGS_block_cache_eviction_policy);
  if (eviction_policy == "LRU") {
    return Cache::EvictionPolicy::LRU;
  }
  if (eviction_policy == "SLRU") {
    return Cache::EvictionPolicy::SLRU;
  }

  LOG(FATAL) << "Unknown block cache eviction policy: '" << eviction_policy
             << "' (expected 'LRU' or 'SLRU')";
  __builtin_unreachable();
}

BlockCache::BlockCache() {
  const auto& eviction_policy = GetCacheEvictionPolicyOrDie();
  if (eviction_policy == Cache::EvictionPolicy::LRU) {
    unique_ptr<Cache> lru_cache(CreateCache(FLAGS_block_cache_capacity_mb * 1024 * 1024));
    cache_ = std::move(lru_cache);
  } else {
    DCHECK(eviction_policy == Cache::EvictionPolicy::SLRU);
    int64_t probationary_capacity =
        static_cast<int>(std::round((1.0 - FLAGS_block_cache_protected_segment_percentage)
        * 1024 * 1024));
    int64_t protected_capacity =
        static_cast<int>(std::round(FLAGS_block_cache_protected_segment_percentage * 1024 * 1024));
    unique_ptr<Cache> slru_cache(CreateCache(probationary_capacity, protected_capacity,
                                             FLAGS_block_cache_lookups_before_upgrade));
    cache_ = std::move(slru_cache);
  }
}

BlockCache::BlockCache(size_t capacity)
    : cache_(CreateCache(capacity)) {
}

BlockCache::BlockCache(size_t probationary_segment_capacity,
                       size_t protected_segment_capacity,
                       size_t lookups)
    : cache_(CreateCache(probationary_segment_capacity, protected_segment_capacity, lookups)) {
}

BlockCache::PendingEntry BlockCache::Allocate(const CacheKey& key, size_t block_size) {
  Slice key_slice(reinterpret_cast<const uint8_t*>(&key), sizeof(key));
  return PendingEntry(cache_->Allocate(key_slice, block_size));
}

bool BlockCache::Lookup(const CacheKey& key, Cache::CacheBehavior behavior,
                        BlockCacheHandle* handle) {
  auto h(cache_->Lookup(
      Slice(reinterpret_cast<const uint8_t*>(&key), sizeof(key)), behavior));
  if (h) {
    handle->SetHandle(std::move(h));
    return true;
  }
  return false;
}

void BlockCache::Insert(BlockCache::PendingEntry* entry, BlockCacheHandle* inserted) {
  auto h(cache_->Insert(std::move(entry->handle_),
                        /* eviction_callback= */ nullptr));
  inserted->SetHandle(std::move(h));
}

void BlockCache::StartInstrumentation(const scoped_refptr<MetricEntity>& metric_entity,
                                      Cache::ExistingMetricsPolicy metrics_policy) {
  const auto& eviction_policy = GetCacheEvictionPolicyOrDie();
  if (eviction_policy == Cache::EvictionPolicy::LRU) {
    unique_ptr<BlockCacheMetrics> metrics(new BlockCacheMetrics(metric_entity));
    cache_->SetMetrics(std::move(metrics), metrics_policy);
  } else {
    DCHECK(eviction_policy == Cache::EvictionPolicy::SLRU);
    unique_ptr<SLRUCacheMetrics> metrics(new SLRUCacheMetrics(metric_entity));
    cache_->SetMetrics(std::move(metrics), metrics_policy);
  }
}

} // namespace cfile
} // namespace kudu
