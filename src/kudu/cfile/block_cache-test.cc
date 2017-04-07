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

#include <cstring>
#include <memory>
#include <ostream>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/cache.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"

DECLARE_double(cache_memtracker_approximation_ratio);

namespace kudu {
namespace cfile {

static const char *DATA_TO_CACHE = "hello world";

TEST(TestBlockCache, TestBasics) {
  // Disable approximate tracking of cache memory since we make specific
  // assertions on the MemTracker in this test.
  FLAGS_cache_memtracker_approximation_ratio = 0;

  size_t data_size = strlen(DATA_TO_CACHE) + 1;
  BlockCache cache(512 * 1024 * 1024);
  BlockCache::FileId id(1234);
  BlockCache::CacheKey key(id, 1);

  std::shared_ptr<MemTracker> mem_tracker;
  if (BlockCache::GetConfiguredCacheTypeOrDie() == DRAM_CACHE) {
    ASSERT_TRUE(MemTracker::FindTracker("block_cache-sharded_lru_cache", &mem_tracker));
  }

  // Lookup something missing from cache
  {
    BlockCacheHandle handle;
    ASSERT_FALSE(cache.Lookup(key, Cache::EXPECT_IN_CACHE, &handle));
    ASSERT_FALSE(handle.valid());
  }

  BlockCache::PendingEntry data = cache.Allocate(key, data_size);
  memcpy(data.val_ptr(), DATA_TO_CACHE, data_size);

  // Insert and re-lookup
  BlockCacheHandle inserted_handle;
  cache.Insert(&data, &inserted_handle);
  ASSERT_FALSE(data.valid());
  ASSERT_TRUE(inserted_handle.valid());

  if (mem_tracker) {
    int overhead = mem_tracker->consumption() - data_size;
    EXPECT_GT(overhead, 0);
    LOG(INFO) << "Cache overhead for one entry: " << overhead;
  }

  BlockCacheHandle retrieved_handle;
  ASSERT_TRUE(cache.Lookup(key, Cache::EXPECT_IN_CACHE, &retrieved_handle));
  ASSERT_TRUE(retrieved_handle.valid());

  ASSERT_EQ(0, memcmp(retrieved_handle.data().data(), DATA_TO_CACHE, data_size));

  // Ensure that a lookup for a different offset doesn't
  // return this data.
  BlockCache::CacheKey key1(id, 3);
  ASSERT_FALSE(cache.Lookup(key1, Cache::EXPECT_IN_CACHE, &retrieved_handle));
}


} // namespace cfile
} // namespace kudu
