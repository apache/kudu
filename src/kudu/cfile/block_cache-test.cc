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

#include <cstring>

#include <gtest/gtest.h>

#include "kudu/cfile/block_cache.h"
#include "kudu/util/cache.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace cfile {

static const char *DATA_TO_CACHE = "hello world";

TEST(TestBlockCache, TestBasics) {
  size_t data_size = strlen(DATA_TO_CACHE) + 1;
  BlockCache cache(512 * 1024 * 1024);
  BlockCache::FileId id(1234);
  BlockCache::CacheKey key(id, 1);

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
