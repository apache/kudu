// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

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

  uint8_t* data = cache.Allocate(data_size);
  memcpy(data, DATA_TO_CACHE, data_size);

  // Lookup something missing from cache
  {
    BlockCacheHandle handle;
    ASSERT_FALSE(cache.Lookup(id, 1, Cache::EXPECT_IN_CACHE, &handle));
    ASSERT_FALSE(handle.valid());
  }

  // Insert and re-lookup
  BlockCacheHandle inserted_handle;
  cache.Insert(id, 1, Slice(data, data_size), &inserted_handle);
  ASSERT_TRUE(inserted_handle.valid());

  BlockCacheHandle retrieved_handle;
  ASSERT_TRUE(cache.Lookup(id, 1, Cache::EXPECT_IN_CACHE, &retrieved_handle));
  ASSERT_TRUE(retrieved_handle.valid());
  ASSERT_EQ(retrieved_handle.data().data(), data);

  // Ensure that a lookup for a different offset doesn't
  // return this data.
  ASSERT_FALSE(cache.Lookup(id, 3, Cache::EXPECT_IN_CACHE, &retrieved_handle));

}


} // namespace cfile
} // namespace kudu
