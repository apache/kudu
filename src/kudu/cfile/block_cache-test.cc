// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <string.h>

#include "kudu/cfile/block_cache.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace cfile {

static const char *DATA_TO_CACHE = "hello world";

TEST(TestBlockCache, TestBasics) {
  size_t data_size = strlen(DATA_TO_CACHE) + 1;
  uint8_t *DATUM_1 = new uint8_t[data_size];
  memcpy(DATUM_1, DATA_TO_CACHE, data_size);

  BlockCache cache(1024);

  BlockCache::FileId id = cache.GenerateFileId();

  // Lookup something missing from cache
  {
    BlockCacheHandle handle;
    ASSERT_FALSE(cache.Lookup(id, 1, &handle));
    ASSERT_FALSE(handle.valid());
  }

  // Insert and re-lookup
  BlockCacheHandle inserted_handle;
  cache.Insert(id, 1, Slice(DATUM_1, data_size), &inserted_handle);
  ASSERT_TRUE(inserted_handle.valid());

  BlockCacheHandle retrieved_handle;
  ASSERT_TRUE(cache.Lookup(id, 1, &retrieved_handle));
  ASSERT_TRUE(retrieved_handle.valid());
  ASSERT_EQ(retrieved_handle.data().data(), DATUM_1);

  // Ensure that a lookup for a different offset doesn't
  // return this data.
  ASSERT_FALSE(cache.Lookup(id, 3, &retrieved_handle));

}


} // namespace cfile
} // namespace kudu
