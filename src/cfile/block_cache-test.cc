// Copyright (c) 2013, Cloudera, inc.

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "cfile/block_cache.h"
#include "util/slice.h"

namespace kudu {
namespace cfile {

TEST(TestBlockCache, TestBasics) {
  const string DATUM_1("hello world");
  const string DATUM_2("goodbye world");

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
  cache.Insert(id, 1, Slice(DATUM_1), &inserted_handle);
  ASSERT_TRUE(inserted_handle.valid());

  BlockCacheHandle retrieved_handle;
  ASSERT_TRUE(cache.Lookup(id, 1, &retrieved_handle));
  ASSERT_TRUE(retrieved_handle.valid());
  ASSERT_EQ(reinterpret_cast<const char *>(retrieved_handle.data().data()),
            DATUM_1.c_str());

  // Ensure that a lookup for a different offset doesn't
  // return this data.
  ASSERT_FALSE(cache.Lookup(id, 3, &retrieved_handle));

}


} // namespace cfile
} // namespace kudu
