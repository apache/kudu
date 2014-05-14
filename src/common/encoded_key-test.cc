// Copyright (c) 2014, Cloudera, inc.

#include "common/encoded_key.h"

#include <boost/assign/list_of.hpp>
#include <gtest/gtest.h>

#include "util/slice.h"
#include "util/test_macros.h"

namespace kudu {

class EncodedKeyTest : public ::testing::Test {
 public:
  EncodedKeyTest()
  : schema_(CreateSchema()),
    key_builder_(schema_) {
  }

  static Schema CreateSchema() {
    return Schema(boost::assign::list_of
                  (ColumnSchema("key", UINT32)), 1);
  }

  EncodedKey* BuildEncodedKey(EncodedKeyBuilder& key_builder, int val) {
    key_builder.Reset();
    key_builder.AddColumnKey(&val);
    return key_builder.BuildEncodedKey();
  }

  // Test whether target lies within the numerical key ranges given by
  // start and end. If -1, an empty slice is used instead.
  bool InRange(int start, int end, int target) {
    gscoped_ptr<EncodedKey> start_key(BuildEncodedKey(key_builder_, start));
    gscoped_ptr<EncodedKey> end_key(BuildEncodedKey(key_builder_, end));
    gscoped_ptr<EncodedKey> target_key(BuildEncodedKey(key_builder_, target));
    return target_key->InRange(start != -1 ? start_key->encoded_key() : Slice(),
                               end != -1 ? end_key->encoded_key() : Slice());
  }

 private:
  Schema schema_;
  EncodedKeyBuilder key_builder_;
};

TEST_F(EncodedKeyTest, TestKeyInRange) {
  ASSERT_TRUE(InRange(-1, -1, 0));
  ASSERT_TRUE(InRange(-1, -1, 50));

  ASSERT_TRUE(InRange(-1, 30, 0));
  ASSERT_TRUE(InRange(-1, 30, 29));
  ASSERT_FALSE(InRange(-1, 30, 30));
  ASSERT_FALSE(InRange(-1, 30, 31));

  ASSERT_FALSE(InRange(10, -1, 9));
  ASSERT_TRUE(InRange(10, -1, 10));
  ASSERT_TRUE(InRange(10, -1, 11));
  ASSERT_TRUE(InRange(10, -1, 31));

  ASSERT_FALSE(InRange(10, 20, 9));
  ASSERT_TRUE(InRange(10, 20, 10));
  ASSERT_TRUE(InRange(10, 20, 19));
  ASSERT_FALSE(InRange(10, 20, 20));
  ASSERT_FALSE(InRange(10, 20, 21));
}

} // namespace kudu
