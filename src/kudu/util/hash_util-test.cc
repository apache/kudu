// Copyright (c) 2015 Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/util/test_util.h"

#include "kudu/util/hash_util.h"

namespace kudu {

// Test Murmur2 Hash64 returns the expected values for inputs. These tests are
// duplicated on the Java side to ensure that hash computations are stable
// across both platforms.
TEST(HashUtilTest, TestMurmur2Hash64) {
  uint64_t hash;

  hash = HashUtil::MurmurHash2_64("ab", 2, 0);
  ASSERT_EQ(7115271465109541368, hash);

  hash = HashUtil::MurmurHash2_64("abcdefg", 7, 0);
  ASSERT_EQ(2601573339036254301, hash);

  hash = HashUtil::MurmurHash2_64("quick brown fox", 15, 42);
  ASSERT_EQ(3575930248840144026, hash);
}

} // namespace kudu
