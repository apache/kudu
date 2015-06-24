// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <gtest/gtest.h>

#include "kudu/common/id_mapping.h"

namespace kudu {
// Basic unit test for IdMapping.
TEST(TestIdMapping, TestSimple) {
  IdMapping m;
  ASSERT_EQ(-1, m.get(1));
  m.set(1, 10);
  m.set(2, 20);
  m.set(3, 30);
  ASSERT_EQ(10, m.get(1));
  ASSERT_EQ(20, m.get(2));
  ASSERT_EQ(30, m.get(3));
}

// Insert enough entries in the mapping so that it is forced to rehash
// itself.
TEST(TestIdMapping, TestRehash) {
  IdMapping m;

  for (int i = 0; i < 1000; i++) {
    m.set(i, i * 10);
  }
  for (int i = 0; i < 1000; i++) {
    ASSERT_EQ(i * 10, m.get(i));
  }
}

} // namespace kudu
