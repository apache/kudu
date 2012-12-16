// Copyright (c) 2012, Cloudera, inc.

#include <gtest/gtest.h>

#include "util/stringbag.h"

namespace kudu {

TEST(TestStringBag, TestBasics) {
  char storage[4096];
  StringBag<uint32_t> *sb =
    new (storage) StringBag<uint32_t>(100, sizeof(storage));

  ASSERT_EQ(0, sb->Get(0).size());

  Slice s0("hello world");
  ASSERT_TRUE(sb->Assign(0, s0));

  Slice s5("goodbye world");
  ASSERT_TRUE(sb->Assign(5, s5));

  ASSERT_EQ(s0.ToString(), sb->Get(0).ToString());
  ASSERT_EQ(s5.ToString(), sb->Get(5).ToString());

  // Reassigning a shorter string should reuse memory
  Slice long_slice("123456");
  ASSERT_TRUE(sb->Assign(0, long_slice));
  Slice long_slice_in_bag = sb->Get(0);

  Slice shorter_slice("xyz");
  ASSERT_TRUE(sb->Assign(0, shorter_slice));
  Slice shorter_slice_in_bag = sb->Get(0);
  ASSERT_EQ(long_slice_in_bag.data(),
            shorter_slice_in_bag.data());
}

TEST(TestStringBag, TestFullBag) {
  char storage[4096];
  int width = 400;
  StringBag<uint32_t> *sb =
    new (storage) StringBag<uint32_t>(width, sizeof(storage));

  Slice s("hello world");

  ASSERT_GT(s.size() * width, sizeof(storage))
    << "should not have room to fill up the bag with this slice";

  // Try to put 10k copies of this slice (won't fit)
  int i;
  for (i = 0; i < 10000; i++) {
    if (!sb->Assign(i, s))
      break;
  }

  // Verify that we got a false response from assign()
  ASSERT_LT(i, 10000);

  LOG(INFO) << "full bag: " << sb->ToString(width);
}

TEST(TestStringBag, TestInsert) {
  char storage[4096];
  int width = 600;
  StringBag<uint32_t> *sb =
    new (storage) StringBag<uint32_t>(width, sizeof(storage));

  ASSERT_TRUE(sb->Assign(0, Slice("hello 0")));
  ASSERT_TRUE(sb->Assign(1, Slice("hello 1")));

  // Contents: "hello 0", "hello 1"

  // Insert a new slice at pos 0: "inserted 0", "hello 0", "hello 1"
  ASSERT_TRUE(sb->Insert(0, 2, Slice("inserted 0")));

  LOG(INFO) << "After insert at 0: " << sb->ToString(width);
  ASSERT_EQ("inserted 0", sb->Get(0).ToString());
  ASSERT_EQ("hello 0", sb->Get(1).ToString());
  ASSERT_EQ("hello 1", sb->Get(2).ToString());

  // Insert a new slice at pos 3:
  // "inserted 0", "hello 0", "hello 1", "inserted 3"
  ASSERT_TRUE(sb->Insert(3, 3, Slice("inserted 3")));
  LOG(INFO) << "After insert at 3: " << sb->ToString(width);
  ASSERT_EQ("inserted 0", sb->Get(0).ToString());
  ASSERT_EQ("hello 0", sb->Get(1).ToString());
  ASSERT_EQ("hello 1", sb->Get(2).ToString());
  ASSERT_EQ("inserted 3", sb->Get(3).ToString());

  // Insert a new slice in the middle:
  // "inserted 0", "hello 0", "middle", "hello 1", "inserted 3"
  ASSERT_TRUE(sb->Insert(2, 4, Slice("middle")));
  LOG(INFO) << "After insert at 2: " << sb->ToString(width);
  ASSERT_EQ("inserted 0", sb->Get(0).ToString());
  ASSERT_EQ("hello 0", sb->Get(1).ToString());
  ASSERT_EQ("middle", sb->Get(2).ToString());
  ASSERT_EQ("hello 1", sb->Get(3).ToString());
  ASSERT_EQ("inserted 3", sb->Get(4).ToString());

  // Fill up the bag by inserting. Should eventually fail
  // due to insufficient space
  int count = 5;
  Slice s("hello world");
  int i;
  for (i = 0; i < 10000; i++) {
    if (!sb->Insert(0, count, s)) {
      break;
    }
    count++;
  }
  ASSERT_LT(count, width);
  ASSERT_LT(i, 10000);
}

// Test case which tries to fill up the bag exactly
// to the end, and verify we don't write past the end
TEST(TestStringBag, TestBoundaryCondition) {
  char storage[104];
  memcpy(&storage[100], "TEST", 4);
  StringBag<uint16_t> *sb =
    new (storage) StringBag<uint16_t>(40, sizeof(storage) - 4);
  Slice s("x");
  for (int i = 0; i < 40; i++) {
    if (!sb->Assign(i, s)) {
      break;
    }
  }
  LOG(INFO) << "full bag: " << sb->ToString(40);

  ASSERT_EQ('x', storage[99]);
  ASSERT_EQ('T', storage[100]);
}

} // namespace kudu
