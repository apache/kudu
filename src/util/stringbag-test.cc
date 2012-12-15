// Copyright (c) 2012, Cloudera, inc.

#include <gtest/gtest.h>

#include "util/stringbag.h"

namespace kudu {

TEST(TestStringBag, TestBasics) {
  char storage[4096];
  StringBag<uint32_t> *sb =
    new (storage) StringBag<uint32_t>(100, sizeof(storage));

  ASSERT_EQ(0, sb->get(0).size());

  Slice s0("hello world");
  ASSERT_TRUE(sb->assign(0, s0));

  Slice s5("goodbye world");
  ASSERT_TRUE(sb->assign(5, s5));

  ASSERT_EQ(s0.ToString(), sb->get(0).ToString());
  ASSERT_EQ(s5.ToString(), sb->get(5).ToString());

  // Reassigning a shorter string should reuse memory
  Slice long_slice("123456");
  ASSERT_TRUE(sb->assign(0, long_slice));
  Slice long_slice_in_bag = sb->get(0);

  Slice shorter_slice("xyz");
  ASSERT_TRUE(sb->assign(0, shorter_slice));
  Slice shorter_slice_in_bag = sb->get(0);
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
    if (!sb->assign(i, s))
      break;
  }

  // Verify that we got a false response from assign()
  ASSERT_LT(i, 10000);

  LOG(INFO) << "full bag: " << sb->ToString(width);
}

} // namespace kudu
