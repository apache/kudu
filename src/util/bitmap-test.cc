// Copyright (c) 2012, Cloudera, inc.

#include <gtest/gtest.h>
#include <vector>

#include "gutil/strings/join.h"
#include "util/bitmap.h"

namespace kudu {

TEST(TestBitMap, TestIteration) {
  uint8_t bm[8];
  memset(bm, 0, sizeof(bm));
  BitmapSet(bm, 0);
  BitmapSet(bm, 8);
  BitmapSet(bm, 31);
  BitmapSet(bm, 32);
  BitmapSet(bm, 33);
  BitmapSet(bm, 63);

  std::vector<size_t> read_back;

  int iters = 0;
  for (TrueBitIterator iter(bm, sizeof(bm)*8);
       !iter.done();
       ++iter) {
    size_t val = *iter;
    read_back.push_back(val);

    iters++;
    ASSERT_LE(iters, 6);
  }

  ASSERT_EQ(6, iters);
  ASSERT_EQ("0,8,31,32,33,63", JoinElements(read_back, ","));
}

} // namespace kudu
