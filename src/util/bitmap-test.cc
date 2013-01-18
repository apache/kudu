// Copyright (c) 2012, Cloudera, inc.

#include <gtest/gtest.h>
#include <vector>

#include "gutil/strings/join.h"
#include "util/bitmap.h"

namespace kudu {

static int ReadBackBitmap(uint8_t *bm, size_t bits,
                           std::vector<size_t> *result) {
  int iters = 0;
  for (TrueBitIterator iter(bm, bits);
       !iter.done();
       ++iter) {
    size_t val = *iter;
    result->push_back(val);

    iters++;
  }
  return iters;
}

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

  int iters = ReadBackBitmap(bm, sizeof(bm)*8, &read_back);
  ASSERT_EQ(6, iters);
  ASSERT_EQ("0,8,31,32,33,63", JoinElements(read_back, ","));
}


TEST(TestBitMap, TestIteration2) {
  uint8_t bm[1];
  memset(bm, 0, sizeof(bm));
  BitmapSet(bm, 1);

  std::vector<size_t> read_back;

  int iters = ReadBackBitmap(bm, 3, &read_back);
  ASSERT_EQ(1, iters);
  ASSERT_EQ("1", JoinElements(read_back, ","));
}

} // namespace kudu
