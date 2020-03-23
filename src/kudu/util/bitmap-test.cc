// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/util/bitmap.h"

#include <cstdint>
#include <cstring>
#include <set>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/util/faststring.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"

namespace kudu {

static void ReadBackBitmap(uint8_t *bm, size_t bits,
                          std::vector<size_t> *result) {
  ForEachSetBit(bm, bits, [&](size_t b) {
                            result->push_back(b);
                          });
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

  EXPECT_EQ("   0: 10000000 10000000 00000000 00000001 11000000 00000000 00000000 00000001 \n",
            BitmapToString(bm, sizeof(bm) * 8));

  std::vector<size_t> read_back;

  ReadBackBitmap(bm, sizeof(bm)*8, &read_back);
  ASSERT_EQ("0,8,31,32,33,63", JoinElements(read_back, ","));
}


TEST(TestBitMap, TestIteration2) {
  uint8_t bm[1];
  memset(bm, 0, sizeof(bm));
  BitmapSet(bm, 1);

  std::vector<size_t> read_back;

  ReadBackBitmap(bm, 3, &read_back);
  ASSERT_EQ("1", JoinElements(read_back, ","));
}

struct RandomBitmap {
  RandomBitmap(int n_bits, double set_ratio) {
    bm.resize(BitmapSize(n_bits));
    Random r(GetRandomSeed32());
    for (int i = 0; i < n_bits; i++) {
      bool set = r.NextDoubleFraction() < set_ratio;
      BitmapChange(bm.data(), i, set);
      if (set) {
        set_bits.insert(i);
      }
    }
  }

  faststring bm;
  std::set<int> set_bits;
};

TEST(TestBitMap, TestIterationRandom) {
  Random r(GetRandomSeed32());
  const auto kNumBits = 1 + r.Uniform(100);
  RandomBitmap bm(kNumBits, r.NextDoubleFraction());

  std::set<int> remaining = bm.set_bits;

  // Iterate over the set bits and remove them from the set. When we're
  // done we should have none left in the set.
  ForEachSetBit(bm.bm.data(), kNumBits,
                [&](size_t b) {
                  EXPECT_EQ(1, remaining.erase(b));
                });
  EXPECT_EQ(remaining.size(), 0);
}



TEST(TestBitMap, Benchmark) {
  static constexpr int kNumBits = 1000;
  static constexpr int kNumTrials = 1000;
  for (int frac = 0; frac <= 100; frac++) {
    RandomBitmap bm(kNumBits, frac/100.0);

    volatile int sink = 0;
    for (int i = 0; i < kNumTrials; i++) {
      int sum = 0;
      ForEachSetBit(bm.bm.data(), kNumBits,
                    [&](size_t b) {
                      sum += b;
                    });
      sink += sum;
    }
  }
}

TEST(TestBitMap, TestSetAndTestBits) {
  uint8_t bm[1];
  memset(bm, 0, sizeof(bm));

  size_t num_bits = sizeof(bm) * 8;
  for (size_t i = 0; i < num_bits; i++) {
    ASSERT_FALSE(BitmapTest(bm, i));

    BitmapSet(bm, i);
    ASSERT_TRUE(BitmapTest(bm, i));

    BitmapClear(bm, i);
    ASSERT_FALSE(BitmapTest(bm, i));

    BitmapChange(bm, i, true);
    ASSERT_TRUE(BitmapTest(bm, i));

    BitmapChange(bm, i, false);
    ASSERT_FALSE(BitmapTest(bm, i));
  }

  // Set the other bit: 01010101
  for (size_t i = 0; i < num_bits; ++i) {
    ASSERT_FALSE(BitmapTest(bm, i));
    if (i & 1) BitmapSet(bm, i);
  }

  // Check and Clear the other bit: 0000000
  for (size_t i = 0; i < num_bits; ++i) {
    ASSERT_EQ(!!(i & 1), BitmapTest(bm, i));
    if (i & 1) BitmapClear(bm, i);
  }

  // Check if bits are zero and change the other to one
  for (size_t i = 0; i < num_bits; ++i) {
    ASSERT_FALSE(BitmapTest(bm, i));
    BitmapChange(bm, i, i & 1);
  }

  // Check the bits change them again
  for (size_t i = 0; i < num_bits; ++i) {
    ASSERT_EQ(!!(i & 1), BitmapTest(bm, i));
    BitmapChange(bm, i, !(i & 1));
  }

  // Check the last setup
  for (size_t i = 0; i < num_bits; ++i) {
    ASSERT_EQ(!(i & 1), BitmapTest(bm, i));
  }
}

TEST(TestBitMap, TestBulkSetAndTestBits) {
  uint8_t bm[16];
  size_t total_size = sizeof(bm) * 8;

  // Test Bulk change bits and test bits
  for (int i = 0; i < 4; ++i) {
    bool value = i & 1;
    size_t num_bits = total_size;
    while (num_bits > 0) {
      for (size_t offset = 0; offset < num_bits; ++offset) {
        BitmapChangeBits(bm, 0, total_size, !value);
        BitmapChangeBits(bm, offset, num_bits - offset, value);

        ASSERT_EQ(value, BitmapIsAllSet(bm, offset, num_bits));
        ASSERT_EQ(!value, BitmapIsAllZero(bm, offset, num_bits));

        if (offset > 1) {
          ASSERT_EQ(value, BitmapIsAllZero(bm, 0, offset - 1));
          ASSERT_EQ(!value, BitmapIsAllSet(bm, 0, offset - 1));
        }

        if ((offset + num_bits) < total_size) {
          ASSERT_EQ(value, BitmapIsAllZero(bm, num_bits, total_size));
          ASSERT_EQ(!value, BitmapIsAllSet(bm, num_bits, total_size));
        }
      }
      num_bits--;
    }
  }
}

TEST(TestBitMap, TestFindBit) {
  uint8_t bm[16];

  size_t num_bits = sizeof(bm) * 8;
  BitmapChangeBits(bm, 0, num_bits, false);
  while (num_bits > 0) {
    for (size_t offset = 0; offset < num_bits; ++offset) {
      size_t idx;
      ASSERT_FALSE(BitmapFindFirstSet(bm, offset, num_bits, &idx));
      ASSERT_TRUE(BitmapFindFirstZero(bm, offset, num_bits, &idx));
      ASSERT_EQ(idx, offset);
    }
    num_bits--;
  }

  num_bits = sizeof(bm) * 8;
  for (int i = 0; i < num_bits; ++i) {
    BitmapChange(bm, i, i & 3);
  }

  for (; num_bits > 0; num_bits--) {
    for (size_t offset = 0; offset < num_bits; ++offset) {
      size_t idx;

      // Find a set bit
      bool res = BitmapFindFirstSet(bm, offset, num_bits, &idx);
      size_t expected_set_idx = (offset + !(offset & 3));
      bool expect_set_found = (expected_set_idx < num_bits);
      ASSERT_EQ(expect_set_found, res);
      if (expect_set_found) {
        ASSERT_EQ(expected_set_idx, idx);
      }

      // Find a zero bit
      res = BitmapFindFirstZero(bm, offset, num_bits, &idx);
      size_t expected_zero_idx = offset + ((offset & 3) ? (4 - (offset & 3)) : 0);
      bool expect_zero_found = (expected_zero_idx < num_bits);
      ASSERT_EQ(expect_zero_found, res);
      if (expect_zero_found) {
        ASSERT_EQ(expected_zero_idx, idx);
      }
    }
  }
}

TEST(TestBitMap, TestBitmapIteration) {
  uint8_t bm[8];
  memset(bm, 0, sizeof(bm));
  BitmapSet(bm, 0);
  BitmapSet(bm, 8);
  BitmapSet(bm, 31);
  BitmapSet(bm, 32);
  BitmapSet(bm, 33);
  BitmapSet(bm, 63);

  BitmapIterator biter(bm, sizeof(bm) * 8);

  size_t i = 0;
  size_t size;
  bool value = false;
  bool expected_value = true;
  size_t expected_sizes[] = {1, 7, 1, 22, 3, 29, 1, 0};
  while ((size = biter.Next(&value)) > 0) {
    ASSERT_LT(i, 8);
    ASSERT_EQ(expected_value, value);
    ASSERT_EQ(expected_sizes[i], size);
    expected_value = !expected_value;
    i++;
  }
  ASSERT_EQ(expected_sizes[i], size);
}

TEST(TestBitMap, TestEquals) {
  uint8_t bm1[8] = { 0 };
  uint8_t bm2[8] = { 0 };
  size_t num_bits = sizeof(bm1) * 8;
  ASSERT_TRUE(BitmapEquals(bm1, bm2, num_bits));

  // Loop over each bit starting from the end and going to the beginning. In
  // each iteration, set the bit in one bitmap and verify that although the two
  // bitmaps aren't equal, if we were to ignore the changed bits, they are still equal.
  for (int i = num_bits - 1; i >= 0; i--) {
    SCOPED_TRACE(i);
    BitmapChange(bm1, i, true);
    ASSERT_FALSE(BitmapEquals(bm1, bm2, num_bits));
    ASSERT_TRUE(BitmapEquals(bm1, bm2, i));
  }

  // Now loop in the other direction, setting the second bitmap bit by bit.
  // As before, if we consider the bitmaps in their entirety, they're not equal,
  // but if we consider just the sequences where both are set, they are equal.
  for (int i = 0; i < num_bits - 1; i++) {
    SCOPED_TRACE(i);
    BitmapChange(bm2, i, true);
    ASSERT_FALSE(BitmapEquals(bm1, bm2, num_bits));
    ASSERT_TRUE(BitmapEquals(bm1, bm2, i + 1));
  }

  // If we set the very last bit, both bitmaps are now equal in their entirety.
  BitmapChange(bm2, num_bits - 1, true);
  ASSERT_TRUE(BitmapEquals(bm1, bm2, num_bits));

  // Test equality on overlapped bitmaps (i.e. a single underlying bitmap, two
  // subsequences of which are considered to be two separate bitmaps).

  // Set every third bit; the rest are unset.
  uint8_t bm3[8] = { 0 };
  for (int i = 0; i < num_bits; i += 3) {
    BitmapChange(bm3, i, true);
  }

  ASSERT_TRUE(BitmapEquals(bm3, bm3, num_bits)); // fully overlapped
  ASSERT_FALSE(BitmapEquals(bm3, bm3 + 1, num_bits - 8)); // off by one byte
  ASSERT_TRUE(BitmapEquals(bm3, bm3 + 3, num_bits - 24)); // off by three bytes
}

TEST(TestBitMap, TestCopy) {
  constexpr int kNumBytes = 8;
  constexpr int kNumBits = kNumBytes * 8;
  constexpr uint8_t kAllZeroes[kNumBytes] = { 0 };

  {
    // Byte-aligned copy with no offsets.
    uint8_t res[kNumBytes];
    BitmapChangeBits(res, 0, kNumBits, 1);

    BitmapCopy(res, 0, kAllZeroes, 0, kNumBits);
    ASSERT_TRUE(BitmapIsAllZero(res, 0, kNumBits));
  }
  {
    // Byte-aligned copy with offsets.
    uint8_t res[kNumBytes];
    BitmapChangeBits(res, 0, kNumBits, 1);

    ASSERT_TRUE(BitmapIsAllSet(res, 0, kNumBits));
    constexpr size_t stride = kNumBits / 4;
    for (int i = 0; i < kNumBits; i += stride) {
      BitmapCopy(res, i, kAllZeroes, i, stride);
      // The bits before the copy should reflect the copied data, while the bits
      // after should reflect the original data.
      ASSERT_TRUE(BitmapIsAllZero(res, 0, stride + i));
      ASSERT_TRUE(BitmapIsAllSet(res, stride + i, kNumBits));
    }
    ASSERT_TRUE(BitmapIsAllZero(res, 0, kNumBits));
  }
  {
    // Non-byte aligned; overwrite all but the first bit.
    uint8_t res[kNumBytes];
    BitmapChangeBits(res, 0, kNumBits, 1);

    BitmapCopy(res, 1, kAllZeroes, 0, kNumBits - 1);
    ASSERT_TRUE(BitmapTest(res, 0));
    ASSERT_TRUE(BitmapIsAllZero(res, 1, kNumBits - 1));
  }
  {
    // Non-byte aligned; overwrite all but the last bit.
    uint8_t res[kNumBytes];
    BitmapChangeBits(res, 0, kNumBits, 1);

    BitmapCopy(res, 0, kAllZeroes, 1, kNumBits - 1);
    ASSERT_TRUE(BitmapTest(res, kNumBits - 1));
    ASSERT_TRUE(BitmapIsAllZero(res, 0, kNumBits - 1));
  }
  {
    // Non-byte aligned; overwrite all but the first and last bits.
    uint8_t res[kNumBytes];
    BitmapChangeBits(res, 0, kNumBits, 1);

    BitmapCopy(res, 1, kAllZeroes, 1, kNumBits - 2);
    ASSERT_TRUE(BitmapTest(res, 0));
    ASSERT_TRUE(BitmapTest(res, kNumBits - 1));
    ASSERT_TRUE(BitmapIsAllZero(res, 1, kNumBits - 2));
  }
}

#ifndef NDEBUG
TEST(TestBitMapDeathTest, TestCopyOverlap) {
  uint8_t bm[2] = { 0 };
  ASSERT_DEATH({ BitmapCopy(bm, 0, bm, 0, 16); },
               "Source and destination overlap");
}

TEST(TestBitMapDeathTest, TestCopyOverlapSrcAfterDst) {
  uint8_t bm[2] = { 0 };
  ASSERT_DEATH({ BitmapCopy(bm, 0, bm, 1, 15); },
               "Source and destination overlap");
}

TEST(TestBitMapDeathTest, TestCopyOverlapDstAfterSrc) {
  uint8_t bm[2] = { 0 };
  ASSERT_DEATH({ BitmapCopy(bm, 1, bm, 0, 15); },
               "Source and destination overlap");
}
#endif

} // namespace kudu
