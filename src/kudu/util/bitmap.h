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
//
// Utility functions for dealing with a byte array as if it were a bitmap.
#ifndef KUDU_UTIL_BITMAP_H
#define KUDU_UTIL_BITMAP_H

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/bits.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/fastmem.h"

namespace kudu {

// Return the number of bytes necessary to store the given number of bits.
inline size_t BitmapSize(size_t num_bits) {
  return (num_bits + 7) / 8;
}

// Set the given bit.
inline void BitmapSet(uint8_t *bitmap, size_t idx) {
  bitmap[idx >> 3] |= 1 << (idx & 7);
}

// Switch the given bit to the specified value.
inline void BitmapChange(uint8_t *bitmap, size_t idx, bool value) {
  bitmap[idx >> 3] = (bitmap[idx >> 3] & ~(1 << (idx & 7))) | ((!!value) << (idx & 7));
}

// Clear the given bit.
inline void BitmapClear(uint8_t *bitmap, size_t idx) {
  bitmap[idx >> 3] &= ~(1 << (idx & 7));
}

// Test/get the given bit.
inline bool BitmapTest(const uint8_t *bitmap, size_t idx) {
  return bitmap[idx >> 3] & (1 << (idx & 7));
}

// Merge the two bitmaps using bitwise or. Both bitmaps should have at least
// n_bits valid bits.
inline void BitmapMergeOr(uint8_t *dst, const uint8_t *src, size_t n_bits) {
  size_t n_bytes = BitmapSize(n_bits);
  for (size_t i = 0; i < n_bytes; i++) {
    *dst++ |= *src++;
  }
}

// Set bits from offset to (offset + num_bits) to the specified value
void BitmapChangeBits(uint8_t *bitmap, size_t offset, size_t num_bits, bool value);

// Find the first bit of the specified value, starting from the specified offset.
bool BitmapFindFirst(const uint8_t *bitmap, size_t offset, size_t bitmap_size,
                     bool value, size_t *idx);

// Find the first set bit in the bitmap, at the specified offset.
inline bool BitmapFindFirstSet(const uint8_t *bitmap, size_t offset,
                               size_t bitmap_size, size_t *idx) {
  return BitmapFindFirst(bitmap, offset, bitmap_size, true, idx);
}

// Find the first zero bit in the bitmap, at the specified offset.
inline bool BitmapFindFirstZero(const uint8_t *bitmap, size_t offset,
                                size_t bitmap_size, size_t *idx) {
  return BitmapFindFirst(bitmap, offset, bitmap_size, false, idx);
}

// Returns true if the bitmap contains only ones.
inline bool BitmapIsAllSet(const uint8_t *bitmap, size_t offset, size_t bitmap_size) {
  DCHECK_LE(offset, bitmap_size);
  size_t idx;
  return !BitmapFindFirstZero(bitmap, offset, bitmap_size, &idx);
}

// Returns true if the bitmap contains only zeros.
inline bool BitmapIsAllZero(const uint8_t *bitmap, size_t offset, size_t bitmap_size) {
  DCHECK_LE(offset, bitmap_size);
  size_t idx;
  return !BitmapFindFirstSet(bitmap, offset, bitmap_size, &idx);
}

// Returns true if the two bitmaps are equal.
//
// It is assumed that both bitmaps have 'bitmap_size' number of bits.
inline bool BitmapEquals(const uint8_t* bm1, const uint8_t* bm2, size_t bitmap_size) {
  // Use memeq() to check all of the full bytes.
  size_t num_full_bytes = bitmap_size >> 3;
  if (!strings::memeq(bm1, bm2, num_full_bytes)) {
    return false;
  }

  // Check any remaining bits in one extra operation.
  size_t num_remaining_bits = bitmap_size - (num_full_bytes << 3);
  if (num_remaining_bits == 0) {
    return true;
  }
  DCHECK_LT(num_remaining_bits, 8);
  uint8_t mask = (1 << num_remaining_bits) - 1;
  return (bm1[num_full_bytes] & mask) == (bm2[num_full_bytes] & mask);
}

// Copies a slice of 'src' to 'dst'. Offsets and sizing are all expected to be
// bit quantities.
//
// Note: 'src' and 'dst' may not overlap.
void BitmapCopy(uint8_t* dst, size_t dst_offset,
                const uint8_t* src, size_t src_offset,
                size_t num_bits);

std::string BitmapToString(const uint8_t *bitmap, size_t num_bits);

// Iterator which yields ranges of set and unset bits.
// Example usage:
//   bool value;
//   size_t size;
//   BitmapIterator iter(bitmap, n_bits);
//   while ((size = iter.Next(&value))) {
//      printf("bitmap block len=%lu value=%d\n", size, value);
//   }
class BitmapIterator {
 public:
  BitmapIterator(const uint8_t *map, size_t num_bits)
    : offset_(0), num_bits_(num_bits), map_(map)
  {}

  bool done() const {
    return (num_bits_ - offset_) == 0;
  }

  void SeekTo(size_t bit) {
    DCHECK_LE(bit, num_bits_);
    offset_ = bit;
  }

  size_t Next(bool *value) {
    size_t len = num_bits_ - offset_;
    if (PREDICT_FALSE(len == 0))
      return(0);

    *value = BitmapTest(map_, offset_);

    size_t index;
    if (BitmapFindFirst(map_, offset_, num_bits_, !(*value), &index)) {
      len = index - offset_;
    } else {
      index = num_bits_;
    }

    offset_ = index;
    return len;
  }

 private:
  size_t offset_;
  size_t num_bits_;
  const uint8_t *map_;
};

// Iterate over the bits in 'bitmap' and call 'func' for each set bit.
// 'func' should take a single argument which is the bit's index.
template<class F>
void ForEachSetBit(const uint8_t* __restrict__ bitmap,
                   int n_bits,
                   const F& func);

// Iterate over the bits in 'bitmap' and call 'func' for each unset bit.
// 'func' should take a single argument which is the bit's index.
template<class F>
void ForEachUnsetBit(const uint8_t* __restrict__ bitmap,
                     int n_bits,
                     const F& func);


////////////////////////////////////////////////////////////
// Implementation details
////////////////////////////////////////////////////////////

template<bool SET, class F>
inline void ForEachBit(const uint8_t* bitmap,
                       int n_bits,
                       const F& func) {
  int rem = n_bits;
  int base_idx = 0;
  while (rem >= 64) {
    uint64_t w = UnalignedLoad<uint64_t>(bitmap);
    if (!SET) {
      w = ~w;
    }
    bitmap += sizeof(uint64_t);

    // Within each word, keep flipping the least significant non-zero bit and adding
    // the bit index to the output until none are set.
    //
    // Get the count up front so that the loop can be unrolled without dependencies.
    // The 'tzcnt' instruction that's generated here has a latency of 3 so unrolling
    // and avoiding any cross-iteration dependencies is beneficial.
    int tot_count = Bits::CountOnes64withPopcount(w);
#ifdef __clang__
#pragma unroll(3)
#else
#pragma GCC unroll 3
#endif
    for (int i = 0; i < tot_count; i++) {
      func(base_idx + Bits::FindLSBSetNonZero64(w));
      w &= w - 1;
    }
    base_idx += 64;
    rem -= 64;
  }

  while (rem > 0) {
    uint8_t b = *bitmap++;
    if (!SET) {
      b = ~b;
    }
    while (b) {
      int idx = base_idx + Bits::FindLSBSetNonZero(b);
      if (idx >= n_bits) break;
      func(idx);
      b &= b - 1;
    }
    base_idx += 8;
    rem -= 8;
  }
}

template<class F>
inline void ForEachSetBit(const uint8_t* __restrict__ bitmap,
                          int n_bits,
                          const F& func) {
  ForEachBit<true, F>(bitmap, n_bits, func);
}

template<class F>
inline void ForEachUnsetBit(const uint8_t* __restrict__ bitmap,
                            int n_bits,
                            const F& func) {
  ForEachBit<false, F>(bitmap, n_bits, func);
}

} // namespace kudu

#endif
