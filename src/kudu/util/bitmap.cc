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

#include <cstring>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/gutil/stringprintf.h"

using std::string;

namespace kudu {

void BitmapChangeBits(uint8_t *bitmap, size_t offset, size_t num_bits, bool value) {
  DCHECK_GT(num_bits, 0);

  size_t start_byte = (offset >> 3);
  size_t end_byte = (offset + num_bits - 1) >> 3;
  int single_byte = (start_byte == end_byte);

  // Change the last bits of the first byte
  size_t left = offset & 0x7;
  size_t right = (single_byte) ? (left + num_bits) : 8;
  uint8_t mask = ((0xff << left) & (0xff >> (8 - right)));
  if (value) {
    bitmap[start_byte++] |= mask;
  } else {
    bitmap[start_byte++] &= ~mask;
  }

  // Nothing left... I'm done
  if (single_byte) {
    return;
  }

  // change the middle bits
  if (end_byte > start_byte) {
    const uint8_t pattern8[2] = { 0x00, 0xff };
    memset(bitmap + start_byte, pattern8[value], end_byte - start_byte);
  }

  // change the first bits of the last byte
  right = offset + num_bits - (end_byte << 3);
  mask = (0xff >> (8 - right));
  if (value) {
    bitmap[end_byte] |= mask;
  } else {
    bitmap[end_byte] &= ~mask;
  }
}

bool BitmapFindFirst(const uint8_t *bitmap, size_t offset, size_t bitmap_size,
                     bool value, size_t *idx) {
  const uint64_t pattern64[2] = { 0xffffffffffffffff, 0x0000000000000000 };
  const uint8_t pattern8[2] = { 0xff, 0x00 };
  size_t bit;

  DCHECK_LE(offset, bitmap_size);

  // Jump to the byte at specified offset
  const uint8_t *p = bitmap + (offset >> 3);
  size_t num_bits = bitmap_size - offset;

  // Find a 'value' bit at the end of the first byte
  if ((bit = offset & 0x7)) {
    for (; bit < 8 && num_bits > 0; ++bit) {
      if (BitmapTest(p, bit) == value) {
        *idx = ((p - bitmap) << 3) + bit;
        return true;
      }

      num_bits--;
    }

    p++;
  }

  // check 64bit at the time for a 'value' bit
  const uint64_t *u64 = (const uint64_t *)p;
  while (num_bits >= 64 && *u64 == pattern64[value]) {
    num_bits -= 64;
    u64++;
  }

  // check 8bit at the time for a 'value' bit
  p = (const uint8_t *)u64;
  while (num_bits >= 8 && *p == pattern8[value]) {
    num_bits -= 8;
    p++;
  }

  // Find a 'value' bit at the beginning of the last byte
  for (bit = 0; num_bits > 0; ++bit) {
    if (BitmapTest(p, bit) == value) {
      *idx = ((p - bitmap) << 3) + bit;
      return true;
    }
    num_bits--;
  }

  return false;
}

void BitmapCopy(uint8_t* dst, size_t dst_offset,
                const uint8_t* src, size_t src_offset,
                size_t num_bits) {
  DCHECK_GT(num_bits, 0);

  // Prohibit overlap in debug builds.
#ifndef NDEBUG
  const uint8_t* src_start = src + (src_offset >> 3);
  const uint8_t* src_end = src + ((src_offset + num_bits) >> 3);
  uint8_t* dst_start = dst + (dst_offset >> 3);
  uint8_t* dst_end = dst + ((dst_offset + num_bits) >> 3);
  DCHECK(src_start >= dst_end || dst_start >= src_end)
      << "Source and destination overlap";
#endif

  // If the copy is entirely byte-aligned, we can use memcpy.
  if ((src_offset & 7) == 0 && (dst_offset & 7) == 0 && (num_bits & 7) == 0) {
    memcpy(dst + (dst_offset >> 3),
           src + (src_offset >> 3),
           BitmapSize(num_bits));
    return;
  }

  // Otherwise, we fallback to a slower bit-by-bit copy.
  //
  // TODO(adar): this can be optimized using word-by-word operations.
  for (size_t bit_num = 0; bit_num < num_bits; bit_num++) {
    BitmapChange(dst, dst_offset + bit_num, BitmapTest(src, src_offset + bit_num));
  }
}

string BitmapToString(const uint8_t *bitmap, size_t num_bits) {
  string s;
  size_t index = 0;
  while (index < num_bits) {
    StringAppendF(&s, "%4zu: ", index);
    for (int i = 0; i < 8 && index < num_bits; ++i) {
      for (int j = 0; j < 8 && index < num_bits; ++j) {
        StringAppendF(&s, "%d", BitmapTest(bitmap, index));
        index++;
      }
      StringAppendF(&s, " ");
    }
    StringAppendF(&s, "\n");
  }
  return s;
}

} // namespace kudu
