// Copyright (c) 2013, Cloudera, inc.

#include "gutil/stringprintf.h"
#include "util/bitmap.h"

namespace kudu {

void BitmapChangeBits(uint8_t *bitmap, size_t offset, size_t num_bits, bool value) {
  const uint64_t pattern64[2] = { 0x0000000000000000, 0xffffffffffffffff };
  const uint8_t pattern8[2] = { 0x00, 0xff };
  size_t bit;

  // Jump to the byte at specified offset
  uint8_t *p = bitmap + (offset >> 3);

  // Change the last bits of the first byte
  if ((bit = offset & 0x7)) {
    // TODO(perf): change me with a mask
    for (; bit < 8 && num_bits > 0; ++bit) {
      BitmapChange(p, bit, value);
      num_bits--;
    }

    p++;
  }

  // change 64bit at the time
  uint64_t *u64 = (uint64_t *)p;
  while (num_bits >= 64) {
    *u64++ = pattern64[value];
    num_bits -= 64;
  }

  // change 8bit at the time
  p = (uint8_t *)u64;
  while (num_bits >= 8) {
    *p++ = pattern8[value];
    num_bits -= 8;
  }

  // change the first bits of the last byte
  for (bit = 0; num_bits > 0; ++bit) {
    // TODO(perf): change me with a mask
    BitmapChange(p, bit, value);
    num_bits--;
  }
}

bool BitmapFindFirst(const uint8_t *bitmap, size_t offset, size_t bitmap_size,
                     bool value, size_t *idx)
{
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

std::string BitmapToString(const uint8_t *bitmap, size_t num_bits) {
  std::string s;
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
