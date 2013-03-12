// Copyright (c) 2012, Cloudera, inc.
//
// Utility functions for dealing with a byte array as if it were a bitmap.
#ifndef KUDU_UTIL_BITMAP_H
#define KUDU_UTIL_BITMAP_H

#include "gutil/bits.h"

namespace kudu {

// Return the number of bytes necessary to store the given number of bits.
inline size_t BitmapSize(size_t num_bits) {
  return (num_bits + 7) / 8;
}

// Set the given bit.
inline void BitmapSet(uint8_t *bitmap, size_t idx) {
  bitmap[idx >> 3] |= 1 << (idx & 7);
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

// Iterator which yields the set bits in a bitmap.
// Example usage:
//   for (TrueBitIterator iter(bitmap, n_bits);
//        !iter.done();
//        ++iter) {
//     int next_onebit_position = *iter;
//   }
class TrueBitIterator {
public:
  TrueBitIterator(const uint8_t *bitmap, size_t n_bits) :
    bitmap_(bitmap),
    cur_byte_(0),
    cur_byte_idx_(0),
    n_bits_(n_bits),
    n_bytes_(BitmapSize(n_bits_)),
    bit_idx_(0)
  {
    if (n_bits_ == 0) {
      cur_byte_idx_ = 1; // sets done
    } else {
      cur_byte_ = bitmap[0];
      AdvanceToNextOneBit();
    }
  }

  TrueBitIterator &operator ++() {
    DCHECK(!done());
    DCHECK(cur_byte_ & 1);
    cur_byte_ &= (~1);
    AdvanceToNextOneBit();
    return *this;
  }

  bool done() const {
    return cur_byte_idx_ >= n_bytes_;
  }

  size_t operator *() const {
    DCHECK(!done());
    return bit_idx_;
  }

private:
  void AdvanceToNextOneBit() {
    while (cur_byte_ == 0) {
      cur_byte_idx_++;
      if (cur_byte_idx_ >= n_bytes_) return;
      cur_byte_ = bitmap_[cur_byte_idx_];
      bit_idx_ = cur_byte_idx_ * 8;
    }
    LOG(INFO) << "Found next nonzero byte at " << (int)cur_byte_idx_
              << " val=" << (int)cur_byte_;

    DCHECK_NE(cur_byte_, 0);
    int set_bit = Bits::FindLSBSetNonZero(cur_byte_);
    bit_idx_ += set_bit;
    cur_byte_ >>= set_bit;
  }

  const uint8_t *bitmap_;
  uint8_t cur_byte_;
  uint8_t cur_byte_idx_;

  const size_t n_bits_;
  const size_t n_bytes_;
  size_t bit_idx_;
};

} // namespace kudu

#endif
