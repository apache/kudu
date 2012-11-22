// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_UTIL_BITMAP_H
#define KUDU_UTIL_BITMAP_H

#include "gutil/bits.h"

namespace kudu {

inline size_t BitmapSize(size_t num_bits) {
  return (num_bits + 7) / 8;
}

inline void BitmapSet(uint8_t *bitmap, size_t idx) {
  bitmap[idx >> 3] |= 1 << (idx & 7);
}

inline void BitmapClear(uint8_t *bitmap, size_t idx) {
  bitmap[idx >> 3] &= ~(1 << (idx & 7));
}

inline bool BitmapTest(const uint8_t *bitmap, size_t idx) {
  return bitmap[idx >> 3] & (1 << (idx & 7));
}

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
    cur_byte_idx_(0),
    cur_byte_(0),
    n_bits_(n_bits),
    bit_idx_(0),
    done_(false)
  {
    if (n_bits_ == 0) {
      done_ = true;
    } else {
      cur_byte_ = bitmap[0];
      AdvanceToNext();
    }
  }

  TrueBitIterator &operator ++() {
    DCHECK(!done());
    DCHECK(cur_byte_ & 1);
    cur_byte_ >>= 1;
    bit_idx_++;
    AdvanceToNext();
    return *this;
  }

  bool done() const {
    return done_;
  }

  size_t operator *() const {
    DCHECK(!done());
    return bit_idx_;
  }

private:
  // Position cur_byte_ to the next non-zero byte,
  // and shift it such that the least significant bit is
  // 1. Sets bit_idx_ to correspond to this bit.
  void AdvanceToNext() {

    // TODO: can iterate by uint32 or uint64 instead of by
    // byte for better performance here down the line.
    while (cur_byte_ == 0 &&
           bit_idx_ < n_bits_) {
      cur_byte_idx_++;
      cur_byte_ = bitmap_[cur_byte_idx_];
      bit_idx_ = cur_byte_idx_ * 8;
    }

    if (cur_byte_ == 0) {
      done_ = true;
      return;
    }

    int set_bit = Bits::FindLSBSetNonZero(cur_byte_);
    bit_idx_ += set_bit;
    cur_byte_ >>= set_bit;
  }

  const uint8_t *bitmap_;
  uint8_t cur_byte_idx_;

  uint8_t cur_byte_;
  const size_t n_bits_;
  size_t bit_idx_;
  bool done_;
};

} // namespace kudu

#endif
