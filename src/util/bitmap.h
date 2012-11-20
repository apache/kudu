// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_UTIL_BITMAP_H
#define KUDU_UTIL_BITMAP_H

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

} // namespace kudu

#endif
