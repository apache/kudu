// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_COMMON_KEYENCODER_H
#define KUDU_COMMON_KEYENCODER_H


#include <arpa/inet.h>
#include <emmintrin.h>
#include <smmintrin.h>
#include <string.h>

#include "gutil/macros.h"
#include "util/faststring.h"

// The SSE-based encoding is not yet working. Don't define this!
#undef KEY_ENCODER_USE_SSE

namespace kudu {

// Utility class for generating a lexicographically comparable string from a
// composite key. This uses the following encoding:
//
// strings:
//   - Null terminate with "\x00\x00" (unless this is the last column in the key)
//   - escape '\0' with "\x00\x01" so that shorter strings compare before longer.
// unsigned ints: encode as big-endian so that smaller ints compare before larger
//
// TODO: use memcmpable_varint code here to make denser int keys
class KeyEncoder {
 public:
  explicit KeyEncoder(faststring *dst) : dst_(dst) {}

  const Slice ResetBufferAndEncodeToSlice(DataType type, const Slice &key) {
    Reset();
    dst_->append(key.data(), key.size());
    return Slice(*dst_);
  }

  const Slice ResetBufferAndEncodeToSlice(DataType type, const void* key) {
    Reset();
    switch (type) {
      case UINT32:
        EncodeUInt32(*reinterpret_cast<const uint32_t*>(key), false);
        break;
      case INT32:
        EncodeInt32(*reinterpret_cast<const int32_t*>(key), false);
        break;
      case STRING:
        const Slice* slice = reinterpret_cast<const Slice *>(key);
        dst_->append(slice->data(), slice->size());
        break;
    }
    return Slice(*dst_);
  }

  const Slice ResetBufferAndEncodeToSlice(DataType type, uint32_t key) {
    Reset();
    switch (type) {
      case UINT32:
        EncodeUInt32(key, false);
        break;
      case INT32:
        EncodeInt32(key, false);
        break;
      default:
        CHECK(false) << "Unexpected Type";
    }
    return Slice(*dst_);
  }

  void Reset() {
    dst_->clear();
  }

  faststring* Buffer() {
    return dst_;
  }

  void EncodeUInt32(uint32_t x, bool is_last) {
    // Byteswap so it is correctly comparable
    x = htonl(x);
    dst_->append(reinterpret_cast<uint8_t *>(&x), sizeof(x));
  }

  void EncodeInt32(int32_t x, bool is_last) {
    // flip the sign bit
    x ^= 1 << 31;
    // Byteswap so it is correctly comparable
    x = htonl(x);
    dst_->append(reinterpret_cast<uint8_t *>(&x), sizeof(x));
  }

  void EncodeBytes(const Slice &s, bool is_last) {
#ifdef KEY_ENCODER_USE_SSE
    // Work-in-progress code for using SSE to do the string escaping.
    // This doesn't work correctly yet, and this hasn't been a serious hot spot.
    char buf[16];
    const uint8_t *p = s.data();
    size_t rem = s.size();

    __m128i xmm_zero = _mm_setzero_si128();

    while (rem > 0) {
      size_t chunk = (rem < 16) ? rem : 16;
      memset(buf, 0, sizeof(buf));
      memcpy(buf, p, chunk);
      rem -= chunk;

      __m128i xmm_chunk = _mm_load_si128(
        reinterpret_cast<__m128i *>(buf));
      uint16_t zero_mask = _mm_movemask_epi8(
        _mm_cmpeq_epi8(xmm_zero, xmm_chunk));

      zero_mask &= ((1 << chunk) - 1);

      if (PREDICT_TRUE(zero_mask == 0)) {
        dst_->append(buf, chunk);
      } else {
        // zero_mask now has bit 'n' set for each n where
        // buf[n] == '\0'
        // TODO: use the two halves of the bitmask in a lookup
        // table with pshufb?

        CHECK(0) << "TODO";
      }
    }
    uint8_t zeros[] = {0, 0};
    if (!is_last) {
      dst_->append(zeros, 2);
    }
#else
    for (int i = 0; i < s.size(); i++) {
      if (PREDICT_FALSE(s[i] == '\0')) {
        dst_->append("\x00\x01", 2);
      } else {
        dst_->push_back(s[i]);
      }
    }
    if (!is_last) {
      dst_->append("\x00\x00", 2);
    }
#endif
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(KeyEncoder);
  faststring *dst_;
};

} // namespace kudu

#endif
