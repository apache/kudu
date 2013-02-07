// Copyright (c) 2012, Cloudera, inc.

#ifndef KUDU_COMMON_KEYENCODER_H
#define KUDU_COMMON_KEYENCODER_H

#include <boost/noncopyable.hpp>

#include <arpa/inet.h>
#include <emmintrin.h>
#include <smmintrin.h>
#include <string.h>

#include "util/faststring.h"

namespace kudu {

class KeyEncoder : boost::noncopyable {
public:
  KeyEncoder(faststring *dst) : dst_(dst) {}

  void EncodeUInt32(uint32_t x, bool is_last) {
    // Byteswap so it is correctly comparable
    x = htonl(x);

    dst_->append(reinterpret_cast<uint8_t *>(&x), sizeof(x));
  }

  void EncodeBytes(const Slice &s, bool is_last) {
#ifdef USE_SSE
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
  faststring *dst_;
};

} // namespace kudu

#endif
