// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_UTIL_GROUP_VARINT_INL_H
#define KUDU_UTIL_GROUP_VARINT_INL_H

#include <boost/utility/binary.hpp>
#include <glog/logging.h>
#include <stdint.h>
#include <smmintrin.h>

#include "util/faststring.h"

namespace kudu {
namespace coding {

extern bool SSE_TABLE_INITTED;
extern uint8_t SSE_TABLE[256 * 16] __attribute__ ((aligned(16)));
extern uint8_t VARINT_SELECTOR_LENGTHS[256];

const uint32_t MASKS[4] = { 0xff, 0xffff, 0xffffff, 0xffffffff };


// Calculate the number of bytes to encode the given unsigned int.
inline size_t CalcRequiredBytes32(uint32_t i) {
  if (i == 0) return 1;

  return sizeof(long) - __builtin_clzl(i)/8;
}

// Decode a set of 4 group-varint encoded integers from the given pointer.
//
// Returns a pointer following the last decoded integer.
inline const uint8_t *DecodeGroupVarInt32(
  const uint8_t *src,
  uint32_t *a, uint32_t *b, uint32_t *c, uint32_t *d) {

  uint8_t a_sel = (*src & BOOST_BINARY( 11 00 00 00)) >> 6;
  uint8_t b_sel = (*src & BOOST_BINARY( 00 11 00 00)) >> 4;
  uint8_t c_sel = (*src & BOOST_BINARY( 00 00 11 00)) >> 2;
  uint8_t d_sel = (*src & BOOST_BINARY( 00 00 00 11 ));

  src++; // skip past selector byte

  *a = *reinterpret_cast<const uint32_t *>(src) & MASKS[a_sel];
  src += a_sel + 1;

  *b = *reinterpret_cast<const uint32_t *>(src) & MASKS[b_sel];
  src += b_sel + 1;

  *c = *reinterpret_cast<const uint32_t *>(src) & MASKS[c_sel];
  src += c_sel + 1;

  *d = *reinterpret_cast<const uint32_t *>(src) & MASKS[d_sel];
  src += d_sel + 1;

  return src;
} 

inline void DoExtractM128(__m128i results,
                          uint32_t *a, uint32_t *b, uint32_t *c, uint32_t *d)
{
#define SSE_USE_EXTRACT_PS
#ifdef SSE_USE_EXTRACT_PS
  // _mm_extract_ps turns into extractps, which is slightly faster
  // than _mm_extract_epi32 (which turns into pextrd)
  // Apparently pextrd involves one more micro-op
  // than extractps.
  //
  // A uint32 cfile macro-benchmark is about 3% faster with this code path.
  *a = _mm_extract_ps((__v4sf)results, 0);
  *b = _mm_extract_ps((__v4sf)results, 1);
  *c = _mm_extract_ps((__v4sf)results, 2);
  *d = _mm_extract_ps((__v4sf)results, 3);
#else 
  *a = _mm_extract_epi32(results, 0);
  *b = _mm_extract_epi32(results, 1);
  *c = _mm_extract_epi32(results, 2);
  *d = _mm_extract_epi32(results, 3);
#endif
}

// Same as above, but uses SSE so may be faster.
// TODO: remove this and just automatically pick the right implementation at runtime.
inline const uint8_t *DecodeGroupVarInt32_SSE(
  const uint8_t *src,
  uint32_t *a, uint32_t *b, uint32_t *c, uint32_t *d) {

  DCHECK(SSE_TABLE_INITTED);

  uint8_t sel_byte = *src++;
  __m128i shuffle_mask = _mm_load_si128(
    reinterpret_cast<__m128i *>(&SSE_TABLE[sel_byte * 16]));
  __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

  __m128i results = _mm_shuffle_epi8(data, shuffle_mask);

  // It would look like the following would be most efficient,
  // since it turns into a single movdqa instruction:
  //   *reinterpret_cast<__m128i *>(ret) = results;
  // (where ret is an aligned array of ints, which the user must pass)
  // but it is actually slower than the below alternatives by a
  // good amount -- even though these result in more instructions.
  DoExtractM128(results, a, b, c, d);
  src += VARINT_SELECTOR_LENGTHS[sel_byte];

  return src;
}

// Optimized function which decodes a group of uint32s from 'src' into 'ret',
// which should have enough space for 4 uint32s. During decoding, adds 'add'
// to the vector in parallel.
inline const uint8_t *DecodeGroupVarInt32_SSE_Add(
  const uint8_t *src,
  uint32_t *ret,
  __m128i add) {

  DCHECK(SSE_TABLE_INITTED);

  uint8_t sel_byte = *src++;
  __m128i shuffle_mask = _mm_load_si128(
    reinterpret_cast<__m128i *>(&SSE_TABLE[sel_byte * 16]));
  __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i *>(src));

  __m128i decoded_deltas = _mm_shuffle_epi8(data, shuffle_mask);
  __m128i results = _mm_add_epi32(decoded_deltas, add);

  DoExtractM128(results, &ret[0], &ret[1], &ret[2], &ret[3]);

  src += VARINT_SELECTOR_LENGTHS[sel_byte];
  return src;
}

static void AppendShorterInt(faststring *s, uint32_t i, size_t bytes) {
  DCHECK_GE(bytes, 0);
  DCHECK_LE(bytes, 4);

#if __BYTE_ORDER == __LITTLE_ENDIAN
  // LSBs come first, so we can just reinterpret-cast
  // and set the right length
  s->append(reinterpret_cast<uint8_t *>(&i), bytes);
#else
#error dont support big endian currently
#endif
}


// Append a set of group-varint encoded integers to the given faststring.
inline void AppendGroupVarInt32(
  faststring *s,
  uint32_t a, uint32_t b, uint32_t c, uint32_t d) {

  uint8_t a_req = CalcRequiredBytes32(a);
  uint8_t b_req = CalcRequiredBytes32(b);
  uint8_t c_req = CalcRequiredBytes32(c);
  uint8_t d_req = CalcRequiredBytes32(d);

  uint8_t prefix_byte =
    ((a_req - 1) << 6) |
    ((b_req - 1) << 4) |
    ((c_req - 1) << 2) |
    (d_req - 1);

  s->push_back(prefix_byte);
  AppendShorterInt(s, a, a_req);
  AppendShorterInt(s, b, b_req);
  AppendShorterInt(s, c, c_req);
  AppendShorterInt(s, d, d_req);
}

// Append a sequence of uint32s encoded using group-varint.
//
// 'frame_of_reference' is also subtracted from each integer
// before encoding.
//
// If frame_of_reference is greater than any element in the array,
// results are undefined.
//
// For best performance, users should already have reserved adequate
// space in 's' (CalcRequiredBytes32 can be handy here)
inline void AppendGroupVarInt32Sequence(
  faststring *s, uint32_t frame_of_reference,
  uint32_t *ints, size_t size)
{
  uint32_t *p = ints;
  while (size >= 4) {
    AppendGroupVarInt32(s,
                        p[0] - frame_of_reference,
                        p[1] - frame_of_reference,
                        p[2] - frame_of_reference,
                        p[3] - frame_of_reference);
    size -= 4;
    p += 4;
  }


  uint32_t trailer[4] = {0, 0, 0, 0};
  uint32_t *trailer_p = &trailer[0];

  if (size > 0) {
    while (size > 0) {
      *trailer_p++ = *p++ - frame_of_reference;
      size--;
    }

    AppendGroupVarInt32(s, trailer[0], trailer[1], trailer[2], trailer[3]);
  }
}


} // namespace coding
} // namespace kudu

#endif
