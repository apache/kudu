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

#include "kudu/util/char_util.h"

#ifdef __aarch64__
#include <arm_neon.h>
#else
#include <emmintrin.h>
#include <smmintrin.h>
#endif //__aarch64__

#include <algorithm>
#include <cstring>

namespace kudu {

#ifdef __aarch64__
static inline bool AllASCII16B(const uint8_t* str, const uint8x16_t mask) {
  uint8x16_t a = vld1q_u8(str);
  uint64x2_t res = vreinterpretq_u64_u8(vtstq_u8(a, mask));
  return 0 == (vgetq_lane_u64(res, 0) | vgetq_lane_u64(res, 1));
}
#elif defined(__x86_64__)
static inline bool AllASCII16B(const uint8_t* str, const __m128i mask) {
  return _mm_test_all_zeros(_mm_loadu_si128(reinterpret_cast<const __m128i*>(str)), mask) == 1;
}
#endif

Slice UTF8Truncate(Slice val, size_t max_utf8_length) {
  size_t num_utf8_chars = 0;
  const uint8_t* str;
  const uint8_t* start;
  str = start = val.data();
  size_t num_bytes = 0;
  size_t size = val.size();

  // Mask used to determine whether there are any non-ASCII characters in a
  // 128-bit chunk
#ifdef __aarch64__
  const uint8x16_t mask = vdupq_n_u8(0x80);
#elif defined(__x86_64__)
  const __m128i mask = _mm_set1_epi32(0x80808080);
#endif

  while (num_bytes < size) {
    // If the next chunk of bytes are all ASCII we can fast path them.
    if (size - num_bytes >= 16 &&
        max_utf8_length - num_utf8_chars >= 16 &&
        AllASCII16B(str, mask)) {
      num_utf8_chars += 16;
      num_bytes += 16;
      str += 16;
    } else if (size - num_bytes >= 8 &&
               max_utf8_length - num_utf8_chars >= 8 &&
               (*(reinterpret_cast<const int64_t*>(str)) & 0x8080808080808080) == 0) {
      num_utf8_chars += 8;
      num_bytes += 8;
      str += 8;
    } else {
      num_utf8_chars += (*str++ & 0xc0) != 0x80;
      num_bytes++;
      if (num_utf8_chars > max_utf8_length) {
        num_bytes--;
        num_utf8_chars--;
        break;
      }
    }
  }
  num_bytes = std::min<size_t>(size, num_bytes);
  auto relocated = new uint8_t[num_bytes];
  memcpy(relocated, val.data(), num_bytes);
  return Slice(relocated, num_bytes);
}

} // namespace kudu
