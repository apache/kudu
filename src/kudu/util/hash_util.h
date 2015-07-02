// Copyright (c) 2015 Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.

#include <stdint.h>

#ifndef KUDU_UTIL_HASH_UTIL_H
#define KUDU_UTIL_HASH_UTIL_H

namespace kudu {

/// Utility class to compute hash values.
class HashUtil {
 public:

  static const uint64_t MURMUR_PRIME = 0xc6a4a7935bd1e995;
  static const int MURMUR_R = 47;

  /// Murmur2 hash implementation returning 64-bit hashes.
  static uint64_t MurmurHash2_64(const void* input, int len, uint64_t seed) {
    uint64_t h = seed ^ (len * MURMUR_PRIME);

    const uint64_t* data = reinterpret_cast<const uint64_t*>(input);
    const uint64_t* end = data + (len / sizeof(uint64_t));

    while (data != end) {
      uint64_t k = *data++;
      k *= MURMUR_PRIME;
      k ^= k >> MURMUR_R;
      k *= MURMUR_PRIME;
      h ^= k;
      h *= MURMUR_PRIME;
    }

    const uint8_t* data2 = reinterpret_cast<const uint8_t*>(data);
    switch (len & 7) {
      case 7: h ^= static_cast<uint64_t>(data2[6]) << 48;
      case 6: h ^= static_cast<uint64_t>(data2[5]) << 40;
      case 5: h ^= static_cast<uint64_t>(data2[4]) << 32;
      case 4: h ^= static_cast<uint64_t>(data2[3]) << 24;
      case 3: h ^= static_cast<uint64_t>(data2[2]) << 16;
      case 2: h ^= static_cast<uint64_t>(data2[1]) << 8;
      case 1: h ^= static_cast<uint64_t>(data2[0]);
              h *= MURMUR_PRIME;
    }

    h ^= h >> MURMUR_R;
    h *= MURMUR_PRIME;
    h ^= h >> MURMUR_R;
    return h;
  }
};

} // namespace kudu
#endif
