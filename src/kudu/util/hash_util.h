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

#ifndef KUDU_UTIL_HASH_UTIL_H
#define KUDU_UTIL_HASH_UTIL_H

#include <cstdint>

// Including glog/logging.h causes problems while compiling in Apache Impala for codegen.
// IWYU pragma: no_include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/util/hash.pb.h"
#include "kudu/util/slice.h"

namespace kudu {

/// Utility class to compute hash values.
class HashUtil {
 public:

  /// Murmur2 hash implementation returning 64-bit hashes.
  ATTRIBUTE_NO_SANITIZE_INTEGER
  static uint64_t MurmurHash2_64(const void* input, int len, uint64_t seed) {
    static constexpr uint64_t MURMUR_PRIME = 0xc6a4a7935bd1e995UL;
    static constexpr int MURMUR_R = 47;

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


  // FastHash is simple, robust, and efficient general-purpose hash function from Google.
  // Implementation is adapted from https://code.google.com/archive/p/fast-hash/
  //
  // Compute 64-bit FastHash.
  ATTRIBUTE_NO_SANITIZE_INTEGER
  static uint64_t FastHash64(const void* buf, size_t len, uint64_t seed) {
    static constexpr uint64_t kMultiplier = 0x880355f21e6d1965UL;
    const uint64_t* pos = static_cast<const uint64_t*>(buf);
    const uint64_t* end = pos + (len / 8);
    uint64_t h = seed ^ (len * kMultiplier);
    uint64_t v;

    while (pos != end) {
      v  = *pos++;
      h ^= FastHashMix(v);
      h *= kMultiplier;
    }

    const uint8_t* pos2 = reinterpret_cast<const uint8_t*>(pos);
    v = 0;

    switch (len & 7) {
      case 7: v ^= static_cast<uint64_t>(pos2[6]) << 48;
      case 6: v ^= static_cast<uint64_t>(pos2[5]) << 40;
      case 5: v ^= static_cast<uint64_t>(pos2[4]) << 32;
      case 4: v ^= static_cast<uint64_t>(pos2[3]) << 24;
      case 3: v ^= static_cast<uint64_t>(pos2[2]) << 16;
      case 2: v ^= static_cast<uint64_t>(pos2[1]) << 8;
      case 1: v ^= static_cast<uint64_t>(pos2[0]);
        h ^= FastHashMix(v);
        h *= kMultiplier;
    }

    return FastHashMix(h);
  }

  // Compute 32-bit FastHash.
  static uint32_t FastHash32(const void* buf, size_t len, uint32_t seed) {
    // the following trick converts the 64-bit hashcode to Fermat
    // residue, which shall retain information from both the higher
    // and lower parts of hashcode.
    uint64_t h = FastHash64(buf, len, seed);
    return h - (h >> 32);
  }

  // Checks whether 32-bit version of the hash algorithm is available.
  // Must be kept in sync with ComputeHash32() function.
  static bool IsComputeHash32Available(HashAlgorithm hash_algorithm) {
    switch (hash_algorithm) {
      case FAST_HASH:
        return true;
      default:
        return false;
    }
  }

  // Compute 32-bit hash of the supplied data using the specified hash algorithm.
  // Must be kept in sync with IsComputeHash32Available() function.
  static uint32_t ComputeHash32(const Slice& data, HashAlgorithm hash_algorithm, uint32_t seed) {
    // TODO(bankim): Consider adding special handling for zero-length/NULL objects.
    switch (hash_algorithm) {
      case FAST_HASH:
        return FastHash32(data.data(), data.size(), seed);
      default:
        // Can't use LOG(FATAL)/CHECK() since including glog/logging.h causes problems
        // with code-gen in Impala.
        abort();
    }
  }

 private:
  // Compression function for Merkle-Damgard construction.
  ATTRIBUTE_NO_SANITIZE_INTEGER
  static uint64_t FastHashMix(uint64_t h) {
    h ^= h >> 23;
    h *= 0x2127599bf4325c37UL;
    h ^= h >> 47;
    return h;
  }
};

} // namespace kudu
#endif
