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

// This file is conditionally compiled if compiler supports AVX2.
// However the tidy bot appears to compile this file regardless and does not define the USE_AVX2
// macro raising incorrect errors.
#if defined(CLANG_TIDY)
#define USE_AVX2 1
#endif

#include "kudu/util/block_bloom_filter.h"

#include <cstdint>
#include <immintrin.h>

#include "kudu/gutil/port.h"

namespace kudu {

// A static helper function for the AVX2 methods. Turns a 32-bit hash into a 256-bit Bucket
// with 1 single 1-bit set in each 32-bit lane.
static inline ATTRIBUTE_ALWAYS_INLINE __attribute__((__target__("avx2"))) __m256i MakeMask(
    const uint32_t hash) {
  const __m256i ones = _mm256_set1_epi32(1);
  const __m256i rehash = _mm256_setr_epi32(BLOOM_HASH_CONSTANTS);
  // Load hash into a YMM register, repeated eight times
  __m256i hash_data = _mm256_set1_epi32(hash);
  // Multiply-shift hashing ala Dietzfelbinger et al.: multiply 'hash' by eight different
  // odd constants, then keep the 5 most significant bits from each product.
  hash_data = _mm256_mullo_epi32(rehash, hash_data);
  hash_data = _mm256_srli_epi32(hash_data, 27);
  // Use these 5 bits to shift a single bit to a location in each 32-bit lane
  return _mm256_sllv_epi32(ones, hash_data);
}

void BlockBloomFilter::BucketInsertAVX2(const uint32_t bucket_idx, const uint32_t hash) noexcept {
  const __m256i mask = MakeMask(hash);
  __m256i* const bucket = &(reinterpret_cast<__m256i*>(directory_)[bucket_idx]);
  _mm256_store_si256(bucket, _mm256_or_si256(*bucket, mask));
  // For SSE compatibility, unset the high bits of each YMM register so SSE instructions
  // dont have to save them off before using XMM registers.
  _mm256_zeroupper();
}

bool BlockBloomFilter::BucketFindAVX2(const uint32_t bucket_idx, const uint32_t hash) const
    noexcept {
  const __m256i mask = MakeMask(hash);
  const __m256i bucket = reinterpret_cast<__m256i*>(directory_)[bucket_idx];
  // We should return true if 'bucket' has a one wherever 'mask' does. _mm256_testc_si256
  // takes the negation of its first argument and ands that with its second argument. In
  // our case, the result is zero everywhere iff there is a one in 'bucket' wherever
  // 'mask' is one. testc returns 1 if the result is 0 everywhere and returns 0 otherwise.
  const bool result = _mm256_testc_si256(bucket, mask);
  _mm256_zeroupper();
  return result;
}

void BlockBloomFilter::InsertAvx2(const uint32_t hash) noexcept {
  always_false_ = false;
  const uint32_t bucket_idx = Rehash32to32(hash) & directory_mask_;
  BucketInsertAVX2(bucket_idx, hash);
}

} // namespace kudu
