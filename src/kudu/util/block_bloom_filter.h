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

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/gutil/cpu.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/status.h"

DECLARE_bool(disable_blockbloomfilter_avx2);

namespace kudu {

// Forward declaration.
class BlockBloomFilterBufferAllocatorIf;

// Space and cache efficient block based BloomFilter that takes 32-bit hash as key.
// For a simple BloomFilter that takes arbitrary datatype as key see BloomFilter in bloom_filter.h
//
// A BloomFilter stores sets of items and offers a query operation indicating whether or
// not that item is in the set.  BloomFilters use much less space than other compact data
// structures, but they are less accurate: for a small percentage of elements, the query
// operation incorrectly returns true even when the item is not in the set.
//
// When talking about Bloom filter size, rather than talking about 'size', which might be
// ambiguous, we distinguish two different quantities:
//
// 1. Space: the amount of memory used
//
// 2. NDV: the number of unique items that have been inserted
//
// BlockBloomFilter is implemented using block Bloom filters from Putze et al.'s "Cache-,
// Hash- and Space-Efficient Bloom Filters". The basic idea is to hash the item to a tiny
// Bloom filter the size of a single cache line or smaller. This implementation sets 8
// bits in each tiny Bloom filter. This provides a false positive rate near optimal for
// between 5 and 15 bits per distinct value, which corresponds to false positive
// probabilities between 0.1% (for 15 bits) and 10% (for 5 bits).
//
// Our tiny BloomFilters are 32 bytes to take advantage of 32-byte SIMD in newer Intel
// machines.
class BlockBloomFilter {
 public:
  explicit BlockBloomFilter(BlockBloomFilterBufferAllocatorIf* buffer_allocator);
  ~BlockBloomFilter();

  // Reset the filter state, allocate/reallocate the internal data structures.
  // All calls to Insert() and Find() should only be done between the calls to Init() and
  // Close().Init and Close are safe to call multiple times.
  Status Init(int log_space_bytes);
  void Close();

  // Representation of a filter which allows all elements to pass.
  static constexpr BlockBloomFilter* const kAlwaysTrueFilter = nullptr;

  bool AlwaysFalse() const { return always_false_; }

  // Adds an element to the BloomFilter. The function used to generate 'hash' need not
  // have good uniformity, but it should have low collision probability. For instance, if
  // the set of values is 32-bit ints, the identity function is a valid hash function for
  // this Bloom filter, since the collision probability (the probability that two
  // non-equal values will have the same hash value) is 0.
  void Insert(uint32_t hash) noexcept;

  // Finds an element in the BloomFilter, returning true if it is found and false (with
  // high probability) if it is not.
  bool Find(uint32_t hash) const noexcept;

  // As more distinct items are inserted into a BloomFilter, the false positive rate
  // rises. MaxNdv() returns the NDV (number of distinct values) at which a BloomFilter
  // constructed with (1 << log_space_bytes) bytes of space hits false positive
  // probability fpp.
  static size_t MaxNdv(int log_space_bytes, double fpp);

  // If we expect to fill a Bloom filter with 'ndv' different unique elements and we
  // want a false positive probability of less than 'fpp', then this is the log (base 2)
  // of the minimum number of bytes we need.
  static int MinLogSpace(size_t ndv, double fpp);

  // Returns the expected false positive rate for the given ndv and log_space_bytes.
  static double FalsePositiveProb(size_t ndv, int log_space_bytes);

  // Returns amount of space used, in bytes.
  int64_t GetSpaceUsed() const { return sizeof(Bucket) * (1LL << log_num_buckets_); }

  static int64_t GetExpectedMemoryUsed(uint32_t log_heap_size) {
    DCHECK_GE(log_heap_size, kLogBucketWordBits);
    return sizeof(Bucket) * (1LL << std::max<int>(1, log_heap_size - kLogBucketWordBits));
  }

 private:
  // always_false_ is true when the bloom filter hasn't had any elements inserted.
  bool always_false_;

  static const base::CPU kCpu;

  // The BloomFilter is divided up into Buckets and each Bucket comprises of 8 BucketWords of
  // 4 bytes each.
  static constexpr uint64_t kBucketWords = 8;
  typedef uint32_t BucketWord;

  // log2(number of bits in a BucketWord)
  static constexpr int kLogBucketWordBits = 5;
  static constexpr BucketWord kBucketWordMask = (1 << kLogBucketWordBits) - 1;

  // log2(number of bytes in a bucket)
  static constexpr int kLogBucketByteSize = 5;

  static_assert((1 << kLogBucketWordBits) == std::numeric_limits<BucketWord>::digits,
      "BucketWord must have a bit-width that is be a power of 2, like 64 for uint64_t.");

  typedef BucketWord Bucket[kBucketWords];

  BlockBloomFilterBufferAllocatorIf* buffer_allocator_;

  // log_num_buckets_ is the log (base 2) of the number of buckets in the directory.
  int log_num_buckets_;

  // directory_mask_ is (1 << log_num_buckets_) - 1. It is precomputed for
  // efficiency reasons.
  uint32_t directory_mask_;

  Bucket* directory_;

  // Same as Insert(), but skips the CPU check and assumes that AVX is not available.
  void InsertNoAvx2(uint32_t hash) noexcept;

  // Does the actual work of Insert(). bucket_idx is the index of the bucket to insert
  // into and 'hash' is the value passed to Insert().
  void BucketInsert(uint32_t bucket_idx, uint32_t hash) noexcept;

  bool BucketFind(uint32_t bucket_idx, uint32_t hash) const noexcept;

#ifdef USE_AVX2
  // Same as Insert(), but skips the CPU check and assumes that AVX is available.
  void InsertAvx2(uint32_t hash) noexcept __attribute__((__target__("avx2")));

  // A faster SIMD version of BucketInsert().
  void BucketInsertAVX2(uint32_t bucket_idx, uint32_t hash) noexcept
      __attribute__((__target__("avx2")));

  // A faster SIMD version of BucketFind().
  bool BucketFindAVX2(uint32_t bucket_idx, uint32_t hash) const noexcept
      __attribute__((__target__("avx2")));
#endif

  // Function pointers initialized in constructor to avoid run-time cost
  // in hot-path of Find and Insert operations.
  decltype(&BlockBloomFilter::BucketInsert) bucket_insert_func_ptr_;
  decltype(&BlockBloomFilter::BucketFind) bucket_find_func_ptr_;

  int64_t directory_size() const {
    return 1ULL << (log_num_buckets_ + kLogBucketByteSize);
  }

  // Detect at run-time whether CPU supports AVX2
  static bool has_avx2() {
    return !FLAGS_disable_blockbloomfilter_avx2 && kCpu.has_avx2();
  }

  // Some constants used in hashing. #defined for efficiency reasons.
#define BLOOM_HASH_CONSTANTS                                             \
  0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, \
      0x9efc4947U, 0x5c6bfb31U

  // kRehash is used as 8 odd 32-bit unsigned ints.  See Dietzfelbinger et al.'s "A
  // reliable randomized algorithm for the closest-pair problem".
  static constexpr uint32_t kRehash[8]
      __attribute__((aligned(32))) = {BLOOM_HASH_CONSTANTS};

  // Get 32 more bits of randomness from a 32-bit hash:
  ATTRIBUTE_NO_SANITIZE_INTEGER
  static inline uint32_t Rehash32to32(const uint32_t hash) {
    // Constants generated by uuidgen(1) with the -r flag
    static constexpr uint64_t m = 0x7850f11ec6d14889ULL;
    static constexpr uint64_t a = 0x6773610597ca4c63ULL;
    // This is strongly universal hashing following Dietzfelbinger's "Universal hashing
    // and k-wise independent random variables via integer arithmetic without primes". As
    // such, for any two distinct uint32_t's hash1 and hash2, the probability (over the
    // randomness of the constants) that any subset of bit positions of
    // Rehash32to32(hash1) is equal to the same subset of bit positions
    // Rehash32to32(hash2) is minimal.
    return (static_cast<uint64_t>(hash) * m + a) >> 32U;
  }

  DISALLOW_COPY_AND_ASSIGN(BlockBloomFilter);
};

// Generic interface to allocate and de-allocate memory for the BlockBloomFilter.
class BlockBloomFilterBufferAllocatorIf {
 public:
  virtual Status AllocateBuffer(size_t bytes, void** ptr) = 0;
  virtual void FreeBuffer(void* ptr) = 0;
};

class DefaultBlockBloomFilterBufferAllocator : public BlockBloomFilterBufferAllocatorIf {
 public:
  // Required for Singleton.
  DefaultBlockBloomFilterBufferAllocator() = default;

  Status AllocateBuffer(size_t bytes, void** ptr) override;
  void FreeBuffer(void* ptr) override;

  static DefaultBlockBloomFilterBufferAllocator* GetSingleton();
 private:
  DISALLOW_COPY_AND_ASSIGN(DefaultBlockBloomFilterBufferAllocator);
};

}  // namespace kudu
