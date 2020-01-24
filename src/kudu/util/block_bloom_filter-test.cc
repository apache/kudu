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

#include "kudu/util/block_bloom_filter.h"

#include <cmath> // IWYU pragma: keep
#include <cstdint>
#include <cstdlib>
#include <iosfwd>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/hash.pb.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DECLARE_bool(disable_blockbloomfilter_avx2);

using namespace std; // NOLINT(*)

namespace kudu {

class BlockBloomFilterTest : public KuduTest {
 public:
  void SetUp() override {
    SeedRandom();
    allocator_ = DefaultBlockBloomFilterBufferAllocator::GetSingleton();
  }
  // Make a random uint32_t, avoiding the absent high bit and the low-entropy low bits
  // produced by rand().
  static uint32_t MakeRand() {
    uint32_t result = (rand() >> 8) & 0xffff;
    result <<= 16;
    result |= (rand() >> 8) & 0xffff;
    return result;
  }

  BlockBloomFilter* CreateBloomFilter(size_t log_space_bytes) {
    FLAGS_disable_blockbloomfilter_avx2 = (MakeRand() & 0x1) == 0;

    unique_ptr<BlockBloomFilter> bf(new BlockBloomFilter(allocator_));
    CHECK_OK(bf->Init(log_space_bytes, FAST_HASH, 0));
    bloom_filters_.emplace_back(move(bf));
    return bloom_filters_.back().get();
  }

  void TearDown() override {
    for (const auto& bf : bloom_filters_) {
      bf->Close();
    }
  }

 protected:
  DefaultBlockBloomFilterBufferAllocator* allocator_;

 private:
  vector<unique_ptr<BlockBloomFilter>> bloom_filters_;
};

// We can construct (and destruct) Bloom filters with different spaces.
TEST_F(BlockBloomFilterTest, Constructor) {
  for (int i = 0; i < 30; ++i) {
    CreateBloomFilter(i);
  }
}

TEST_F(BlockBloomFilterTest, Clone) {
  Arena arena(1024);
  ArenaBlockBloomFilterBufferAllocator arena_allocator(&arena);
  std::shared_ptr<BlockBloomFilterBufferAllocatorIf> allocator_clone = arena_allocator.Clone();

  for (int log_space_bytes = 1; log_space_bytes <= 20; ++log_space_bytes) {
    auto* bf = CreateBloomFilter(log_space_bytes);
    int max_elems = BlockBloomFilter::MaxNdv(log_space_bytes, 0.01 /* fpp */);
    while (max_elems-- > 0) {
      bf->Insert(MakeRand());
    }
    unique_ptr<BlockBloomFilter> bf_clone;
    ASSERT_OK(bf->Clone(allocator_clone.get(), &bf_clone));
    ASSERT_NE(nullptr, bf_clone);
    ASSERT_EQ(*bf_clone, *bf);
  }
}

TEST_F(BlockBloomFilterTest, InvalidSpace) {
  BlockBloomFilter bf(allocator_);
  // Random number in the range [38, 64).
  const int log_space_bytes = 38 + rand() % (64 - 38);
  Status s = bf.Init(log_space_bytes, FAST_HASH, 0);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "Bloom filter too large");
  bf.Close();
}

TEST_F(BlockBloomFilterTest, InvalidHashAlgorithm) {
  BlockBloomFilter bf(allocator_);
  Status s = bf.Init(4, UNKNOWN_HASH, 0);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid/Unsupported hash algorithm");
}

// We can Insert() hashes into a Bloom filter with different spaces.
TEST_F(BlockBloomFilterTest, Insert) {
  for (int i = 13; i < 17; ++i) {
    auto* bf = CreateBloomFilter(i);
    for (int k = 0; k < (1 << 15); ++k) {
      bf->Insert(MakeRand());
    }
  }
}

// After Insert()ing something into a Bloom filter, it can be found again immediately.
TEST_F(BlockBloomFilterTest, Find) {
  for (int i = 13; i < 17; ++i) {
    auto* bf = CreateBloomFilter(i);
    for (int k = 0; k < (1 << 15); ++k) {
      const auto to_insert = MakeRand();
      bf->Insert(to_insert);
      EXPECT_TRUE(bf->Find(to_insert));
    }
  }
}

// After Insert()ing something into a Bloom filter, it can be found again much later.
TEST_F(BlockBloomFilterTest, CumulativeFind) {
  for (int i = 5; i < 11; ++i) {
    vector<uint32_t> inserted;
    auto* bf = CreateBloomFilter(i);
    for (int k = 0; k < (1 << 10); ++k) {
      const uint32_t to_insert = MakeRand();
      inserted.push_back(to_insert);
      bf->Insert(to_insert);
      for (int n = 0; n < inserted.size(); ++n) {
        EXPECT_TRUE(bf->Find(inserted[n]));
      }
    }
  }
}

// The empirical false positives we find when looking for random items is with a constant
// factor of the false positive probability the Bloom filter was constructed for.
TEST_F(BlockBloomFilterTest, FindInvalid) {
  static const int find_limit = 1 << 20;
  unordered_set<uint32_t> to_find;
  while (to_find.size() < find_limit) {
    to_find.insert(MakeRand());
  }
  static const int max_log_ndv = 19;
  unordered_set<uint32_t> to_insert;
  while (to_insert.size() < (1ULL << max_log_ndv)) {
    const auto candidate = MakeRand();
    if (to_find.find(candidate) == to_find.end()) {
      to_insert.insert(candidate);
    }
  }
  vector<uint32_t> shuffled_insert(to_insert.begin(), to_insert.end());
  for (int log_ndv = 12; log_ndv < max_log_ndv; ++log_ndv) {
    for (int log_fpp = 4; log_fpp < 15; ++log_fpp) {
      double fpp = 1.0 / (1 << log_fpp);
      const size_t ndv = 1 << log_ndv;
      const int log_heap_space = BlockBloomFilter::MinLogSpace(ndv, fpp);
      auto* bf = CreateBloomFilter(log_heap_space);
      // Fill up a BF with exactly as much ndv as we planned for it:
      for (size_t i = 0; i < ndv; ++i) {
        bf->Insert(shuffled_insert[i]);
      }
      int found = 0;
      // Now we sample from the set of possible hashes, looking for hits.
      for (const auto& i : to_find) {
        found += bf->Find(i);
      }
      EXPECT_LE(found, find_limit * fpp * 2)
          << "Too many false positives with -log2(fpp) = " << log_fpp;
      // Because the space is rounded up to a power of 2, we might actually get a lower
      // fpp than the one passed to MinLogSpace().
      const double expected_fpp = BlockBloomFilter::FalsePositiveProb(ndv, log_heap_space);
      EXPECT_GE(found, find_limit * expected_fpp)
          << "Too few false positives with -log2(fpp) = " << log_fpp;
      EXPECT_LE(found, find_limit * expected_fpp * 16)
          << "Too many false positives with -log2(fpp) = " << log_fpp;
    }
  }
}

// Test that MaxNdv() and MinLogSpace() are dual
TEST_F(BlockBloomFilterTest, MinSpaceMaxNdv) {
  for (int log2fpp = -2; log2fpp >= -63; --log2fpp) {
    const double fpp = pow(2, log2fpp);
    for (int given_log_space = 8; given_log_space < 30; ++given_log_space) {
      const size_t derived_ndv = BlockBloomFilter::MaxNdv(given_log_space, fpp);
      int derived_log_space = BlockBloomFilter::MinLogSpace(derived_ndv, fpp);

      EXPECT_EQ(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;

      // If we lower the fpp, we need more space; if we raise it we need less.
      derived_log_space = BlockBloomFilter::MinLogSpace(derived_ndv, fpp / 2);
      EXPECT_GE(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;
      derived_log_space = BlockBloomFilter::MinLogSpace(derived_ndv, fpp * 2);
      EXPECT_LE(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;
    }
    for (size_t given_ndv = 1000; given_ndv < 1000 * 1000; given_ndv *= 3) {
      const int derived_log_space = BlockBloomFilter::MinLogSpace(given_ndv, fpp);
      const size_t derived_ndv = BlockBloomFilter::MaxNdv(derived_log_space, fpp);

      // The max ndv is close to, but larger than, then ndv we asked for
      EXPECT_LE(given_ndv, derived_ndv) << "fpp: " << fpp
                                        << " derived_log_space: " << derived_log_space;
      EXPECT_GE(given_ndv * 2, derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;

      // Changing the fpp changes the ndv capacity in the expected direction.
      size_t new_derived_ndv = BlockBloomFilter::MaxNdv(derived_log_space, fpp / 2);
      EXPECT_GE(derived_ndv, new_derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;
      new_derived_ndv = BlockBloomFilter::MaxNdv(derived_log_space, fpp * 2);
      EXPECT_LE(derived_ndv, new_derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;
    }
  }
}

TEST_F(BlockBloomFilterTest, MinSpaceEdgeCase) {
  int min_space = BlockBloomFilter::MinLogSpace(1, 0.75);
  EXPECT_GE(min_space, 0) << "LogSpace should always be >= 0";
}

// Check that MinLogSpace() and FalsePositiveProb() are dual
TEST_F(BlockBloomFilterTest, MinSpaceForFpp) {
  for (size_t ndv = 10000; ndv < 100 * 1000 * 1000; ndv *= 1.01) {
    for (double fpp = 0.1; fpp > pow(2, -20); fpp *= 0.99) { // NOLINT: loop on double
      // When contructing a Bloom filter, we can request a particular fpp by calling
      // MinLogSpace().
      const int min_log_space = BlockBloomFilter::MinLogSpace(ndv, fpp);
      // However, at the resulting ndv and space, the expected fpp might be lower than
      // the one that was requested.
      double expected_fpp = BlockBloomFilter::FalsePositiveProb(ndv, min_log_space);
      EXPECT_LE(expected_fpp, fpp);
      // The fpp we get might be much lower than the one we asked for. However, if the
      // space were just one size smaller, the fpp we get would be larger than the one we
      // asked for.
      expected_fpp = BlockBloomFilter::FalsePositiveProb(ndv, min_log_space - 1);
      EXPECT_GE(expected_fpp, fpp);
      // Therefore, the return value of MinLogSpace() is actually the minimum
      // log space at which we can guarantee the requested fpp.
    }
  }
}
}  // namespace kudu
