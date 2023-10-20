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

#include "kudu/util/slru_cache.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/util/cache.h"
#include "kudu/util/coding.h"
#include "kudu/util/faststring.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_util.h"

DECLARE_bool(cache_force_single_shard);

namespace kudu {

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeInt(int k) {
  faststring result;
  PutFixed32(&result, k);
  return result.ToString();
}
static int DecodeInt(const Slice& k) {
  CHECK_EQ(4, k.size());
  return DecodeFixed32(k.data());
}

// Cache sharding policy affects the composition of the cache. Some test
// scenarios assume cache is single-sharded to keep the logic simpler.
enum class ShardingPolicy {
  MultiShard,
  SingleShard,
};

constexpr size_t kSmallerSegmentSize = 4 * 1024 * 1024;
constexpr size_t kLargerSegmentSize = 12 * 1024 * 1024;
constexpr uint32_t kLookups = 5;

class SLRUCacheBaseTest : public KuduTest,
                          public Cache::EvictionCallback {
 public:
  SLRUCacheBaseTest(size_t probationary_segment_size,
                    size_t protected_segment_size,
                    uint32_t lookups)
      : probationary_segment_size_(probationary_segment_size),
        protected_segment_size_(protected_segment_size),
        total_cache_size_(probationary_segment_size + protected_segment_size),
        lookups_threshold_(lookups)
        { }

  // Implementation of the EvictionCallback interface.
  void EvictedEntry(Slice key, Slice val) override {
    evicted_keys_.push_back(DecodeInt(key));
    evicted_values_.push_back(DecodeInt(val));
  }

  // Returns -1 if no key is found in cache.
  // When inserting entries, don't use -1 as the value.
  int Lookup(int key) {
    auto handle(slru_cache_->Lookup(EncodeInt(key), Cache::EXPECT_IN_CACHE));
    return handle ? DecodeInt(slru_cache_->Value(handle)) : -1;
  }

  void Insert(int key, int value, int charge = 1) {
    std::string key_str = EncodeInt(key);
    std::string val_str = EncodeInt(value);
    auto handle(slru_cache_->Allocate(key_str, val_str.size(), charge));
    CHECK(handle);
    memcpy(slru_cache_->MutableValue(&handle), val_str.data(), val_str.size());
    slru_cache_->Insert(std::move(handle), this);
  }

  void Erase(int key) {
    slru_cache_->Erase(EncodeInt(key));
  }

  // Returns true if probationary segment contains key.
  // Returns false if not.
  bool ProbationaryContains(const Slice& key) {
    const uint32_t hash = slru_cache_->HashSlice(key);
    return slru_cache_->shards_[slru_cache_->Shard(hash)]->ProbationaryContains(key, hash);
  }

  // Returns true if protected segment contains key.
  // Returns false if not.
  bool ProtectedContains(const Slice& key) {
    const uint32_t hash = slru_cache_->HashSlice(key);
    return slru_cache_->shards_[slru_cache_->Shard(hash)]->ProtectedContains(key, hash);
  }

  protected:
   void SetupWithParameters(Cache::MemoryType mem_type,
                            ShardingPolicy sharding_policy) {
     // Using single shard makes the logic of scenarios simple
     // for capacity and eviction-related behavior.
     FLAGS_cache_force_single_shard = (sharding_policy == ShardingPolicy::SingleShard);

     if (mem_type == Cache::MemoryType::DRAM) {
       slru_cache_.reset(NewSLRUCache<Cache::MemoryType::DRAM>(probationary_segment_size_,
                                                               protected_segment_size_,
                                                               "slru_cache_test",
                                                               lookups_threshold_));
     } else {
       FAIL() << mem_type << ": unrecognized slru cache memory type";
     }
     MemTracker::FindTracker("slru_cache_test-sharded_slru_cache", &mem_tracker_);

     ASSERT_TRUE(mem_tracker_.get());
  }

   const size_t probationary_segment_size_;
   const size_t protected_segment_size_;
   const size_t total_cache_size_;
   const uint32_t lookups_threshold_;
   std::vector<int> evicted_keys_;
   std::vector<int> evicted_values_;
   std::shared_ptr<MemTracker> mem_tracker_;
   std::unique_ptr<ShardedSLRUCache> slru_cache_;
};

class SLRUCacheTest :
    public SLRUCacheBaseTest,
    public ::testing::WithParamInterface<std::pair<Cache::MemoryType, ShardingPolicy>> {

 public:
  SLRUCacheTest()
      : SLRUCacheBaseTest(kSmallerSegmentSize, kLargerSegmentSize, kLookups) {
}

  void SetUp() override {
    const auto& param = GetParam();
    SetupWithParameters(param.first, param.second);
  }
};

INSTANTIATE_TEST_SUITE_P(
    CacheTypes, SLRUCacheTest,
    ::testing::Values(
        std::make_pair(Cache::MemoryType::DRAM,
                       ShardingPolicy::MultiShard),
        std::make_pair(Cache::MemoryType::DRAM,
                       ShardingPolicy::SingleShard)));

class SLRUSingleShardCacheTest :
    public SLRUCacheBaseTest,
    public ::testing::WithParamInterface<Cache::MemoryType> {

 public:
  SLRUSingleShardCacheTest()
      : SLRUCacheBaseTest(kSmallerSegmentSize, kLargerSegmentSize, kLookups) {
  }

  void SetUp() override {
    const auto& param = GetParam();
    SetupWithParameters(param, ShardingPolicy::SingleShard);
  }
};

INSTANTIATE_TEST_SUITE_P(
    CacheTypes, SLRUSingleShardCacheTest,
    ::testing::Values(Cache::MemoryType::DRAM));

class SLRUSingleShardTest :
    public SLRUCacheBaseTest,
    public ::testing::WithParamInterface<Cache::MemoryType> {

 public:
  SLRUSingleShardTest()
      : SLRUCacheBaseTest(kLargerSegmentSize, kSmallerSegmentSize, kLookups) {
  }

  void SetUp() override {
    const auto& param = GetParam();
    SetupWithParameters(param, ShardingPolicy::SingleShard);
  }
};

INSTANTIATE_TEST_SUITE_P(
    CacheTypes, SLRUSingleShardTest,
    ::testing::Values(Cache::MemoryType::DRAM));

// Tests that an entry is upgraded from probationary to protected segment
// after being looked up more than lookups_threshold_ times.
TEST_P(SLRUCacheTest, Upgrade) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1,  Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));
  ASSERT_TRUE(ProbationaryContains(EncodeInt(100)));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));
  ASSERT_TRUE(ProbationaryContains(EncodeInt(200)));

  Insert(100, 102);
  ASSERT_EQ(102, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1,  Lookup(300));
  ASSERT_TRUE(ProbationaryContains(EncodeInt(100)));

  // Upgrade entry to protected segment.
  for (auto i = 0; i < lookups_threshold_ - 1; ++i) {
    ASSERT_EQ(102, Lookup(100));
  }
  ASSERT_FALSE(ProbationaryContains(EncodeInt(100)));
  ASSERT_TRUE(ProtectedContains(EncodeInt(100)));

  // Lookup entry multiple times in protected segment.
  for (auto i = 0; i < 3 * lookups_threshold_; ++i) {
    ASSERT_EQ(102, Lookup(100));
  }
  ASSERT_FALSE(ProbationaryContains(EncodeInt(100)));
  ASSERT_TRUE(ProtectedContains(EncodeInt(100)));

  // Check that entry in protected segment works with upsert case.
  Insert(100, 103);
  ASSERT_EQ(103, Lookup(100));
  ASSERT_FALSE(ProbationaryContains(EncodeInt(100)));
  ASSERT_TRUE(ProtectedContains(EncodeInt(100)));
}

// Tests that entries are properly erased from both the probationary and protected segments.
TEST_P(SLRUCacheTest, Erase) {
  Erase(200);
  ASSERT_EQ(0, evicted_keys_.size());

  Insert(100, 101);
  Insert(200, 201);

  // Erase first entry, verify it's removed from cache by checking eviction callback.
  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(100, evicted_keys_[0]);
  ASSERT_EQ(101, evicted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1,  Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1, evicted_keys_.size());

  // Upgrade second entry to protected segment.
  for (auto i = 0; i < lookups_threshold_ - 1; ++i) {
    ASSERT_EQ(201, Lookup(200));
  }
  ASSERT_FALSE(ProbationaryContains(EncodeInt(200)));
  ASSERT_TRUE(ProtectedContains(EncodeInt(200)));

  // Erase second entry from protected segment.
  Erase(200);
  ASSERT_EQ(2, evicted_keys_.size());
  ASSERT_EQ(200, evicted_keys_[1]);
  ASSERT_EQ(201, evicted_values_[1]);
}

// Underlying entry isn't actually deleted until handle around it from lookup is reset.
TEST_P(SLRUCacheTest, EntriesArePinned) {
  Insert(100, 101);
  auto h1 = slru_cache_->Lookup(EncodeInt(100), Cache::EXPECT_IN_CACHE);
  ASSERT_EQ(101, DecodeInt(slru_cache_->Value(h1)));

  Insert(100, 102);
  auto h2 = slru_cache_->Lookup(EncodeInt(100), Cache::EXPECT_IN_CACHE);
  ASSERT_EQ(102, DecodeInt(slru_cache_->Value(h2)));
  ASSERT_EQ(0, evicted_keys_.size());

  h1.reset();
  ASSERT_EQ(1, evicted_keys_.size());
  ASSERT_EQ(100, evicted_keys_[0]);
  ASSERT_EQ(101, evicted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1, evicted_keys_.size());

  h2.reset();
  ASSERT_EQ(2, evicted_keys_.size());
  ASSERT_EQ(100, evicted_keys_[1]);
  ASSERT_EQ(102, evicted_values_[1]);
}

// Tests that a frequently accessed entry is not evicted.
TEST_P(SLRUCacheTest, LRUEviction) {
  static constexpr int kNumElems = 1000;
  const int size_per_elem = static_cast<int>(total_cache_size_ / kNumElems);

  Insert(100, 101);
  Insert(200, 201);

  // Loop adding and looking up new entries, but repeatedly accessing key 101.
  // This frequently-used entry should not be evicted.
  for (int i = 0; i < kNumElems + 1000; i++) {
    Insert(1000+i, 2000+i, size_per_elem);
    ASSERT_EQ(2000+i, Lookup(1000+i));
    ASSERT_EQ(101, Lookup(100));
  }
  ASSERT_FALSE(ProbationaryContains(EncodeInt(100)));
  ASSERT_TRUE(ProtectedContains(EncodeInt(100)));
  ASSERT_EQ(101, Lookup(100));
  // Since '200' wasn't accessed in the loop above, it should have been evicted.
  ASSERT_EQ(-1, Lookup(200));
}

// Tests that entries are evicted from protected segment when it's at capacity.
// Also tests the upsert case for a full protected segment.
TEST_P(SLRUSingleShardCacheTest, Downgrade) {
  const int weight = static_cast<int>(protected_segment_size_ / 100);
  int added_weight = 0;
  int i = 0;
  const int delta = 100;
  std::vector<int> keys;
  std::vector<int> values;
  // Add enough entries to fill protected segment to capacity.
  while (added_weight < 0.99 * protected_segment_size_) {
    Insert(i, delta + i, weight);
    keys.emplace_back(i);
    values.emplace_back(delta + i);
    ASSERT_TRUE(ProbationaryContains(EncodeInt(i)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(i)));
    // Upgrade to protected segment.
    for (int j = 0; j < lookups_threshold_; ++j) {
      ASSERT_EQ(delta + i, Lookup(i));
    }
    // Verify entry was upgraded.
    ASSERT_FALSE(ProbationaryContains(EncodeInt(i)));
    ASSERT_TRUE(ProtectedContains(EncodeInt(i)));
    added_weight += weight;
    ++i;
  }
  // Ensure that the same underlying entry is used when upgraded and eviction callback is not used.
  ASSERT_TRUE(evicted_keys_.empty());
  ASSERT_TRUE(evicted_values_.empty());

  int added_weight1 = 0;
  int k = 200;
  int r = 0;              // Mirrors i from previous loop, so we can track evicted entries.
  int evicted_index = 0;
  // Add entries to probationary segment, upgrade them to protected segment thus
  // kicking out entries from the protected segment since it's already at capacity.
  while (added_weight1 < 0.5 * probationary_segment_size_) {
    Insert(k, delta + k, weight);
    ASSERT_TRUE(ProbationaryContains(EncodeInt(k)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(k)));
    // Upgrade to protected segment, will kick out the LRU entry to make space.
    for (int j = 0; j < lookups_threshold_; ++j) {
      ASSERT_EQ(delta + k, Lookup(k));
    }
    // Verify current entry is upgraded.
    ASSERT_FALSE(ProbationaryContains(EncodeInt(k)));
    ASSERT_TRUE(ProtectedContains(EncodeInt(k)));
    // Verify LRU entry from protected segment is evicted to probationary segment.
    ASSERT_EQ(r, keys[evicted_index]);
    ASSERT_EQ(delta + r, values[evicted_index]);
    ASSERT_TRUE(ProbationaryContains(EncodeInt(r)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(r)));
    added_weight1 += weight;
    ++k, ++r, ++evicted_index;
  }

  // Upsert middle entry in full protected segment with larger weight to kick out
  // the LRU entry to probationary segment.
  int entry = 50;
  // Assert LRU entry of protected segment.
  ASSERT_FALSE(ProbationaryContains(EncodeInt(r)));
  ASSERT_TRUE(ProtectedContains(EncodeInt(r)));
  // Upsert middle entry of full protected segment to kick out LRU entry.
  ASSERT_FALSE(ProbationaryContains(EncodeInt(entry)));
  ASSERT_TRUE(ProtectedContains(EncodeInt(entry)));
  ASSERT_EQ(delta + entry, Lookup(entry));
  Insert(entry, delta * 2 + entry, static_cast<int>(1.25 * weight));
  ASSERT_FALSE(ProbationaryContains(EncodeInt(entry)));
  ASSERT_TRUE(ProtectedContains(EncodeInt(entry)));
  ASSERT_EQ(delta * 2 + entry, Lookup(entry));
  // Ensure LRU entry was evicted to probationary shard.
  ASSERT_TRUE(ProbationaryContains(EncodeInt(r)));
  ASSERT_FALSE(ProtectedContains(EncodeInt(r)));
  ASSERT_EQ(delta + r, Lookup(r));
}

// Tests that the LRU entries from the probationary segment are evicted to make space for
// any entries being downgraded from the protected segment. Ensure that only enough LRU entries to
// make space for new entries will be evicted.
TEST_P(SLRUSingleShardCacheTest, DowngradeEviction) {
  const int weight = static_cast<int>(protected_segment_size_ / 100);
  int added_weight = 0;
  int i = 0;
  const int delta = 100;
  // Add enough entries to fill protected segment to capacity.
  while (added_weight < 0.99 * protected_segment_size_) {
    Insert(i, delta + i, weight);
    ASSERT_TRUE(ProbationaryContains(EncodeInt(i)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(i)));
    // Upgrade to protected segment.
    for (int j = 0; j < lookups_threshold_; ++j) {
      ASSERT_EQ(delta + i, Lookup(i));
    }
    // Verify entry was upgraded.
    ASSERT_FALSE(ProbationaryContains(EncodeInt(i)));
    ASSERT_TRUE(ProtectedContains(EncodeInt(i)));
    added_weight += weight;
    ++i;
  }
  // Ensure that the same underlying entry is used when upgraded and eviction callback is not used.
  ASSERT_TRUE(evicted_keys_.empty());
  ASSERT_TRUE(evicted_values_.empty());

  const int probationary_weight = static_cast<int>(probationary_segment_size_ / 100);
  int probationary_added_weight = 0;
  int j = 200;
  std::vector<int> probationary_keys;
  std::vector<int> probationary_values;
  // Add enough entries to fill probationary segment to capacity.
  while (probationary_added_weight < 0.99 * probationary_segment_size_) {
    Insert(j, delta + j, probationary_weight);
    probationary_keys.emplace_back(j);
    probationary_values.emplace_back(delta + j);
    ASSERT_TRUE(ProbationaryContains(EncodeInt(j)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(j)));
    probationary_added_weight += probationary_weight;
    ++j;
  }


  // Upgrade the most recent probationary entry.
  auto last_key = probationary_keys.back();
  auto last_value = probationary_values.back();
  for (int k = 0; k < lookups_threshold_; ++k) {
    ASSERT_EQ(last_value, Lookup(last_key));
  }
  ASSERT_FALSE(ProbationaryContains(EncodeInt(last_key)));
  ASSERT_TRUE(ProtectedContains(EncodeInt(last_key)));


  // Verify that the LRU entries from the probationary segment are evicted to make space for entry
  // from protected segment being downgraded.
  for (int i = 0; i < evicted_keys_.size(); ++i) {
    ASSERT_EQ(probationary_keys[i], evicted_keys_[i]);
    ASSERT_EQ(probationary_values[i], evicted_values_[i]);
  }
}

// Test that entries in protected segment stay there after inserts larger than total cache size.
TEST_P(SLRUSingleShardCacheTest, LongInserts) {
  const int weight = static_cast<int>(protected_segment_size_ / 100);
  int added_weight = 0;
  int i = 0;
  // Add enough entries to fill protected segment to capacity.
  while (added_weight < 0.99 * protected_segment_size_) {
    Insert(i, 100 + i, weight);
    ASSERT_TRUE(ProbationaryContains(EncodeInt(i)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(i)));
    // Upgrade to protected segment.
    for (int j = 0; j < lookups_threshold_; ++j) {
      ASSERT_EQ(100 + i, Lookup(i));
    }
    // Verify entry was upgraded.
    ASSERT_FALSE(ProbationaryContains(EncodeInt(i)));
    ASSERT_TRUE(ProtectedContains(EncodeInt(i)));
    added_weight += weight;
    ++i;
  }

  // Insert random entries, total weight will be larger than the entire cache's size.
  int added_weight1 = 0;
  int k = 200;
  while (added_weight1 < 3 * total_cache_size_) {
    Insert(k, 100 + k, weight);
    ASSERT_TRUE(ProbationaryContains(EncodeInt(k)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(k)));
    added_weight1 += weight;
    ++k;
  }

  // Verify entries from protected segment were not affected from the inserts above.
  for (int l = 0; l < 100; ++l) {
    ASSERT_EQ(100 + l, Lookup(l));
    ASSERT_FALSE(ProbationaryContains(EncodeInt(l)));
    ASSERT_TRUE(ProtectedContains(EncodeInt(l)));
  }
}

// Tests simple corner cases ensuring entries with size that's greater than the probationary
// segment will not be inserted into the cache.
TEST_P(SLRUSingleShardCacheTest, CornerCases) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101, kSmallerSegmentSize + kLargerSegmentSize);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_FALSE(ProbationaryContains(EncodeInt(100)));

  Insert(100, 101, static_cast<int>(1.01 * kSmallerSegmentSize));
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_FALSE(ProbationaryContains(EncodeInt(100)));

  Insert(100, 101, kSmallerSegmentSize);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_TRUE(ProbationaryContains(EncodeInt(100)));
}

// Fills the probationary segment to capacity then inserts an entry with size greater
// than the probationary segment thus evicting all prior entries from the probationary segment.
TEST_P(SLRUSingleShardTest, CapacityCases) {
  const int weight = static_cast<int>(probationary_segment_size_ / 100);
  int added_weight = 0;
  int i = 0;
  const int delta = 100;
  std::vector<int> keys;
  std::vector<int> values;

  // Add enough entries to fill probationary segment to capacity.
  while (added_weight < 0.99 * probationary_segment_size_) {
    Insert(i, delta + i, weight);
    keys.emplace_back(i);
    values.emplace_back(delta + i);
    ASSERT_TRUE(ProbationaryContains(EncodeInt(i)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(i)));
    added_weight += weight;
    ++i;
  }

  ASSERT_EQ(100, keys.size());
  ASSERT_EQ(100, values.size());
  ASSERT_EQ(0, evicted_keys_.size());
  ASSERT_EQ(0, evicted_values_.size());

  for (int j = 0; j < keys.size(); ++j) {
    ASSERT_TRUE(ProbationaryContains(EncodeInt(keys[j])));
    ASSERT_EQ(values[j], Lookup(keys[j]));
  }

  // Insert entry whose size is equal to capacity of the probationary segment, this entry's
  // insertion will force the eviction of all existing entries in the probationary segment.
  Insert(200, 201, probationary_segment_size_);
  ASSERT_EQ(201, Lookup(200));
  ASSERT_TRUE(ProbationaryContains(EncodeInt(200)));

  ASSERT_EQ(100, evicted_keys_.size());
  ASSERT_EQ(100, evicted_values_.size());

  // Verify that the entries in the probationary segment prior to the insertion
  // of the entry above are evicted from the cache.
  for (int j = 0; j < evicted_keys_.size(); j++) {
    ASSERT_FALSE(ProbationaryContains(EncodeInt(keys[j])));
    ASSERT_EQ(-1, Lookup(keys[j]));
    ASSERT_EQ(keys[j], evicted_keys_[j]);
    ASSERT_EQ(values[j], evicted_values_[j]);
  }
}

// Tests the case where an entry with size greater than the protected segment is not
// upgraded from the probationary segment and any entries in the protected segment are unaffected.
TEST_P(SLRUSingleShardTest, EdgeCase) {
  const int weight = static_cast<int>(protected_segment_size_ / 100);
  int added_weight = 0;
  int i = 0;
  const int delta = 100;
  std::vector<int> keys;
  std::vector<int> values;

  // Add enough entries to fill protected segment to capacity.
  while (added_weight < 0.99 * protected_segment_size_) {
    Insert(i, delta + i, weight);
    keys.emplace_back(i);
    values.emplace_back(delta + i);
    ASSERT_TRUE(ProbationaryContains(EncodeInt(i)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(i)));
    // Upgrade to protected segment.
    for (int j = 0; j < lookups_threshold_; ++j) {
      ASSERT_EQ(delta + i, Lookup(i));
    }
    // Verify entry was upgraded.
    ASSERT_FALSE(ProbationaryContains(EncodeInt(i)));
    ASSERT_TRUE(ProtectedContains(EncodeInt(i)));
    added_weight += weight;
    ++i;
  }

  // Insert entry whose size is equal to capacity of the probationary segment.
  Insert(200, 201, probationary_segment_size_);
  ASSERT_EQ(201, Lookup(200));
  ASSERT_TRUE(ProbationaryContains(EncodeInt(200)));

  // Attempt to upgrade larger entry to the protected segment.
  for (int k = 0; k < lookups_threshold_; k++) {
    ASSERT_EQ(201, Lookup(200));
  }
  // Verify entry is not upgraded since its size is larger
  // than the capacity of the protected segment.
  ASSERT_TRUE(ProbationaryContains(EncodeInt(200)));
  ASSERT_FALSE(ProtectedContains(EncodeInt(200)));

  // Verify entries in protected segment were not affected
  // by attempted upgrade of large entry.
  for (int j = 0; j < keys.size(); j++) {
    ASSERT_FALSE(ProbationaryContains(EncodeInt(keys[j])));
    ASSERT_TRUE(ProtectedContains(EncodeInt(keys[j])));
    ASSERT_EQ(values[j], Lookup(keys[j]));
  }
}

// Insert entries with same key but different weights for
// both the probationary and protected segments.
TEST_P(SLRUSingleShardTest, InsertAndLookupPatterns) {
  std::vector<int> weights = {10, 3, 2, 1};
  int key = 0;
  const int value = 100;
  const int delta = 10;

  // Insert entry with same key in probationary segment with different weights.
  for (auto j = 0; j < weights.size(); ++j) {
    const int weight = static_cast<int>(probationary_segment_size_ / weights[j]);
    Insert(key, value, weight);
    ASSERT_EQ(value, Lookup(key));
    ASSERT_TRUE(ProbationaryContains(EncodeInt(key)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(key)));

    // Upsert case in the probationary segment.
    Insert(key, value + delta, weight);
    ASSERT_EQ(value + delta, Lookup(key));
    ASSERT_TRUE(ProbationaryContains(EncodeInt(key)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(key)));
    Erase(key);
  }

  // Insert entry with same key in protected segment with different weights.
  for (auto j = 0; j < weights.size(); ++j) {
    const int weight = static_cast<int>(protected_segment_size_ / weights[j]);
    Insert(key, value, weight);
    ASSERT_EQ(value, Lookup(key));
    ASSERT_TRUE(ProbationaryContains(EncodeInt(key)));
    ASSERT_FALSE(ProtectedContains(EncodeInt(key)));

    // Upgrade entry to the protected segment.
    for (auto j = 0; j < lookups_threshold_; ++j) {
      ASSERT_EQ(value, Lookup(key));
    }
    ASSERT_FALSE(ProbationaryContains(EncodeInt(key)));
    ASSERT_TRUE(ProtectedContains(EncodeInt(key)));

    // Upsert case in the protected segment.
    Insert(key, value + delta, weight);
    ASSERT_EQ(value + delta, Lookup(key));
    ASSERT_FALSE(ProbationaryContains(EncodeInt(key)));
    ASSERT_TRUE(ProtectedContains(EncodeInt(key)));
    Erase(key);
  }
}

} // namespace kudu
