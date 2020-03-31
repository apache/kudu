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

#include "kudu/util/random_util.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <ostream>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/util/int128.h"
#include "kudu/util/random.h"
#include "kudu/util/test_util.h"

using std::unordered_set;

namespace kudu {

class RandomUtilTest : public KuduTest {
 protected:
  RandomUtilTest() : rng_(SeedRandom()) {}

  Random rng_;

  static const int kLenMax = 100;
  static const int kNumTrials = 100;
};

namespace {

// Checks string defined at start is set to \0 everywhere but [from, to)
void CheckEmpty(char* start, int from, int to, int stop) {
  DCHECK_LE(0, from);
  DCHECK_LE(from, to);
  DCHECK_LE(to, stop);
  for (int j = 0; (j == from ? j = to : j) < stop; ++j) {
    CHECK_EQ(start[j], '\0') << "Index " << j << " not null after defining"
                             << "indices [" << from << "," << to << ") of "
                             << "a nulled string [0," << stop << ").";
  }
}

} // anonymous namespace

// Makes sure that RandomString only writes the specified amount
TEST_F(RandomUtilTest, TestRandomString) {
  char start[kLenMax];

  for (int i = 0; i < kNumTrials; ++i) {
    memset(start, '\0', kLenMax);
    int to = rng_.Uniform(kLenMax + 1);
    int from = rng_.Uniform(to + 1);
    RandomString(start + from, to - from, &rng_);
    CheckEmpty(start, from, to, kLenMax);
  }

  // Corner case
  memset(start, '\0', kLenMax);
  RandomString(start, 0, &rng_);
  CheckEmpty(start, 0, 0, kLenMax);
}

// Static helper function for testing generation of random numbers in range.
// CreateRandomUniqueIntegers() doesn't support 128-bit integers yet. Hence a separate
// template function instead of member of TemplateRandomUtilTest class.
// TODO(bankim): Once CreateRandomIntegersInRange() supports 128-bit integers make it a
//               member function of TemplateRandomUtilTest class.
template <typename IntType>
static void RunCreateRandomIntegersInRangerHelper(int num_trials, IntType min_val, IntType max_val,
                                                  Random* rng) {
  static constexpr int kMaxNumVals = 1000;
  for (int i = 0; i < num_trials; ++i) {
    int num_vals = rng->Uniform(kMaxNumVals);
    auto vals = CreateRandomIntegersInRange<IntType>(num_vals, min_val, max_val, rng);
    ASSERT_EQ(num_vals, vals.size());
    for (const auto& v : vals) {
      ASSERT_GE(v, min_val);
      ASSERT_LT(v, max_val);
    }
  }
}

template<typename IntType>
class TemplateRandomUtilTest : public RandomUtilTest {
 public:
  void RunCreateRandomUniqueIntegers() {
    static constexpr int kMaxNumVals = 1000;
    for (int i = 0; i < kNumTrials; ++i) {
      unordered_set<IntType> avoid;
      for (int j = 0; j < i; ++j) {
        avoid.insert(j);
      }
      int num_vals = rng_.Uniform(kMaxNumVals);
      auto vals = CreateRandomUniqueIntegers<IntType>(num_vals, avoid, &rng_);
      ASSERT_EQ(num_vals, vals.size());
      for (const auto& v : vals) {
        ASSERT_FALSE(ContainsKey(avoid, v));
      }
    }
  }

  void RunCreateRandomIntegersInRange() {
    static constexpr IntType min_val = std::numeric_limits<IntType>::min();
    static constexpr IntType max_val = std::numeric_limits<IntType>::max();
    // Exercise entire range.
    RunCreateRandomIntegersInRangerHelper(kNumTrials, min_val, max_val, &rng_);
    // Exercise partial range.
    RunCreateRandomIntegersInRangerHelper(kNumTrials, min_val / 2, max_val / 2, &rng_);
  }
};

// Testing with char, short data-types will result in compile-time error, as expected.
// Hence no run-time unit tests for non-32/64-bit integers.
typedef ::testing::Types<int32_t, uint32_t, int64_t, uint64_t> IntTypes;
TYPED_TEST_CASE(TemplateRandomUtilTest, IntTypes);

TYPED_TEST(TemplateRandomUtilTest, RunCreateRandomUniqueIntegers) {
  this->RunCreateRandomUniqueIntegers();
}

TYPED_TEST(TemplateRandomUtilTest, RunCreateRandomIntegersInRange) {
  this->RunCreateRandomIntegersInRange();
}

// TODO(bankim): CreateRandomUniqueIntegers() doesn't support 128-bit integers yet.
//               Once it does add int128_t and uint128_t types to IntTypes and use
//               templatized TemplateRandomUtilTest instead.
TEST_F(RandomUtilTest, RunCreateRandom128BitIntegersInRange) {
  // Exercise entire range.
  RunCreateRandomIntegersInRangerHelper<int128_t>(kNumTrials, INT128_MIN, INT128_MAX, &rng_);
  RunCreateRandomIntegersInRangerHelper<uint128_t>(kNumTrials, 0, UINT128_MAX, &rng_);

  // Exercise partial range.
  RunCreateRandomIntegersInRangerHelper<int128_t>(kNumTrials, INT128_MIN / 2, INT128_MAX / 2,
                                                  &rng_);
  RunCreateRandomIntegersInRangerHelper<uint128_t>(kNumTrials, 0, UINT128_MAX / 2, &rng_);
}

} // namespace kudu
