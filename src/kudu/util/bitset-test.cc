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

#include "kudu/util/bitset.h"

#include <cstddef>
#include <set>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/test_macros.h"

using std::set;
using std::string;
using strings::Substitute;

namespace {

enum TestEnum {
  BEGINNING,
  MIDDLE,
  END,
};
constexpr size_t kMaxEnumVal = TestEnum::END + 1;

typedef set<TestEnum> EnumSet;
typedef FixedBitSet<TestEnum, kMaxEnumVal> EnumBitSet;

const EnumSet kFullEnumSet({
  TestEnum::BEGINNING,
  TestEnum::MIDDLE,
  TestEnum::END,
});

bool CompareContainers(EnumSet enum_set, EnumBitSet ebs) {
  if (enum_set.empty() != ebs.empty()) {
    LOG(INFO) << Substitute("enum set is $0, bitset is $1",
                            enum_set.empty() ? "empty" : "not empty",
                            ebs.empty() ? "empty" : "not empty");
    return false;
  }
  if (enum_set.size() != ebs.size()) {
    LOG(INFO) << Substitute("enum set has $0 elements, bitset has $1",
                            enum_set.size(), ebs.size());
    return false;
  }
  for (const auto& e : enum_set) {
    if (!ContainsKey(ebs, e)) {
      LOG(INFO) << Substitute("enum set contains $0, not found in bitset", e);
      return false;
    }
  }
  for (TestEnum e : ebs) {
    if (!ContainsKey(enum_set, e)) {
      LOG(INFO) << Substitute("bitset contains $0, not found in enum set", e);
      return false;
    }
  }
  return true;
}

} // anonymous namespace

TEST(BitSetTest, TestConstruction) {
  EnumSet enum_set;
  for (int i = 0; i < kMaxEnumVal; i++) {
    InsertOrDie(&enum_set, static_cast<TestEnum>(i));
    EnumBitSet ebs(enum_set);
    ASSERT_TRUE(CompareContainers(enum_set, ebs));
  }
}

TEST(BitSetTest, TestInitializerList) {
  EnumBitSet ebs({ TestEnum::BEGINNING });
  ASSERT_TRUE(ContainsKey(ebs, TestEnum::BEGINNING));
  ASSERT_EQ(1, ebs.size());
}

// Test basic operations for a bitset of enums, comparing is to an STL
// container of enums.
TEST(BitSetTest, TestBasicOperations) {
  EnumBitSet bitset;
  ASSERT_TRUE(bitset.empty());

  EnumSet enum_set;
  const auto add_to_containers = [&] (TestEnum e) {
    ASSERT_EQ(InsertIfNotPresent(&enum_set, e),
              InsertIfNotPresent(&bitset, e));
  };
  const auto remove_from_containers = [&] (TestEnum e) {
    ASSERT_EQ(enum_set.erase(e), bitset.erase(e));
  };
  // Insert all elements, checking to make sure our containers' contents remain
  // the same.
  for (const auto& e : kFullEnumSet) {
    NO_FATALS(add_to_containers(e));
    ASSERT_TRUE(CompareContainers(enum_set, bitset));
  }

  // Do a sanity check that we can't insert something that already exists in
  // the set.
  ASSERT_FALSE(InsertIfNotPresent(&bitset, TestEnum::BEGINNING));

  // Now remove all elements.
  for (const auto& e : kFullEnumSet) {
    NO_FATALS(remove_from_containers(e));
    ASSERT_TRUE(CompareContainers(enum_set, bitset));
  }

  // Do a final sanity check that the bitset looks how we expect.
  ASSERT_TRUE(CompareContainers(enum_set, bitset));
  ASSERT_TRUE(bitset.empty());
}

// Test the set's insert interface.
TEST(BitSetTest, TestInsert) {
  EnumBitSet bitset;
  {
    auto iter_and_inserted = bitset.insert(TestEnum::BEGINNING);
    ASSERT_EQ(TestEnum::BEGINNING, *iter_and_inserted.first);
    ASSERT_TRUE(iter_and_inserted.second);
  }
  {
    auto iter_and_inserted = bitset.insert(TestEnum::BEGINNING);
    ASSERT_EQ(TestEnum::BEGINNING, *iter_and_inserted.first);
    ASSERT_FALSE(iter_and_inserted.second);
  }
}

#ifndef NDEBUG
// Make sure we hit a check failure if we attempt to use the bitset with values
// outside of its range.
TEST(BitSetDeathTest, TestInvalidUsage) {
  const string kDeathMessage = "Check failed";
  const TestEnum kOutOfRange = TestEnum(kMaxEnumVal);
  EnumBitSet bitset;
  EXPECT_DEATH({
    bitset.insert(kOutOfRange);
  }, kDeathMessage);

  EXPECT_DEATH({
    bitset.erase(kOutOfRange);
  }, kDeathMessage);

  EXPECT_DEATH({
    bitset.contains(kOutOfRange);
  }, kDeathMessage);

  ASSERT_TRUE(bitset.empty());
}
#endif // NDEBUG
