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

// This unit test belongs in gutil, but it depends on test_main which is
// part of util.
#include "kudu/gutil/map-util.h"

#include <map>
#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest.h>

using std::map;
using std::string;
using std::shared_ptr;
using std::unique_ptr;

namespace kudu {

TEST(FloorTest, TestMapUtil) {
  map<int, int> my_map;

  ASSERT_EQ(nullptr, FindFloorOrNull(my_map, 5));

  my_map[5] = 5;
  ASSERT_EQ(5, *FindFloorOrNull(my_map, 6));
  ASSERT_EQ(5, *FindFloorOrNull(my_map, 5));
  ASSERT_EQ(nullptr, FindFloorOrNull(my_map, 4));

  my_map[1] = 1;
  ASSERT_EQ(5, *FindFloorOrNull(my_map, 6));
  ASSERT_EQ(5, *FindFloorOrNull(my_map, 5));
  ASSERT_EQ(1, *FindFloorOrNull(my_map, 4));
  ASSERT_EQ(1, *FindFloorOrNull(my_map, 1));
  ASSERT_EQ(nullptr, FindFloorOrNull(my_map, 0));
}

TEST(ComputeIfAbsentTest, TestComputeIfAbsent) {
  map<string, string> my_map;
  auto result = ComputeIfAbsent(&my_map, "key", []{ return "hello_world"; });
  ASSERT_EQ(*result, "hello_world");
  auto result2 = ComputeIfAbsent(&my_map, "key", [] { return "hello_world2"; });
  ASSERT_EQ(*result2, "hello_world");
}

TEST(ComputeIfAbsentTest, TestComputeIfAbsentAndReturnAbsense) {
  map<string, string> my_map;
  auto result = ComputeIfAbsentReturnAbsense(&my_map, "key", []{ return "hello_world"; });
  ASSERT_TRUE(result.second);
  ASSERT_EQ(*result.first, "hello_world");
  auto result2 = ComputeIfAbsentReturnAbsense(&my_map, "key", [] { return "hello_world2"; });
  ASSERT_FALSE(result2.second);
  ASSERT_EQ(*result2.first, "hello_world");
}

TEST(FindPointeeOrNullTest, TestFindPointeeOrNull) {
  map<string, unique_ptr<string>> my_map;
  auto iter = my_map.emplace("key", unique_ptr<string>(new string("hello_world")));
  ASSERT_TRUE(iter.second);
  string* value = FindPointeeOrNull(my_map, "key");
  ASSERT_TRUE(value != nullptr);
  ASSERT_EQ(*value, "hello_world");
  my_map.erase(iter.first);
  value = FindPointeeOrNull(my_map, "key");
  ASSERT_TRUE(value == nullptr);
}

TEST(EraseKeyReturnValuePtrTest, TestRawAndSmartSmartPointers) {
  map<string, unique_ptr<string>> my_map;
  unique_ptr<string> value = EraseKeyReturnValuePtr(&my_map, "key");
  ASSERT_TRUE(value.get() == nullptr);
  my_map.emplace("key", unique_ptr<string>(new string("hello_world")));
  value = EraseKeyReturnValuePtr(&my_map, "key");
  ASSERT_EQ(*value, "hello_world");
  value.reset();
  value = EraseKeyReturnValuePtr(&my_map, "key");
  ASSERT_TRUE(value.get() == nullptr);
  map<string, shared_ptr<string>> my_map2;
  shared_ptr<string> value2 = EraseKeyReturnValuePtr(&my_map2, "key");
  ASSERT_TRUE(value2.get() == nullptr);
  my_map2.emplace("key", std::make_shared<string>("hello_world"));
  value2 = EraseKeyReturnValuePtr(&my_map2, "key");
  ASSERT_EQ(*value2, "hello_world");
  map<string, string*> my_map_raw;
  my_map_raw.emplace("key", new string("hello_world"));
  value.reset(EraseKeyReturnValuePtr(&my_map_raw, "key"));
  ASSERT_EQ(*value, "hello_world");
}

TEST(EmplaceTest, TestEmplace) {
  string key1("k");
  string key2("k2");
  // Map with move-only value type.
  map<string, unique_ptr<string>> my_map;
  unique_ptr<string> val(new string("foo"));
  ASSERT_TRUE(EmplaceIfNotPresent(&my_map, key1, std::move(val)));
  ASSERT_TRUE(ContainsKey(my_map, key1));
  ASSERT_FALSE(EmplaceIfNotPresent(&my_map, key1, nullptr))
      << "Should return false for already-present";

  val = unique_ptr<string>(new string("bar"));
  ASSERT_TRUE(EmplaceOrUpdate(&my_map, key2, std::move(val)));
  ASSERT_TRUE(ContainsKey(my_map, key2));
  ASSERT_EQ("bar", *FindOrDie(my_map, key2));
  val = unique_ptr<string>(new string("foobar"));
  ASSERT_FALSE(EmplaceOrUpdate(&my_map, key2, std::move(val)));
  ASSERT_EQ("foobar", *FindOrDie(my_map, key2));
}

} // namespace kudu
