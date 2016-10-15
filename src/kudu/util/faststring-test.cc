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

#include <algorithm>
#include <cstring>
#include <memory>

#include <gtest/gtest.h>

#include "kudu/util/faststring.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/test_util.h"

namespace kudu {
class FaststringTest : public KuduTest {};

TEST_F(FaststringTest, TestShrinkToFit_Empty) {
  faststring s;
  s.shrink_to_fit();
  ASSERT_EQ(faststring::kInitialCapacity, s.capacity());
}

// Test that, if the string contents is shorter than the initial capacity
// of the faststring, shrink_to_fit() leaves the string in the built-in
// array.
TEST_F(FaststringTest, TestShrinkToFit_SmallerThanInitialCapacity) {
  faststring s;
  s.append("hello");
  s.shrink_to_fit();
  ASSERT_EQ(faststring::kInitialCapacity, s.capacity());
}

TEST_F(FaststringTest, TestShrinkToFit_Random) {
  Random r(GetRandomSeed32());
  int kMaxSize = faststring::kInitialCapacity * 2;
  std::unique_ptr<char[]> random_bytes(new char[kMaxSize]);
  RandomString(random_bytes.get(), kMaxSize, &r);

  faststring s;
  for (int i = 0; i < 100; i++) {
    int new_size = r.Uniform(kMaxSize);
    s.resize(new_size);
    memcpy(s.data(), random_bytes.get(), new_size);
    s.shrink_to_fit();
    ASSERT_EQ(0, memcmp(s.data(), random_bytes.get(), new_size));
    ASSERT_EQ(std::max<int>(faststring::kInitialCapacity, new_size), s.capacity());
  }
}

} // namespace kudu
