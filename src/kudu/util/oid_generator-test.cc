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

#include "kudu/util/oid_generator.h"

#include <string>

#include <gtest/gtest.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;

namespace kudu {

TEST(ObjectIdGeneratorTest, TestCanoicalizeUuid) {
  ObjectIdGenerator gen;
  const string kExpectedCanonicalized = "0123456789abcdef0123456789abcdef";
  string canonicalized;
  Status s = gen.Canonicalize("not_a_uuid", &canonicalized);
  {
    SCOPED_TRACE(s.ToString());
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "invalid uuid");
  }
  ASSERT_OK(gen.Canonicalize(
      "01234567-89ab-cdef-0123-456789abcdef", &canonicalized));
  ASSERT_EQ(kExpectedCanonicalized, canonicalized);
  ASSERT_OK(gen.Canonicalize(
      "0123456789abcdef0123456789abcdef", &canonicalized));
  ASSERT_EQ(kExpectedCanonicalized, canonicalized);
  ASSERT_OK(gen.Canonicalize(
      "0123456789AbCdEf0123456789aBcDeF", &canonicalized));
  ASSERT_EQ(kExpectedCanonicalized, canonicalized);
}

} // namespace kudu
