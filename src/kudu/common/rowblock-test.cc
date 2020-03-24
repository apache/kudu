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

#include "kudu/common/rowblock.h"

#include <cstddef>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

using std::vector;

namespace kudu {

TEST(TestSelectionVector, TestEquals) {
  SelectionVector sv1(10);
  SelectionVector sv2(10);

  // Test both false and true.
  sv1.SetAllFalse();
  sv2.SetAllFalse();
  ASSERT_EQ(sv1, sv2);
  sv1.SetAllTrue();
  sv2.SetAllTrue();
  ASSERT_EQ(sv1, sv2);

  // One row differs.
  sv2.SetRowUnselected(0);
  ASSERT_NE(sv1, sv2);

  // The length differs.
  SelectionVector sv3(5);
  sv3.SetAllTrue();
  ASSERT_NE(sv1, sv3);
}

// Test that SelectionVector functions that operate on bytes rather
// than bits work correctly even if we haven't set or unset all bytes en masse.
TEST(TestSelectionVector, TestNonByteAligned) {
  SelectionVector sv(3);

  for (size_t i = 0; i < sv.nrows(); i++) {
    sv.SetRowSelected(i);
  }
  ASSERT_EQ(sv.nrows(), sv.CountSelected());
  ASSERT_TRUE(sv.AnySelected());

  vector<uint16_t> sel;
  ASSERT_FALSE(sv.GetSelectedRows(&sel));

  for (size_t i = 0; i < sv.nrows(); i++) {
    sv.SetRowUnselected(i);
  }
  ASSERT_EQ(0, sv.CountSelected());
  ASSERT_FALSE(sv.AnySelected());
  ASSERT_TRUE(sv.GetSelectedRows(&sel));
  ASSERT_EQ(0, sel.size());
}

TEST(TestSelectionVector, TestGetSelectedRows) {
  vector<uint16_t> expected = {1, 4, 9, 10, 18};
  SelectionVector sv(20);
  sv.SetAllFalse();
  for (int i : expected) {
    sv.SetRowSelected(i);
  }
  vector<uint16_t> selected;
  ASSERT_TRUE(sv.GetSelectedRows(&selected));
  ASSERT_EQ(expected, selected);
}

} // namespace kudu
