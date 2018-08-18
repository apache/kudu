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

#include <gtest/gtest.h>

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

} // namespace kudu
