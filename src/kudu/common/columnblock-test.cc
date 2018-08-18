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

#include "kudu/common/columnblock.h"

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"

namespace kudu {

TEST(TestColumnBlock, TestEquals) {
  ScopedColumnBlock<UINT32> scb1(1);
  ScopedColumnBlock<UINT32> scb2(1);
  ASSERT_EQ(scb1, scb2);

  // Even though this updates a value, scb2 is still entirely null.
  scb2[0] = 5;
  ASSERT_EQ(scb1, scb2);

  // If we un-null that cell in scb2, the two column blocks are no longer equal.
  scb2.SetCellIsNull(0, false);
  ASSERT_NE(scb1, scb2);

  // Now let's also un-null that cell in scb1. The null bitmaps match again, but
  // the data itself does not.
  scb1.SetCellIsNull(0, false);
  ASSERT_NE(scb1, scb2);

  // These two column blocks aren't the same length.
  ScopedColumnBlock<UINT32> scb3(1);
  ScopedColumnBlock<UINT32> scb4(2);
  ASSERT_NE(scb3, scb4);

  // Let's make sure they're equal even when the data is indirected.
  ScopedColumnBlock<STRING> scb5(1);
  ScopedColumnBlock<STRING> scb6(1);
  scb5.SetCellIsNull(0, false);
  scb5[0] = "foo";
  scb6.SetCellIsNull(0, false);
  scb6[0] = "foo";
  ASSERT_EQ(scb5, scb6);
}

} // namespace kudu
