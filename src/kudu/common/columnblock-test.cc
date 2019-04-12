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

#include <string>

#include <gtest/gtest.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/types.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/test_macros.h"

namespace kudu {
class Slice;
} // namespace kudu

using std::string;
using strings::Substitute;

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

TEST(TestColumnBlock, TestCopyTo) {
  ScopedColumnBlock<UINT32> src(8, /*allow_nulls=*/false);
  ScopedColumnBlock<UINT32> dst(8, /*allow_nulls=*/false);

  for (int i = 0; i < src.nrows(); i++) {
    src[i] = i;
  }
  for (int i = 0; i < dst.nrows(); i++) {
    dst[i] = 100;
  }

  SelectionVector sv(src.nrows());
  sv.SetAllTrue();

  // src: 0   1   2   3   4   5   6   7
  // dst: 100 100 100 100 100 100 100 100
  // ------------------------------------
  // dst: 100 100 100 100 100 3   4   5
  ASSERT_OK(src.CopyTo(sv, &dst, 3, 5, 3));

  for (int i = 0; i < dst.nrows(); i++) {
    int expected_val = i < 5 ? 100 : i - 2;
    ASSERT_EQ(expected_val, dst[i]);
  }
}

TEST(TestColumnBlock, TestCopyToIndirectData) {
  ScopedColumnBlock<STRING> src(8, /*allow_nulls=*/false);
  ScopedColumnBlock<STRING> dst(8, /*allow_nulls=*/false);

  // Ignore idx 3, and poke a corresponding hole in the selection vector.
  Slice* next_cell = reinterpret_cast<Slice*>(src.data());
  for (int i = 0; i < src.nrows(); i++, next_cell++) {
    if (i == 3) continue;
    ASSERT_TRUE(src.arena()->RelocateSlice(Substitute("h$0", i), next_cell));
  }
  next_cell = reinterpret_cast<Slice*>(dst.data());
  for (int i = 0; i < dst.nrows(); i++, next_cell++) {
    ASSERT_TRUE(dst.arena()->RelocateSlice("", next_cell));
  }

  SelectionVector sv(src.nrows());
  sv.SetAllTrue();
  sv.SetRowUnselected(3);

  // src: h0 h1 h2 ?? h4 h5 h6 h7
  // dst: "" "" "" "" "" "" "" ""
  // ----------------------------
  // dst: "" "" "" "" "" "" h4 h5
  ASSERT_OK(src.CopyTo(sv, &dst, 3, 5, 3));

  for (int i = 0; i < dst.nrows(); i++) {
    string expected_val = i < 6 ? "" : Substitute("h$0", i - 2);
    ASSERT_EQ(expected_val, dst[i].ToString());
  }
}

TEST(TestColumnBlock, TestCopyToNulls) {
  ScopedColumnBlock<UINT32> src(8);
  ScopedColumnBlock<UINT32> dst(8);

  // Initialize idx 3 to null in both 'src' and 'dst'.
  for (int i = 0; i < src.nrows(); i++) {
    src.SetCellIsNull(i, i == 3);
    if (i != 3) {
      src[i] = i;
    }
  }
  for (int i = 0; i < dst.nrows(); i++) {
    dst.SetCellIsNull(i, i == 3);
    if (i != 3) {
      dst[i] = 100;
    }
  }

  SelectionVector sv(src.nrows());
  sv.SetAllTrue();

  // src: 0   1   2   null 4   5    6   7
  // dst: 100 100 100 null 100 100  100 100
  // --------------------------------------
  // dst: 100 100 100 null 100 null 4   5
  ASSERT_OK(src.CopyTo(sv, &dst, 3, 5, 3));

  for (int i = 0; i < dst.nrows(); i++) {
    SCOPED_TRACE(i);
    if (i == 3 || i == 5) {
      ASSERT_TRUE(dst.is_null(i));
    } else {
      ASSERT_FALSE(dst.is_null(i));
      int expected_val = i < 6 ? 100 : i - 2;
      ASSERT_EQ(expected_val, dst[i]);
    }
  }
}

} // namespace kudu
