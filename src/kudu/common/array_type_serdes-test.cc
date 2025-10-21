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

#include "kudu/common/array_type_serdes.h"

#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/common/array_cell_view.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/types.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/test_macros.h"

using std::unique_ptr;
using std::vector;

namespace kudu {

namespace serdes {
struct Int32Array;
} // namespace serdes

TEST(ArrayTypeSerdesTest, Basic) {
  const vector<int32_t> val{ 0, 1, 12, 5, 26, 42, };
  const uint8_t validity_bitmap[] = { 0b00111010 };
  const vector<bool> validity_vector(BitmapToVector(validity_bitmap, val.size()));
  ASSERT_EQ(val.size(), validity_vector.size());

  unique_ptr<uint8_t[]> buf_data;
  size_t buf_data_size = 0;
  ASSERT_OK(serdes::Serialize(GetTypeInfo(INT32),
                              reinterpret_cast<const uint8_t*>(val.data()),
                              val.size(),
                              validity_vector,
                              &buf_data,
                              &buf_data_size));
  ASSERT_TRUE(buf_data);
  const Slice cell(buf_data.get(), buf_data_size);

  Arena arena(128);
  Slice arena_cell;
  ASSERT_OK(serdes::SerializeIntoArena(
      GetTypeInfo(INT32),
      reinterpret_cast<const uint8_t*>(val.data()),
      validity_bitmap,
      val.size(),
      &arena,
      &arena_cell));

  // Make sure Serialize() an SerializeInfoArena() produce the same data.
  ASSERT_EQ(cell, arena_cell);

  // Peek into the serialized buffer using ArrayCellMetadataView and compare
  // the source data with the view into the serialized buffer.
  ArrayCellMetadataView view(cell.data(), cell.size());
  ASSERT_OK(view.Init());
  ASSERT_EQ(val.size(), view.elem_num());
  const auto* view_validity_bitmap = view.not_null_bitmap();
  ASSERT_NE(nullptr, view_validity_bitmap);
  ASSERT_TRUE(view.has_nulls());

  // Verify the data matches the source.
  {
    const uint8_t* data_view = view.data_as(INT32);
    ASSERT_NE(nullptr, data_view);
    ASSERT_EQ(0, memcmp(data_view, val.data(), sizeof(int32_t) * val.size()));
  }
  {
    const uint8_t* data_view = view.data<serdes::Int32Array>();
    ASSERT_NE(nullptr, data_view);
    ASSERT_EQ(0, memcmp(data_view, val.data(), sizeof(int32_t) * val.size()));
  }

  // Try peeking at the data as of wrong type: it should return nullptr.
  {
    ASSERT_EQ(nullptr, view.data_as(UINT32));
    ASSERT_EQ(nullptr, view.data_as(INT64));
  }
}

// When all the elements in the array are non-null/valid, the validity bitmap
// accessed via ArrayCellMetadataView is null in both two cases:
//   * the supplied validity vector have all the elements set to 'true'
//   * the supplied validity vector is empty
TEST(ArrayTypeSerdesTest, AllNonNullElements) {
  constexpr const uint8_t kThreeOnes[] = { 0b00000111 };
  constexpr const uint8_t* kNull = nullptr;
  const vector<bool> kEmpty{};

  for (const uint8_t* validity_bitmap : { kNull, kThreeOnes }) {
    const vector<int16_t> val{ 0, 1, 2, };
    const vector<bool>& validity_vector =
        validity_bitmap ? BitmapToVector(validity_bitmap, val.size()) : kEmpty;
    if (validity_bitmap) {
      ASSERT_EQ(val.size(), validity_vector.size());
    }

    unique_ptr<uint8_t[]> buf_data;
    size_t buf_data_size = 0;
    ASSERT_OK(serdes::Serialize(GetTypeInfo(INT16),
                                reinterpret_cast<const uint8_t*>(val.data()),
                                val.size(),
                                validity_vector,
                                &buf_data,
                                &buf_data_size));
    ASSERT_TRUE(buf_data);
    const Slice cell(buf_data.get(), buf_data_size);

    Arena arena(128);
    Slice arena_cell;
    ASSERT_OK(serdes::SerializeIntoArena(
        GetTypeInfo(INT16),
        reinterpret_cast<const uint8_t*>(val.data()),
        validity_bitmap,
        val.size(),
        &arena,
        &arena_cell));

    // Make sure Serialize() an SerializeInfoArena() produce the same data.
    ASSERT_EQ(cell, arena_cell);

    // Peek into the serialized buffer using ArrayCellMetadataView and compare
    // the source data with the view into the serialized buffer.
    ArrayCellMetadataView view(cell.data(), cell.size());
    ASSERT_OK(view.Init());
    ASSERT_EQ(val.size(), view.elem_num());
    const auto* view_validity_bitmap = view.not_null_bitmap();
    ASSERT_EQ(nullptr, view_validity_bitmap);
    ASSERT_FALSE(view.has_nulls());

    // Verify the data matches the source.
    {
      const uint8_t* data_view = view.data_as(INT16);
      ASSERT_NE(nullptr, data_view);
      ASSERT_EQ(0, memcmp(data_view, val.data(), sizeof(int16_t) * val.size()));
    }
  }
}

} // namespace kudu
