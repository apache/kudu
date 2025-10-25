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

#include <flatbuffers/base.h>
#include <flatbuffers/flatbuffer_builder.h>
#include <gtest/gtest.h>

#include "kudu/common/array_cell_view.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/serdes/array1d.fb.h"
#include "kudu/common/types.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::unique_ptr;
using std::vector;

namespace kudu {
namespace serdes {

TEST(ArrayTypeSerdesTest, Basic) {
  const vector<int32_t> val{ 0, 1, 12, 5, 26, 42, };
  const uint8_t validity_bitmap[] = { 0b00111010 };
  const vector<bool> validity_vector(BitmapToVector(validity_bitmap, val.size()));
  ASSERT_EQ(val.size(), validity_vector.size());

  unique_ptr<uint8_t[]> buf_data;
  size_t buf_data_size = 0;
  ASSERT_OK(Serialize(GetTypeInfo(INT32),
                      reinterpret_cast<const uint8_t*>(val.data()),
                      val.size(),
                      validity_vector,
                      &buf_data,
                      &buf_data_size));
  ASSERT_TRUE(buf_data);
  const Slice cell(buf_data.get(), buf_data_size);

  Arena arena(128);
  Slice arena_cell;
  ASSERT_OK(SerializeIntoArena(GetTypeInfo(INT32),
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
  ASSERT_EQ(0, memcmp(view_validity_bitmap, validity_bitmap, 1));

  // Verify the data matches the source.
  {
    const uint8_t* data_view = view.data_as(INT32);
    ASSERT_NE(nullptr, data_view);
    ASSERT_EQ(0, memcmp(data_view, val.data(), sizeof(int32_t) * val.size()));
  }
  {
    const uint8_t* data_view = view.data<Int32Array>();
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
    SCOPED_TRACE(validity_bitmap ? "all ones" : "null");
    const vector<int16_t> val{ 0, 1, 2, };
    const vector<bool>& validity_vector =
        validity_bitmap ? BitmapToVector(validity_bitmap, val.size()) : kEmpty;
    if (validity_bitmap) {
      ASSERT_EQ(val.size(), validity_vector.size());
    }

    unique_ptr<uint8_t[]> buf_data;
    size_t buf_data_size = 0;
    ASSERT_OK(Serialize(GetTypeInfo(INT16),
                        reinterpret_cast<const uint8_t*>(val.data()),
                        val.size(),
                        validity_vector,
                        &buf_data,
                        &buf_data_size));
    ASSERT_TRUE(buf_data);
    const Slice cell(buf_data.get(), buf_data_size);

    Arena arena(128);
    Slice arena_cell;
    ASSERT_OK(SerializeIntoArena(GetTypeInfo(INT16),
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

TEST(ArrayTypeSerdesTest, EmptyData) {
  for (const auto array_type : EnumValuesScalarArray()) {
    SCOPED_TRACE(EnumNameScalarArray(array_type));
    const vector<int32_t> val{};
    unique_ptr<uint8_t[]> buf_data;
    size_t buf_data_size = 0;
    ASSERT_OK(Serialize(GetTypeInfo(INT8),
                        reinterpret_cast<const uint8_t*>(val.data()),
                        val.size(),
                        {},
                        &buf_data,
                        &buf_data_size));
    ASSERT_TRUE(buf_data);
    const Slice cell(buf_data.get(), buf_data_size);

    ArrayCellMetadataView view(cell.data(), cell.size());
    ASSERT_OK(view.Init());
    ASSERT_EQ(val.size(), view.elem_num());
    ASSERT_TRUE(view.empty());
    ASSERT_FALSE(view.has_nulls());
    const auto* view_validity_bitmap = view.not_null_bitmap();
    ASSERT_EQ(nullptr, view_validity_bitmap);
  }
}

static const DataType kScalarColumnTypes[] = {
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    STRING,
    BOOL,
    FLOAT,
    DOUBLE,
    BINARY,
    UNIXTIME_MICROS,
    INT128,
    DECIMAL32,
    DECIMAL64,
    DECIMAL128,
    VARCHAR,
    DATE,
};

// Verify how ArrayCellMetadataView works with empty content, where when neither
// 'data' nor 'validity' field is present, and other combinations when one field
// is present, but empty, and another is absent or empty. It shouldn't crash
// and should successfully parse the flatbuffers data as expected.
TEST(ArrayTypeSerdesTest, EmptyContent) {
  constexpr int NO_DATA_NO_VALIDITY = 0;
  constexpr int NO_DATA_EMPTY_VALIDITY = 1;
  constexpr int EMPTY_DATA_NO_VALIDITY = 2;
  constexpr int EMPTY_DATA_EMPTY_VALIDITY = 3;

  constexpr int kFieldOptions[] = {
    NO_DATA_NO_VALIDITY,
    NO_DATA_EMPTY_VALIDITY,
    EMPTY_DATA_NO_VALIDITY,
    EMPTY_DATA_EMPTY_VALIDITY,
  };

  // Try various types that correspond to different code paths in the
  // implementation of ArrayCellMetadataView::Init().
  for (auto opt : kFieldOptions) {
    for (const auto array_type : EnumValuesScalarArray()) {
      SCOPED_TRACE(EnumNameScalarArray(array_type));
      flatbuffers::FlatBufferBuilder bld;
      switch (opt) {
        case NO_DATA_NO_VALIDITY:
          bld.Finish(CreateContent(bld, array_type, 0, 0));
          break;
        case NO_DATA_EMPTY_VALIDITY:
          bld.Finish(CreateContent(bld, array_type, 0, {}));
          break;
        case EMPTY_DATA_NO_VALIDITY:
          bld.Finish(CreateContent(bld, array_type, {}, 0));
          break;
        case EMPTY_DATA_EMPTY_VALIDITY:
          bld.Finish(CreateContent(bld, array_type, {}, {}));
          break;
        default:
          FAIL();
          break;
      }

      size_t buf_size = 0;
      size_t buf_offset = 0;
      unique_ptr<uint8_t[]> buf(bld.ReleaseRaw(buf_size, buf_offset));
      ASSERT_NE(nullptr, buf.get());
      ASSERT_LT(buf_offset, buf_size);
      const Slice cell(buf.get() + buf_offset, buf_size);
      ASSERT_GE(buf_size - buf_offset, FLATBUFFERS_MIN_BUFFER_SIZE);

      ArrayCellMetadataView view(cell.data(), cell.size());
      ASSERT_OK(view.Init());
      ASSERT_TRUE(view.empty());
      ASSERT_EQ(0, view.elem_num());
      ASSERT_FALSE(view.has_nulls());
      ASSERT_EQ(nullptr, view.not_null_bitmap());
      for (auto data_type : kScalarColumnTypes) {
        SCOPED_TRACE(DataType_Name(data_type));
        ASSERT_EQ(nullptr, view.data_as(data_type));
      }
    }
  }
}

TEST(ArrayTypeSerdesTest, NoDataButNonEmptyValidity) {
  const vector<uint8_t> validity{ 0x01 };
  // Try various types that correspond to different code paths in the
  // implementation of ArrayCellMetadataView::Init().
  for (const auto array_type : EnumValuesScalarArray()) {
    SCOPED_TRACE(EnumNameScalarArray(array_type));
    flatbuffers::FlatBufferBuilder bld;
    const auto validity_fb = bld.CreateVector(validity);
    bld.Finish(CreateContent(bld, array_type, 0, validity_fb));

    size_t buf_size = 0;
    size_t buf_offset = 0;
    unique_ptr<uint8_t[]> buf(bld.ReleaseRaw(buf_size, buf_offset));
    ASSERT_NE(nullptr, buf.get());
    ASSERT_LT(buf_offset, buf_size);
    const Slice cell(buf.get() + buf_offset, buf_size);

    ArrayCellMetadataView view(cell.data(), cell.size());
    const auto s = view.Init();
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_STR_CONTAINS(s.ToString(), "'validity' and 'data' fields not in sync");
  }
}

} // namespace serdes
} // namespace kudu
