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

#pragma once

#include <memory>
#include <vector>

#include <flatbuffers/base.h>
#include <flatbuffers/buffer.h>
#include <flatbuffers/flatbuffer_builder.h>
#include <flatbuffers/stl_emulation.h>

#include "kudu/common/serdes/array1d.fb.h"
#include "kudu/common/types.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/status.h"

namespace kudu {
namespace serdes {

template<DataType KUDU_DATA_TYPE, typename FB_TYPE>
void BuildFlatbuffers(
    const uint8_t* column_data,
    size_t nrows,
    const uint8_t* validity_bitmap,
    flatbuffers::FlatBufferBuilder* bld) {
  typedef typename DataTypeTraits<KUDU_DATA_TYPE>::cpp_type ElementType;

  DCHECK(bld);
  auto& builder = *bld;

  if (validity_bitmap && BitmapIsAllSet(validity_bitmap, 0, nrows)) {
    // All bits in the validity bitmap are set: it's equivalent of null bitmap.
    validity_bitmap = nullptr;
  }

  std::vector<ElementType> val;
  val.resize(nrows);
  if (column_data != nullptr) {
    DCHECK_NE(0, nrows);
    if constexpr (std::is_same<Slice, ElementType>::value) {
      static const Slice kEmptySlice(static_cast<uint8_t*>(nullptr), 0);
      const Slice* ptr = reinterpret_cast<const Slice*>(column_data);
      for (size_t idx = 0; idx < nrows; ++idx) {
        val[idx] = (!validity_bitmap || BitmapTest(validity_bitmap, idx))
            ? *(ptr + idx) : kEmptySlice;
      }
    } else {
      static_assert(!std::is_same<Slice, ElementType>::value,
                    "cannot be a binary type");
      memcpy(val.data(), column_data, nrows * sizeof(ElementType));
    }
  }

  std::vector<uint8_t> validity_vector;
  if (validity_bitmap) {
    validity_vector.resize(BitmapSize(nrows));
    memcpy(validity_vector.data(), validity_bitmap, validity_vector.size());
  }
  const auto validity_fb =
      validity_vector.empty() ? 0 : builder.CreateVector(validity_vector);

  if constexpr (KUDU_DATA_TYPE == STRING) {
    auto values = FB_TYPE::Traits::Create(
        builder, builder.CreateVectorOfStrings<ElementType>(val));
    builder.Finish(CreateContent(builder,
                                 KuduToScalarArrayType(KUDU_DATA_TYPE),
                                 values.Union(),
                                 validity_fb));
  } else if constexpr (KUDU_DATA_TYPE == BINARY) {
    std::vector<flatbuffers::Offset<serdes::UInt8Array>> offsets;
    offsets.reserve(val.size());
    for (const auto& e : val) {
      auto ev = builder.CreateVector(
          reinterpret_cast<const uint8_t*>(e.data()), e.size());
      offsets.emplace_back(serdes::CreateUInt8Array(builder, ev));
    }

    auto values = CreateBinaryArrayDirect(builder, &offsets);
    builder.Finish(CreateContent(builder,
                                 KuduToScalarArrayType(KUDU_DATA_TYPE),
                                 values.Union(),
                                 validity_fb));
  } else {
    auto values = FB_TYPE::Traits::Create(
        builder, builder.CreateVector<ElementType>(val));
    builder.Finish(CreateContent(builder,
                                 KuduToScalarArrayType(KUDU_DATA_TYPE),
                                 values.Union(),
                                 validity_fb));
  }
}

template<DataType KUDU_DATA_TYPE, typename FB_TYPE>
Status Serialize(
    const uint8_t* column_data,
    size_t nrows,
    const std::vector<bool>& validity,
    std::unique_ptr<uint8_t[]>* out_buf,
    size_t* out_buf_size) {
  typedef typename DataTypeTraits<KUDU_DATA_TYPE>::cpp_type ElementType;

  DCHECK(out_buf);
  DCHECK(out_buf_size);
  DCHECK(validity.empty() || validity.size() == nrows);

  if (PREDICT_FALSE(column_data == nullptr && nrows > 0)) {
    return Status::InvalidArgument("inconsistent data and validity info for array");
  }

  flatbuffers::FlatBufferBuilder builder(
      nrows * sizeof(ElementType) + nrows + FLATBUFFERS_MIN_BUFFER_SIZE);

  const uint8_t* validity_bitmap = nullptr;
  std::vector<uint8_t> validity_vector;
  if (!validity.empty() && std::any_of(validity.begin(), validity.end(),
                                       [&](bool e) { return !e; })) {
    validity_vector.resize(BitmapSize(nrows));
    VectorToBitmap(validity, validity_vector.data());
    validity_bitmap = validity_vector.data();
  }
  BuildFlatbuffers<KUDU_DATA_TYPE, FB_TYPE>(column_data, nrows, validity_bitmap, &builder);
  DCHECK(builder.GetBufferPointer());

  // TODO(aserbin): would it be better to copy the data from the builder
  //                instead of using ReleaseRaw?
  size_t buf_size = 0;
  size_t buf_offset = 0;
  std::unique_ptr<uint8_t[]> buf(builder.ReleaseRaw(buf_size, buf_offset));
  DCHECK_LT(buf_offset, buf_size);
  DCHECK(buf);
  // TODO(aserbin): introduce offset and use it instead of calling memmove()?
  memmove(buf.get(), buf.get() + buf_offset, buf_size - buf_offset);

  *out_buf = std::move(buf);
  *out_buf_size = buf_size - buf_offset;

  return Status::OK();
}

template<DataType KUDU_DATA_TYPE, typename FB_TYPE>
Status SerializeIntoArena(
    const uint8_t* column_data,
    const uint8_t* validity_bitmap,
    size_t nrows,
    Arena* arena,
    Slice* out) {
  typedef typename DataTypeTraits<KUDU_DATA_TYPE>::cpp_type ElementType;

  DCHECK(arena);
  DCHECK(out);

  flatbuffers::FlatBufferBuilder builder(
      nrows * sizeof(ElementType) + nrows + FLATBUFFERS_MIN_BUFFER_SIZE);
  BuildFlatbuffers<KUDU_DATA_TYPE, FB_TYPE>(column_data, nrows, validity_bitmap, &builder);

  // Copy the serialized data into the arena.
  //
  // TODO(aserbin): is it possible to avoid copying and serialize directly into
  //                the provided Arena by using the arena's allocator as the
  //                custom allocator for FlatbufferBuilder, and then maybe use
  //                DetachedBuffer?
  const auto* buf = builder.GetBufferPointer();
  DCHECK(buf);
  const auto data_size = builder.GetSize();

  uint8_t* data = reinterpret_cast<uint8_t*>(arena->AllocateBytes(data_size));
  if (PREDICT_FALSE(!data)) {
    return Status::RuntimeError("out of memory serialzing array column data");
  }
  memcpy(data, buf, data_size);
  *out = Slice(data, data_size);
  return Status::OK();
}

inline Status Serialize(
    const TypeInfo* elem_typeinfo,
    const uint8_t* column_data,
    size_t nrows,
    const std::vector<bool>& validity,
    std::unique_ptr<uint8_t[]>* out_buf,
    size_t* out_buf_size) {
  switch (elem_typeinfo->type()) {
    case INT8:
      return Serialize<INT8, serdes::Int8Array>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case BOOL:
    case UINT8:
      return Serialize<UINT8, serdes::UInt8Array>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case INT16:
      return Serialize<INT16, serdes::Int16Array>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case UINT16:
      return Serialize<UINT16, serdes::UInt16Array>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case DATE:
    case DECIMAL32:
    case INT32:
      return Serialize<INT32, serdes::Int32Array>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case UINT32:
      return Serialize<UINT32, serdes::UInt32Array>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case DECIMAL64:
    case INT64:
    case UNIXTIME_MICROS:
      return Serialize<INT64, serdes::Int64Array>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case UINT64:
      return Serialize<UINT64, serdes::UInt64Array>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case FLOAT:
      return Serialize<FLOAT, serdes::FloatArray>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case DOUBLE:
      return Serialize<DOUBLE, serdes::DoubleArray>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case BINARY:
      return Serialize<BINARY, serdes::BinaryArray>(
          column_data, nrows, validity, out_buf, out_buf_size);
    case STRING:
    case VARCHAR:
      return Serialize<STRING, serdes::StringArray>(
          column_data, nrows, validity, out_buf, out_buf_size);
    default:
      return Status::NotSupported("unsupported array element type",
                                  DataType_Name(elem_typeinfo->type()));
  }
}

inline Status SerializeIntoArena(
    const TypeInfo* elem_typeinfo,
    const uint8_t* column_data,
    const uint8_t* validity_bitmap,
    size_t nrows,
    Arena* arena,
    Slice* out) {
  switch (elem_typeinfo->type()) {
    case INT8:
      return SerializeIntoArena<INT8, serdes::Int8Array>(
          column_data, validity_bitmap, nrows, arena, out);
    case BOOL:
    case UINT8:
      return SerializeIntoArena<UINT8, serdes::UInt8Array>(
          column_data, validity_bitmap, nrows, arena, out);
    case INT16:
      return SerializeIntoArena<INT16, serdes::Int16Array>(
          column_data, validity_bitmap, nrows, arena, out);
    case UINT16:
      return SerializeIntoArena<UINT16, serdes::UInt16Array>(
          column_data, validity_bitmap, nrows, arena, out);
    case DATE:
    case DECIMAL32:
    case INT32:
      return SerializeIntoArena<INT32, serdes::Int32Array>(
          column_data, validity_bitmap, nrows, arena, out);
    case UINT32:
      return SerializeIntoArena<UINT32, serdes::UInt32Array>(
          column_data, validity_bitmap, nrows, arena, out);
    case DECIMAL64:
    case INT64:
    case UNIXTIME_MICROS:
      return SerializeIntoArena<INT64, serdes::Int64Array>(
          column_data, validity_bitmap, nrows, arena, out);
    case UINT64:
      return SerializeIntoArena<UINT64, serdes::UInt64Array>(
          column_data, validity_bitmap, nrows, arena, out);
    case FLOAT:
      return SerializeIntoArena<FLOAT, serdes::FloatArray>(
          column_data, validity_bitmap, nrows, arena, out);
    case DOUBLE:
      return SerializeIntoArena<DOUBLE, serdes::DoubleArray>(
          column_data, validity_bitmap, nrows, arena, out);
    case BINARY:
      return SerializeIntoArena<BINARY, serdes::BinaryArray>(
          column_data, validity_bitmap, nrows, arena, out);
    case STRING:
    case VARCHAR:
      return SerializeIntoArena<STRING, serdes::StringArray>(
          column_data, validity_bitmap, nrows, arena, out);
    default:
      return Status::NotSupported("unsupported array element type",
                                  DataType_Name(elem_typeinfo->type()));
  }
}

} // namespace serdes
} // namespace kudu
