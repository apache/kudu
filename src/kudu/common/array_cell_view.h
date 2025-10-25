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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <flatbuffers/vector.h>
#include <flatbuffers/verifier.h>
#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/common/common.pb.h"
#include "kudu/common/serdes/array1d.fb.h"
#include "kudu/gutil/port.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/status.h"

namespace kudu {

namespace serdes {
class ArrayTypeSerdesTest_Basic_Test;
}

constexpr serdes::ScalarArray KuduToScalarArrayType(DataType data_type) {
  switch (data_type) {
    case INT8:
      return serdes::ScalarArray::Int8Array;
    case BOOL:
    case UINT8:
      return serdes::ScalarArray::UInt8Array;
    case INT16:
      return serdes::ScalarArray::Int16Array;
    case UINT16:
      return serdes::ScalarArray::UInt16Array;
    case DATE:
    case INT32:
      return serdes::ScalarArray::Int32Array;
    case UINT32:
      return serdes::ScalarArray::UInt32Array;
    case INT64:
    case UNIXTIME_MICROS:
      return serdes::ScalarArray::Int64Array;
    case UINT64:
      return serdes::ScalarArray::UInt64Array;
    case FLOAT:
      return serdes::ScalarArray::FloatArray;
    case DOUBLE:
      return serdes::ScalarArray::DoubleArray;
    case STRING:
    case VARCHAR:
      return serdes::ScalarArray::StringArray;
    case BINARY:
      return serdes::ScalarArray::BinaryArray;
    case DECIMAL32:
      return serdes::ScalarArray::Int32Array;
    case DECIMAL64:
      return serdes::ScalarArray::Int64Array;
    case DECIMAL128:
    case INT128:
      LOG(DFATAL) << DataType_Name(data_type)
                 << ": type isn't yet supported in 1D arrays";
      return serdes::ScalarArray::NONE;
    default:
      LOG(DFATAL) << "unknown type: " << DataType_Name(data_type);
      return serdes::ScalarArray::NONE;
  }
}

class ArrayCellMetadataView final {
 public:
  static constexpr const size_t kArrayMaxElemNum = 65536;

  // buf: data raw pointer
  // len: size of the buffer (bytes) pointed at by the 'buf' pointer
  ArrayCellMetadataView(const uint8_t* buf, const size_t size)
       : data_(buf),
         size_(size),
         content_(nullptr),
         elem_num_(0),
         is_initialized_(false),
         has_nulls_(false) {
  }

  Status Init() {
    DCHECK(!is_initialized_);
    if (size_ == 0) {
      content_ = nullptr;
      elem_num_ = 0;
      has_nulls_ = false;
      is_initialized_ = true;
      return Status::OK();
    }

    DCHECK_GT(size_, 0);
    DCHECK(data_);
    {
      flatbuffers::Verifier::Options opt;
      // While verifying the input data, rely on the built-in constant of
      // FLATBUFFERS_MAX_BUFFER_SIZE.  2GiBytes - 1 bytes seems big enough
      // to fit a single one-dimensional array.
      if (PREDICT_FALSE(size_ + 1 > FLATBUFFERS_MAX_BUFFER_SIZE)) {
        return Status::InvalidArgument("serialized flatbuffers too big");
      }

      // Keep the verifier's parameters strict:
      //   * depth of 3 is enough to verify contents of serdes::Binary, the type
      //     from array1d.fbs of the deepest nesting
      //   * maximum number of tables to verify is set to 65536 + 1 to support
      //     the maximum possible number of serdes::UInt8Array elements as the
      //     contents of serdes::BinaryArray table: 65536 is the the limitation
      //     based on CFile's array data block format, and extra 1 comes from
      //     the top-level Content table (see serdes/array1d.fbs)
      //   * max_size is set to the size of the memory buffer plus one extra
      //     byte due to the strict 'less than' (not 'less than or equal')
      //     comparison criteria in the flatbuffers' logic that asserts the
      //     buffers size restriction
      opt.max_depth = 3;
      opt.max_tables = kArrayMaxElemNum + 1;
      opt.max_size = size_ + 1;

      flatbuffers::Verifier verifier(data_, size_, opt);
      if (PREDICT_FALSE(!serdes::VerifyContentBuffer(verifier))) {
        return Status::Corruption("corrupted flatbuffers data");
      }
    }

    content_ = serdes::GetContent(data_);
    if (PREDICT_FALSE(!content_)) {
      return Status::IllegalState("null flatbuffers of non-zero size");
    }

    // Short-curciut for the case when no fields are present.
    if (PREDICT_FALSE(!content_->data() && !content_->validity())) {
      elem_num_ = 0;
      has_nulls_ = false;
      is_initialized_ = true;
      return Status::OK();
    }

    elem_num_ = GetElemNum();
    const size_t validity_size = content_->validity() ? content_->validity()->size() : 0;
    if (validity_size != 0 && BitmapSize(elem_num_) != validity_size) {
      return Status::Corruption("'validity' and 'data' fields not in sync");
    }

    has_nulls_ = false;
    if (validity_size != 0) {
      // If the validity vector is supplied and with all its elements non-zero.
      const auto& validity = *(DCHECK_NOTNULL(content_->validity()));
      has_nulls_ = !BitmapIsAllSet(validity.Data(), 0, elem_num_);
    }

    if (has_nulls_) {
      DCHECK_GT(validity_size, 0);
      DCHECK_GT(elem_num_, 0);
      DCHECK_EQ(validity_size, BitmapSize(elem_num_));
      bitmap_.reset(new uint8_t[validity_size]);
      memcpy(bitmap_.get(), content_->validity()->Data(), validity_size);
    }
    const auto data_type = content_->data_type();
    if (data_type != serdes::ScalarArray::BinaryArray &&
        data_type != serdes::ScalarArray::StringArray) {
      // For non-binary types, there is nothing else to do.
      is_initialized_ = true;
      return Status::OK();
    }

    // Build the metadata on the spans of binary/string elements
    // in the buffer.
    DCHECK(content_->data());
    if (data_type == serdes::ScalarArray::StringArray) {
      const auto* values = DCHECK_NOTNULL(
          content_->data_as<serdes::StringArray>())->values();
      binary_data_spans_.reserve(values->size());
      for (auto cit = values->cbegin(); cit != values->cend(); ++cit) {
        const auto* str = DCHECK_NOTNULL(*cit);
        binary_data_spans_.emplace_back(str->c_str(), str->size());
      }
    } else {
      DCHECK(serdes::ScalarArray::BinaryArray == data_type);
      const auto* values = DCHECK_NOTNULL(
          content_->data_as<serdes::BinaryArray>())->values();
      binary_data_spans_.reserve(values->size());
      for (auto cit = values->cbegin(); cit != values->cend(); ++cit) {
        const auto* byte_seq = DCHECK_NOTNULL(cit->values());
        binary_data_spans_.emplace_back(byte_seq->Data(), byte_seq->size());
      }
    }
    is_initialized_ = true;
    return Status::OK();
  }

  ~ArrayCellMetadataView() = default;

  // Number of elements in the array.
  size_t elem_num() const {
    DCHECK(is_initialized_);
    return elem_num_;
  }

  bool empty() const {
    return elem_num() == 0;
  }

  // Non-null (a.k.a. validity) bitmap for the array elements.
  const uint8_t* not_null_bitmap() const {
    DCHECK(is_initialized_);
    DCHECK((has_nulls_ && bitmap_) || (!has_nulls_ && !bitmap_));
    return bitmap_.get();
  }

  bool has_nulls() const {
    DCHECK(is_initialized_);
    DCHECK((has_nulls_ && bitmap_) || (!has_nulls_ && !bitmap_));
    return has_nulls_;
  }

  const uint8_t* data_as(DataType data_type) const {
    DCHECK(is_initialized_);
    if (empty()) {
      return nullptr;
    }
    if (PREDICT_FALSE(content_->data_type() != KuduToScalarArrayType(data_type))) {
      return nullptr;
    }
    switch (data_type) {
      case DataType::BOOL:
        return data<serdes::UInt8Array>();
      case DataType::INT8:
        return data<serdes::Int8Array>();
      case DataType::UINT8:
        return data<serdes::UInt8Array>();
      case DataType::INT16:
        return data<serdes::Int16Array>();
      case DataType::UINT16:
        return data<serdes::UInt16Array>();
      case DataType::DATE:
      case DataType::DECIMAL32:
      case DataType::INT32:
        return data<serdes::Int32Array>();
      case DataType::UINT32:
        return data<serdes::UInt32Array>();
      case DataType::DECIMAL64:
      case DataType::INT64:
      case DataType::UNIXTIME_MICROS:
        return data<serdes::Int64Array>();
      case DataType::UINT64:
        return data<serdes::UInt64Array>();
      case DataType::FLOAT:
        return data<serdes::FloatArray>();
      case DataType::DOUBLE:
        return data<serdes::DoubleArray>();
      case DataType::BINARY:
      case DataType::STRING:
      case DataType::VARCHAR:
        // For binary/non-integer types, Kudu expects Slice elements.
        return reinterpret_cast<const uint8_t*>(binary_data_spans_.data());
      default:
        DCHECK(false) << "unsupported type: " << DataType_Name(data_type);
        return nullptr;
    }
  }

 private:
  FRIEND_TEST(serdes::ArrayTypeSerdesTest, Basic);

  template<typename T>
  const uint8_t* data() const {
    DCHECK(is_initialized_);
    return content_ ? content_->data_as<T>()->values()->Data() : nullptr;
  }

  size_t GetElemNum() const {
    if (PREDICT_FALSE(!content_)) {
      return 0;
    }
    if (PREDICT_FALSE(!content_->data())) {
      return 0;
    }
    const auto data_type = content_->data_type();
    switch (data_type) {
      case serdes::ScalarArray::Int8Array:
        return content_->data_as<serdes::Int8Array>()->values()->size();
      case serdes::ScalarArray::UInt8Array:
        return content_->data_as<serdes::UInt8Array>()->values()->size();
      case serdes::ScalarArray::Int16Array:
        return content_->data_as<serdes::Int16Array>()->values()->size();
      case serdes::ScalarArray::UInt16Array:
        return content_->data_as<serdes::UInt16Array>()->values()->size();
      case serdes::ScalarArray::Int32Array:
        return content_->data_as<serdes::Int32Array>()->values()->size();
      case serdes::ScalarArray::UInt32Array:
        return content_->data_as<serdes::UInt32Array>()->values()->size();
      case serdes::ScalarArray::Int64Array:
        return content_->data_as<serdes::Int64Array>()->values()->size();
      case serdes::ScalarArray::UInt64Array:
        return content_->data_as<serdes::UInt64Array>()->values()->size();
      case serdes::ScalarArray::FloatArray:
        return content_->data_as<serdes::FloatArray>()->values()->size();
      case serdes::ScalarArray::DoubleArray:
        return content_->data_as<serdes::DoubleArray>()->values()->size();
      case serdes::ScalarArray::StringArray:
        return content_->data_as<serdes::StringArray>()->values()->size();
      case serdes::ScalarArray::BinaryArray:
        return content_->data_as<serdes::BinaryArray>()->values()->size();
      default:
        LOG(DFATAL) << "unknown ScalarArray type: " << static_cast<size_t>(data_type);
        break;
    }

    return 0;
  }

  // Flatbuffer-encoded data; a non-owning raw pointer.
  const uint8_t* const data_;

  // Size of the encoded data, i.e. the number of bytes in the memory after
  // the 'data_' pointer that represent the serialized array.
  const size_t size_;

  // A non-owning raw pointer to the flatbuffers serialized buffer. It's nullptr
  // for an empty (size_ == 0) buffer.
  const serdes::Content* content_;

  // Number of elements in the underlying flatbuffers buffer.
  size_t elem_num_;

  // A bitmap built of the boolean validity vector.
  // TODO(aserbin): switch array1d to bitfield instead of bool vector for validity?
  std::unique_ptr<uint8_t[]> bitmap_;

  // Spans of binary data in the serialized buffer. This is populated only
  // for string/binary types.
  std::vector<Slice> binary_data_spans_;

  // Whether the Init() method has been successfully run for this object.
  bool is_initialized_;

  // Whether the underlying array has at least one null/invalid element.
  bool has_nulls_;
};

} // namespace kudu
