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

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/common/array_cell_view.h"
#include "kudu/common/array_type_serdes.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/columnblock-test-util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/util/int128.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

DEFINE_int32(cfile_test_block_size, 1024,
             "Block size to use for testing cfiles. "
             "Default is low to stress code, but can be set higher for "
             "performance testing");

namespace kudu {
namespace cfile {

// The true/false pattern alternates every 2^(N-1) consequitive integer
// input values, cycling between:
//   2^(N-1) true
//   2^(N-1) alternating true/false
//   2^(N-1) false
//   2^(N-1) alternating true/false
//
// This is useful for stress-testing the run-length coding of NULL values
// in data blocks.
template<size_t N>
bool IsNullAlternating(size_t n) {
  switch ((n >> N) & 3) {
    case 1:
    case 3:
      return n & 1;
    case 2:
      return false;
    default:
      return true;
  }
}

// Abstract test data generator.
// You must implement BuildTestValue() to return your test value.
//    Usage example:
//        StringDataGenerator<true> datagen;
//        datagen.Build(10);
//        for (int i = 0; i < datagen.block_entries(); ++i) {
//          bool is_null = !BitmapTest(datagen.non_null_bitmap(), i);
//          Slice& v = datagen[i];
//        }
template<DataType DATA_TYPE, bool HAS_NULLS>
class DataGenerator {
 public:
  static constexpr bool has_nulls() {
    return HAS_NULLS;
  }

  static constexpr bool is_array() {
    return false;
  }

  static constexpr const DataType kDataType = DATA_TYPE;

  typedef typename DataTypeTraits<DATA_TYPE>::cpp_type cpp_type;

  DataGenerator()
      : block_entries_(0),
        total_entries_(0) {
  }

  virtual ~DataGenerator() = default;

  void Reset() {
    block_entries_ = 0;
    total_entries_ = 0;
  }

  void Build(size_t num_entries) {
    Build(total_entries_, num_entries);
    total_entries_ += num_entries;
  }

  // Build "num_entries" using (offset + i) as value
  // You can get the data values and the null bitmap using values() and non_null_bitmap()
  // both are valid until the class is destructed or until Build() is called again.
  void Build(size_t offset, size_t num_entries) {
    Resize(num_entries);

    for (size_t i = 0; i < num_entries; ++i) {
      if (HAS_NULLS) {
        BitmapChange(non_null_bitmap_.get(), i, !TestValueShouldBeNull(offset + i));
      }
      values_[i] = BuildTestValue(i, offset + i);
    }
  }

  virtual cpp_type BuildTestValue(size_t block_index, size_t value) = 0;

  bool TestValueShouldBeNull(size_t n) {
    if (!HAS_NULLS) {
      return false;
    }
    return IsNullAlternating<6>(n);
  }

  virtual void Resize(size_t num_entries) {
    if (block_entries_ >= num_entries) {
      block_entries_ = num_entries;
      return;
    }

    values_.reset(new cpp_type[num_entries]);
    non_null_bitmap_.reset(new uint8_t[BitmapSize(num_entries)]);
    block_entries_ = num_entries;
  }

  size_t block_entries() const { return block_entries_; }
  size_t total_entries() const { return total_entries_; }

  const cpp_type* values() const { return values_.get(); }
  const uint8_t* non_null_bitmap() const { return non_null_bitmap_.get(); }

  const cpp_type& operator[](size_t index) const {
    return values_[index];
  }

 protected:
  std::unique_ptr<cpp_type[]> values_;
  std::unique_ptr<uint8_t[]> non_null_bitmap_;
  size_t block_entries_;
  size_t total_entries_;
};

template<bool HAS_NULLS>
class UInt8DataGenerator : public DataGenerator<UINT8, HAS_NULLS> {
 public:
  UInt8DataGenerator() {}
  ATTRIBUTE_NO_SANITIZE_INTEGER
  uint8_t BuildTestValue(size_t /*block_index*/, size_t value) override {
    return (value * 10) % 256;
  }
};

template<bool HAS_NULLS>
class Int8DataGenerator : public DataGenerator<INT8, HAS_NULLS> {
 public:
  Int8DataGenerator() {}
  ATTRIBUTE_NO_SANITIZE_INTEGER
  int8_t BuildTestValue(size_t /*block_index*/, size_t value) override {
    return ((value * 10) % 128) * (value % 2 == 0 ? -1 : 1);
  }
};

template<bool HAS_NULLS>
class UInt16DataGenerator : public DataGenerator<UINT16, HAS_NULLS> {
 public:
  UInt16DataGenerator() {}
  ATTRIBUTE_NO_SANITIZE_INTEGER
  uint16_t BuildTestValue(size_t /*block_index*/, size_t value) override {
    return (value * 10) % 65536;
  }
};

template<bool HAS_NULLS>
class Int16DataGenerator : public DataGenerator<INT16, HAS_NULLS> {
 public:
  Int16DataGenerator() {}
  ATTRIBUTE_NO_SANITIZE_INTEGER
  int16_t BuildTestValue(size_t block_index, size_t value) override {
    return ((value * 10) % 32768) * (value % 2 == 0 ? -1 : 1);
  }
};

template<bool HAS_NULLS>
class UInt32DataGenerator : public DataGenerator<UINT32, HAS_NULLS> {
 public:
  UInt32DataGenerator() {}
  ATTRIBUTE_NO_SANITIZE_INTEGER
  uint32_t BuildTestValue(size_t /*block_index*/, size_t value) override {
    return value * 10;
  }
};

template<bool HAS_NULLS>
class Int32DataGenerator : public DataGenerator<INT32, HAS_NULLS> {
 public:
  Int32DataGenerator() {}
  ATTRIBUTE_NO_SANITIZE_INTEGER
  int32_t BuildTestValue(size_t /*block_index*/, size_t value) override {
    return (value * 10) *(value % 2 == 0 ? -1 : 1);
  }
};

template<bool HAS_NULLS>
class UInt64DataGenerator : public DataGenerator<UINT64, HAS_NULLS> {
 public:
  UInt64DataGenerator() {}
  ATTRIBUTE_NO_SANITIZE_INTEGER
  uint64_t BuildTestValue(size_t /*block_index*/, size_t value) override {
    return value * 0x123456789abcdefULL;
  }
};

template<bool HAS_NULLS>
class Int64DataGenerator : public DataGenerator<INT64, HAS_NULLS> {
 public:
  Int64DataGenerator() {}
  ATTRIBUTE_NO_SANITIZE_INTEGER
  int64_t BuildTestValue(size_t /*block_index*/, size_t value) override {
    int64_t r = (value * 0x123456789abcdefULL) & 0x7fffffffffffffffULL;
    return value % 2 == 0 ? r : -r;
  }
};

template<bool HAS_NULLS>
class Int128DataGenerator : public DataGenerator<INT128, HAS_NULLS> {
public:
  Int128DataGenerator() {}
  ATTRIBUTE_NO_SANITIZE_INTEGER
  int128_t BuildTestValue(size_t /*block_index*/, size_t value) override {
    int128_t r = (value * 0x123456789abcdefULL) & 0x7fffffffffffffffULL;
    return value % 2 == 0 ? r : -r;
  }
};

// Floating-point data generator.
// This works for both floats and doubles.
template<DataType DATA_TYPE, bool HAS_NULLS>
class FPDataGenerator : public DataGenerator<DATA_TYPE, HAS_NULLS> {
 public:
  typedef typename DataTypeTraits<DATA_TYPE>::cpp_type cpp_type;

  FPDataGenerator() {}
  cpp_type BuildTestValue(size_t /*block_index*/, size_t value) override {
    return static_cast<cpp_type>(value) * 1.0001;
  }
};

template<bool HAS_NULLS>
class StringDataGenerator : public DataGenerator<STRING, HAS_NULLS> {
 public:
  explicit StringDataGenerator(const char* format)
      : StringDataGenerator(
          [=](size_t x) { return StringPrintf(format, x); }) {
  }

  explicit StringDataGenerator(std::function<std::string(size_t)> formatter)
      : formatter_(std::move(formatter)) {
  }

  Slice BuildTestValue(size_t block_index, size_t value) override {
    data_buffers_[block_index] = formatter_(value);
    return Slice(data_buffers_[block_index]);
  }

  void Resize(size_t num_entries) override {
    data_buffers_.resize(num_entries);
    DataGenerator<STRING, HAS_NULLS>::Resize(num_entries);
  }

 private:
  std::vector<std::string> data_buffers_;
  std::function<std::string(size_t)> formatter_;
};

// Class for generating strings that contain duplicate
template<bool HAS_NULLS>
class DuplicateStringDataGenerator : public DataGenerator<STRING, HAS_NULLS> {
 public:

  // num specify number of possible unique strings that can be generated
  explicit DuplicateStringDataGenerator(const char* format, int num)
  : format_(format),
    num_(num) {
  }

  Slice BuildTestValue(size_t block_index, size_t value) override {
    // random number from 0 ~ num_-1
    value = random() % num_;
    char* buf = data_buffer_[block_index].data;
    int len = snprintf(buf, kItemBufferSize - 1, format_, value);
    DCHECK_LT(len, kItemBufferSize);
    return Slice(buf, len);
  }

  void Resize(size_t num_entries) override {
    if (num_entries > this->block_entries()) {
      data_buffer_.reset(new Buffer[num_entries]);
    }
    DataGenerator<STRING, HAS_NULLS>::Resize(num_entries);
  }

 private:
  static const int kItemBufferSize = 16;

  struct Buffer {
    char data[kItemBufferSize];
  };

  std::unique_ptr<Buffer[]> data_buffer_;
  const char* format_;
  int num_;
};

// Generator for fully random int32 data.
class RandomInt32DataGenerator : public DataGenerator<INT32, /* HAS_NULLS= */ false> {
 public:
  int32_t BuildTestValue(size_t /*block_index*/, size_t /*value*/) override {
    return random();
  }
};

// Generic data generation interface for array cells. It's supposed to be
// pluggable with minimal constexpr-eval style changes into the places
// where DataGenerator is used.
template<DataType DATA_TYPE,
         typename FB_TYPE,
         bool HAS_NULLS,
         bool HAS_NULLS_IN_ARRAY,
         size_t MAX_NUM_ELEMENTS_IN_ARRAY>
class ArrayDataGenerator {
 public:
  typedef typename DataTypeTraits<DATA_TYPE>::cpp_type cpp_type;

  static constexpr const DataType kDataType = DATA_TYPE;

  static constexpr bool has_nulls() {
    return HAS_NULLS;
  }

  static constexpr bool is_array() {
    return true;
  }

  ArrayDataGenerator()
      : block_entries_(0),
        total_entries_(0),
        total_elem_count_(0),
        values_total_sum_(0),
        str_values_total_size_(0) {
  }

  virtual ~ArrayDataGenerator() = default;

  void Reset() {
    block_entries_ = 0;
    total_entries_ = 0;
    total_elem_count_ = 0;
    values_total_sum_ = 0;
    str_values_total_size_ = 0;
  }

  void Build(size_t num_entries) {
    Build(total_entries_, num_entries);
    total_entries_ += num_entries;
  }

  // Build "num_entries" of array cells using (offset + i) as value.
  // The data is available via 'values()', non-null for whole array cells
  // via 'non_null_bitmap()', and validity of elements in each of the generated
  // arrays via 'cell_non_null_bitmaps()'. Those are alive while an instance
  // of this class is alive, and invalidated by next calls to either 'Build()'
  // or 'Resize()'.
  void Build(size_t offset, size_t num_entries) {
    Resize(num_entries);

    DCHECK_EQ(num_entries, array_cells_.size());
    DCHECK_EQ(num_entries, flatbuffers_.size());
    DCHECK_EQ(num_entries, cell_non_null_bitmaps_.size());
    DCHECK_EQ(num_entries, cell_non_null_bitmaps_container_.size());
    DCHECK_EQ(num_entries, cell_non_null_bitmaps_sizes_.size());
    DCHECK_EQ(num_entries, values_vector_.size());
    DCHECK_EQ(num_entries, values_vector_str_.size());
    DCHECK_EQ(num_entries, block_entries_);

    for (size_t i = 0; i < num_entries; ++i) {
      const bool is_null_array =
          HAS_NULLS ? TestValueShouldBeNull(offset + i) : false;
      BitmapChange(non_null_bitmap_.get(), i, !is_null_array);

      if (is_null_array) {
        cell_non_null_bitmaps_sizes_[i] = 0;
        cell_non_null_bitmaps_[i] = nullptr;
        array_cells_[i].clear();
        values_vector_[i].clear();
        values_vector_str_[i].clear();
        flatbuffers_[i].reset();
        continue;
      }

      const size_t array_elem_num = random() % MAX_NUM_ELEMENTS_IN_ARRAY;
      total_elem_count_ += array_elem_num;

      // Set the number of bits for the array bitmap correspondingly.
      cell_non_null_bitmaps_sizes_[i] = array_elem_num;
      if (array_elem_num == 0) {
        // Build an empty array for this cell.
        values_vector_[i].clear();
        values_vector_str_[i].clear();

#if DCHECK_IS_ON()
        // As an extra provision to catch mistakes in debug builds, supply
        // null pointers for empty (i.e. zero-sized) Slices instances.
        // For an empty Slice, the code shouldn't try dereferencing its data
        // pointer, and that would be a mistake doing otherwise.
        array_cells_[i] = (i % 2 == 0) ? Slice(static_cast<uint8_t*>(nullptr), 0)
                                       : Slice();
#else
        array_cells_[i] = Slice();
#endif
      } else {
        // Helper container for flatbuffer's serialization.
        std::vector<bool> validity_src(array_elem_num, false);
        values_vector_[i].resize(array_elem_num);
        values_vector_str_[i].resize(array_elem_num);

        cell_non_null_bitmaps_[i] = cell_non_null_bitmaps_container_[i].get();
        uint8_t* array_non_null_bitmap = cell_non_null_bitmaps_[i];
        DCHECK(array_non_null_bitmap);
        for (size_t j = 0; j < array_elem_num; ++j) {
          const bool array_elem_is_null =
              HAS_NULLS_IN_ARRAY ? TestValueInArrayShouldBeNull(offset + i + j)
                                 : false;
          BitmapChange(array_non_null_bitmap, j, !array_elem_is_null);
          validity_src[j] = !array_elem_is_null;
          if constexpr (DATA_TYPE == STRING) {
            auto& str = values_vector_str_[i][j];
            if (array_elem_is_null) {
              str.clear();
              values_vector_[i][j] = Slice();
            } else {
              str.reserve(20);
              str = std::to_string(random());
              values_vector_[i][j] = Slice(str.data(), str.size());
              str_values_total_size_ += str.size();
            }
          } else if constexpr (DATA_TYPE == BINARY) {
            auto& str = values_vector_str_[i][j];
            if (array_elem_is_null) {
              str.clear();
              values_vector_[i][j] = Slice();
            } else {
              str.reserve(45);
              const auto r = random();
              str.push_back(r % 128);
              str.push_back(42 + r % 32);
              str.append(std::to_string(r));
              str.append({ 0x00, 0x7f, 0x42 });
              str.append(std::to_string(random()));
              values_vector_[i][j] = Slice(str.data(), str.size());
              str_values_total_size_ += str.size();
            }
          } else {
            static_assert(!std::is_same<Slice, cpp_type>::value,
                          "cannot be a binary type");
            if (array_elem_is_null) {
              values_vector_[i][j] = 0;
            } else {
              values_vector_[i][j] = BuildTestValue(i, offset + i + j);
              IncrementValuesTotalSum(static_cast<size_t>(values_vector_[i][j]));
            }
          }
        }

        size_t buf_size = 0;
        const auto s = serdes::Serialize<DATA_TYPE, FB_TYPE>(
            reinterpret_cast<uint8_t*>(values_vector_[i].data()),
            array_elem_num,
            validity_src,
            &flatbuffers_[i],
            &buf_size);
        CHECK_OK(s);
        array_cells_[i] = Slice(flatbuffers_[i].get(), buf_size);
      }
    }
  }

  virtual cpp_type BuildTestValue(size_t block_index, size_t value) = 0;

  bool TestValueShouldBeNull(size_t n) {
    if (!HAS_NULLS) {
      return false;
    }
    return IsNullAlternating</*N=*/5>(n);
  }

  bool TestValueInArrayShouldBeNull(size_t n) {
    if (!HAS_NULLS_IN_ARRAY) {
      return false;
    }
    return IsNullAlternating</*N=*/6>(n);
  }

  // Resize() resets all the underlying data if 'num_entries' is greater
  // than 'block_entries_'.
  virtual void Resize(size_t num_entries) {
    cell_non_null_bitmaps_container_.resize(num_entries);

    cell_non_null_bitmaps_.resize(num_entries);
    cell_non_null_bitmaps_sizes_.resize(num_entries);

    array_cells_.resize(num_entries);
    flatbuffers_.resize(num_entries);
    values_vector_.resize(num_entries);
    values_vector_str_.resize(num_entries);

    if (block_entries_ >= num_entries) {
      block_entries_ = num_entries;
      return;
    }

    for (auto i = 0; i < num_entries; ++i) {
      // Allocate up to the maximum possible number of elements in each array
      // cell. The cells are populated with elements (or nullified) by Build().
      cell_non_null_bitmaps_sizes_[i] = MAX_NUM_ELEMENTS_IN_ARRAY;
      cell_non_null_bitmaps_container_[i].reset(new uint8_t[BitmapSize(cell_non_null_bitmaps_sizes_[i])]);
      cell_non_null_bitmaps_[i] = cell_non_null_bitmaps_container_[i].get();

      array_cells_[i] = Slice();
      flatbuffers_[i].reset();
      values_vector_[i].clear();
      values_vector_str_[i].clear();
    }
    non_null_bitmap_.reset(new uint8_t[BitmapSize(num_entries)]);
    block_entries_ = num_entries;
    // The 'total_entries_' isn't touched -- it corresponds to the offset
    // in the generated CFile, so it's possible to continue writing into
    // the same file even after Resize().
  }

  size_t block_entries() const {
    return block_entries_;
  }
  size_t total_entries() const {
    return total_entries_;
  }

  const uint8_t* non_null_bitmap() const {
    return non_null_bitmap_.get();
  }

  const void* cells() const {
    return reinterpret_cast<const void*>(array_cells_.data());
  }

  size_t total_elem_count() const {
    return total_elem_count_;
  }

  size_t values_total_sum() const {
    return values_total_sum_;
  }

  size_t str_values_total_size() const {
    return str_values_total_size_;
  }

 private:
  ATTRIBUTE_NO_SANITIZE_INTEGER
  void IncrementValuesTotalSum(size_t increment) {
    // The overflow of 'values_total_sum_' is intended.
    values_total_sum_ += static_cast<size_t>(increment);
  }

  // The storage of non-null bitmaps for each of the generated array cells:
  // these are bitmaps to reflect on the nullability of individual array
  // elements in each array cell.
  // This container performs auto-cleanup upon destruction of the generator.
  std::vector<std::unique_ptr<uint8_t[]>> cell_non_null_bitmaps_container_;

  // Slices that point to the memory stored in 'flatbuffers_', with some offset.
  std::vector<Slice> array_cells_;

  // Flatbuffers-serialized representations of arrays.
  std::vector<std::unique_ptr<uint8_t[]>> flatbuffers_;

  // This is a complimentary container to store std::string instances when the
  // 'values_vector_' container below stores Slice instances.
  std::vector<std::vector<std::string>> values_vector_str_;

  // The storage of test values. This constainer stores Slice instances for
  // STRING/BINARY Kudu types, and the std::string instances backing these
  // are stored in the 'values_vector_str_' complimentary container.
  std::vector<std::vector<cpp_type>> values_vector_;

  // Bitmap to track whether it's an array (maybe an empty one) or just null.
  std::unique_ptr<uint8_t[]> non_null_bitmap_;

  // Bitmaps to track null elements in the arrays themselves: this container
  // stores raw pointers to memory backed by corresponding elemenents
  // of the 'cell_non_null_bitmaps_container_' field.
  std::vector<uint8_t*> cell_non_null_bitmaps_;

  // Number of bits in the corresponding bitmaps from 'cell_non_null_bitmaps_';
  // null and empty arrays has zero bits in their bitmaps.
  std::vector<size_t> cell_non_null_bitmaps_sizes_;

  size_t block_entries_;
  size_t total_entries_;

  // Total number of array elements (including null/non-valid elements in array cells).
  size_t total_elem_count_;

  // Sum of all integer values generated during Build(): useful for verification
  // of the result when reading the stored values from CFile.
  size_t values_total_sum_;
  // Total length/size of all binary/string value generated during Build():
  // useful for verification of the result when reading the data from CFile.
  size_t str_values_total_size_;
};

// This template is for numeric/non-binary data types for array elements,
// where the return type of random() can be statically cast into the type
// of the element.
template<DataType KUDU_DATA_TYPE,
         typename PB_TYPE,
         bool HAS_NULLS,
         bool HAS_NULLS_IN_ARRAY,
         size_t MAX_NUM_ELEMENTS_IN_ARRAY>
class IntArrayRandomDataGenerator :
    public ArrayDataGenerator<KUDU_DATA_TYPE,
                              PB_TYPE,
                              HAS_NULLS,
                              HAS_NULLS_IN_ARRAY,
                              MAX_NUM_ELEMENTS_IN_ARRAY> {
 public:
  typedef typename DataTypeTraits<KUDU_DATA_TYPE>::cpp_type ElemType;
  ElemType BuildTestValue(size_t /*block_index*/, size_t /*value*/) override {
    return static_cast<ElemType>(random());
  }
};

template<DataType KUDU_DATA_TYPE,
         typename PB_TYPE,
         bool HAS_NULLS,
         bool HAS_NULLS_IN_ARRAY,
         size_t MAX_NUM_ELEMENTS_IN_ARRAY>
class StrArrayRandomDataGenerator :
    public ArrayDataGenerator<KUDU_DATA_TYPE,
                              PB_TYPE,
                              HAS_NULLS,
                              HAS_NULLS_IN_ARRAY,
                              MAX_NUM_ELEMENTS_IN_ARRAY> {
 public:
  typedef typename DataTypeTraits<KUDU_DATA_TYPE>::cpp_type ElemType;

  // This is a fake implementation just to satisfy the signature of the
  // overloaded function.
  Slice BuildTestValue(size_t /*block_index*/, size_t /*value*/) override {
    static const Slice kEmpty;
    return kEmpty;
  }
};

template<bool HAS_NULLS,
         bool HAS_NULLS_IN_ARRAY,
         size_t MAX_NUM_ELEMENTS_IN_ARRAY>
using Int8ArrayRandomDataGenerator =
    IntArrayRandomDataGenerator<INT8,
                                serdes::Int8Array,
                                HAS_NULLS,
                                HAS_NULLS_IN_ARRAY,
                                MAX_NUM_ELEMENTS_IN_ARRAY>;

template<bool HAS_NULLS,
         bool HAS_NULLS_IN_ARRAY,
         size_t MAX_NUM_ELEMENTS_IN_ARRAY>
using Int32ArrayRandomDataGenerator =
    IntArrayRandomDataGenerator<INT32,
                                serdes::Int32Array,
                                HAS_NULLS,
                                HAS_NULLS_IN_ARRAY,
                                MAX_NUM_ELEMENTS_IN_ARRAY>;

template<bool HAS_NULLS,
         bool HAS_NULLS_IN_ARRAY,
         size_t MAX_NUM_ELEMENTS_IN_ARRAY>
using StringArrayRandomDataGenerator =
    StrArrayRandomDataGenerator<STRING,
                                serdes::StringArray,
                                HAS_NULLS,
                                HAS_NULLS_IN_ARRAY,
                                MAX_NUM_ELEMENTS_IN_ARRAY>;

template<bool HAS_NULLS,
         bool HAS_NULLS_IN_ARRAY,
         size_t MAX_NUM_ELEMENTS_IN_ARRAY>
using BinaryArrayRandomDataGenerator =
    StrArrayRandomDataGenerator<BINARY,
                                serdes::BinaryArray,
                                HAS_NULLS,
                                HAS_NULLS_IN_ARRAY,
                                MAX_NUM_ELEMENTS_IN_ARRAY>;


class CFileTestBase : public KuduTest {
 public:
  void SetUp() override {
    KuduTest::SetUp();

    fs_manager_.reset(new FsManager(env_, FsManagerOpts(GetTestPath("fs_root"))));
    ASSERT_OK(fs_manager_->CreateInitialFileSystemLayout());
    ASSERT_OK(fs_manager_->Open());
  }

  // Place a ColumnBlock and SelectionVector into a context. This context will
  // not support decoder evaluation.
  ColumnMaterializationContext CreateNonDecoderEvalContext(ColumnBlock* cb, SelectionVector* sel) {
    return ColumnMaterializationContext(0, nullptr, cb, sel);
  }
 protected:
  enum Flags {
    NO_FLAGS = 0,
    WRITE_VALIDX = 1,
    SMALL_BLOCKSIZE = 1 << 1
  };

  template<class DataGeneratorType>
  void WriteTestFile(DataGeneratorType* data_generator,
                     EncodingType encoding,
                     CompressionType compression,
                     int num_entries,
                     uint32_t flags,
                     BlockId* block_id) {
    std::unique_ptr<fs::WritableBlock> sink;
    ASSERT_OK(fs_manager_->CreateNewBlock({}, &sink));
    *block_id = sink->id();
    WriterOptions opts;
    opts.write_posidx = true;

    if (flags & WRITE_VALIDX) {
      opts.write_validx = true;
    }
    if (flags & SMALL_BLOCKSIZE) {
      // Use a smaller block size to exercise multi-level indexing.
      opts.storage_attributes.cfile_block_size = 1024;
    }

    opts.storage_attributes.encoding = encoding;
    opts.storage_attributes.compression = compression;
    CFileWriter w(opts,
                  DataGeneratorType::is_array()
                      ? GetArrayTypeInfo(DataGeneratorType::kDataType)
                      : GetTypeInfo(DataGeneratorType::kDataType),
                  DataGeneratorType::has_nulls(),
                  std::move(sink));

    ASSERT_OK(w.Start());

    // Append given number of values to the test tree. We use 100 to match
    // the output block size of compaction (kCompactionOutputBlockNumRows in
    // compaction.cc, unfortunately not linkable from the cfile/ module)
    const size_t kBufferSize = 100;
    size_t i = 0;
    while (i < num_entries) {
      int towrite = std::min(num_entries - i, kBufferSize);

      data_generator->Build(towrite);
      DCHECK_EQ(towrite, data_generator->block_entries());

      if constexpr (DataGeneratorType::is_array()) {
        ASSERT_OK_FAST(w.AppendNullableArrayEntries(
            data_generator->non_null_bitmap(),
            data_generator->cells(),
            towrite));
      } else {
        if constexpr (DataGeneratorType::has_nulls()) {
          ASSERT_OK_FAST(w.AppendNullableEntries(data_generator->non_null_bitmap(),
                                                 data_generator->values(),
                                                 towrite));
        } else {
          ASSERT_OK_FAST(w.AppendEntries(data_generator->values(), towrite));
        }
      }
      i += towrite;
    }

    ASSERT_OK(w.Finish());
  }

  std::unique_ptr<FsManager> fs_manager_;
};

// Fast unrolled summing of a vector.
// GCC's auto-vectorization doesn't work here, because there isn't
// enough guarantees on alignment and it can't seem to decode the
// constant stride.
template<class Indexable, typename SumType>
ATTRIBUTE_NO_SANITIZE_INTEGER
SumType FastSum(const Indexable& data, size_t n) {
  SumType sums[4] = {0, 0, 0, 0};
  size_t rem = n;
  int i = 0;
  for (; rem >= 4; rem -= 4) {
    sums[0] += data[i];
    sums[1] += data[i+1];
    sums[2] += data[i+2];
    sums[3] += data[i+3];
    i += 4;
  }
  for (; rem > 0; rem--) {
    sums[3] += data[i++];
  }
  return sums[0] + sums[1] + sums[2] + sums[3];
}

template<DataType Type>
Status TimeReadFileForArrayDataType(
    CFileIterator* iter,
    size_t* out_row_count,
    size_t* out_total_array_element_count = nullptr,
    size_t* out_total_elem_str_size = nullptr) {
  size_t row_count = 0;
  size_t null_arrays = 0;
  size_t empty_arrays = 0;
  size_t null_array_elements = 0;
  size_t total_array_elements = 0;
  size_t total_elem_str_size = 0;

  ScopedColumnBlock<Type, /*IS_ARRAY=*/true> cb(100000); // up to 100K rows
  SelectionVector sel(cb.nrows());
  ColumnMaterializationContext ctx(0, nullptr, &cb, &sel);
  ctx.SetDecoderEvalNotSupported();
  while (iter->HasNext()) {
    size_t n = cb.nrows();
    RETURN_NOT_OK(iter->CopyNextValues(&n, &ctx));
    row_count += n;
    for (size_t ri = 0; ri < n; ++ri) {
      const Slice* cell_ptr = reinterpret_cast<const Slice*>(cb.cell(ri).ptr());
      if (cell_ptr == nullptr || cell_ptr->empty()) {
        ++null_arrays;
        continue;
      }
      ArrayCellMetadataView view(cell_ptr->data(), cell_ptr->size());
      RETURN_NOT_OK(view.Init());
      if (view.empty()) {
        ++empty_arrays;
        continue;
      }

      const auto elem_num = view.elem_num();
      total_array_elements += elem_num;
      if (view.not_null_bitmap()) {
        DCHECK(view.has_nulls());
        BitmapIterator bit(view.not_null_bitmap(), elem_num);
        bool is_not_null = false;
        while (size_t elem_count = bit.Next(&is_not_null)) {
          if (!is_not_null) {
            null_array_elements += elem_count;
          }
        }
      }

      // For binary/string types, calculate the total length of strings/buffers
      // of all the array elements.
      if constexpr (Type == DataType::BINARY ||
                    Type == DataType::STRING ||
                    Type == DataType::VARCHAR) {
        const Slice* data = reinterpret_cast<const Slice*>(view.data_as(Type));
        DCHECK(data);
        const auto* bm = view.not_null_bitmap();
        for (size_t i = 0; i < elem_num; ++i, ++data) {
          if (!bm || BitmapTest(bm, i)) {
            total_elem_str_size += data->size();
          }
        }
      }
    }
    cb.memory()->Reset();
  }
  if (out_row_count) {
    *out_row_count = row_count;
  }
  if (out_total_array_element_count) {
    *out_total_array_element_count = total_array_elements;
  }
  if (out_total_elem_str_size) {
    *out_total_elem_str_size = total_elem_str_size;
  }
  LOG(INFO)<< "Row count           : " << row_count;
  LOG(INFO)<< "NULL rows/arrays    : " << null_arrays;
  LOG(INFO)<< "Empty arrays        : " << empty_arrays;
  LOG(INFO)<< "NULL array elements : " << null_array_elements;
  LOG(INFO)<< "Total array elements: " << total_array_elements;
  return Status::OK();
}

template<DataType Type, typename SumType>
Status TimeReadFileForDataType(CFileIterator* iter, size_t* count) {
  ScopedColumnBlock<Type> cb(8192);
  SelectionVector sel(cb.nrows());
  ColumnMaterializationContext ctx(0, nullptr, &cb, &sel);
  ctx.SetDecoderEvalNotSupported();
  SumType sum = 0;
  size_t row_count = 0;
  while (iter->HasNext()) {
    size_t n = cb.nrows();
    RETURN_NOT_OK(iter->CopyNextValues(&n, &ctx));
    sum += FastSum<ScopedColumnBlock<Type>, SumType>(cb, n);
    row_count += n;
    cb.memory()->Reset();
  }
  if (count) {
    *count = row_count;
  }
  LOG(INFO)<< "Sum: " << sum;
  LOG(INFO)<< "Count: " << row_count;
  return Status::OK();
}

template<DataType Type>
Status ReadBinaryFile(CFileIterator* iter, size_t* count) {
  ScopedColumnBlock<Type> cb(100);
  SelectionVector sel(cb.nrows());
  ColumnMaterializationContext ctx(0, nullptr, &cb, &sel);
  ctx.SetDecoderEvalNotSupported();
  uint64_t sum_lens = 0;
  size_t row_count = 0;
  while (iter->HasNext()) {
    size_t n = cb.nrows();
    RETURN_NOT_OK(iter->CopyNextValues(&n, &ctx));
    for (size_t i = 0; i < n; i++) {
      if (!cb.is_null(i)) {
        sum_lens += cb[i].size();
      }
    }
    row_count += n;
    cb.memory()->Reset();
  }
  if (count) {
    *count = row_count;
  }
  LOG(INFO) << "Sum of value lengths: " << sum_lens;
  LOG(INFO) << "Count: " << row_count;
  return Status::OK();
}

Status TimeReadFileForScalars(DataType data_type,
                              CFileIterator* iter,
                              size_t* count_ret);

Status TimeReadFileForArrays(DataType data_type,
                             CFileIterator* iter,
                             size_t* count_ret,
                             size_t* total_array_element_count_ret = nullptr,
                             size_t* total_elem_str_size_ret = nullptr);

Status TimeReadFile(FsManager* fs_manager,
                    const BlockId& block_id,
                    size_t* count_ret,
                    size_t* total_array_element_count_ret = nullptr,
                    size_t* total_elem_str_size_ret = nullptr);

} // namespace cfile
} // namespace kudu
