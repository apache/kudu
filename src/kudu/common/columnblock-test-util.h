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

#include <cstdint>
#include <memory>
#include <type_traits>

#include "kudu/gutil/macros.h"

#include "kudu/common/columnblock.h"
#include "kudu/common/rowblock.h"

namespace kudu {

// Utility class which allocates temporary storage for a
// dense block of column data, freeing it when it goes
// out of scope.
//
// This is more useful in test code than production code,
// since it doesn't allocate from an arena, etc.
template<DataType type, bool IS_ARRAY = false>
class ScopedColumnBlock : public ColumnBlock {
 public:
  typedef typename std::conditional<
       IS_ARRAY, Slice, typename TypeTraits<type>::cpp_type>::type cpp_type;

  explicit ScopedColumnBlock(size_t n_rows, bool allow_nulls = true)
      : ColumnBlock(IS_ARRAY ? GetArrayTypeInfo(type) : GetTypeInfo(type),
                    allow_nulls ? new uint8_t[BitmapSize(n_rows)] : nullptr,
                    new cpp_type[n_rows],
                    n_rows,
                    new RowBlockMemory),
        non_null_bitmap_buf_(non_null_bitmap_),
        data_buf_(reinterpret_cast<cpp_type*>(data_)),
        memory_buf_(memory_) {
    if (allow_nulls) {
      // All rows begin null.
      BitmapChangeBits(non_null_bitmap_, /*offset=*/ 0, n_rows, /*value=*/ false);
    }
  }

  const cpp_type& operator[](size_t idx) const {
    return data_buf_[idx];
  }

  cpp_type& operator[](size_t idx) {
    return data_buf_[idx];
  }

 private:
  std::unique_ptr<uint8_t[]> non_null_bitmap_buf_;
  std::unique_ptr<cpp_type[]> data_buf_;
  std::unique_ptr<RowBlockMemory> memory_buf_;
};

} // namespace kudu
