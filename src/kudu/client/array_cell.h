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

#ifndef KUDU_CLIENT_ARRAY_CELL_H
#define KUDU_CLIENT_ARRAY_CELL_H

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif

#include "kudu/client/schema.h"
#include "kudu/util/kudu_export.h"
#include "kudu/util/status.h"

namespace kudu {
namespace client {

// TODO(aserbin): add doxygen comments
class KUDU_EXPORT KuduArrayCellView {
 public:
  // cell_ptr: pointer to an array cell in scan results, returned by, e.g.,
  // by KuduScanBatch::RowPtr::cell(idx)
  explicit KuduArrayCellView(const void* cell_ptr);
  // buf: data raw pointer
  // len: size of the buffer (bytes) pointed at by the 'buf' pointer
  KuduArrayCellView(const uint8_t* buf, size_t size);
  ~KuduArrayCellView();

  // Process the input data. This method must be called once prior to calling
  // any other methods of this class.
  Status Init();

  // Number of elements in the array.
  size_t elem_num() const;

  // Whether the array cell is empty, i.e. does not contain any elements.
  bool empty() const;

  // Whether at least one element in the array is null/invalid. If !has_nulls()
  // holds true, it helps avoiding calls to 'not_null_bitmap()' and evaluating
  // non-nullness/validity per element in particular usage scenarios.
  bool has_nulls() const;

  // Get non-null (a.k.a. validity) bitmap for the array elements. This returns
  // null if has_nulls() == false.
  const uint8_t* not_null_bitmap() const;

  // Accessor for the cell's raw data in the format similar to what
  // KuduScanBatch::RowPtr::direct_data() provides
  const void* data(
      KuduColumnSchema::DataType data_type,
      const KuduColumnTypeAttributes& attributes = KuduColumnTypeAttributes()) const;

 private:
  class KUDU_NO_EXPORT Data;

  // Owned.
  Data* data_;

  DISALLOW_COPY_AND_ASSIGN(KuduArrayCellView);
};

} // namespace client
} // namespace kudu

#endif // #ifndef KUDU_CLIENT_ARRAY_CELL_H ...
