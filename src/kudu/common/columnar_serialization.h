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
#include <utility>
#include <vector>

#include <boost/optional/optional.hpp>

#include "kudu/util/faststring.h"

namespace kudu {

class RowBlock;
class Schema;

// A pending batch of serialized rows, suitable for easy conversion
// into the protobuf representation and a set of sidecars.
class ColumnarSerializedBatch {
 public:
  // 'rowblock_schema': the schema of the RowBlocks that will be passed to
  //                    AddRowBlock().
  //
  // 'client_schema': the schema to be returned to the client, which may
  //                  contain a subset of columns
  //
  // 'expected_batch_size_bytes':
  //      the batch size at which the caller expects to stop adding new rows to
  //      this batch. This is is only a hint and does not affect correctness.
  ColumnarSerializedBatch(const Schema& rowblock_schema,
                          const Schema& client_schema,
                          int expected_batch_size_bytes);

  // Append the data in 'block' into this columnar batch.
  //
  // Returns the number of selected rows serialized.
  int AddRowBlock(const RowBlock& block);

  struct Column {
    // The index of the column in the schema of the RowBlocks to be appended.
    int rowblock_schema_col_idx;

    // Underlying column data.
    faststring data;

    // Data for varlen columns (those with BINARY physical type)
    boost::optional<faststring> varlen_data;

    // Each bit is set when a value is non-null
    boost::optional<faststring> non_null_bitmap;
  };

  const std::vector<Column>& columns() const {
    return columns_;
  }

  std::vector<Column> TakeColumns() && {
    return std::move(columns_);
  }

 private:
  friend class WireProtocolTest;
  std::vector<Column> columns_;
};


////////////////////////////////////////////////////////////
// Expose these internal functions for unit testing.
// Do not call them outside of tests!
// See .cc file for docs.
////////////////////////////////////////////////////////////
namespace internal {
void ZeroNullValues(int sizeof_type,
                    int dst_idx,
                    int n_rows,
                    uint8_t* dst_values_buf,
                    uint8_t* non_null_bitmap);

void CopyNonNullBitmap(const uint8_t* non_null_bitmap,
                       const uint8_t* sel_bitmap,
                       int dst_idx,
                       int n_rows,
                       uint8_t* dst_non_null_bitmap);

void CopySelectedRows(const std::vector<uint16_t>& sel_rows,
                      int sizeof_type,
                      const uint8_t* __restrict__ src_buf,
                      uint8_t* __restrict__ dst_buf);


enum class PextMethod {
#ifdef __x86_64__
  kPextInstruction,
  kClmul,
#endif
  kSimple
};

extern PextMethod g_pext_method;

std::vector<PextMethod> GetAvailablePextMethods();

} // namespace internal
} // namespace kudu
