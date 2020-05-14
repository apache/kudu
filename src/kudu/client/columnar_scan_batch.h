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
#ifndef KUDU_CLIENT_COLUMNAR_SCAN_BATCH_H
#define KUDU_CLIENT_COLUMNAR_SCAN_BATCH_H

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#else
#include "kudu/client/stubs.h"
#endif

#include "kudu/util/kudu_export.h"
#include "kudu/util/status.h"

namespace kudu {
class Slice;

namespace client {

/// @brief A batch of columnar data returned from a scanner
///
/// Similar to KuduScanBatch, this contains a batch of rows returned from a scanner.
/// This type of batch is used if the KuduScanner::COLUMNAR_LAYOUT row format flag
/// is enabled.
///
/// Retrieving rows in columnar layout can be significantly more efficient. It saves
/// some CPU cycles on the Kudu cluster and can also enable faster processing of the
/// returned data in certain client applications.
///
/// The columnar data retrieved by this class matches the columnar encoding described by
/// Apache Arrow[1], but without the alignment and padding guarantees that are made by
/// the Arrow IPC serialization.
///
/// [1] https://arrow.apache.org/docs/format/Columnar.html
///
/// NOTE: this class is not thread-safe.
class KUDU_EXPORT KuduColumnarScanBatch {
 public:
  KuduColumnarScanBatch();
  ~KuduColumnarScanBatch();

  /// @return The number of rows in this batch.
  int NumRows() const;

  /// Get the raw columnar data corresponding to the primitive-typed column with index 'idx'.
  ///
  /// For variable-length (e.g. STRING, BINARY, VARCHAR) columns, use
  /// GetVariableLengthColumn instead.
  ///
  /// @note The Slice returned is only valid for the lifetime of the KuduColumnarScanBatch.
  ///
  /// @param [in] idx
  ///   The column index.
  /// @param [out] data
  ///   The data is in little-endian packed array format. No alignment or padding is guaranteed.
  ///   Space is reserved for all cells regardless of whether they might be null.
  ///   The data stored in a null cell may or may not be zeroed.
  /// @return Operation result status.
  Status GetFixedLengthColumn(int idx, Slice* data) const;

  /// Return the variable-length data for the variable-length-typed column with index 'idx'.
  ///
  /// @param [in] idx
  ///   The column index.
  /// @param [out] offsets
  ///   If NumRows() is 0, the 'offsets' array will have length 0. Otherwise, this array
  ///   will contain NumRows() + 1 entries, each indicating an offset within the
  ///   variable-length data array returned in 'data'. For each cell with index 'n',
  ///   offsets[n] indicates the starting offset of that cell, and offsets[n+1] indicates
  ///   the ending offset of that cell.
  /// @param [out] data
  ///   The variable-length data.
  /// @return Operation result status.
  Status GetVariableLengthColumn(int idx, Slice* offsets, Slice* data) const;

  /// Get a bitmap corresponding to the non-null status of the cells in the given column.
  ///
  /// It is an error to call this function on a column which is not marked as nullable
  /// in the schema.
  ///
  /// @note The Slice returned is only valid for the lifetime of the KuduColumnarScanBatch.
  ///
  /// @param [in] idx
  ///   The column index.
  /// @param [out] data
  ///   The bitmap corresponding to the non-null status of the cells in the given column.
  ///   A set bit indicates a non-null cell.
  ///   If the number of rows is not a multiple of 8, the state of the trailing bits in the
  ///   bitmap is undefined.
  /// @return Operation result status.
  Status GetNonNullBitmapForColumn(int idx, Slice* data) const;

 private:
  class KUDU_NO_EXPORT Data;

  friend class KuduScanner;

  Data* data_;
  DISALLOW_COPY_AND_ASSIGN(KuduColumnarScanBatch);
};


} // namespace client
} // namespace kudu

#endif
