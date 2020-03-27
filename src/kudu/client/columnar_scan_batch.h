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
/// NOTE: this class is not thread-safe.
class KUDU_EXPORT KuduColumnarScanBatch {
 public:
  KuduColumnarScanBatch();
  ~KuduColumnarScanBatch();

  /// @return The number of rows in this batch.
  int NumRows() const;

  /// Get the raw columnar data corresponding to the column with index 'idx'.
  ///
  /// The data is in little-endian packed array format. No alignment is guaranteed.
  /// Space is reserved for all cells regardless of whether they might be null.
  /// The data stored in a null cell may or may not be zeroed.
  ///
  /// If this returns an error for a given column, then a second call for the same
  /// column has undefined results.
  ///
  /// @note The Slice returned is only valid for the lifetime of the KuduColumnarScanBatch.
  Status GetDataForColumn(int idx, Slice* data) const;

  /// Get a bitmap corresponding to the non-null status of the cells in the given column.
  ///
  /// A set bit indicates a non-null cell.
  /// If the number of rows is not a multiple of 8, the state of the trailing bits in the
  /// bitmap is undefined.
  ///
  /// It is an error to call this function on a column which is not marked as nullable
  /// in the schema.
  ///
  /// @note The Slice returned is only valid for the lifetime of the KuduColumnarScanBatch.
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
