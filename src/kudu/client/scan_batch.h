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
#ifndef KUDU_CLIENT_SCAN_BATCH_H
#define KUDU_CLIENT_SCAN_BATCH_H

// NOTE: using stdint.h instead of cstdint because this file is supposed
//       to be processed by a compiler lacking C++11 support.
#include <stdint.h>

#include <cstddef>
#include <iterator>
#include <string>

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif

#include "kudu/util/int128.h"
#include "kudu/util/kudu_export.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {
class Schema;

namespace tools {
class ReplicaDumper;
} // namespace tools

namespace client {
class KuduSchema;

/// @brief A batch of zero or more rows returned by a scan operation.
///
/// Every call to KuduScanner::NextBatch() returns a batch of zero or more rows.
/// You can iterate over the rows in the batch using:
///
/// range-foreach loop (C++11):
/// @code
///   for (KuduScanBatch::RowPtr row : batch) {
///     ... row.GetInt(1, ...)
///     ...
///   }
/// @endcode
///
/// regular for loop (C++03):
/// @code
///   for (KuduScanBatch::const_iterator it = batch.begin(), it != batch.end();
///        ++it) {
///     KuduScanBatch::RowPtr row(*it);
///     ...
///   }
/// @endcode
/// or
/// @code
///   for (int i = 0, num_rows = batch.NumRows();
///        i < num_rows;
///        i++) {
///     KuduScanBatch::RowPtr row = batch.Row(i);
///     ...
///   }
/// @endcode
///
/// @note In the above example, NumRows() is only called once at the
///   beginning of the loop to avoid extra calls to the non-inlined method.
class KUDU_EXPORT KuduScanBatch {
 public:
  /// @brief A single row result from a scan.
  ///
  /// @note This object acts as a pointer into a KuduScanBatch, and therefore
  ///   is valid only as long as the batch it was constructed from.
  class RowPtr;

  /// @brief C++ forward iterator over the rows in a KuduScanBatch.
  ///
  /// This iterator yields KuduScanBatch::RowPtr objects which point inside
  /// the row batch itself. Thus, the iterator and any objects obtained from it
  /// are invalidated if the KuduScanBatch is destroyed or used
  /// for a new NextBatch() call.
  class const_iterator;

  /// A handy typedef for the RowPtr.
  typedef RowPtr value_type;

  KuduScanBatch();
  ~KuduScanBatch();

  /// @return The number of rows in this batch.
  int NumRows() const;

  /// Get a row at the specified index.
  ///
  /// @param [in] idx
  ///   The index of the row to return.
  /// @return A reference to one of the rows in this batch.
  ///   The returned object is only valid for as long as this KuduScanBatch
  ///   object is valid.
  KuduScanBatch::RowPtr Row(int idx) const;

  /// @return Forward iterator to the start of the rows in the batch.
  const_iterator begin() const;
  /// @return Forward iterator to the end of the rows in the batch.
  const_iterator end() const;

  /// @return The projection schema for this batch.
  ///   All KuduScanBatch::RowPtr returned by this batch are guaranteed
  ///   to have this schema.
  const KuduSchema* projection_schema() const;

  /// @name Advanced/Unstable API
  ///
  /// There are no guarantees on the stability of the format returned
  /// by these methods, which might change at any given time.
  ///
  /// @note The Slices returned by both direct_data() and indirect_data()
  ///   are only valid for the lifetime of the KuduScanBatch.
  //
  ///@{
  /// Return a slice that points to the direct row data received from the
  /// server. Users of this API must have knowledge of the data format in
  /// order to decode the data.
  ///
  /// @return a Slice that points to the raw direct row data.
  Slice direct_data() const;

  /// Like the method above, but for indirect data.
  ///
  /// @return a Slice that points to the raw indirect row data.
  Slice indirect_data() const;
  ///@}

 private:
  class KUDU_NO_EXPORT Data;
  friend class KuduScanner;
  friend class tools::ReplicaDumper;

  Data* data_;
  DISALLOW_COPY_AND_ASSIGN(KuduScanBatch);
};

class KUDU_EXPORT KuduScanBatch::RowPtr {
 public:
  /// Construct an invalid RowPtr. Before use, you must assign
  /// a properly-initialized value.
  RowPtr() : schema_(NULL), row_data_(NULL) {}

  /// @param [in] col_name
  ///   Name of the column.
  /// @return @c true iff the specified column of the row has @c NULL value.
  bool IsNull(const Slice& col_name) const;

  /// @param [in] col_idx
  ///   Index of the column.
  /// @return @c true iff the specified column of the row has @c NULL value.
  bool IsNull(int col_idx) const;

  /// @name Getters for integral type columns by column name.
  ///
  /// @param [in] col_name
  ///   The name of the target column.
  /// @param [out] val
  ///   Placeholder for the result value.
  /// @return Operation result status. Return a bad Status if at least one
  ///   of the following is @c true:
  ///     @li The type does not match.
  ///     @li The value is unset.
  ///     @li The value is @c NULL.
  ///
  ///@{
  Status GetBool(const Slice& col_name, bool* val) const WARN_UNUSED_RESULT;

  Status GetInt8(const Slice& col_name, int8_t* val) const WARN_UNUSED_RESULT;
  Status GetInt16(const Slice& col_name, int16_t* val) const WARN_UNUSED_RESULT;
  Status GetInt32(const Slice& col_name, int32_t* val) const WARN_UNUSED_RESULT;
  Status GetInt64(const Slice& col_name, int64_t* val) const WARN_UNUSED_RESULT;
  Status GetUnixTimeMicros(const Slice& col_name, int64_t* micros_since_utc_epoch)
    const WARN_UNUSED_RESULT;

  Status GetFloat(const Slice& col_name, float* val) const WARN_UNUSED_RESULT;
  Status GetDouble(const Slice& col_name, double* val) const WARN_UNUSED_RESULT;

#if KUDU_INT128_SUPPORTED
  Status GetUnscaledDecimal(const Slice& col_name, int128_t* val) const WARN_UNUSED_RESULT;
#endif
  ///@}

  /// @name Getters for integral type columns by column index.
  ///
  /// These methods are faster than their name-based counterparts
  /// since using indices avoids a hashmap lookup, so index-based getters
  /// should be preferred in performance-sensitive code.
  ///
  /// @param [in] col_index
  ///   The index of the column.
  /// @param [out] val
  ///   Pointer to the placeholder to put the resulting value.
  ///
  /// @return Operation result status. Return a bad Status if at least one
  ///   of the following is @c true:
  ///     @li The type does not match.
  ///     @li The value is unset.
  ///     @li The value is @c NULL.
  ///
  ///@{
  Status GetBool(int col_idx, bool* val) const WARN_UNUSED_RESULT;

  Status GetInt8(int col_idx, int8_t* val) const WARN_UNUSED_RESULT;
  Status GetInt16(int col_idx, int16_t* val) const WARN_UNUSED_RESULT;
  Status GetInt32(int col_idx, int32_t* val) const WARN_UNUSED_RESULT;
  Status GetInt64(int col_idx, int64_t* val) const WARN_UNUSED_RESULT;
  Status GetUnixTimeMicros(int col_idx, int64_t* micros_since_utc_epoch) const WARN_UNUSED_RESULT;

  Status GetFloat(int col_idx, float* val) const WARN_UNUSED_RESULT;
  Status GetDouble(int col_idx, double* val) const WARN_UNUSED_RESULT;

#if KUDU_INT128_SUPPORTED
  Status GetUnscaledDecimal(int col_idx, int128_t* val) const WARN_UNUSED_RESULT;
#endif
  ///@}

  /// @name Getters for string/binary column by column name.
  ///
  /// Get the string/binary value for a column by its name.
  ///
  /// @param [in] col_name
  ///   Name of the column.
  /// @param [out] val
  ///   Pointer to the placeholder to put the resulting value.
  ///   Note that the method does not copy the value. Callers should copy
  ///   the resulting Slice if necessary.
  /// @return Operation result status. Return a bad Status if at least one
  ///   of the following is @c true:
  ///     @li The type does not match.
  ///     @li The value is unset.
  ///     @li The value is @c NULL.
  ///
  ///@{
  Status GetString(const Slice& col_name, Slice* val) const WARN_UNUSED_RESULT;
  Status GetBinary(const Slice& col_name, Slice* val) const WARN_UNUSED_RESULT;
  ///@}

  /// @name Getters for string/binary column by column index.
  ///
  /// Get the string/binary value for a column by its index.
  ///
  /// These methods are faster than their name-based counterparts
  /// since using indices avoids a hashmap lookup, so index-based getters
  /// should be preferred in performance-sensitive code.
  ///
  /// @param [in] col_index
  ///   The index of the column.
  /// @param [out] val
  ///   Pointer to the placeholder to put the resulting value.
  ///   Note that the method does not copy the value. Callers should copy
  ///   the resulting Slice if necessary.
  /// @return Operation result status. Return a bad Status if at least one
  ///   of the following is @c true:
  ///     @li The type does not match.
  ///     @li The value is unset.
  ///     @li The value is @c NULL.
  ///
  ///@{
  Status GetString(int col_idx, Slice* val) const WARN_UNUSED_RESULT;
  Status GetBinary(int col_idx, Slice* val) const WARN_UNUSED_RESULT;
  ///@}

  /// Get the column's row data.
  ///
  /// @note Should be avoided unless absolutely necessary.
  /// @param [in] col_idx
  ///   The index of the column.
  /// @return Raw cell data for the specified index.
  const void* cell(int col_idx) const;

  /// @return String representation for this row.
  std::string ToString() const;

 private:
  friend class KuduScanBatch;
  template<typename KeyTypeWrapper> friend struct SliceKeysTestSetup;
  template<typename KeyTypeWrapper> friend struct IntKeysTestSetup;

  // Only invoked by KuduScanner.
  RowPtr(const Schema* schema,
         const uint8_t* row_data)
      : schema_(schema),
        row_data_(row_data) {
  }

  template<typename T>
  Status Get(const Slice& col_name, typename T::cpp_type* val) const;

  template<typename T>
  Status Get(int col_idx, typename T::cpp_type* val) const;

  const Schema* schema_;
  const uint8_t* row_data_;
};

class KUDU_EXPORT KuduScanBatch::const_iterator
    : public std::iterator<std::forward_iterator_tag, KuduScanBatch::RowPtr> {
 public:
  ~const_iterator() {}

  /// @return The row in the batch the iterator is pointing at.
  KuduScanBatch::RowPtr operator*() const {
    return batch_->Row(idx_);
  }

  /// Prefix increment operator: advances the iterator to the next position.
  ///
  /// @return The reference to the iterator, pointing to the next position.
  const_iterator& operator++() {
    ++idx_;
    return *this;
  }

  /// Postfix increment operator: advances the iterator to the next position.
  ///
  /// @return A copy of the iterator pointing to the pre-increment position.
  const_iterator operator++(int) {
    const_iterator tmp(batch_, idx_);
    ++idx_;
    return tmp;
  }

  /// An operator to check whether two iterators are 'equal'.
  ///
  /// @param [in] other
  ///   The iterator to compare with.
  /// @return @c true iff the other iterator points to the same row
  ///   of the same batch.
  bool operator==(const const_iterator& other) const {
    return (idx_ == other.idx_) && (batch_ == other.batch_);
  }

  /// An operator to check whether two iterators are 'not equal'.
  ///
  /// @param [in] other
  ///   The iterator to compare with.
  /// @return @c true iff the other iterator points to a different row
  ///   in the same batch, or to a row belonging to a different batch
  ///   altogether.
  bool operator!=(const const_iterator& other) const {
    return !(*this == other);
  }

 private:
  friend class KuduScanBatch;
  const_iterator(const KuduScanBatch* b, int idx)
      : batch_(b),
        idx_(idx) {
  }

  const KuduScanBatch* const batch_;
  int idx_;
};


inline KuduScanBatch::const_iterator KuduScanBatch::begin() const {
  return const_iterator(this, 0);
}

inline KuduScanBatch::const_iterator KuduScanBatch::end() const {
  return const_iterator(this, NumRows());
}

} // namespace client
} // namespace kudu

#endif
