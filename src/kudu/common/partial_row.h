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
#ifndef KUDU_COMMON_PARTIAL_ROW_H
#define KUDU_COMMON_PARTIAL_ROW_H

// NOTE: using stdint.h instead of cstdint because this file is supposed
//       to be processed by a compiler lacking C++11 support.
#include <stdint.h>

#include <string>

#ifdef KUDU_HEADERS_NO_STUBS
#include <gtest/gtest_prod.h>

#include "kudu/gutil/port.h"
#else
// This is a poor module interdependency, but the stubs are header-only and
// it's only for exported header builds, so we'll make an exception.
#include "kudu/client/stubs.h"
#endif

#include "kudu/util/int128.h"
#include "kudu/util/kudu_export.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

/// @cond
namespace kudu {
class ColumnSchema;
namespace client {
class KuduWriteOperation;
template<typename KeyTypeWrapper> struct SliceKeysTestSetup;// IWYU pragma: keep
template<typename KeyTypeWrapper> struct IntKeysTestSetup;  // IWYU pragma: keep
} // namespace client

namespace tablet {
  template<typename KeyTypeWrapper> struct SliceTypeRowOps; // IWYU pragma: keep
  template<typename KeyTypeWrapper> struct NumTypeRowOps;   // IWYU pragma: keep
} // namespace tablet

namespace tools {
class TableScanner;
} // namespace tools

/// @endcond

class Schema;

/// @brief A row which may only contain values for a subset of the columns.
///
/// This object contains a normal contiguous row, plus a bitfield indicating
/// which columns have been set. Additionally, this type may optionally own
/// copies of indirect data for variable length columns.
class KUDU_EXPORT KuduPartialRow {
 public:
  /// @param [in] schema
  ///   Schema to use for the row. The given Schema object must remain valid
  ///   for the lifetime of this row.
  explicit KuduPartialRow(const Schema* schema);

  virtual ~KuduPartialRow();

  /// Create a copy of KuduPartialRow instance.
  ///
  /// @param [in] other
  ///   KuduPartialRow instance to copy from.
  KuduPartialRow(const KuduPartialRow& other);

  /// Overwrite this KuduPartialRow instance with data from other instance.
  ///
  /// @param [in] other
  ///   KuduPartialRow instance to assign from.
  /// @return Reference to the updated object.
  KuduPartialRow& operator=(KuduPartialRow other);

  /// @name Setters for integral type columns by name.
  ///
  /// Set value for a column by name.
  ///
  /// @param [in] col_name
  ///   Name of the target column.
  /// @param [in] val
  ///   The value to set.
  /// @return Operation result status.
  ///
  ///@{
  Status SetBool(const Slice& col_name, bool val) WARN_UNUSED_RESULT;

  Status SetInt8(const Slice& col_name, int8_t val) WARN_UNUSED_RESULT;
  Status SetInt16(const Slice& col_name, int16_t val) WARN_UNUSED_RESULT;
  Status SetInt32(const Slice& col_name, int32_t val) WARN_UNUSED_RESULT;
  Status SetInt64(const Slice& col_name, int64_t val) WARN_UNUSED_RESULT;
  Status SetUnixTimeMicros(const Slice& col_name,
                           int64_t micros_since_utc_epoch) WARN_UNUSED_RESULT;

  Status SetFloat(const Slice& col_name, float val) WARN_UNUSED_RESULT;
  Status SetDouble(const Slice& col_name, double val) WARN_UNUSED_RESULT;
#if KUDU_INT128_SUPPORTED
  Status SetUnscaledDecimal(const Slice& col_name, int128_t val) WARN_UNUSED_RESULT;
#endif
  ///@}

  /// @name Setters for integral type columns by index.
  ///
  /// Set value for a column by index.
  ///
  /// These setters are the same as corresponding column-name-based setters,
  /// but with numeric column indexes. These are faster since they avoid
  /// hashmap lookups, so should be preferred in performance-sensitive code
  /// (e.g. bulk loaders).
  ///
  /// @param [in] col_idx
  ///   The index of the target column.
  /// @param [in] val
  ///   The value to set.
  /// @return Operation result status.
  ///
  ///@{
  Status SetBool(int col_idx, bool val) WARN_UNUSED_RESULT;

  Status SetInt8(int col_idx, int8_t val) WARN_UNUSED_RESULT;
  Status SetInt16(int col_idx, int16_t val) WARN_UNUSED_RESULT;
  Status SetInt32(int col_idx, int32_t val) WARN_UNUSED_RESULT;
  Status SetInt64(int col_idx, int64_t val) WARN_UNUSED_RESULT;
  Status SetUnixTimeMicros(int col_idx, int64_t micros_since_utc_epoch) WARN_UNUSED_RESULT;

  Status SetFloat(int col_idx, float val) WARN_UNUSED_RESULT;
  Status SetDouble(int col_idx, double val) WARN_UNUSED_RESULT;
#if KUDU_INT128_SUPPORTED
  Status SetUnscaledDecimal(int col_idx, int128_t val) WARN_UNUSED_RESULT;
#endif
  ///@}

  /// @name Setters for binary/string columns by name (copying).
  ///
  /// Set the binary/string value for a column by name, copying the specified
  /// data immediately.
  ///
  /// @note The copying behavior is new for these methods starting Kudu 0.10.
  ///   Prior to Kudu 0.10, these methods behaved like
  ///   KuduPartialRow::SetStringNoCopy() and KuduPartialRow::SetBinaryNoCopy()
  ///   correspondingly.
  ///
  /// @param [in] col_name
  ///   Name of the target column.
  /// @param [in] val
  ///   The value to set.
  /// @return Operation result status.
  ///
  ///@{
  Status SetBinary(const Slice& col_name, const Slice& val) WARN_UNUSED_RESULT;
  Status SetString(const Slice& col_name, const Slice& val) WARN_UNUSED_RESULT;
  ///@}

  /// @name Setters for binary/string columns by index (copying).
  ///
  /// Set the binary/string value for a column by index, copying the specified
  /// data immediately.
  ///
  /// These setters are the same as the corresponding column-name-based setters,
  /// but with numeric column indexes. These are faster since they avoid
  /// hashmap lookups, so should be preferred in performance-sensitive code
  /// (e.g. bulk loaders).
  ///
  /// @note The copying behavior is new for these methods starting Kudu 0.10.
  ///   Prior to Kudu 0.10, these methods behaved like
  ///   KuduPartialRow::SetStringNoCopy() and KuduPartialRow::SetBinaryNoCopy()
  ///   correspondingly.
  ///
  /// @param [in] col_idx
  ///   The index of the target column.
  /// @param [in] val
  ///   The value to set.
  /// @return Operation result status.
  ///
  ///@{
  Status SetBinary(int col_idx, const Slice& val) WARN_UNUSED_RESULT;
  Status SetString(int col_idx, const Slice& val) WARN_UNUSED_RESULT;
  ///@}

  /// @name Setters for binary/string columns by name (copying).
  ///
  /// Set the binary/string value for a column by name, copying the specified
  /// data immediately.
  ///
  /// @param [in] col_name
  ///   Name of the target column.
  /// @param [in] val
  ///   The value to set.
  /// @return Operation result status.
  ///
  ///@{
  Status SetBinaryCopy(const Slice& col_name, const Slice& val) WARN_UNUSED_RESULT;
  Status SetStringCopy(const Slice& col_name, const Slice& val) WARN_UNUSED_RESULT;
  ///@}

  /// @name Setters for binary/string columns by index (copying).
  ///
  /// Set the binary/string value for a column by index, copying the specified
  /// data immediately.
  ///
  /// These setters are the same as the corresponding column-name-based setters,
  /// but with numeric column indexes. These are faster since they avoid
  /// hashmap lookups, so should be preferred in performance-sensitive code
  /// (e.g. bulk loaders).
  ///
  /// @param [in] col_idx
  ///   The index of the target column.
  /// @param [in] val
  ///   The value to set.
  /// @return Operation result status.
  ///
  ///@{
  Status SetStringCopy(int col_idx, const Slice& val) WARN_UNUSED_RESULT;
  Status SetBinaryCopy(int col_idx, const Slice& val) WARN_UNUSED_RESULT;
  ///@}

  /// @name Setters for binary/string columns by name (non-copying).
  ///
  /// Set the binary/string value for a column by name, not copying the
  /// specified data.
  ///
  /// @note The specified data must remain valid until the corresponding
  ///   RPC calls are completed to be able to access error buffers,
  ///   if any errors happened (the errors can be fetched using the
  ///   KuduSession::GetPendingErrors() method).
  ///
  /// @param [in] col_name
  ///   Name of the target column.
  /// @param [in] val
  ///   The value to set.
  /// @return Operation result status.
  ///
  ///@{
  Status SetBinaryNoCopy(const Slice& col_name, const Slice& val) WARN_UNUSED_RESULT;
  Status SetStringNoCopy(const Slice& col_name, const Slice& val) WARN_UNUSED_RESULT;
  ///@}

  /// @name Setters for binary/string columns by index (non-copying).
  ///
  /// Set the binary/string value for a column by index, not copying the
  /// specified data.
  ///
  /// These setters are the same as the corresponding column-name-based setters,
  /// but with numeric column indexes. These are faster since they avoid
  /// hashmap lookups, so should be preferred in performance-sensitive code
  /// (e.g. bulk loaders).
  ///
  /// @note The specified data must remain valid until the corresponding
  ///   RPC calls are completed to be able to access error buffers,
  ///   if any errors happened (the errors can be fetched using the
  ///   KuduSession::GetPendingErrors() method).
  ///
  /// @param [in] col_idx
  ///   The index of the target column.
  /// @param [in] val
  ///   The value to set.
  /// @return Operation result status.
  ///
  ///@{
  Status SetBinaryNoCopy(int col_idx, const Slice& val) WARN_UNUSED_RESULT;
  Status SetStringNoCopy(int col_idx, const Slice& val) WARN_UNUSED_RESULT;
  ///@}

  /// Set column value to @c NULL; the column is identified by its name.
  ///
  /// This will only succeed on nullable columns. Use Unset() to restore
  /// column value to its default.
  ///
  /// @param [in] col_name
  ///   Name of the target column.
  /// @return Operation result status.
  Status SetNull(const Slice& col_name) WARN_UNUSED_RESULT;

  /// Set column value to @c NULL; the column is identified by its index.
  ///
  /// This will only succeed on nullable columns. Use Unset() to restore
  /// column value to its default.
  ///
  /// @param [in] col_idx
  ///   The index of the target column.
  /// @return Operation result status.
  Status SetNull(int col_idx) WARN_UNUSED_RESULT;

  /// Unset the given column by name, restoring its default value.
  ///
  /// @note This is different from setting it to @c NULL.
  ///
  /// @param [in] col_name
  ///   Name of the target column.
  /// @return Operation result status.
  Status Unset(const Slice& col_name) WARN_UNUSED_RESULT;

  /// Unset the given column by index, restoring its default value.
  ///
  /// @note This is different from setting it to @c NULL.
  ///
  /// @param [in] col_idx
  ///   The index of the target column.
  /// @return Operation result status.
  Status Unset(int col_idx) WARN_UNUSED_RESULT;

  /// Check whether the specified column is set for the row.
  ///
  /// @param [in] col_name
  ///   Name of the column.
  /// @return @c true iff the given column has been specified.
  bool IsColumnSet(const Slice& col_name) const;

  /// Check whether the specified column is set for the row.
  ///
  /// @param [in] col_idx
  ///   The index of the column.
  /// @return @c true iff the given column has been specified.
  bool IsColumnSet(int col_idx) const;

  /// Check whether the specified column is @c NULL for the row.
  ///
  /// @param [in] col_name
  ///   Name of the target column.
  /// @return @c true iff the given column's value is @c NULL.
  bool IsNull(const Slice& col_name) const;

  /// Check whether the specified column is @c NULL for the row.
  ///
  /// @param [in] col_idx
  ///   The index of the column.
  /// @return @c true iff the given column's value is @c NULL.
  bool IsNull(int col_idx) const;

  /// @name Getters for integral type columns by column name.
  ///
  /// Get value of the column specified by name.
  ///
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
  Status GetUnixTimeMicros(const Slice& col_name,
                      int64_t* micros_since_utc_epoch) const WARN_UNUSED_RESULT;

  Status GetFloat(const Slice& col_name, float* val) const WARN_UNUSED_RESULT;
  Status GetDouble(const Slice& col_name, double* val) const WARN_UNUSED_RESULT;
#if KUDU_INT128_SUPPORTED
  // NOTE: The non-const version of this function is kept for backwards compatibility.
  Status GetUnscaledDecimal(const Slice& col_name, int128_t* val) WARN_UNUSED_RESULT;
  Status GetUnscaledDecimal(const Slice& col_name, int128_t* val) const WARN_UNUSED_RESULT;
#endif
  ///@}

  /// @name Getters for column of integral type by column index.
  ///
  /// Get value of a column of integral type by column index.
  ///
  /// These getters are the same as the corresponding column-name-based getters,
  /// but with numeric column indexes. These are faster since they avoid
  /// hashmap lookups, so should be preferred in performance-sensitive code
  /// (e.g. bulk loaders).
  ///
  /// @param [in] col_idx
  ///   The index of the target column.
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
  // NOTE: The non-const version of this function is kept for backwards compatibility.
  Status GetUnscaledDecimal(int col_idx, int128_t* val) WARN_UNUSED_RESULT;
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
  /// since they use indices to avoid hashmap lookups, so index-based getters
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

  //------------------------------------------------------------
  // Key-encoding related functions
  //------------------------------------------------------------

  /// Encode a row key.
  ///
  /// The result is suitable for use as a tablet split key, an encoded
  /// key range, etc.
  ///
  /// @pre All of the key columns must be set.
  ///
  /// @param [out] encoded_key
  ///   The encoded key (i.e. the result of the encoding).
  /// @return Operation result status. In particular, this method returns
  ///   InvalidArgument if not all the key columns are set.
  Status EncodeRowKey(std::string* encoded_key) const;

  /// Convenience method which is similar to EncodeRowKey.
  ///
  /// This is equivalent to the EncodeRowKey, but triggers a FATAL error
  /// on failure.
  ///
  /// @return The encoded key.
  std::string ToEncodedRowKeyOrDie() const;

  //------------------------------------------------------------
  // Utility code
  //------------------------------------------------------------

  /// @return @c true if all key column values have been set
  ///   for this mutation.
  bool IsKeySet() const;

  /// @return @c true if all column values have been set.
  bool AllColumnsSet() const;

  /// @return String representation for the partial row.
  ///
  /// @internal
  /// @note this method does note redact row values. The
  ///   caller must handle redaction themselves, if necessary.
  std::string ToString() const;

  /// @return The schema object for the partial row.
  const Schema* schema() const { return schema_; }

 private:
  friend class client::KuduWriteOperation;   // for row_data_.
  friend class KeyUtilTest;
  friend class PartitionSchema;
  friend class RowOperationsPBDecoder;
  friend class RowOperationsPBEncoder;
  friend class tools::TableScanner;
  friend class TestScanSpec;
  template<typename KeyTypeWrapper> friend struct client::SliceKeysTestSetup;
  template<typename KeyTypeWrapper> friend struct client::IntKeysTestSetup;
  template<typename KeyTypeWrapper> friend struct tablet::SliceTypeRowOps;
  template<typename KeyTypeWrapper> friend struct tablet::NumTypeRowOps;
  FRIEND_TEST(KeyUtilTest, TestIncrementInt128PrimaryKey);
  FRIEND_TEST(PartitionPrunerTest, TestIntPartialPrimaryKeyRangePruning);
  FRIEND_TEST(PartitionPrunerTest, TestPartialPrimaryKeyRangePruning);
  FRIEND_TEST(PartitionPrunerTest, TestPrimaryKeyRangePruning);

  template<typename T>
  Status Set(const Slice& col_name, const typename T::cpp_type& val,
             bool owned = false);

  template<typename T>
  Status Set(int col_idx, const typename T::cpp_type& val,
             bool owned = false);

  // Runtime version of the generic setter.
  Status Set(int32_t column_idx, const uint8_t* val);

  template<typename T>
  Status Get(const Slice& col_name, typename T::cpp_type* val) const;

  template<typename T>
  Status Get(int col_idx, typename T::cpp_type* val) const;

  template<typename T>
  Status SetSliceCopy(const Slice& col_name, const Slice& val);

  template<typename T>
  Status SetSliceCopy(int col_idx, const Slice& val);

  // If the given column is a variable length column whose memory is owned by this instance,
  // deallocates the value.
  // NOTE: Does not mutate the isset bitmap.
  // REQUIRES: col_idx must be a variable length column.
  void DeallocateStringIfSet(int col_idx, const ColumnSchema& col);

  // Deallocate any string/binary values whose memory is managed by this object.
  void DeallocateOwnedStrings();

  const Schema* schema_;

  // 1-bit set for any field which has been explicitly set. This is distinct
  // from NULL -- an "unset" field will take the server-side default on insert,
  // whereas a field explicitly set to NULL will override the default.
  uint8_t* isset_bitmap_;

  // 1-bit set for any variable length columns whose memory is managed by this instance.
  // These strings need to be deallocated whenever the value is reset,
  // or when the instance is destructed.
  uint8_t* owned_strings_bitmap_;

  // The normal "contiguous row" format row data. Any column whose data is unset
  // or NULL can have undefined bytes.
  uint8_t* row_data_;
};

} // namespace kudu
#endif /* KUDU_COMMON_PARTIAL_ROW_H */
