// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_ROW_RESULT_H
#define KUDU_CLIENT_ROW_RESULT_H

#include <stdint.h>
#include <string>

#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/port.h"
#else
#include "kudu/client/stubs.h"
#endif
#include "kudu/util/kudu_export.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class Schema;

namespace client {

// A single row result from a scan.
//
// Drawn extensively from ContiguousRow and KuduPartialRow.
class KUDU_EXPORT KuduRowResult {
 public:
  bool IsNull(const Slice& col_name) const;
  bool IsNull(int col_idx) const;

  // These getters return a bad Status if the type does not match,
  // the value is unset, or the value is NULL. Otherwise they return
  // the current set value in *val.
  Status GetBool(const Slice& col_name, bool* val) const WARN_UNUSED_RESULT;

  Status GetInt8(const Slice& col_name, int8_t* val) const WARN_UNUSED_RESULT;
  Status GetInt16(const Slice& col_name, int16_t* val) const WARN_UNUSED_RESULT;
  Status GetInt32(const Slice& col_name, int32_t* val) const WARN_UNUSED_RESULT;
  Status GetInt64(const Slice& col_name, int64_t* val) const WARN_UNUSED_RESULT;
  Status GetTimestamp(const Slice& col_name, int64_t* micros_since_utc_epoch)
    const WARN_UNUSED_RESULT;

  Status GetFloat(const Slice& col_name, float* val) const WARN_UNUSED_RESULT;
  Status GetDouble(const Slice& col_name, double* val) const WARN_UNUSED_RESULT;

  // Same as above getters, but with numeric column indexes.
  // These are faster since they avoid a hashmap lookup, so should
  // be preferred in performance-sensitive code.
  Status GetBool(int col_idx, bool* val) const WARN_UNUSED_RESULT;

  Status GetInt8(int col_idx, int8_t* val) const WARN_UNUSED_RESULT;
  Status GetInt16(int col_idx, int16_t* val) const WARN_UNUSED_RESULT;
  Status GetInt32(int col_idx, int32_t* val) const WARN_UNUSED_RESULT;
  Status GetInt64(int col_idx, int64_t* val) const WARN_UNUSED_RESULT;
  Status GetTimestamp(int col_idx, int64_t* micros_since_utc_epoch) const WARN_UNUSED_RESULT;

  Status GetFloat(int col_idx, float* val) const WARN_UNUSED_RESULT;
  Status GetDouble(int col_idx, double* val) const WARN_UNUSED_RESULT;

  // Gets the string/binary value but does not copy the value. Callers should
  // copy the resulting Slice if necessary.
  Status GetString(const Slice& col_name, Slice* val) const WARN_UNUSED_RESULT;
  Status GetString(int col_idx, Slice* val) const WARN_UNUSED_RESULT;
  Status GetBinary(const Slice& col_name, Slice* val) const WARN_UNUSED_RESULT;
  Status GetBinary(int col_idx, Slice* val) const WARN_UNUSED_RESULT;

  // Raw cell access. Should be avoided unless absolutely necessary.
  const void* cell(int col_idx) const;

  std::string ToString() const;

 private:
  friend class KuduScanner;
  template<typename KeyTypeWrapper> friend struct SliceKeysTestSetup;
  template<typename KeyTypeWrapper> friend struct IntKeysTestSetup;

  // Only invoked by KuduScanner.
  //
  // Why not a constructor? Because we want to create a large number of row
  // results in one allocation, each with distinct state. Doing that in a
  // constructor means one allocation per result.
  void Init(const Schema* schema, const uint8_t* row_data) {
    schema_ = schema;
    row_data_ = row_data;
  }

  template<typename T>
  Status Get(const Slice& col_name, typename T::cpp_type* val) const;

  template<typename T>
  Status Get(int col_idx, typename T::cpp_type* val) const;

  const Schema* schema_;
  const uint8_t* row_data_;
};

} // namespace client
} // namespace kudu
#endif
