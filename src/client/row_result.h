// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_ROW_RESULT_H
#define KUDU_CLIENT_ROW_RESULT_H

#include <glog/logging.h>
#include <string>

#include "common/schema.h"
#include "common/types.h"
#include "gutil/port.h"
#include "util/slice.h"
#include "util/status.h"

namespace kudu {
namespace client {

// A single row result from a scan.
//
// Drawn extensively from ContiguousRow and PartialRow.
class KuduRowResult {
 public:
  bool IsNull(const Slice& col_name) const;
  bool IsNull(int col_idx) const;

  // These getters return a bad Status if the type does not match,
  // the value is unset, or the value is NULL. Otherwise they return
  // the current set value in *val.
  Status GetInt8(const Slice& col_name, int8_t* val) const WARN_UNUSED_RESULT;
  Status GetInt16(const Slice& col_name, int16_t* val) const WARN_UNUSED_RESULT;
  Status GetInt32(const Slice& col_name, int32_t* val) const WARN_UNUSED_RESULT;
  Status GetInt64(const Slice& col_name, int64_t* val) const WARN_UNUSED_RESULT;

  Status GetUInt8(const Slice& col_name, uint8_t* val) const WARN_UNUSED_RESULT;
  Status GetUInt16(const Slice& col_name, uint16_t* val) const WARN_UNUSED_RESULT;
  Status GetUInt32(const Slice& col_name, uint32_t* val) const WARN_UNUSED_RESULT;
  Status GetUInt64(const Slice& col_name, uint64_t* val) const WARN_UNUSED_RESULT;

  // Same as above getters, but with numeric column indexes.
  // These are faster since they avoid a hashmap lookup, so should
  // be preferred in performance-sensitive code.
  Status GetInt8(int col_idx, int8_t* val) const WARN_UNUSED_RESULT;
  Status GetInt16(int col_idx, int16_t* val) const WARN_UNUSED_RESULT;
  Status GetInt32(int col_idx, int32_t* val) const WARN_UNUSED_RESULT;
  Status GetInt64(int col_idx, int64_t* val) const WARN_UNUSED_RESULT;

  Status GetUInt8(int col_idx, uint8_t* val) const WARN_UNUSED_RESULT;
  Status GetUInt16(int col_idx, uint16_t* val) const WARN_UNUSED_RESULT;
  Status GetUInt32(int col_idx, uint32_t* val) const WARN_UNUSED_RESULT;
  Status GetUInt64(int col_idx, uint64_t* val) const WARN_UNUSED_RESULT;

  // Gets the string but does not copy the value. Callers should
  // copy the resulting Slice if necessary.
  Status GetString(const Slice& col_name, Slice* val) const WARN_UNUSED_RESULT;
  Status GetString(int col_idx, Slice* val) const WARN_UNUSED_RESULT;

  // Raw cell access. Should be avoided unless absolutely necessary.
  const void* cell(int col_idx) const;

  std::string ToString() const;

 private:
  friend class KuduScanner;

  // Only invoked by KuduScanner.
  //
  // Why not a constructor? Because we want to create a large number of row
  // results in one allocation, each with distinct state. Doing that in a
  // constructor means one allocation per result.
  void Init(const Schema* schema, const uint8_t* row_data) {
    schema_ = schema;
    row_data_ = row_data;
  }

  template<DataType TYPE>
  Status Get(const Slice& col_name,
             typename DataTypeTraits<TYPE>::cpp_type* val) const;

  template<DataType TYPE>
  Status Get(int col_idx,
             typename DataTypeTraits<TYPE>::cpp_type* val) const;

  const Schema* schema_;
  const uint8_t* row_data_;
};

} // namespace client
} // namespace kudu
#endif
