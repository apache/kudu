// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/common/row_util.h"

#include "kudu/common/partial_row.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/slice.h"

namespace kudu {

using std::string;
using strings::Substitute;
using strings::SubstituteAndAppend;

namespace {

// Helper to stringify any numerical value.
template<class Type>
inline string Stringify(Type value) {
  return Substitute("$0", value);
}

} // anonymous namespace

string DebugPartialRowToString(const KuduPartialRow& row) {
  string out;
  const Schema* schema = row.schema();
  size_t cols = schema->num_columns();
  for (int col_idx = 0; col_idx < cols; col_idx++) {
    if (row.IsColumnSet(col_idx)) {
      if (!out.empty()) out += ", ";
      SubstituteAndAppend(&out, "$0 $1 = $2",
                          schema->column(col_idx).type_info()->name(),
                          schema->column(col_idx).name(),
                          DebugColumnValueToString(row, col_idx));
    }
  }
  return out;
}

string DebugColumnValueToString(const KuduPartialRow& row,
                                int col_idx) {
  const Schema* schema = row.schema();
  DataType type = schema->column(col_idx).type_info()->type();
  switch (type) {
    case UINT8: {
      uint8_t val;
      CHECK_OK(row.GetUInt8(col_idx, &val));
      return Stringify(val);
    }
    case INT8: {
      int8_t val;
      CHECK_OK(row.GetInt8(col_idx, &val));
      return Stringify(val);
    }
    case UINT16: {
      uint16_t val;
      CHECK_OK(row.GetUInt16(col_idx, &val));
      return Stringify(val);
    }
    case INT16: {
      int16_t val;
      CHECK_OK(row.GetInt16(col_idx, &val));
      return Stringify(val);
    }
    case UINT32: {
      uint32_t val;
      CHECK_OK(row.GetUInt32(col_idx, &val));
      return Stringify(val);
    }
    case INT32: {
      int32_t val;
      CHECK_OK(row.GetInt32(col_idx, &val));
      return Stringify(val);
    }
    case UINT64: {
      uint64_t val;
      CHECK_OK(row.GetUInt64(col_idx, &val));
      return Stringify(val);
    }
    case INT64: {
      int64_t val;
      CHECK_OK(row.GetInt64(col_idx, &val));
      return Stringify(val);
    }
    case STRING: {
      Slice val;
      CHECK_OK(row.GetString(col_idx, &val));
      return val.ToString();
    }
    case BOOL: {
      bool val;
      CHECK_OK(row.GetBool(col_idx, &val));
      return Stringify(val);
    }
    default:
      LOG(FATAL) << "Unknown type: " << type;
  }
}

} // namespace kudu

