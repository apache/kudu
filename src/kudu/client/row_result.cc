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

#include "kudu/client/row_result.h"

#include <string>

#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"

using std::string;
using strings::Substitute;

namespace kudu {
namespace client {

namespace {

inline Status FindColumn(const Schema& schema, const Slice& col_name, int* idx) {
  StringPiece sp(reinterpret_cast<const char*>(col_name.data()), col_name.size());
  *idx = schema.find_column(sp);
  if (PREDICT_FALSE(*idx == -1)) {
    return Status::NotFound("No such column", col_name);
  }
  return Status::OK();
}

// Just enough of a "cell" to support the Schema::DebugCellAppend calls
// made by KuduRowResult::ToString.
class RowCell {
 public:
  RowCell(const KuduRowResult* row, int idx)
    : row_(row),
      col_idx_(idx) {
  }

  bool is_null() const {
    return row_->IsNull(col_idx_);
  }
  const void* ptr() const {
    return row_->cell(col_idx_);
  }

 private:
  const KuduRowResult* row_;
  const int col_idx_;
};

} // anonymous namespace

bool KuduRowResult::IsNull(int col_idx) const {
  const ColumnSchema& col = schema_->column(col_idx);
  if (!col.is_nullable()) {
    return false;
  }

  return BitmapTest(row_data_ + schema_->byte_size(), col_idx);
}

bool KuduRowResult::IsNull(const Slice& col_name) const {
  int col_idx;
  CHECK_OK(FindColumn(*schema_, col_name, &col_idx));
  return IsNull(col_idx);
}

Status KuduRowResult::GetBool(const Slice& col_name, bool* val) const {
  return Get<TypeTraits<BOOL> >(col_name, val);
}

Status KuduRowResult::GetInt8(const Slice& col_name, int8_t* val) const {
  return Get<TypeTraits<INT8> >(col_name, val);
}

Status KuduRowResult::GetInt16(const Slice& col_name, int16_t* val) const {
  return Get<TypeTraits<INT16> >(col_name, val);
}

Status KuduRowResult::GetInt32(const Slice& col_name, int32_t* val) const {
  return Get<TypeTraits<INT32> >(col_name, val);
}

Status KuduRowResult::GetInt64(const Slice& col_name, int64_t* val) const {
  return Get<TypeTraits<INT64> >(col_name, val);
}

Status KuduRowResult::GetTimestamp(const Slice& col_name, int64_t* val) const {
  return Get<TypeTraits<TIMESTAMP> >(col_name, val);
}

Status KuduRowResult::GetFloat(const Slice& col_name, float* val) const {
  return Get<TypeTraits<FLOAT> >(col_name, val);
}

Status KuduRowResult::GetDouble(const Slice& col_name, double* val) const {
  return Get<TypeTraits<DOUBLE> >(col_name, val);
}

Status KuduRowResult::GetString(const Slice& col_name, Slice* val) const {
  return Get<TypeTraits<STRING> >(col_name, val);
}

Status KuduRowResult::GetBinary(const Slice& col_name, Slice* val) const {
  return Get<TypeTraits<BINARY> >(col_name, val);
}

Status KuduRowResult::GetBool(int col_idx, bool* val) const {
  return Get<TypeTraits<BOOL> >(col_idx, val);
}

Status KuduRowResult::GetInt8(int col_idx, int8_t* val) const {
  return Get<TypeTraits<INT8> >(col_idx, val);
}

Status KuduRowResult::GetInt16(int col_idx, int16_t* val) const {
  return Get<TypeTraits<INT16> >(col_idx, val);
}

Status KuduRowResult::GetInt32(int col_idx, int32_t* val) const {
  return Get<TypeTraits<INT32> >(col_idx, val);
}

Status KuduRowResult::GetInt64(int col_idx, int64_t* val) const {
  return Get<TypeTraits<INT64> >(col_idx, val);
}

Status KuduRowResult::GetTimestamp(int col_idx, int64_t* val) const {
  return Get<TypeTraits<TIMESTAMP> >(col_idx, val);
}

Status KuduRowResult::GetFloat(int col_idx, float* val) const {
  return Get<TypeTraits<FLOAT> >(col_idx, val);
}

Status KuduRowResult::GetDouble(int col_idx, double* val) const {
  return Get<TypeTraits<DOUBLE> >(col_idx, val);
}

Status KuduRowResult::GetString(int col_idx, Slice* val) const {
  return Get<TypeTraits<STRING> >(col_idx, val);
}

Status KuduRowResult::GetBinary(int col_idx, Slice* val) const {
  return Get<TypeTraits<BINARY> >(col_idx, val);
}

template<typename T>
Status KuduRowResult::Get(const Slice& col_name, typename T::cpp_type* val) const {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));
  return Get<T>(col_idx, val);
}

template<typename T>
Status KuduRowResult::Get(int col_idx, typename T::cpp_type* val) const {
  const ColumnSchema& col = schema_->column(col_idx);
  if (PREDICT_FALSE(col.type_info()->type() != T::type)) {
    // TODO: at some point we could allow type coercion here.
    return Status::InvalidArgument(
        Substitute("invalid type $0 provided for column '$1' (expected $2)",
                   T::name(),
                   col.name(), col.type_info()->name()));
  }

  if (col.is_nullable() && IsNull(col_idx)) {
    return Status::NotFound("column is NULL");
  }

  memcpy(val, row_data_ + schema_->column_offset(col_idx), sizeof(*val));
  return Status::OK();
}

const void* KuduRowResult::cell(int col_idx) const {
  return row_data_ + schema_->column_offset(col_idx);
}

//------------------------------------------------------------
// Template instantiations: We instantiate all possible templates to avoid linker issues.
// see: https://isocpp.org/wiki/faq/templates#separate-template-fn-defn-from-decl
// TODO We can probably remove this when we move to c++11 and can use "extern template"
//------------------------------------------------------------

template
Status KuduRowResult::Get<TypeTraits<BOOL> >(const Slice& col_name, bool* val) const;

template
Status KuduRowResult::Get<TypeTraits<INT8> >(const Slice& col_name, int8_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<INT16> >(const Slice& col_name, int16_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<INT32> >(const Slice& col_name, int32_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<INT64> >(const Slice& col_name, int64_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<TIMESTAMP> >(const Slice& col_name, int64_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<FLOAT> >(const Slice& col_name, float* val) const;

template
Status KuduRowResult::Get<TypeTraits<DOUBLE> >(const Slice& col_name, double* val) const;

template
Status KuduRowResult::Get<TypeTraits<STRING> >(const Slice& col_name, Slice* val) const;

template
Status KuduRowResult::Get<TypeTraits<BINARY> >(const Slice& col_name, Slice* val) const;

template
Status KuduRowResult::Get<TypeTraits<BOOL> >(int col_idx, bool* val) const;

template
Status KuduRowResult::Get<TypeTraits<INT8> >(int col_idx, int8_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<INT16> >(int col_idx, int16_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<INT32> >(int col_idx, int32_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<INT64> >(int col_idx, int64_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<TIMESTAMP> >(int col_idx, int64_t* val) const;

template
Status KuduRowResult::Get<TypeTraits<FLOAT> >(int col_idx, float* val) const;

template
Status KuduRowResult::Get<TypeTraits<DOUBLE> >(int col_idx, double* val) const;

template
Status KuduRowResult::Get<TypeTraits<STRING> >(int col_idx, Slice* val) const;

template
Status KuduRowResult::Get<TypeTraits<BINARY> >(int col_idx, Slice* val) const;

string KuduRowResult::ToString() const {
  string ret;
  ret.append("(");
  bool first = true;
  for (int i = 0; i < schema_->num_columns(); i++) {
    if (!first) {
      ret.append(", ");
    }
    RowCell cell(this, i);
    schema_->column(i).DebugCellAppend(cell, &ret);
    first = false;
  }
  ret.append(")");
  return ret;
}

} // namespace client
} // namespace kudu
