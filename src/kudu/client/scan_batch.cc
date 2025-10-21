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

#include "kudu/client/scan_batch.h"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <string>
#include <type_traits>
#include <vector>

#include <glog/logging.h>

#include "kudu/client/row_result.h"
#include "kudu/client/scanner-internal.h"
#include "kudu/common/array_cell_view.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/schema.h"
#include "kudu/common/types.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/int128.h"
#include "kudu/util/logging.h"

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace client {

////////////////////////////////////////////////////////////
// KuduScanBatch
////////////////////////////////////////////////////////////

KuduScanBatch::KuduScanBatch() : data_(new Data()) {}

KuduScanBatch::~KuduScanBatch() {
  delete data_;
}

int KuduScanBatch::NumRows() const {
  return data_->num_rows();
}

KuduRowResult KuduScanBatch::Row(int idx) const {
  return data_->row(idx);
}

const KuduSchema* KuduScanBatch::projection_schema() const {
  return data_->client_projection_;
}

Slice KuduScanBatch::direct_data() const {
  return data_->direct_data_;
}

Slice KuduScanBatch::indirect_data() const {
  return data_->indirect_data_;
}

////////////////////////////////////////////////////////////
// KuduScanBatch::RowPtr
////////////////////////////////////////////////////////////

namespace {

// Just enough of a "cell" to support the Schema::DebugCellAppend calls
// made by KuduScanBatch::RowPtr::ToString.
class RowCell {
 public:
  RowCell(const KuduScanBatch::RowPtr* row, int idx)
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
  const KuduScanBatch::RowPtr* row_;
  const int col_idx_;
};

Status BadTypeStatus(const char* provided_type_name, const ColumnSchema& col) {
  return Status::InvalidArgument(
      Substitute("invalid type $0 provided for column '$1' (expected $2)",
                 provided_type_name, col.name(), col.type_info()->name()));
}

} // anonymous namespace

bool KuduScanBatch::RowPtr::IsNull(int col_idx) const {
  const ColumnSchema& col = schema_->column(col_idx);
  if (!col.is_nullable()) {
    return false;
  }

  return BitmapTest(row_data_ + schema_->byte_size(), col_idx);
}

bool KuduScanBatch::RowPtr::IsNull(const Slice& col_name) const {
  int col_idx;
  CHECK_OK(schema_->FindColumn(col_name, &col_idx));
  return IsNull(col_idx);
}

Status KuduScanBatch::RowPtr::IsDeleted(bool* val) const {
  int col_idx = schema_->first_is_deleted_virtual_column_idx();
  if (col_idx == Schema::kColumnNotFound) {
    return Status::NotFound("IS_DELETED virtual column not found");
  }
  return Get<TypeTraits<IS_DELETED> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetBool(const Slice& col_name, bool* val) const {
  return Get<TypeTraits<BOOL> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetInt8(const Slice& col_name, int8_t* val) const {
  return Get<TypeTraits<INT8> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetInt16(const Slice& col_name, int16_t* val) const {
  return Get<TypeTraits<INT16> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetInt32(const Slice& col_name, int32_t* val) const {
  return Get<TypeTraits<INT32> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetInt64(const Slice& col_name, int64_t* val) const {
  return Get<TypeTraits<INT64> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetUnixTimeMicros(const Slice& col_name,
                                                int64_t* micros_since_utc_epoch) const {
  return Get<TypeTraits<UNIXTIME_MICROS> >(col_name, micros_since_utc_epoch);
}

Status KuduScanBatch::RowPtr::GetDate(const Slice& col_name, int32_t* days_since_unix_epoch) const {
  return Get<TypeTraits<DATE> >(col_name, days_since_unix_epoch);
}

Status KuduScanBatch::RowPtr::GetFloat(const Slice& col_name, float* val) const {
  return Get<TypeTraits<FLOAT> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetDouble(const Slice& col_name, double* val) const {
  return Get<TypeTraits<DOUBLE> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetUnscaledDecimal(const Slice& col_name, int128_t* val) const {
  int col_idx;
  RETURN_NOT_OK(schema_->FindColumn(col_name, &col_idx));
  return GetUnscaledDecimal(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetString(const Slice& col_name, Slice* val) const {
  return Get<TypeTraits<STRING> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetBinary(const Slice& col_name, Slice* val) const {
  return Get<TypeTraits<BINARY> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetVarchar(const Slice& col_name, Slice* val) const {
  return Get<TypeTraits<VARCHAR> >(col_name, val);
}

Status KuduScanBatch::RowPtr::GetBool(int col_idx, bool* val) const {
  return Get<TypeTraits<BOOL> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetInt8(int col_idx, int8_t* val) const {
  return Get<TypeTraits<INT8> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetInt16(int col_idx, int16_t* val) const {
  return Get<TypeTraits<INT16> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetInt32(int col_idx, int32_t* val) const {
  return Get<TypeTraits<INT32> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetInt64(int col_idx, int64_t* val) const {
  return Get<TypeTraits<INT64> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetUnixTimeMicros(int col_idx,
                                                int64_t* micros_since_utc_epoch) const {
  return Get<TypeTraits<UNIXTIME_MICROS> >(col_idx, micros_since_utc_epoch);
}

Status KuduScanBatch::RowPtr::GetDate(int col_idx, int32_t* days_since_unix_epoch) const {
  return Get<TypeTraits<DATE> >(col_idx, days_since_unix_epoch);
}

Status KuduScanBatch::RowPtr::GetFloat(int col_idx, float* val) const {
  return Get<TypeTraits<FLOAT> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetDouble(int col_idx, double* val) const {
  return Get<TypeTraits<DOUBLE> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetString(int col_idx, Slice* val) const {
  return Get<TypeTraits<STRING> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetBinary(int col_idx, Slice* val) const {
  return Get<TypeTraits<BINARY> >(col_idx, val);
}

Status KuduScanBatch::RowPtr::GetVarchar(int col_idx, Slice* val) const {
  return Get<TypeTraits<VARCHAR> >(col_idx, val);
}

template<typename T>
Status KuduScanBatch::RowPtr::Get(const Slice& col_name, typename T::cpp_type* val) const {
  int col_idx;
  RETURN_NOT_OK(schema_->FindColumn(col_name, &col_idx));
  return Get<T>(col_idx, val);
}

template<typename T>
Status KuduScanBatch::RowPtr::Get(int col_idx, typename T::cpp_type* val) const {
  const ColumnSchema& col = schema_->column(col_idx);
  if (PREDICT_FALSE(col.type_info()->type() != T::type)) {
    // TODO(todd): at some point we could allow type coercion here.
    // Explicitly out-of-line the construction of this Status in order to
    // keep the getter code footprint as small as possible.
    return BadTypeStatus(T::name(), col);
  }

  if (PREDICT_FALSE(col.is_nullable() && IsNull(col_idx))) {
    return Status::NotFound("column is NULL");
  }

  memcpy(val, row_data_ + schema_->column_offset(col_idx), sizeof(*val));
  return Status::OK();
}

namespace {

Status ArrayValidation(const ColumnSchema& col,
                       const char* type_name) {
  if (PREDICT_FALSE(col.type_info()->type() != NESTED)) {
    return BadTypeStatus(type_name, col);
  }
  const auto* descriptor = col.type_info()->nested_type_info();
  if (PREDICT_FALSE(!descriptor)) {
    return Status::InvalidArgument(Substitute(
        "column '$0': missing type descriptor for NESTED type", col.name()));
  }
  if (PREDICT_FALSE(!descriptor->is_array())) {
    return Status::InvalidArgument(Substitute(
        "column '$0': underlying NESTED type isn't an array", col.name()));
  }
  return Status::OK();
}

} // anonymous namespace

template<typename T>
Status KuduScanBatch::RowPtr::GetArray(const Slice& col_name,
                                       vector<typename T::cpp_type>* data_out,
                                       vector<bool>* validity_out) const {
  int col_idx;
  RETURN_NOT_OK(schema_->FindColumn(col_name, &col_idx));
  return GetArray<T>(col_idx, data_out, validity_out);
}

template<typename T>
Status KuduScanBatch::RowPtr::GetArray(int col_idx,
                                       vector<typename T::cpp_type>* data_out,
                                       vector<bool>* validity_out) const {
  const ColumnSchema& col = schema_->column(col_idx);
  RETURN_NOT_OK(ArrayValidation(col, T::name()));
  if (PREDICT_FALSE(col.is_nullable() && IsNull(col_idx))) {
    return Status::NotFound("column is NULL");
  }
  const Slice* cell_data = reinterpret_cast<const Slice*>(
      row_data_ + schema_->column_offset(col_idx));
  ArrayCellMetadataView view(cell_data->data(), cell_data->size());
  RETURN_NOT_OK(view.Init());

  const size_t elem_num = view.elem_num();
  if (data_out) {
    data_out->resize(elem_num);
    if (!view.empty()) {
      const uint8_t* data_raw = view.data_as(T::type);
      DCHECK(data_raw);
      if constexpr (std::is_same<T, TypeTraits<BOOL>>::value) {
        // Since std::vector<bool> isn't a standard vector container, the data()
        // accessor isn't available and copying the data using memcpy() isn't
        // feasible. So, copying the data element by element: it's not as
        // performant as memcpy(), but it works.
        data_out->clear();
        data_out->reserve(elem_num);
        std::copy(data_raw, data_raw + elem_num, std::back_inserter(*data_out));
      } else {
        memcpy(data_out->data(), data_raw, elem_num * sizeof(typename T::cpp_type));
      }
    }
  }
  if (validity_out) {
    validity_out->resize(elem_num);
    if (!view.empty()) {
      if (view.not_null_bitmap()) {
        *validity_out = BitmapToVector(view.not_null_bitmap(), elem_num);
      } else {
        // All elements are valid: the validity array is empty.
        validity_out->clear();
      }
    }
  }
  return Status::OK();
}

Status KuduScanBatch::RowPtr::GetArrayBool(int col_idx,
                                           vector<bool>* data,
                                           vector<bool>* validity) const {
  return GetArray<TypeTraits<BOOL>>(col_idx, data, validity);
}

Status KuduScanBatch::RowPtr::GetArrayInt8(int col_idx,
                                           vector<int8_t>* data,
                                           vector<bool>* validity) const {
  return GetArray<TypeTraits<INT8>>(col_idx, data, validity);
}

Status KuduScanBatch::RowPtr::GetArrayInt16(int col_idx,
                                            vector<int16_t>* data,
                                            vector<bool>* validity) const {
  return GetArray<TypeTraits<INT16>>(col_idx, data, validity);
}

Status KuduScanBatch::RowPtr::GetArrayInt32(int col_idx,
                                            vector<int32_t>* data,
                                            vector<bool>* validity) const {
  return GetArray<TypeTraits<INT32>>(col_idx, data, validity);
}

Status KuduScanBatch::RowPtr::GetArrayInt64(int col_idx,
                                            vector<int64_t>* data,
                                            vector<bool>* validity) const {
  return GetArray<TypeTraits<INT64>>(col_idx, data, validity);
}
Status KuduScanBatch::RowPtr::GetArrayUnscaledDecimal(int col_idx,
                                                      vector<int32_t>* data,
                                                      vector<bool>* validity) const {
  return GetArray<TypeTraits<DECIMAL32>>(col_idx, data, validity);
}
Status KuduScanBatch::RowPtr::GetArrayUnscaledDecimal(int col_idx,
                                                      vector<int64_t>* data,
                                                      vector<bool>* validity) const {
  return GetArray<TypeTraits<DECIMAL64>>(col_idx, data, validity);
}
Status KuduScanBatch::RowPtr::GetArrayUnixTimeMicros(int col_idx,
                                                     vector<int64_t>* data,
                                                     vector<bool>* validity) const {
  return GetArray<TypeTraits<UNIXTIME_MICROS>>(col_idx, data, validity);
}
Status KuduScanBatch::RowPtr::GetArrayDate(int col_idx,
                                           vector<int32_t>* data,
                                           vector<bool>* validity) const {
  return GetArray<TypeTraits<DATE>>(col_idx, data, validity);
}
Status KuduScanBatch::RowPtr::GetArrayFloat(int col_idx,
                                            vector<float>* data,
                                            vector<bool>* validity) const {
  return GetArray<TypeTraits<FLOAT>>(col_idx, data, validity);
}
Status KuduScanBatch::RowPtr::GetArrayDouble(int col_idx,
                                             vector<double>* data,
                                             vector<bool>* validity) const {
  return GetArray<TypeTraits<DOUBLE>>(col_idx, data, validity);
}
Status KuduScanBatch::RowPtr::GetArrayString(int col_idx,
                                             vector<Slice>* data,
                                             vector<bool>* validity) const {
  return GetArray<TypeTraits<STRING>>(col_idx, data, validity);
}
Status KuduScanBatch::RowPtr::GetArrayBinary(int col_idx,
                                            vector<Slice>* data,
                                            vector<bool>* validity) const {
  return GetArray<TypeTraits<BINARY>>(col_idx, data, validity);
}
Status KuduScanBatch::RowPtr::GetArrayVarchar(int col_idx,
                                              vector<Slice>* data,
                                              vector<bool>* validity) const {
  return GetArray<TypeTraits<VARCHAR>>(col_idx, data, validity);
}

const void* KuduScanBatch::RowPtr::cell(int col_idx) const {
  return row_data_ + schema_->column_offset(col_idx);
}

//------------------------------------------------------------
// Template instantiations: We instantiate all possible templates to avoid linker issues.
// see: https://isocpp.org/wiki/faq/templates#separate-template-fn-defn-from-decl
// TODO We can probably remove this when we move to c++11 and can use "extern template"
//------------------------------------------------------------

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<BOOL> >(const Slice& col_name, bool* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT8> >(const Slice& col_name, int8_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT16> >(const Slice& col_name, int16_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT32> >(const Slice& col_name, int32_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT64> >(const Slice& col_name, int64_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT128> >(const Slice& col_name, int128_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<UNIXTIME_MICROS> >(
    const Slice& col_name, int64_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<DATE> >(const Slice& col_name, int32_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<FLOAT> >(const Slice& col_name, float* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<DOUBLE> >(const Slice& col_name, double* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<STRING> >(const Slice& col_name, Slice* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<BINARY> >(const Slice& col_name, Slice* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<VARCHAR> >(const Slice& col_name, Slice* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<BOOL> >(int col_idx, bool* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT8> >(int col_idx, int8_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT16> >(int col_idx, int16_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT32> >(int col_idx, int32_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT64> >(int col_idx, int64_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<INT128> >(int col_idx, int128_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<UNIXTIME_MICROS> >(int col_idx, int64_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<DATE> >(int col_idx, int32_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<FLOAT> >(int col_idx, float* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<DOUBLE> >(int col_idx, double* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<STRING> >(int col_idx, Slice* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<BINARY> >(int col_idx, Slice* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<VARCHAR> >(int col_idx, Slice* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<DECIMAL32> >(int col_idx, int32_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<DECIMAL64> >(int col_idx, int64_t* val) const;

template
Status KuduScanBatch::RowPtr::Get<TypeTraits<DECIMAL128> >(int col_idx, int128_t* val) const;


template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<BOOL>>(
    const Slice& col_name,
    vector<bool>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<INT8>>(
    const Slice& col_name,
    vector<int8_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<INT16>>(
    const Slice& col_name,
    vector<int16_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<INT32>>(
    const Slice& col_name,
    vector<int32_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<INT64>>(
    const Slice& col_name,
    vector<int64_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<UNIXTIME_MICROS>>(
    const Slice& col_name,
    vector<int64_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<DATE>>(
    const Slice& col_name,
    vector<int32_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<FLOAT>>(
    const Slice& col_name,
    vector<float>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<DOUBLE>>(
    const Slice& col_name,
    vector<double>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<STRING>>(
    const Slice& col_name,
    vector<Slice>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<BINARY>>(
    const Slice& col_name,
    vector<Slice>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<VARCHAR>>(
    const Slice& col_name,
    vector<Slice>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<DECIMAL32>>(
    const Slice& col_name,
    vector<int32_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<DECIMAL64>>(
    const Slice& col_name,
    vector<int64_t>* data,
    vector<bool>* validity) const;

template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<BOOL>>(
    int col_idx,
    vector<bool>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<INT8>>(
    int col_idx,
    vector<int8_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<INT16>>(
    int col_idx,
    vector<int16_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<INT32>>(
    int col_idx,
    vector<int32_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<INT64>>(
    int col_idx,
    vector<int64_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<UNIXTIME_MICROS>>(
    int col_idx,
    vector<int64_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<DATE>>(
    int col_idx,
    vector<int32_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<FLOAT>>(
    int col_idx,
    vector<float>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<DOUBLE>>(
    int col_idx,
    vector<double>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<STRING>>(
    int col_idx,
    vector<Slice>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<BINARY>>(
    int col_idx,
    vector<Slice>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<VARCHAR>>(
    int col_idx,
    vector<Slice>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<DECIMAL32>>(
    int col_idx,
    vector<int32_t>* data,
    vector<bool>* validity) const;
template
Status KuduScanBatch::RowPtr::GetArray<TypeTraits<DECIMAL64>>(
    int col_idx,
    vector<int64_t>* data,
    vector<bool>* validity) const;


Status KuduScanBatch::RowPtr::GetUnscaledDecimal(int col_idx, int128_t* val) const {
  const ColumnSchema& col = schema_->column(col_idx);
  const DataType col_type = col.type_info()->type();
  switch (col_type) {
    case DECIMAL32:
      int32_t i32_val;
      RETURN_NOT_OK(Get<TypeTraits<DECIMAL32> >(col_idx, &i32_val));
      *val = i32_val;
      return Status::OK();
    case DECIMAL64:
      int64_t i64_val;
      RETURN_NOT_OK(Get<TypeTraits<DECIMAL64> >(col_idx, &i64_val));
      *val = i64_val;
      return Status::OK();
    case DECIMAL128:
      int128_t i128_val;
      RETURN_NOT_OK(Get<TypeTraits<DECIMAL128> >(col_idx, &i128_val));
      *val = i128_val;
      return Status::OK();
    default:
      return Status::InvalidArgument(
          Substitute("invalid type $0 provided for column '$1' (expected decimal)",
                     col.type_info()->name(), col.name()));
  }
}

string KuduScanBatch::RowPtr::ToString() const {
  // Client-users calling ToString() will likely expect it to not be redacted.
  ScopedDisableRedaction no_redaction;

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
