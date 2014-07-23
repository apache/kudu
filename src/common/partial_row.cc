// Copyright (c) 2013, Cloudera, inc.

#include "common/partial_row.h"

#include <string>

#include "common/row.h"
#include "common/schema.h"
#include "common/wire_protocol.pb.h"
#include "gutil/strings/substitute.h"
#include "util/bitmap.h"
#include "common/row_changelist.h"
#include "util/status.h"

using strings::Substitute;

namespace kudu {

namespace {
inline Status FindColumn(const Schema& schema, const Slice& col_name, int* idx) {
  StringPiece sp(reinterpret_cast<const char*>(col_name.data()), col_name.size());
  *idx = schema.find_column(sp);
  if (PREDICT_FALSE(*idx == -1)) {
    return Status::NotFound("No such column", col_name);
  }
  return Status::OK();
}
} // anonymous namespace

KuduPartialRow::KuduPartialRow(const Schema* schema)
  : schema_(schema) {
  DCHECK(schema_->initialized());
  size_t column_bitmap_size = BitmapSize(schema_->num_columns());
  size_t row_size = ContiguousRowHelper::row_size(*schema);

  uint8_t* dst = new uint8_t[2 * column_bitmap_size + row_size];
  isset_bitmap_ = dst;
  owned_strings_bitmap_ = isset_bitmap_ + column_bitmap_size;

  memset(isset_bitmap_, 0, 2 * column_bitmap_size);

  row_data_ = owned_strings_bitmap_ + column_bitmap_size;
#ifndef NDEBUG
  OverwriteWithPattern(reinterpret_cast<char*>(row_data_),
                       row_size, "NEWNEWNEWNEWNEW");
#endif
  ContiguousRowHelper::InitNullsBitmap(
    *schema_, row_data_, ContiguousRowHelper::null_bitmap_size(*schema_));
}

KuduPartialRow::~KuduPartialRow() {
  DeallocateOwnedStrings();
  // Both the row data and bitmap came from the same allocation.
  // The bitmap is at the start of it.
  delete [] isset_bitmap_;
}

template<DataType TYPE>
Status KuduPartialRow::Set(const Slice& col_name,
                       const typename DataTypeTraits<TYPE>::cpp_type& val,
                       bool owned) {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));
  return Set<TYPE>(col_idx, val, owned);
}

template<DataType TYPE>
Status KuduPartialRow::Set(int col_idx,
                       const typename DataTypeTraits<TYPE>::cpp_type& val,
                       bool owned) {
  const ColumnSchema& col = schema_->column(col_idx);
  if (PREDICT_FALSE(col.type_info()->type() != TYPE)) {
    // TODO: at some point we could allow type coercion here.
    return Status::InvalidArgument(
      Substitute("invalid type $0 provided for column '$1' (expected $2)",
                 DataTypeTraits<TYPE>::name(),
                 col.name(), col.type_info()->name()));
  }

  ContiguousRow row(*schema_, row_data_);

  // If we're replacing an existing STRING value, deallocate the old value.
  if (TYPE == STRING) DeallocateStringIfSet(col_idx);

  // Mark the column as set.
  BitmapSet(isset_bitmap_, col_idx);

  if (col.is_nullable()) {
    row.set_null(col_idx, false);
  }

  ContiguousRowCell<ContiguousRow> dst(&row, col_idx);
  memcpy(dst.mutable_ptr(), &val, sizeof(val));
  if (owned) {
    BitmapSet(owned_strings_bitmap_, col_idx);
  }
  return Status::OK();
}

void KuduPartialRow::DeallocateStringIfSet(int col_idx) {
  if (BitmapTest(owned_strings_bitmap_, col_idx)) {
    ContiguousRow row(*schema_, row_data_);
    const Slice* dst = schema_->ExtractColumnFromRow<STRING>(row, col_idx);
    delete [] dst->data();
    BitmapClear(owned_strings_bitmap_, col_idx);
  }
}
void KuduPartialRow::DeallocateOwnedStrings() {
  for (int i = 0; i < schema_->num_columns(); i++) {
    DeallocateStringIfSet(i);
  }
}

//------------------------------------------------------------
// Setters
//------------------------------------------------------------

Status KuduPartialRow::SetInt8(const Slice& col_name, int8_t val) {
  return Set<INT8>(col_name, val);
}
Status KuduPartialRow::SetInt16(const Slice& col_name, int16_t val) {
  return Set<INT16>(col_name, val);
}
Status KuduPartialRow::SetInt32(const Slice& col_name, int32_t val) {
  return Set<INT32>(col_name, val);
}
Status KuduPartialRow::SetInt64(const Slice& col_name, int64_t val) {
  return Set<INT64>(col_name, val);
}
Status KuduPartialRow::SetUInt8(const Slice& col_name, uint8_t val) {
  return Set<UINT8>(col_name, val);
}
Status KuduPartialRow::SetUInt16(const Slice& col_name, uint16_t val) {
  return Set<UINT16>(col_name, val);
}
Status KuduPartialRow::SetUInt32(const Slice& col_name, uint32_t val) {
  return Set<UINT32>(col_name, val);
}
Status KuduPartialRow::SetUInt64(const Slice& col_name, uint64_t val) {
  return Set<UINT64>(col_name, val);
}
Status KuduPartialRow::SetString(const Slice& col_name, const Slice& val) {
  return Set<STRING>(col_name, val, false);
}

Status KuduPartialRow::SetInt8(int col_idx, int8_t val) {
  return Set<INT8>(col_idx, val);
}
Status KuduPartialRow::SetInt16(int col_idx, int16_t val) {
  return Set<INT16>(col_idx, val);
}
Status KuduPartialRow::SetInt32(int col_idx, int32_t val) {
  return Set<INT32>(col_idx, val);
}
Status KuduPartialRow::SetInt64(int col_idx, int64_t val) {
  return Set<INT64>(col_idx, val);
}
Status KuduPartialRow::SetUInt8(int col_idx, uint8_t val) {
  return Set<UINT8>(col_idx, val);
}
Status KuduPartialRow::SetUInt16(int col_idx, uint16_t val) {
  return Set<UINT16>(col_idx, val);
}
Status KuduPartialRow::SetUInt32(int col_idx, uint32_t val) {
  return Set<UINT32>(col_idx, val);
}
Status KuduPartialRow::SetUInt64(int col_idx, uint64_t val) {
  return Set<UINT64>(col_idx, val);
}
Status KuduPartialRow::SetString(int col_idx, const Slice& val) {
  return Set<STRING>(col_idx, val, false);
}

Status KuduPartialRow::SetStringCopy(const Slice& col_name, const Slice& val) {
  uint8_t* relocated = new uint8_t[val.size()];
  memcpy(relocated, val.data(), val.size());
  Slice relocated_val(relocated, val.size());
  Status s = Set<STRING>(col_name, relocated_val, true);
  if (!s.ok()) {
    delete [] relocated;
  }
  return s;
}

Status KuduPartialRow::SetStringCopy(int col_idx, const Slice& val) {
  uint8_t* relocated = new uint8_t[val.size()];
  memcpy(relocated, val.data(), val.size());
  Slice relocated_val(relocated, val.size());
  Status s = Set<STRING>(col_idx, relocated_val, true);
  if (!s.ok()) {
    delete [] relocated;
  }
  return s;
}

Status KuduPartialRow::SetNull(const Slice& col_name) {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));
  return SetNull(col_idx);
}

Status KuduPartialRow::SetNull(int col_idx) {
  const ColumnSchema& col = schema_->column(col_idx);
  if (PREDICT_FALSE(!col.is_nullable())) {
    return Status::InvalidArgument("column not nullable", col.ToString());
  }

  if (col.type_info()->type() == STRING) DeallocateStringIfSet(col_idx);

  ContiguousRow row(*schema_, row_data_);
  row.set_null(col_idx, true);

  // Mark the column as set.
  BitmapSet(isset_bitmap_, col_idx);
  return Status::OK();
}

Status KuduPartialRow::Unset(const Slice& col_name) {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));
  return Unset(col_idx);
}

Status KuduPartialRow::Unset(int col_idx) {
  const ColumnSchema& col = schema_->column(col_idx);
  if (col.type_info()->type() == STRING) DeallocateStringIfSet(col_idx);
  BitmapClear(isset_bitmap_, col_idx);
  return Status::OK();
}

//------------------------------------------------------------
// Getters
//------------------------------------------------------------
bool KuduPartialRow::IsColumnSet(int col_idx) const {
  DCHECK_GE(col_idx, 0);
  DCHECK_LT(col_idx, schema_->num_columns());
  return BitmapTest(isset_bitmap_, col_idx);
}

bool KuduPartialRow::IsColumnSet(const Slice& col_name) const {
  int col_idx;
  CHECK_OK(FindColumn(*schema_, col_name, &col_idx));
  return IsColumnSet(col_idx);
}

bool KuduPartialRow::IsNull(int col_idx) const {
  const ColumnSchema& col = schema_->column(col_idx);
  if (!col.is_nullable()) {
    return false;
  }

  if (!IsColumnSet(col_idx)) return false;

  ContiguousRow row(*schema_, row_data_);
  return row.is_null(col_idx);
}

bool KuduPartialRow::IsNull(const Slice& col_name) const {
  int col_idx;
  CHECK_OK(FindColumn(*schema_, col_name, &col_idx));
  return IsNull(col_idx);
}

Status KuduPartialRow::GetInt8(const Slice& col_name, int8_t* val) const {
  return Get<INT8>(col_name, val);
}
Status KuduPartialRow::GetInt16(const Slice& col_name, int16_t* val) const {
  return Get<INT16>(col_name, val);
}
Status KuduPartialRow::GetInt32(const Slice& col_name, int32_t* val) const {
  return Get<INT32>(col_name, val);
}
Status KuduPartialRow::GetInt64(const Slice& col_name, int64_t* val) const {
  return Get<INT64>(col_name, val);
}
Status KuduPartialRow::GetUInt8(const Slice& col_name, uint8_t* val) const {
  return Get<UINT8>(col_name, val);
}
Status KuduPartialRow::GetUInt16(const Slice& col_name, uint16_t* val) const {
  return Get<UINT16>(col_name, val);
}
Status KuduPartialRow::GetUInt32(const Slice& col_name, uint32_t* val) const {
  return Get<UINT32>(col_name, val);
}
Status KuduPartialRow::GetUInt64(const Slice& col_name, uint64_t* val) const {
  return Get<UINT64>(col_name, val);
}
Status KuduPartialRow::GetString(const Slice& col_name, Slice* val) const {
  return Get<STRING>(col_name, val);
}

Status KuduPartialRow::GetInt8(int col_idx, int8_t* val) const {
  return Get<INT8>(col_idx, val);
}
Status KuduPartialRow::GetInt16(int col_idx, int16_t* val) const {
  return Get<INT16>(col_idx, val);
}
Status KuduPartialRow::GetInt32(int col_idx, int32_t* val) const {
  return Get<INT32>(col_idx, val);
}
Status KuduPartialRow::GetInt64(int col_idx, int64_t* val) const {
  return Get<INT64>(col_idx, val);
}
Status KuduPartialRow::GetUInt8(int col_idx, uint8_t* val) const {
  return Get<UINT8>(col_idx, val);
}
Status KuduPartialRow::GetUInt16(int col_idx, uint16_t* val) const {
  return Get<UINT16>(col_idx, val);
}
Status KuduPartialRow::GetUInt32(int col_idx, uint32_t* val) const {
  return Get<UINT32>(col_idx, val);
}
Status KuduPartialRow::GetUInt64(int col_idx, uint64_t* val) const {
  return Get<UINT64>(col_idx, val);
}
Status KuduPartialRow::GetString(int col_idx, Slice* val) const {
  return Get<STRING>(col_idx, val);
}

template<DataType TYPE>
Status KuduPartialRow::Get(const Slice& col_name,
                       typename DataTypeTraits<TYPE>::cpp_type* val) const {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));
return Get<TYPE>(col_idx, val);
}

template<DataType TYPE>
Status KuduPartialRow::Get(int col_idx,
                       typename DataTypeTraits<TYPE>::cpp_type* val) const {
  const ColumnSchema& col = schema_->column(col_idx);
  if (PREDICT_FALSE(col.type_info()->type() != TYPE)) {
    // TODO: at some point we could allow type coercion here.
    return Status::InvalidArgument(
      Substitute("invalid type $0 provided for column '$1' (expected $2)",
                 DataTypeTraits<TYPE>::name(),
                 col.name(), col.type_info()->name()));
  }

  if (PREDICT_FALSE(!IsColumnSet(col_idx))) {
    return Status::NotFound("column not set");
  }
  if (col.is_nullable() && IsNull(col_idx)) {
    return Status::NotFound("column is NULL");
  }

  ContiguousRow row(*schema_, row_data_);
  memcpy(val, row.cell_ptr(col_idx), sizeof(*val));
  return Status::OK();
}

//------------------------------------------------------------
// Utility code
//------------------------------------------------------------

bool KuduPartialRow::AllColumnsSet() const {
  return BitMapIsAllSet(isset_bitmap_, 0, schema_->num_columns());
}

bool KuduPartialRow::IsKeySet() const {
  return BitMapIsAllSet(isset_bitmap_, 0, schema_->num_key_columns());
}


std::string KuduPartialRow::ToString() const {
  ContiguousRow row(*schema_, row_data_);
  std::string ret;
  bool first = true;
  for (int i = 0; i < schema_->num_columns(); i++) {
    if (IsColumnSet(i)) {
      if (!first) {
        ret.append(", ");
      }
      schema_->column(i).DebugCellAppend(row.cell(i), &ret);
      first = false;
    }
  }
  return ret;
}

//------------------------------------------------------------
// Serialization/deserialization
//------------------------------------------------------------


} // namespace kudu
