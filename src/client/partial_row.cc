// Copyright (c) 2013, Cloudera, inc.

#include "client/partial_row.h"

#include <string>

#include "common/row.h"
#include "common/schema.h"
#include "common/wire_protocol.pb.h"
#include "gutil/strings/substitute.h"
#include "util/bitmap.h"
#include "util/safe_math.h"
#include "util/status.h"

using strings::Substitute;

namespace kudu {
namespace client {

namespace {
inline Status FindColumn(const Schema& schema, const Slice& col_name, int* idx) {
  // TODO(perf): find_column forces us to allocate a string here --
  // should make some version which can do the hash lookup from a Slice
  // instead.
  *idx = schema.find_column(col_name.ToString());
  if (PREDICT_FALSE(*idx == -1)) {
    return Status::NotFound("No such column", col_name);
  }
  return Status::OK();
}
} // anonymous namespace

PartialRow::PartialRow(const Schema* schema)
  : schema_(schema) {
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

PartialRow::~PartialRow() {
  DeallocateOwnedStrings();
  // Both the row data and bitmap came from the same allocation.
  // The bitmap is at the start of it.
  delete [] isset_bitmap_;
}

template<DataType TYPE>
Status PartialRow::Set(const Slice& col_name,
                       const typename DataTypeTraits<TYPE>::cpp_type& val,
                       bool owned) {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));

  const ColumnSchema& col = schema_->column(col_idx);
  if (PREDICT_FALSE(col.type_info().type() != TYPE)) {
    // TODO: at some point we could allow type coercion here.
    return Status::InvalidArgument(
      Substitute("invalid type $0 provided for column '$1' (expected $2)",
                 DataTypeTraits<TYPE>::name(),
                 col.name(), col.type_info().name()));
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
  RETURN_NOT_OK(CopyCellData(SimpleConstCell(col, &val), &dst,
                             reinterpret_cast<Arena*>(NULL)));
  if (owned) {
    BitmapSet(owned_strings_bitmap_, col_idx);
  }
  return Status::OK();
}

void PartialRow::DeallocateStringIfSet(int col_idx) {
  if (BitmapTest(owned_strings_bitmap_, col_idx)) {
    ContiguousRow row(*schema_, row_data_);
    const Slice* dst = schema_->ExtractColumnFromRow<STRING>(row, col_idx);
    delete [] dst->data();
    BitmapClear(owned_strings_bitmap_, col_idx);
  }
}
void PartialRow::DeallocateOwnedStrings() {
  for (int i = 0; i < schema_->num_columns(); i++) {
    DeallocateStringIfSet(i);
  }
}


Status PartialRow::SetInt8(const Slice& col_name, int8_t val) {
  return Set<INT8>(col_name, val);
}
Status PartialRow::SetInt16(const Slice& col_name, int16_t val) {
  return Set<INT16>(col_name, val);
}
Status PartialRow::SetInt32(const Slice& col_name, int32_t val) {
  return Set<INT32>(col_name, val);
}
Status PartialRow::SetInt64(const Slice& col_name, int64_t val) {
  return Set<INT64>(col_name, val);
}
Status PartialRow::SetUInt8(const Slice& col_name, uint8_t val) {
  return Set<UINT8>(col_name, val);
}
Status PartialRow::SetUInt16(const Slice& col_name, uint16_t val) {
  return Set<UINT16>(col_name, val);
}
Status PartialRow::SetUInt32(const Slice& col_name, uint32_t val) {
  return Set<UINT32>(col_name, val);
}
Status PartialRow::SetUInt64(const Slice& col_name, uint64_t val) {
  return Set<UINT64>(col_name, val);
}
Status PartialRow::SetStringCopy(const Slice& col_name, const Slice& val) {
  uint8_t* relocated = new uint8_t[val.size()];
  memcpy(relocated, val.data(), val.size());
  Slice relocated_val(relocated, val.size());
  Status s = Set<STRING>(col_name, relocated_val, true);
  if (!s.ok()) {
    delete [] relocated;
  }
  return s;
}

Status PartialRow::SetNull(const Slice& col_name) {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));
  const ColumnSchema& col = schema_->column(col_idx);
  if (PREDICT_FALSE(!col.is_nullable())) {
    return Status::InvalidArgument("column not nullable", col.ToString());
  }

  if (col.type_info().type() == STRING) DeallocateStringIfSet(col_idx);

  ContiguousRow row(*schema_, row_data_);
  row.set_null(col_idx, true);

  // Mark the column as set.
  BitmapSet(isset_bitmap_, col_idx);
  return Status::OK();
}

Status PartialRow::Unset(const Slice& col_name) {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));
  const ColumnSchema& col = schema_->column(col_idx);
  if (col.type_info().type() == STRING) DeallocateStringIfSet(col_idx);
  BitmapClear(isset_bitmap_, col_idx);
  return Status::OK();
}

bool PartialRow::IsKeySet() const {
  for (int i = 0; i < schema_->num_key_columns(); i++) {
    if (!IsColumnSet(i)) {
      return false;
    }
  }
  return true;
}

bool PartialRow::IsColumnSet(int col_idx) const {
  DCHECK_GE(col_idx, 0);
  DCHECK_LT(col_idx, schema_->num_columns());
  return BitmapTest(isset_bitmap_, col_idx);
}

std::string PartialRow::ToString() const {
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

#define CHARP(x) reinterpret_cast<const char*>((x))

void PartialRow::AppendToPB(PartialRowsPB* pb) const {
  // See wire_protocol.pb for a description of the format.

  string* dst = pb->mutable_rows();
  dst->append(CHARP(isset_bitmap_), BitmapSize(schema_->num_columns()));
  dst->append(CHARP(ContiguousRowHelper::null_bitmap_ptr(*schema_, row_data_)),
              ContiguousRowHelper::null_bitmap_size(*schema_));

  ContiguousRow row(*schema_, row_data_);
  for (int i = 0; i < schema_->num_columns(); i++) {
    if (!IsColumnSet(i)) continue;
    const ColumnSchema& col = schema_->column(i);

    if (col.is_nullable() && row.is_null(i)) continue;

    if (col.type_info().type() == STRING) {
      const Slice* val = reinterpret_cast<const Slice*>(row.cell_ptr(i));
      size_t indirect_offset = pb->mutable_indirect_data()->size();
      pb->mutable_indirect_data()->append(CHARP(val->data()), val->size());
      Slice to_append(reinterpret_cast<const uint8_t*>(indirect_offset),
                      val->size());
      dst->append(CHARP(&to_append), sizeof(Slice));
    } else {
      dst->append(CHARP(row.cell_ptr(i)), col.type_info().size());
    }
  }
}
#undef CHARP

Status PartialRow::CopyFromPB(const PartialRowsPB& pb,
                              int offset) {
  DeallocateOwnedStrings();

  // See wire_protocol.pb for a description of the format.
  Slice src(pb.rows().data(), pb.rows().size());
  if (PREDICT_FALSE(src.size() < offset)) {
    return Status::Corruption(Substitute("Cannot seek to offset $0 in PB",
                                         offset));
  }
  src.remove_prefix(offset);

  // Read the isset bitmap
  int bm_size = BitmapSize(schema_->num_columns());
  if (PREDICT_FALSE(src.size() < bm_size)) {
    return Status::Corruption("Cannot find isset bitmap");
  }
  memcpy(isset_bitmap_, src.data(), bm_size);
  src.remove_prefix(bm_size);

  // Read the null bitmap if present
  if (schema_->has_nullables()) {
    if (PREDICT_FALSE(src.size() < bm_size)) {
      return Status::Corruption("Cannot find null bitmap");
    }
    uint8_t* null_bm = ContiguousRowHelper::null_bitmap_ptr(*schema_, row_data_);
    memcpy(null_bm, src.data(), bm_size);
    src.remove_prefix(bm_size);
  }

  // Read the data for each present column.
  ContiguousRow row(*schema_, row_data_);
  for (int i = 0; i < schema_->num_columns(); i++) {
    // Unset columns aren't present
    if (!IsColumnSet(i)) continue;

    // NULL columns aren't present
    const ColumnSchema& col = schema_->column(i);
    if (col.is_nullable() && row.is_null(i)) continue;

    int size = col.type_info().size();
    if (PREDICT_FALSE(src.size() < size)) {
      return Status::Corruption("Not enough data for column", col.ToString());
    }
    // Copy the data
    if (col.type_info().type() == STRING) {
      // The Slice in the protobuf has a pointer relative to the indirect data,
      // not a real pointer. Need to fix that.
      const Slice* slice = reinterpret_cast<const Slice*>(src.data());
      size_t offset_in_indirect = reinterpret_cast<uintptr_t>(slice->data());
      bool overflowed = false;
      size_t max_offset = AddWithOverflowCheck(offset_in_indirect, slice->size(), &overflowed);
      if (PREDICT_FALSE(overflowed || max_offset > pb.indirect_data().size())) {
        return Status::Corruption("Bad indirect slice");
      }

      Slice real_slice(&pb.indirect_data()[offset_in_indirect], slice->size());
      memcpy(row.mutable_cell_ptr(i), &real_slice, size);
    } else {
      memcpy(row.mutable_cell_ptr(i), src.data(), size);
    }
    src.remove_prefix(size);
  }
  return Status::OK();
}

} // namespace client
} // namespace kudu
