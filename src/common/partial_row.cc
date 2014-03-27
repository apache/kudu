// Copyright (c) 2013, Cloudera, inc.

#include "common/partial_row.h"

#include <string>

#include "common/row.h"
#include "common/schema.h"
#include "common/wire_protocol.pb.h"
#include "gutil/strings/substitute.h"
#include "util/bitmap.h"
#include "util/safe_math.h"
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
  return Set<TYPE>(col_idx, val, owned);
}

template<DataType TYPE>
Status PartialRow::Set(int col_idx,
                       const typename DataTypeTraits<TYPE>::cpp_type& val,
                       bool owned) {
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
  memcpy(dst.mutable_ptr(), &val, sizeof(val));
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

//------------------------------------------------------------
// Setters
//------------------------------------------------------------

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
Status PartialRow::SetString(const Slice& col_name, const Slice& val) {
  return Set<STRING>(col_name, val, false);
}

Status PartialRow::SetInt8(int col_idx, int8_t val) {
  return Set<INT8>(col_idx, val);
}
Status PartialRow::SetInt16(int col_idx, int16_t val) {
  return Set<INT16>(col_idx, val);
}
Status PartialRow::SetInt32(int col_idx, int32_t val) {
  return Set<INT32>(col_idx, val);
}
Status PartialRow::SetInt64(int col_idx, int64_t val) {
  return Set<INT64>(col_idx, val);
}
Status PartialRow::SetUInt8(int col_idx, uint8_t val) {
  return Set<UINT8>(col_idx, val);
}
Status PartialRow::SetUInt16(int col_idx, uint16_t val) {
  return Set<UINT16>(col_idx, val);
}
Status PartialRow::SetUInt32(int col_idx, uint32_t val) {
  return Set<UINT32>(col_idx, val);
}
Status PartialRow::SetUInt64(int col_idx, uint64_t val) {
  return Set<UINT64>(col_idx, val);
}
Status PartialRow::SetString(int col_idx, const Slice& val) {
  return Set<STRING>(col_idx, val, false);
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

Status PartialRow::SetStringCopy(int col_idx, const Slice& val) {
  uint8_t* relocated = new uint8_t[val.size()];
  memcpy(relocated, val.data(), val.size());
  Slice relocated_val(relocated, val.size());
  Status s = Set<STRING>(col_idx, relocated_val, true);
  if (!s.ok()) {
    delete [] relocated;
  }
  return s;
}

Status PartialRow::SetNull(const Slice& col_name) {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));
  return SetNull(col_idx);
}

Status PartialRow::SetNull(int col_idx) {
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
  return Unset(col_idx);
}

Status PartialRow::Unset(int col_idx) {
  const ColumnSchema& col = schema_->column(col_idx);
  if (col.type_info().type() == STRING) DeallocateStringIfSet(col_idx);
  BitmapClear(isset_bitmap_, col_idx);
  return Status::OK();
}

//------------------------------------------------------------
// Getters
//------------------------------------------------------------
bool PartialRow::IsColumnSet(int col_idx) const {
  DCHECK_GE(col_idx, 0);
  DCHECK_LT(col_idx, schema_->num_columns());
  return BitmapTest(isset_bitmap_, col_idx);
}

bool PartialRow::IsColumnSet(const Slice& col_name) const {
  int col_idx;
  CHECK_OK(FindColumn(*schema_, col_name, &col_idx));
  return IsColumnSet(col_idx);
}

bool PartialRow::IsNull(int col_idx) const {
  const ColumnSchema& col = schema_->column(col_idx);
  if (!col.is_nullable()) {
    return false;
  }

  if (!IsColumnSet(col_idx)) return false;

  ContiguousRow row(*schema_, row_data_);
  return row.is_null(col_idx);
}

bool PartialRow::IsNull(const Slice& col_name) const {
  int col_idx;
  CHECK_OK(FindColumn(*schema_, col_name, &col_idx));
  return IsNull(col_idx);
}

Status PartialRow::GetInt8(const Slice& col_name, int8_t* val) const {
  return Get<INT8>(col_name, val);
}
Status PartialRow::GetInt16(const Slice& col_name, int16_t* val) const {
  return Get<INT16>(col_name, val);
}
Status PartialRow::GetInt32(const Slice& col_name, int32_t* val) const {
  return Get<INT32>(col_name, val);
}
Status PartialRow::GetInt64(const Slice& col_name, int64_t* val) const {
  return Get<INT64>(col_name, val);
}
Status PartialRow::GetUInt8(const Slice& col_name, uint8_t* val) const {
  return Get<UINT8>(col_name, val);
}
Status PartialRow::GetUInt16(const Slice& col_name, uint16_t* val) const {
  return Get<UINT16>(col_name, val);
}
Status PartialRow::GetUInt32(const Slice& col_name, uint32_t* val) const {
  return Get<UINT32>(col_name, val);
}
Status PartialRow::GetUInt64(const Slice& col_name, uint64_t* val) const {
  return Get<UINT64>(col_name, val);
}
Status PartialRow::GetString(const Slice& col_name, Slice* val) const {
  return Get<STRING>(col_name, val);
}

Status PartialRow::GetInt8(int col_idx, int8_t* val) const {
  return Get<INT8>(col_idx, val);
}
Status PartialRow::GetInt16(int col_idx, int16_t* val) const {
  return Get<INT16>(col_idx, val);
}
Status PartialRow::GetInt32(int col_idx, int32_t* val) const {
  return Get<INT32>(col_idx, val);
}
Status PartialRow::GetInt64(int col_idx, int64_t* val) const {
  return Get<INT64>(col_idx, val);
}
Status PartialRow::GetUInt8(int col_idx, uint8_t* val) const {
  return Get<UINT8>(col_idx, val);
}
Status PartialRow::GetUInt16(int col_idx, uint16_t* val) const {
  return Get<UINT16>(col_idx, val);
}
Status PartialRow::GetUInt32(int col_idx, uint32_t* val) const {
  return Get<UINT32>(col_idx, val);
}
Status PartialRow::GetUInt64(int col_idx, uint64_t* val) const {
  return Get<UINT64>(col_idx, val);
}
Status PartialRow::GetString(int col_idx, Slice* val) const {
  return Get<STRING>(col_idx, val);
}

template<DataType TYPE>
Status PartialRow::Get(const Slice& col_name,
                       typename DataTypeTraits<TYPE>::cpp_type* val) const {
  int col_idx;
  RETURN_NOT_OK(FindColumn(*schema_, col_name, &col_idx));
return Get<TYPE>(col_idx, val);
}

template<DataType TYPE>
Status PartialRow::Get(int col_idx,
                       typename DataTypeTraits<TYPE>::cpp_type* val) const {
  const ColumnSchema& col = schema_->column(col_idx);
  if (PREDICT_FALSE(col.type_info().type() != TYPE)) {
    // TODO: at some point we could allow type coercion here.
    return Status::InvalidArgument(
      Substitute("invalid type $0 provided for column '$1' (expected $2)",
                 DataTypeTraits<TYPE>::name(),
                 col.name(), col.type_info().name()));
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

bool PartialRow::AllColumnsSet() const {
  return BitMapIsAllSet(isset_bitmap_, 0, schema_->num_columns());
}

bool PartialRow::IsKeySet() const {
  return BitMapIsAllSet(isset_bitmap_, 0, schema_->num_key_columns());
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

//------------------------------------------------------------
// Serialization/deserialization
//------------------------------------------------------------

#define CHARP(x) reinterpret_cast<const char*>((x))

void PartialRow::AppendToPB(RowOperationsPB::Type op_type, RowOperationsPB* pb) const {
  // See wire_protocol.pb for a description of the format.
  string* dst = pb->mutable_rows();

  // Compute a bound on much space we may need in the 'rows' field.
  // Then, resize it to this much space. This allows us to use simple
  // memcpy() calls to copy the data, rather than string->append(), which
  // reduces branches significantly in this fairly hot code path.
  // (std::string::append doesn't get inlined).
  // At the end of the function, we'll resize() the string back down to the
  // right size.
  int isset_bitmap_size = BitmapSize(schema_->num_columns());
  int null_bitmap_size = ContiguousRowHelper::null_bitmap_size(*schema_);
  int type_size = 1; // type uses one byte
  int max_size = type_size + schema_->byte_size() + isset_bitmap_size + null_bitmap_size;
  int old_size = dst->size();
  dst->resize(dst->size() + max_size);

  uint8_t* dst_ptr = reinterpret_cast<uint8_t*>(&(*dst)[old_size]);

  *dst_ptr++ = static_cast<uint8_t>(op_type);
  memcpy(dst_ptr, isset_bitmap_, isset_bitmap_size);
  dst_ptr += isset_bitmap_size;

  memcpy(dst_ptr,
         ContiguousRowHelper::null_bitmap_ptr(*schema_, row_data_),
         null_bitmap_size);
  dst_ptr += null_bitmap_size;

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
      memcpy(dst_ptr, &to_append, sizeof(Slice));
      dst_ptr += sizeof(Slice);
    } else {
      memcpy(dst_ptr, row.cell_ptr(i), col.type_info().size());
      dst_ptr += col.type_info().size();
    }
  }

  dst->resize(reinterpret_cast<char*>(dst_ptr) - &(*dst)[0]);
}
#undef CHARP

namespace {

// Utility class for decoding RowOperationsPB.
//
// This is factored out so that we can either decode into a new PartialRow
// object, or into a list of contiguous rows in DecodeAndProject().
//
// See wire_protocol.pb for a description of the format.
class PBDecoder {
 public:
  PBDecoder(const Schema* schema,
            const RowOperationsPB* pb)
    : schema_(schema),
      pb_(pb),
      bm_size_(BitmapSize(schema_->num_columns())),
      src_(pb->rows().data(), pb->rows().size()) {
  }


  Status SkipToOffset(int offset) {
    if (PREDICT_FALSE(src_.size() < offset)) {
      return Status::Corruption(Substitute("Cannot seek to offset $0 in PB",
                                           offset));
    }
    src_.remove_prefix(offset);
    return Status::OK();
  }

  Status ReadOpType(RowOperationsPB::Type* type) {
    if (PREDICT_FALSE(src_.empty())) {
      return Status::Corruption("Cannot find operation type");
    }
    if (PREDICT_FALSE(!RowOperationsPB_Type_IsValid(src_[0]))) {
      return Status::Corruption(Substitute("Unknown operation type: $0", src_[0]));
    }
    *type = static_cast<RowOperationsPB::Type>(src_[0]);
    src_.remove_prefix(1);
    return Status::OK();
  }

  Status ReadIssetBitmap(uint8_t* bitmap) {
    if (PREDICT_FALSE(src_.size() < bm_size_)) {
      return Status::Corruption("Cannot find isset bitmap");
    }
    memcpy(bitmap, src_.data(), bm_size_);
    src_.remove_prefix(bm_size_);
    return Status::OK();
  }

  Status ReadNullBitmap(uint8_t* null_bm) {
    if (PREDICT_FALSE(src_.size() < bm_size_)) {
      return Status::Corruption("Cannot find null bitmap");
    }
    memcpy(null_bm, src_.data(), bm_size_);
    src_.remove_prefix(bm_size_);
    return Status::OK();
  }

  Status ReadColumn(const ColumnSchema& col, uint8_t* dst) {
    int size = col.type_info().size();
    if (PREDICT_FALSE(src_.size() < size)) {
      return Status::Corruption("Not enough data for column", col.ToString());
    }
    // Copy the data
    if (col.type_info().type() == STRING) {
      // The Slice in the protobuf has a pointer relative to the indirect data,
      // not a real pointer. Need to fix that.
      const Slice* slice = reinterpret_cast<const Slice*>(src_.data());
      size_t offset_in_indirect = reinterpret_cast<uintptr_t>(slice->data());
      bool overflowed = false;
      size_t max_offset = AddWithOverflowCheck(offset_in_indirect, slice->size(), &overflowed);
      if (PREDICT_FALSE(overflowed || max_offset > pb_->indirect_data().size())) {
        return Status::Corruption("Bad indirect slice");
      }

      Slice real_slice(&pb_->indirect_data()[offset_in_indirect], slice->size());
      memcpy(dst, &real_slice, size);
    } else {
      memcpy(dst, src_.data(), size);
    }
    src_.remove_prefix(size);
    return Status::OK();
  }

  bool has_next() const {
    return !src_.empty();
  }

  size_t bitmap_size() const {
    return bm_size_;
  }

 private:
  const Schema* const schema_;
  const RowOperationsPB* const pb_;
  const int bm_size_;
  Slice src_;

  DISALLOW_COPY_AND_ASSIGN(PBDecoder);
};
} // anonymous namespace

Status PartialRow::CopyFromPB(const RowOperationsPB& pb,
                              int offset,
                              RowOperationsPB::Type* type) {
  DeallocateOwnedStrings();

  PBDecoder decoder(schema_, &pb);
  RETURN_NOT_OK(decoder.SkipToOffset(offset));

  // Read the type
  RETURN_NOT_OK(decoder.ReadOpType(type));

  // Read the isset bitmap
  RETURN_NOT_OK(decoder.ReadIssetBitmap(isset_bitmap_));

  // Read the null bitmap if present
  if (schema_->has_nullables()) {
    uint8_t* null_bm = ContiguousRowHelper::null_bitmap_ptr(*schema_, row_data_);
    RETURN_NOT_OK(decoder.ReadNullBitmap(null_bm));
  }

  // Read the data for each present column.
  ContiguousRow row(*schema_, row_data_);
  for (int i = 0; i < schema_->num_columns(); i++) {
    // Unset columns aren't present
    if (!IsColumnSet(i)) continue;

    // NULL columns aren't present
    const ColumnSchema& col = schema_->column(i);
    if (col.is_nullable() && row.is_null(i)) continue;

    RETURN_NOT_OK(decoder.ReadColumn(col, row.mutable_cell_ptr(i)));
  }
  return Status::OK();
}

namespace {

void SetupPrototypeRow(const Schema& schema,
                       ContiguousRow* row) {
  for (int i = 0; i < schema.num_columns(); i++) {
    const ColumnSchema& col = schema.column(i);
    if (col.has_write_default()) {
      if (col.is_nullable()) {
        row->set_null(i, false);
      }
      memcpy(row->mutable_cell_ptr(i), col.write_default_value(), col.type_info().size());
    } else if (col.is_nullable()) {
      row->set_null(i, true);
    } else {
      // No default and not nullable. Therefore this column is required,
      // and we'll ensure that it gets during the projection step.
    }
  }
}

// Projector implementation which handles mapping the client column indexes
// to server-side column indexes, ensuring that all of the columns exist,
// and that every required (non-null, non-default) column in the server
// schema is also present in the client.
class ClientServerMapping {
 public:
  ClientServerMapping(const Schema* client_schema,
                      const Schema* tablet_schema)
    : client_schema_(client_schema),
      tablet_schema_(tablet_schema),
      saw_tablet_col_(tablet_schema->num_columns()) {
  }

  Status ProjectBaseColumn(size_t client_col_idx, size_t tablet_col_idx) {
    // We should get this called exactly once for every input column,
    // since the input columns must be a strict subset of the tablet columns.
    DCHECK_EQ(client_to_tablet_.size(), client_col_idx);
    DCHECK_LT(tablet_col_idx, saw_tablet_col_.size());
    client_to_tablet_.push_back(tablet_col_idx);
    saw_tablet_col_[tablet_col_idx] = 1;
    return Status::OK();
  }

  Status ProjectDefaultColumn(size_t client_col_idx) {
    // Even if the client provides a default (which it shouldn't), we don't
    // want to accept writes with an extra column.
    return ProjectExtraColumn(client_col_idx);
  }

  Status ProjectExtraColumn(size_t client_col_idx) {
    return Status::InvalidArgument(
      Substitute("Client provided column $0 not present in tablet",
                 client_schema_->column(client_col_idx).ToString()));
  }

  // Translate from a client schema index to the tablet schema index
  int client_to_tablet_idx(int client_idx) const {
    DCHECK_LT(client_idx, client_to_tablet_.size());
    return client_to_tablet_[client_idx];
  }

  int num_mapped() const {
    return client_to_tablet_.size();
  }

  // Ensure that any required (non-null, non-defaulted) columns from the
  // server side schema are found in the client-side schema. If not,
  // returns an InvalidArgument.
  Status CheckAllRequiredColumnsPresent() {
    for (int tablet_col_idx = 0;
         tablet_col_idx < tablet_schema_->num_columns();
         tablet_col_idx++) {
      const ColumnSchema& col = tablet_schema_->column(tablet_col_idx);
      if (!col.has_write_default() &&
          !col.is_nullable()) {
        // All clients must pass this column.
        if (!saw_tablet_col_[tablet_col_idx]) {
          return Status::InvalidArgument(
            "Client missing required column", col.ToString());
        }
      }
    }
    return Status::OK();
  }

 private:
  const Schema* const client_schema_;
  const Schema* const tablet_schema_;
  vector<int> client_to_tablet_;
  vector<bool> saw_tablet_col_;
  DISALLOW_COPY_AND_ASSIGN(ClientServerMapping);
};

} // anonymous namespace

Status PartialRow::DecodeAndProject(const RowOperationsPB& pb,
                                    const Schema& client_schema,
                                    const Schema& tablet_schema,
                                    std::vector<uint8_t*>* rows,
                                    Arena* dst_arena) {
  // TODO: there's a bug here, in that if a client passes some column
  // in its schema that has been deleted on the server, it will fail
  // even if the client never actually specified any values for it.
  // For example, a DBA might do a thorough audit that no one is using
  // some column anymore, and then drop the column, expecting it to be
  // compatible, but all writes would start failing until clients
  // refreshed their schema.
  CHECK(!client_schema.has_column_ids());
  ClientServerMapping mapping(&client_schema, &tablet_schema);
  RETURN_NOT_OK(client_schema.GetProjectionMapping(tablet_schema, &mapping));
  DCHECK_EQ(mapping.num_mapped(), client_schema.num_columns());
  RETURN_NOT_OK(mapping.CheckAllRequiredColumnsPresent());

  PBDecoder decoder(&client_schema, &pb);
  uint8_t client_isset_map[decoder.bitmap_size()];
  uint8_t client_null_map[decoder.bitmap_size()];
  memset(client_null_map, 0, decoder.bitmap_size());
  size_t tablet_row_size = ContiguousRowHelper::row_size(tablet_schema);

  // Make a "prototype row" which has all the defaults filled in. We can copy
  // this to create a starting point for each row as we decode it, with
  // all the defaults in place without having to loop.
  uint8_t prototype_row_storage[tablet_row_size];
  ContiguousRow prototype_row(tablet_schema, prototype_row_storage);
  SetupPrototypeRow(tablet_schema, &prototype_row);

  while (decoder.has_next()) {
    RowOperationsPB::Type type;
    RETURN_NOT_OK(decoder.ReadOpType(&type));
    if (PREDICT_FALSE(type != RowOperationsPB::INSERT)) {
      return Status::Corruption("Expected only INSERT operations");
    }

    // Read the null and isset bitmaps for the client-provided row into
    // our stack storage.
    RETURN_NOT_OK(decoder.ReadIssetBitmap(client_isset_map));
    if (client_schema.has_nullables()) {
      RETURN_NOT_OK(decoder.ReadNullBitmap(client_null_map));
    }

    // Allocate a row with the tablet's layout.
    uint8_t* tablet_row_storage = reinterpret_cast<uint8_t*>(
      dst_arena->AllocateBytesAligned(tablet_row_size, 8));
    if (PREDICT_FALSE(!tablet_row_storage)) {
      return Status::RuntimeError("Out of memory");
    }

    // Initialize the new row from the 'prototype' row which has been set
    // with all of the server-side default values. This copy may be entirely
    // overwritten in the case that all columns are specified, but this is
    // still likely faster (and simpler) than looping through all the server-side
    // columns to initialize defaults where non-set on every row.
    memcpy(tablet_row_storage, prototype_row_storage, tablet_row_size);
    ContiguousRow tablet_row(tablet_schema, tablet_row_storage);

    // Now handle each of the columns passed by the user, replacing the defaults
    // from the prototype.
    for (int client_col_idx = 0; client_col_idx < client_schema.num_columns(); client_col_idx++) {
      // Look up the corresponding column from the tablet. We use the server-side
      // ColumnSchema object since it has the most up-to-date default, nullability,
      // etc.
      int tablet_col_idx = mapping.client_to_tablet_idx(client_col_idx);
      DCHECK_GE(tablet_col_idx, 0);
      const ColumnSchema& col = tablet_schema.column(tablet_col_idx);

      if (BitmapTest(client_isset_map, client_col_idx)) {
        // If the client provided a value for this column, copy it.

        // Copy null-ness. Even if it's non-nullable, this is safe to do.
        // The null flag will be ignored.
        bool client_set_to_null = BitmapTest(client_null_map, client_col_idx);
        tablet_row.set_null(tablet_col_idx, client_set_to_null);
        // Copy the value if it's not null
        if (!client_set_to_null) {
          RETURN_NOT_OK(decoder.ReadColumn(col, tablet_row.mutable_cell_ptr(tablet_col_idx)));
        }
      } else {
        // If the client didn't provide a value, then the column must either be nullable or
        // have a default (which was already set in the prototype row.

        if (PREDICT_FALSE(!(col.is_nullable() || col.has_write_default()))) {
          // TODO: change this to return per-row errors. Otherwise if one row in a batch
          // is missing a field for some reason, the whole batch will fail.
          return Status::InvalidArgument("No value provided for required column",
                                         col.ToString());
        }
      }
    }

    rows->push_back(tablet_row_storage);
  }
  return Status::OK();
}


Status PartialRow::DecodeAndProjectUpdates(
    const RowOperationsPB& pb,
    const Schema& client_schema,
    const Schema& tablet_schema,
    std::vector<uint8_t*>* row_keys,
    std::vector<RowChangeList>* changelists,
    Arena* dst_arena) {
  CHECK(!client_schema.has_column_ids());
  DCHECK(tablet_schema.has_column_ids());

  // TODO(perf): in the server, we do this for the updates and the inserts
  // separately, but we could do it just once.
  ClientServerMapping mapping(&client_schema, &tablet_schema);
  RETURN_NOT_OK(client_schema.GetProjectionMapping(tablet_schema, &mapping));
  DCHECK_EQ(mapping.num_mapped(), client_schema.num_columns());

  DCHECK_KEY_PROJECTION_SCHEMA_EQ(client_schema, tablet_schema);
  int rowkey_size = tablet_schema.key_byte_size();

  PBDecoder decoder(&client_schema, &pb);
  uint8_t client_isset_map[decoder.bitmap_size()];
  uint8_t client_null_map[decoder.bitmap_size()];
  memset(client_null_map, 0, decoder.bitmap_size());
  faststring buf;
  RowChangeListEncoder rcl_encoder(tablet_schema, &buf);

  while (decoder.has_next()) {
    RowOperationsPB::Type type;
    RETURN_NOT_OK(decoder.ReadOpType(&type));
    if (PREDICT_FALSE(type != RowOperationsPB::UPDATE)) {
      return Status::Corruption("Expected only UPDATE operations");
    }

    // Read the null and isset bitmaps for the client-provided row into
    // our stack storage.
    RETURN_NOT_OK(decoder.ReadIssetBitmap(client_isset_map));
    if (client_schema.has_nullables()) {
      RETURN_NOT_OK(decoder.ReadNullBitmap(client_null_map));
    }

    // Allocate space for the row key.
    uint8_t* rowkey_storage = reinterpret_cast<uint8_t*>(
      dst_arena->AllocateBytesAligned(rowkey_size, 8));
    if (PREDICT_FALSE(!rowkey_storage)) {
      return Status::RuntimeError("Out of memory");
    }

    // We're passing the full schema instead of the key schema here.
    // That's OK because the keys come at the bottom. We lose some bounds
    // checking in debug builds, but it avoids an extra copy of the key schema.
    ContiguousRow rowkey(tablet_schema, rowkey_storage);

    // First process the key columns.
    int client_col_idx = 0;
    for (; client_col_idx < client_schema.num_key_columns(); client_col_idx++) {
      // Look up the corresponding column from the tablet. We use the server-side
      // ColumnSchema object since it has the most up-to-date default, nullability,
      // etc.
      DCHECK_EQ(mapping.client_to_tablet_idx(client_col_idx),
                client_col_idx) << "key columns should match";
      int tablet_col_idx = client_col_idx;

      const ColumnSchema& col = tablet_schema.column(tablet_col_idx);
      if (PREDICT_FALSE(!BitmapTest(client_isset_map, client_col_idx))) {
        return Status::InvalidArgument("No value provided for key column",
                                       col.ToString());
      }

      bool client_set_to_null = BitmapTest(client_null_map, client_col_idx);
      if (PREDICT_FALSE(client_set_to_null)) {
        return Status::InvalidArgument("NULL values not allowed for key column",
                                       col.ToString());
      }

      RETURN_NOT_OK(decoder.ReadColumn(col, rowkey.mutable_cell_ptr(tablet_col_idx)));
    }

    // Now process the rest of columns as updates.
    rcl_encoder.Reset();
    for (; client_col_idx < client_schema.num_columns(); client_col_idx++) {
      int tablet_col_idx = mapping.client_to_tablet_idx(client_col_idx);
      DCHECK_GE(tablet_col_idx, 0);
      const ColumnSchema& col = tablet_schema.column(tablet_col_idx);

      if (BitmapTest(client_isset_map, client_col_idx)) {
        bool client_set_to_null = BitmapTest(client_null_map, client_col_idx);
        uint8_t scratch[kLargestTypeSize];
        uint8_t* val_to_add;
        if (!client_set_to_null) {
          RETURN_NOT_OK(decoder.ReadColumn(col, scratch));
          val_to_add = scratch;
        } else {
          DCHECK(col.is_nullable());
          val_to_add = NULL;
        }
        rcl_encoder.AddColumnUpdate(tablet_schema.column_id(tablet_col_idx), val_to_add);
      }
    }

    if (PREDICT_FALSE(buf.size() == 0)) {
      // No actual column updates specified!
      return Status::InvalidArgument("No fields updated, key is",
                                     tablet_schema.DebugRowKey(rowkey));
    }

    // Copy the row-changelist to the arena.
    uint8_t* rcl_in_arena = reinterpret_cast<uint8_t*>(
      dst_arena->AllocateBytesAligned(buf.size(), 8));
    if (PREDICT_FALSE(rcl_in_arena == NULL)) {
      return Status::RuntimeError("Out of memory allocating RCL");
    }
    memcpy(rcl_in_arena, buf.data(), buf.size());

    // Append results.
    row_keys->push_back(rowkey_storage);
    changelists->push_back(RowChangeList(Slice(rcl_in_arena, buf.size())));
  }
  return Status::OK();
}

// TODO: share more code with above two methods
Status PartialRow::DecodeAndProjectDeletes(
    const RowOperationsPB& pb,
    const Schema& client_schema,
    const Schema& tablet_schema,
    std::vector<uint8_t*>* row_keys,
    std::vector<RowChangeList>* changelists,
    Arena* dst_arena) {
  CHECK(!client_schema.has_column_ids());
  DCHECK(tablet_schema.has_column_ids());

  // TODO(perf): in the server, we do this for the updates and the inserts
  // separately, but we could do it just once.
  ClientServerMapping mapping(&client_schema, &tablet_schema);
  RETURN_NOT_OK(client_schema.GetProjectionMapping(tablet_schema, &mapping));
  DCHECK_EQ(mapping.num_mapped(), client_schema.num_columns());

  DCHECK_KEY_PROJECTION_SCHEMA_EQ(client_schema, tablet_schema);
  int rowkey_size = tablet_schema.key_byte_size();

  PBDecoder decoder(&client_schema, &pb);
  uint8_t client_isset_map[decoder.bitmap_size()];
  uint8_t client_null_map[decoder.bitmap_size()];
  memset(client_null_map, 0, decoder.bitmap_size());

  while (decoder.has_next()) {
    RowOperationsPB::Type type;
    RETURN_NOT_OK(decoder.ReadOpType(&type));
    if (PREDICT_FALSE(type != RowOperationsPB::DELETE)) {
      return Status::Corruption("Expected only DELETE operations");
    }

    // Read the null and isset bitmaps for the client-provided row into
    // our stack storage.
    RETURN_NOT_OK(decoder.ReadIssetBitmap(client_isset_map));
    if (client_schema.has_nullables()) {
      RETURN_NOT_OK(decoder.ReadNullBitmap(client_null_map));
    }

    // Allocate space for the row key.
    uint8_t* rowkey_storage = reinterpret_cast<uint8_t*>(
      dst_arena->AllocateBytesAligned(rowkey_size, 8));
    if (PREDICT_FALSE(!rowkey_storage)) {
      return Status::RuntimeError("Out of memory");
    }

    // We're passing the full schema instead of the key schema here.
    // That's OK because the keys come at the bottom. We lose some bounds
    // checking in debug builds, but it avoids an extra copy of the key schema.
    ContiguousRow rowkey(tablet_schema, rowkey_storage);

    // First process the key columns.
    int client_col_idx = 0;
    for (; client_col_idx < client_schema.num_key_columns(); client_col_idx++) {
      // Look up the corresponding column from the tablet. We use the server-side
      // ColumnSchema object since it has the most up-to-date default, nullability,
      // etc.
      DCHECK_EQ(mapping.client_to_tablet_idx(client_col_idx),
                client_col_idx) << "key columns should match";
      int tablet_col_idx = client_col_idx;

      const ColumnSchema& col = tablet_schema.column(tablet_col_idx);
      if (PREDICT_FALSE(!BitmapTest(client_isset_map, client_col_idx))) {
        return Status::InvalidArgument("No value provided for key column",
                                       col.ToString());
      }

      bool client_set_to_null = BitmapTest(client_null_map, client_col_idx);
      if (PREDICT_FALSE(client_set_to_null)) {
        return Status::InvalidArgument("NULL values not allowed for key column",
                                       col.ToString());
      }

      RETURN_NOT_OK(decoder.ReadColumn(col, rowkey.mutable_cell_ptr(tablet_col_idx)));
    }

    // Ensure that no other columns are set.
    for (; client_col_idx < client_schema.num_columns(); client_col_idx++) {
      if (BitmapTest(client_isset_map, client_col_idx)) {
        int tablet_col_idx = mapping.client_to_tablet_idx(client_col_idx);
        DCHECK_GE(tablet_col_idx, 0);
        const ColumnSchema& col = tablet_schema.column(tablet_col_idx);

        return Status::InvalidArgument("DELETE should not have a value for column",
                                       col.ToString());
      }
    }

    changelists->push_back(RowChangeList::CreateDelete());
    row_keys->push_back(rowkey_storage);
  }
  return Status::OK();
}

} // namespace kudu
