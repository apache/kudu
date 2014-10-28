// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include <string>

#include "kudu/common/columnblock.h"
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/faststring.h"

using strings::Substitute;

namespace kudu {

string RowChangeList::ToString(const Schema &schema) const {
  DCHECK_GT(encoded_data_.size(), 0);
  RowChangeListDecoder decoder(&schema, *this);

  Status s = decoder.Init();
  if (!s.ok()) {
    return "[invalid: " + s.ToString() + "]";
  }

  if (decoder.is_delete()) {
    return string("DELETE");
  } else if (decoder.is_reinsert()) {
    ConstContiguousRow row(&schema, decoder.remaining_);
    return string("REINSERT ") + schema.DebugRow(row);
  } else {
    CHECK(decoder.is_update()) << "Unknown changelist type!";
  }

  string ret = "SET ";

  bool first = true;
  while (decoder.HasNext()) {
    if (!first) {
      ret.append(", ");
    }
    first = false;

    size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
    const void *new_val = NULL;
    s = decoder.DecodeNext(&updated_col, &new_val);
    if (!s.ok()) {
      return "[invalid: " + s.ToString() + ", before corruption: " + ret + "]";
    }

    const ColumnSchema& col_schema = schema.column_by_id(updated_col);
    ret.append(col_schema.name());
    ret.append("=");
    if (col_schema.is_nullable() && new_val == NULL) {
      ret.append("NULL");
    } else {
      ret.append(col_schema.Stringify(new_val));
    }
  }

  return ret;
}

void RowChangeListEncoder::AddColumnUpdate(size_t col_id, const void *new_val) {
  if (type_ == RowChangeList::kUninitialized) {
    SetType(RowChangeList::kUpdate);
  } else {
    DCHECK_EQ(RowChangeList::kUpdate, type_);
  }

  const ColumnSchema& col_schema = schema_->column_by_id(col_id);
  const TypeInfo* ti = col_schema.type_info();

  InlinePutVarint32(dst_, col_id);

  // If the column is nullable set the null flag
  if (col_schema.is_nullable()) {
    dst_->push_back(new_val == NULL);
    if (new_val == NULL) return;
  }

  // Copy the new value itself
  if (ti->type() == STRING) {
    Slice src;
    memcpy(&src, new_val, sizeof(Slice));

    // If it's a Slice column, copy the length followed by the data.
    InlinePutVarint32(dst_, src.size());
    dst_->append(src.data(), src.size());
  } else {
    // Otherwise, just copy the data itself.
    dst_->append(new_val, ti->size());
  }
}

Status RowChangeListDecoder::Init() {
  if (PREDICT_FALSE(remaining_.empty())) {
    return Status::Corruption("empty changelist - expected type");
  }

  bool was_valid = tight_enum_test_cast<RowChangeList::ChangeType>(remaining_[0], &type_);
  if (PREDICT_FALSE(!was_valid || type_ == RowChangeList::kUninitialized)) {
    return Status::Corruption(Substitute("bad type enum value: $0 in $1",
                                         static_cast<int>(remaining_[0]),
                                         remaining_.ToDebugString()));
  }
  if (PREDICT_FALSE(is_delete() && remaining_.size() != 1)) {
    return Status::Corruption("DELETE changelist too long",
                              remaining_.ToDebugString());
  }

  if (PREDICT_FALSE(is_reinsert())) {
    int expected_size = ContiguousRowHelper::row_size(*schema_) + 1;
    if (remaining_.size() != expected_size) {
      return Status::Corruption(Substitute("REINSERT changelist wrong length (expected $0)",
                                           expected_size,
                                           remaining_.ToDebugString()));
    }
  }

  remaining_.remove_prefix(1);
  return Status::OK();
}

Status RowChangeListDecoder::ProjectUpdate(const DeltaProjector& projector,
                                           const RowChangeList& src,
                                           faststring *buf) {
  RowChangeListDecoder decoder(projector.delta_schema(), src);
  RETURN_NOT_OK(decoder.Init());

  buf->clear();
  RowChangeListEncoder encoder(projector.projection(), buf);
  if (decoder.is_delete()) {
    encoder.SetToDelete();
  } else if (decoder.is_reinsert()) {
    // ReInsert = MemStore Insert -> Delete -> (Re)Insert
    ConstContiguousRow src_row = ConstContiguousRow(projector.delta_schema(),
                                                    decoder.reinserted_row_slice());
    RowProjector row_projector(projector.delta_schema(), projector.projection());
    size_t row_size = ContiguousRowHelper::row_size(*projector.projection());
    uint8_t buffer[row_size];
    ContiguousRow row(projector.projection(), buffer);
    RETURN_NOT_OK(row_projector.Init());
    RETURN_NOT_OK(row_projector.ProjectRowForRead(src_row, &row, static_cast<Arena*>(NULL)));
    encoder.SetToReinsert(Slice(buffer, row_size));
  } else if (decoder.is_update()) {
    while (decoder.HasNext()) {
      size_t col_id = 0xdeadbeef; // avoid un-initialized usage warning
      const void *col_val = NULL;
      RETURN_NOT_OK(decoder.DecodeNext(&col_id, &col_val));

      // If the new schema doesn't have this column, throw away the update.
      if (projector.projection()->find_column_by_id(col_id) == Schema::kColumnNotFound) {
        continue;
      }

      encoder.AddColumnUpdate(col_id, col_val);
    }
  }
  return Status::OK();
}

Status RowChangeListDecoder::ApplyRowUpdate(RowBlockRow *dst_row, Arena *arena,
                                            RowChangeListEncoder* undo_encoder) {
  DCHECK(schema_->Equals(*dst_row->schema()));

  while (HasNext()) {
    size_t updated_col_id = 0xdeadbeef; // avoid un-initialized usage warning
    const void *new_val = NULL;
    RETURN_NOT_OK(DecodeNext(&updated_col_id, &new_val));

    int dst_idx = dst_row->schema()->find_column_by_id(updated_col_id);
    // TODO: I think this assertion may be invalid in some alter-table scenarios.
    // As we expand test coverage for alter-table, it might fail and need some fixing.
    CHECK_NE(dst_idx, static_cast<int>(Schema::kColumnNotFound));

    SimpleConstCell src(&schema_->column_by_id(updated_col_id), new_val);

    RowBlockRow::Cell dst_cell = dst_row->cell(dst_idx);

    // save the old cell on the undo encoder
    undo_encoder->AddColumnUpdate(updated_col_id, dst_cell.ptr());

    // copy the new cell to the row
    RETURN_NOT_OK(CopyCell(src, &dst_cell, arena));
  }
  return Status::OK();
}

Status RowChangeListDecoder::ApplyToOneColumn(size_t row_idx, ColumnBlock* dst_col,
                                              size_t col_idx, Arena *arena) {
  DCHECK_EQ(RowChangeList::kUpdate, type_);

  const ColumnSchema& col_schema = schema_->column(col_idx);
  size_t col_id = schema_->column_id(col_idx);

  // TODO: Handle the "different type" case (adapter_cols_mapping)
  DCHECK_EQ(col_schema.type_info()->type(), dst_col->type_info()->type());

  while (HasNext()) {
    size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
    const void *new_val = NULL;
    RETURN_NOT_OK(DecodeNext(&updated_col, &new_val));
    if (updated_col == col_id) {
      SimpleConstCell src(&col_schema, new_val);
      ColumnBlock::Cell dst_cell = dst_col->cell(row_idx);
      RETURN_NOT_OK(CopyCell(src, &dst_cell, arena));
      // TODO: could potentially break; here if we're guaranteed to only have one update
      // per column in a RowChangeList (which would make sense!)
    }
  }
  return Status::OK();
}

Status RowChangeListDecoder::RemoveColumnsFromChangeList(const RowChangeList& src,
                                                         const std::vector<size_t>& column_indexes,
                                                         const Schema &schema,
                                                         RowChangeListEncoder* out) {
  RowChangeListDecoder decoder(&schema, src);
  RETURN_NOT_OK(decoder.Init());
  if (decoder.is_delete()) {
    out->SetToDelete();
  } else if (decoder.is_reinsert()) {
    out->SetToReinsert(decoder.reinserted_row_slice());
  } else if (decoder.is_update()) {
    while (decoder.HasNext()) {
      size_t col_id = 0xdeadbeef;
      const void *col_val = NULL;
      RETURN_NOT_OK(decoder.DecodeNext(&col_id, &col_val));
      if (!std::binary_search(column_indexes.begin(), column_indexes.end(), col_id)) {
        out->AddColumnUpdate(col_id, col_val);
      }
    }
  }
  return Status::OK();
}

Status RowChangeListDecoder::DecodeNext(size_t *col_id, const void ** val_out) {
  DCHECK_NE(type_, RowChangeList::kUninitialized) << "Must call Init()";
  // Decode the column id.
  uint32_t id;
  if (!GetVarint32(&remaining_, &id)) {
    return Status::Corruption("Invalid column ID varint in delta");
  }

  *col_id = id;

  const ColumnSchema& col_schema = schema_->column_by_id(id);
  const TypeInfo* ti = col_schema.type_info();

  // If the column is nullable check the null flag
  if (col_schema.is_nullable()) {
    if (remaining_.size() < 1) {
      return Status::Corruption("Missing column nullable varint in delta");
    }

    int is_null = *remaining_.data();
    remaining_.remove_prefix(1);

    // The value is null
    if (is_null) {
      *val_out = NULL;
      return Status::OK();
    }
  }

  // Decode the value itself
  if (ti->type() == STRING) {
    if (!GetLengthPrefixedSlice(&remaining_, &last_decoded_slice_)) {
      return Status::Corruption("invalid slice in delta");
    }

    *val_out = &last_decoded_slice_;

  } else {
    *val_out = remaining_.data();
    remaining_.remove_prefix(ti->size());
  }

  return Status::OK();
}

} // namespace kudu
