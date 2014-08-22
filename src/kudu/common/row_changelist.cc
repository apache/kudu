// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>
#include "kudu/common/row.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/schema.h"
#include "kudu/gutil/strings/substitute.h"

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

      size_t proj_id = 0xdeadbeef; // avoid un-initialized usage warning
      if (projector.get_proj_col_from_base_id(col_id, &proj_id)) {
        encoder.AddColumnUpdate(proj_id, col_val);
      } else if (projector.get_proj_col_from_adapter_id(col_id, &proj_id)) {
        // TODO: Handle the "different type" case (adapter_cols_mapping)
        LOG(DFATAL) << "Alter type is not implemented yet";
        return Status::NotSupported("Alter type is not implemented yet");
      }
    }
  }
  return Status::OK();
}

Status RowChangeListDecoder::ApplyRowUpdate(RowBlockRow *dst_row, Arena *arena,
                                            RowChangeListEncoder* undo_encoder) {
  DCHECK(schema_->Equals(*dst_row->schema()));

  while (HasNext()) {
    size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
    const void *new_val = NULL;
    RETURN_NOT_OK(DecodeNext(&updated_col, &new_val));

    SimpleConstCell src(&schema_->column(updated_col), new_val);
    RowBlockRow::Cell dst_cell = dst_row->cell(updated_col);

    // save the old cell on the undo encoder
    undo_encoder->AddColumnUpdate(updated_col, dst_cell.ptr());

    // copy the new cell to the row
    RETURN_NOT_OK(CopyCell(src, &dst_cell, arena));
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

} // namespace kudu
