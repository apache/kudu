// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include <string>
#include "common/row.h"
#include "common/row_changelist.h"
#include "common/schema.h"
#include "gutil/strings/substitute.h"

using strings::Substitute;

namespace kudu {

string RowChangeList::ToString(const Schema &schema) const {
  DCHECK_GT(encoded_data_.size(), 0);
  RowChangeListDecoder decoder(schema, *this);

  Status s = decoder.Init();
  if (!s.ok()) {
    return "[invalid: " + s.ToString() + "]";
  }

  if (decoder.is_delete()) {
    return string("DELETE");
  } else if (decoder.is_reinsert()) {
    ConstContiguousRow row(schema, decoder.remaining_);
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
    int expected_size = ContiguousRowHelper::row_size(schema_) + 1;
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
    encoder.SetToReinsert(decoder.reinserted_row_slice());
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

} // namespace kudu
