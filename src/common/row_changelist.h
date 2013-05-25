// Copyright (c) 2013, Cloudera, inc.
//
// Row changelists are simply an encoded form of a list of updates to columns
// within a row. These are stored within the delta memstore and delta files.
#ifndef KUDU_COMMON_ROW_CHANGELIST_H
#define KUDU_COMMON_ROW_CHANGELIST_H

#include <boost/noncopyable.hpp>
#include <gtest/gtest.h>

#include "common/schema.h"
#include "util/coding.h"
#include "util/coding-inl.h"
#include "util/faststring.h"
#include "util/memory/arena.h"

namespace kudu {

class RowChangeList {
public:
  RowChangeList() {}

  explicit RowChangeList(const faststring &fs) :
    encoded_data_(fs)
  {}

  explicit RowChangeList(const Slice &s) :
    encoded_data_(s)
  {}

  const Slice &slice() const { return encoded_data_; }

  // Return a string form of this changelist.
  string ToString(const Schema &schema) const;

private:
  Slice encoded_data_;
};

class RowChangeListEncoder {
public:
  // Construct a new encoder.
  // NOTE: The 'schema' parameter is stored by reference, rather than copied.
  // It is assumed that this class is only used in tightly scoped contexts where
  // this is appropriate.
  RowChangeListEncoder(const Schema &schema,
                       faststring *dst) :
    schema_(schema),
    dst_(dst)
  {}

  void AddColumnUpdate(size_t col_idx, const void *new_val) {
    const ColumnSchema& col_schema = schema_.column(col_idx);
    const TypeInfo &ti = col_schema.type_info();

    // Encode the column index
    InlinePutVarint32(dst_, col_idx);

    // If the column is nullable set the null flag
    if (col_schema.is_nullable()) {
      dst_->push_back(new_val == NULL);
      if (new_val == NULL) return;
    }

    // Copy the new value itself
    if (ti.type() == STRING) {
      const Slice *src = reinterpret_cast<const Slice *>(new_val);

      // If it's a Slice column, copy the length followed by the data.
      InlinePutVarint32(dst_, src->size());
      dst_->append(src->data(), src->size());
    } else {
      // Otherwise, just copy the data itself.
      dst_->append(new_val, ti.size());
    }
  }

  RowChangeList as_changelist() {
    return RowChangeList(*dst_);
  }

private:
  const Schema &schema_;

  faststring *dst_;
};


class RowChangeListDecoder {
public:

  // Construct a new encoder.
  // NOTE: The 'schema' parameter is stored by reference, rather than copied.
  // It is assumed that this class is only used in tightly scoped contexts where
  // this is appropriate.
  RowChangeListDecoder(const Schema &schema,
                       const RowChangeList &src) :
    schema_(schema),
    remaining_(src.slice())
  {}

  bool HasNext() const {
    return !remaining_.empty();
  }

  template<class RowType, class ArenaType>
  Status ApplyRowUpdate(RowType *dst_row, ArenaType *arena) {
    // TODO: Handle different schema
    DCHECK(schema_.Equals(dst_row->schema()));

    while (HasNext()) {
      size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
      const void *new_val = NULL;
      RETURN_NOT_OK(DecodeNext(&updated_col, &new_val));
      if (schema_.column(updated_col).is_nullable()) {
        RETURN_NOT_OK(dst_row->CopyNullableCell(schema_, updated_col, new_val, arena));
      } else {
        RETURN_NOT_OK(dst_row->CopyCell(schema_, updated_col, new_val, arena));
      }
    }
    return Status::OK();
  }

  template<class ColumnType, class ArenaType>
  Status ApplyToOneColumn(size_t row_idx, ColumnType *dst_col, size_t col_idx, ArenaType *arena) {
    while (HasNext()) {
      size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
      const void *new_val = NULL;
      RETURN_NOT_OK(DecodeNext(&updated_col, &new_val));
      if (updated_col == col_idx) {
        if (schema_.column(updated_col).is_nullable()) {
          RETURN_NOT_OK(dst_col->CopyNullableCell(row_idx, new_val, arena));
        } else {
          RETURN_NOT_OK(dst_col->CopyCell(row_idx, new_val, arena));
        }
        // TODO: could potentially break; here if we're guaranteed to only have one update
        // per column in a RowChangeList (which would make sense!)
      }
    }
    return Status::OK();
  }

private:
  FRIEND_TEST(TestRowChangeList, TestEncodeDecode);
  friend class RowChangeList;

  // Decode the next changed column.
  // Sets *col_idx to the changed column index.
  // Sets *val_out to point to the new value.
  //
  // *val_out may be set to temporary storage which is part of the
  // RowChangeListDecoder instance. So, the value is only valid until
  // the next call to DecodeNext.
  //
  // That is to say, in the case of a string column, *val_out will
  // be a temporary Slice object which is only temporarily valid.
  // But, that Slice object will itself point to data which is part
  // of the source data that was passed in.
  Status DecodeNext(size_t *col_idx, const void ** val_out) {
    // Decode the column index.
    uint32_t idx;
    if (!GetVarint32(&remaining_, &idx)) {
      return Status::Corruption("Invalid column index varint in delta");
    }

    *col_idx = idx;

    const ColumnSchema& col_schema = schema_.column(idx);
    const TypeInfo &ti = col_schema.type_info();

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
    if (ti.type() == STRING) {
      if (!GetLengthPrefixedSlice(&remaining_, &last_decoded_slice_)) {
        return Status::Corruption("invalid slice in delta");
      }

      *val_out = &last_decoded_slice_;

    } else {
      *val_out = remaining_.data();
      remaining_.remove_prefix(ti.size());
    }

    return Status::OK();
  }


  const Schema &schema_;

  // The source data being decoded.
  const Slice src_;

  // Data remaining in src_. This slice is advanced forward as entries are decoded.
  Slice remaining_;

  // If an update is encountered which uses indirect data (eg a string update), then
  // this Slice is used as temporary storage to point to that indirected data.
  Slice last_decoded_slice_;
};


} // namespace kudu


#endif
