// Copyright (c) 2013, Cloudera, inc.
//
// Row changelists are simply an encoded form of a list of updates to columns
// within a row. These are stored within the delta memstore and delta files.
#ifndef KUDU_COMMON_ROW_CHANGELIST_H
#define KUDU_COMMON_ROW_CHANGELIST_H

#include <boost/noncopyable.hpp>

#include "common/schema.h"
#include "util/coding.h"
#include "util/coding-inl.h"
#include "util/faststring.h"
#include "util/memory/arena.h"

namespace kudu {

class RowChangeList {
public:
  explicit RowChangeList(const faststring &fs) :
    encoded_data_(fs)
  {}

  explicit RowChangeList(const Slice &s) :
    encoded_data_(s)
  {}

  const Slice &slice() const { return encoded_data_; }

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
    const TypeInfo &ti = schema_.column(col_idx).type_info();\

    // Encode the column index
    InlinePutVarint32(dst_, col_idx);

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
                       const Slice &src) :
    schema_(schema),
    src_(src)
  {}

  bool HasNext() const {
    return !src_.empty();
  }

  // Decode the next changed row.
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
    CHECK(HasNext()) << "Should not call DecodeNext() when !HasNext()";

    // Decode the column index.
    uint32_t idx;
    if (!GetVarint32(&src_, &idx)) {
      return Status::Corruption("Invalid column index varint in delta");
    }

    *col_idx = idx;

    const TypeInfo &ti = schema_.column(idx).type_info();

    // Decode the value itself
    if (ti.type() == STRING) {
      if (!GetLengthPrefixedSlice(&src_, &last_decoded_slice_)) {
        return Status::Corruption("invalid slice in delta");
      }

      *val_out = &last_decoded_slice_;

    } else {
      *val_out = src_.data();
      src_.remove_prefix(ti.size());
    }

    return Status::OK();
  }

  template<class ARENA>
  Status ApplyRowUpdate(Slice *dst_row, ARENA *arena) {
    DCHECK_EQ(dst_row->size(), schema_.byte_size());

    while (HasNext()) {
      size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
      const void *new_val = NULL;
      RETURN_NOT_OK(DecodeNext(&updated_col, &new_val));
      uint8_t *dst_cell = dst_row->mutable_data() + schema_.column_offset(updated_col);
      schema_.column(updated_col).CopyCell(dst_cell, new_val, arena);
    }
    return Status::OK();
  }

  template<class ARENA>
  Status ApplyToOneColumn(size_t col_idx, void *dst_cell, ARENA *arena) {
    while (HasNext()) {
      size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
      const void *new_val = NULL;
      RETURN_NOT_OK(DecodeNext(&updated_col, &new_val));
      if (updated_col == col_idx) {
        schema_.column(col_idx).CopyCell(dst_cell, new_val, arena);
        // TODO: could potentially break; here if we're guaranteed to only have one update
        // per column in a RowChangeList (which would make sense!)
      }
    }
    return Status::OK();
  }

  // Return a string form of this changelist.
  string ToString() {
    Slice save_src_(src_);
    string ret = "SET ";

    bool first = true;
    while (HasNext()) {
      if (!first) {
        ret.append(", ");
      }
      first = false;

      size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
      const void *new_val = NULL;
      CHECK_OK(DecodeNext(&updated_col, &new_val));

      ret.append(schema_.column(updated_col).name());
      ret.append("=");
      ret.append(schema_.column(updated_col).Stringify(new_val));
    }

    // Reset state.
    src_ = save_src_;

    return ret;
  }


private:
  const Schema &schema_;

  // The source data being decoded. This slice is advanced forward
  // as entries are decoded.
  Slice src_;


  Slice last_decoded_slice_;
};


} // namespace kudu


#endif
