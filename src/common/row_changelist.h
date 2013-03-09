// Copyright (c) 2013, Cloudera, inc.
//
// Row changelists are simply an encoded form of a list of updates to columns
// within a row. These are stored within the delta memstore and delta files.
#ifndef KUDU_COMMON_ROW_CHANGELIST_H
#define KUDU_COMMON_ROW_CHANGELIST_H

#include "util/coding.h"
#include "util/coding-inl.h"
#include "util/faststring.h"

namespace kudu {

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

private:
  const Schema &schema_;

  // The source data being decoded. This slice is advanced forward
  // as entries are decoded.
  Slice src_;


  Slice last_decoded_slice_;
};


} // namespace kudu


#endif
