// Copyright (c) 2013, Cloudera, inc.
//
// Row changelists are simply an encoded form of a list of updates to columns
// within a row. These are stored within the delta memstore and delta files.
#ifndef KUDU_COMMON_ROW_CHANGELIST_H
#define KUDU_COMMON_ROW_CHANGELIST_H

#include <gtest/gtest.h>
#include <string>

#include "common/row.h"
#include "common/schema.h"
#include "gutil/casts.h"
#include "util/coding.h"
#include "util/coding-inl.h"
#include "util/faststring.h"
#include "util/memory/arena.h"

namespace kudu {


// A RowChangeList is a wrapper around a Slice which contains a "changelist".
//
// A changelist is a single mutation to a row -- it may be one of three types:
//  - UPDATE (set a new value for one or more columns)
//  - DELETE (remove the row)
//  - REINSERT (re-insert a "ghost" row, used only in the MemRowSet)
//
// RowChangeLists should be constructed using RowChangeListEncoder, and read
// using RowChangeListDecoder. NOTE that the schema passed to the Decoder must
// be the same one used by the Encoder.
class RowChangeList {
 public:
  RowChangeList() {}

  explicit RowChangeList(const faststring &fs)
    : encoded_data_(fs) {
  }

  explicit RowChangeList(const Slice &s)
    : encoded_data_(s) {
  }

  const Slice &slice() const { return encoded_data_; }

  // Return a string form of this changelist.
  string ToString(const Schema &schema) const;

  bool is_reinsert() const {
    DCHECK_GT(encoded_data_.size(), 0);
    return encoded_data_[0] == kReinsert;
  }

  enum ChangeType {
    ChangeType_min = 0,
    kUninitialized = 0,
    kUpdate = 1,
    kDelete = 2,
    kReinsert = 3,
    ChangeType_max = 3
  };

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
    type_(RowChangeList::kUninitialized),
    dst_(dst)
  {}

  void Reset() {
    dst_->clear();
    type_ = RowChangeList::kUninitialized;
  }

  void SetToDelete() {
    SetType(RowChangeList::kDelete);
  }

  // TODO: This doesn't currently copy the indirected data, so
  // REINSERT deltas can't possibly work anywhere but in memory.
  // For now, there is an assertion in the DeltaFile flush code
  // that prevents us from accidentally depending on this anywhere
  // but in-memory.
  void SetToReinsert(const Slice &row_data) {
    SetType(RowChangeList::kReinsert);
    dst_->append(row_data.data(), row_data.size());
  }

  void AddColumnUpdate(size_t col_idx, const void *new_val) {
    if (type_ == RowChangeList::kUninitialized) {
      SetType(RowChangeList::kUpdate);
    } else {
      DCHECK_EQ(RowChangeList::kUpdate, type_);
    }

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
    DCHECK_GT(dst_->size(), 0);
    return RowChangeList(*dst_);
  }

 private:
  void SetType(RowChangeList::ChangeType type) {
    DCHECK_EQ(type_, RowChangeList::kUninitialized);
    type_ = type;
    dst_->push_back(type);
  }

  const Schema &schema_;
  RowChangeList::ChangeType type_;
  faststring *dst_;
};


class RowChangeListDecoder {
 public:

  // Construct a new decoder.
  // NOTE: The 'schema' must be the same one used to encode the RowChangeList.
  // NOTE: The 'schema' parameter is stored by reference, rather than copied.
  // It is assumed that this class is only used in tightly scoped contexts where
  // this is appropriate.
  RowChangeListDecoder(const Schema &schema,
                       const RowChangeList &src)
    : schema_(schema),
      remaining_(src.slice()),
      type_(RowChangeList::kUninitialized) {
  }

  // Initialize the decoder. This will return an invalid Status if the RowChangeList
  // appears to be corrupt/malformed.
  Status Init();

  bool HasNext() const {
    DCHECK(!is_delete());
    return !remaining_.empty();
  }

  bool is_update() const {
    return type_ == RowChangeList::kUpdate;
  }

  bool is_delete() const {
    return type_ == RowChangeList::kDelete;
  }

  bool is_reinsert() const {
    return type_ == RowChangeList::kReinsert;
  }

  Slice reinserted_row_slice() const {
    DCHECK(is_reinsert());
    return remaining_;
  }

  template<class RowType, class ArenaType>
  Status ApplyRowUpdate(RowType *dst_row, ArenaType *arena) {
    // TODO: Handle different schema
    DCHECK(schema_.Equals(dst_row->schema()));

    while (HasNext()) {
      size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
      const void *new_val = NULL;
      RETURN_NOT_OK(DecodeNext(&updated_col, &new_val));

      SimpleConstCell src(schema_.column(updated_col), new_val);
      typename RowType::Cell dst_cell = dst_row->cell(updated_col);
      RETURN_NOT_OK(CopyCell(src, &dst_cell, arena));
    }
    return Status::OK();
  }

  // TODO: It will be nice have the same function taking the destination type
  //       to been able to call the "alter type" adapter.
  // This method is used by MemRowSet, DeltaMemStore and DeltaFile.
  template<class ColumnType, class ArenaType>
  Status ApplyToOneColumn(size_t row_idx, ColumnType *dst_col, size_t col_idx, ArenaType *arena) {
    DCHECK_EQ(RowChangeList::kUpdate, type_);
    while (HasNext()) {
      size_t updated_col = 0xdeadbeef; // avoid un-initialized usage warning
      const void *new_val = NULL;
      RETURN_NOT_OK(DecodeNext(&updated_col, &new_val));
      if (updated_col == col_idx) {
        SimpleConstCell src(schema_.column(updated_col), new_val);
        typename ColumnType::Cell dst_cell = dst_col->cell(row_idx);
        RETURN_NOT_OK(CopyCell(src, &dst_cell, arena));
        // TODO: could potentially break; here if we're guaranteed to only have one update
        // per column in a RowChangeList (which would make sense!)
      }
    }
    return Status::OK();
  }

  // If this changelist is a DELETE or REINSERT, twiddle '*deleted' to reference
  // the new state of the row. If it is an UPDATE, this call has no effect.
  //
  // This is used during mutation traversal, to keep track of whether a row is
  // deleted or not.
  void TwiddleDeleteStatus(bool *deleted) {
    if (is_delete()) {
      DCHECK(!*deleted);
      *deleted = true;
    } else if (is_reinsert()) {
      DCHECK(*deleted);
      *deleted = false;
    }
  }

  // Project the 'src' RowChangeList using the delta 'projector'
  // The projected RowChangeList will be encoded to specified 'buf'.
  // The buffer will be cleared before adding the result.
  static Status ProjectUpdate(const DeltaProjector& projector,
                              const RowChangeList& src,
                              faststring *buf);

 private:
  FRIEND_TEST(TestRowChangeList, TestEncodeDecodeUpdates);
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
    DCHECK_NE(type_, RowChangeList::kUninitialized) << "Must call Init()";
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

  // Data remaining in the source buffer.
  // This slice is advanced forward as entries are decoded.
  Slice remaining_;

  RowChangeList::ChangeType type_;

  // If an update is encountered which uses indirect data (eg a string update), then
  // this Slice is used as temporary storage to point to that indirected data.
  Slice last_decoded_slice_;
};


} // namespace kudu

// Defined for tight_enum_test_cast<> -- has to be defined outside of any namespace.
MAKE_ENUM_LIMITS(kudu::RowChangeList::ChangeType,
                 kudu::RowChangeList::ChangeType_min,
                 kudu::RowChangeList::ChangeType_max);

#endif
