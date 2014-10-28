// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Row changelists are simply an encoded form of a list of updates to columns
// within a row. These are stored within the delta memstore and delta files.
#ifndef KUDU_COMMON_ROW_CHANGELIST_H
#define KUDU_COMMON_ROW_CHANGELIST_H

#include <gtest/gtest_prod.h>
#include <string>
#include <vector>

#include "kudu/common/row.h"
#include "kudu/gutil/casts.h"
#include "kudu/util/bitmap.h"

namespace kudu {

class faststring;

class Arena;
class ColumnBlock;
class RowBlockRow;
class Schema;

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

  // Create a RowChangeList which represents a delete.
  // This points to static (const) memory and should not be
  // mutated or freed.
  static RowChangeList CreateDelete() {
    return RowChangeList(Slice("\x02"));
  }

  const Slice &slice() const { return encoded_data_; }

  // Return a string form of this changelist.
  string ToString(const Schema &schema) const;

  bool is_reinsert() const {
    DCHECK_GT(encoded_data_.size(), 0);
    return encoded_data_[0] == kReinsert;
  }

  bool is_delete() const {
    DCHECK_GT(encoded_data_.size(), 0);
    return encoded_data_[0] == kDelete;
  }

  bool is_null() const {
    return encoded_data_.size() == 0;
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
  // NOTE: The 'schema' parameter must remain valid as long as the encoder.
  RowChangeListEncoder(const Schema* schema,
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

  void AddColumnUpdate(size_t col_id, const void *new_val);

  RowChangeList as_changelist() {
    DCHECK_GT(dst_->size(), 0);
    return RowChangeList(*dst_);
  }

  bool is_initialized() const {
    return type_ != RowChangeList::kUninitialized;
  }

 private:
  void SetType(RowChangeList::ChangeType type) {
    DCHECK_EQ(type_, RowChangeList::kUninitialized);
    type_ = type;
    dst_->push_back(type);
  }

  const Schema* schema_;
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
  RowChangeListDecoder(const Schema* schema,
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


  // Sets bits in 'bitmap' that  correspond to column indexes that are
  // present in the underlying RowChangeList. The bitmap is not zeroed
  // by this function, it is the caller's responsibility to zero it
  // first.
  Status GetIncludedColumns(uint8_t* bitmap) {
    while (HasNext()) {
      size_t col_id = 0xdeadbeef;
      const void* col_val = NULL;
      RETURN_NOT_OK(DecodeNext(&col_id, &col_val));
      int col_idx = schema_->find_column_by_id(col_id);
      // TODO: likely need to fix this to skip the column if it's not in the
      // schema. This whole function is a little suspect that it's setting up
      // a bitmap, only for the caller to iterate back through the bitmap.
      // Maybe it should instead enqueue ids onto a vector, or just increment
      // counts directly?
      CHECK_NE(col_idx, -1);
      BitmapSet(bitmap, col_idx);
    }
    return Status::OK();
  }

  // Applies this changes in this decoder to the specified row and saves the old
  // state of the row into the undo_encoder.
  Status ApplyRowUpdate(RowBlockRow *dst_row, Arena *arena, RowChangeListEncoder* undo_encoder);

  // This method is used by MemRowSet, DeltaMemStore and DeltaFile.
  Status ApplyToOneColumn(size_t row_idx, ColumnBlock* dst_col, size_t col_idx, Arena *arena);

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

  // If 'src' is an update, then only add changes for columns NOT
  // specified by 'column_indexes' to 'out'. Delete and Re-insert
  // changes are added to 'out' as-is. If an update only contained
  // changes for 'column_indexes', then out->is_initialized() will
  // return false.
  // 'column_indexes' must be sorted; 'out' must be
  // valid for the duration of this method, but not have been
  // previously initialized.
  //
  // TODO: 'column_indexes' is actually being treated like 'column_ids'
  // in this function. No tests are currently failing, but this is almost
  // certainly wrong. Need to track down callers.
  static Status RemoveColumnsFromChangeList(const RowChangeList& src,
                                            const std::vector<size_t>& column_indexes,
                                            const Schema& schema,
                                            RowChangeListEncoder* out);

 private:
  FRIEND_TEST(TestRowChangeList, TestEncodeDecodeUpdates);
  friend class RowChangeList;

  // Decode the next changed column.
  // Sets *col_id to the changed column id.
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
  Status DecodeNext(size_t *col_id, const void ** val_out);

  const Schema* schema_;

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
