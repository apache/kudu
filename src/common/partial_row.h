// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_COMMON_PARTIAL_ROW_H
#define KUDU_COMMON_PARTIAL_ROW_H

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "common/types.h"
#include "common/row.h"
#include "common/wire_protocol.pb.h"
#include "gutil/macros.h"
#include "util/slice.h"

namespace kudu {

class Arena;
class RowChangeList;
class Schema;

// A row which may only contain values for a subset of the columns.
// This type contains a normal contiguous row, plus a bitfield indicating
// which columns have been set. Additionally, this type may optionally own
// copies of indirect data (eg STRING values).
class PartialRow {
 public:
  // The given Schema object must remain valid for the lifetime of this
  // row.
  explicit PartialRow(const Schema* schema);
  virtual ~PartialRow();

  //------------------------------------------------------------
  // Setters
  //------------------------------------------------------------

  Status SetInt8(const Slice& col_name, int8_t val);
  Status SetInt16(const Slice& col_name, int16_t val);
  Status SetInt32(const Slice& col_name, int32_t val);
  Status SetInt64(const Slice& col_name, int64_t val);

  Status SetUInt8(const Slice& col_name, uint8_t val);
  Status SetUInt16(const Slice& col_name, uint16_t val);
  Status SetUInt32(const Slice& col_name, uint32_t val);
  Status SetUInt64(const Slice& col_name, uint64_t val);

  // Same as above setters, but with numeric column indexes.
  // These are faster since they avoid a hashmap lookup, so should
  // be preferred in performance-sensitive code (eg bulk loaders).
  Status SetInt8(int col_idx, int8_t val);
  Status SetInt16(int col_idx, int16_t val);
  Status SetInt32(int col_idx, int32_t val);
  Status SetInt64(int col_idx, int64_t val);

  Status SetUInt8(int col_idx, uint8_t val);
  Status SetUInt16(int col_idx, uint16_t val);
  Status SetUInt32(int col_idx, uint32_t val);
  Status SetUInt64(int col_idx, uint64_t val);

  // Sets the string but does not copy the value. The string
  // must remain valid until the call to AppendToPB().
  Status SetString(const Slice& col_name, const Slice& val);
  Status SetString(int col_idx, const Slice& val);

  // Copies 'val' immediately.
  Status SetStringCopy(const Slice& col_name, const Slice& val);
  Status SetStringCopy(int col_idx, const Slice& val);

  // Set the given column to NULL. This will only succeed on nullable
  // columns. Use Unset(...) to restore a column to its default.
  Status SetNull(const Slice& col_name);
  Status SetNull(int col_idx);

  // Unsets the given column. Note that this is different from setting
  // it to NULL.
  Status Unset(const Slice& col_name);
  Status Unset(int col_idx);

  //------------------------------------------------------------
  // Getters
  //------------------------------------------------------------
  // These getters return a bad Status if the type does not match,
  // the value is unset, or the value is NULL. Otherwise they return
  // the current set value in *val.

  // Return true if the given column has been specified.
  bool IsColumnSet(const Slice& col_name) const;
  bool IsColumnSet(int col_idx) const;

  bool IsNull(const Slice& col_name) const;
  bool IsNull(int col_idx) const;

  Status GetInt8(const Slice& col_name, int8_t* val) const;
  Status GetInt16(const Slice& col_name, int16_t* val) const;
  Status GetInt32(const Slice& col_name, int32_t* val) const;
  Status GetInt64(const Slice& col_name, int64_t* val) const;

  Status GetUInt8(const Slice& col_name, uint8_t* val) const;
  Status GetUInt16(const Slice& col_name, uint16_t* val) const;
  Status GetUInt32(const Slice& col_name, uint32_t* val) const;
  Status GetUInt64(const Slice& col_name, uint64_t* val) const;

  // Same as above getters, but with numeric column indexes.
  // These are faster since they avoid a hashmap lookup, so should
  // be preferred in performance-sensitive code.
  Status GetInt8(int col_idx, int8_t* val) const;
  Status GetInt16(int col_idx, int16_t* val) const;
  Status GetInt32(int col_idx, int32_t* val) const;
  Status GetInt64(int col_idx, int64_t* val) const;

  Status GetUInt8(int col_idx, uint8_t* val) const;
  Status GetUInt16(int col_idx, uint16_t* val) const;
  Status GetUInt32(int col_idx, uint32_t* val) const;
  Status GetUInt64(int col_idx, uint64_t* val) const;

  // Gets the string but does not copy the value. Callers should
  // copy the resulting Slice if necessary.
  Status GetString(const Slice& col_name, Slice* val) const;
  Status GetString(int col_idx, Slice* val) const;

  //------------------------------------------------------------
  // Utility code
  //------------------------------------------------------------

  // Return true if all of the key columns have been specified
  // for this mutation.
  bool IsKeySet() const;

  // Return true if all columns have been specified.
  bool AllColumnsSet() const;

  std::string ToString() const;

  //------------------------------------------------------------
  // Serialization/deserialization support
  //------------------------------------------------------------

  // Append this partial row to the given protobuf.
  void AppendToPB(RowOperationsPB::Type op_type, RowOperationsPB* pb) const;

  // Parse this partial row out of the given protobuf.
  // 'offset' is the offset within the 'rows' field at which
  // to begin parsing.
  //
  // NOTE: any string fields in this PartialRow will continue to reference
  // the protobuf data, so the protobuf must remain valid.
  //
  // TODO: instead of 'type' being an out-parameter, we should probably rename
  // PartialRow entirely to RowOperation and set it as a member!
  // TODO: in fact, this method seems to only be used from tests - kill it?
  Status CopyFromPB(const RowOperationsPB& pb, int offset,
                    RowOperationsPB::Type* type);

  // Decode the given protobuf, which contains rows according to 'client_schema'.
  // As they are decoded, they are projected into 'tablet_schema', filling in any
  // default values, handling NULLs, etc. The resulting rows are pushed onto
  // '*rows', with their storage allocated from 'dst_arena'.
  static Status DecodeAndProject(const RowOperationsPB& pb,
                                 const Schema& client_schema,
                                 const Schema& tablet_schema,
                                 std::vector<uint8_t*>* rows,
                                 Arena* dst_arena);

  // Decode the given protobuf, which contains rows according to 'client_schema'.
  // The encoded rows represent updates -- they must each contain all of the key
  // columns. Any further columns that they contain are decoded as updates,
  // and a RowChangeList is allocated with the updated columns.
  // The row keys are made contiguous and returned in row_keys.
  // The resulting 'row_keys' and 'changelists' lists will be exactly the same length.
  // All allocations are done out of 'dst_arena'.
  // TODO: change the output vectors to be a vector of structs?
  static Status DecodeAndProjectUpdates(const RowOperationsPB& pb,
                                        const Schema& client_schema,
                                        const Schema& tablet_schema,
                                        std::vector<uint8_t*>* row_keys,
                                        std::vector<RowChangeList>* changelists,
                                        Arena* dst_arena);

  // Similar to the above, but for deletes.
  // Each row should have all of its key columns set, and none of its non-key-columns.
  // 'row_keys' is appended to with the decoded keys, and changelists is appended
  // to with "DELETE" changelists, one for each row. The keys are allocated out of
  // 'dst_arena'.
  static Status DecodeAndProjectDeletes(const RowOperationsPB& pb,
                                        const Schema& client_schema,
                                        const Schema& tablet_schema,
                                        std::vector<uint8_t*>* row_keys,
                                        std::vector<RowChangeList>* changelists,
                                        Arena* dst_arena);


  const Schema* schema() const { return schema_; }

  // Return this row as a contiguous row. This will crash unless all columns
  // are set.
  // TODO: this is only so that the existing insert RPC can be used with
  // PartialRow on the client side. We have to switch over the WriteRequestPB
  // to use RowOperationsPB instead, and then kill off this method.
  ConstContiguousRow as_contiguous_row() const {
    DCHECK(AllColumnsSet());
    return ConstContiguousRow(*schema_, row_data_);
  }

 private:
  template<DataType TYPE>
  Status Set(const Slice& col_name,
             const typename DataTypeTraits<TYPE>::cpp_type& val,
             bool owned = false);

  template<DataType TYPE>
  Status Set(int col_idx,
             const typename DataTypeTraits<TYPE>::cpp_type& val,
             bool owned = false);

  template<DataType TYPE>
  Status Get(const Slice& col_name,
             typename DataTypeTraits<TYPE>::cpp_type* val) const;

  template<DataType TYPE>
  Status Get(int col_idx,
             typename DataTypeTraits<TYPE>::cpp_type* val) const;


  // If the given column is a string whose memory is owned by this instance,
  // deallocates the value.
  // NOTE: Does not mutate the isset bitmap.
  // REQUIRES: col_idx must be a string column.
  void DeallocateStringIfSet(int col_idx);

  // Deallocate any strings whose memory is managed by this object.
  void DeallocateOwnedStrings();

  const Schema* schema_;

  // 1-bit set for any field which has been explicitly set. This is distinct
  // from NULL -- an "unset" field will take the server-side default on insert,
  // whereas a field explicitly set to NULL will override the default.
  uint8_t* isset_bitmap_;

  // 1-bit set for any strings whose memory is managed by this instance.
  // These strings need to be deallocated whenever the value is reset,
  // or when the instance is destructed.
  uint8_t* owned_strings_bitmap_;

  // The normal "contiguous row" format row data. Any column whose data is unset
  // or NULL can have undefined bytes.
  uint8_t* row_data_;

  DISALLOW_COPY_AND_ASSIGN(PartialRow);
};

} // namespace kudu
#endif /* KUDU_COMMON_PARTIAL_ROW_H */
