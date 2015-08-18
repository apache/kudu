// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_COMMON_ROW_OPERATIONS_H
#define KUDU_COMMON_ROW_OPERATIONS_H

#include "kudu/common/row_changelist.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

#include <string>
#include <vector>

namespace kudu {

class Arena;
class KuduPartialRow;
class Schema;

class ClientServerMapping;

class RowOperationsPBEncoder {
 public:
  explicit RowOperationsPBEncoder(RowOperationsPB* pb);
  ~RowOperationsPBEncoder();

  // Append this partial row to the protobuf.
  void Add(RowOperationsPB::Type type, const KuduPartialRow& row);

 private:
  RowOperationsPB* pb_;

  DISALLOW_COPY_AND_ASSIGN(RowOperationsPBEncoder);
};

struct DecodedRowOperation {
  RowOperationsPB::Type type;

  // For INSERT, the whole projected row.
  // For UPDATE or DELETE, the row key.
  const uint8_t* row_data;

  // For UPDATE and DELETE types, the changelist
  RowChangeList changelist;

  // For SPLIT_ROW, the partial row to split on.
  shared_ptr<KuduPartialRow> split_row;

  std::string ToString(const Schema& schema) const;
};

class RowOperationsPBDecoder {
 public:
  RowOperationsPBDecoder(const RowOperationsPB* pb,
                         const Schema* client_schema,
                         const Schema* tablet_schema,
                         Arena* dst_arena);
  ~RowOperationsPBDecoder();

  Status DecodeOperations(std::vector<DecodedRowOperation>* ops);

 private:
  Status ReadOpType(RowOperationsPB::Type* type);
  Status ReadIssetBitmap(const uint8_t** bitmap);
  Status ReadNullBitmap(const uint8_t** null_bm);
  Status GetColumnSlice(const ColumnSchema& col, Slice* slice);
  Status ReadColumn(const ColumnSchema& col, uint8_t* dst);
  bool HasNext() const;

  Status DecodeInsert(const uint8_t* prototype_row_storage,
                      const ClientServerMapping& mapping,
                      DecodedRowOperation* op);
  //------------------------------------------------------------
  // Serialization/deserialization support
  //------------------------------------------------------------

  // Decode the next encoded operation, which must be UPDATE or DELETE.
  Status DecodeUpdateOrDelete(const ClientServerMapping& mapping,
                              DecodedRowOperation* op);

  // Decode the next encoded operation, which must be SPLIT_KEY.
  Status DecodeSplitRow(const ClientServerMapping& mapping,
                        DecodedRowOperation* op);

  const RowOperationsPB* const pb_;
  const Schema* const client_schema_;
  const Schema* const tablet_schema_;
  Arena* const dst_arena_;

  const int bm_size_;
  const int tablet_row_size_;
  Slice src_;


  DISALLOW_COPY_AND_ASSIGN(RowOperationsPBDecoder);
};
} // namespace kudu
#endif /* KUDU_COMMON_ROW_OPERATIONS_H */
