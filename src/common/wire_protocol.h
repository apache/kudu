// Copyright (c) 2013, Cloudera, inc.
//
// Helpers for dealing with the protobufs defined in wire_protocol.proto.
#ifndef KUDU_COMMON_WIRE_PROTOCOL_H
#define KUDU_COMMON_WIRE_PROTOCOL_H

#include <vector>

#include "common/wire_protocol.pb.h"
#include "util/status.h"

namespace kudu {

class ConstContiguousRow;
class ColumnSchema;
class RowBlock;
class RowBlockRow;
class RowChangeList;
class Schema;

// Convert the given C++ Status object into the equivalent Protobuf.
void StatusToPB(const Status& status, AppStatusPB* pb);

// Convert the given protobuf into the equivalent C++ Status object.
Status StatusFromPB(const AppStatusPB& pb);

// Convert the specified schema to protobuf.
Status SchemaToPB(const Schema& schema, SchemaPB *pb);

// Returns the Schema created from the specified protobuf.
// If the schema is invalid, return a non-OK status.
Status SchemaFromPB(const SchemaPB& pb, Schema *schema);

// Convert the specified column schema to protobuf.
void ColumnSchemaToPB(const ColumnSchema& schema, ColumnSchemaPB *pb);

// Return the ColumnSchema created from the specified protobuf.
ColumnSchema ColumnSchemaFromPB(const ColumnSchemaPB& pb);

// Convert the given list of ColumnSchemaPB objects into a Schema object.
//
// Returns InvalidArgument if the provided columns don't make a valid Schema
// (eg if the keys are non-contiguous or nullable).
Status ColumnPBsToSchema(
  const google::protobuf::RepeatedPtrField<ColumnSchemaPB>& column_pbs,
  Schema* schema);

// Extract the columns of the given Schema into protobuf objects.
//
// The 'cols' list is replaced by this method.
Status SchemaToColumnPBs(
  const Schema& schema,
  google::protobuf::RepeatedPtrField<ColumnSchemaPB>* cols);

// Encode the given row into the provided protobuf.
//
// All data (both direct and indirect) is copied into the protobuf by this method,
// so the original row may be destroyed safely after this returns.
void AddRowToRowBlockPB(const ConstContiguousRow& row, RowwiseRowBlockPB* pb);

// Same as above, but for RowBlockRow. Typically, one should use ConvertRowBlockToPB
// below, rather than looping through this function.
// TODO: remove it entirely?
void AddRowToRowBlockPB(const RowBlockRow& row, RowwiseRowBlockPB* pb);

// Similar to the above, but converts a whole RowBlock at a time.
// This only converts those rows whose selection vector entry is true.
void ConvertRowBlockToPB(const RowBlock& block, RowwiseRowBlockPB* pb);

// Extract the rows stored in this protobuf, which must have exactly the
// given Schema. This Schema may be obtained using ColumnPBsToSchema.
//
// Pointers are added to 'rows' for each of the extracted rows. These
// pointers are suitable for constructing ConstContiguousRow objects.
// TODO: would be nice to just return a vector<ConstContiguousRow>, but
// they're not currently copyable, so this can't be done.
//
// Note that the returned rows refer directly to memory managed by 'rowblock_pb';
// thus, the protobuf may not be safely destroyed until the rows are no longer
// needed. This is also the reason that 'rowblock_pb' is a non-const pointer
// argument: the internal data is mutated in-place to restore the validity of
// indirect data pointers, which are relative on the wire but must be absolute
// while in-memory.
//
// Returns a bad Status if the provided data is invalid or corrupt.
Status ExtractRowsFromRowBlockPB(const Schema& schema,
                                 RowwiseRowBlockPB* rowblock_pb,
                                 std::vector<const uint8_t*>* rows);

// Extracts n_rows mutations from the provided buffer into the provided
// mutations vector. The resulting mutations continue to refer to data in
// 'buffer'.
//
// NOTE: the mutations are not validated in any way by this function.
// In order to validate, you need to instantiate a RowChangeListDecoder.
Status ExtractMutationsFromBuffer(uint32_t n_mutations,
                                  const uint8_t* buffer,
                                  uint32_t buffer_size,
                                  vector<const RowChangeList *> *mutations);

} // namespace kudu
#endif
