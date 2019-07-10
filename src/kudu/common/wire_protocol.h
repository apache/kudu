// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// Helpers for dealing with the protobufs defined in wire_protocol.proto.
#ifndef KUDU_COMMON_WIRE_PROTOCOL_H
#define KUDU_COMMON_WIRE_PROTOCOL_H

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "kudu/util/status.h"

namespace boost {
template <class T>
class optional;
}

namespace google {
namespace protobuf {
template <typename Element> class RepeatedPtrField;
template <typename Key, typename T> class Map;
}
}

namespace kudu {

class Arena;
class ColumnPredicate;
class ColumnSchema;
class faststring;
class HostPort;
class RowBlock;
class Schema;
class Slice;
class Sockaddr;
struct ColumnSchemaDelta;

class AppStatusPB;
class ColumnPredicatePB;
class ColumnSchemaDeltaPB;
class ColumnSchemaPB;
class HostPortPB;
class RowwiseRowBlockPB;
class SchemaPB;
class ServerEntryPB;
class ServerRegistrationPB;
class TableExtraConfigPB;

// Convert the given C++ Status object into the equivalent Protobuf.
void StatusToPB(const Status& status, AppStatusPB* pb);

// Convert the given protobuf into the equivalent C++ Status object.
Status StatusFromPB(const AppStatusPB& pb);

// Convert the specified HostPort to protobuf.
Status HostPortToPB(const HostPort& host_port, HostPortPB* host_port_pb);

// Returns the HostPort created from the specified protobuf.
Status HostPortFromPB(const HostPortPB& host_port_pb, HostPort* host_port);

// Convert the column schema delta `col_delta` to protobuf.
void ColumnSchemaDeltaToPB(const ColumnSchemaDelta& col_delta, ColumnSchemaDeltaPB *pb);

// Return the ColumnSchemaDelta created from the protobuf `pb`.
// The protobuf must outlive the returned ColumnSchemaDelta.
ColumnSchemaDelta ColumnSchemaDeltaFromPB(const ColumnSchemaDeltaPB& pb);

  // Adds addresses in 'addrs' to 'pbs'. If an address is a wildcard
// (e.g., "0.0.0.0"), then the local machine's hostname is used in
// its place.
Status AddHostPortPBs(const std::vector<Sockaddr>& addrs,
                      google::protobuf::RepeatedPtrField<HostPortPB>* pbs);

enum SchemaPBConversionFlags {
  SCHEMA_PB_WITHOUT_IDS = 1 << 0,
  SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES = 1 << 1,

  // When serializing, only write the 'read_default' value into the
  // protobuf. Used when sending schemas from the client to the master
  // for create/alter table.
  SCHEMA_PB_WITHOUT_WRITE_DEFAULT = 1 << 2,

  SCHEMA_PB_WITHOUT_COMMENT = 1 << 3,
};

// Convert the specified schema to protobuf.
// 'flags' is a bitfield of SchemaPBConversionFlags values.
Status SchemaToPB(const Schema& schema, SchemaPB* pb, int flags = 0);

// Returns the Schema created from the specified protobuf.
// If the schema is invalid, return a non-OK status.
Status SchemaFromPB(const SchemaPB& pb, Schema *schema);

// Convert the specified column schema to protobuf.
// 'flags' is a bitfield of SchemaPBConversionFlags values.
void ColumnSchemaToPB(const ColumnSchema& schema, ColumnSchemaPB *pb, int flags = 0);

// Return the ColumnSchema created from the specified protobuf.
// If the column schema is invalid, return a non-OK status.
Status ColumnSchemaFromPB(const ColumnSchemaPB& pb, boost::optional<ColumnSchema>* col_schema);

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
// 'flags' is a bitfield of SchemaPBConversionFlags values.
Status SchemaToColumnPBs(
  const Schema& schema,
  google::protobuf::RepeatedPtrField<ColumnSchemaPB>* cols,
  int flags = 0);

// Convert the column predicate to protobuf.
void ColumnPredicateToPB(const ColumnPredicate& predicate, ColumnPredicatePB* pb);

// Convert a column predicate protobuf to a column predicate. The resulting
// predicate is stored in the 'predicate' out parameter, if the result is
// successful.
Status ColumnPredicateFromPB(const Schema& schema,
                             Arena* arena,
                             const ColumnPredicatePB& pb,
                             boost::optional<ColumnPredicate>* predicate);

// Convert a extra configuration properties protobuf to map.
Status ExtraConfigPBToMap(const TableExtraConfigPB& pb,
                          std::map<std::string, std::string>* configs);

// Convert the table's extra configuration protobuf::map to protobuf.
Status ExtraConfigPBFromPBMap(const google::protobuf::Map<std::string, std::string>& configs,
                              TableExtraConfigPB* pb);

// Parse int32_t type value from 'value', and store in 'result' when succeed.
Status ParseInt32Config(const std::string& name, const std::string& value, int32_t* result);

// Convert a extra configuration properties protobuf to protobuf::map.
Status ExtraConfigPBToPBMap(const TableExtraConfigPB& pb,
                            google::protobuf::Map<std::string, std::string>* configs);

// Encode the given row block into the provided protobuf and data buffers.
//
// All data (both direct and indirect) for each selected row in the RowBlock is
// copied into the protobuf and faststrings.
// The original data may be destroyed safely after this returns.
//
// This only converts those rows whose selection vector entry is true.
// If 'client_projection_schema' is not NULL, then only columns specified in
// 'client_projection_schema' will be projected to 'data_buf'.
//
// If 'pad_unixtime_micros_to_16_bytes' is true, UNIXTIME_MICROS slots in the projection
// schema will be padded to the right by 8 (zero'd) bytes for a total of 16 bytes.
//
// Requires that block.nrows() > 0
void SerializeRowBlock(const RowBlock& block, RowwiseRowBlockPB* rowblock_pb,
                       const Schema* projection_schema,
                       faststring* data_buf, faststring* indirect_data,
                       bool pad_unixtime_micros_to_16_bytes = false);

// Rewrites the data pointed-to by row data slice 'row_data_slice' by replacing
// relative indirect data pointers with absolute ones in 'indirect_data_slice'.
// At the time of this writing, this rewriting is only done for STRING types.
//
// It 'pad_unixtime_micros_to_16_bytes' is true, this function will take padding into
// account when rewriting the block pointers.
// See: SerializeRowBlock() for the actual format.
//
// Returns a bad Status if the provided data is invalid or corrupt.
Status RewriteRowBlockPointers(const Schema& schema, const RowwiseRowBlockPB& rowblock_pb,
                               const Slice& indirect_data_slice, Slice* row_data_slice,
                               bool pad_unixtime_micros_to_16_bytes = false);

// Extract the rows stored in this protobuf, which must have exactly the
// given Schema. This Schema may be obtained using ColumnPBsToSchema.
//
// Pointers are added to 'rows' for each of the extracted rows. These
// pointers are suitable for constructing ConstContiguousRow objects.
// TODO: would be nice to just return a vector<ConstContiguousRow>, but
// they're not currently copyable, so this can't be done.
//
// Note that the returned rows refer to memory managed by 'rows_data' and
// 'indirect_data'. This is also the reason that 'rows_data' is a non-const pointer
// argument: the internal data is mutated in-place to restore the validity of
// indirect data pointers, which are relative on the wire but must be absolute
// while in-memory.
//
// Returns a bad Status if the provided data is invalid or corrupt.
Status ExtractRowsFromRowBlockPB(const Schema& schema,
                                 const RowwiseRowBlockPB& rowblock_pb,
                                 const Slice& indirect_data,
                                 Slice* rows_data,
                                 std::vector<const uint8_t*>* rows);

// Set 'leader_hostport' to the host/port of the leader server if one
// can be found in 'entries'.
//
// Returns Status::NotFound if no leader is found.
Status FindLeaderHostPort(const google::protobuf::RepeatedPtrField<ServerEntryPB>& entries,
                          HostPort* leader_hostport);

// Extract 'start_time' in ServerRegistrationPB, and convert it to localtime as a string
// or '<unknown>' if lack this field.
std::string StartTimeToString(const ServerRegistrationPB& reg);
} // namespace kudu
#endif
