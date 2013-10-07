// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TABLET_METADATA_UTIL_H
#define KUDU_TABLET_METADATA_UTIL_H

#include <tr1/unordered_map>
#include <tr1/unordered_set>

#include <memory>
#include <string>
#include <vector>
#include <map>

#include "common/schema.h"
#include "server/fsmanager.h"
#include "server/metadata.pb.h"

namespace kudu {
namespace metadata {

// converts the specified BlockId to protobuf
void BlockIdToPB(const BlockId& block_id, BlockIdPB *pb);

// returns the BlockId created from the specified protobuf
BlockId BlockIdFromPB(const BlockIdPB& pb);

// converts the specified schema to protobuf
void TableSchemaToPB(const Schema& schema, TableSchemaPB *pb);

// Returns the Schema created from the specified protobuf.
//
// If the schema is invalid, may return a non-OK status.
Status TableSchemaFromPB(const TableSchemaPB& pb, Schema* schema);

} // namespace metadata
} // namespace kudu

#endif
