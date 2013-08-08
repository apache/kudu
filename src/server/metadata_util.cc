// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <glog/logging.h>

#include "common/wire_protocol.h"
#include "server/metadata.pb.h"
#include "server/metadata_util.h"

namespace kudu {
namespace metadata {

void BlockIdToPB(const BlockId& block_id, BlockIdPB *pb) {
  pb->set_id(block_id.ToString());
}

BlockId BlockIdFromPB(const BlockIdPB& pb) {
  return BlockId(pb.id());
}

void SchemaToPB(const Schema& schema, TableSchemaPB *pb) {
  pb->Clear();
  CHECK_OK(SchemaToColumnPBs(schema, pb->mutable_columns()));
  // TODO: we need some better terminology to distinguish between
  // Schema (the minimum descriptor needed to enumerate columns) vs
  // all the attributes of a table, which also includes things like the
  // encodings, etc.
}

Status SchemaFromPB(const TableSchemaPB& pb, Schema* schema) {
  // TODO: see above
  return ColumnPBsToSchema(pb.columns(), schema);
}


} // namespace metadata
} // namespace kudu

