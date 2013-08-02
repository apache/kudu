// Copyright (c) 2013, Cloudera, inc.

#include <boost/foreach.hpp>
#include <glog/logging.h>

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

void ColumnSchemaToPB(const ColumnSchema& col_schema, ColumnSchemaPB *pb) {
  pb->set_name(col_schema.name());
  pb->set_type(col_schema.type_info().type());
  pb->set_is_nullable(col_schema.is_nullable());
}

ColumnSchema ColumnSchemaFromPB(const ColumnSchemaPB& pb) {
  return ColumnSchema(pb.name(), pb.type(), pb.is_nullable());
}

void SchemaToPB(const Schema& schema, TableSchemaPB *pb) {
  pb->Clear();
  for (size_t i = 0; i < schema.num_columns(); ++i) {
    const ColumnSchema& col_schema = schema.column(i);

    ColumnSchemaPB *col_pb = pb->add_columns();
    col_pb->set_id(i);
    col_pb->set_is_key(i < schema.num_key_columns());
    ColumnSchemaToPB(col_schema, col_pb);
  }
}

Schema SchemaFromPB(const TableSchemaPB& pb) {
  vector<ColumnSchema> columns;
  int num_key_columns = 0;
  BOOST_FOREACH(const ColumnSchemaPB& col_pb, pb.columns()) {
    num_key_columns += col_pb.is_key();
    columns.push_back(ColumnSchema(col_pb.name(), col_pb.type(), col_pb.is_nullable()));
  }
  return Schema(columns, num_key_columns);
}


} // namespace metadata
} // namespace kudu

