// Copyright (c) 2013, Cloudera, inc.
//
// Inline function to create the YCSB schema
#ifndef KUDU_BENCHMARKS_YCSB_SCHEMA_H
#define KUDU_BENCHMARKS_YCSB_SCHEMA_H

#include <boost/assign/list_of.hpp>
#include "common/schema.h"

namespace kudu {
inline Schema CreateYCSBSchema() {
  return Schema(boost::assign::list_of
                (ColumnSchema("key", STRING))
                (ColumnSchema("field0", STRING))
                (ColumnSchema("field1", STRING))
                (ColumnSchema("field2", STRING))
                (ColumnSchema("field3", STRING))
                (ColumnSchema("field4", STRING))
                (ColumnSchema("field5", STRING))
                (ColumnSchema("field6", STRING))
                (ColumnSchema("field7", STRING))
                (ColumnSchema("field8", STRING))
                (ColumnSchema("field9", STRING))
                , 1);
}

} // namespace kudu
#endif

