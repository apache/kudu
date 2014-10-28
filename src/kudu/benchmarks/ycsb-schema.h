// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Inline function to create the YCSB schema
#ifndef KUDU_BENCHMARKS_YCSB_SCHEMA_H
#define KUDU_BENCHMARKS_YCSB_SCHEMA_H

#include <boost/assign/list_of.hpp>
#include "kudu/client/schema.h"

namespace kudu {

static const client::KuduColumnSchema::DataType kString =
    client::KuduColumnSchema::STRING;

inline client::KuduSchema CreateYCSBSchema() {
  return client::KuduSchema(boost::assign::list_of
                (client::KuduColumnSchema("key", kString))
                (client::KuduColumnSchema("field0", kString))
                (client::KuduColumnSchema("field1", kString))
                (client::KuduColumnSchema("field2", kString))
                (client::KuduColumnSchema("field3", kString))
                (client::KuduColumnSchema("field4", kString))
                (client::KuduColumnSchema("field5", kString))
                (client::KuduColumnSchema("field6", kString))
                (client::KuduColumnSchema("field7", kString))
                (client::KuduColumnSchema("field8", kString))
                (client::KuduColumnSchema("field9", kString))
                , 1);
}

} // namespace kudu
#endif

