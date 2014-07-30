// Copyright (c) 2013, Cloudera, inc.
//
// Inline function to create the YCSB schema
#ifndef KUDU_BENCHMARKS_YCSB_SCHEMA_H
#define KUDU_BENCHMARKS_YCSB_SCHEMA_H

#include <boost/assign/list_of.hpp>
#include "kudu/client/schema.h"

namespace kudu {
inline client::KuduSchema CreateYCSBSchema() {
  return client::KuduSchema(boost::assign::list_of
                (client::KuduColumnSchema("key", STRING))
                (client::KuduColumnSchema("field0", STRING))
                (client::KuduColumnSchema("field1", STRING))
                (client::KuduColumnSchema("field2", STRING))
                (client::KuduColumnSchema("field3", STRING))
                (client::KuduColumnSchema("field4", STRING))
                (client::KuduColumnSchema("field5", STRING))
                (client::KuduColumnSchema("field6", STRING))
                (client::KuduColumnSchema("field7", STRING))
                (client::KuduColumnSchema("field8", STRING))
                (client::KuduColumnSchema("field9", STRING))
                , 1);
}

} // namespace kudu
#endif

