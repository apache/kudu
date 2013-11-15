// Copyright (c) 2013, Cloudera, inc.
//
// Inline functions to create the TPC-H schemas
#ifndef KUDU_BENCHMARKS_TPCH_SCHEMAS_H
#define KUDU_BENCHMARKS_TPCH_SCHEMAS_H

#include <boost/assign/list_of.hpp>
#include "common/schema.h"

namespace kudu {
namespace tpch {
inline Schema CreateLineItemSchema() {
  return Schema(boost::assign::list_of
                (ColumnSchema("l_orderkey", UINT32))
                (ColumnSchema("l_linenumber", UINT32))
                (ColumnSchema("l_partkey", UINT32))
                (ColumnSchema("l_suppkey", UINT32))
                (ColumnSchema("l_quantity", UINT32)) // decimal??
                (ColumnSchema("l_extendedprice", UINT32)) // storing * 100
                (ColumnSchema("l_discount", UINT32)) // storing * 100
                (ColumnSchema("l_tax", UINT32)) // storing * 100
                (ColumnSchema("l_returnflag", STRING))
                (ColumnSchema("l_linestatus", STRING))
                (ColumnSchema("l_shipdate", STRING))
                (ColumnSchema("l_commitdate", STRING))
                (ColumnSchema("l_receiptdate", STRING))
                (ColumnSchema("l_shipinstruct", STRING))
                (ColumnSchema("l_shipmode", STRING))
                (ColumnSchema("l_comment", STRING))
                , 2);
}

inline Schema CreateTpch1QuerySchema() {
  return Schema(boost::assign::list_of
                (ColumnSchema("l_shipdate", STRING))
                (ColumnSchema("l_returnflag", STRING))
                (ColumnSchema("l_linestatus", STRING))
                (ColumnSchema("l_quantity", UINT32))
                (ColumnSchema("l_extendedprice", UINT32))
                (ColumnSchema("l_discount", UINT32))
                (ColumnSchema("l_tax", UINT32))
                , 0);
}

inline Schema CreateMS3DemoQuerySchema() {
  return Schema(boost::assign::list_of
                (ColumnSchema("l_orderkey", UINT32))
                (ColumnSchema("l_linenumber", UINT32))
                (ColumnSchema("l_quantity", UINT32))
                // Without this line we get old results back
                // TODO remove once it is fixed on the server-side
                //  JD 11/22/13
                ,boost::assign::list_of(0)(1)(4)
                , 0);
}

} // namespace tpch
} // namespace kudu
#endif
