// Copyright (c) 2013, Cloudera, inc.
//
// Inline functions to create the TPC-H schemas
#ifndef KUDU_BENCHMARKS_TPCH_SCHEMAS_H
#define KUDU_BENCHMARKS_TPCH_SCHEMAS_H

#include <boost/assign/list_of.hpp>
#include "common/schema.h"

namespace kudu {
namespace tpch {

static const char* const kOrderKeyColName = "l_orderkey";
static const char* const kLineNumberColName = "l_linenumber";
static const char* const kPartKeyColName = "l_partkey";
static const char* const kSuppKeyColName = "l_suppkey";
static const char* const kQuantityColName = "l_quantity";
static const char* const kExtendedPriceColName = "l_extendedprice";
static const char* const kDiscountColName = "l_discount";
static const char* const kTaxColName = "l_tax";
static const char* const kReturnFlagColName = "l_returnflag";
static const char* const kLineStatusColName = "l_linestatus";
static const char* const kShipDateColName = "l_shipdate";
static const char* const kCommitDateColName = "l_commitdate";
static const char* const kReceiptDateColName = "l_receiptdate";
static const char* const kShipInstructColName = "l_shipinstruct";
static const char* const kShipModeColName = "l_shipmode";
static const char* const kCommentColName = "l_comment";

enum {
  kOrderKeyColIdx = 0,
  kLineNumberColIdx,
  kPartKeyColIdx,
  kSuppKeyColIdx,
  kQuantityColIdx,
  kExtendedPriceColIdx,
  kDiscountColIdx,
  kTaxColIdx,
  kReturnFlagColIdx,
  kLineStatusColIdx,
  kShipDateColIdx,
  kCommitDateColIdx,
  kReceiptDateColIdx,
  kShipInstructColIdx,
  kShipModeColIdx,
  kCommentColIdx
};

inline Schema CreateLineItemSchema() {
  return Schema(boost::assign::list_of
                (ColumnSchema(kOrderKeyColName, UINT32))
                (ColumnSchema(kLineNumberColName, UINT32))
                (ColumnSchema(kPartKeyColName, UINT32))
                (ColumnSchema(kSuppKeyColName, UINT32))
                (ColumnSchema(kQuantityColName, UINT32)) // decimal??
                (ColumnSchema(kExtendedPriceColName, UINT32)) // storing * 100
                (ColumnSchema(kDiscountColName, UINT32)) // storing * 100
                (ColumnSchema(kTaxColName, UINT32)) // storing * 100
                (ColumnSchema(kReturnFlagColName, STRING))
                (ColumnSchema(kLineStatusColName, STRING))
                (ColumnSchema(kShipDateColName, STRING))
                (ColumnSchema(kCommitDateColName, STRING))
                (ColumnSchema(kReceiptDateColName, STRING))
                (ColumnSchema(kShipInstructColName, STRING))
                (ColumnSchema(kShipModeColName, STRING))
                (ColumnSchema(kCommentColName, STRING))
                , 2);
}

inline Schema CreateTpch1QuerySchema() {
  return Schema(boost::assign::list_of
                (ColumnSchema(kShipDateColName, STRING))
                (ColumnSchema(kReturnFlagColName, STRING))
                (ColumnSchema(kLineStatusColName, STRING))
                (ColumnSchema(kQuantityColName, UINT32))
                (ColumnSchema(kExtendedPriceColName, UINT32))
                (ColumnSchema(kDiscountColName, UINT32))
                (ColumnSchema(kTaxColName, UINT32))
                , 0);
}

inline Schema CreateMS3DemoQuerySchema() {
  return Schema(boost::assign::list_of
                (ColumnSchema(kOrderKeyColName, UINT32))
                (ColumnSchema(kLineNumberColName, UINT32))
                (ColumnSchema(kQuantityColName, UINT32))
                , 0);
}

} // namespace tpch
} // namespace kudu
#endif
