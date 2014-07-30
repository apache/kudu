// Copyright (c) 2013, Cloudera, inc.
//
// Inline functions to create the TPC-H schemas
#ifndef KUDU_BENCHMARKS_TPCH_SCHEMAS_H
#define KUDU_BENCHMARKS_TPCH_SCHEMAS_H

#include <boost/assign/list_of.hpp>
#include "kudu/client/schema.h"

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

static const client::KuduColumnStorageAttributes kPlainEncoding =
    client::KuduColumnStorageAttributes(PLAIN_ENCODING);

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

inline client::KuduSchema CreateLineItemSchema() {
  return client::KuduSchema(boost::assign::list_of
                (client::KuduColumnSchema(kOrderKeyColName, UINT32))
                (client::KuduColumnSchema(kLineNumberColName, UINT32))
                (client::KuduColumnSchema(kPartKeyColName, UINT32))
                (client::KuduColumnSchema(kSuppKeyColName, UINT32))
                (client::KuduColumnSchema(kQuantityColName, UINT32)) // decimal??
                (client::KuduColumnSchema(kExtendedPriceColName, UINT32)) // storing * 100
                (client::KuduColumnSchema(kDiscountColName, UINT32)) // storing * 100
                (client::KuduColumnSchema(kTaxColName, UINT32)) // storing * 100
                (client::KuduColumnSchema(kReturnFlagColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kLineStatusColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kShipDateColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kCommitDateColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kReceiptDateColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kShipInstructColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kShipModeColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kCommentColName, STRING,
                                          false, NULL, kPlainEncoding))
                , 2);
}

inline client::KuduSchema CreateTpch1QuerySchema() {
  return client::KuduSchema(boost::assign::list_of
                (client::KuduColumnSchema(kShipDateColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kReturnFlagColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kLineStatusColName, STRING,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kQuantityColName, UINT32))
                (client::KuduColumnSchema(kExtendedPriceColName, UINT32))
                (client::KuduColumnSchema(kDiscountColName, UINT32))
                (client::KuduColumnSchema(kTaxColName, UINT32))
                , 0);
}

inline client::KuduSchema CreateMS3DemoQuerySchema() {
  return client::KuduSchema(boost::assign::list_of
                (client::KuduColumnSchema(kOrderKeyColName, UINT32))
                (client::KuduColumnSchema(kLineNumberColName, UINT32))
                (client::KuduColumnSchema(kQuantityColName, UINT32))
                , 0);
}

} // namespace tpch
} // namespace kudu
#endif
