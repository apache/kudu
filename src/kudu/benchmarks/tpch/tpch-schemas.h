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
    client::KuduColumnStorageAttributes(client::KuduColumnStorageAttributes::PLAIN_ENCODING);

static const client::KuduColumnSchema::DataType kUint32 =
    client::KuduColumnSchema::UINT32;
static const client::KuduColumnSchema::DataType kString =
    client::KuduColumnSchema::STRING;

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
                (client::KuduColumnSchema(kOrderKeyColName, kUint32))
                (client::KuduColumnSchema(kLineNumberColName, kUint32))
                (client::KuduColumnSchema(kPartKeyColName, kUint32))
                (client::KuduColumnSchema(kSuppKeyColName, kUint32))
                (client::KuduColumnSchema(kQuantityColName, kUint32)) // decimal??
                (client::KuduColumnSchema(kExtendedPriceColName, kUint32)) // storing * 100
                (client::KuduColumnSchema(kDiscountColName, kUint32)) // storing * 100
                (client::KuduColumnSchema(kTaxColName, kUint32)) // storing * 100
                (client::KuduColumnSchema(kReturnFlagColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kLineStatusColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kShipDateColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kCommitDateColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kReceiptDateColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kShipInstructColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kShipModeColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kCommentColName, kString,
                                          false, NULL, kPlainEncoding))
                , 2);
}

inline client::KuduSchema CreateTpch1QuerySchema() {
  return client::KuduSchema(boost::assign::list_of
                (client::KuduColumnSchema(kShipDateColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kReturnFlagColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kLineStatusColName, kString,
                                          false, NULL, kPlainEncoding))
                (client::KuduColumnSchema(kQuantityColName, kUint32))
                (client::KuduColumnSchema(kExtendedPriceColName, kUint32))
                (client::KuduColumnSchema(kDiscountColName, kUint32))
                (client::KuduColumnSchema(kTaxColName, kUint32))
                , 0);
}

inline client::KuduSchema CreateMS3DemoQuerySchema() {
  return client::KuduSchema(boost::assign::list_of
                (client::KuduColumnSchema(kOrderKeyColName, kUint32))
                (client::KuduColumnSchema(kLineNumberColName, kUint32))
                (client::KuduColumnSchema(kQuantityColName, kUint32))
                , 0);
}

} // namespace tpch
} // namespace kudu
#endif
