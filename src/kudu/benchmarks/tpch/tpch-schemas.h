// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
//
// Inline functions to create the TPC-H schemas
#ifndef KUDU_BENCHMARKS_TPCH_SCHEMAS_H
#define KUDU_BENCHMARKS_TPCH_SCHEMAS_H

#include <boost/assign/list_of.hpp>
#include <string>
#include <vector>

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

static const client::KuduColumnSchema::DataType kInt64 =
    client::KuduColumnSchema::INT64;
static const client::KuduColumnSchema::DataType kInt32 =
    client::KuduColumnSchema::INT32;
static const client::KuduColumnSchema::DataType kString =
    client::KuduColumnSchema::STRING;
static const client::KuduColumnSchema::DataType kDouble =
    client::KuduColumnSchema::DOUBLE;

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
                (client::KuduColumnSchema(kOrderKeyColName, kInt64))
                (client::KuduColumnSchema(kLineNumberColName, kInt32))
                (client::KuduColumnSchema(kPartKeyColName, kInt32))
                (client::KuduColumnSchema(kSuppKeyColName, kInt32))
                (client::KuduColumnSchema(kQuantityColName, kInt32)) // decimal??
                (client::KuduColumnSchema(kExtendedPriceColName, kDouble))
                (client::KuduColumnSchema(kDiscountColName, kDouble))
                (client::KuduColumnSchema(kTaxColName, kDouble))
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

inline std::vector<std::string> GetTpchQ1QueryColumns() {
  return boost::assign::list_of<std::string>
    (kShipDateColName)
    (kReturnFlagColName)
    (kLineStatusColName)
    (kQuantityColName)
    (kExtendedPriceColName)
    (kDiscountColName)
    (kTaxColName);
}

} // namespace tpch
} // namespace kudu
#endif
