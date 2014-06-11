// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_LINE_ITEM_DAO_H
#define KUDU_TPCH_LINE_ITEM_DAO_H

#include <boost/function.hpp>

#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/row.h"

namespace kudu {

class PartialRow;

// Abstract class to read/write line item rows
class LineItemDAO {
 public:
  // Parameter function defines write/mutate operation.
  virtual void WriteLine(boost::function<void(PartialRow*)> f) = 0;
  virtual void MutateLine(boost::function<void(PartialRow*)> f) = 0;
  virtual void Init() = 0;
  virtual void FinishWriting() = 0;
  virtual void OpenScanner(const Schema &query_schema, ScanSpec *spec) = 0;
  virtual bool HasMore() = 0;
  virtual void GetNext(RowBlock *block) = 0;
  virtual bool IsTableEmpty() = 0;
  virtual ~LineItemDAO() {}
};

} // namespace kudu
#endif
