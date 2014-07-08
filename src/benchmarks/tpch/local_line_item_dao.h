// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_LOCAL_LINE_ITEM_DAO_H
#define KUDU_TPCH_LOCAL_LINE_ITEM_DAO_H

#include <string>
#include <vector>

#include "benchmarks/tpch/line_item_dao.h"
#include "client/scan_predicate.h"
#include "client/schema.h"
#include "consensus/opid_anchor_registry.h"
#include "tablet/tablet.h"
#include "tablet/transactions/write_transaction.h"

namespace kudu {

// Implementation of LineItemDAO that starts a local tablet and rw from it.
// FinishWriting here has the effect of flushing all the memrowsets
class LocalLineItemDAO : public LineItemDAO {
 public:
  explicit LocalLineItemDAO(const string &path);
  virtual ~LocalLineItemDAO() OVERRIDE;

  virtual void WriteLine(boost::function<void(PartialRow*)> f) OVERRIDE;
  virtual void MutateLine(boost::function<void(PartialRow*)> f) OVERRIDE;
  virtual void Init() OVERRIDE;
  virtual void FinishWriting() OVERRIDE;
  virtual void OpenScanner(const client::KuduSchema& query_schema,
                           const std::vector<client::KuduColumnRangePredicate>& preds) OVERRIDE;
  virtual bool HasMore() OVERRIDE;
  virtual void GetNext(RowBlock *block) OVERRIDE;
  virtual bool IsTableEmpty() OVERRIDE;

 private:
  void WriteLine(const PartialRow& row);
  void MutateLine(const PartialRow& row);

  kudu::FsManager fs_manager_;
  gscoped_ptr<kudu::tablet::Tablet> tablet_;
  tablet::WriteTransactionState tx_state_;
  gscoped_ptr<RowwiseIterator> current_iter_;
  gscoped_ptr<ScanSpec> current_iter_spec_;
  client::KuduSchema schema_;
};

} // namespace kudu
#endif
