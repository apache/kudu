// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_LOCAL_LINE_ITEM_DAO_H
#define KUDU_TPCH_LOCAL_LINE_ITEM_DAO_H

#include <string>

#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/row.h"
#include "tablet/tablet.h"
#include "tablet/transactions/write_transaction.h"
#include "benchmarks/tpch/line_item_dao.h"

namespace kudu {

// Implementation of LineItemDAO that starts a local tablet and rw from it.
// FinishWriting here has the effect of flushing all the memrowsets
class LocalLineItemDAO : public LineItemDAO {
 public:
  explicit LocalLineItemDAO(const string &path)
      : fs_manager_(kudu::Env::Default(), path) {
    Status s = fs_manager_.Open();
    if (s.IsNotFound()) {
      CHECK_OK(fs_manager_.CreateInitialFileSystemLayout());
      CHECK_OK(fs_manager_.Open());
    }
  }
  virtual void WriteLine(const PartialRow& row);
  virtual void MutateLine(const ConstContiguousRow &row, const faststring &mutations);
  virtual void Init();
  virtual void FinishWriting();
  virtual void OpenScanner(const Schema &query_schema, ScanSpec *spec);
  virtual bool HasMore();
  virtual void GetNext(RowBlock *block);
  virtual bool IsTableEmpty();
  ~LocalLineItemDAO();

 private:
  kudu::FsManager fs_manager_;
  gscoped_ptr<kudu::tablet::Tablet> tablet_;
  tablet::WriteTransactionContext tx_ctx_;
  gscoped_ptr<RowwiseIterator> current_iter_;
};

} // namespace kudu
#endif
