// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_RPC_LINE_ITEM_DAO_H
#define KUDU_TPCH_RPC_LINE_ITEM_DAO_H

#include <set>
#include <string>
#include <tr1/memory>
#include <utility>
#include <vector>

#include "benchmarks/tpch/line_item_dao.h"
#include "benchmarks/tpch/tpch-schemas.h"
#include "client/client.h"
#include "client/row_result.h"
#include "gutil/atomicops.h"
#include "gutil/ref_counted.h"

namespace kudu {

using tserver::TabletServerServiceProxy;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;
using tserver::ColumnRangePredicatePB;
using std::tr1::shared_ptr;

class RpcLineItemDAO : public LineItemDAO {
 public:
  RpcLineItemDAO(const string& master_address, const string& table_name,
                 const int batch_size, const int mstimeout = 5000);
  virtual void WriteLine(boost::function<void(PartialRow*)> f) OVERRIDE;
  virtual void MutateLine(boost::function<void(PartialRow*)> f) OVERRIDE;
  virtual void Init() OVERRIDE;
  virtual void FinishWriting() OVERRIDE;
  virtual void OpenScanner(const client::KuduSchema &query_schema,
                           const std::vector<client::KuduColumnRangePredicate>& preds) OVERRIDE;
  virtual bool HasMore() OVERRIDE;
  virtual void GetNext(RowBlock *block) OVERRIDE;
  void GetNext(std::vector<client::KuduRowResult> *rows);
  virtual bool IsTableEmpty() OVERRIDE;
  ~RpcLineItemDAO();

 private:
  // Sending the same key more than once in the same batch crashes the server
  // This method is used to know if it's safe to add the row in that regard
  bool ShouldAddKey(const PartialRow& row);

  simple_spinlock lock_;
  shared_ptr<client::KuduClient> client_;
  shared_ptr<client::KuduSession> session_;
  scoped_refptr<client::KuduTable> client_table_;
  gscoped_ptr<client::KuduScanner> current_scanner_;
  // Keeps track of all the orders batched for writing
  std::set<std::pair<uint32_t, uint32_t> > orders_in_request_;
  const string master_address_;
  const string table_name_;
  const int timeout_;
  const int batch_max_;
  int batch_size_;
  Atomic32 semaphore_;
};

} //namespace kudu
#endif
