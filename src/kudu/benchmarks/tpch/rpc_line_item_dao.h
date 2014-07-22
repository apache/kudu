// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_RPC_LINE_ITEM_DAO_H
#define KUDU_TPCH_RPC_LINE_ITEM_DAO_H

#include <boost/function.hpp>
#include <set>
#include <string>
#include <tr1/memory>
#include <utility>
#include <vector>

#include "kudu/benchmarks/tpch/tpch-schemas.h"
#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"

namespace kudu {

class RpcLineItemDAO {
 public:
  RpcLineItemDAO(const std::string& master_address, const std::string& table_name,
                 const int batch_size, const int mstimeout = 5000);
  ~RpcLineItemDAO();
  void WriteLine(boost::function<void(KuduPartialRow*)> f);
  void MutateLine(boost::function<void(KuduPartialRow*)> f);
  void Init();
  void FinishWriting();
  void OpenScanner(const client::KuduSchema &query_schema,
                   const std::vector<client::KuduColumnRangePredicate>& preds);
  bool HasMore();
  void GetNext(std::vector<client::KuduRowResult> *rows);
  bool IsTableEmpty();

 private:
  // Sending the same key more than once in the same batch crashes the server
  // This method is used to know if it's safe to add the row in that regard
  bool ShouldAddKey(const KuduPartialRow& row);

  simple_spinlock lock_;
  std::tr1::shared_ptr<client::KuduClient> client_;
  std::tr1::shared_ptr<client::KuduSession> session_;
  scoped_refptr<client::KuduTable> client_table_;
  gscoped_ptr<client::KuduScanner> current_scanner_;
  // Keeps track of all the orders batched for writing
  std::set<std::pair<uint32_t, uint32_t> > orders_in_request_;
  const std::string master_address_;
  const std::string table_name_;
  const int timeout_;
  const int batch_max_;
  int batch_size_;
  Atomic32 semaphore_;
};

} //namespace kudu
#endif
