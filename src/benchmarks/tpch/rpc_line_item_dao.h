// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_RPC_LINE_ITEM_DAO_H
#define KUDU_TPCH_RPC_LINE_ITEM_DAO_H

#include <tr1/memory>
#include <utility>
#include <vector>
#include <set>
#include <string>

#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/row.h"
#include "common/wire_protocol.h"
#include "benchmarks/tpch/line_item_dao.h"
#include "tserver/tserver_service.proxy.h"
#include "benchmarks/tpch/tpch-schemas.h"
#include "client/client.h"

namespace kudu {
using tserver::TabletServerServiceProxy;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;
using tserver::ColumnRangePredicatePB;
using std::tr1::shared_ptr;

class RpcLineItemDAO : public LineItemDAO {
 public:
  RpcLineItemDAO(const string &master_address, const string& tablet_id, const int batch_size) :
    request_pending_(false), master_address_(master_address),
    tablet_id_(tablet_id), batch_size_(batch_size) {}
  virtual void WriteLine(const ConstContiguousRow &row);
  virtual void MutateLine(const ConstContiguousRow &row, const faststring &mutations);
  virtual void Init();
  virtual void FinishWriting();
  void BatchFinished();
  virtual void OpenScanner(const Schema &query_schema, ScanSpec *spec);
  void OpenScanner(Schema &query_schema, ColumnRangePredicatePB &pred);
  virtual bool HasMore();
  virtual void GetNext(RowBlock *block);
  void GetNext(vector<const uint8_t*> *rows);
  virtual bool IsTableEmpty();
  ~RpcLineItemDAO();

 private:
  void ApplyBackpressure();
  // Sending the same key more than once in the same batch crashes the server
  // This method is used to know if it's safe to add the row in that regard
  bool ShouldAddKey(const ConstContiguousRow &row);
  void DoWriteAsync(RowwiseRowBlockPB *data);

  std::tr1::shared_ptr<TabletServerServiceProxy> proxy_;
  tserver::WriteRequestPB request_;
  tserver::WriteResponsePB response_;
  rpc::RpcController rpc_;
  bool request_pending_;
  simple_spinlock lock_;
  shared_ptr<client::KuduClient> client_;
  shared_ptr<client::KuduTable> client_table_;
  gscoped_ptr<client::KuduScanner> current_scanner_;
  // Keeps track of all the orders batched for writing
  std::set<std::pair<uint32_t, uint32_t> > orders_in_request_;
  string master_address_;
  string tablet_id_;
  int batch_size_;
};

} //namespace kudu
#endif
