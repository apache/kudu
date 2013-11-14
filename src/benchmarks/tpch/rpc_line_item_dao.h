// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_TPCH_RPC_LINE_ITEM_DAO_H
#define KUDU_TPCH_RPC_LINE_ITEM_DAO_H

#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/row.h"
#include "benchmarks/tpch/line_item_dao.h"
#include "tserver/tserver_service.proxy.h"
#include "benchmarks/tpch/tpch-schemas.h"

#include <tr1/memory>

namespace kudu {

using tserver::TabletServerServiceProxy;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;

class RpcLineItemDAO : public LineItemDAO {
 public:
  RpcLineItemDAO() : request_pending_(false) {
    request_.set_tablet_id("tpch1");
    CHECK_OK(SchemaToColumnPBs(tpch::CreateLineItemSchema(), request_.mutable_to_insert_rows()->mutable_schema()));
    CHECK_OK(SchemaToColumnPBs(tpch::CreateLineItemSchema(), request_.mutable_to_mutate_row_keys()->mutable_schema()));
  }
  virtual void WriteLine(const ConstContiguousRow &row);
  virtual void MutateLine(const ConstContiguousRow &row, const faststring &mutations);
  virtual void Init();
  virtual void FinishWriting();
  void BatchFinished();
  virtual void OpenScanner(const Schema &query_schema, ScanSpec *spec);
  virtual bool HasMore();
  virtual void GetNext(RowBlock *block);
  virtual bool IsTableEmpty();
  ~RpcLineItemDAO();

 private:
  std::tr1::shared_ptr<TabletServerServiceProxy> proxy_;
  tserver::WriteRequestPB request_;
  tserver::WriteResponsePB response_;
  rpc::RpcController rpc_;
  bool request_pending_;
  simple_spinlock lock_;

  void DoWriteAsync(RowwiseRowBlockPB *data);
};

} //namespace kudu
#endif
