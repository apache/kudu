// Copyright (c) 2013, Cloudera, inc.

#include <boost/bind.hpp>
#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <vector>
#include <tr1/memory>
#include <utility>

#include "gutil/map-util.h"

#include "client/client.h"
#include "common/scan_spec.h"
#include "common/schema.h"
#include "common/row.h"
#include "common/wire_protocol.h"
#include "tserver/tserver_service.proxy.h"
#include "util/status.h"
#include "benchmarks/tpch/rpc_line_item_dao.h"
#include "util/locks.h"
#include "util/coding.h"

using std::tr1::shared_ptr;

namespace kudu {
using tserver::TabletServerServiceProxy;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;

void RpcLineItemDAO::Init() {
  const Schema s = tpch::CreateLineItemSchema();

  client::KuduClientOptions opts;
  opts.master_server_addr = master_address_;
  CHECK_OK(client::KuduClient::Create(opts, &client_));
  CHECK_OK(client_->GetTabletProxy(tablet_id_, &proxy_));
  CHECK_OK(client_->OpenTable(tablet_id_, s, &client_table_));

  request_.set_tablet_id(tablet_id_);
  CHECK_OK(SchemaToColumnPBs(s, request_.mutable_to_insert_rows()->mutable_schema()));
  CHECK_OK(SchemaToColumnPBs(s, request_.mutable_to_mutate_row_keys()->mutable_schema()));
}

void RpcLineItemDAO::WriteLine(const ConstContiguousRow &row) {
  if (!ShouldAddKey(row)) return;

  RowwiseRowBlockPB* data = request_.mutable_to_insert_rows();
  AddRowToRowBlockPB(row, data);
  DoWriteAsync(data);
  ApplyBackpressure();
}

void RpcLineItemDAO::MutateLine(const ConstContiguousRow &row, const faststring &mutations) {
  if (!ShouldAddKey(row)) return;

  RowwiseRowBlockPB* keys = request_.mutable_to_mutate_row_keys();
  AddRowToRowBlockPB(row, keys);

  faststring tmp;
  tmp.append(request_.encoded_mutations().c_str(), request_.encoded_mutations().size());
  PutFixed32LengthPrefixedSlice(&tmp, Slice(mutations));
  request_.set_encoded_mutations(tmp.data(), tmp.size());

  DoWriteAsync(keys);
  ApplyBackpressure();
}

bool RpcLineItemDAO::ShouldAddKey(const ConstContiguousRow &row) {
  uint32_t l_ordernumber = *row.schema().ExtractColumnFromRow<UINT32>(row, 0);
  uint32_t l_linenumber = *row.schema().ExtractColumnFromRow<UINT32>(row, 1);
  std::pair<uint32_t, uint32_t> composite_k(l_ordernumber, l_linenumber);

  boost::lock_guard<simple_spinlock> l(lock_);
  return InsertIfNotPresent(&orders_in_request_, composite_k);
}

void RpcLineItemDAO::ApplyBackpressure() {
  while (true) {
    {
      boost::lock_guard<simple_spinlock> l(lock_);
      if (!request_pending_) return;

      int row_count = request_.to_insert_rows().num_rows() +
        request_.to_mutate_row_keys().num_rows();
      if (row_count < batch_size_) {
        break;
      }
    }
    usleep(1000);
  }
}

void RpcLineItemDAO::DoWriteAsync(RowwiseRowBlockPB *data) {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (!request_pending_) {
    request_pending_ = true;

    rpc_.Reset();
    rpc_.set_timeout(MonoDelta::FromMilliseconds(2000));
    VLOG(1) << "Sending batch of " << data->num_rows();
    proxy_->WriteAsync(request_, &response_, &rpc_,
                       boost::bind(&RpcLineItemDAO::BatchFinished, this));

    // TODO figure how to clean better
    data->set_num_rows(0);
    data->clear_rows();
    data->clear_indirect_data();
    request_.clear_encoded_mutations();
    orders_in_request_.clear();
  }
}

void RpcLineItemDAO::BatchFinished() {
  boost::lock_guard<simple_spinlock> l(lock_);
  request_pending_ = false;

  Status s = rpc_.status();
  if (!s.ok()) {
    int n_rows = request_.to_insert_rows().num_rows() +
      request_.to_mutate_row_keys().num_rows();
    LOG(WARNING) << "RPC error inserting row: " << s.ToString()
                 << " (" << n_rows << " rows, " << request_.ByteSize() << " bytes)";
    return;
  }

  if (response_.has_error()) {
    LOG(WARNING) << "Unable to insert rows: " << response_.error().DebugString();
    return;
  }

  if (response_.per_row_errors().size() > 0) {
    BOOST_FOREACH(const WriteResponsePB::PerRowErrorPB& err, response_.per_row_errors()) {
      if (err.error().code() != AppStatusPB::ALREADY_PRESENT) {
        LOG(WARNING) << "Per-row errors for row " << err.row_index() << ": " << err.DebugString();
      }
    }
  }
}

void RpcLineItemDAO::FinishWriting() {
  while (true) {
    {
      boost::lock_guard<simple_spinlock> l(lock_);
      if (!request_pending_) return;
    }
    usleep(1000);
  }
}

void RpcLineItemDAO::OpenScanner(const Schema &query_schema, ScanSpec *spec) {}

void RpcLineItemDAO::OpenScanner(Schema &query_schema, ColumnRangePredicatePB &pred) {
  client::KuduScanner *scanner = new client::KuduScanner(client_table_.get());
  current_scanner_.reset(scanner);
  CHECK_OK(current_scanner_->SetProjection(query_schema));
  if (pred.has_column()) {
    CHECK_OK(current_scanner_->AddConjunctPredicate(pred));
  }
  CHECK_OK(current_scanner_->Open());
}

bool RpcLineItemDAO::HasMore() {
  bool has_more = current_scanner_->HasMoreRows();
  if (!has_more) {
    current_scanner_->Close();
  }
  return has_more;
}


void RpcLineItemDAO::GetNext(vector<const uint8_t*> *rows) {
  CHECK_OK(current_scanner_->NextBatch(rows));
}

bool RpcLineItemDAO::IsTableEmpty() {
  return true;
}

void RpcLineItemDAO::GetNext(RowBlock *block) {}
RpcLineItemDAO::~RpcLineItemDAO() {
  FinishWriting();
}

} // namespace kudu
