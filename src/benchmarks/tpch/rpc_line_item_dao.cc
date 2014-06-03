// Copyright (c) 2013, Cloudera, inc.

#include <boost/bind.hpp>
#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <vector>
#include <tr1/memory>
#include <utility>

#include "gutil/map-util.h"

#include "client/client.h"
#include "client/meta_cache.h"
#include "common/row.h"
#include "common/row_operations.h"
#include "common/scan_spec.h"
#include "common/schema.h"
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
  const Schema schema = tpch::CreateLineItemSchema();

  client::KuduClientOptions opts;
  opts.master_server_addr = master_address_;
  CHECK_OK(client::KuduClient::Create(opts, &client_));
  Status s = client_->OpenTable(table_name_, &client_table_);
  if (s.IsNotFound()) {
    CHECK_OK(client_->CreateTable(table_name_, schema));
    CHECK_OK(client_->OpenTable(table_name_, &client_table_));
  } else {
    CHECK_OK(s);
  }

  // TODO: Use the client api instead of the direct request. This only
  // works because we've created a single-tablet table.
  Synchronizer sync;
  scoped_refptr<client::RemoteTablet> remote;
  client_->meta_cache_->LookupTabletByKey(client_table_.get(), Slice(),
                                     &remote, sync.callback());
  CHECK_OK(sync.Wait());
  CHECK_OK(client_->GetTabletProxy(remote->tablet_id(), client::KuduClient::LEADER_ONLY,
                                   &proxy_));
  request_.set_tablet_id(remote->tablet_id());
  CHECK_OK(SchemaToPB(schema, request_.mutable_schema()));
}

void RpcLineItemDAO::WriteLine(const PartialRow& row) {
  if (!ShouldAddKey(row)) return;

  RowOperationsPB* data = request_.mutable_row_operations();
  RowOperationsPBEncoder enc(data);
  enc.Add(RowOperationsPB::INSERT, row);
  num_pending_rows_++;
  DoWriteAsync();
  ApplyBackpressure();
}

void RpcLineItemDAO::MutateLine(const PartialRow& row) {
  if (!ShouldAddKey(row)) return;

  RowOperationsPB* data = request_.mutable_row_operations();
  RowOperationsPBEncoder enc(data);
  enc.Add(RowOperationsPB::UPDATE, row);
  num_pending_rows_++;
  DoWriteAsync();
  ApplyBackpressure();
}

bool RpcLineItemDAO::ShouldAddKey(const PartialRow &row) {
  uint32_t l_ordernumber;
  CHECK_OK(row.GetUInt32(tpch::kOrderKeyColIdx, &l_ordernumber));
  uint32_t l_linenumber;
  CHECK_OK(row.GetUInt32(tpch::kLineNumberColIdx, &l_linenumber));
  std::pair<uint32_t, uint32_t> composite_k(l_ordernumber, l_linenumber);

  boost::lock_guard<simple_spinlock> l(lock_);
  return InsertIfNotPresent(&orders_in_request_, composite_k);
}

void RpcLineItemDAO::ApplyBackpressure() {
  while (true) {
    {
      boost::lock_guard<simple_spinlock> l(lock_);
      if (!request_pending_) return;
      if (num_pending_rows_ < batch_size_) {
        break;
      }
    }
    usleep(1000);
  }
}

void RpcLineItemDAO::DoWriteAsync() {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (!request_pending_) {
    request_pending_ = true;

    rpc_.Reset();
    rpc_.set_timeout(MonoDelta::FromMilliseconds(2000));

    VLOG(1) << "Sending batch of " << num_pending_rows_;
    proxy_->WriteAsync(request_, &response_, &rpc_,
                       boost::bind(&RpcLineItemDAO::BatchFinished, this));

    num_pending_rows_ = 0;

    request_.mutable_row_operations()->Clear();
    orders_in_request_.clear();
  }
}

void RpcLineItemDAO::BatchFinished() {
  boost::lock_guard<simple_spinlock> l(lock_);
  request_pending_ = false;

  Status s = rpc_.status();
  if (!s.ok()) {
    // We can't log which rows failed, since we already cleared the Request
    // object right after sending it.
    LOG(WARNING) << "RPC error inserting rows: " << s.ToString();
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
      boost::unique_lock<simple_spinlock> l(lock_);
      if (!request_pending_) {
        if (num_pending_rows_ > 0) {
          l.unlock();
          DoWriteAsync();
        } else {
          return;
        }
      }
    }
    usleep(1000);
  }
}

void RpcLineItemDAO::OpenScanner(const Schema &query_schema, ScanSpec *spec) {
  client::KuduScanner *scanner = new client::KuduScanner(client_table_.get());
  current_scanner_.reset(scanner);
  CHECK_OK(current_scanner_->SetProjection(&query_schema));
  BOOST_FOREACH(const ColumnRangePredicate& pred, spec->predicates()) {
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
