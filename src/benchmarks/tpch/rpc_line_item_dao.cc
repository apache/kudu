// Copyright (c) 2013, Cloudera, inc.

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <vector>
#include <tr1/memory>

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

DEFINE_string(master_address, "localhost",
              "Address of master for the cluster to operate on");
DEFINE_int32(tpch_max_batch_size, 1000,
             "Maximum number of inserts/updates to batch at once");

const char * const kTabletId = "tpch1";

using std::tr1::shared_ptr;

namespace kudu {

using tserver::TabletServerServiceProxy;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;

void RpcLineItemDAO::Init() {
  client::KuduClientOptions opts;
  opts.master_server_addr = FLAGS_master_address;
  shared_ptr<client::KuduClient> client;
  CHECK_OK(client::KuduClient::Create(opts, &client));
  CHECK_OK(client->GetTabletProxy(kTabletId, &proxy_));
}

void RpcLineItemDAO::WriteLine(const ConstContiguousRow &row) {
  RowwiseRowBlockPB* data = request_.mutable_to_insert_rows();
  AddRowToRowBlockPB(row, data);
  DoWriteAsync(data);
  ApplyBackpressure();
}

void RpcLineItemDAO::MutateLine(const ConstContiguousRow &row, const faststring &mutations) {
  RowwiseRowBlockPB* keys = request_.mutable_to_mutate_row_keys();
  AddRowToRowBlockPB(row, keys);

  faststring tmp;
  tmp.append(request_.encoded_mutations().c_str(), request_.encoded_mutations().size());
  PutFixed32LengthPrefixedSlice(&tmp, Slice(mutations));
  request_.set_encoded_mutations(tmp.data(), tmp.size());

  DoWriteAsync(keys);
  ApplyBackpressure();
}

void RpcLineItemDAO::ApplyBackpressure() {
  while (true) {
    {
      boost::lock_guard<simple_spinlock> l(lock_);
      if (!request_pending_) return;

      int row_count = request_.to_insert_rows().num_rows() +
        request_.to_mutate_row_keys().num_rows();
      if (row_count < FLAGS_tpch_max_batch_size) {
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
    rpc_.set_timeout(MonoDelta::FromMilliseconds(1000));
    VLOG(1) << "Sending batch of " << data->num_rows();
    proxy_->WriteAsync(request_, &response_, &rpc_, boost::bind(&RpcLineItemDAO::BatchFinished, this));

    // TODO figure how to clean better
    data->set_num_rows(0);
    data->clear_rows();
    data->clear_indirect_data();
    request_.clear_encoded_mutations();
    //request_.clear_to_mutate_row_keys();
    //request_.clear_to_insert_rows();
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
}

void RpcLineItemDAO::OpenScanner(const Schema &query_schema, ScanSpec *spec) {
}

bool RpcLineItemDAO::HasMore() {
  return true;
}

void RpcLineItemDAO::GetNext(RowBlock *block) {

}

bool RpcLineItemDAO::IsTableEmpty() {
  return true;
}

RpcLineItemDAO::~RpcLineItemDAO() {

}

} // namespace kudu
