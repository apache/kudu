// Copyright (c) 2014, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/client/scanner-internal.h"

#include <boost/bind.hpp>
#include <string>
#include <vector>

#include "kudu/client/client-internal.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/row_result.h"
#include "kudu/client/table-internal.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/hexdump.h"

using std::set;
using std::string;

namespace kudu {

using rpc::RpcController;
using tserver::ColumnRangePredicatePB;
using tserver::NewScanRequestPB;
using tserver::ScanResponsePB;

namespace client {

using internal::RemoteTabletServer;

static const int64_t kNoTimestamp = -1;

KuduScanner::Data::Data(KuduTable* table)
  : open_(false),
    data_in_open_(false),
    has_batch_size_bytes_(false),
    batch_size_bytes_(0),
    selection_(KuduClient::CLOSEST_REPLICA),
    read_mode_(READ_LATEST),
    order_mode_(UNORDERED),
    snapshot_timestamp_(kNoTimestamp),
    table_(DCHECK_NOTNULL(table)),
    projection_(table->schema().schema_.get()),
    arena_(1024, 1024*1024),
    spec_encoder_(table->schema().schema_.get()),
    timeout_(MonoDelta::FromMilliseconds(kScanTimeoutMillis)) {
}

KuduScanner::Data::~Data() {
}

Status KuduScanner::Data::CheckForErrors() {
  if (PREDICT_TRUE(!last_response_.has_error())) {
    return Status::OK();
  }

  return StatusFromPB(last_response_.error().status());
}

void KuduScanner::Data::CopyPredicateBound(const ColumnSchema& col,
                                           const void* bound_src,
                                           string* bound_dst) {
  const void* src;
  size_t size;
  if (col.type_info()->type() == STRING) {
    // Copying a string involves an extra level of indirection through its
    // owning slice.
    const Slice* s = reinterpret_cast<const Slice*>(bound_src);
    src = s->data();
    size = s->size();
  } else {
    src = bound_src;
    size = col.type_info()->size();
  }
  bound_dst->assign(reinterpret_cast<const char*>(src), size);
}

Status KuduScanner::Data::CanBeRetried(const bool isNewScan,
                                       const Status& rpc_status, const Status& server_status,
                                       const MonoTime& actual_deadline, const MonoTime& deadline,
                                       const vector<RemoteTabletServer*>& candidates,
                                       set<string>* blacklist) {
  CHECK(!rpc_status.ok() || !server_status.ok());

  // Start by checking network errors.
  if (!rpc_status.ok()) {
    if (rpc_status.IsTimedOut() && actual_deadline.Equals(deadline)) {
      // If we ended because of the overall deadline, we're done.
      // We didn't wait a full RPC timeout though, so don't mark the tserver as failed.
      LOG(INFO) << "Scan of tablet " << remote_->tablet_id() << " at "
          << ts_->ToString() << " deadline expired.";
      return rpc_status;
    } else {
      // All other types of network errors are retriable, and also indicate the tserver is failed.
      table_->client()->data_->meta_cache_->MarkTSFailed(ts_, rpc_status);
    }
  }

  // If we're in the middle of a batch and doing an uordered scan, then we cannot retry.
  // Unordered scans can still be retried on a tablet boundary (i.e. an OpenTablet call).
  if (!isNewScan && order_mode_ == KuduScanner::UNORDERED) {
    return !rpc_status.ok() ? rpc_status : server_status;
  }

  // For retries, the correct action depends on the particular failure condition.
  //
  // On an RPC error, we retry at a different tablet server.
  //
  // If the server returned an error code, it depends:
  //
  //   - SCANNER_EXPIRED    : The scan can be retried at the same tablet server.
  //
  //   - TABLET_NOT_RUNNING : The scan can be retried at a different tablet server, subject
  //                          to the client's specified selection criteria.
  //
  //   - Any other error    : Fatal. This indicates an unexpected error while processing the scan
  //                          request.
  if (rpc_status.ok() && !server_status.ok()) {
    const tserver::TabletServerErrorPB& error = last_response_.error();
    if (error.code() == tserver::TabletServerErrorPB::SCANNER_EXPIRED) {
      VLOG(1) << "Got SCANNER_EXPIRED error code, non-fatal error.";
    } else if (error.code() == tserver::TabletServerErrorPB::TABLET_NOT_RUNNING) {
      VLOG(1) << "Got TABLET_NOT_RUNNING error code, temporarily blacklisting node "
          << ts_->permanent_uuid();
      blacklist->insert(ts_->permanent_uuid());
      // We've blacklisted all the live candidate tservers.
      // Do a short random sleep, clear the temp blacklist, then do another round of retries.
      if (!candidates.empty() && candidates.size() == blacklist->size()) {
        MonoDelta sleep_delta = MonoDelta::FromMilliseconds((random() % 5000) + 1000);
        LOG(INFO) << "All live candidate nodes are unavailable because of transient errors."
            << " Sleeping for " << sleep_delta.ToMilliseconds() << " ms before trying again.";
        SleepFor(sleep_delta);
        blacklist->clear();
      }
    } else {
      // All other server errors are fatal. Usually indicates a malformed request, e.g. a bad scan
      // specification.
      return server_status;
    }
  }

  return Status::OK();
}

Status KuduScanner::Data::OpenTablet(const Slice& key,
                                     const MonoTime& deadline,
                                     set<string>* blacklist) {

  Synchronizer sync;
  table_->client()->data_->meta_cache_->LookupTabletByKey(table_,
                                                          key,
                                                          deadline,
                                                          &remote_,
                                                          sync.AsStatusCallback());
  RETURN_NOT_OK(sync.Wait());

  // Scan it.
  PrepareRequest(KuduScanner::Data::NEW);
  next_req_.clear_scanner_id();
  NewScanRequestPB* scan = next_req_.mutable_new_scan_request();
  switch (read_mode_) {
    case READ_LATEST: scan->set_read_mode(kudu::READ_LATEST); break;
    case READ_AT_SNAPSHOT: scan->set_read_mode(kudu::READ_AT_SNAPSHOT); break;
    default: LOG(FATAL) << "Unexpected read mode.";
  }

  switch (order_mode_) {
    case UNORDERED: scan->set_order_mode(kudu::UNORDERED); break;
    case ORDERED: scan->set_order_mode(kudu::ORDERED); break;
    default: LOG(FATAL) << "Unexpected order mode.";
  }

  if (encoded_last_row_key_.length() > 0) {
    VLOG(1) << "Setting NewScanRequestPB encoded_last_row_key to hex value "
        << HexDump(encoded_last_row_key_);
    scan->set_encoded_last_row_key(encoded_last_row_key_);
  }

  scan->set_cache_blocks(spec_.cache_blocks());

  if (snapshot_timestamp_ != kNoTimestamp) {
    if (PREDICT_FALSE(read_mode_ != READ_AT_SNAPSHOT)) {
      LOG(WARNING) << "Scan snapshot timestamp set but read mode was READ_LATEST."
          " Ignoring timestamp.";
    } else {
      scan->set_snap_timestamp(snapshot_timestamp_);
    }
  }

  // Set up the predicates.
  scan->clear_range_predicates();
  BOOST_FOREACH(const ColumnRangePredicate& pred, spec_.predicates()) {
    const ColumnSchema& col = pred.column();
    const ValueRange& range = pred.range();
    ColumnRangePredicatePB* pb = scan->add_range_predicates();
    if (range.has_lower_bound()) {
      CopyPredicateBound(col, range.lower_bound(),
                         pb->mutable_lower_bound());
    }
    if (range.has_upper_bound()) {
      CopyPredicateBound(col, range.upper_bound(),
                         pb->mutable_upper_bound());
    }
    ColumnSchemaToPB(col, pb->mutable_column());
  }

  if (spec_.lower_bound_key()) {
    scan->mutable_encoded_start_key()->assign(
      reinterpret_cast<const char*>(spec_.lower_bound_key()->encoded_key().data()),
      spec_.lower_bound_key()->encoded_key().size());
  } else {
    scan->clear_encoded_start_key();
  }
  if (spec_.upper_bound_key()) {
    scan->mutable_encoded_stop_key()->assign(
      reinterpret_cast<const char*>(spec_.upper_bound_key()->encoded_key().data()),
      spec_.upper_bound_key()->encoded_key().size());
  } else {
    scan->clear_encoded_stop_key();
  }

  scan->set_tablet_id(remote_->tablet_id());
  RETURN_NOT_OK(SchemaToColumnPBs(*projection_, scan->mutable_projected_columns()));

  for (;;) {
    // Recalculate the deadlines.
    MonoTime rpc_deadline = MonoTime::Now(MonoTime::FINE);
    rpc_deadline.AddDelta(table_->client()->default_rpc_timeout());
    MonoTime actual_deadline = MonoTime::Earliest(rpc_deadline, deadline);

    controller_.Reset();
    controller_.set_deadline(actual_deadline);
    RemoteTabletServer *ts;
    vector<RemoteTabletServer*> candidates;
    // TODO: Add retry logic for GetTabletServer too. A leader could be mark as failed, but
    // then come back up within the operation timeout. Refreshing liveness information involves
    // another LookupTabletByKey.
    RETURN_NOT_OK(table_->client()->data_->GetTabletServer(
        table_->client(),
        remote_->tablet_id(),
        selection_,
        *blacklist,
        &candidates,
        &ts));
    CHECK(ts);
    CHECK(ts->proxy());
    ts_ = ts;
    proxy_ = ts->proxy();
    const Status rpc_status = proxy_->Scan(next_req_, &last_response_, &controller_);
    const Status server_status = CheckForErrors();
    if (rpc_status.ok() && server_status.ok()) {
      break;
    }
    RETURN_NOT_OK(CanBeRetried(true, rpc_status, server_status, actual_deadline, deadline,
                               candidates, blacklist));
  }

  next_req_.clear_new_scan_request();
  data_in_open_ = last_response_.has_data();
  if (last_response_.has_more_results()) {
    next_req_.set_scanner_id(last_response_.scanner_id());
    VLOG(1) << "Opened tablet " << remote_->tablet_id()
            << ", scanner ID " << last_response_.scanner_id();
  } else if (last_response_.has_data()) {
    VLOG(1) << "Opened tablet " << remote_->tablet_id() << ", no scanner ID assigned";
  } else {
    VLOG(1) << "Opened tablet " << remote_->tablet_id() << " (no rows), no scanner ID assigned";
  }

  // If present in the response, set the snapshot timestamp and the encoded last row key.
  // This is used when retrying the scan elsewhere.
  // The last row key is also updated on each scan response.
  if (order_mode_ == ORDERED) {
    CHECK(last_response_.has_snap_timestamp());
    snapshot_timestamp_ = last_response_.snap_timestamp();
    if (last_response_.has_encoded_last_row_key()) {
      encoded_last_row_key_ = last_response_.encoded_last_row_key();
    }
  }

  if (last_response_.has_snap_timestamp()) {
    table_->client()->data_->UpdateLatestObservedTimestamp(last_response_.snap_timestamp());
  }

  return Status::OK();
}

Status KuduScanner::Data::ExtractRows(vector<KuduRowResult>* rows) {
  return ExtractRows(controller_, projection_, &last_response_, rows);
}

Status KuduScanner::Data::ExtractRows(const RpcController& controller,
                                      const Schema* projection,
                                      ScanResponsePB* resp,
                                      vector<KuduRowResult>* rows) {
  // First, rewrite the relative addresses into absolute ones.
  RowwiseRowBlockPB* rowblock_pb = resp->mutable_data();
  Slice direct, indirect;

  if (PREDICT_FALSE(!rowblock_pb->has_rows_sidecar())) {
    return Status::Corruption("Server sent invalid response: no row data");
  } else {
    Status s = controller.GetSidecar(rowblock_pb->rows_sidecar(), &direct);
    if (!s.ok()) {
      return Status::Corruption("Server sent invalid response: row data "
                                "sidecar index corrupt", s.ToString());
    }
  }

  if (rowblock_pb->has_indirect_data_sidecar()) {
    Status s = controller.GetSidecar(rowblock_pb->indirect_data_sidecar(),
                                      &indirect);
    if (!s.ok()) {
      return Status::Corruption("Server sent invalid response: indirect data "
                                "sidecar index corrupt", s.ToString());
    }
  }

  RETURN_NOT_OK(RewriteRowBlockPointers(*projection, *rowblock_pb, indirect, &direct));

  int n_rows = rowblock_pb->num_rows();
  if (PREDICT_FALSE(n_rows == 0)) {
    // Early-out here to avoid a UBSAN failure.
    VLOG(1) << "Extracted 0 rows";
    return Status::OK();
  }

  // Next, allocate a block of KuduRowResults in 'rows'.
  size_t before = rows->size();
  rows->resize(before + n_rows);

  // Lastly, initialize each KuduRowResult with data from the response.
  //
  // Doing this resize and array indexing turns out to be noticeably faster
  // than using reserve and push_back.
  int projected_row_size = CalculateProjectedRowSize(*projection);
  const uint8_t* src = direct.data();
  KuduRowResult* dst = &(*rows)[before];
  while (n_rows > 0) {
    dst->Init(projection, src);
    dst++;
    src += projected_row_size;
    n_rows--;
  }
  VLOG(1) << "Extracted " << rows->size() - before << " rows";
  return Status::OK();
}

bool KuduScanner::Data::MoreTablets() const {
  CHECK(open_);
  return !remote_->end_key().empty() &&
    (spec_.upper_bound_key() == NULL ||
     spec_.upper_bound_key()->encoded_key().compare(remote_->end_key()) > 0);
}

void KuduScanner::Data::PrepareRequest(RequestType state) {
  if (state == KuduScanner::Data::CLOSE) {
    next_req_.set_batch_size_bytes(0);
  } else if (has_batch_size_bytes_) {
    next_req_.set_batch_size_bytes(batch_size_bytes_);
  } else {
    next_req_.clear_batch_size_bytes();
  }

  if (state == KuduScanner::Data::NEW) {
    next_req_.set_call_seq_id(0);
  } else {
    next_req_.set_call_seq_id(next_req_.call_seq_id() + 1);
  }
}

size_t KuduScanner::Data::CalculateProjectedRowSize(const Schema& proj) {
  return proj.byte_size() +
        (proj.has_nullables() ? BitmapSize(proj.num_columns()) : 0);
}

} // namespace client
} // namespace kudu
