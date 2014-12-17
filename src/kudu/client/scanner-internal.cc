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
    snapshot_timestamp_(kNoTimestamp),
    table_(DCHECK_NOTNULL(table)),
    projection_(table->schema().schema_.get()),
    projected_row_size_(CalculateProjectedRowSize(*projection_)),
    arena_(1024, 1024*1024),
    spec_encoder_(table->schema().schema_.get()),
    timeout_(MonoDelta::FromMilliseconds(kRpcTimeoutMillis)) {
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

Status KuduScanner::Data::OpenTablet(const Slice& key) {
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(timeout_);

  // TODO: scanners don't really require a leader. For now, however,
  // we always scan from the leader.
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
    controller_.Reset();
    controller_.set_deadline(deadline);
    RemoteTabletServer *ts;
    RETURN_NOT_OK(table_->client()->data_->GetTabletServer(
        table_->client(),
        remote_->tablet_id(),
        selection_,
        &ts));
    CHECK(ts);
    CHECK(ts->proxy());
    proxy_ = ts->proxy();
    Status s = proxy_->Scan(next_req_, &last_response_, &controller_);
    if (s.ok()) {
      break;
    }

    // On error, mark any replicas hosted by this TS as failed, then try
    // another replica.
    table_->client()->data_->meta_cache_->MarkTSFailed(ts, s);
  }
  RETURN_NOT_OK(CheckForErrors());

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

  return Status::OK();
}

Status KuduScanner::Data::ExtractRows(vector<KuduRowResult>* rows) {
  // First, rewrite the relative addresses into absolute ones.
  RowwiseRowBlockPB* rowblock_pb = last_response_.mutable_data();
  RETURN_NOT_OK(RewriteRowBlockPB(*projection_, last_response_.mutable_data()));

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
  string* row_data = rowblock_pb->mutable_rows();
  const uint8_t* src = reinterpret_cast<const uint8_t*>(&(*row_data)[0]);
  KuduRowResult* dst = &(*rows)[before];
  while (n_rows > 0) {
    dst->Init(projection_, src);
    dst++;
    src += projected_row_size_;
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
