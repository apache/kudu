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
    is_fault_tolerant_(false),
    snapshot_timestamp_(kNoTimestamp),
    table_(DCHECK_NOTNULL(table)),
    projection_(table->schema().schema_),
    arena_(1024, 1024*1024),
    spec_encoder_(table->schema().schema_, &arena_),
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
  if (col.type_info()->physical_type() == BINARY) {
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

  // If we're in the middle of a batch and doing a non fault-tolerant scan, then
  // we cannot retry. Non fault-tolerant scans can still be retried on a tablet
  // boundary (i.e. an OpenTablet call).
  if (!isNewScan && !is_fault_tolerant_) {
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

Status KuduScanner::Data::OpenTablet(const string& partition_key,
                                     const MonoTime& deadline,
                                     set<string>* blacklist) {

  PrepareRequest(KuduScanner::Data::NEW);
  next_req_.clear_scanner_id();
  NewScanRequestPB* scan = next_req_.mutable_new_scan_request();
  switch (read_mode_) {
    case READ_LATEST: scan->set_read_mode(kudu::READ_LATEST); break;
    case READ_AT_SNAPSHOT: scan->set_read_mode(kudu::READ_AT_SNAPSHOT); break;
    default: LOG(FATAL) << "Unexpected read mode.";
  }

  if (is_fault_tolerant_) {
    scan->set_order_mode(kudu::ORDERED);
  } else {
    scan->set_order_mode(kudu::UNORDERED);
  }

  if (last_primary_key_.length() > 0) {
    VLOG(1) << "Setting NewScanRequestPB last_primary_key to hex value "
        << HexDump(last_primary_key_);
    scan->set_last_primary_key(last_primary_key_);
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
    scan->mutable_start_primary_key()->assign(
      reinterpret_cast<const char*>(spec_.lower_bound_key()->encoded_key().data()),
      spec_.lower_bound_key()->encoded_key().size());
  } else {
    scan->clear_start_primary_key();
  }
  if (spec_.exclusive_upper_bound_key()) {
    scan->mutable_stop_primary_key()->assign(
      reinterpret_cast<const char*>(spec_.exclusive_upper_bound_key()->encoded_key().data()),
      spec_.exclusive_upper_bound_key()->encoded_key().size());
  } else {
    scan->clear_stop_primary_key();
  }
  RETURN_NOT_OK(SchemaToColumnPBs(*projection_, scan->mutable_projected_columns(),
                                  SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES | SCHEMA_PB_WITHOUT_IDS));

  for (int attempt = 1;; attempt++) {
    Synchronizer sync;
    table_->client()->data_->meta_cache_->LookupTabletByKey(table_,
                                                            partition_key,
                                                            deadline,
                                                            &remote_,
                                                            sync.AsStatusCallback());
    RETURN_NOT_OK(sync.Wait());

    scan->set_tablet_id(remote_->tablet_id());

    RemoteTabletServer *ts;
    vector<RemoteTabletServer*> candidates;
    Status lookup_status = table_->client()->data_->GetTabletServer(
        table_->client(),
        remote_->tablet_id(),
        selection_,
        *blacklist,
        &candidates,
        &ts);
    // If we get ServiceUnavailable, this indicates that the tablet doesn't
    // currently have any known leader. We should sleep and retry, since
    // it's likely that the tablet is undergoing a leader election and will
    // soon have one.
    if (lookup_status.IsServiceUnavailable() &&
        MonoTime::Now(MonoTime::FINE).ComesBefore(deadline)) {
      int sleep_ms = attempt * 100;
      VLOG(1) << "Tablet " << remote_->tablet_id() << " current unavailable: "
              << lookup_status.ToString() << ". Sleeping for " << sleep_ms << "ms "
              << "and retrying...";
      SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
      continue;
    }
    RETURN_NOT_OK(lookup_status);

    // Recalculate the deadlines.
    // If we have other replicas beyond this one to try, then we'll try to
    // open the scanner with the default RPC timeout. That gives us time to
    // try other replicas later. Otherwise, we open the scanner using the
    // full remaining deadline for the user's call.
    MonoTime rpc_deadline;
    if (static_cast<int>(candidates.size()) - blacklist->size() > 1) {
      rpc_deadline = MonoTime::Now(MonoTime::FINE);
      rpc_deadline.AddDelta(table_->client()->default_rpc_timeout());
      rpc_deadline = MonoTime::Earliest(deadline, rpc_deadline);
    } else {
      rpc_deadline = deadline;
    }

    controller_.Reset();
    controller_.set_deadline(rpc_deadline);

    CHECK(ts->proxy());
    ts_ = CHECK_NOTNULL(ts);
    proxy_ = ts->proxy();
    const Status rpc_status = proxy_->Scan(next_req_, &last_response_, &controller_);
    const Status server_status = CheckForErrors();
    if (rpc_status.ok() && server_status.ok()) {
      break;
    }
    RETURN_NOT_OK(CanBeRetried(true, rpc_status, server_status, rpc_deadline, deadline,
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

  // If present in the response, set the snapshot timestamp and the encoded last
  // primary key.  This is used when retrying the scan elsewhere.  The last
  // primary key is also updated on each scan response.
  if (is_fault_tolerant_) {
    CHECK(last_response_.has_snap_timestamp());
    snapshot_timestamp_ = last_response_.snap_timestamp();
    if (last_response_.has_last_primary_key()) {
      last_primary_key_ = last_response_.last_primary_key();
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
  // TODO(KUDU-565): add a test which has a scan end on a tablet boundary

  if (remote_->partition().partition_key_end().empty()) {
    // Last tablet -- nothing more to scan.
    return false;
  }

  if (!table_->partition_schema().IsSimplePKRangePartitioning(*table_->schema().schema_)) {
    // We can't do culling yet if the partitioning isn't simple.
    return true;
  }

  if (spec_.exclusive_upper_bound_key() == NULL) {
    // No upper bound - keep going!
    return true;
  }

  // Otherwise, we have to compare the upper bound.
  return spec_.exclusive_upper_bound_key()->encoded_key()
          .compare(remote_->partition().partition_key_end()) > 0;
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
