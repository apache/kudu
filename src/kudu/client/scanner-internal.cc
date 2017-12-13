// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/client/scanner-internal.h"

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include "kudu/client/client-internal.h"
#include "kudu/client/meta_cache.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/partition.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/async_util.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"

using google::protobuf::FieldDescriptor;
using google::protobuf::Reflection;
using std::set;
using std::string;
using std::vector;

namespace kudu {

using rpc::CredentialsPolicy;
using rpc::RpcController;
using strings::Substitute;
using tserver::NewScanRequestPB;
using tserver::TabletServerFeatures;

namespace client {

using internal::RemoteTabletServer;

KuduScanner::Data::Data(KuduTable* table)
  : configuration_(table),
    open_(false),
    data_in_open_(false),
    short_circuit_(false),
    table_(DCHECK_NOTNULL(table)->shared_from_this()),
    scan_attempts_(0) {
}

KuduScanner::Data::~Data() {
}

Status KuduScanner::Data::HandleError(const ScanRpcStatus& err,
                                      const MonoTime& deadline,
                                      set<string>* blacklist) {
  // If we timed out because of the overall deadline, we're done.
  // We didn't wait a full RPC timeout, though, so don't mark the tserver as failed.
  if (err.result == ScanRpcStatus::OVERALL_DEADLINE_EXCEEDED) {
      LOG(INFO) << "Scan of tablet " << remote_->tablet_id() << " at "
          << ts_->ToString() << " deadline expired.";
      return last_error_.ok()
          ? err.status : err.status.CloneAndAppend(last_error_.ToString());
  }

  UpdateLastError(err.status);

  bool mark_ts_failed = false;
  bool blacklist_location = false;
  bool mark_locations_stale = false;
  bool can_retry = true;
  bool backoff = false;
  bool reacquire_authn_token = false;
  switch (err.result) {
    case ScanRpcStatus::SERVICE_UNAVAILABLE:
      backoff = true;
      break;
    case ScanRpcStatus::RPC_DEADLINE_EXCEEDED:
    case ScanRpcStatus::RPC_ERROR:
      blacklist_location = true;
      mark_ts_failed = true;
      break;
    case ScanRpcStatus::SCANNER_EXPIRED:
      break;
    case ScanRpcStatus::RPC_INVALID_AUTHENTICATION_TOKEN:
      // Usually this happens if doing an RPC call with an expired auth token.
      // Retrying with a new authn token should help.
      reacquire_authn_token = true;
      break;
    case ScanRpcStatus::TABLET_NOT_RUNNING:
      blacklist_location = true;
      break;
    case ScanRpcStatus::TABLET_NOT_FOUND:
      // There was either a tablet configuration change or the table was
      // deleted, since at the time of this writing we don't support splits.
      // Force a re-fetch of the tablet metadata.
      mark_locations_stale = true;
      blacklist_location = true;
      break;
    default:
      can_retry = false;
      break;
  }

  if (mark_ts_failed) {
    table_->client()->data_->meta_cache_->MarkTSFailed(ts_, err.status);
    DCHECK(blacklist_location);
  }

  if (blacklist_location) {
    blacklist->insert(ts_->permanent_uuid());
  }

  if (mark_locations_stale) {
    remote_->MarkStale();
  }

  if (reacquire_authn_token) {
    // Re-connect to the cluster to get a new authn token.
    KuduClient* c = table_->client();
    const Status& s = c->data_->ConnectToCluster(
        c, deadline, CredentialsPolicy::PRIMARY_CREDENTIALS);
    if (!s.ok()) {
      KLOG_EVERY_N_SECS(WARNING, 1)
          << "Couldn't reconnect to the cluster: " << s.ToString();
      backoff = true;
    }
  }

  if (backoff) {
    MonoDelta sleep =
        KuduClient::Data::ComputeExponentialBackoff(scan_attempts_);
    MonoTime now = MonoTime::Now() + sleep;
    if (deadline < now) {
      Status ret = Status::TimedOut("unable to retry before timeout",
                                    err.status.ToString());
      return last_error_.ok() ?
          ret : ret.CloneAndAppend(last_error_.ToString());
    }
    VLOG(1) << "Error scanning on server " << ts_->ToString() << ": "
            << err.status.ToString() << ". Will retry after "
            << sleep.ToString() << "; attempt " << scan_attempts_;
    SleepFor(sleep);
  }

  if (can_retry) {
    return Status::OK();
  }
  return err.status;
}

void KuduScanner::Data::UpdateResourceMetrics() {
  if (last_response_.has_resource_metrics()) {
    tserver::ResourceMetricsPB resource_metrics = last_response_.resource_metrics();
    const Reflection* reflection = resource_metrics.GetReflection();
    vector<const FieldDescriptor*> fields;
    reflection->ListFields(resource_metrics, &fields);
    for (const FieldDescriptor* field : fields) {
      if (reflection->HasField(resource_metrics, field) &&
          field->cpp_type() == FieldDescriptor::CPPTYPE_INT64) {
        resource_metrics_.Increment(field->name(), reflection->GetInt64(resource_metrics, field));
      }
    }
  }
}

ScanRpcStatus KuduScanner::Data::AnalyzeResponse(const Status& rpc_status,
                                                 const MonoTime& overall_deadline,
                                                 const MonoTime& deadline) {
  if (rpc_status.ok() && !last_response_.has_error()) {
    return ScanRpcStatus{ScanRpcStatus::OK, Status::OK()};
  }

  // Check for various RPC-level errors.
  if (!rpc_status.ok()) {
    // Handle various RPC-system level errors that came back from the server. These
    // errors indicate that the TS is actually up.
    if (rpc_status.IsRemoteError()) {
      DCHECK(controller_.error_response());
      switch (controller_.error_response()->code()) {
        case rpc::ErrorStatusPB::ERROR_INVALID_REQUEST:
          return ScanRpcStatus{ScanRpcStatus::INVALID_REQUEST, rpc_status};
        case rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY: // fall-through
        case rpc::ErrorStatusPB::ERROR_UNAVAILABLE:
          return ScanRpcStatus{
              ScanRpcStatus::SERVICE_UNAVAILABLE, rpc_status};
        default:
          return ScanRpcStatus{ScanRpcStatus::RPC_ERROR, rpc_status};
      }
    }

    if (rpc_status.IsNotAuthorized() && controller_.error_response()) {
      switch (controller_.error_response()->code()) {
        case rpc::ErrorStatusPB::FATAL_INVALID_AUTHENTICATION_TOKEN:
          return ScanRpcStatus{
              ScanRpcStatus::RPC_INVALID_AUTHENTICATION_TOKEN, rpc_status};
        default:
          return ScanRpcStatus{ScanRpcStatus::OTHER_TS_ERROR, rpc_status};
      }
    }

    if (rpc_status.IsTimedOut()) {
      if (overall_deadline == deadline) {
        return ScanRpcStatus{ScanRpcStatus::OVERALL_DEADLINE_EXCEEDED, rpc_status};
      } else {
        return ScanRpcStatus{ScanRpcStatus::RPC_DEADLINE_EXCEEDED, rpc_status};
      }
    }
    return ScanRpcStatus{ScanRpcStatus::RPC_ERROR, rpc_status};
  }

  // If we got this far, it indicates that the tserver service actually handled the
  // call, but it was an error for some reason.
  Status server_status = StatusFromPB(last_response_.error().status());
  DCHECK(!server_status.ok());
  const tserver::TabletServerErrorPB& error = last_response_.error();
  switch (error.code()) {
    case tserver::TabletServerErrorPB::SCANNER_EXPIRED:
      return ScanRpcStatus{ScanRpcStatus::SCANNER_EXPIRED, server_status};
    case tserver::TabletServerErrorPB::TABLET_NOT_RUNNING:
      return ScanRpcStatus{ScanRpcStatus::TABLET_NOT_RUNNING, server_status};
    case tserver::TabletServerErrorPB::TABLET_FAILED: // fall-through
    case tserver::TabletServerErrorPB::TABLET_NOT_FOUND:
      return ScanRpcStatus{ScanRpcStatus::TABLET_NOT_FOUND, server_status};
    default:
      return ScanRpcStatus{ScanRpcStatus::OTHER_TS_ERROR, server_status};
  }
}

Status KuduScanner::Data::OpenNextTablet(const MonoTime& deadline,
                                         std::set<std::string>* blacklist) {
  return OpenTablet(partition_pruner_.NextPartitionKey(),
                    deadline,
                    blacklist);
}

Status KuduScanner::Data::ReopenCurrentTablet(const MonoTime& deadline,
                                              std::set<std::string>* blacklist) {
  return OpenTablet(remote_->partition().partition_key_start(),
                    deadline,
                    blacklist);
}

ScanRpcStatus KuduScanner::Data::SendScanRpc(const MonoTime& overall_deadline,
                                             bool allow_time_for_failover) {
  // The user has specified a timeout which should apply to the total time for each call
  // to NextBatch(). However, for fault-tolerant scans, or for when we are first opening
  // a scanner, it's preferable to set a shorter timeout (the "default RPC timeout") for
  // each individual RPC call. This gives us time to fail over to a different server
  // if the first server we try happens to be hung.
  MonoTime rpc_deadline;
  if (allow_time_for_failover) {
    rpc_deadline = MonoTime::Now() + table_->client()->default_rpc_timeout();
    rpc_deadline = std::min(overall_deadline, rpc_deadline);
  } else {
    rpc_deadline = overall_deadline;
  }

  controller_.Reset();
  controller_.set_deadline(rpc_deadline);
  if (!configuration_.spec().predicates().empty()) {
    controller_.RequireServerFeature(TabletServerFeatures::COLUMN_PREDICATES);
  }
  if (configuration().row_format_flags() & KuduScanner::PAD_UNIXTIME_MICROS_TO_16_BYTES) {
    controller_.RequireServerFeature(TabletServerFeatures::PAD_UNIXTIME_MICROS_TO_16_BYTES);
  }
  ScanRpcStatus scan_status = AnalyzeResponse(
      proxy_->Scan(next_req_,
                   &last_response_,
                   &controller_),
      rpc_deadline, overall_deadline);
  if (scan_status.result == ScanRpcStatus::OK) {
    UpdateResourceMetrics();
  }
  return scan_status;
}

Status KuduScanner::Data::OpenTablet(const string& partition_key,
                                     const MonoTime& deadline,
                                     set<string>* blacklist) {

  PrepareRequest(KuduScanner::Data::NEW);
  next_req_.clear_scanner_id();
  NewScanRequestPB* scan = next_req_.mutable_new_scan_request();
  scan->set_row_format_flags(configuration_.row_format_flags());
  const KuduScanner::ReadMode read_mode = configuration_.read_mode();
  switch (read_mode) {
    case KuduScanner::READ_LATEST:
      scan->set_read_mode(kudu::READ_LATEST);
      if (configuration_.has_snapshot_timestamp()) {
        LOG(FATAL) << "Snapshot timestamp should only be configured "
                      "for READ_AT_SNAPSHOT scan mode.";
      }
      break;
    case KuduScanner::READ_AT_SNAPSHOT:
      scan->set_read_mode(kudu::READ_AT_SNAPSHOT);
      if (configuration_.has_snapshot_timestamp()) {
        scan->set_snap_timestamp(configuration_.snapshot_timestamp());
      }
      break;
    case KuduScanner::READ_YOUR_WRITES:
      scan->set_read_mode(kudu::READ_YOUR_WRITES);
      if (configuration_.has_snapshot_timestamp()) {
        LOG(FATAL) << "Snapshot timestamp should only be configured "
                      "for READ_AT_SNAPSHOT scan mode.";
      }
      break;
    default:
      LOG(FATAL) << Substitute("$0: unexpected read mode", read_mode);
  }

  if (configuration_.is_fault_tolerant()) {
    scan->set_order_mode(kudu::ORDERED);
  } else {
    scan->set_order_mode(kudu::UNORDERED);
  }

  if (last_primary_key_.length() > 0) {
    VLOG(2) << "Setting NewScanRequestPB last_primary_key to hex value "
        << HexDump(last_primary_key_);
    scan->set_last_primary_key(last_primary_key_);
  }

  scan->set_cache_blocks(configuration_.spec().cache_blocks());

  // For consistent operations, propagate the timestamp among all operations
  // performed the context of the same client. For READ_YOUR_WRITES scan, use
  // the propagation timestamp from the scan config.
  uint64_t ts = KuduClient::kNoTimestamp;
  if (read_mode == KuduScanner::READ_YOUR_WRITES) {
    if (configuration_.has_lower_bound_propagation_timestamp()) {
      ts = configuration_.lower_bound_propagation_timestamp();
    }
  } else {
    ts = table_->client()->data_->GetLatestObservedTimestamp();
  }
  if (ts != KuduClient::kNoTimestamp) {
    scan->set_propagated_timestamp(ts);
  }

  // Set up the predicates.
  scan->clear_column_predicates();
  for (const auto& col_pred : configuration_.spec().predicates()) {
    ColumnPredicateToPB(col_pred.second, scan->add_column_predicates());
  }

  if (configuration_.spec().lower_bound_key()) {
    scan->mutable_start_primary_key()->assign(
      reinterpret_cast<const char*>(configuration_.spec().lower_bound_key()->encoded_key().data()),
      configuration_.spec().lower_bound_key()->encoded_key().size());
  } else {
    scan->clear_start_primary_key();
  }
  if (configuration_.spec().exclusive_upper_bound_key()) {
    scan->mutable_stop_primary_key()->assign(reinterpret_cast<const char*>(
          configuration_.spec().exclusive_upper_bound_key()->encoded_key().data()),
      configuration_.spec().exclusive_upper_bound_key()->encoded_key().size());
  } else {
    scan->clear_stop_primary_key();
  }
  RETURN_NOT_OK(SchemaToColumnPBs(*configuration_.projection(), scan->mutable_projected_columns(),
                                  SCHEMA_PB_WITHOUT_STORAGE_ATTRIBUTES | SCHEMA_PB_WITHOUT_IDS));

  for (int attempt = 1;; attempt++) {
    Synchronizer sync;
    table_->client()->data_->meta_cache_->LookupTabletByKeyOrNext(table_.get(),
                                                                  partition_key,
                                                                  deadline,
                                                                  &remote_,
                                                                  sync.AsStatusCallback());
    Status s = sync.Wait();
    if (s.IsNotFound()) {
      // No more tablets in the table.
      partition_pruner_.RemovePartitionKeyRange("");
      return Status::OK();
    }
    RETURN_NOT_OK(s);

    // Check if the meta cache returned a tablet covering a partition key range past
    // what we asked for. This can happen if the requested partition key falls
    // in a non-covered range. In this case we can potentially prune the tablet.
    if (partition_key < remote_->partition().partition_key_start() &&
        partition_pruner_.ShouldPrune(remote_->partition())) {
      partition_pruner_.RemovePartitionKeyRange(remote_->partition().partition_key_end());
      return Status::OK();
    }

    scan->set_tablet_id(remote_->tablet_id());

    RemoteTabletServer *ts;
    vector<RemoteTabletServer*> candidates;
    Status lookup_status = table_->client()->data_->GetTabletServer(
        table_->client(),
        remote_,
        configuration_.selection(),
        *blacklist,
        &candidates,
        &ts);
    // If we get ServiceUnavailable, this indicates that the tablet doesn't
    // currently have any known leader. We should sleep and retry, since
    // it's likely that the tablet is undergoing a leader election and will
    // soon have one.
    if (lookup_status.IsServiceUnavailable() && MonoTime::Now() < deadline) {
      // ServiceUnavailable means that we have already blacklisted all of the candidate
      // tablet servers. So, we clear the list so that we will cycle through them all
      // another time.
      blacklist->clear();
      int sleep_ms = attempt * 100;
      // TODO: should ensure that sleep_ms does not pass the provided deadline.
      VLOG(1) << "Tablet " << remote_->tablet_id() << " currently unavailable: "
              << lookup_status.ToString() << ". Sleeping for " << sleep_ms << "ms "
              << "and retrying...";
      SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
      continue;
    }
    RETURN_NOT_OK(lookup_status);
    CHECK(ts->proxy());
    ts_ = CHECK_NOTNULL(ts);
    proxy_ = ts_->proxy();

    bool allow_time_for_failover = candidates.size() > blacklist->size() + 1;
    ScanRpcStatus scan_status = SendScanRpc(deadline, allow_time_for_failover);
    if (scan_status.result == ScanRpcStatus::OK) {
      last_error_ = Status::OK();
      scan_attempts_ = 0;
      break;
    }
    scan_attempts_++;
    RETURN_NOT_OK(HandleError(scan_status, deadline, blacklist));
  }

  partition_pruner_.RemovePartitionKeyRange(remote_->partition().partition_key_end());

  next_req_.clear_new_scan_request();
  data_in_open_ = last_response_.has_data() && last_response_.data().num_rows() > 0;
  if (last_response_.has_more_results()) {
    next_req_.set_scanner_id(last_response_.scanner_id());
    VLOG(2) << "Opened tablet " << remote_->tablet_id()
            << ", scanner ID " << last_response_.scanner_id();
  } else if (last_response_.has_data()) {
    VLOG(2) << "Opened tablet " << remote_->tablet_id() << ", no scanner ID assigned";
  } else {
    VLOG(2) << "Opened tablet " << remote_->tablet_id() << " (no rows), no scanner ID assigned";
  }

  // If present in the response, set the snapshot timestamp and the encoded last
  // primary key.  This is used when retrying the scan elsewhere.  The last
  // primary key is also updated on each scan response.
  if (configuration().is_fault_tolerant()) {
    if (last_response_.has_last_primary_key()) {
      last_primary_key_ = last_response_.last_primary_key();
    }
  }

  if (configuration_.read_mode() == KuduScanner::READ_AT_SNAPSHOT &&
      !configuration_.has_snapshot_timestamp()) {
    // There must be a snapshot timestamp returned by the tablet server:
    // it's the first response from the tablet server when scanning in the
    // READ_AT_SNAPSHOT mode with unspecified snapshot timestamp.
    CHECK(last_response_.has_snap_timestamp());
    configuration_.SetSnapshotRaw(last_response_.snap_timestamp());
  }

  // For READ_YOUR_WRITES mode, updates the latest observed timestamp with
  // the chosen snapshot timestamp sent back from the server, to avoid
  // unnecessarily wait for subsequent reads.
  if (configuration_.read_mode() == KuduScanner::READ_YOUR_WRITES) {
    CHECK(last_response_.has_snap_timestamp());
    table_->client()->data_->UpdateLatestObservedTimestamp(
        last_response_.snap_timestamp());
  } else if (last_response_.has_propagated_timestamp()) {
    table_->client()->data_->UpdateLatestObservedTimestamp(
        last_response_.propagated_timestamp());
  }

  return Status::OK();
}

Status KuduScanner::Data::KeepAlive() {
  if (!open_) return Status::IllegalState("Scanner was not open.");
  // If there is no scanner to keep alive, we still return Status::OK().
  if (!last_response_.IsInitialized() || !last_response_.has_more_results() ||
      !next_req_.has_scanner_id()) {
    return Status::OK();
  }

  RpcController controller;
  controller.set_timeout(configuration_.timeout());
  tserver::ScannerKeepAliveRequestPB request;
  request.set_scanner_id(next_req_.scanner_id());
  tserver::ScannerKeepAliveResponsePB response;
  RETURN_NOT_OK(proxy_->ScannerKeepAlive(request, &response, &controller));
  if (response.has_error()) {
    return StatusFromPB(response.error().status());
  }
  return Status::OK();
}

bool KuduScanner::Data::MoreTablets() const {
  CHECK(open_);
  // TODO(KUDU-565): add a test which has a scan end on a tablet boundary
  return partition_pruner_.HasMorePartitionKeyRanges();
}

void KuduScanner::Data::PrepareRequest(RequestType state) {
  if (state == KuduScanner::Data::CLOSE) {
    next_req_.set_batch_size_bytes(0);
  } else if (configuration_.has_batch_size_bytes()) {
    next_req_.set_batch_size_bytes(configuration_.batch_size_bytes());
  } else {
    next_req_.clear_batch_size_bytes();
  }

  if (state == KuduScanner::Data::NEW) {
    next_req_.set_call_seq_id(0);
  } else {
    next_req_.set_call_seq_id(next_req_.call_seq_id() + 1);
  }
}

void KuduScanner::Data::UpdateLastError(const Status& error) {
  if (last_error_.ok() || last_error_.IsTimedOut()) {
    last_error_ = error;
  }
}

////////////////////////////////////////////////////////////
// KuduScanBatch
////////////////////////////////////////////////////////////

KuduScanBatch::Data::Data() : projection_(NULL), row_format_flags_(KuduScanner::NO_FLAGS) {}

KuduScanBatch::Data::~Data() {}

size_t KuduScanBatch::Data::CalculateProjectedRowSize(const Schema& proj) {
  return proj.byte_size() +
        (proj.has_nullables() ? BitmapSize(proj.num_columns()) : 0);
}

Status KuduScanBatch::Data::Reset(RpcController* controller,
                                  const Schema* projection,
                                  const KuduSchema* client_projection,
                                  uint64_t row_format_flags,
                                  gscoped_ptr<RowwiseRowBlockPB> resp_data) {
  CHECK(controller->finished());
  controller_.Swap(controller);
  projection_ = projection;
  projected_row_size_ = CalculateProjectedRowSize(*projection_);
  client_projection_ = client_projection;
  row_format_flags_ = row_format_flags;
  if (!resp_data) {
    // No new data; just clear out the old stuff.
    resp_data_.Clear();
    return Status::OK();
  }

  // There's new data. Swap it in and process it.
  resp_data_.Swap(resp_data.get());

  // First, rewrite the relative addresses into absolute ones.
  if (PREDICT_FALSE(!resp_data_.has_rows_sidecar())) {
    return Status::Corruption("Server sent invalid response: no row data");
  }

  Status s = controller_.GetInboundSidecar(resp_data_.rows_sidecar(), &direct_data_);
  if (!s.ok()) {
    return Status::Corruption("Server sent invalid response: "
        "row data sidecar index corrupt", s.ToString());
  }

  if (resp_data_.has_indirect_data_sidecar()) {
    Status s = controller_.GetInboundSidecar(resp_data_.indirect_data_sidecar(),
                                             &indirect_data_);
    if (!s.ok()) {
      return Status::Corruption("Server sent invalid response: "
          "indirect data sidecar index corrupt", s.ToString());
    }
  }

  bool pad_unixtime_micros_to_16_bytes = false;
  if (row_format_flags_ & KuduScanner::PAD_UNIXTIME_MICROS_TO_16_BYTES) {
    pad_unixtime_micros_to_16_bytes = true;
  }

  return RewriteRowBlockPointers(*projection_, resp_data_, indirect_data_, &direct_data_,
                                 pad_unixtime_micros_to_16_bytes);
}

void KuduScanBatch::Data::ExtractRows(vector<KuduScanBatch::RowPtr>* rows) {
  DCHECK_EQ(row_format_flags_, KuduScanner::NO_FLAGS) << "Cannot extract rows. "
      << "Row format modifier flags were selected: " << row_format_flags_;
  int n_rows = resp_data_.num_rows();
  rows->resize(n_rows);

  if (PREDICT_FALSE(n_rows == 0)) {
    // Early-out here to avoid a UBSAN failure.
    VLOG(2) << "Extracted 0 rows";
    return;
  }

  // Initialize each RowPtr with data from the response.
  //
  // Doing this resize and array indexing turns out to be noticeably faster
  // than using reserve and push_back.
  const uint8_t* src = direct_data_.data();
  KuduScanBatch::RowPtr* dst = &(*rows)[0];
  while (n_rows > 0) {
    *dst = KuduScanBatch::RowPtr(projection_, src);
    dst++;
    src += projected_row_size_;
    n_rows--;
  }
  VLOG(2) << "Extracted " << rows->size() << " rows";
}

void KuduScanBatch::Data::Clear() {
  resp_data_.Clear();
  controller_.Reset();
}

} // namespace client
} // namespace kudu
