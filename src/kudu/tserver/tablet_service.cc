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

#include "kudu/tserver/tablet_service.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <ostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/clock/clock.h"
#include "kudu/common/column_predicate.h"
#include "kudu/common/columnar_serialization.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/key_range.h"
#include "kudu/common/partition.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/consensus/replica_management.pb.h"
#include "kudu/consensus/time_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/casts.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/remote_user.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/rpc_verification_util.h"
#include "kudu/security/token.pb.h"
#include "kudu/security/token_verifier.h"
#include "kudu/server/server_base.h"
#include "kudu/tablet/compaction.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_metrics.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tablet/transactions/alter_schema_transaction.h"
#include "kudu/tablet/transactions/transaction.h"
#include "kudu/tablet/transactions/write_transaction.h"
#include "kudu/tserver/scanners.h"
#include "kudu/tserver/tablet_replica_lookup.h"
#include "kudu/tserver/tablet_server.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/tserver/tserver.pb.h"
#include "kudu/tserver/tserver_admin.pb.h"
#include "kudu/tserver/tserver_service.pb.h"
#include "kudu/util/auto_release_pool.h"
#include "kudu/util/bitset.h"
#include "kudu/util/crc.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/faststring.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/process_memory.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"
#include "kudu/util/trace_metrics.h"

DEFINE_int32(scanner_default_batch_size_bytes, 1024 * 1024,
             "The default size for batches of scan results");
TAG_FLAG(scanner_default_batch_size_bytes, advanced);
TAG_FLAG(scanner_default_batch_size_bytes, runtime);

DEFINE_int32(scanner_max_batch_size_bytes, 8 * 1024 * 1024,
             "The maximum batch size that a client may request for "
             "scan results.");
TAG_FLAG(scanner_max_batch_size_bytes, advanced);
TAG_FLAG(scanner_max_batch_size_bytes, runtime);

// The default value is sized to a power of 2 to improve BitmapCopy performance
// when copying a RowBlock (in ORDERED scans).
DEFINE_int32(scanner_batch_size_rows, 128,
             "The number of rows to batch for servicing scan requests.");
TAG_FLAG(scanner_batch_size_rows, advanced);
TAG_FLAG(scanner_batch_size_rows, runtime);

DEFINE_bool(scanner_allow_snapshot_scans_with_logical_timestamps, false,
            "If set, the server will support snapshot scans with logical timestamps.");
TAG_FLAG(scanner_allow_snapshot_scans_with_logical_timestamps, unsafe);

DEFINE_int32(scanner_max_wait_ms, 1000,
             "The maximum amount of time (in milliseconds) we'll hang a scanner thread waiting for "
             "safe time to advance or transactions to commit, even if its deadline allows waiting "
             "longer.");
TAG_FLAG(scanner_max_wait_ms, advanced);

// Fault injection flags.
DEFINE_int32(scanner_inject_latency_on_each_batch_ms, 0,
             "If set, the scanner will pause the specified number of milliesconds "
             "before reading each batch of data on the tablet server. "
             "Used for tests.");
TAG_FLAG(scanner_inject_latency_on_each_batch_ms, unsafe);

DEFINE_bool(scanner_inject_service_unavailable_on_continue_scan, false,
           "If set, the scanner will return a ServiceUnavailable Status on "
           "any Scan continuation RPC call. Used for tests.");
TAG_FLAG(scanner_inject_service_unavailable_on_continue_scan, unsafe);

DEFINE_bool(tserver_enforce_access_control, false,
            "If set, the server will apply fine-grained access control rules "
            "to client RPCs.");
TAG_FLAG(tserver_enforce_access_control, runtime);

DEFINE_double(tserver_inject_invalid_authz_token_ratio, 0.0,
              "Fraction of the time that authz token validation will fail. Used for tests.");
TAG_FLAG(tserver_inject_invalid_authz_token_ratio, hidden);

DECLARE_bool(raft_prepare_replacement_before_eviction);
DECLARE_int32(memory_limit_warn_threshold_percentage);
DECLARE_int32(tablet_history_max_age_sec);

using google::protobuf::RepeatedPtrField;
using kudu::consensus::BulkChangeConfigRequestPB;
using kudu::consensus::ChangeConfigRequestPB;
using kudu::consensus::ChangeConfigResponsePB;
using kudu::consensus::ConsensusRequestPB;
using kudu::consensus::ConsensusResponsePB;
using kudu::consensus::GetLastOpIdRequestPB;
using kudu::consensus::GetNodeInstanceRequestPB;
using kudu::consensus::GetNodeInstanceResponsePB;
using kudu::consensus::LeaderStepDownMode;
using kudu::consensus::LeaderStepDownRequestPB;
using kudu::consensus::LeaderStepDownResponsePB;
using kudu::consensus::OpId;
using kudu::consensus::RaftConsensus;
using kudu::consensus::RunLeaderElectionRequestPB;
using kudu::consensus::RunLeaderElectionResponsePB;
using kudu::consensus::StartTabletCopyRequestPB;
using kudu::consensus::StartTabletCopyResponsePB;
using kudu::consensus::TimeManager;
using kudu::consensus::UnsafeChangeConfigRequestPB;
using kudu::consensus::UnsafeChangeConfigResponsePB;
using kudu::consensus::VoteRequestPB;
using kudu::consensus::VoteResponsePB;
using kudu::fault_injection::MaybeTrue;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using kudu::rpc::ParseVerificationResult;
using kudu::rpc::ErrorStatusPB;
using kudu::rpc::RpcContext;
using kudu::rpc::RpcSidecar;
using kudu::security::TokenVerifier;
using kudu::security::TokenPB;
using kudu::server::ServerBase;
using kudu::tablet::AlterSchemaTransactionState;
using kudu::tablet::MvccSnapshot;
using kudu::tablet::TABLET_DATA_COPYING;
using kudu::tablet::TABLET_DATA_DELETED;
using kudu::tablet::TABLET_DATA_TOMBSTONED;
using kudu::tablet::Tablet;
using kudu::tablet::TabletReplica;
using kudu::tablet::TransactionCompletionCallback;
using kudu::tablet::WriteAuthorizationContext;
using kudu::tablet::WritePrivileges;
using kudu::tablet::WritePrivilegeType;
using kudu::tablet::WriteTransactionState;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace cfile {
extern const char* CFILE_CACHE_MISS_BYTES_METRIC_NAME;
extern const char* CFILE_CACHE_HIT_BYTES_METRIC_NAME;
}

namespace tserver {

const char* SCANNER_BYTES_READ_METRIC_NAME = "scanner_bytes_read";

namespace {

// Lookup the given tablet, only ensuring that it exists.
// If it does not, responds to the RPC associated with 'context' after setting
// resp->mutable_error() to indicate the failure reason.
//
// Returns true if successful.
template<class RespClass>
bool LookupTabletReplicaOrRespond(TabletReplicaLookupIf* tablet_manager,
                                  const string& tablet_id,
                                  RespClass* resp,
                                  rpc::RpcContext* context,
                                  scoped_refptr<TabletReplica>* replica) {
  Status s = tablet_manager->GetTabletReplica(tablet_id, replica);
  if (PREDICT_FALSE(!s.ok())) {
    if (s.IsServiceUnavailable()) {
      // If the tablet manager isn't initialized, the remote should check again
      // soon.
      SetupErrorAndRespond(resp->mutable_error(), s,
                           TabletServerErrorPB::UNKNOWN_ERROR, context);
    } else {
      SetupErrorAndRespond(resp->mutable_error(), s,
                           TabletServerErrorPB::TABLET_NOT_FOUND, context);
    }
    return false;
  }
  return true;
}

template<class RespClass>
void RespondTabletNotRunning(const scoped_refptr<TabletReplica>& replica,
                             tablet::TabletStatePB tablet_state,
                             RespClass* resp,
                             rpc::RpcContext* context) {
  Status s = Status::IllegalState("Tablet not RUNNING",
                                  tablet::TabletStatePB_Name(tablet_state));
  auto error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
  if (replica->tablet_metadata()->tablet_data_state() == TABLET_DATA_TOMBSTONED ||
      replica->tablet_metadata()->tablet_data_state() == TABLET_DATA_DELETED) {
    // Treat tombstoned tablets as if they don't exist for most purposes.
    // This takes precedence over failed, since we don't reset the failed
    // status of a TabletReplica when deleting it. Only tablet copy does that.
    error_code = TabletServerErrorPB::TABLET_NOT_FOUND;
  } else if (tablet_state == tablet::FAILED) {
    s = s.CloneAndAppend(replica->error().ToString());
    error_code = TabletServerErrorPB::TABLET_FAILED;
  }
  SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
}

// Check if the replica is running.
template<class RespClass>
bool CheckTabletReplicaRunningOrRespond(const scoped_refptr<TabletReplica>& replica,
                                        RespClass* resp,
                                        rpc::RpcContext* context) {
  // Check RUNNING state.
  tablet::TabletStatePB state = replica->state();
  if (PREDICT_FALSE(state != tablet::RUNNING)) {
    RespondTabletNotRunning(replica, state, resp, context);
    return false;
  }
  return true;
}

// Lookup the given tablet, ensuring that it both exists and is RUNNING.
// If it is not, responds to the RPC associated with 'context' after setting
// resp->mutable_error() to indicate the failure reason.
//
// Returns true if successful.
template<class RespClass>
bool LookupRunningTabletReplicaOrRespond(TabletReplicaLookupIf* tablet_manager,
                                         const string& tablet_id,
                                         RespClass* resp,
                                         rpc::RpcContext* context,
                                         scoped_refptr<TabletReplica>* replica) {
  if (!LookupTabletReplicaOrRespond(tablet_manager, tablet_id, resp, context, replica)) {
    return false;
  }
  if (!CheckTabletReplicaRunningOrRespond(*replica, resp, context)) {
    return false;
  }
  return true;
}

template<class ReqClass, class RespClass>
bool CheckUuidMatchOrRespond(TabletReplicaLookupIf* tablet_manager,
                             const char* method_name,
                             const ReqClass* req,
                             RespClass* resp,
                             rpc::RpcContext* context) {
  const string& local_uuid = tablet_manager->NodeInstance().permanent_uuid();
  if (PREDICT_FALSE(!req->has_dest_uuid())) {
    // Maintain compat in release mode, but complain.
    string msg = Substitute("$0: Missing destination UUID in request from $1: $2",
                            method_name, context->requestor_string(), SecureShortDebugString(*req));
#ifdef NDEBUG
    KLOG_EVERY_N(ERROR, 100) << msg;
#else
    LOG(DFATAL) << msg;
#endif
    return true;
  }
  if (PREDICT_FALSE(req->dest_uuid() != local_uuid)) {
    Status s = Status::InvalidArgument(Substitute("$0: Wrong destination UUID requested. "
                                                  "Local UUID: $1. Requested UUID: $2",
                                                  method_name, local_uuid, req->dest_uuid()));
    LOG(WARNING) << s.ToString() << ": from " << context->requestor_string()
                 << ": " << SecureShortDebugString(*req);
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::WRONG_SERVER_UUID, context);
    return false;
  }
  return true;
}

template<class RespClass>
bool GetConsensusOrRespond(const scoped_refptr<TabletReplica>& replica,
                           RespClass* resp,
                           rpc::RpcContext* context,
                           shared_ptr<RaftConsensus>* consensus_out) {
  shared_ptr<RaftConsensus> tmp_consensus = replica->shared_consensus();
  if (!tmp_consensus) {
    Status s = Status::ServiceUnavailable("Raft Consensus unavailable",
                                          "Tablet replica not initialized");
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::TABLET_NOT_RUNNING, context);
    return false;
  }
  *consensus_out = std::move(tmp_consensus);
  return true;
}

template<class RespClass>
bool CheckTabletServerNotQuiescingOrRespond(const TabletServer* server, RespClass* resp,
                                            rpc::RpcContext* context) {
  if (PREDICT_FALSE(server->quiescing())) {
    Status s = Status::ServiceUnavailable("Tablet server is quiescing");
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::TABLET_NOT_RUNNING, context);
    return false;
  }
  return true;
}

Status GetTabletRef(const scoped_refptr<TabletReplica>& replica,
                    shared_ptr<Tablet>* tablet,
                    TabletServerErrorPB::Code* error_code) {
  *DCHECK_NOTNULL(tablet) = replica->shared_tablet();
  if (PREDICT_FALSE(!*tablet)) {
    *error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
    return Status::IllegalState("Tablet is not running");
  }
  return Status::OK();
}

template <class RespType>
void HandleUnknownError(const Status& s, RespType* resp, RpcContext* context) {
  resp->Clear();
  SetupErrorAndRespond(resp->mutable_error(), s,
                       TabletServerErrorPB::UNKNOWN_ERROR,
                       context);
}

template <class ReqType, class RespType>
void HandleResponse(const ReqType* req, RespType* resp,
                    RpcContext* context, const Status& s) {
  if (PREDICT_FALSE(!s.ok())) {
    HandleUnknownError(s, resp, context);
    return;
  }
  context->RespondSuccess();
}

} // namespace

typedef ListTabletsResponsePB::StatusAndSchemaPB StatusAndSchemaPB;

// Populates 'required_column_privileges' with the column-level privileges
// required to perform the scan specified by 'scan_pb', consulting the column
// IDs found in 'schema'.
//
// Users of NewScanRequestPB (e.g. Scans and Checksums) require the following
// privileges:
//   if no projected columns (i.e. a "counting" scan) ||
//       projected columns has virtual column (e.g. "diff" scan):
//     SCAN ON TABLE || foreach (column): SCAN ON COLUMN
//   else:
//     if uses pk (e.g. ORDERED scan, or primary key fields set):
//       foreach(primary key column): SCAN ON COLUMN
//     foreach(projected column): SCAN ON COLUMN
//     foreach(predicated column): SCAN ON COLUMN
//
// Returns false if the request is malformed (e.g. unknown non-virtual column
// name), and sends an error response via 'context' if so. 'req_type' is used
// to add context in logs.
static bool GetScanPrivilegesOrRespond(const NewScanRequestPB& scan_pb, const Schema& schema,
                                       const string& req_type,
                                       unordered_set<ColumnId>* required_column_privileges,
                                       RpcContext* context) {
  const auto respond_not_authorized = [&] (const string& col_name) {
    LOG(WARNING) << Substitute("rejecting $0 request from $1: no column named '$2'",
                               req_type, context->requestor_string(), col_name);
    context->RespondRpcFailure(rpc::ErrorStatusPB::FATAL_UNAUTHORIZED,
        Status::NotAuthorized(Substitute("not authorized to $0", req_type)));
  };
  // If there is no projection (i.e. this is a "counting" scan), the user
  // needs full scan privileges on the table.
  if (scan_pb.projected_columns_size() == 0) {
    *required_column_privileges = unordered_set<ColumnId>(schema.column_ids().begin(),
                                                          schema.column_ids().end());
    return true;
  }
  unordered_set<ColumnId> required_privileges;
  // Determine the scan's projected key column IDs.
  for (int i = 0; i < scan_pb.projected_columns_size(); i++) {
    boost::optional<ColumnSchema> projected_column;
    Status s = ColumnSchemaFromPB(scan_pb.projected_columns(i), &projected_column);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(WARNING) << s.ToString();
      context->RespondRpcFailure(rpc::ErrorStatusPB::ERROR_INVALID_REQUEST, s);
      return false;
    }
    // A projection may contain virtual columns, which don't exist in the
    // tablet schema. If we were to search for a virtual column, we would
    // incorrectly get a "not found" error. To reconcile this with the fact
    // that we want to return an authorization error if the user has requested
    // a non-virtual column that doesn't exist, we require full scan privileges
    // for virtual columns.
    if (projected_column->type_info()->is_virtual()) {
      *required_column_privileges = unordered_set<ColumnId>(schema.column_ids().begin(),
                                                            schema.column_ids().end());
      return true;
    }
    int col_idx = schema.find_column(projected_column->name());
    if (col_idx == Schema::kColumnNotFound) {
      respond_not_authorized(scan_pb.projected_columns(i).name());
      return false;
    }
    EmplaceIfNotPresent(&required_privileges, schema.column_id(col_idx));
  }
  // Ordered scans and any scans that make use of the primary key require
  // privileges to scan across all primary key columns.
  if (scan_pb.order_mode() == ORDERED ||
      scan_pb.has_start_primary_key() ||
      scan_pb.has_stop_primary_key() ||
      scan_pb.has_last_primary_key()) {
    const auto& key_cols = schema.get_key_column_ids();
    required_privileges.insert(key_cols.begin(), key_cols.end());
  }
  // Determine the scan's predicate column IDs.
  for (int i = 0; i < scan_pb.column_predicates_size(); i++) {
    int col_idx = schema.find_column(scan_pb.column_predicates(i).column());
    if (col_idx == Schema::kColumnNotFound) {
      respond_not_authorized(scan_pb.column_predicates(i).column());
      return false;
    }
    EmplaceIfNotPresent(&required_privileges, schema.column_id(col_idx));
  }
  // Do the same for the DEPRECATED_range_predicates field. Even though this
  // field is deprecated, it is still exposed as a part of our public API and
  // thus needs to be taken into account.
  for (int i = 0; i < scan_pb.deprecated_range_predicates_size(); i++) {
    int col_idx = schema.find_column(scan_pb.deprecated_range_predicates(i).column().name());
    if (col_idx == Schema::kColumnNotFound) {
      respond_not_authorized(scan_pb.deprecated_range_predicates(i).column().name());
      return false;
    }
    EmplaceIfNotPresent(&required_privileges, schema.column_id(col_idx));
  }
  *required_column_privileges = std::move(required_privileges);
  return true;
}

// Checks the column-level privileges required to perform the scan specified by
// 'scan_pb' against the authorized column IDs listed in
// 'authorized_column_ids', consulting the column IDs found in 'schema'.
//
// Returns false if the scan isn't authorized and uses 'context' to send an
// error response. 'req_type' is used for logging'.
static bool CheckScanPrivilegesOrRespond(const NewScanRequestPB& scan_pb, const Schema& schema,
                                         const unordered_set<ColumnId>& authorized_column_ids,
                                         const string& req_type, RpcContext* context) {
  unordered_set<ColumnId> required_column_privileges;
  if (!GetScanPrivilegesOrRespond(scan_pb, schema, req_type,
                                  &required_column_privileges, context)) {
    return false;
  }
  for (const auto& required_col_id : required_column_privileges) {
    if (!ContainsKey(authorized_column_ids, required_col_id)) {
      LOG(WARNING) << Substitute("rejecting $0 request from $1: authz token doesn't "
                                 "authorize column ID $2", req_type, context->requestor_string(),
                                 required_col_id);
      context->RespondRpcFailure(rpc::ErrorStatusPB::FATAL_UNAUTHORIZED,
          Status::NotAuthorized(Substitute("not authorized to $0", req_type)));
      return false;
    }
  }
  return true;
}

// Returns false if the table ID of 'privilege' doesn't match 'table_id',
// responding with an error via 'context' if so. Otherwise, returns true.
// 'req_type' is used for logging purposes.
static bool CheckMatchingTableIdOrRespond(const security::TablePrivilegePB& privilege,
                                          const string& table_id, const string& req_type,
                                          RpcContext* context) {
  if (privilege.table_id() != table_id) {
    LOG(WARNING) << Substitute("rejecting $0 request from $1: '$2', expected '$3'",
                               req_type, context->requestor_string(),
                               privilege.table_id(), table_id);
    context->RespondRpcFailure(rpc::ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
        Status::NotAuthorized("authorization token is for the wrong table ID"));
    return false;
  }
  return true;
}

// Returns false if the privilege has neither full scan privileges nor any
// column-level scan privileges, in which case any scan-like request should be
// rejected. Otherwise returns true, and returns any column-level scan
// privileges in 'privilege'.
static bool CheckMayHaveScanPrivilegesOrRespond(const security::TablePrivilegePB& privilege,
                                                const string& req_type,
                                                unordered_set<ColumnId>* authorized_column_ids,
                                                RpcContext* context) {
  DCHECK(authorized_column_ids);
  DCHECK(authorized_column_ids->empty());
  if (privilege.column_privileges_size() > 0) {
    for (const auto& col_id_and_privilege : privilege.column_privileges()) {
      if (col_id_and_privilege.second.scan_privilege()) {
        EmplaceOrDie(authorized_column_ids, col_id_and_privilege.first);
      }
    }
  }
  if (privilege.scan_privilege() || !authorized_column_ids->empty()) {
    return true;
  }
  LOG(WARNING) << Substitute("rejecting $0 request from $1: no column privileges",
                             req_type, context->requestor_string());
  context->RespondRpcFailure(rpc::ErrorStatusPB::FATAL_UNAUTHORIZED,
      Status::NotAuthorized(Substitute("not authorized to $0", req_type)));
  return false;
}

// Verifies the authorization token's correctness. Returns false and sends an
// appropriate response if the request's authz token is invalid.
template <class AuthorizableRequest>
static bool VerifyAuthzTokenOrRespond(const TokenVerifier& token_verifier,
                                      const AuthorizableRequest& req,
                                      rpc::RpcContext* context,
                                      TokenPB* token) {
  DCHECK(token);
  if (!req.has_authz_token()) {
    context->RespondRpcFailure(rpc::ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
        Status::NotAuthorized("no authorization token presented"));
    return false;
  }
  TokenPB token_pb;
  const auto result = token_verifier.VerifyTokenSignature(req.authz_token(), &token_pb);
  ErrorStatusPB::RpcErrorCodePB error;
  Status s = ParseVerificationResult(result,
      ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN, &error);
  if (!s.ok()) {
    context->RespondRpcFailure(error, s.CloneAndPrepend("authz token verification failure"));
    return false;
  }
  if (!token_pb.has_authz() ||
      !token_pb.authz().has_table_privilege() ||
      token_pb.authz().username() != context->remote_user().username()) {
    context->RespondRpcFailure(ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
        Status::NotAuthorized("invalid authorization token presented"));
    return false;
  }
  if (MaybeTrue(FLAGS_tserver_inject_invalid_authz_token_ratio)) {
    context->RespondRpcFailure(ErrorStatusPB::ERROR_INVALID_AUTHORIZATION_TOKEN,
        Status::NotAuthorized("INJECTED FAILURE"));
    return false;
  }
  *token = std::move(token_pb);
  return true;
}

static void SetupErrorAndRespond(TabletServerErrorPB* error,
                                 const Status& s,
                                 TabletServerErrorPB::Code code,
                                 rpc::RpcContext* context) {
  // Non-authorized errors will drop the connection.
  if (code == TabletServerErrorPB::NOT_AUTHORIZED) {
    DCHECK(s.IsNotAuthorized());
    context->RespondRpcFailure(rpc::ErrorStatusPB::FATAL_UNAUTHORIZED, s);
    return;
  }
  // Generic "service unavailable" errors will cause the client to retry later.
  if ((code == TabletServerErrorPB::UNKNOWN_ERROR ||
       code == TabletServerErrorPB::THROTTLED) && s.IsServiceUnavailable()) {
    context->RespondRpcFailure(rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY, s);
    return;
  }

  StatusToPB(s, error->mutable_status());
  error->set_code(code);
  context->RespondNoCache();
}

template <class ReqType, class RespType>
void HandleErrorResponse(const ReqType* req, RespType* resp, RpcContext* context,
                         const boost::optional<TabletServerErrorPB::Code>& error_code,
                         const Status& s) {
  resp->Clear();
  if (error_code) {
    SetupErrorAndRespond(resp->mutable_error(), s, *error_code, context);
  } else {
    HandleUnknownError(s, resp, context);
  }
}

// A transaction completion callback that responds to the client when transactions
// complete and sets the client error if there is one to set.
template<class Response>
class RpcTransactionCompletionCallback : public TransactionCompletionCallback {
 public:
  RpcTransactionCompletionCallback(rpc::RpcContext* context,
                                   Response* response)
 : context_(context),
   response_(response) {}

  virtual void TransactionCompleted() OVERRIDE {
    if (!status_.ok()) {
      LOG(WARNING) << Substitute("failed transaction from $0: $1",
                                 context_->requestor_string(), status_.ToString());
      SetupErrorAndRespond(get_error(), status_, code_, context_);
    } else {
      context_->RespondSuccess();
    }
  };

 private:

  TabletServerErrorPB* get_error() {
    return response_->mutable_error();
  }

  rpc::RpcContext* context_;
  Response* response_;
  tablet::TransactionState* state_;
};

// Generic interface to handle scan results.
class ScanResultCollector {
 public:
  virtual void HandleRowBlock(Scanner* scanner,
                              const RowBlock& row_block) = 0;

  // Returns number of bytes which will be returned in the response.
  virtual int64_t ResponseSize() const = 0;

  // Return the number of rows actually returned to the client.
  virtual int64_t NumRowsReturned() const = 0;

  // Initialize the serializer with the given row format flags.
  //
  // This is a separate function instead of a constructor argument passed to specific
  // collector implementations because, currently, the collector is built before the
  // request is decoded and checked for 'row_format_flags'.
  //
  // Does nothing by default.
  virtual Status InitSerializer(uint64_t /* row_format_flags */) {
    return Status::OK();
  }

  CpuTimes* cpu_times() {
    return &cpu_times_;
  }

 private:
  CpuTimes cpu_times_;
};

namespace {

// Given a RowBlock, set last_primary_key to the primary key of the last selected row
// in the RowBlock. If no row is selected, last_primary_key is not set.
void SetLastRow(const RowBlock& row_block, faststring* last_primary_key) {
  // Find the last selected row and save its encoded key.
  const SelectionVector* sel = row_block.selection_vector();
  if (sel->AnySelected()) {
    for (int i = sel->nrows() - 1; i >= 0; i--) {
      if (sel->IsRowSelected(i)) {
        RowBlockRow last_row = row_block.row(i);
        const Schema* schema = last_row.schema();
        schema->EncodeComparableKey(last_row, last_primary_key);
        break;
      }
    }
  }
}

// Interface for serializing results into a scan response.
class ResultSerializer {
 public:
  virtual ~ResultSerializer() = default;

  // Add the given RowBlock to the pending response.
  virtual int SerializeRowBlock(const RowBlock& row_block,
                                const Schema* client_projection_schema) = 0;

  // Return the approximate size (in bytes) of the pending response. Once this
  // result is greater than the client's requested batch size, the pending rows
  // will be returned to the client.
  virtual size_t ResponseSize() const = 0;

  // Serialize the pending rows into the response protobuf.
  // Must be called at most once.
  virtual void SetupResponse(rpc::RpcContext* context, ScanResponsePB* resp) = 0;
};

class RowwiseResultSerializer : public ResultSerializer {
 public:
  RowwiseResultSerializer(int batch_size_bytes, uint64_t flags)
      : rows_data_(batch_size_bytes * 11 / 10),
        indirect_data_(batch_size_bytes * 11 / 10),
        pad_unixtime_micros_to_16_bytes_(flags & RowFormatFlags::PAD_UNIX_TIME_MICROS_TO_16_BYTES) {
    // TODO(todd): use a chain of faststrings instead of a single one to avoid
    // allocating this large buffer. Large buffer allocations are slow and
    // potentially wasteful.
  }

  int SerializeRowBlock(const RowBlock& row_block,
                        const Schema* client_projection_schema) override {
    int num_selected = kudu::SerializeRowBlock(
        row_block, client_projection_schema,
        &rows_data_, &indirect_data_, pad_unixtime_micros_to_16_bytes_);
    rowblock_pb_.set_num_rows(rowblock_pb_.num_rows() + num_selected);
    return num_selected;
  }

  size_t ResponseSize() const override {
    return rows_data_.size() + indirect_data_.size();
  }

  void SetupResponse(rpc::RpcContext* context, ScanResponsePB* resp) override {
    CHECK(!done_);
    done_ = true;

    *resp->mutable_data() = std::move(rowblock_pb_);
    // Add sidecar data to context and record the returned indices.
    int rows_idx;
    CHECK_OK(context->AddOutboundSidecar(
        RpcSidecar::FromFaststring((std::move(rows_data_))), &rows_idx));
    resp->mutable_data()->set_rows_sidecar(rows_idx);

    // Add indirect data as a sidecar, if applicable.
    if (indirect_data_.size() > 0) {
      int indirect_idx;
      CHECK_OK(context->AddOutboundSidecar(
          RpcSidecar::FromFaststring(std::move(indirect_data_)), &indirect_idx));
      resp->mutable_data()->set_indirect_data_sidecar(indirect_idx);
    }
  }

 private:
  RowwiseRowBlockPB rowblock_pb_;
  faststring rows_data_;
  faststring indirect_data_;
  bool pad_unixtime_micros_to_16_bytes_;
  bool done_ = false;
};

class ColumnarResultSerializer : public ResultSerializer {
 public:
  static Status Create(uint64_t flags, unique_ptr<ResultSerializer>* serializer) {
    if (flags & ~RowFormatFlags::COLUMNAR_LAYOUT) {
      return Status::InvalidArgument("Row format flags not supported with columnar layout");
    }
    serializer->reset(new ColumnarResultSerializer());
    return Status::OK();
  }

  int SerializeRowBlock(const RowBlock& row_block,
                        const Schema* client_projection_schema) override {
    CHECK(!done_);
    int n_sel = SerializeRowBlockColumnar(row_block, client_projection_schema, &results_);
    num_rows_ += n_sel;
    return n_sel;
  }

  size_t ResponseSize() const override {
    CHECK(!done_);

    int total = 0;
    for (const auto& col : results_.columns) {
      total += col.data.size();
      if (col.indirect_data) {
        total += col.indirect_data->size();
      }
      if (col.non_null_bitmap) {
        total += col.non_null_bitmap->size();
      }
    }
    return total;
  }

  void SetupResponse(rpc::RpcContext* context, ScanResponsePB* resp) override {
    CHECK(!done_);
    done_ = true;
    ColumnarRowBlockPB* data = resp->mutable_columnar_data();
    for (auto& col : results_.columns) {
      auto* col_pb = data->add_columns();
      int sidecar_idx;
      CHECK_OK(context->AddOutboundSidecar(
          RpcSidecar::FromFaststring((std::move(col.data))), &sidecar_idx));
      col_pb->set_data_sidecar(sidecar_idx);

      if (col.indirect_data) {
        CHECK_OK(context->AddOutboundSidecar(
            RpcSidecar::FromFaststring((std::move(*col.indirect_data))), &sidecar_idx));
        col_pb->set_indirect_data_sidecar(sidecar_idx);
      }

      if (col.non_null_bitmap) {
        CHECK_OK(context->AddOutboundSidecar(
            RpcSidecar::FromFaststring((std::move(*col.non_null_bitmap))), &sidecar_idx));
        col_pb->set_non_null_bitmap_sidecar(sidecar_idx);
      }
    }
    data->set_num_rows(num_rows_);
  }

 private:
  ColumnarResultSerializer() {}

  int64_t num_rows_ = 0;
  ColumnarSerializedBatch results_;
  bool done_ = false;
};

} // anonymous namespace

// Copies the scan result to the given row block PB and data buffers.
//
// This implementation is used in the common case where a client is running
// a scan and the data needs to be returned to the client.
//
// (This is in contrast to some other ScanResultCollector implementation that
// might do an aggregation or gather some other types of statistics via a
// server-side scan and thus never need to return the actual data.)
class ScanResultCopier : public ScanResultCollector {
 public:
  explicit ScanResultCopier(int batch_size_bytes)
      : batch_size_bytes_(batch_size_bytes),
        num_rows_returned_(0) {
  }

  void HandleRowBlock(Scanner* scanner, const RowBlock& row_block) override {
    int num_selected = serializer_->SerializeRowBlock(
        row_block, scanner->client_projection_schema());

    if (num_selected > 0) {
      num_rows_returned_ += num_selected;
      scanner->add_num_rows_returned(num_selected);
      SetLastRow(row_block, &last_primary_key_);
    }
  }

  // Returns number of bytes buffered to return.
  int64_t ResponseSize() const override {
    return serializer_->ResponseSize();
  }

  int64_t NumRowsReturned() const override {
    return num_rows_returned_;
  }

  Status InitSerializer(uint64_t row_format_flags) override {
    if (serializer_) {
      // TODO(todd) for the NewScanner case, this gets called twice
      // which is a bit ugly. Refactor to avoid!
      return Status::OK();
    }
    if (row_format_flags & COLUMNAR_LAYOUT) {
      return ColumnarResultSerializer::Create(row_format_flags, &serializer_);
    }
    serializer_.reset(new RowwiseResultSerializer(batch_size_bytes_, row_format_flags));
    return Status::OK();
  }

  void SetupResponse(rpc::RpcContext* context, ScanResponsePB* resp) {
    if (serializer_) {
      serializer_->SetupResponse(context, resp);
    }

    // Set the last row found by the collector.
    //
    // We could have an empty batch if all the remaining rows are filtered by the
    // predicate, in which case do not set the last row.
    if (last_primary_key_.length() > 0) {
      resp->set_last_primary_key(last_primary_key_.ToString());
    }
  }

 private:
  int batch_size_bytes_;
  int64_t num_rows_returned_;
  faststring last_primary_key_;
  unique_ptr<ResultSerializer> serializer_;

  DISALLOW_COPY_AND_ASSIGN(ScanResultCopier);
};

// Checksums the scan result.
class ScanResultChecksummer : public ScanResultCollector {
 public:
  ScanResultChecksummer()
      : crc_(crc::GetCrc32cInstance()),
        agg_checksum_(0),
        rows_checksummed_(0) {
  }

  virtual void HandleRowBlock(Scanner* scanner,
                              const RowBlock& row_block) OVERRIDE {

    const Schema* client_projection_schema = scanner->client_projection_schema();
    if (!client_projection_schema) {
      client_projection_schema = row_block.schema();
    }

    size_t nrows = row_block.nrows();
    for (size_t i = 0; i < nrows; i++) {
      if (!row_block.selection_vector()->IsRowSelected(i)) continue;
      uint32_t row_crc = CalcRowCrc32(*client_projection_schema, row_block.row(i));
      agg_checksum_ += row_crc;
      rows_checksummed_++;
    }
    // Find the last selected row and save its encoded key.
    SetLastRow(row_block, &encoded_last_row_);
  }

  // Returns a constant -- we only return checksum based on a time budget.
  virtual int64_t ResponseSize() const OVERRIDE { return sizeof(agg_checksum_); }

  virtual int64_t NumRowsReturned() const OVERRIDE {
    return 0;
  }

  int64_t rows_checksummed() const {
    return rows_checksummed_;
  }

  // Accessors for initializing / setting the checksum.
  void set_agg_checksum(uint64_t value) { agg_checksum_ = value; }
  uint64_t agg_checksum() const { return agg_checksum_; }

 private:
  // Calculates a CRC32C for the given row.
  uint32_t CalcRowCrc32(const Schema& projection, const RowBlockRow& row) {
    tmp_buf_.clear();

    for (size_t j = 0; j < projection.num_columns(); j++) {
      uint32_t col_index = static_cast<uint32_t>(j);  // For the CRC.
      tmp_buf_.append(&col_index, sizeof(col_index));
      ColumnBlockCell cell = row.cell(j);
      if (cell.is_nullable()) {
        uint8_t is_defined = cell.is_null() ? 0 : 1;
        tmp_buf_.append(&is_defined, sizeof(is_defined));
        if (!is_defined) continue;
      }
      if (cell.typeinfo()->physical_type() == BINARY) {
        const Slice* data = reinterpret_cast<const Slice *>(cell.ptr());
        tmp_buf_.append(data->data(), data->size());
      } else {
        tmp_buf_.append(cell.ptr(), cell.size());
      }
    }

    uint64_t row_crc = 0;
    crc_->Compute(tmp_buf_.data(), tmp_buf_.size(), &row_crc, nullptr);
    return static_cast<uint32_t>(row_crc); // CRC32 only uses the lower 32 bits.
  }


  faststring tmp_buf_;
  crc::Crc* const crc_;
  uint64_t agg_checksum_;
  int64_t rows_checksummed_;
  faststring encoded_last_row_;

  DISALLOW_COPY_AND_ASSIGN(ScanResultChecksummer);
};

// Return the batch size to use for a given request, after clamping
// the user-requested request within the server-side allowable range.
// This is only a hint, really more of a threshold since returned bytes
// may exceed this limit, but hopefully only by a little bit.
static size_t GetMaxBatchSizeBytesHint(const ScanRequestPB* req) {
  if (!req->has_batch_size_bytes()) {
    return FLAGS_scanner_default_batch_size_bytes;
  }

  return std::min(req->batch_size_bytes(),
                  implicit_cast<uint32_t>(FLAGS_scanner_max_batch_size_bytes));
}

TabletServiceImpl::TabletServiceImpl(TabletServer* server)
  : TabletServerServiceIf(server->metric_entity(), server->result_tracker()),
    server_(server) {
}

bool TabletServiceImpl::AuthorizeClientOrServiceUser(const google::protobuf::Message* /*req*/,
                                                     google::protobuf::Message* /*resp*/,
                                                     rpc::RpcContext* context) {
  return server_->Authorize(context, ServerBase::SUPER_USER | ServerBase::USER |
                            ServerBase::SERVICE_USER);
}

bool TabletServiceImpl::AuthorizeListTablets(const google::protobuf::Message* req,
                                             google::protobuf::Message* resp,
                                             rpc::RpcContext* context) {
  if (FLAGS_tserver_enforce_access_control) {
    return server_->Authorize(context, ServerBase::SUPER_USER);
  }
  return AuthorizeClient(req, resp, context);
}

bool TabletServiceImpl::AuthorizeClient(const google::protobuf::Message* /*req*/,
                                        google::protobuf::Message* /*resp*/,
                                        rpc::RpcContext* context) {
  return server_->Authorize(context, ServerBase::SUPER_USER | ServerBase::USER);
}

void TabletServiceImpl::Ping(const PingRequestPB* /*req*/,
                             PingResponsePB* /*resp*/,
                             rpc::RpcContext* context) {
  context->RespondSuccess();
}

TabletServiceAdminImpl::TabletServiceAdminImpl(TabletServer* server)
  : TabletServerAdminServiceIf(server->metric_entity(), server->result_tracker()),
    server_(server) {
}

bool TabletServiceAdminImpl::AuthorizeServiceUser(const google::protobuf::Message* /*req*/,
                                                  google::protobuf::Message* /*resp*/,
                                                  rpc::RpcContext* context) {
  return server_->Authorize(context, ServerBase::SUPER_USER | ServerBase::SERVICE_USER);
}

void TabletServiceAdminImpl::AlterSchema(const AlterSchemaRequestPB* req,
                                         AlterSchemaResponsePB* resp,
                                         rpc::RpcContext* context) {
  if (!CheckUuidMatchOrRespond(server_->tablet_manager(), "AlterSchema", req, resp, context)) {
    return;
  }
  DVLOG(3) << "Received Alter Schema RPC: " << SecureDebugString(*req);

  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(server_->tablet_manager(), req->tablet_id(), resp,
                                           context, &replica)) {
    return;
  }

  uint32_t schema_version = replica->tablet_metadata()->schema_version();

  // If the schema was already applied, respond as succeded
  if (schema_version == req->schema_version()) {
    // Sanity check, to verify that the tablet should have the same schema
    // specified in the request.
    Schema req_schema;
    Status s = SchemaFromPB(req->schema(), &req_schema);
    if (!s.ok()) {
      SetupErrorAndRespond(resp->mutable_error(), s,
                           TabletServerErrorPB::INVALID_SCHEMA, context);
      return;
    }

    Schema tablet_schema = replica->tablet_metadata()->schema();
    if (req_schema.Equals(tablet_schema)) {
      context->RespondSuccess();
      return;
    }

    schema_version = replica->tablet_metadata()->schema_version();
    if (schema_version == req->schema_version()) {
      LOG(ERROR) << "The current schema does not match the request schema."
                 << " version=" << schema_version
                 << " current-schema=" << tablet_schema.ToString()
                 << " request-schema=" << req_schema.ToString()
                 << " (corruption)";
      SetupErrorAndRespond(resp->mutable_error(),
                           Status::Corruption("got a different schema for the same version number"),
                           TabletServerErrorPB::MISMATCHED_SCHEMA, context);
      return;
    }
  }

  // If the current schema is newer than the one in the request reject the request.
  if (schema_version > req->schema_version()) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::InvalidArgument("Tablet has a newer schema"),
                         TabletServerErrorPB::TABLET_HAS_A_NEWER_SCHEMA, context);
    return;
  }

  unique_ptr<AlterSchemaTransactionState> tx_state(
    new AlterSchemaTransactionState(replica.get(), req, resp));

  tx_state->set_completion_callback(unique_ptr<TransactionCompletionCallback>(
      new RpcTransactionCompletionCallback<AlterSchemaResponsePB>(context,
                                                                  resp)));

  // Submit the alter schema op. The RPC will be responded to asynchronously.
  Status s = replica->SubmitAlterSchema(std::move(tx_state));
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
}

bool TabletServiceAdminImpl::SupportsFeature(uint32_t feature) const {
  switch (feature) {
    case TabletServerFeatures::COLUMN_PREDICATES:
    case TabletServerFeatures::PAD_UNIXTIME_MICROS_TO_16_BYTES:
    case TabletServerFeatures::QUIESCING:
    case TabletServerFeatures::BLOOM_FILTER_PREDICATE:
      return true;
    default:
      return false;
  }
}

void TabletServiceAdminImpl::Quiesce(const QuiesceTabletServerRequestPB* req,
                                     QuiesceTabletServerResponsePB* resp,
                                     rpc::RpcContext* context) {
  if (req->has_quiesce()) {
    bool quiesce_tserver = req->quiesce();
    *server_->mutable_quiescing() = quiesce_tserver;
    LOG(INFO) << Substitute("Tablet server $0 set to $1",
                            server_->fs_manager()->uuid(),
                            (quiesce_tserver ? "quiescing" : "not quiescing"));
  }
  if (req->return_stats()) {
    resp->set_num_leaders(server_->num_raft_leaders()->value());
    resp->set_num_active_scanners(server_->scanner_manager()->CountActiveScanners());
    LOG(INFO) << Substitute("Tablet server has $0 leaders and $1 scanners",
        resp->num_leaders(), resp->num_active_scanners());
  }
  resp->set_is_quiescing(server_->quiescing());
  context->RespondSuccess();
}

void TabletServiceAdminImpl::CreateTablet(const CreateTabletRequestPB* req,
                                          CreateTabletResponsePB* resp,
                                          rpc::RpcContext* context) {
  if (!CheckUuidMatchOrRespond(server_->tablet_manager(), "CreateTablet", req, resp, context)) {
    return;
  }
  TRACE_EVENT1("tserver", "CreateTablet",
               "tablet_id", req->tablet_id());

  Schema schema;
  Status s = SchemaFromPB(req->schema(), &schema);
  DCHECK(schema.has_column_ids());
  if (!s.ok()) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::InvalidArgument("Invalid Schema."),
                         TabletServerErrorPB::INVALID_SCHEMA, context);
    return;
  }

  PartitionSchema partition_schema;
  s = PartitionSchema::FromPB(req->partition_schema(), schema, &partition_schema);
  if (!s.ok()) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::InvalidArgument("Invalid PartitionSchema."),
                         TabletServerErrorPB::INVALID_SCHEMA, context);
    return;
  }

  Partition partition;
  Partition::FromPB(req->partition(), &partition);

  LOG(INFO) << "Processing CreateTablet for tablet " << req->tablet_id()
            << " (table=" << req->table_name()
            << " [id=" << req->table_id() << "]), partition="
            << partition_schema.PartitionDebugString(partition, schema);
  VLOG(1) << "Full request: " << SecureDebugString(*req);

  s = server_->tablet_manager()->CreateNewTablet(
      req->table_id(),
      req->tablet_id(),
      partition,
      req->table_name(),
      schema,
      partition_schema,
      req->config(),
      req->has_extra_config() ? boost::make_optional(req->extra_config()) : boost::none,
      req->has_dimension_label() ? boost::make_optional(req->dimension_label()) : boost::none,
      nullptr);
  if (PREDICT_FALSE(!s.ok())) {
    TabletServerErrorPB::Code code;
    if (s.IsAlreadyPresent()) {
      code = TabletServerErrorPB::TABLET_ALREADY_EXISTS;
    } else {
      code = TabletServerErrorPB::UNKNOWN_ERROR;
    }
    SetupErrorAndRespond(resp->mutable_error(), s, code, context);
    return;
  }
  context->RespondSuccess();
}

void TabletServiceAdminImpl::DeleteTablet(const DeleteTabletRequestPB* req,
                                          DeleteTabletResponsePB* resp,
                                          rpc::RpcContext* context) {
  if (!CheckUuidMatchOrRespond(server_->tablet_manager(), "DeleteTablet", req, resp, context)) {
    return;
  }
  TRACE_EVENT2("tserver", "DeleteTablet",
               "tablet_id", req->tablet_id(),
               "reason", req->reason());

  tablet::TabletDataState delete_type = tablet::TABLET_DATA_UNKNOWN;
  if (req->has_delete_type()) {
    delete_type = req->delete_type();
  }
  LOG(INFO) << "Processing DeleteTablet for tablet " << req->tablet_id()
            << " with delete_type " << TabletDataState_Name(delete_type)
            << (req->has_reason() ? (" (" + req->reason() + ")") : "")
            << " from " << context->requestor_string();
  VLOG(1) << "Full request: " << SecureDebugString(*req);

  boost::optional<int64_t> cas_config_opid_index_less_or_equal;
  if (req->has_cas_config_opid_index_less_or_equal()) {
    cas_config_opid_index_less_or_equal = req->cas_config_opid_index_less_or_equal();
  }

  auto response_callback = [context, req, resp](const Status& s, TabletServerErrorPB::Code code) {
    if (PREDICT_FALSE(!s.ok())) {
      HandleErrorResponse(req, resp, context, code, s);
      return;
    }
    context->RespondSuccess();
  };
  server_->tablet_manager()->DeleteTabletAsync(req->tablet_id(),
                                               delete_type,
                                               cas_config_opid_index_less_or_equal,
                                               response_callback);
}

void TabletServiceImpl::Write(const WriteRequestPB* req,
                              WriteResponsePB* resp,
                              rpc::RpcContext* context) {
  TRACE_EVENT1("tserver", "TabletServiceImpl::Write",
               "tablet_id", req->tablet_id());
  DVLOG(3) << "Received Write RPC: " << SecureDebugString(*req);
  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(server_->tablet_manager(), req->tablet_id(), resp,
                                           context, &replica)) {
    return;
  }
  boost::optional<WriteAuthorizationContext> authz_context;
  if (FLAGS_tserver_enforce_access_control) {
    TokenPB token;
    if (!VerifyAuthzTokenOrRespond(server_->token_verifier(), *req, context, &token)) {
      return;
    }
    const auto& privilege = token.authz().table_privilege();
    if (!CheckMatchingTableIdOrRespond(privilege, replica->tablet_metadata()->table_id(),
                                       "Write", context)) {
      return;
    }
    WritePrivileges privileges;
    if (privilege.insert_privilege()) {
      InsertOrDie(&privileges, WritePrivilegeType::INSERT);
    }
    if (privilege.update_privilege()) {
      InsertOrDie(&privileges, WritePrivilegeType::UPDATE);
    }
    if (privilege.delete_privilege()) {
      InsertOrDie(&privileges, WritePrivilegeType::DELETE);
    }
    if (privileges.empty()) {
      // If we know there are no write-related privileges outright, we can
      // short-circuit further checking and reject the request immediately.
      // Otherwise, we'll defer the checking to the prepare phase of the
      // transaction after decoding the operations.
      LOG(WARNING) << Substitute("rejecting Write request from $0: no write privileges",
                                 context->requestor_string());
      context->RespondRpcFailure(rpc::ErrorStatusPB::FATAL_UNAUTHORIZED,
          Status::NotAuthorized("not authorized to write"));
      return;
    }
    authz_context = { privileges, /*requested_op_types=*/{} };
  }

  shared_ptr<Tablet> tablet;
  TabletServerErrorPB::Code error_code;
  Status s = GetTabletRef(replica, &tablet, &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
    return;
  }

  uint64_t bytes = req->row_operations().rows().size() +
      req->row_operations().indirect_data().size();
  if (!tablet->ShouldThrottleAllow(bytes)) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::ServiceUnavailable("Rejecting Write request: throttled"),
                         TabletServerErrorPB::THROTTLED,
                         context);
    return;
  }

  // Check for memory pressure; don't bother doing any additional work if we've
  // exceeded the limit.
  double capacity_pct;
  if (process_memory::SoftLimitExceeded(&capacity_pct)) {
    tablet->metrics()->leader_memory_pressure_rejections->Increment();
    string msg = StringPrintf("Soft memory limit exceeded (at %.2f%% of capacity)", capacity_pct);
    if (capacity_pct >= FLAGS_memory_limit_warn_threshold_percentage) {
      KLOG_EVERY_N_SECS(WARNING, 1) << "Rejecting Write request: " << msg << THROTTLE_MSG;
    } else {
      KLOG_EVERY_N_SECS(INFO, 1) << "Rejecting Write request: " << msg << THROTTLE_MSG;
    }
    SetupErrorAndRespond(resp->mutable_error(), Status::ServiceUnavailable(msg),
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }

  if (!server_->clock()->SupportsExternalConsistencyMode(req->external_consistency_mode())) {
    Status s = Status::NotSupported("The configured clock does not support the"
        " required consistency mode.");
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }

  unique_ptr<WriteTransactionState> tx_state(new WriteTransactionState(
      replica.get(),
      req,
      context->AreResultsTracked() ? context->request_id() : nullptr,
      resp,
      std::move(authz_context)));

  // If the client sent us a timestamp, decode it and update the clock so that all future
  // timestamps are greater than the passed timestamp.
  if (req->has_propagated_timestamp()) {
    Timestamp ts(req->propagated_timestamp());
    s = server_->clock()->Update(ts);
  }
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }

  tx_state->set_completion_callback(unique_ptr<TransactionCompletionCallback>(
      new RpcTransactionCompletionCallback<WriteResponsePB>(context,
                                                            resp)));

  // Submit the write. The RPC will be responded to asynchronously.
  s = replica->SubmitWrite(std::move(tx_state));

  // Check that we could submit the write
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
  }
}

ConsensusServiceImpl::ConsensusServiceImpl(ServerBase* server,
                                           TabletReplicaLookupIf* tablet_manager)
    : ConsensusServiceIf(server->metric_entity(), server->result_tracker()),
      server_(server),
      tablet_manager_(tablet_manager) {
}

ConsensusServiceImpl::~ConsensusServiceImpl() {
}

bool ConsensusServiceImpl::AuthorizeServiceUser(const google::protobuf::Message* /*req*/,
                                                google::protobuf::Message* /*resp*/,
                                                rpc::RpcContext* rpc) {
  return server_->Authorize(rpc, ServerBase::SUPER_USER | ServerBase::SERVICE_USER);
}

void ConsensusServiceImpl::UpdateConsensus(const ConsensusRequestPB* req,
                                           ConsensusResponsePB* resp,
                                           rpc::RpcContext* context) {
  DVLOG(3) << "Received Consensus Update RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "UpdateConsensus", req, resp, context)) {
    return;
  }
  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(tablet_manager_, req->tablet_id(), resp, context,
                                           &replica)) {
    return;
  }

  // Submit the update directly to the TabletReplica's RaftConsensus instance.
  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(replica, resp, context, &consensus)) return;
  Status s = consensus->Update(req, resp);
  if (PREDICT_FALSE(!s.ok())) {
    // Clear the response first, since a partially-filled response could
    // result in confusing a caller, or in having missing required fields
    // in embedded optional messages.
    resp->Clear();

    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::RequestConsensusVote(const VoteRequestPB* req,
                                                VoteResponsePB* resp,
                                                rpc::RpcContext* context) {
  DVLOG(3) << "Received Consensus Request Vote RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "RequestConsensusVote", req, resp, context)) {
    return;
  }

  // Because the last-logged opid is stored in the TabletMetadata we go through
  // the following dance:
  // 1. Get a reference to the currently-registered TabletReplica.
  // 2. Fetch (non-atomically) the current data state and last-logged opid from
  //    the TabletMetadata.
  // 3. If the data state is COPYING or TOMBSTONED, pass the last-logged opid
  // from the TabletMetadata into RaftConsensus::RequestVote().
  //
  // The reason this sequence is safe to do without atomic locks is the
  // RaftConsensus object associated with the TabletReplica will be Shutdown()
  // and thus unable to vote if another TabletCopy operation comes between
  // steps 1 and 3.
  //
  // TODO(mpercy): Move the last-logged opid into ConsensusMetadata to avoid
  // this hacky plumbing. An additional benefit would be that we would be able
  // to easily "tombstoned vote" while the tablet is bootstrapping.

  scoped_refptr<TabletReplica> replica;
  if (!LookupTabletReplicaOrRespond(tablet_manager_, req->tablet_id(), resp, context, &replica)) {
    return;
  }

  boost::optional<OpId> last_logged_opid;
  tablet::TabletDataState data_state = replica->tablet_metadata()->tablet_data_state();

  LOG(INFO) << "Received RequestConsensusVote() RPC: " << SecureShortDebugString(*req);

  // We cannot vote while DELETED. This check is not racy because DELETED is a
  // terminal state; it is not possible to transition out of DELETED.
  if (data_state == TABLET_DATA_DELETED) {
    RespondTabletNotRunning(replica, replica->state(), resp, context);
    return;
  }

  // Attempt to vote while copying or tombstoned.
  if (data_state == TABLET_DATA_COPYING || data_state == TABLET_DATA_TOMBSTONED) {
    last_logged_opid = replica->tablet_metadata()->tombstone_last_logged_opid();
  }

  // Submit the vote request directly to the consensus instance.
  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(replica, resp, context, &consensus)) return;

  Status s = consensus->RequestVote(req,
                                    consensus::TabletVotingState(std::move(last_logged_opid),
                                                                 data_state),
                                    resp);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::ChangeConfig(const ChangeConfigRequestPB* req,
                                        ChangeConfigResponsePB* resp,
                                        RpcContext* context) {
  VLOG(1) << "Received ChangeConfig RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "ChangeConfig", req, resp, context)) {
    return;
  }
  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(tablet_manager_, req->tablet_id(), resp, context,
                                           &replica)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(replica, resp, context, &consensus)) return;
  boost::optional<TabletServerErrorPB::Code> error_code;
  Status s = consensus->ChangeConfig(
      *req, [req, resp, context](const Status& s) {
        HandleResponse(req, resp, context, s);
      },
      &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    HandleErrorResponse(req, resp, context, error_code, s);
    return;
  }
  // The success case is handled when the callback fires.
}

void ConsensusServiceImpl::BulkChangeConfig(const BulkChangeConfigRequestPB* req,
                                            ChangeConfigResponsePB* resp,
                                            RpcContext* context) {
  VLOG(1) << "Received BulkChangeConfig RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "BulkChangeConfig", req, resp, context)) {
    return;
  }
  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(tablet_manager_, req->tablet_id(), resp, context,
                                           &replica)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(replica, resp, context, &consensus)) return;
  boost::optional<TabletServerErrorPB::Code> error_code;
  Status s = consensus->BulkChangeConfig(
      *req, [req, resp, context](const Status& s) {
        HandleResponse(req, resp, context, s);
      },
      &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    HandleErrorResponse(req, resp, context, error_code, s);
    return;
  }
  // The success case is handled when the callback fires.
}

void ConsensusServiceImpl::UnsafeChangeConfig(const UnsafeChangeConfigRequestPB* req,
                                              UnsafeChangeConfigResponsePB* resp,
                                              RpcContext* context) {
  LOG(INFO) << "Received UnsafeChangeConfig RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!CheckUuidMatchOrRespond(tablet_manager_, "UnsafeChangeConfig", req, resp, context)) {
    return;
  }
  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(tablet_manager_, req->tablet_id(), resp, context,
                                           &replica)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(replica, resp, context, &consensus)) {
    return;
  }
  boost::optional<TabletServerErrorPB::Code> error_code;
  const Status s = consensus->UnsafeChangeConfig(*req, &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    HandleErrorResponse(req, resp, context, error_code, s);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::GetNodeInstance(const GetNodeInstanceRequestPB* req,
                                           GetNodeInstanceResponsePB* resp,
                                           rpc::RpcContext* context) {
  VLOG(1) << "Received Get Node Instance RPC: " << SecureDebugString(*req);
  resp->mutable_node_instance()->CopyFrom(tablet_manager_->NodeInstance());
  context->RespondSuccess();
}

void ConsensusServiceImpl::RunLeaderElection(const RunLeaderElectionRequestPB* req,
                                             RunLeaderElectionResponsePB* resp,
                                             rpc::RpcContext* context) {
  LOG(INFO) << "Received Run Leader Election RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (!CheckUuidMatchOrRespond(tablet_manager_, "RunLeaderElection", req, resp, context)) {
    return;
  }
  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(tablet_manager_, req->tablet_id(), resp, context,
                                           &replica)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(replica, resp, context, &consensus)) return;
  Status s = consensus->StartElection(
      consensus::RaftConsensus::ELECT_EVEN_IF_LEADER_IS_ALIVE,
      consensus::RaftConsensus::EXTERNAL_REQUEST);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }
  context->RespondSuccess();
}

void ConsensusServiceImpl::LeaderStepDown(const LeaderStepDownRequestPB* req,
                                          LeaderStepDownResponsePB* resp,
                                          RpcContext* context) {
  LOG(INFO) << "Received LeaderStepDown RPC: " << SecureDebugString(*req)
            << " from " << context->requestor_string();
  if (PREDICT_FALSE(!req->new_leader_uuid().empty() &&
                    req->mode() == LeaderStepDownMode::ABRUPT)) {
    Status s = Status::InvalidArgument(
        "cannot specify a new leader uuid for an abrupt step down");
    SetupErrorAndRespond(resp->mutable_error(), s,
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
  }
  if (!CheckUuidMatchOrRespond(tablet_manager_, "LeaderStepDown", req, resp, context)) {
    return;
  }
  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(tablet_manager_, req->tablet_id(), resp, context,
                                           &replica)) {
    return;
  }

  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(replica, resp, context, &consensus)) return;
  switch (req->mode()) {
    case LeaderStepDownMode::ABRUPT:
      HandleResponse(req, resp, context, consensus->StepDown(resp));
      break;
    case LeaderStepDownMode::GRACEFUL: {
      const auto new_leader_uuid =
        req->new_leader_uuid().empty() ?
        boost::none :
        boost::make_optional(req->new_leader_uuid());
      Status s = consensus->TransferLeadership(new_leader_uuid, resp);
      HandleResponse(req, resp, context, s);
      break;
    }
    default:
      Status s = Status::InvalidArgument(
          Substitute("unknown LeaderStepDown mode: $0", req->mode()));
      HandleUnknownError(s, resp, context);
  }
}

void ConsensusServiceImpl::GetLastOpId(const consensus::GetLastOpIdRequestPB *req,
                                       consensus::GetLastOpIdResponsePB *resp,
                                       rpc::RpcContext *context) {
  DVLOG(3) << "Received GetLastOpId RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "GetLastOpId", req, resp, context)) {
    return;
  }
  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(tablet_manager_, req->tablet_id(), resp, context,
                                           &replica)) {
    return;
  }

  if (replica->state() != tablet::RUNNING) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::ServiceUnavailable("TabletReplica not in RUNNING state"),
                         TabletServerErrorPB::TABLET_NOT_RUNNING, context);
    return;
  }
  shared_ptr<RaftConsensus> consensus;
  if (!GetConsensusOrRespond(replica, resp, context, &consensus)) return;
  if (PREDICT_FALSE(req->opid_type() == consensus::UNKNOWN_OPID_TYPE)) {
    HandleUnknownError(Status::InvalidArgument("Invalid opid_type specified to GetLastOpId()"),
                       resp, context);
    return;
  }
  boost::optional<OpId> opid = consensus->GetLastOpId(req->opid_type());
  if (!opid) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::IllegalState("Cannot fetch last OpId in WAL"),
                         TabletServerErrorPB::TABLET_NOT_RUNNING,
                         context);
    return;
  }
  *resp->mutable_opid() = *opid;
  context->RespondSuccess();
}

void ConsensusServiceImpl::GetConsensusState(const consensus::GetConsensusStateRequestPB* req,
                                             consensus::GetConsensusStateResponsePB* resp,
                                             rpc::RpcContext* context) {
  DVLOG(3) << "Received GetConsensusState RPC: " << SecureDebugString(*req);
  if (!CheckUuidMatchOrRespond(tablet_manager_, "GetConsensusState", req, resp, context)) {
    return;
  }

  unordered_set<string> requested_ids(req->tablet_ids().begin(), req->tablet_ids().end());
  bool all_ids = requested_ids.empty();

  vector<scoped_refptr<TabletReplica>> tablet_replicas;
  tablet_manager_->GetTabletReplicas(&tablet_replicas);
  for (const scoped_refptr<TabletReplica>& replica : tablet_replicas) {
    if (!all_ids && !ContainsKey(requested_ids, replica->tablet_id())) {
      continue;
    }

    shared_ptr<RaftConsensus> consensus(replica->shared_consensus());
    if (!consensus) {
      continue;
    }

    consensus::GetConsensusStateResponsePB_TabletConsensusInfoPB tablet_info;
    Status s = consensus->ConsensusState(tablet_info.mutable_cstate(), req->report_health());
    if (!s.ok()) {
      DCHECK(s.IsIllegalState()) << s.ToString();
      continue;
    }
    tablet_info.set_tablet_id(replica->tablet_id());
    *resp->add_tablets() = std::move(tablet_info);
  }
  const auto scheme = FLAGS_raft_prepare_replacement_before_eviction
      ? consensus::ReplicaManagementInfoPB::PREPARE_REPLACEMENT_BEFORE_EVICTION
      : consensus::ReplicaManagementInfoPB::EVICT_FIRST;
  resp->mutable_replica_management_info()->set_replacement_scheme(scheme);

  context->RespondSuccess();
}

void ConsensusServiceImpl::StartTabletCopy(const StartTabletCopyRequestPB* req,
                                           StartTabletCopyResponsePB* resp,
                                           rpc::RpcContext* context) {
  if (!CheckUuidMatchOrRespond(tablet_manager_, "StartTabletCopy", req, resp, context)) {
    return;
  }
  auto response_callback = [context, resp](const Status& s, TabletServerErrorPB::Code code) {
    if (s.ok()) {
      context->RespondSuccess();
    } else {
      // Skip calling SetupErrorAndRespond since this path doesn't need the
      // error to be transformed.
      StatusToPB(s, resp->mutable_error()->mutable_status());
      resp->mutable_error()->set_code(code);
      context->RespondNoCache();
    }
  };
  tablet_manager_->StartTabletCopy(req, response_callback);
}

void TabletServiceImpl::ScannerKeepAlive(const ScannerKeepAliveRequestPB *req,
                                         ScannerKeepAliveResponsePB *resp,
                                         rpc::RpcContext *context) {
  DCHECK(req->has_scanner_id());
  SharedScanner scanner;
  TabletServerErrorPB::Code error_code = TabletServerErrorPB::UNKNOWN_ERROR;
  Status s = server_->scanner_manager()->LookupScanner(req->scanner_id(),
                                                       context->remote_user().username(),
                                                       &error_code,
                                                       &scanner);
  if (!s.ok()) {
    StatusToPB(s, resp->mutable_error()->mutable_status());
    LOG(INFO) << Substitute("ScannerKeepAlive: $0: remote=$1",
                            s.ToString(), context->requestor_string());
    if (PREDICT_TRUE(s.IsNotFound())) {
      resp->mutable_error()->set_code(error_code);
      StatusToPB(s, resp->mutable_error()->mutable_status());
      context->RespondSuccess();
      return;
    }
    DCHECK(s.IsNotAuthorized());
    SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
    return;
  }
  scanner->UpdateAccessTime();
  context->RespondSuccess();
}

namespace {
void SetResourceMetrics(const rpc::RpcContext* context,
                        const CpuTimes* cpu_times,
                        ResourceMetricsPB* metrics) {
  metrics->set_cfile_cache_miss_bytes(
    context->trace()->metrics()->GetMetric(cfile::CFILE_CACHE_MISS_BYTES_METRIC_NAME));
  metrics->set_cfile_cache_hit_bytes(
    context->trace()->metrics()->GetMetric(cfile::CFILE_CACHE_HIT_BYTES_METRIC_NAME));

  metrics->set_bytes_read(
    context->trace()->metrics()->GetMetric(SCANNER_BYTES_READ_METRIC_NAME));

  rpc::InboundCallTiming timing;
  timing.time_handled = context->GetTimeHandled();
  timing.time_received = context->GetTimeReceived();
  timing.time_completed = MonoTime::Now();

  metrics->set_queue_duration_nanos(timing.QueueDuration().ToNanoseconds());
  metrics->set_total_duration_nanos(timing.TotalDuration().ToNanoseconds());
  metrics->set_cpu_system_nanos(cpu_times->system);
  metrics->set_cpu_user_nanos(cpu_times->user);
}
} // anonymous namespace

void TabletServiceImpl::Scan(const ScanRequestPB* req,
                             ScanResponsePB* resp,
                             rpc::RpcContext* context) {
  TRACE_EVENT0("tserver", "TabletServiceImpl::Scan");

  // Validate the request: user must pass a new_scan_request or
  // a scanner ID, but not both.
  if (PREDICT_FALSE(req->has_scanner_id() &&
                    req->has_new_scan_request())) {
    context->RespondFailure(Status::InvalidArgument(
                            "Must not pass both a scanner_id and new_scan_request"));
    return;
  }

  // If this is a new scan request, we must enforce the appropriate privileges.
  TokenPB token;
  if (FLAGS_tserver_enforce_access_control && req->has_new_scan_request()) {
    const auto& scan_pb = req->new_scan_request();
    if (!VerifyAuthzTokenOrRespond(server_->token_verifier(),
                                   req->new_scan_request(), context, &token)) {
      return;
    }
    scoped_refptr<TabletReplica> replica;
    if (!LookupRunningTabletReplicaOrRespond(server_->tablet_manager(),
        req->new_scan_request().tablet_id(), resp, context, &replica)) {
      return;
    }
    const auto& privilege = token.authz().table_privilege();
    if (!CheckMatchingTableIdOrRespond(privilege, replica->tablet_metadata()->table_id(),
                                       "Scan", context)) {
      return;
    }
    unordered_set<ColumnId> authorized_column_ids;
    if (!CheckMayHaveScanPrivilegesOrRespond(privilege, "Scan", &authorized_column_ids, context)) {
      return;
    }
    // If the token doesn't have full scan privileges for the table, check
    // for required privileges based on the scan request.
    if (!privilege.scan_privilege()) {
      const auto& schema = replica->tablet_metadata()->schema();
      if (!CheckScanPrivilegesOrRespond(scan_pb, schema, authorized_column_ids,
                                        "Scan", context)) {
        return;
      }
    }
  }

  size_t batch_size_bytes = GetMaxBatchSizeBytesHint(req);
  ScanResultCopier collector(batch_size_bytes);

  bool has_more_results = false;
  TabletServerErrorPB::Code error_code = TabletServerErrorPB::UNKNOWN_ERROR;
  if (req->has_new_scan_request()) {
    if (!CheckTabletServerNotQuiescingOrRespond(server_, resp, context)) {
      return;
    }
    const NewScanRequestPB& scan_pb = req->new_scan_request();
    scoped_refptr<TabletReplica> replica;
    if (!LookupRunningTabletReplicaOrRespond(server_->tablet_manager(), scan_pb.tablet_id(), resp,
                                             context, &replica)) {
      return;
    }
    string scanner_id;
    Timestamp scan_timestamp;
    Status s = HandleNewScanRequest(replica.get(), req, context,
                                    &collector, &scanner_id, &scan_timestamp, &has_more_results,
                                    &error_code);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
      return;
    }

    // Only set the scanner id if we have more results.
    if (has_more_results) {
      resp->set_scanner_id(scanner_id);
    }
    if (scan_timestamp != Timestamp::kInvalidTimestamp) {
      resp->set_snap_timestamp(scan_timestamp.ToUint64());
    }
  } else if (req->has_scanner_id()) {
    Status s = HandleContinueScanRequest(req, context, &collector, &has_more_results, &error_code);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
      return;
    }
  } else {
    context->RespondFailure(Status::InvalidArgument(
                              "Must pass either a scanner_id or new_scan_request"));
    return;
  }

  collector.SetupResponse(context, resp);
  resp->set_has_more_results(has_more_results);
  resp->set_propagated_timestamp(server_->clock()->Now().ToUint64());

  SetResourceMetrics(context, collector.cpu_times(), resp->mutable_resource_metrics());
  context->RespondSuccess();
}

void TabletServiceImpl::ListTablets(const ListTabletsRequestPB* req,
                                    ListTabletsResponsePB* resp,
                                    rpc::RpcContext* context) {
  vector<scoped_refptr<TabletReplica>> replicas;
  server_->tablet_manager()->GetTabletReplicas(&replicas);
  RepeatedPtrField<StatusAndSchemaPB>* replica_status = resp->mutable_status_and_schema();
  for (const scoped_refptr<TabletReplica>& replica : replicas) {
    StatusAndSchemaPB* status = replica_status->Add();
    replica->GetTabletStatusPB(status->mutable_tablet_status());

    if (req->need_schema_info()) {
      CHECK_OK(SchemaToPB(replica->tablet_metadata()->schema(),
                          status->mutable_schema()));
      replica->tablet_metadata()->partition_schema().ToPB(status->mutable_partition_schema());
    }
  }
  context->RespondSuccess();
}

void TabletServiceImpl::SplitKeyRange(const SplitKeyRangeRequestPB* req,
                                      SplitKeyRangeResponsePB* resp,
                                      rpc::RpcContext* context) {
  TRACE_EVENT1("tserver", "TabletServiceImpl::SplitKeyRange",
               "tablet_id", req->tablet_id());
  DVLOG(3) << "Received SplitKeyRange RPC: " << SecureDebugString(*req);

  scoped_refptr<TabletReplica> replica;
  if (!LookupRunningTabletReplicaOrRespond(server_->tablet_manager(), req->tablet_id(), resp,
                                           context, &replica)) {
    return;
  }

  if (FLAGS_tserver_enforce_access_control) {
    TokenPB token;
    if (!VerifyAuthzTokenOrRespond(server_->token_verifier(), *req, context, &token)) {
      return;
    }
    const auto& privilege = token.authz().table_privilege();
    if (!CheckMatchingTableIdOrRespond(privilege, replica->tablet_metadata()->table_id(),
                                       "SplitKeyRange", context)) {
      return;
    }
    // Split-key requests require:
    //   if uses pk (e.g. primary key fields set):
    //     foreach(primary key column): SCAN ON COLUMN
    //   foreach(requested column): SCAN ON COLUMN
    //
    // If the privilege doesn't have full scan privileges, or column-level scan
    // privileges, the user is definitely not authorized to perform a scan.
    unordered_set<ColumnId> authorized_column_ids;
    if (!CheckMayHaveScanPrivilegesOrRespond(privilege, "SplitKeyRange",
                                             &authorized_column_ids, context)) {
      return;
    }
    if (!privilege.scan_privilege()) {
      const auto& schema = replica->tablet_metadata()->schema();
      unordered_set<ColumnId> required_column_privileges;
      if (req->has_start_primary_key() || req->has_stop_primary_key()) {
        const auto& key_cols = schema.get_key_column_ids();
        required_column_privileges.insert(key_cols.begin(), key_cols.end());
      }
      bool is_authorized = true;
      const string rejection_prefix = Substitute("rejecting SplitKeyRange request from $0",
                                                 context->requestor_string());
      for (int i = 0; i < req->columns_size(); i++) {
        const auto& column_name = req->columns(i).name();
        int col_idx = schema.find_column(req->columns(i).name());
        if (col_idx == Schema::kColumnNotFound) {
          LOG(WARNING) << Substitute("$0: no column named '$1'", rejection_prefix, column_name);
          is_authorized = false;
          break;
        }
        EmplaceIfNotPresent(&required_column_privileges, schema.column_id(col_idx));
      }
      for (const auto& required_col_id : required_column_privileges) {
        if (!ContainsKey(authorized_column_ids, required_col_id)) {
          LOG(WARNING) << Substitute("$0: authz token doesn't authorize column ID $1",
                                     rejection_prefix, required_col_id);
          is_authorized = false;
          break;
        }
      }
      if (!is_authorized) {
        context->RespondRpcFailure(rpc::ErrorStatusPB::FATAL_UNAUTHORIZED,
            Status::NotAuthorized("not authorized to SplitKeyRange"));
        return;
      }
    }
  }

  shared_ptr<Tablet> tablet;
  TabletServerErrorPB::Code error_code;
  Status s = GetTabletRef(replica, &tablet, &error_code);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
    return;
  }

  // Decode encoded key
  Arena arena(256);
  Schema tablet_schema = replica->tablet_metadata()->schema();
  unique_ptr<EncodedKey> start;
  unique_ptr<EncodedKey> stop;
  if (req->has_start_primary_key()) {
    s = EncodedKey::DecodeEncodedString(tablet_schema, &arena, req->start_primary_key(), &start);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(),
                           Status::InvalidArgument("Invalid SplitKeyRange start primary key"),
                           TabletServerErrorPB::UNKNOWN_ERROR,
                           context);
      return;
    }
  }
  if (req->has_stop_primary_key()) {
    s = EncodedKey::DecodeEncodedString(tablet_schema, &arena, req->stop_primary_key(), &stop);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(),
                           Status::InvalidArgument("Invalid SplitKeyRange stop primary key"),
                           TabletServerErrorPB::UNKNOWN_ERROR,
                           context);
      return;
    }
  }
  if (req->has_start_primary_key() && req->has_stop_primary_key()) {
    // Validate the start key is less than the stop key, if they are both set
    if (start->encoded_key().compare(stop->encoded_key()) > 0) {
      SetupErrorAndRespond(resp->mutable_error(),
                           Status::InvalidArgument("Invalid primary key range"),
                           TabletServerErrorPB::UNKNOWN_ERROR,
                           context);
      return;
    }
  }

  // Validate the column are valid
  Schema schema;
  s = ColumnPBsToSchema(req->columns(), &schema);
  if (PREDICT_FALSE(!s.ok())) {
    SetupErrorAndRespond(resp->mutable_error(),
                         s,
                         TabletServerErrorPB::INVALID_SCHEMA,
                         context);
    return;
  }
  if (schema.has_column_ids()) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::InvalidArgument("User requests should not have Column IDs"),
                         TabletServerErrorPB::INVALID_SCHEMA,
                         context);
    return;
  }

  vector<ColumnId> column_ids;
  for (const ColumnSchema& column : schema.columns()) {
    int column_idx = tablet_schema.find_column(column.name());
    if (PREDICT_FALSE(column_idx == Schema::kColumnNotFound)) {
      SetupErrorAndRespond(resp->mutable_error(),
                           Status::InvalidArgument(
                               "Invalid SplitKeyRange column name", column.name()),
                           TabletServerErrorPB::INVALID_SCHEMA,
                           context);
      return;
    }
    column_ids.emplace_back(tablet_schema.column_id(column_idx));
  }

  // Validate the target chunk size are valid
  if (req->target_chunk_size_bytes() == 0) {
    SetupErrorAndRespond(resp->mutable_error(),
                         Status::InvalidArgument("Invalid SplitKeyRange target chunk size"),
                         TabletServerErrorPB::UNKNOWN_ERROR,
                         context);
    return;
  }

  vector<KeyRange> ranges;
  tablet->SplitKeyRange(start.get(), stop.get(), column_ids,
                        req->target_chunk_size_bytes(), &ranges);
  for (auto range : ranges) {
    range.ToPB(resp->add_ranges());
  }

  context->RespondSuccess();
}

void TabletServiceImpl::Checksum(const ChecksumRequestPB* req,
                                 ChecksumResponsePB* resp,
                                 rpc::RpcContext* context) {
  VLOG(1) << "Full request: " << SecureDebugString(*req);

  // Validate the request: user must pass a new_scan_request or
  // a scanner ID, but not both.
  if (PREDICT_FALSE(req->has_new_request() &&
                    req->has_continue_request())) {
    context->RespondFailure(Status::InvalidArgument(
                            "Must not pass both a scanner_id and new_scan_request"));
    return;
  }

  // Convert ChecksumRequestPB to a ScanRequestPB.
  ScanRequestPB scan_req;
  if (req->has_call_seq_id()) scan_req.set_call_seq_id(req->call_seq_id());
  if (req->has_batch_size_bytes()) scan_req.set_batch_size_bytes(req->batch_size_bytes());
  if (req->has_close_scanner()) scan_req.set_close_scanner(req->close_scanner());

  ScanResultChecksummer collector;
  bool has_more = false;
  TabletServerErrorPB::Code error_code = TabletServerErrorPB::UNKNOWN_ERROR;
  // TODO(KUDU-2870): the CLI tool doesn't currently fetch authz tokens when
  // checksumming. Until it does, allow the super-user to avoid fine-grained
  // privilege checking.
  if (FLAGS_tserver_enforce_access_control &&
      !server_->IsFromSuperUser(context) &&
      req->has_new_request()) {
    const NewScanRequestPB& new_req = req->new_request();
    TokenPB token;
    if (!VerifyAuthzTokenOrRespond(server_->token_verifier(), req->new_request(),
                                    context, &token)) {
      return;
    }
    scoped_refptr<TabletReplica> replica;
    if (!LookupRunningTabletReplicaOrRespond(server_->tablet_manager(), new_req.tablet_id(), resp,
                                             context, &replica)) {
      return;
    }
    const auto& privilege = token.authz().table_privilege();
    if (!CheckMatchingTableIdOrRespond(privilege, replica->tablet_metadata()->table_id(),
                                       "Checksum", context)) {
      return;
    }
    unordered_set<ColumnId> authorized_column_ids;
    if (!CheckMayHaveScanPrivilegesOrRespond(privilege, "Checksum",
                                             &authorized_column_ids, context)) {
      return;
    }
    // If the token doesn't have full scan privileges for the table, check
    // for required privileges based on the checksum request.
    if (!privilege.scan_privilege()) {
      const auto& schema = replica->tablet_metadata()->schema();
      if (!CheckScanPrivilegesOrRespond(new_req, schema, authorized_column_ids,
                                        "Checksum", context)) {
        return;
      }
    }
  }
  if (req->has_new_request()) {
    if (!CheckTabletServerNotQuiescingOrRespond(server_, resp, context)) {
      return;
    }
    const NewScanRequestPB& new_req = req->new_request();
    scan_req.mutable_new_scan_request()->CopyFrom(req->new_request());
    scoped_refptr<TabletReplica> replica;
    if (!LookupRunningTabletReplicaOrRespond(server_->tablet_manager(), new_req.tablet_id(), resp,
                                             context, &replica)) {
      return;
    }

    string scanner_id;
    Timestamp snap_timestamp;
    Status s = HandleNewScanRequest(replica.get(), &scan_req, context,
                                    &collector, &scanner_id, &snap_timestamp, &has_more,
                                    &error_code);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
      return;
    }
    resp->set_scanner_id(scanner_id);
    if (snap_timestamp != Timestamp::kInvalidTimestamp) {
      resp->set_snap_timestamp(snap_timestamp.ToUint64());
    }
  } else if (req->has_continue_request()) {
    const ContinueChecksumRequestPB& continue_req = req->continue_request();
    collector.set_agg_checksum(continue_req.previous_checksum());
    scan_req.set_scanner_id(continue_req.scanner_id());
    Status s = HandleContinueScanRequest(&scan_req, context, &collector, &has_more, &error_code);
    if (PREDICT_FALSE(!s.ok())) {
      SetupErrorAndRespond(resp->mutable_error(), s, error_code, context);
      return;
    }
  } else {
    context->RespondFailure(Status::InvalidArgument(
                            "Must pass either new_request or continue_request"));
    return;
  }

  resp->set_checksum(collector.agg_checksum());
  resp->set_has_more_results(has_more);
  resp->set_rows_checksummed(collector.rows_checksummed());

  SetResourceMetrics(context, collector.cpu_times(), resp->mutable_resource_metrics());
  context->RespondSuccess();
}

bool TabletServiceImpl::SupportsFeature(uint32_t feature) const {
  switch (feature) {
    case TabletServerFeatures::COLUMN_PREDICATES:
    case TabletServerFeatures::PAD_UNIXTIME_MICROS_TO_16_BYTES:
    case TabletServerFeatures::QUIESCING:
    case TabletServerFeatures::BLOOM_FILTER_PREDICATE:
    case TabletServerFeatures::COLUMNAR_LAYOUT_FEATURE:
      return true;
    default:
      return false;
  }
}

void TabletServiceImpl::Shutdown() {
}

// Extract a void* pointer suitable for use in a ColumnRangePredicate from the
// user-specified protobuf field.
// This validates that the pb_value has the correct length, copies the data into
// 'arena', and sets *result to point to it.
// Returns bad status if the user-specified value is the wrong length.
static Status ExtractPredicateValue(const ColumnSchema& schema,
                                    const string& pb_value,
                                    Arena* arena,
                                    const void** result) {
  // Copy the data from the protobuf into the Arena.
  uint8_t* data_copy = static_cast<uint8_t*>(arena->AllocateBytes(pb_value.size()));
  memcpy(data_copy, &pb_value[0], pb_value.size());

  // If the type is of variable length, then we need to return a pointer to a Slice
  // element pointing to the string. Otherwise, just verify that the provided
  // value was the right size.
  if (schema.type_info()->physical_type() == BINARY) {
    *result = arena->NewObject<Slice>(data_copy, pb_value.size());
  } else {
    // TODO: add test case for this invalid request
    size_t expected_size = schema.type_info()->size();
    if (pb_value.size() != expected_size) {
      return Status::InvalidArgument(
        StringPrintf("Bad predicate on %s. Expected value size %zd, got %zd",
                     schema.ToString().c_str(), expected_size, pb_value.size()));
    }
    *result = data_copy;
  }

  return Status::OK();
}

static Status DecodeEncodedKeyRange(const NewScanRequestPB& scan_pb,
                                    const Schema& tablet_schema,
                                    const SharedScanner& scanner,
                                    ScanSpec* spec) {
  unique_ptr<EncodedKey> start;
  unique_ptr<EncodedKey> stop;
  if (scan_pb.has_start_primary_key()) {
    RETURN_NOT_OK_PREPEND(EncodedKey::DecodeEncodedString(
                            tablet_schema, scanner->arena(),
                            scan_pb.start_primary_key(), &start),
                          "Invalid scan start key");
  }

  if (scan_pb.has_stop_primary_key()) {
    RETURN_NOT_OK_PREPEND(EncodedKey::DecodeEncodedString(
                            tablet_schema, scanner->arena(),
                            scan_pb.stop_primary_key(), &stop),
                          "Invalid scan stop key");
  }

  if (scan_pb.order_mode() == ORDERED && scan_pb.has_last_primary_key()) {
    if (start) {
      return Status::InvalidArgument("Cannot specify both a start key and a last key");
    }
    // Set the start key to the last key from a previous scan result.
    RETURN_NOT_OK_PREPEND(EncodedKey::DecodeEncodedString(tablet_schema, scanner->arena(),
                                                          scan_pb.last_primary_key(), &start),
                          "Failed to decode last primary key");
    // Increment the start key, so we don't return the last row again.
    RETURN_NOT_OK_PREPEND(EncodedKey::IncrementEncodedKey(tablet_schema, &start, scanner->arena()),
                          "Failed to increment encoded last row key");
  }

  if (start) {
    spec->SetLowerBoundKey(start.get());
    scanner->autorelease_pool()->Add(start.release());
  }
  if (stop) {
    spec->SetExclusiveUpperBoundKey(stop.get());
    scanner->autorelease_pool()->Add(stop.release());
  }

  return Status::OK();
}

static Status SetupScanSpec(const NewScanRequestPB& scan_pb,
                            const Schema& tablet_schema,
                            const SharedScanner& scanner,
                            ScanSpec* spec) {
  spec->set_cache_blocks(scan_pb.cache_blocks());

  // First the column predicates.
  for (const ColumnPredicatePB& pred_pb : scan_pb.column_predicates()) {
    boost::optional<ColumnPredicate> predicate;
    RETURN_NOT_OK(ColumnPredicateFromPB(tablet_schema, scanner->arena(), pred_pb, &predicate));
    spec->AddPredicate(std::move(*predicate));
  }

  // Then the column range predicates.
  // TODO: remove this once all clients have moved to ColumnPredicatePB and
  // backwards compatibility can be broken.
  for (const ColumnRangePredicatePB& pred_pb : scan_pb.deprecated_range_predicates()) {
    if (!pred_pb.has_lower_bound() && !pred_pb.has_inclusive_upper_bound()) {
      return Status::InvalidArgument(
        string("Invalid predicate ") + SecureShortDebugString(pred_pb) +
        ": has no lower or upper bound.");
    }
    boost::optional<ColumnSchema> col;
    RETURN_NOT_OK(ColumnSchemaFromPB(pred_pb.column(), &col));

    const void* lower_bound = nullptr;
    const void* upper_bound = nullptr;
    if (pred_pb.has_lower_bound()) {
      const void* val;
      RETURN_NOT_OK(ExtractPredicateValue(*col, pred_pb.lower_bound(),
                                          scanner->arena(),
                                          &val));
      lower_bound = val;
    }
    if (pred_pb.has_inclusive_upper_bound()) {
      const void* val;
      RETURN_NOT_OK(ExtractPredicateValue(*col, pred_pb.inclusive_upper_bound(),
                                          scanner->arena(),
                                          &val));
      upper_bound = val;
    }

    auto pred = ColumnPredicate::InclusiveRange(*col, lower_bound, upper_bound, scanner->arena());
    if (pred) {
      VLOG(3) << Substitute("Parsed predicate $0 from $1",
                            pred->ToString(), SecureShortDebugString(scan_pb));
      spec->AddPredicate(*pred);
    }
  }

  // Then any encoded key range predicates.
  RETURN_NOT_OK(DecodeEncodedKeyRange(scan_pb, tablet_schema, scanner, spec));

  // If the scanner has a limit, set it now.
  if (scan_pb.has_limit()) {
    spec->set_limit(scan_pb.limit());
  }

  return Status::OK();
}

namespace {
// Checks if 'timestamp' is before the tablet's AHM if this is a
// READ_AT_SNAPSHOT/READ_YOUR_WRITES scan. Returns Status::OK() if it's
// not or Status::InvalidArgument() if it is.
Status VerifyNotAncientHistory(Tablet* tablet, ReadMode read_mode, Timestamp timestamp,
                               const string& timestamp_desc) {
  tablet::HistoryGcOpts history_gc_opts = tablet->GetHistoryGcOpts();
  if ((read_mode == READ_AT_SNAPSHOT || read_mode == READ_YOUR_WRITES) &&
      history_gc_opts.IsAncientHistory(timestamp)) {
    return Status::InvalidArgument(
        Substitute("$0 is earlier than the ancient history mark. Consider "
                   "increasing the value of the configuration parameter "
                   "--tablet_history_max_age_sec. Snapshot timestamp: $1 "
                   "Ancient History Mark: $2 Physical time difference: $3",
                   timestamp_desc,
                   tablet->clock()->Stringify(timestamp),
                   tablet->clock()->Stringify(history_gc_opts.ancient_history_mark()),
                   tablet->clock()->GetPhysicalComponentDifference(
                       timestamp, history_gc_opts.ancient_history_mark()).ToString()));
  }
  return Status::OK();
}

// Verify that the start (if specified) and end snapshot timestamps are legal
// to read by checking against the ancient history mark and ensuring that the
// start timestamp is earlier than the end timestamp.
Status VerifyLegalSnapshotTimestamps(Tablet* tablet, ReadMode read_mode,
                                     const boost::optional<Timestamp>& snap_start_timestamp,
                                     const Timestamp& snap_end_timestamp) {
  RETURN_NOT_OK(VerifyNotAncientHistory(tablet, read_mode, snap_end_timestamp,
                                        "snapshot scan end timestamp"));
  if (snap_start_timestamp) {
    // Validate diff scan start timestamp, if set.
    RETURN_NOT_OK(VerifyNotAncientHistory(tablet, read_mode, *snap_start_timestamp,
                                          "snapshot scan start timestamp"));
    if (snap_start_timestamp->CompareTo(snap_end_timestamp) > 0) {
      return Status::InvalidArgument(
          Substitute("start timestamp ($0) must be less than or equal to end timestamp ($1)",
                     tablet->clock()->Stringify(*snap_start_timestamp),
                     tablet->clock()->Stringify(snap_end_timestamp)));
    }
  }
  return Status::OK();
}
} // anonymous namespace

// Start a new scan.
Status TabletServiceImpl::HandleNewScanRequest(TabletReplica* replica,
                                               const ScanRequestPB* req,
                                               const RpcContext* rpc_context,
                                               ScanResultCollector* result_collector,
                                               std::string* scanner_id,
                                               Timestamp* snap_timestamp,
                                               bool* has_more_results,
                                               TabletServerErrorPB::Code* error_code) {
  DCHECK(result_collector != nullptr);
  DCHECK(error_code != nullptr);
  DCHECK(req->has_new_scan_request());
  const NewScanRequestPB& scan_pb = req->new_scan_request();
  TRACE_EVENT1("tserver", "TabletServiceImpl::HandleNewScanRequest",
               "tablet_id", scan_pb.tablet_id());

  Status s = result_collector->InitSerializer(scan_pb.row_format_flags());
  if (!s.ok()) {
    *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
    return s;
  }

  const Schema& tablet_schema = replica->tablet_metadata()->schema();

  SharedScanner scanner;
  server_->scanner_manager()->NewScanner(replica,
                                         rpc_context->remote_user(),
                                         scan_pb.row_format_flags(),
                                         &scanner);
  TRACE("Created scanner $0 for tablet $1", scanner->id(), scanner->tablet_id());

  // If we early-exit out of this function, automatically unregister
  // the scanner.
  ScopedUnregisterScanner unreg_scanner(server_->scanner_manager(), scanner->id());
  ScopedAddScannerTiming scanner_timer(scanner.get(), result_collector->cpu_times());

  // Create the user's requested projection.
  // TODO(todd): Add test cases for bad projections including 0 columns.
  Schema projection;
  s = ColumnPBsToSchema(scan_pb.projected_columns(), &projection);
  if (PREDICT_FALSE(!s.ok())) {
    *error_code = TabletServerErrorPB::INVALID_SCHEMA;
    return s;
  }

  if (projection.has_column_ids()) {
    *error_code = TabletServerErrorPB::INVALID_SCHEMA;
    return Status::InvalidArgument("User requests should not have Column IDs");
  }

  if (scan_pb.order_mode() == UNKNOWN_ORDER_MODE) {
    *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
    return Status::InvalidArgument("Unknown order mode specified");
  }

  if (scan_pb.order_mode() == ORDERED) {
    // Ordered scans must be at a snapshot so that we perform a serializable read (which can be
    // resumed). Otherwise, this would be read committed isolation, which is not resumable.
    if (scan_pb.read_mode() != READ_AT_SNAPSHOT) {
      *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
          return Status::InvalidArgument("Cannot do an ordered scan that is not a snapshot read");
    }
  }

  ScanSpec spec;
  s = SetupScanSpec(scan_pb, tablet_schema, scanner, &spec);
  if (PREDICT_FALSE(!s.ok())) {
    *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
    return s;
  }

  VLOG(3) << "Before optimizing scan spec: " << spec.ToString(tablet_schema);
  spec.OptimizeScan(tablet_schema, scanner->arena(), scanner->autorelease_pool(), true);
  VLOG(3) << "After optimizing scan spec: " << spec.ToString(tablet_schema);

  // Missing columns will contain the columns that are not mentioned in the
  // client projection but are actually needed for the scan, such as columns
  // referred to by predicates.
  //
  // NOTE: We should build the missing column after optimizing scan which will
  // remove unnecessary predicates.
  vector<ColumnSchema> missing_cols = spec.GetMissingColumns(projection);
  if (spec.CanShortCircuit()) {
    VLOG(1) << "short-circuiting without creating a server-side scanner.";
    *has_more_results = false;
    return Status::OK();
  }

  // Store the original projection.
  {
    unique_ptr<Schema> orig_projection(new Schema(projection));
    scanner->set_client_projection_schema(std::move(orig_projection));
  }

  // Build a new projection with the projection columns and the missing columns,
  // annotating each column as a key column appropriately.
  //
  // Note: the projection is a consistent schema (i.e. no duplicate columns).
  // However, it has some different semantics as compared to the tablet schema:
  // - It might not contain all of the columns in the tablet.
  // - It might contain extra columns not found in the tablet schema. Virtual
  //   columns are permitted, while others will cause the scan to fail later,
  //   when the tablet validates the projection.
  // - It doesn't know which of its columns are key columns. That's fine for
  //   an UNORDERED scan, but we'll need to fix this for an ORDERED scan, which
  //   requires all key columns in tablet schema order.
  SchemaBuilder projection_builder;
  if (scan_pb.order_mode() == ORDERED) {
    for (int i = 0; i < tablet_schema.num_key_columns(); i++) {
      const auto& col = tablet_schema.column(i);
      // CHECK_OK is safe because the tablet schema has no duplicate columns.
      CHECK_OK(projection_builder.AddColumn(col, /* is_key= */ true));
    }
    for (int i = 0; i < projection.num_columns(); i++) {
      const auto& col = projection.column(i);
      // Any key columns in the projection will be ignored.
      ignore_result(projection_builder.AddColumn(col, /* is_key= */ false));
    }
    for (const ColumnSchema& col : missing_cols) {
      // Any key columns in 'missing_cols' will be ignored.
      ignore_result(projection_builder.AddColumn(col, /* is_key= */ false));
    }
  } else {
    projection_builder.Reset(projection);
    for (const ColumnSchema& col : missing_cols) {
      // CHECK_OK is safe because the builder's columns (from the projection)
      // and the missing columns are disjoint sets.
      //
      // UNORDERED scans don't need to know which columns are part of the key.
      CHECK_OK(projection_builder.AddColumn(col, /* is_key= */ false));
    }
  }
  projection = projection_builder.BuildWithoutIds();
  VLOG(3) << "Scan projection: " << projection.ToString(Schema::BASE_INFO);

  // It's important to keep the reference to the tablet for the case when the
  // tablet replica's shutdown is run concurrently with the code below.
  shared_ptr<Tablet> tablet;
  RETURN_NOT_OK(GetTabletRef(replica, &tablet, error_code));

  // Ensure the tablet has a valid clean time.
  s = tablet->mvcc_manager()->CheckIsCleanTimeInitialized();
  if (!s.ok()) {
    LOG(WARNING) << Substitute("Rejecting scan request for tablet $0: $1",
                               tablet->tablet_id(), s.ToString());
    // Return TABLET_NOT_RUNNING so the scan can be handled appropriately (fail
    // over to another tablet server if fault-tolerant).
    *error_code = TabletServerErrorPB::TABLET_NOT_RUNNING;
    return s;
  }

  unique_ptr<RowwiseIterator> iter;
  boost::optional<Timestamp> snap_start_timestamp;

  {
    TRACE("Creating iterator");
    TRACE_EVENT0("tserver", "Create iterator");

    switch (scan_pb.read_mode()) {
      case UNKNOWN_READ_MODE: {
        *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
        return Status::NotSupported("Unknown read mode.");
      }
      case READ_LATEST: {
        if (scan_pb.has_snap_start_timestamp()) {
          *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
          return Status::InvalidArgument("scan start timestamp is only supported "
                                         "in READ_AT_SNAPSHOT read mode");
        }
        s = tablet->NewRowIterator(projection, &iter);
        break;
      }
      case READ_YOUR_WRITES: // Fallthrough intended
      case READ_AT_SNAPSHOT: {
        s = HandleScanAtSnapshot(
            scan_pb, rpc_context, projection, tablet.get(), replica->time_manager(),
            &iter, &snap_start_timestamp, snap_timestamp, error_code);
        break;
      }
    }
    TRACE("Iterator created");
  }

  // Make a copy of the optimized spec before it's passed to the iterator.
  // This copy will be given to the Scanner so it can report its predicates to
  // /scans. The copy is necessary because the original spec will be modified
  // as its predicates are pushed into lower-level iterators.
  unique_ptr<ScanSpec> orig_spec(new ScanSpec(spec));

  if (PREDICT_TRUE(s.ok())) {
    TRACE_EVENT0("tserver", "iter->Init");
    s = iter->Init(&spec);
    if (PREDICT_FALSE(s.IsInvalidArgument())) {
      // Tablet::Iterator::Init() returns InvalidArgument when an invalid
      // projection is specified.
      // TODO(todd): would be nice if we threaded these more specific
      // error codes throughout Kudu.
      *error_code = TabletServerErrorPB::MISMATCHED_SCHEMA;
      return s;
    }
  }

  TRACE("Iterator init: $0", s.ToString());

  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << Substitute("Error setting up scanner with request $0: $1",
                               SecureShortDebugString(*req), s.ToString());
    // If the replica has been stopped, e.g. due to disk failure, return
    // TABLET_FAILED so the scan can be handled appropriately (fail over to
    // another tablet server if fault-tolerant).
    if (tablet->HasBeenStopped()) {
      *error_code = TabletServerErrorPB::TABLET_FAILED;
    }
    return s;
  }

  // If this is a snapshot scan and the user specified a specific timestamp to
  // scan at, then check that we are not attempting to scan at a time earlier
  // than the ancient history mark. Only perform this check if tablet history
  // GC is enabled.
  //
  // TODO: This validation essentially prohibits scans with READ_AT_SNAPSHOT
  // when history_max_age is set to zero. There is a tablet history GC related
  // race when the history max age is set to very low, or zero. Imagine a case
  // where a scan was started and READ_AT_SNAPSHOT was specified without
  // specifying a snapshot timestamp, and --tablet_history_max_age_sec=0. The
  // above code path will select the latest timestamp (under a lock) prior to
  // calling RowIterator::Init(), which actually opens the blocks. That means
  // that there is an opportunity in between those two calls for tablet history
  // GC to kick in and delete some history. In fact, we may easily not actually
  // end up with a valid snapshot in that case. It would be more correct to
  // initialize the row iterator and then select the latest timestamp
  // represented by those open files in that case.
  //
  // Now that we have initialized our row iterator at a snapshot, return an
  // error if the snapshot timestamp was prior to the ancient history mark.
  // We have to check after we open the iterator in order to avoid a TOCTOU
  // error since it's possible that initializing the row iterator could race
  // against the tablet history GC maintenance task.
  RETURN_NOT_OK_EVAL(VerifyLegalSnapshotTimestamps(tablet.get(), scan_pb.read_mode(),
                                                   snap_start_timestamp, *snap_timestamp),
                     *error_code = TabletServerErrorPB::INVALID_SNAPSHOT);

  *has_more_results = iter->HasNext() && !scanner->has_fulfilled_limit();
  TRACE("has_more: $0", *has_more_results);
  if (!*has_more_results) {
    // If there are no more rows, we can short circuit some work and respond immediately.
    VLOG(1) << "No more rows, short-circuiting out without creating a server-side scanner.";
    return Status::OK();
  }

  scanner->Init(std::move(iter), std::move(orig_spec));

  // Stop the scanner timer because ContinueScanRequest starts its own timer.
  scanner_timer.Stop();
  unreg_scanner.Cancel();
  *scanner_id = scanner->id();

  VLOG(1) << "Started scanner " << scanner->id() << ": " << scanner->iter()->ToString();

  size_t batch_size_bytes = GetMaxBatchSizeBytesHint(req);
  if (batch_size_bytes > 0) {
    TRACE("Continuing scan request");
    // TODO(wdberkeley): Instead of copying the pb, instead split
    // HandleContinueScanRequest and call the second half directly. Once that's
    // done, remove the call to ScopedAddScannerTiming::Stop() above (and the
    // method as it won't be used) and start the timing for continue requests
    // from the first half that is no longer executed in this codepath.
    ScanRequestPB continue_req(*req);
    continue_req.set_scanner_id(scanner->id());
    RETURN_NOT_OK(HandleContinueScanRequest(&continue_req, rpc_context, result_collector,
                                            has_more_results, error_code));
  } else {
    // Increment the scanner call sequence ID. HandleContinueScanRequest handles
    // this in the non-empty scan case.
    scanner->IncrementCallSeqId();
  }
  return Status::OK();
}

// Continue an existing scan request.
Status TabletServiceImpl::HandleContinueScanRequest(const ScanRequestPB* req,
                                                    const RpcContext* rpc_context,
                                                    ScanResultCollector* result_collector,
                                                    bool* has_more_results,
                                                    TabletServerErrorPB::Code* error_code) {
  DCHECK(req->has_scanner_id());
  TRACE_EVENT1("tserver", "TabletServiceImpl::HandleContinueScanRequest",
               "scanner_id", req->scanner_id());

  size_t batch_size_bytes = GetMaxBatchSizeBytesHint(req);

  // TODO(todd): need some kind of concurrency control on these scanner objects
  // in case multiple RPCs hit the same scanner at the same time. Probably
  // just a trylock and fail the RPC if it contends.
  SharedScanner scanner;
  TabletServerErrorPB::Code code = TabletServerErrorPB::UNKNOWN_ERROR;
  Status s = server_->scanner_manager()->LookupScanner(req->scanner_id(),
                                                       rpc_context->remote_user().username(),
                                                       &code,
                                                       &scanner);
  if (!s.ok()) {
    if (s.IsNotFound() && batch_size_bytes == 0 && req->close_scanner()) {
      // Silently ignore any request to close a non-existent scanner.
      return Status::OK();
    }
    LOG(INFO) << Substitute("Scan: $0: call sequence id=$1, remote=$2",
                            s.ToString(), req->call_seq_id(), rpc_context->requestor_string());
    *error_code = code;
    return s;
  }

  if (PREDICT_FALSE(FLAGS_scanner_inject_service_unavailable_on_continue_scan)) {
    return Status::ServiceUnavailable("Injecting service unavailable status on Scan due to "
                                      "--scanner_inject_service_unavailable_on_continue_scan");
  }

  // If we early-exit out of this function, automatically unregister the scanner.
  ScopedUnregisterScanner unreg_scanner(server_->scanner_manager(), scanner->id());
  ScopedAddScannerTiming scanner_timer(scanner.get(), result_collector->cpu_times());

  VLOG(2) << "Found existing scanner " << scanner->id() << " for request: "
          << SecureShortDebugString(*req);
  TRACE("Found scanner $0 for tablet $1", scanner->id(), scanner->tablet_id());

  // Set the row format flags on the ScanResultCollector.
  s = result_collector->InitSerializer(scanner->row_format_flags());
  if (!s.ok()) {
    *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
    return s;
  }

  if (batch_size_bytes == 0 && req->close_scanner()) {
    *has_more_results = false;
    return Status::OK();
  }

  if (req->call_seq_id() != scanner->call_seq_id()) {
    *error_code = TabletServerErrorPB::INVALID_SCAN_CALL_SEQ_ID;
    return Status::InvalidArgument("Invalid call sequence ID in scan request");
  }
  scanner->IncrementCallSeqId();

  RowwiseIterator* iter = scanner->iter();

  // TODO(todd): could size the RowBlock based on the user's requested batch size?
  // If people had really large indirect objects, we would currently overshoot
  // their requested batch size by a lot.
  Arena arena(32 * 1024);
  RowBlock block(&scanner->iter()->schema(),
                 FLAGS_scanner_batch_size_rows, &arena);

  // TODO(todd): in the future, use the client timeout to set a budget. For now,
  // just use a half second, which should be plenty to amortize call overhead.
  int budget_ms = 500;
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromMilliseconds(budget_ms);

  int64_t rows_scanned = 0;
  while (iter->HasNext() && !scanner->has_fulfilled_limit()) {
    if (PREDICT_FALSE(FLAGS_scanner_inject_latency_on_each_batch_ms > 0)) {
      SleepFor(MonoDelta::FromMilliseconds(FLAGS_scanner_inject_latency_on_each_batch_ms));
    }

    Status s = iter->NextBlock(&block);
    if (PREDICT_FALSE(!s.ok())) {
      LOG(WARNING) << "Copying rows from internal iterator for request "
                   << SecureShortDebugString(*req);
      *error_code = TabletServerErrorPB::UNKNOWN_ERROR;
      return s;
    }

    if (PREDICT_TRUE(block.nrows() > 0)) {
      // Count the number of rows scanned, regardless of predicates or deletions.
      // The collector will separately count the number of rows actually returned to
      // the client.
      rows_scanned += block.nrows();
      if (scanner->spec().has_limit()) {
        int64_t rows_left = scanner->spec().limit() - scanner->num_rows_returned();
        DCHECK_GT(rows_left, 0);  // Guaranteed by has_fulfilled_limit()
        block.selection_vector()->ClearToSelectAtMost(static_cast<size_t>(rows_left));
      }
      result_collector->HandleRowBlock(scanner.get(), block);
    }

    int64_t response_size = result_collector->ResponseSize();

    if (VLOG_IS_ON(2)) {
      // This may be fairly expensive if row block size is small
      TRACE("Copied block (nrows=$0), new size=$1", block.nrows(), response_size);
    }

    // TODO: should check if RPC got cancelled, once we implement RPC cancellation.
    if (PREDICT_FALSE(MonoTime::Now() >= deadline)) {
      TRACE("Deadline expired - responding early");
      break;
    }

    if (response_size >= batch_size_bytes) {
      break;
    }
  }

  scoped_refptr<TabletReplica> replica = scanner->tablet_replica();
  shared_ptr<Tablet> tablet;
  TabletServerErrorPB::Code tablet_ref_error_code;
  s = GetTabletRef(replica, &tablet, &tablet_ref_error_code);
  // If the tablet is not running, but the scan operation in progress
  // has reached this point, the tablet server has the necessary data to
  // send in response for the scan continuation request.
  if (PREDICT_FALSE(!s.ok() && tablet_ref_error_code !=
                        TabletServerErrorPB::TABLET_NOT_RUNNING)) {
    *error_code = tablet_ref_error_code;
    return s;
  }

  // Calculate the number of rows/cells/bytes actually processed. Here we have to dig
  // into the per-column iterator stats, sum them up, and then subtract out the
  // total that we already reported in a previous scan.
  vector<IteratorStats> stats_by_col;
  scanner->GetIteratorStats(&stats_by_col);
  IteratorStats total_stats = std::accumulate(stats_by_col.begin(),
                                              stats_by_col.end(),
                                              IteratorStats());

  IteratorStats delta_stats = total_stats - scanner->already_reported_stats();
  scanner->set_already_reported_stats(total_stats);
  TRACE_COUNTER_INCREMENT(SCANNER_BYTES_READ_METRIC_NAME, delta_stats.bytes_read);

  // Update metrics based on this scan request.
  if (tablet) {
    // The number of rows/cells/bytes actually returned to the user.
    tablet->metrics()->scanner_rows_returned->IncrementBy(
        result_collector->NumRowsReturned());
    tablet->metrics()->scanner_cells_returned->IncrementBy(
        result_collector->NumRowsReturned() *
            scanner->client_projection_schema()->num_columns());
    tablet->metrics()->scanner_bytes_returned->IncrementBy(
        result_collector->ResponseSize());

    // The number of rows/cells/bytes actually processed.
    tablet->metrics()->scanner_rows_scanned->IncrementBy(rows_scanned);
    tablet->metrics()->scanner_cells_scanned_from_disk->IncrementBy(delta_stats.cells_read);
    tablet->metrics()->scanner_bytes_scanned_from_disk->IncrementBy(delta_stats.bytes_read);

    // Last read timestamp.
    tablet->UpdateLastReadTime();
  }

  *has_more_results = !req->close_scanner() && iter->HasNext() &&
      !scanner->has_fulfilled_limit();
  if (*has_more_results) {
    unreg_scanner.Cancel();
  } else {
    VLOG(2) << "Scanner " << scanner->id() << " complete: removing...";
  }

  return Status::OK();
}

namespace {
// Helper to clamp a client deadline for a scan to the max supported by the server.
MonoTime ClampScanDeadlineForWait(const MonoTime& deadline, bool* was_clamped) {
  MonoTime now = MonoTime::Now();
  if ((deadline - now).ToMilliseconds() > FLAGS_scanner_max_wait_ms) {
    *was_clamped = true;
    return now + MonoDelta::FromMilliseconds(FLAGS_scanner_max_wait_ms);
  }
  *was_clamped = false;
  return deadline;
}
} // anonymous namespace

Status TabletServiceImpl::HandleScanAtSnapshot(const NewScanRequestPB& scan_pb,
                                               const RpcContext* rpc_context,
                                               const Schema& projection,
                                               Tablet* tablet,
                                               TimeManager* time_manager,
                                               unique_ptr<RowwiseIterator>* iter,
                                               boost::optional<Timestamp>* snap_start_timestamp,
                                               Timestamp* snap_timestamp,
                                               TabletServerErrorPB::Code* error_code) {
  switch (scan_pb.read_mode()) {
    case READ_AT_SNAPSHOT: // Fallthrough intended
    case READ_YOUR_WRITES:
      break;
    default:
      LOG(FATAL) << "Unsupported snapshot scan mode specified.";
  }

  // Based on the read mode, pick a timestamp and verify it.
  Timestamp tmp_snap_timestamp;
  Status s = PickAndVerifyTimestamp(scan_pb, tablet, &tmp_snap_timestamp);
  if (PREDICT_FALSE(!s.ok())) {
    *error_code = TabletServerErrorPB::INVALID_SNAPSHOT;
    return s.CloneAndPrepend("cannot verify timestamp");
  }

  // Reduce the client's deadline by a few msecs to allow for overhead.
  MonoTime client_deadline = rpc_context->GetClientDeadline() - MonoDelta::FromMilliseconds(10);

  // Its not good for the tablet server or for the client if we hang here forever. The tablet
  // server will have one less available thread and the client might be stuck spending all
  // of the allotted time for the scan on a partitioned server that will never have a consistent
  // snapshot at 'snap_timestamp'.
  // Because of this we clamp the client's deadline to the maximum configured
  // scanner wait time. If the client sets a longer timeout then it can use it
  // by retrying (possibly on other servers).
  bool was_clamped = false;
  MonoTime final_deadline = ClampScanDeadlineForWait(client_deadline, &was_clamped);

  // Wait for the tablet to know that 'snap_timestamp' is safe. I.e. that all operations
  // that came before it are, at least, started. This, together with waiting for the mvcc
  // snapshot to be clean below, allows us to always return the same data when scanning at
  // the same timestamp (repeatable reads).
  TRACE("Waiting safe time to advance");
  MonoTime before = MonoTime::Now();
  s = time_manager->WaitUntilSafe(tmp_snap_timestamp, final_deadline);

  tablet::MvccSnapshot snap;
  if (PREDICT_TRUE(s.ok())) {
    // Wait for the in-flights in the snapshot to be finished.
    TRACE("Waiting for operations to commit");
    s = tablet->mvcc_manager()->WaitForSnapshotWithAllCommitted(
          tmp_snap_timestamp, &snap, client_deadline);
  }

  // If we got an TimeOut but we had clamped the deadline, return a ServiceUnavailable instead
  // so that the client retries.
  if (PREDICT_FALSE(s.IsTimedOut() && was_clamped)) {
    *error_code = TabletServerErrorPB::THROTTLED;
    return Status::ServiceUnavailable(s.CloneAndPrepend(
        "could not wait for desired snapshot timestamp to be consistent").ToString());
  }
  RETURN_NOT_OK(s);

  uint64_t duration_usec = (MonoTime::Now() - before).ToMicroseconds();
  tablet->metrics()->snapshot_read_inflight_wait_duration->Increment(duration_usec);
  TRACE("All operations in snapshot committed. Waited for $0 microseconds", duration_usec);

  tablet::RowIteratorOptions opts;
  opts.projection = &projection;
  opts.snap_to_include = snap;
  opts.order = scan_pb.order_mode();

  boost::optional<Timestamp> tmp_snap_start_timestamp;
  if (scan_pb.has_snap_start_timestamp()) {
    if (scan_pb.read_mode() != READ_AT_SNAPSHOT) {
      // TODO(mpercy): Should we allow READ_YOUR_WRITES mode? There is no
      // obvious use for it, but also no obvious reason not to support it,
      // except for the fact that we would also have to test it.
      *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
      return Status::InvalidArgument("scan start timestamp is only supported "
                                     "in READ_AT_SNAPSHOT read mode");
    }
    if (scan_pb.order_mode() != ORDERED) {
      *error_code = TabletServerErrorPB::INVALID_SCAN_SPEC;
      return Status::InvalidArgument("scan start timestamp is only supported "
                                     "in ORDERED order mode");
    }
    tmp_snap_start_timestamp = Timestamp(scan_pb.snap_start_timestamp());
    opts.snap_to_exclude = MvccSnapshot(*tmp_snap_start_timestamp);
    opts.include_deleted_rows = true;
  }

  // Before we open / wait on anything check that the timestamp(s) are after
  // the AHM. This is not the final check. We'll check this again after the
  // iterators are open but there is no point in doing the work to initialize
  // the iterators and spending the time to wait for a snapshot timestamp to be
  // readable when we can't read back to one of the requested timestamps.
  RETURN_NOT_OK_EVAL(VerifyLegalSnapshotTimestamps(tablet, scan_pb.read_mode(),
                                                   tmp_snap_start_timestamp,
                                                   tmp_snap_timestamp),
                     *error_code = TabletServerErrorPB::INVALID_SNAPSHOT);

  RETURN_NOT_OK(tablet->NewRowIterator(std::move(opts), iter));

  // Return the picked snapshot timestamp for both READ_AT_SNAPSHOT
  // and READ_YOUR_WRITES mode, as well as the parsed start timestamp for
  // READ_AT_SNAPSHOT mode, if specified.
  *snap_start_timestamp = std::move(tmp_snap_start_timestamp);
  *snap_timestamp = tmp_snap_timestamp;
  return Status::OK();
}

Status TabletServiceImpl::ValidateTimestamp(const Timestamp& snap_timestamp) {
  Timestamp max_allowed_ts;
  Status s = server_->clock()->GetGlobalLatest(&max_allowed_ts);
  if (PREDICT_FALSE(s.IsNotSupported()) &&
      PREDICT_TRUE(!FLAGS_scanner_allow_snapshot_scans_with_logical_timestamps)) {
    return Status::NotSupported("Snapshot scans not supported on this server",
                                s.ToString());
  }

  // Note: if 'max_allowed_ts' is not obtained from clock_->GetGlobalLatest(), e.g.,
  // in case logical clock is used, it's guaranteed to be higher than 'tmp_snap_timestamp',
  // since 'max_allowed_ts' is default-constructed to kInvalidTimestamp (MAX_LONG - 1).
  if (snap_timestamp > max_allowed_ts) {
    return Status::InvalidArgument(
        Substitute("Snapshot time $0 in the future. Max allowed timestamp is $1",
                   server_->clock()->Stringify(snap_timestamp),
                   server_->clock()->Stringify(max_allowed_ts)));
  }

  return Status::OK();
}

Status TabletServiceImpl::PickAndVerifyTimestamp(const NewScanRequestPB& scan_pb,
                                                 Tablet* tablet,
                                                 Timestamp* snap_timestamp) {
  // If the client sent a timestamp update our clock with it.
  if (scan_pb.has_propagated_timestamp()) {
    Timestamp propagated_timestamp(scan_pb.propagated_timestamp());

    // Update the clock so that we never generate snapshots lower than
    // 'propagated_timestamp'. If 'propagated_timestamp' is lower than
    // 'now' this call has no effect. If 'propagated_timestamp' is too far
    // into the future this will fail and we abort.
    RETURN_NOT_OK(server_->clock()->Update(propagated_timestamp));
  }

  Timestamp tmp_snap_timestamp;
  ReadMode read_mode = scan_pb.read_mode();
  tablet::MvccManager* mvcc_manager = tablet->mvcc_manager();

  if (read_mode == READ_AT_SNAPSHOT) {
    // For READ_AT_SNAPSHOT mode,
    //   1) if the client provided no snapshot timestamp we take the current
    //      clock time as the snapshot timestamp.
    //   2) else we use the client provided one, but make sure it is not too
    //      far in the future as to be invalid.
    if (!scan_pb.has_snap_timestamp()) {
      tmp_snap_timestamp = server_->clock()->Now();
    } else {
      tmp_snap_timestamp.FromUint64(scan_pb.snap_timestamp());
      RETURN_NOT_OK(ValidateTimestamp(tmp_snap_timestamp));
    }
  } else {
    // For READ_YOUR_WRITES mode, we use the following to choose a
    // snapshot timestamp: MAX(propagated timestamp + 1, 'clean' timestamp).
    // There is no need to validate if the chosen timestamp is too far in
    // the future, since:
    //   1) MVCC 'clean' timestamp is by definition in the past (it's maximally
    //      bounded by safe time).
    //   2) the propagated timestamp was used to update the clock above and the
    //      update would have returned an error if the the timestamp was too
    //      far in the future.
    uint64_t clean_timestamp = mvcc_manager->GetCleanTimestamp().ToUint64();
    uint64_t propagated_timestamp = scan_pb.has_propagated_timestamp() ?
                                    scan_pb.propagated_timestamp() : Timestamp::kMin.ToUint64();
    tmp_snap_timestamp = Timestamp(std::max(propagated_timestamp + 1, clean_timestamp));
  }
  *snap_timestamp = tmp_snap_timestamp;
  return Status::OK();
}

} // namespace tserver
} // namespace kudu
