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
#include "kudu/tserver/tablet_copy_service.h"

#include <cstdint>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/server/server_base.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_replica_lookup.h"
#include "kudu/util/crc.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/metrics.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random_util.h"

#define RPC_RETURN_NOT_OK(expr, app_err, message, context) \
  do { \
    const Status& s = (expr); \
    if (PREDICT_FALSE(!s.ok())) { \
      SetupErrorAndRespond(context, app_err, message, s); \
      return; \
    } \
  } while (false)

DEFINE_uint64(tablet_copy_idle_timeout_sec, 600,
              "Amount of time without activity before a tablet copy "
              "session will expire, in seconds");
TAG_FLAG(tablet_copy_idle_timeout_sec, advanced);
TAG_FLAG(tablet_copy_idle_timeout_sec, evolving);

DEFINE_uint64(tablet_copy_timeout_poll_period_ms, 10000,
              "How often the tablet_copy service polls for expired "
              "tablet copy sessions, in millis");
TAG_FLAG(tablet_copy_timeout_poll_period_ms, hidden);

DEFINE_double(fault_crash_on_handle_tc_fetch_data, 0.0,
              "Fraction of the time when the tablet will crash while "
              "servicing a TabletCopyService FetchData() RPC call. "
              "(For testing only!)");
TAG_FLAG(fault_crash_on_handle_tc_fetch_data, unsafe);

DEFINE_double(tablet_copy_early_session_timeout_prob, 0,
              "The probability that a tablet copy session will time out early, "
              "resulting in tablet copy failure. (For testing only!)");
TAG_FLAG(tablet_copy_early_session_timeout_prob, unsafe);

using std::string;
using std::vector;
using strings::Substitute;

namespace google {
namespace protobuf {
class Message;
}
}

namespace kudu {

using crc::Crc32c;
using server::ServerBase;
using pb_util::SecureShortDebugString;
using tablet::TabletReplica;

namespace tserver {

static MonoTime GetNewExpireTime() {
  return MonoTime::Now() + MonoDelta::FromSeconds(FLAGS_tablet_copy_idle_timeout_sec);
}

TabletCopyServiceImpl::TabletCopyServiceImpl(
    ServerBase* server,
    TabletReplicaLookupIf* tablet_replica_lookup)
    : TabletCopyServiceIf(server->metric_entity(), server->result_tracker()),
      server_(server),
      fs_manager_(CHECK_NOTNULL(server->fs_manager())),
      tablet_replica_lookup_(CHECK_NOTNULL(tablet_replica_lookup)),
      rand_(GetRandomSeed32()),
      shutdown_latch_(1),
      tablet_copy_metrics_(server->metric_entity()) {
  CHECK_OK(Thread::Create("tablet-copy", "tc-session-exp",
                          &TabletCopyServiceImpl::EndExpiredSessions, this,
                          &session_expiration_thread_));
}

bool TabletCopyServiceImpl::AuthorizeServiceUser(const google::protobuf::Message* /*req*/,
                                                 google::protobuf::Message* /*resp*/,
                                                 rpc::RpcContext* rpc) {
  return server_->Authorize(rpc, ServerBase::SUPER_USER | ServerBase::SERVICE_USER);
}

void TabletCopyServiceImpl::BeginTabletCopySession(
        const BeginTabletCopySessionRequestPB* req,
        BeginTabletCopySessionResponsePB* resp,
        rpc::RpcContext* context) {
  const string& requestor_uuid = req->requestor_uuid();
  const string& tablet_id = req->tablet_id();

  LOG_WITH_PREFIX(INFO) << Substitute(
      "Received BeginTabletCopySession request for tablet $0 from peer $1 ($2)",
      tablet_id, requestor_uuid, context->requestor_string());

  // For now, we use the requestor_uuid with the tablet id as the session id,
  // but there is no guarantee this will not change in the future.
  const string session_id = Substitute("$0-$1", requestor_uuid, tablet_id);

  scoped_refptr<TabletReplica> tablet_replica;
  RPC_RETURN_NOT_OK(tablet_replica_lookup_->GetTabletReplica(tablet_id, &tablet_replica),
                    TabletCopyErrorPB::TABLET_NOT_FOUND,
                    Substitute("Unable to find specified tablet: $0", tablet_id),
                    context);

  scoped_refptr<TabletCopySourceSession> session;
  {
    MutexLock l(sessions_lock_);
    const SessionEntry* session_entry = FindOrNull(sessions_, session_id);
    if (!session_entry) {
      LOG_WITH_PREFIX(INFO) << Substitute(
          "Beginning new tablet copy session on tablet $0 from peer $1"
          " at $2: session id = $3",
          tablet_id, requestor_uuid, context->requestor_string(), session_id);
      session.reset(new TabletCopySourceSession(tablet_replica, session_id,
                                                requestor_uuid, fs_manager_,
                                                &tablet_copy_metrics_));
      RPC_RETURN_NOT_OK(session->Init(),
                        TabletCopyErrorPB::UNKNOWN_ERROR,
                        Substitute("Error beginning tablet copy session for tablet $0", tablet_id),
                        context);
      InsertOrDie(&sessions_, session_id, { session, GetNewExpireTime() });
    } else {
      session = session_entry->session;
      LOG_WITH_PREFIX(INFO) << Substitute(
          "Re-sending initialization info for existing tablet copy session on tablet $0"
          " from peer $1 at $2: session_id = $3",
          tablet_id, requestor_uuid, context->requestor_string(), session_id);
    }
    ResetSessionExpirationUnlocked(session_id);
  }

  resp->set_responder_uuid(fs_manager_->uuid());
  resp->set_session_id(session_id);
  resp->set_session_idle_timeout_millis(FLAGS_tablet_copy_idle_timeout_sec * 1000);
  resp->mutable_superblock()->CopyFrom(session->tablet_superblock());
  resp->mutable_initial_cstate()->CopyFrom(session->initial_cstate());

  for (const scoped_refptr<log::ReadableLogSegment>& segment : session->log_segments()) {
    resp->add_wal_segment_seqnos(segment->header().sequence_number());
  }

  // For testing: Close the session prematurely if unsafe gflag is set but
  // still respond as if it was opened.
  if (PREDICT_FALSE(FLAGS_tablet_copy_early_session_timeout_prob > 0 &&
      rand_.NextDoubleFraction() <= FLAGS_tablet_copy_early_session_timeout_prob)) {
    LOG_WITH_PREFIX(WARNING) << "Timing out tablet copy session due to flag "
                             << "--tablet_copy_early_session_timeout_prob "
                             << "being set to " << FLAGS_tablet_copy_early_session_timeout_prob;
    MutexLock l(sessions_lock_);
    TabletCopyErrorPB::Code app_error;
    WARN_NOT_OK(TabletCopyServiceImpl::DoEndTabletCopySessionUnlocked(session_id, &app_error),
                Substitute("Unable to forcibly end tablet copy session $0", session_id));
  }

  context->RespondSuccess();
}

void TabletCopyServiceImpl::CheckSessionActive(
        const CheckTabletCopySessionActiveRequestPB* req,
        CheckTabletCopySessionActiveResponsePB* resp,
        rpc::RpcContext* context) {
  const string& session_id = req->session_id();

  // Look up and validate tablet copy session.
  scoped_refptr<TabletCopySourceSession> session;
  MutexLock l(sessions_lock_);
  TabletCopyErrorPB::Code app_error;
  Status status = FindSessionUnlocked(session_id, &app_error, &session);
  if (status.ok()) {
    if (req->keepalive()) {
      ResetSessionExpirationUnlocked(session_id);
    }
    resp->set_session_is_active(true);
    context->RespondSuccess();
    return;
  } else if (app_error == TabletCopyErrorPB::NO_SESSION) {
    resp->set_session_is_active(false);
    context->RespondSuccess();
    return;
  } else {
    RPC_RETURN_NOT_OK(status, app_error,
                      Substitute("Error trying to check whether session $0 is active", session_id),
                      context);
  }
}

void TabletCopyServiceImpl::FetchData(const FetchDataRequestPB* req,
                                      FetchDataResponsePB* resp,
                                      rpc::RpcContext* context) {
  const string& session_id = req->session_id();

  // Look up and validate tablet copy session.
  scoped_refptr<TabletCopySourceSession> session;
  {
    MutexLock l(sessions_lock_);
    TabletCopyErrorPB::Code app_error;
    RPC_RETURN_NOT_OK(FindSessionUnlocked(session_id, &app_error, &session),
                      app_error, "No such session", context);
    ResetSessionExpirationUnlocked(session_id);
  }

  MAYBE_FAULT(FLAGS_fault_crash_on_handle_tc_fetch_data);

  uint64_t offset = req->offset();
  int64_t client_maxlen = req->max_length();

  const DataIdPB& data_id = req->data_id();
  TabletCopyErrorPB::Code error_code = TabletCopyErrorPB::UNKNOWN_ERROR;
  RPC_RETURN_NOT_OK(ValidateFetchRequestDataId(data_id, &error_code, session),
                    error_code, "Invalid DataId", context);

  DataChunkPB* data_chunk = resp->mutable_chunk();
  string* data = data_chunk->mutable_data();
  int64_t total_data_length = 0;
  if (data_id.type() == DataIdPB::BLOCK) {
    // Fetching a data block chunk.
    const BlockId& block_id = BlockId::FromPB(data_id.block_id());
    RPC_RETURN_NOT_OK(session->GetBlockPiece(block_id, offset, client_maxlen,
                                             data, &total_data_length, &error_code),
                      error_code, "Unable to get piece of data block", context);
  } else {
    // Fetching a log segment chunk.
    uint64_t segment_seqno = data_id.wal_segment_seqno();
    RPC_RETURN_NOT_OK(session->GetLogSegmentPiece(segment_seqno, offset, client_maxlen,
                                                  data, &total_data_length, &error_code),
                      error_code, "Unable to get piece of log segment", context);
  }

  data_chunk->set_total_data_length(total_data_length);
  data_chunk->set_offset(offset);

  tablet_copy_metrics_.bytes_sent->IncrementBy(resp->chunk().data().size());

  // Calculate checksum.
  uint32_t crc32 = Crc32c(data->data(), data->length());
  data_chunk->set_crc32(crc32);

  context->RespondSuccess();
}

void TabletCopyServiceImpl::EndTabletCopySession(
        const EndTabletCopySessionRequestPB* req,
        EndTabletCopySessionResponsePB* resp,
        rpc::RpcContext* context) {
  {
    MutexLock l(sessions_lock_);
    TabletCopyErrorPB::Code app_error;
    LOG_WITH_PREFIX(INFO) << "Request end of tablet copy session " << req->session_id()
                          << " received from " << context->requestor_string();
    RPC_RETURN_NOT_OK(DoEndTabletCopySessionUnlocked(req->session_id(), &app_error),
                      app_error, "No such session", context);
  }
  context->RespondSuccess();
}

void TabletCopyServiceImpl::Shutdown() {
  shutdown_latch_.CountDown();
  session_expiration_thread_->Join();

  // Destroy all tablet copy sessions.
  MutexLock l(sessions_lock_);
  auto iter = sessions_.cbegin();
  while (iter != sessions_.cend()) {
    const string& session_id = iter->first;
    // Increment the iterator before erasing the corresponding session from the
    // map in DoEndTabletCopySessionUnlocked().
    ++iter; // Don't use until next iteration of the loop.
    LOG_WITH_PREFIX(INFO) << "Destroying tablet copy session " << session_id
                          << " due to service shutdown";
    TabletCopyErrorPB::Code app_error;
    WARN_NOT_OK(DoEndTabletCopySessionUnlocked(session_id, &app_error),
                "Unable to end tablet copy session during service shutdown");
  }
}

Status TabletCopyServiceImpl::FindSessionUnlocked(
        const string& session_id,
        TabletCopyErrorPB::Code* app_error,
        scoped_refptr<TabletCopySourceSession>* session) const {
  const SessionEntry* session_entry = FindOrNull(sessions_, session_id);
  if (!session_entry) {
    *app_error = TabletCopyErrorPB::NO_SESSION;
    return Status::NotFound(
        Substitute("Tablet Copy session with Session ID \"$0\" not found", session_id));
  }
  *session = session_entry->session;
  return Status::OK();
}

Status TabletCopyServiceImpl::ValidateFetchRequestDataId(
        const DataIdPB& data_id,
        TabletCopyErrorPB::Code* app_error,
        const scoped_refptr<TabletCopySourceSession>& session) const {
  if (PREDICT_FALSE(data_id.has_block_id() && data_id.has_wal_segment_seqno())) {
    *app_error = TabletCopyErrorPB::INVALID_TABLET_COPY_REQUEST;
    return Status::InvalidArgument(
        Substitute("Only one of BlockId or segment sequence number are required, "
            "but both were specified. DataTypeID: $0", SecureShortDebugString(data_id)));
  } else if (PREDICT_FALSE(!data_id.has_block_id() && !data_id.has_wal_segment_seqno())) {
    *app_error = TabletCopyErrorPB::INVALID_TABLET_COPY_REQUEST;
    return Status::InvalidArgument(
        Substitute("Only one of BlockId or segment sequence number are required, "
            "but neither were specified. DataTypeID: $0", SecureShortDebugString(data_id)));
  }

  if (data_id.type() == DataIdPB::BLOCK) {
    if (PREDICT_FALSE(!data_id.has_block_id())) {
      return Status::InvalidArgument("block_id must be specified for type == BLOCK",
                                     SecureShortDebugString(data_id));
    }
  } else {
    if (PREDICT_FALSE(!data_id.wal_segment_seqno())) {
      return Status::InvalidArgument(
          "segment sequence number must be specified for type == LOG_SEGMENT",
          SecureShortDebugString(data_id));
    }
  }

  return Status::OK();
}

void TabletCopyServiceImpl::ResetSessionExpirationUnlocked(const std::string& session_id) {
  SessionEntry* session_entry = FindOrNull(sessions_, session_id);
  if (!session_entry) return;
  session_entry->expires = GetNewExpireTime();
}

Status TabletCopyServiceImpl::DoEndTabletCopySessionUnlocked(
        const std::string& session_id,
        TabletCopyErrorPB::Code* app_error) {
  sessions_lock_.AssertAcquired();
  scoped_refptr<TabletCopySourceSession> session;
  RETURN_NOT_OK(FindSessionUnlocked(session_id, app_error, &session));
  // Remove the session from the map.
  // It will get destroyed once there are no outstanding refs.
  LOG_WITH_PREFIX(INFO) << "Ending tablet copy session " << session_id << " on tablet "
                        << session->tablet_id() << " with peer " << session->requestor_uuid();
  CHECK_EQ(1, sessions_.erase(session_id));
  return Status::OK();
}

void TabletCopyServiceImpl::EndExpiredSessions() {
  do {
    MutexLock l(sessions_lock_);
    const MonoTime now = MonoTime::Now();

    vector<string> expired_session_ids;
    for (const auto& entry : sessions_) {
      const string& session_id = entry.first;
      const MonoTime& expiration = entry.second.expires;
      if (expiration < now) {
        expired_session_ids.push_back(session_id);
      }
    }
    for (const string& session_id : expired_session_ids) {
      LOG_WITH_PREFIX(INFO) << "Tablet Copy session " << session_id
                            << " has expired. Terminating session.";
      TabletCopyErrorPB::Code app_error;
      CHECK_OK(DoEndTabletCopySessionUnlocked(session_id, &app_error));
    }
  } while (!shutdown_latch_.WaitFor(MonoDelta::FromMilliseconds(
                                    FLAGS_tablet_copy_timeout_poll_period_ms)));
}

string TabletCopyServiceImpl::LogPrefix() const {
  // We use a truncated form of the "T xxxx P yyyy" prefix here, with only the
  // "P" part, because we don't want it to appear that tablet 'foo' is running
  // when logging error messages like "Can't find tablet 'foo'".
  return Substitute("P $0: ", fs_manager_->uuid());
}

void TabletCopyServiceImpl::SetupErrorAndRespond(
    rpc::RpcContext* context,
    TabletCopyErrorPB::Code code,
    const string& message,
    const Status& s) {
  LOG_WITH_PREFIX(WARNING) << "Error handling TabletCopyService RPC request from "
                           << context->requestor_string()
                           << ": " << message << ": " << s.ToString();
  TabletCopyErrorPB error;
  StatusToPB(s, error.mutable_status());
  error.set_code(code);
  context->RespondApplicationError(TabletCopyErrorPB::tablet_copy_error_ext.number(),
                                   message, error);
}

} // namespace tserver
} // namespace kudu
