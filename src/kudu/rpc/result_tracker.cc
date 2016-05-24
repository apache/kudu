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

#include "kudu/rpc/result_tracker.h"

#include "kudu/gutil/map-util.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/trace.h"
#include "kudu/util/pb_util.h"

namespace kudu {
namespace rpc {

using google::protobuf::Message;
using rpc::InboundCall;
using std::move;
using std::lock_guard;
using std::string;
using std::unique_ptr;

ResultTracker::RpcState ResultTracker::TrackRpc(const RequestIdPB& request_id,
                                                Message* response,
                                                RpcContext* context) {
  lock_guard<simple_spinlock> l(lock_);
  return TrackRpcUnlocked(request_id, response, context);
}

ResultTracker::RpcState ResultTracker::TrackRpcUnlocked(const RequestIdPB& request_id,
                                                        Message* response,
                                                        RpcContext* context) {

  ClientState* client_state = ComputeIfAbsent(
      &clients_,
      request_id.client_id(),
      []{ return unique_ptr<ClientState>(new ClientState()); })->get();

  client_state->last_heard_from = MonoTime::Now(MonoTime::FINE);

  auto result = ComputeIfAbsentReturnAbsense(
      &client_state->completion_records,
      request_id.seq_no(),
      []{ return unique_ptr<CompletionRecord>(new CompletionRecord()); });

  CompletionRecord* completion_record = result.first->get();

  if (PREDICT_TRUE(result.second)) {
    completion_record->state = RpcState::IN_PROGRESS;
    completion_record->driver_attempt_no = request_id.attempt_no();
    // When a follower is applying an operation it doesn't have a response yet, and it won't
    // have a context, so only set them if they exist.
    if (response != nullptr) {
      completion_record->ongoing_rpcs.push_back({response,
                                                 DCHECK_NOTNULL(context),
                                                 request_id.attempt_no()});
    }
    return RpcState::NEW;
  }

  switch (completion_record->state) {
    case RpcState::COMPLETED: {
      // If the RPC is COMPLETED and the request originates from a client (context, response are
      // non-null) copy the response and reply immediately. If there is no context/response
      // do nothing.
      if (context != nullptr) {
        DCHECK_NOTNULL(response)->CopyFrom(*completion_record->response);
        context->call_->RespondSuccess(*response);
        delete context;
      }
      return RpcState::COMPLETED;
    }
    case RpcState::IN_PROGRESS: {
      // If the RPC is IN_PROGRESS check if there is a context and, if so, attach it
      // so that the rpc gets the same response when the original one completes.
      if (context != nullptr) {
        completion_record->ongoing_rpcs.push_back({DCHECK_NOTNULL(response),
                                                   context,
                                                   NO_HANDLER});
      }
      return RpcState::IN_PROGRESS;
    }
    default:
      LOG(FATAL) << "Wrong state: " << completion_record->state;
      // dummy return to avoid warnings
      return RpcState::STALE;
  }
}

ResultTracker::RpcState ResultTracker::TrackRpcOrChangeDriver(const RequestIdPB& request_id) {
  lock_guard<simple_spinlock> l(lock_);
  RpcState state = TrackRpcUnlocked(request_id, nullptr, nullptr);

  if (state != RpcState::IN_PROGRESS) return state;

  CompletionRecord* completion_record = FindCompletionRecordOrDieUnlocked(request_id);

  // ... if we did find a CompletionRecord change the driver and return true.
  completion_record->driver_attempt_no = request_id.attempt_no();
  completion_record->ongoing_rpcs.push_back({nullptr,
                                             nullptr,
                                             request_id.attempt_no()});
  // Since we changed the driver of the RPC, return NEW, so that the caller knows
  // to store the result.
  return RpcState::NEW;
}

bool ResultTracker::IsCurrentDriver(const RequestIdPB& request_id) {
  lock_guard<simple_spinlock> l(lock_);
  CompletionRecord* completion_record = FindCompletionRecordOrNullUnlocked(request_id);

  // If we couldn't find the CompletionRecord, someone might have called FailAndRespond() so
  // just return false.
  if (completion_record == nullptr) return false;

  // ... if we did find a CompletionRecord return true if we're the driver or false
  // otherwise.
  return completion_record->driver_attempt_no == request_id.attempt_no();
}

void ResultTracker::LogAndTraceAndRespondSuccess(RpcContext* context,
                                                 const Message& msg) {
  InboundCall* call = context->call_;
  VLOG(1) << this << " " << call->remote_method().service_name() << ": Sending RPC success "
      "response for " << call->ToString() << ":" << std::endl << msg.DebugString();
  TRACE_EVENT_ASYNC_END2("rpc_call", "RPC", this,
                         "response", pb_util::PbTracer::TracePb(msg),
                         "trace", context->trace()->DumpToString());
  call->RespondSuccess(msg);
  delete context;
}

void ResultTracker::LogAndTraceFailure(RpcContext* context,
                                       const Message& msg) {
  InboundCall* call = context->call_;
  VLOG(1) << this << " " << call->remote_method().service_name() << ": Sending RPC failure "
      "response for " << call->ToString() << ": " << msg.DebugString();
  TRACE_EVENT_ASYNC_END2("rpc_call", "RPC", this,
                         "response", pb_util::PbTracer::TracePb(msg),
                         "trace", context->trace()->DumpToString());
}

void ResultTracker::LogAndTraceFailure(RpcContext* context,
                                       ErrorStatusPB_RpcErrorCodePB err,
                                       const Status& status) {
  InboundCall* call = context->call_;
  VLOG(1) << this << " " << call->remote_method().service_name() << ": Sending RPC failure "
      "response for " << call->ToString() << ": " << status.ToString();
  TRACE_EVENT_ASYNC_END2("rpc_call", "RPC", this,
                         "status", status.ToString(),
                         "trace", context->trace()->DumpToString());
}

ResultTracker::CompletionRecord* ResultTracker::FindCompletionRecordOrDieUnlocked(
    const RequestIdPB& request_id) {
  ClientState* client_state = DCHECK_NOTNULL(FindPointeeOrNull(clients_, request_id.client_id()));
  return DCHECK_NOTNULL(FindPointeeOrNull(client_state->completion_records, request_id.seq_no()));
}

pair<ResultTracker::ClientState*, ResultTracker::CompletionRecord*>
ResultTracker::FindClientStateAndCompletionRecordOrNullUnlocked(const RequestIdPB& request_id) {
  ClientState* client_state = FindPointeeOrNull(clients_, request_id.client_id());
  CompletionRecord* completion_record = nullptr;
  if (client_state != nullptr) {
    completion_record = FindPointeeOrNull(client_state->completion_records, request_id.seq_no());
  }
  return make_pair(client_state, completion_record);
}

ResultTracker::CompletionRecord*
ResultTracker::FindCompletionRecordOrNullUnlocked(const RequestIdPB& request_id) {
  return FindClientStateAndCompletionRecordOrNullUnlocked(request_id).second;
}

void ResultTracker::RecordCompletionAndRespond(
    const RequestIdPB& request_id,
    const Message* response) {
  lock_guard<simple_spinlock> l(lock_);

  CompletionRecord* completion_record = FindCompletionRecordOrDieUnlocked(request_id);

  CHECK_EQ(completion_record->driver_attempt_no, request_id.attempt_no())
    << "Called RecordCompletionAndRespond() from an executor identified with an attempt number that"
    << " was not marked as the driver for the RPC.";
  DCHECK_EQ(completion_record->state, RpcState::IN_PROGRESS);
  completion_record->response.reset(DCHECK_NOTNULL(response)->New());
  completion_record->response->CopyFrom(*response);
  completion_record->state = RpcState::COMPLETED;

  CHECK_EQ(completion_record->driver_attempt_no, request_id.attempt_no());

  int64_t handler_attempt_no = request_id.attempt_no();

  // Go through the ongoing RPCs and reply to each one.
  for (auto orpc_iter = completion_record->ongoing_rpcs.rbegin();
       orpc_iter != completion_record->ongoing_rpcs.rend();) {

    const OnGoingRpcInfo& ongoing_rpc = *orpc_iter;
    if (MustHandleRpc(handler_attempt_no, completion_record, ongoing_rpc)) {
      if (ongoing_rpc.context != nullptr) {
        if (PREDICT_FALSE(ongoing_rpc.response != response)) {
          ongoing_rpc.response->CopyFrom(*completion_record->response);
        }
        LogAndTraceAndRespondSuccess(ongoing_rpc.context, *ongoing_rpc.response);
      }
      ++orpc_iter;
      orpc_iter = std::vector<OnGoingRpcInfo>::reverse_iterator(
          completion_record->ongoing_rpcs.erase(orpc_iter.base()));
    } else {
      ++orpc_iter;
    }
  }
}

void ResultTracker::FailAndRespondInternal(const RequestIdPB& request_id,
                                           HandleOngoingRpcFunc func) {
  lock_guard<simple_spinlock> l(lock_);
  auto state_and_record = FindClientStateAndCompletionRecordOrNullUnlocked(request_id);
  CompletionRecord* completion_record = state_and_record.second;

  if (completion_record == nullptr) {
    return;
  }

  int64_t seq_no = request_id.seq_no();
  int64_t handler_attempt_no = request_id.attempt_no();

  // If we're copying from a client originated response we need to take care to reply
  // to that call last, otherwise we'll lose 'response', before we go through all the
  // CompletionRecords.
  for (auto orpc_iter = completion_record->ongoing_rpcs.rbegin();
       orpc_iter != completion_record->ongoing_rpcs.rend();) {

    const OnGoingRpcInfo& ongoing_rpc = *orpc_iter;
    if (MustHandleRpc(handler_attempt_no, completion_record, ongoing_rpc)) {
      if (ongoing_rpc.context != nullptr) {
        func(ongoing_rpc);
        delete ongoing_rpc.context;
      }
      ++orpc_iter;
      orpc_iter = std::vector<OnGoingRpcInfo>::reverse_iterator(
          completion_record->ongoing_rpcs.erase(orpc_iter.base()));
    } else {
      ++orpc_iter;
    }
  }

  // If we're the last ones trying this and the state is not completed,
  // delete the completion record.
  if (completion_record->ongoing_rpcs.size() == 0
      && completion_record->state != RpcState::COMPLETED) {
    unique_ptr<CompletionRecord> completion_record =
        EraseKeyReturnValuePtr(&state_and_record.first->completion_records, seq_no);
  }
}

void ResultTracker::FailAndRespond(const RequestIdPB& request_id, Message* response) {
  auto func = [&](const OnGoingRpcInfo& ongoing_rpc) {
    // In the common case RPCs are just executed once so, in that case, avoid an extra
    // copy of the response.
    if (PREDICT_FALSE(ongoing_rpc.response != response)) {
      ongoing_rpc.response->CopyFrom(*response);
    }
    LogAndTraceFailure(ongoing_rpc.context, *response);
    ongoing_rpc.context->call_->RespondSuccess(*response);
  };
  FailAndRespondInternal(request_id, func);
}

void ResultTracker::FailAndRespond(const RequestIdPB& request_id,
                                   ErrorStatusPB_RpcErrorCodePB err, const Status& status) {
  auto func = [&](const OnGoingRpcInfo& ongoing_rpc) {
    LogAndTraceFailure(ongoing_rpc.context, err, status);
    ongoing_rpc.context->call_->RespondFailure(err, status);
  };
  FailAndRespondInternal(request_id, func);
}

void ResultTracker::FailAndRespond(const RequestIdPB& request_id,
                                   int error_ext_id, const string& message,
                                   const Message& app_error_pb) {
  auto func = [&](const OnGoingRpcInfo& ongoing_rpc) {
    LogAndTraceFailure(ongoing_rpc.context, app_error_pb);
    ongoing_rpc.context->call_->RespondApplicationError(error_ext_id, message, app_error_pb);
  };
  FailAndRespondInternal(request_id, func);
}

} // namespace rpc
} // namespace kudu
