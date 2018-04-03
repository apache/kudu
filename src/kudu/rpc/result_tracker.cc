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

#include <algorithm>
#include <mutex>
#include <ostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/inbound_call.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace.h"
// IWYU pragma: no_include <deque>

DEFINE_int64(remember_clients_ttl_ms, 3600 * 1000 /* 1 hour */,
    "Maximum amount of time, in milliseconds, the server \"remembers\" a client for the "
    "purpose of caching its responses. After this period without hearing from it, the "
    "client is no longer remembered and the memory occupied by its responses is reclaimed. "
    "Retries of requests older than 'remember_clients_ttl_ms' are treated as new "
    "ones.");
TAG_FLAG(remember_clients_ttl_ms, advanced);

DEFINE_int64(remember_responses_ttl_ms, 600 * 1000 /* 10 mins */,
    "Maximum amount of time, in milliseconds, the server \"remembers\" a response to a "
    "specific request for a client. After this period has elapsed, the response may have "
    "been garbage collected and the client might get a response indicating the request is "
    "STALE.");
TAG_FLAG(remember_responses_ttl_ms, advanced);

DEFINE_int64(result_tracker_gc_interval_ms, 1000,
    "Interval at which the result tracker will look for entries to GC.");
TAG_FLAG(result_tracker_gc_interval_ms, hidden);

namespace kudu {
namespace rpc {

using google::protobuf::Message;
using kudu::MemTracker;
using kudu::pb_util::SecureDebugString;
using kudu::pb_util::SecureShortDebugString;
using rpc::InboundCall;
using std::make_pair;
using std::move;
using std::lock_guard;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;
using strings::SubstituteAndAppend;

// This tracks the size changes of anything that has a memory_footprint() method.
// It must be instantiated before the updates, and it makes sure that the MemTracker
// is updated on scope exit.
template <class T>
struct ScopedMemTrackerUpdater {
  ScopedMemTrackerUpdater(MemTracker* tracker_, const T* tracked_)
      : tracker(tracker_),
        tracked(tracked_),
        memory_before(tracked->memory_footprint()),
        cancelled(false) {
  }

  ~ScopedMemTrackerUpdater() {
    if (cancelled) return;
    tracker->Release(memory_before - tracked->memory_footprint());
  }

  void Cancel() {
    cancelled = true;
  }

  MemTracker* tracker;
  const T* tracked;
  int64_t memory_before;
  bool cancelled;
};

ResultTracker::ResultTracker(shared_ptr<MemTracker> mem_tracker)
    : mem_tracker_(std::move(mem_tracker)),
      clients_(ClientStateMap::key_compare(),
               ClientStateMapAllocator(mem_tracker_)),
      gc_thread_stop_latch_(1) {}

ResultTracker::~ResultTracker() {
  if (gc_thread_) {
    gc_thread_stop_latch_.CountDown();
    gc_thread_->Join();
  }

  lock_guard<simple_spinlock> l(lock_);
  // Release all the memory for the stuff we'll delete on destruction.
  for (auto& client_state : clients_) {
    client_state.second->GCCompletionRecords(
        mem_tracker_, [] (SequenceNumber, CompletionRecord*){ return true; });
    mem_tracker_->Release(client_state.second->memory_footprint());
  }
}

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
      [&]{
        unique_ptr<ClientState> client_state(new ClientState(mem_tracker_));
        mem_tracker_->Consume(client_state->memory_footprint());
        client_state->stale_before_seq_no = request_id.first_incomplete_seq_no();
        return client_state;
      })->get();

  client_state->last_heard_from = MonoTime::Now();

  // If the arriving request is older than our per-client GC watermark, report its
  // staleness to the client.
  if (PREDICT_FALSE(request_id.seq_no() < client_state->stale_before_seq_no)) {
    if (context) {
      context->call_->RespondFailure(
          ErrorStatusPB::ERROR_REQUEST_STALE,
          Status::Incomplete(Substitute("Request with id { $0 } is stale.",
                                        SecureShortDebugString(request_id))));
      delete context;
    }
    return RpcState::STALE;
  }

  // GC records according to the client's first incomplete watermark.
  client_state->GCCompletionRecords(
      mem_tracker_,
      [&] (SequenceNumber seq_no, CompletionRecord* completion_record) {
        return completion_record->state != RpcState::IN_PROGRESS &&
            seq_no < request_id.first_incomplete_seq_no();
      });

  auto result = ComputeIfAbsentReturnAbsense(
      &client_state->completion_records,
      request_id.seq_no(),
      [&]{
        unique_ptr<CompletionRecord> completion_record(new CompletionRecord(
            RpcState::IN_PROGRESS, request_id.attempt_no()));
        mem_tracker_->Consume(completion_record->memory_footprint());
        return completion_record;
      });

  CompletionRecord* completion_record = result.first->get();
  ScopedMemTrackerUpdater<CompletionRecord> cr_updater(mem_tracker_.get(), completion_record);

  if (PREDICT_TRUE(result.second)) {
    // When a follower is applying an operation it doesn't have a response yet, and it won't
    // have a context, so only set them if they exist.
    if (response != nullptr) {
      completion_record->ongoing_rpcs.push_back({response,
                                                 DCHECK_NOTNULL(context),
                                                 request_id.attempt_no()});
    }
    return RpcState::NEW;
  }

  completion_record->last_updated = MonoTime::Now();
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
  ScopedMemTrackerUpdater<CompletionRecord> updater(mem_tracker_.get(), completion_record);

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
      "response for " << call->ToString() << ":" << std::endl << SecureDebugString(msg);
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
      "response for " << call->ToString() << ": " << SecureDebugString(msg);
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

void ResultTracker::RecordCompletionAndRespond(const RequestIdPB& request_id,
                                               const Message* response) {
  vector<OnGoingRpcInfo> to_respond;
  {
    lock_guard<simple_spinlock> l(lock_);

    CompletionRecord* completion_record = FindCompletionRecordOrDieUnlocked(request_id);
    ScopedMemTrackerUpdater<CompletionRecord> updater(mem_tracker_.get(), completion_record);

    CHECK_EQ(completion_record->driver_attempt_no, request_id.attempt_no())
        << "Called RecordCompletionAndRespond() from an executor identified with an "
        << "attempt number that was not marked as the driver for the RPC. RequestId: "
        << SecureShortDebugString(request_id) << "\nTracker state:\n " << ToStringUnlocked();
    DCHECK_EQ(completion_record->state, RpcState::IN_PROGRESS);
    completion_record->response.reset(DCHECK_NOTNULL(response)->New());
    completion_record->response->CopyFrom(*response);
    completion_record->state = RpcState::COMPLETED;
    completion_record->last_updated = MonoTime::Now();

    CHECK_EQ(completion_record->driver_attempt_no, request_id.attempt_no());

    int64_t handler_attempt_no = request_id.attempt_no();

    // Go through the ongoing RPCs and reply to each one.
    for (auto orpc_iter = completion_record->ongoing_rpcs.rbegin();
         orpc_iter != completion_record->ongoing_rpcs.rend();) {

      const OnGoingRpcInfo& ongoing_rpc = *orpc_iter;
      if (MustHandleRpc(handler_attempt_no, completion_record, ongoing_rpc)) {
        if (ongoing_rpc.context != nullptr) {
          to_respond.push_back(ongoing_rpc);
        }
        ++orpc_iter;
        orpc_iter = std::vector<OnGoingRpcInfo>::reverse_iterator(
            completion_record->ongoing_rpcs.erase(orpc_iter.base()));
      } else {
        ++orpc_iter;
      }
    }
  }

  // Respond outside of holding the lock. This reduces lock contention and also
  // means that we will have fully updated our memory tracking before responding,
  // which makes testing easier.
  for (auto& ongoing_rpc : to_respond) {
    if (PREDICT_FALSE(ongoing_rpc.response != response)) {
      ongoing_rpc.response->CopyFrom(*response);
    }
    LogAndTraceAndRespondSuccess(ongoing_rpc.context, *ongoing_rpc.response);
  }
}

void ResultTracker::FailAndRespondInternal(const RequestIdPB& request_id,
                                           const HandleOngoingRpcFunc& func) {
  vector<OnGoingRpcInfo> to_handle;
  {
    lock_guard<simple_spinlock> l(lock_);
    auto state_and_record = FindClientStateAndCompletionRecordOrNullUnlocked(request_id);
    if (PREDICT_FALSE(state_and_record.first == nullptr)) {
      LOG(FATAL) << "Couldn't find ClientState for request: " << SecureShortDebugString(request_id)
                 << ". \nTracker state:\n" << ToStringUnlocked();
    }

    CompletionRecord* completion_record = state_and_record.second;

    // It is possible for this method to be called for an RPC that was never actually
    // tracked (though RecordCompletionAndRespond() can't). One such case is when a
    // follower transaction fails on the TransactionManager, for some reason, before it
    // was tracked. The CompletionCallback still calls this method. In this case, do
    // nothing.
    if (completion_record == nullptr) {
      return;
    }

    ScopedMemTrackerUpdater<CompletionRecord> cr_updater(mem_tracker_.get(), completion_record);
    completion_record->last_updated = MonoTime::Now();

    int64_t seq_no = request_id.seq_no();
    int64_t handler_attempt_no = request_id.attempt_no();

    // If we're copying from a client originated response we need to take care to reply
    // to that call last, otherwise we'll lose 'response', before we go through all the
    // CompletionRecords.
    for (auto orpc_iter = completion_record->ongoing_rpcs.rbegin();
         orpc_iter != completion_record->ongoing_rpcs.rend();) {

      const OnGoingRpcInfo& ongoing_rpc = *orpc_iter;
      if (MustHandleRpc(handler_attempt_no, completion_record, ongoing_rpc)) {
        to_handle.push_back(ongoing_rpc);
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
      cr_updater.Cancel();
      unique_ptr<CompletionRecord> completion_record =
          EraseKeyReturnValuePtr(&state_and_record.first->completion_records, seq_no);
      mem_tracker_->Release(completion_record->memory_footprint());
    }
  }

  // Wait until outside the lock to do the heavy-weight work.
  for (auto& ongoing_rpc : to_handle) {
    if (ongoing_rpc.context != nullptr) {
      func(ongoing_rpc);
      delete ongoing_rpc.context;
    }
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

void ResultTracker::StartGCThread() {
  CHECK(!gc_thread_);
  CHECK_OK(Thread::Create("server", "result-tracker", &ResultTracker::RunGCThread,
                          this, &gc_thread_));
}

void ResultTracker::RunGCThread() {
  while (!gc_thread_stop_latch_.WaitFor(MonoDelta::FromMilliseconds(
             FLAGS_result_tracker_gc_interval_ms))) {
    GCResults();
  }
}

void ResultTracker::GCResults() {
  lock_guard<simple_spinlock> l(lock_);
  MonoTime now = MonoTime::Now();
  // Calculate the instants before which we'll start GCing ClientStates and CompletionRecords.
  MonoTime time_to_gc_clients_from = now;
  time_to_gc_clients_from.AddDelta(
      MonoDelta::FromMilliseconds(-FLAGS_remember_clients_ttl_ms));
  MonoTime time_to_gc_responses_from = now;
  time_to_gc_responses_from.AddDelta(
      MonoDelta::FromMilliseconds(-FLAGS_remember_responses_ttl_ms));

  // Now go through the ClientStates. If we haven't heard from a client in a while
  // GC it and all its completion records (making sure there isn't actually one in progress first).
  // If we've heard from a client recently, but some of its responses are old, GC those responses.
  for (auto iter = clients_.begin(); iter != clients_.end();) {
    auto& client_state = iter->second;
    if (client_state->last_heard_from < time_to_gc_clients_from) {
      // Client should be GCed.
      bool ongoing_request = false;
      client_state->GCCompletionRecords(
          mem_tracker_,
          [&] (SequenceNumber, CompletionRecord* completion_record) {
            if (PREDICT_FALSE(completion_record->state == RpcState::IN_PROGRESS)) {
              ongoing_request = true;
              return false;
            }
            return true;
          });
      // Don't delete the client state if there is still a request in execution.
      if (PREDICT_FALSE(ongoing_request)) {
        ++iter;
        continue;
      }
      mem_tracker_->Release(client_state->memory_footprint());
      iter = clients_.erase(iter);
    } else {
      // Client can't be GCed, but its calls might be GCable.
      iter->second->GCCompletionRecords(
          mem_tracker_,
          [&] (SequenceNumber, CompletionRecord* completion_record) {
            return completion_record->state != RpcState::IN_PROGRESS &&
                completion_record->last_updated < time_to_gc_responses_from;
          });
      ++iter;
    }
  }
}

string ResultTracker::ToString() {
  lock_guard<simple_spinlock> l(lock_);
  return ToStringUnlocked();
}

string ResultTracker::ToStringUnlocked() const {
  string result = Substitute("ResultTracker[this: $0, Num. Client States: $1, Client States:\n",
                             this, clients_.size());
  for (auto& cs : clients_) {
    SubstituteAndAppend(&result, Substitute("\n\tClient: $0, $1", cs.first, cs.second->ToString()));
  }
  result.append("]");
  return result;
}

template<class MustGcRecordFunc>
void ResultTracker::ClientState::GCCompletionRecords(
    const shared_ptr<kudu::MemTracker>& mem_tracker,
    MustGcRecordFunc must_gc_record_func) {
  ScopedMemTrackerUpdater<ClientState> updater(mem_tracker.get(), this);
  for (auto iter = completion_records.begin(); iter != completion_records.end();) {
    if (must_gc_record_func(iter->first, iter->second.get())) {
      mem_tracker->Release(iter->second->memory_footprint());
      SequenceNumber deleted_seq_no = iter->first;
      iter = completion_records.erase(iter);
      // Each time we GC a response, update 'stale_before_seq_no'.
      // This will allow to answer clients that their responses are stale if we get
      // a request with a sequence number lower than or equal to this one.
      stale_before_seq_no = std::max(deleted_seq_no + 1, stale_before_seq_no);
      continue;
    }
    // Since we store completion records in order, if we found one that shouldn't be GCed,
    // don't GC anything after it.
    return;
  }
}

string ResultTracker::ClientState::ToString() const {
  auto since_last_heard =
      MonoTime::Now().GetDeltaSince(last_heard_from);
  string result = Substitute("Client State[Last heard from: $0s ago, "
                             "$1 CompletionRecords:",
                             since_last_heard.ToString(),
                             completion_records.size());
  for (auto& completion_record : completion_records) {
    SubstituteAndAppend(&result, Substitute("\n\tCompletion Record: $0, $1",
                                            completion_record.first,
                                            completion_record.second->ToString()));
  }
  result.append("\t]");
  return result;
}

string ResultTracker::CompletionRecord::ToString() const {
  string result = Substitute("Completion Record[State: $0, Driver: $1, "
                             "Cached response: $2, $3 OngoingRpcs:",
                             state,
                             driver_attempt_no,
                             response ? SecureShortDebugString(*response) : "None",
                             ongoing_rpcs.size());
  for (auto& orpc : ongoing_rpcs) {
    SubstituteAndAppend(&result, Substitute("\n\t$0", orpc.ToString()));
  }
  result.append("\t\t]");
  return result;
}

string ResultTracker::OnGoingRpcInfo::ToString() const {
  return Substitute("OngoingRpc[Handler: $0, Context: $1, Response: $2]",
                    handler_attempt_no, context,
                    response ? SecureShortDebugString(*response) : "NULL");
}

} // namespace rpc
} // namespace kudu
