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
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <google/protobuf/message.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/malloc.h"
#include "kudu/util/mem_tracker.h"
#include "kudu/util/monotime.h"

namespace kudu {

class Status;
class Thread;

namespace rpc {
class RpcContext;

// A ResultTracker for RPC results.
//
// The ResultTracker is responsible for tracking the results of RPCs and making sure that
// client calls with the same client ID and sequence number (first attempt and subsequent retries)
// are executed exactly once.
//
// In most cases, the use of ResultTracker is internal to the RPC system: RPCs are tracked when
// they first arrive, before service methods are called, and calls to ResultTracker to store
// responses are performed internally by RpcContext. The exception is when an RPC is replicated
// across multiple servers, such as with writes, in which case direct interaction with the result
// tracker is required so as to cache responses on replicas which did not receive the RPC directly
// from the client.
//
// Throughout this header and elsewhere we use the following terms:
//
// RPC - The operation that a client or another server wants to execute on this server. The client
//       might attempt one RPC many times, for instance if failures or timeouts happen.
// Attempt - Each individual attempt of an RPC on the server.
// Handler - A thread executing an attempt. Usually there is only one handler that executes the
//           first attempt of an RPC and, when it completes, replies to its own attempt and to all
//           other attempts that might have arrived after it started.
// Driver - Only important in cases where there might be multiple handlers (e.g. in replicated
//          RPCs). In these cases there might be two handlers executing the same RPC, corresponding
//          to different attempts. Since the RPC must be executed exactly once, only one of the
//          handlers must be selected as the "driver" and actually perform the operation.
//
// If a client wishes to track the result of a given RPC it must send on the RPC header
// a RequestId with the following information:
//
//       Client ID - Uniquely identifies a single client. All the RPCs originating from the same
//                   client must have the same ID.
// Sequence number - Uniquely identifies a single RPC, even across retries to multiple servers, for
//                   replicated RPCs. All retries of the same RPC must have the same sequence
//                   number.
//  Attempt number - Uniquely identifies each retry of the same RPC. All retries of the same RPC
//                   must have different attempt numbers.
//
// When a call first arrives from the client the RPC subsystem will call TrackRpc() which
// will return the state of the RPC in the form of an RpcState enum.
//
// If the ResultTracker returns NEW, this signals that it's the first time the server has heard
// of the RPC and that the corresponding server function should be executed.
//
// If anything other than NEW is returned it means that the call has either previously completed or
// is in the process of being executed. In this case the caller should _not_ execute the function
// corresponding to the RPC. The ResultTracker itself will take care of responding to the client
// appropriately. If the RPC was already completed, the ResultTracker replies to the client
// immediately. If the RPC is still ongoing, the attempt gets "attached" to the ongoing one and will
// receive the same response when its handler finishes.
//
// If handling of the RPC is successful, RecordCompletionAndRespond() must be called
// to register successful completion, in which case all pending or future RPCs with the same
// sequence number, from the same client, will receive the same response.
//
// On the other hand, if execution of the server function is not successful then one of
// the FailAndRespond() methods should be called, causing all _pending_ attempts to receive the same
// error. However this error is not stored, any future attempt with the same sequence number and
// same client ID will be given a new chance to execute, as if it it had never been tried before.
// This gives the client a chance to either retry (if the failure reason is transient) or give up.
//
// ============================================================================
// RPCs with multiple handlers
// ============================================================================
//
// Some RPCs results are tracked by single server, i.e. they correspond to the modification of an
// unreplicated resource and are unpersisted. For those no additional care needs to be taken, the
// first attempt will be the only handler, and subsequent attempts will receive the response when
// that first attempt is done.
// However some RPCs are replicated across servers, using consensus, and thus can have multiple
// handlers executing different attempts at the same time, e.g. one handler from a client
// originating retry, and one from a previous leader originating update.
//
// In this case we need to make sure that the following invariants are enforced:
// - Only one handler can actually record a response, the "driver" handler.
// - Only one handler must respond to "attached" attempts.
// - Each handler replies to their own RPCs, to avoid races. That is, a live handler should
//   not mutate another live handler's response/context.
//
// This is achieved by naming one handler the "driver" of the RPC and making sure that only
// the driver can successfully complete it, i.e. call RecordCompletionAndRespond().
//
// In order to make sure there is only one driver, there must be an _external_ serialization
// point, before the final response is produced, after which only one of the handlers will
// be marked as the driver. For instance, for writes, this serialization point is in
// OpDriver, in a synchronized block where a logic such as this one happens (here
// in pseudo-ish code):
//
// {
//   lock_guard<simple_spinlock> l(lock_);
//   if (follower_op) {
//     result_tracker_->TrackRpcOrChangeDriver(request_id);
//     continue_with_op();
//   } else if (client_op) {
//     bool is_still_driver = result_tracker_->IsCurrentDriver(request_id);
//     if (is_still_driver) continue_with_op();
//     else abort_op();
//   }
// }
//
// This class is thread safe.
class ResultTracker : public RefCountedThreadSafe<ResultTracker> {
 public:
  typedef rpc::RequestTracker::SequenceNumber SequenceNumber;
  static const int NO_HANDLER = -1;
  // Enum returned by TrackRpc that reflects the state of the RPC.
  enum RpcState {
    // The RPC is new.
    NEW,
    // The RPC has previously completed and the same response has been sent
    // to the client.
    COMPLETED,
    // The RPC is currently in-progress and, when it completes, the same response
    // will be sent to the client.
    IN_PROGRESS,
    // The RPC's state is stale, meaning it's older than our per-client garbage
    // collection watermark and we do not recall the original response.
    STALE
  };

  explicit ResultTracker(std::shared_ptr<kudu::MemTracker> mem_tracker);
  ~ResultTracker();

  // Tracks the RPC and returns its current state.
  //
  // If the RpcState == NEW the caller is supposed to actually start executing the RPC.
  // The caller still owns the passed 'response' and 'context'.
  //
  // If the RpcState is anything else all remaining actions will be taken care of internally,
  // i.e. the caller no longer needs to execute the RPC and this takes ownership of the passed
  // 'response' and 'context'.
  RpcState TrackRpc(const RequestIdPB& request_id,
                    google::protobuf::Message* response,
                    RpcContext* context);

  // Used to track RPC attempts which originate from other replicas, and which may race with
  // client originated ones.
  // Tracks the RPC if it is untracked or changes the current driver of this RPC, i.e. sets the
  // attempt number in 'request_id' as the driver of the RPC, if it is tracked and IN_PROGRESS.
  RpcState TrackRpcOrChangeDriver(const RequestIdPB& request_id);

  // Checks if the attempt at an RPC identified by 'request_id' is the current driver of the
  // RPC. That is, if the attempt number in 'request_id' corresponds to the attempt marked
  // as the driver of this RPC, either by initially getting NEW from TrackRpc() or by
  // explicit driver change with ChangeDriver().
  bool IsCurrentDriver(const RequestIdPB& request_id);

  // Records the completion of sucessful operation.
  // This will respond to all RPCs from the same client with the same sequence_number.
  // The response will be stored so that any future retries of this RPC get the same response.
  //
  // Requires that TrackRpc() was called before with the same 'client_id' and
  // 'sequence_number'.
  // Requires that the attempt indentified by 'request_id' is the current driver
  // of the RPC.
  void RecordCompletionAndRespond(const RequestIdPB& request_id,
                                  const google::protobuf::Message* response);

  // Responds to all RPCs identified by 'client_id' and 'sequence_number' with the same response,
  // but doesn't actually store the response.
  // This should be called when the RPC failed validation or if some transient error occurred.
  // Based on the response the client can then decide whether to retry the RPC (which will
  // be treated as a new one) or to give up.
  //
  // Requires that TrackRpc() was called before with the same 'client_id' and
  // 'sequence_number'.
  // Requires that the attempt indentified by 'request_id' is the current driver
  // of the RPC.
  void FailAndRespond(const RequestIdPB& request_id,
                      google::protobuf::Message* response);

  // Overload to match other types of RpcContext::Respond*Failure()
  void FailAndRespond(const RequestIdPB& request_id,
                      ErrorStatusPB_RpcErrorCodePB err, const Status& status);

  // Overload to match other types of RpcContext::Respond*Failure()
  void FailAndRespond(const RequestIdPB& request_id,
                      int error_ext_id, const std::string& message,
                      const google::protobuf::Message& app_error_pb);

  // Start a background thread which periodically runs GCResults().
  // This thread is automatically stopped in the destructor.
  //
  // Must be called at most once.
  void StartGCThread();

  // Runs time-based garbage collection on the results this result tracker is caching.
  // When garbage collection runs, it goes through all ClientStates and:
  // - If a ClientState is older than the 'remember_clients_ttl_ms' flag and no
  //   requests are in progress, GCs the ClientState and all its CompletionRecords.
  // - If a ClientState is newer than the 'remember_clients_ttl_ms' flag, goes
  //   through all CompletionRecords and:
  //   - If the CompletionRecord is older than the 'remember_responses_ttl_secs' flag,
  //     GCs the CompletionRecord and advances the 'stale_before_seq_no' watermark.
  //
  // Typically this is invoked from an internal thread started by 'StartGCThread()'.
  void GCResults();

  std::string ToString();

 private:
  // Information about client originated ongoing RPCs.
  // The lifecycle of 'response' and 'context' is managed by the RPC layer.
  struct OnGoingRpcInfo {
    google::protobuf::Message* response;
    RpcContext* context;
    int64_t handler_attempt_no;

    std::string ToString() const;
  };
  // A completion record for an IN_PROGRESS or COMPLETED RPC.
  struct CompletionRecord {
    CompletionRecord(RpcState state, int64_t driver_attempt_no)
        : state(state),
          driver_attempt_no(driver_attempt_no),
          last_updated(MonoTime::Now()) {
    }

    // The current state of the RPC.
    RpcState state;

    // The attempt number that is/was "driving" this RPC.
    int64_t driver_attempt_no;

    // The timestamp of the last CompletionRecord update.
    MonoTime last_updated;

    // The cached response, if this RPC is in COMPLETED state.
    std::unique_ptr<google::protobuf::Message> response;

    // The set of ongoing RPCs that correspond to this record.
    std::vector<OnGoingRpcInfo> ongoing_rpcs;

    std::string ToString() const;

    // Calculates the memory footprint of this struct.
    int64_t memory_footprint() const {
      return kudu_malloc_usable_size(this)
          + (ongoing_rpcs.capacity() > 0 ? kudu_malloc_usable_size(ongoing_rpcs.data()) : 0)
          + (response.get() != nullptr ? response->SpaceUsed() : 0);
    }
  };

  // The state corresponding to a single client.
  struct ClientState {
    typedef MemTrackerAllocator<
        std::pair<const SequenceNumber,
                  std::unique_ptr<CompletionRecord>>> CompletionRecordMapAllocator;
    typedef std::map<SequenceNumber,
                     std::unique_ptr<CompletionRecord>,
                     std::less<SequenceNumber>,
                     CompletionRecordMapAllocator> CompletionRecordMap;

    explicit ClientState(std::shared_ptr<MemTracker> mem_tracker)
        : stale_before_seq_no(0),
          completion_records(CompletionRecordMap::key_compare(),
                             CompletionRecordMapAllocator(std::move(mem_tracker))) {}

    // The last time we've heard from this client.
    MonoTime last_heard_from;

    // The sequence number of the first response we remember for this client.
    // All sequence numbers before this one are considered STALE.
    SequenceNumber stale_before_seq_no;

    // The (un gc'd) CompletionRecords for this client.
    CompletionRecordMap completion_records;

    // Garbage collects this client's CompletionRecords for which MustGcRecordFunc returns
    // true. We use a lambda here so that we can have a single method that GCs and releases
    // the memory for CompletionRecords based on different policies.
    //
    // 'func' should have the following signature:
    //   bool MyFunction(SequenceNumber seq_no, CompletionRecord* record);
    //
    template<class MustGcRecordFunc>
    void GCCompletionRecords(const std::shared_ptr<kudu::MemTracker>& mem_tracker,
                             MustGcRecordFunc func);

    std::string ToString() const;

    // Calculates the memory footprint of this struct.
    // This calculation is shallow and doesn't account for the memory the nested data
    // structures occupy.
    int64_t memory_footprint() const {
      return kudu_malloc_usable_size(this);
    }
  };

  RpcState TrackRpcUnlocked(const RequestIdPB& request_id,
                            google::protobuf::Message* response,
                            RpcContext* context);

  typedef std::function<void (const OnGoingRpcInfo&)> HandleOngoingRpcFunc;

  // Helper method to handle the multiple overloads of FailAndRespond. Takes a lambda
  // that knows what to do with OnGoingRpcInfo in each individual case.
  void FailAndRespondInternal(const rpc::RequestIdPB& request_id,
                              const HandleOngoingRpcFunc& func);

  CompletionRecord* FindCompletionRecordOrNullUnlocked(const RequestIdPB& request_id);
  CompletionRecord* FindCompletionRecordOrDieUnlocked(const RequestIdPB& request_id);
  std::pair<ClientState*, CompletionRecord*> FindClientStateAndCompletionRecordOrNullUnlocked(
      const RequestIdPB& request_id);

  // A handler must handle an RPC attempt if:
  // 1 - It's its own attempt. I.e. it has the same attempt number of the handler.
  // 2 - It's the driver of the RPC and the attempt has no handler (was attached).
  bool MustHandleRpc(int64_t handler_attempt_no,
                     CompletionRecord* completion_record,
                     const OnGoingRpcInfo& ongoing_rpc) {
    if (PREDICT_TRUE(ongoing_rpc.handler_attempt_no == handler_attempt_no)) {
      return true;
    }
    if (completion_record->driver_attempt_no == handler_attempt_no) {
      return ongoing_rpc.handler_attempt_no == NO_HANDLER;
    }
    return false;
  }

  void LogAndTraceAndRespondSuccess(RpcContext* context, const google::protobuf::Message& msg);
  void LogAndTraceFailure(RpcContext* context, const google::protobuf::Message& msg);
  void LogAndTraceFailure(RpcContext* context, ErrorStatusPB_RpcErrorCodePB err,
                          const Status& status);

  std::string ToStringUnlocked() const;

  void RunGCThread();

  // The memory tracker that tracks this ResultTracker's memory consumption.
  std::shared_ptr<kudu::MemTracker> mem_tracker_;

  // Lock that protects access to 'clients_' and to the state contained in each
  // ClientState.
  // TODO consider a per-ClientState lock if we find this too coarse grained.
  simple_spinlock lock_;

  typedef MemTrackerAllocator<std::pair<const std::string,
                                        std::unique_ptr<ClientState>>> ClientStateMapAllocator;
  typedef std::map<std::string,
                   std::unique_ptr<ClientState>,
                   std::less<std::string>,
                   ClientStateMapAllocator> ClientStateMap;

  ClientStateMap clients_;

  // The thread which runs GC, and a latch to stop it.
  scoped_refptr<Thread> gc_thread_;
  CountDownLatch gc_thread_stop_latch_;

  DISALLOW_COPY_AND_ASSIGN(ResultTracker);
};

} // namespace rpc
} // namespace kudu
