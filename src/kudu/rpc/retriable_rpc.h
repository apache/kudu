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

#include <memory>
#include <string>

#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/request_tracker.h"
#include "kudu/rpc/rpc.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/monotime.h"

namespace kudu {
namespace rpc {

namespace internal {
typedef rpc::RequestTracker::SequenceNumber SequenceNumber;
}

// A base class for retriable RPCs that handles replica picking and retry logic.
//
// The 'Server' template parameter refers to the type of the server that will be looked up
// and passed to the derived classes on Try(). For instance in the case of WriteRpc it's
// RemoteTabletServer.
//
// TODO(unknown): merge RpcRetrier into this class? Can't be done right now as the retrier is used
// independently elsewhere, but likely possible when all replicated RPCs have a ReplicaPicker.
//
// TODO(unknown): allow to target replicas other than the leader, if needed.
//
// TODO(unknown): once we have retry handling on all the RPCs merge this with rpc::Rpc.
template <class Server, class RequestPB, class ResponsePB>
class RetriableRpc : public Rpc {
 public:
  RetriableRpc(const scoped_refptr<ServerPicker<Server>>& server_picker,
               const scoped_refptr<RequestTracker>& request_tracker,
               const MonoTime& deadline,
               std::shared_ptr<Messenger> messenger)
      : Rpc(deadline, std::move(messenger)),
        server_picker_(server_picker),
        request_tracker_(request_tracker),
        sequence_number_(RequestTracker::kNoSeqNo),
        num_attempts_(0) {}

  virtual ~RetriableRpc() {
    DCHECK_EQ(sequence_number_, RequestTracker::kNoSeqNo);
  }

  // Performs server lookup/initialization.
  // If/when the server is looked up and initialized successfully RetriableRpc will call
  // Try() to actually send the request.
  void SendRpc() override;

  // The callback to call upon retrieving (of failing to retrieve) a new authn
  // token. This is the callback that subclasses should call in their custom
  // implementation of the GetNewAuthnTokenAndRetry() method.
  void GetNewAuthnTokenAndRetryCb(const Status& status);

 protected:
  // Subclasses implement this method to actually try the RPC.
  // The server been looked up and is ready to be used.
  virtual void Try(Server* replica, const ResponseCallback& callback) = 0;

  // Subclasses implement this method to analyze 'status', the controller status or
  // the response and return a RetriableRpcStatus which will then be used
  // to decide how to proceed (retry or give up).
  virtual RetriableRpcStatus AnalyzeResponse(const Status& status) = 0;

  // Subclasses implement this method to perform cleanup and/or final steps.
  // After this is called the RPC will be no longer retried.
  virtual void Finish(const Status& status) = 0;

  // Returns 'true' if the RPC is to scheduled for retry with a new authn token,
  // 'false' otherwise. For RPCs performed in the context of providing token
  // for authentication it's necessary to implement this method. The default
  // implementation returns 'false' meaning the calls returning
  // INVALID_AUTHENTICATION_TOKEN RPC status are not retried.
  virtual bool GetNewAuthnTokenAndRetry() {
    return false;
  }

  // Request body.
  RequestPB req_;

  // Response body.
  ResponsePB resp_;

 private:
  friend class CalculatorServiceRpc;

  // Decides whether to retry the RPC, based on the result of AnalyzeResponse()
  // and retries if that is the case.
  // Returns true if the RPC was retried or false otherwise.
  bool RetryIfNeeded(const RetriableRpcStatus& result, Server* server);

  // Called when the replica has been looked up.
  void ReplicaFoundCb(const Status& status, Server* server);

  // Called after the RPC was performed.
  void SendRpcCb(const Status& status) override;

  // Performs final cleanup, after the RPC is done (independently of success).
  void FinishInternal();

  scoped_refptr<ServerPicker<Server>> server_picker_;
  scoped_refptr<RequestTracker> request_tracker_;
  std::shared_ptr<Messenger> messenger_;

  // The sequence number for this RPC.
  internal::SequenceNumber sequence_number_;

  // The number of times this RPC has been attempted
  int32 num_attempts_;

  // Keeps track of the replica the RPCs were sent to.
  // TODO Remove this and pass the used replica around. For now we need to keep this as
  // the retrier calls the SendRpcCb directly and doesn't know the replica that was
  // being written to.
  Server* current_;
};

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::SendRpc()  {
  if (sequence_number_ == RequestTracker::kNoSeqNo) {
    CHECK_OK(request_tracker_->NewSeqNo(&sequence_number_));
  }
  server_picker_->PickLeader(Bind(&RetriableRpc::ReplicaFoundCb,
                                  Unretained(this)),
                             retrier().deadline());
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::GetNewAuthnTokenAndRetryCb(
    const Status& status) {
  if (status.ok()) {
    // Perform the RPC call with the newly fetched authn token.
    mutable_retrier()->mutable_controller()->Reset();
    SendRpc();
  } else {
    // Back to the retry sequence, hoping for better conditions after some time.
    VLOG(1) << "Failed to get new authn token: " << status.ToString();
    mutable_retrier()->DelayedRetry(this, status);
  }
}

template <class Server, class RequestPB, class ResponsePB>
bool RetriableRpc<Server, RequestPB, ResponsePB>::RetryIfNeeded(
    const RetriableRpcStatus& result, Server* server) {
  // Handle the cases where we retry.
  switch (result.result) {
    case RetriableRpcStatus::SERVICE_UNAVAILABLE:
      // For writes, always retry the request on the same server in case of the
      // SERVICE_UNAVAILABLE error.
      break;

    case RetriableRpcStatus::SERVER_NOT_ACCESSIBLE:
      // TODO(KUDU-1745): not checking for null here results in a crash, since in the case
      // of a failed master lookup we have no tablet server corresponding to the error.
      //
      // But, with the null check, we end up with a relatively tight retry loop
      // in this scenario whereas we should be backing off. Need to improve
      // test coverage here to understand why the back-off is not taking effect.
      if (server != nullptr) {
        VLOG(1) << "Failing " << ToString() << " to a new target: " << result.status.ToString();
        // Mark the server as failed. As for details on the only existing
        // implementation of ServerPicker::MarkServerFailed(), see the note on
        // the MetaCacheServerPicker::MarkServerFailed() method.
        server_picker_->MarkServerFailed(server, result.status);
      }
      break;

    case RetriableRpcStatus::RESOURCE_NOT_FOUND:
      // The TabletServer was not part of the config serving the tablet.
      // We mark our tablet cache as stale, forcing a master lookup on the
      // next attempt.
      //
      // TODO(KUDU-1314): Don't backoff the first time we hit this error.
      server_picker_->MarkResourceNotFound(server);
      break;

    case RetriableRpcStatus::REPLICA_NOT_LEADER:
      // The TabletServer was not the leader of the quorum.
      server_picker_->MarkReplicaNotLeader(server);
      break;

    case RetriableRpcStatus::INVALID_AUTHENTICATION_TOKEN: {
      // This is a special case for retry: first it's necessary to get a new
      // authn token and then retry the operation with the new token.
      if (GetNewAuthnTokenAndRetry()) {
        // The RPC will be retried.
        resp_.Clear();
        return true;
      }
      // Do not retry.
      return false;
    }

    case RetriableRpcStatus::NON_RETRIABLE_ERROR:
      if (server != nullptr && result.status.IsTimedOut()) {
        // For the NON_RETRIABLE_ERROR result in case of TimedOut status,
        // mark the server as failed. As for details on the only existing
        // implementation of ServerPicker::MarkServerFailed(), see the note on
        // the MetaCacheServerPicker::MarkServerFailed() method.
        VLOG(1) << "Failing " << ToString() << " to a new target: " << result.status.ToString();
        server_picker_->MarkServerFailed(server, result.status);
      }
      // Do not retry in the case of non-retriable error.
      return false;

    default:
      // For the OK case we should not retry.
      DCHECK(result.result == RetriableRpcStatus::OK);
      return false;
  }
  resp_.Clear();
  current_ = nullptr;
  mutable_retrier()->DelayedRetry(this, result.status);
  return true;
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::FinishInternal() {
  // Mark the RPC as completed and set the sequence number to kNoSeqNo to make
  // sure we're in the appropriate state before destruction.
  request_tracker_->RpcCompleted(sequence_number_);
  sequence_number_ = RequestTracker::kNoSeqNo;
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::ReplicaFoundCb(const Status& status,
                                                                 Server* server) {
  // NOTE: 'server' here may be nullptr in the case that status is not OK!
  RetriableRpcStatus result = AnalyzeResponse(status);
  if (RetryIfNeeded(result, server)) return;

  if (result.result == RetriableRpcStatus::NON_RETRIABLE_ERROR) {
    FinishInternal();
    Finish(result.status);
    return;
  }

  // We successfully found a replica, so prepare the RequestIdPB before we send out the call.
  std::unique_ptr<RequestIdPB> request_id(new RequestIdPB());
  request_id->set_client_id(request_tracker_->client_id());
  request_id->set_seq_no(sequence_number_);
  request_id->set_first_incomplete_seq_no(request_tracker_->FirstIncomplete());
  request_id->set_attempt_no(num_attempts_++);

  mutable_retrier()->mutable_controller()->SetRequestIdPB(std::move(request_id));

  DCHECK_EQ(result.result, RetriableRpcStatus::OK);
  current_ = server;
  Try(server, boost::bind(&RetriableRpc::SendRpcCb, this, Status::OK()));
}

template <class Server, class RequestPB, class ResponsePB>
void RetriableRpc<Server, RequestPB, ResponsePB>::SendRpcCb(const Status& status) {
  RetriableRpcStatus result = AnalyzeResponse(status);
  if (RetryIfNeeded(result, current_)) return;

  FinishInternal();

  // From here on out the RPC has either succeeded of suffered a non-retriable
  // failure.
  Status final_status = result.status;
  if (!final_status.ok()) {
    std::string error_string;
    if (current_) {
      error_string = strings::Substitute("Failed to write to server: $0", current_->ToString());
    } else {
      error_string = "Failed to write to server: (no server available)";
    }
    final_status = final_status.CloneAndPrepend(error_string);
  }
  Finish(final_status);
}

} // namespace rpc
} // namespace kudu
