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

#include "kudu/subprocess/server.h"

#include <csignal>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/util/async_util.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/thread.h"

DEFINE_int32(subprocess_request_queue_size_bytes, 4 * 1024 * 1024,
             "Maximum size in bytes of the outbound request queue. This is best "
             "effort: if a single request is larger than this, it is still "
             "added to the queue");
TAG_FLAG(subprocess_request_queue_size_bytes, advanced);

DEFINE_int32(subprocess_response_queue_size_bytes, 4 * 1024 * 1024,
             "Maximum size in bytes of the inbound response queue. This is best "
             "effort: if a single request is larger than this, it is still "
             "added to the queue");
TAG_FLAG(subprocess_response_queue_size_bytes, advanced);

DEFINE_int32(subprocess_num_responder_threads, 3,
             "Number of threads that will be dedicated to reading responses "
             "from the inbound queue and returning to callers");
TAG_FLAG(subprocess_num_responder_threads, advanced);

DEFINE_int32(subprocess_timeout_secs, 15,
             "Number of seconds a call to the subprocess is allowed to "
             "take before a timeout error is returned to the calling process");
TAG_FLAG(subprocess_timeout_secs, advanced);

DEFINE_int32(subprocess_queue_full_retry_ms, 50,
             "Number of milliseconds between attempts to enqueue the "
             "request to the subprocess");
TAG_FLAG(subprocess_queue_full_retry_ms, runtime);
TAG_FLAG(subprocess_queue_full_retry_ms, advanced);

DEFINE_int32(subprocess_deadline_checking_interval_ms, 50,
             "Interval in milliseconds at which Kudu will check the deadlines "
             "of in-flight calls to the subprocess");
TAG_FLAG(subprocess_deadline_checking_interval_ms, runtime);
TAG_FLAG(subprocess_deadline_checking_interval_ms, advanced);

using std::make_shared;
using std::pair;
using std::shared_ptr;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace subprocess {

SubprocessServer::SubprocessServer(vector<string> subprocess_argv)
    : call_timeout_(MonoDelta::FromSeconds(FLAGS_subprocess_timeout_secs)),
      next_id_(1),
      closing_(false),
      process_(make_shared<Subprocess>(std::move(subprocess_argv))),
      outbound_call_queue_(FLAGS_subprocess_request_queue_size_bytes),
      inbound_response_queue_(FLAGS_subprocess_response_queue_size_bytes) {
  process_->ShareParentStdin(false);
  process_->ShareParentStdout(false);
}

SubprocessServer::~SubprocessServer() {
  Shutdown();
}

Status SubprocessServer::Init() {
  VLOG(2) << "Starting the subprocess";
  RETURN_NOT_OK_PREPEND(process_->Start(), "Failed to start subprocess");

  // Start the message protocol.
  CHECK(!message_protocol_);
  message_protocol_.reset(new SubprocessProtocol(SubprocessProtocol::SerializationMode::PB,
                                                 SubprocessProtocol::CloseMode::CLOSE_ON_DESTROY,
                                                 process_->ReleaseChildStdoutFd(),
                                                 process_->ReleaseChildStdinFd()));
  const int num_threads = FLAGS_subprocess_num_responder_threads;
  responder_threads_.resize(num_threads);
  for (int i = 0; i < num_threads; i++) {
    RETURN_NOT_OK(Thread::Create("subprocess", "responder", &SubprocessServer::ResponderThread,
                                 this, &responder_threads_[i]));
  }
  RETURN_NOT_OK(Thread::Create("subprocess", "reader", &SubprocessServer::ReceiveMessagesThread,
                               this, &read_thread_));
  RETURN_NOT_OK(Thread::Create("subprocess", "writer", &SubprocessServer::SendMessagesThread,
                               this, &write_thread_));
  return Thread::Create("subprocess", "deadline-checker",
                        &SubprocessServer::CheckDeadlinesThread,
                        this, &deadline_checker_);
}

Status SubprocessServer::Execute(SubprocessRequestPB* req,
                                 SubprocessResponsePB* resp) {
  DCHECK(!req->has_id());
  req->set_id(next_id_++);
  Synchronizer sync;
  auto cb = sync.AsStdStatusCallback();
  auto call = make_shared<SubprocessCall>(req, resp, &cb, MonoTime::Now() + call_timeout_);
  RETURN_NOT_OK(QueueCall(call));
  return sync.Wait();
}

void SubprocessServer::Shutdown() {
  // Stop further work from happening by killing the subprocess and shutting
  // down the queues. We do the atomic exchange to avoid multiple threads
  // racing on Shutdown.
  // NOTE: compare_exchange_strong() takes a references as its first arg.
  bool false_ref = false;
  if (!closing_.compare_exchange_strong(false_ref, true)) {
    return;
  }
  // NOTE: ordering isn't too important as long as we shut everything down.
  WARN_NOT_OK(process_->KillAndWait(SIGTERM), "failed to stop subprocess");
  inbound_response_queue_.Shutdown();
  outbound_call_queue_.Shutdown();

  // We should be able to clean up our threads; they'll see that we're closing,
  // the pipe has been closed, or the queues have been shut down.
  write_thread_->Join();
  read_thread_->Join();
  deadline_checker_->Join();
  for (const auto& t : responder_threads_) {
    t->Join();
  }

  // Call any of the remaining callbacks.
  std::map<CallId, shared_ptr<SubprocessCall>> calls;
  {
    std::lock_guard<simple_spinlock> l(in_flight_lock_);
    calls = std::move(call_by_id_);
  }
  for (const auto& id_and_call : calls) {
    const auto& call = id_and_call.second;
    call->RespondError(Status::ServiceUnavailable("subprocess is shutting down"));
  }
}

void SubprocessServer::ReceiveMessagesThread() {
  DCHECK(message_protocol_) << "message protocol is not initialized";
  while (!closing_.load()) {
    // Receive a new response from the subprocess.
    SubprocessResponsePB response;
    Status s = message_protocol_->ReceiveMessage(&response);
    if (s.IsEndOfFile()) {
      // The underlying pipe was closed. We're likely shutting down.
      DCHECK(closing_);
      return;
    }
    // TODO(awong): getting an error here indicates that this server and the
    // underlying subprocess are not in sync (e.g. not speaking the same
    // protocol). We should consider either crashing here, or restarting the
    // subprocess.
    DCHECK(s.ok());
    WARN_NOT_OK(s, "failed to receive response from the subprocess");
    if (s.ok() && !inbound_response_queue_.BlockingPut(response)) {
      // The queue has been shut down and we should shut down too.
      DCHECK(closing_);
      LOG(INFO) << "failed to put response onto inbound queue";
      return;
    }
  }
}

void SubprocessServer::ResponderThread() {
  Status s;
  do {
    vector<SubprocessResponsePB> resps;
    // NOTE: since we don't supply a deadline, this will only fail if the queue
    // is shutting down. Also note that even if this fails because we're
    // shutting down, we still populate 'resps' and must run their callbacks.
    s = inbound_response_queue_.BlockingDrainTo(&resps);
    for (const auto& resp : resps) {
      if (!resp.has_id()) {
        LOG(FATAL) << Substitute("Received invalid response: $0",
                                 pb_util::SecureDebugString(resp));
      }
    }
    vector<pair<shared_ptr<SubprocessCall>, SubprocessResponsePB>> calls_and_resps;
    calls_and_resps.reserve(resps.size());
    {
      std::lock_guard<simple_spinlock> l(in_flight_lock_);
      for (auto& resp : resps) {
        auto id = resp.id();
        auto call = EraseKeyReturnValuePtr(&call_by_id_, id);
        if (call) {
          calls_and_resps.emplace_back(std::move(call), std::move(resp));
        }
      }
    }
    for (auto& call_and_resp : calls_and_resps) {
      call_and_resp.first->RespondSuccess(std::move(call_and_resp.second));
    }
    // If we didn't find our call, it timed out and the its callback has
    // already been called by the deadline checker.
  } while (s.ok());
  DCHECK(s.IsAborted());
  DCHECK(closing_);
  LOG(INFO) << "get failed, inbound queue shut down: " << s.ToString();
}

void SubprocessServer::CheckDeadlinesThread() {
  while (!closing_.load()) {
    MonoTime now = MonoTime::Now();
    vector<shared_ptr<SubprocessCall>> timed_out_calls;
    {
      std::lock_guard<simple_spinlock> l(in_flight_lock_);
      // NOTE: this is an approximation for age based on ID. That's OK because
      // deadline-checking is best-effort.
      auto earliest_call_within_deadline = call_by_id_.begin();
      for (; earliest_call_within_deadline != call_by_id_.end();
           earliest_call_within_deadline++) {
        const auto& call = earliest_call_within_deadline->second;
        if (now > call->deadline()) {
          timed_out_calls.emplace_back(call);
        } else {
          // This is the call with the earliest deadline that hasn't passed.
          break;
        }
      }
      // All calls older than the earliest call whose deadline hasn't passed
      // are timed out.
      call_by_id_.erase(call_by_id_.begin(), earliest_call_within_deadline);
    }
    for (const auto& call : timed_out_calls) {
      call->RespondError(Status::TimedOut("timed out while in flight"));
    }
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_subprocess_deadline_checking_interval_ms));
  }
}

void SubprocessServer::SendMessagesThread() {
  DCHECK(message_protocol_) << "message protocol is not initialized";
  Status s;
  do {
    vector<shared_ptr<SubprocessCall>> calls;
    // NOTE: since we don't supply a deadline, this will only fail if the queue
    // is shutting down. Also note that even if this fails because we're
    // shutting down, we still populate 'calls' and should add them to the
    // in-flight map. We'll run their callbacks as a part of shutdown.
    s = outbound_call_queue_.BlockingDrainTo(&calls);
    {
      std::lock_guard<simple_spinlock> l(in_flight_lock_);
      for (const auto& call : calls) {
        EmplaceOrDie(&call_by_id_, call->id(), call);
      }
    }
    // NOTE: it's possible that before sending the request, the call already
    // timed out and the deadline checker already called its callback. If so,
    // the following call will no-op.
    for (const auto& call : calls) {
      call->SendRequest(message_protocol_.get());
    }
  } while (s.ok());
  DCHECK(s.IsAborted());
  DCHECK(closing_);
  LOG(INFO) << "outbound queue shut down: " << s.ToString();
}

Status SubprocessServer::QueueCall(const shared_ptr<SubprocessCall>& call) {
  if (MonoTime::Now() > call->deadline()) {
    return Status::TimedOut("timed out before queueing call");
  }

  do {
    QueueStatus queue_status = outbound_call_queue_.Put(call);
    switch (queue_status) {
      case QUEUE_SUCCESS:
        return Status::OK();
      case QUEUE_SHUTDOWN:
        return Status::ServiceUnavailable("outbound queue shutting down");
      case QUEUE_FULL: {
        // If we still have more time allotted for this call, wait for a bit
        // and try again; otherwise, time out.
        if (MonoTime::Now() > call->deadline()) {
          return Status::TimedOut("timed out trying to queue call");
        }
        SleepFor(MonoDelta::FromMilliseconds(FLAGS_subprocess_queue_full_retry_ms));
      }
    }
  } while (true);

  return Status::OK();
}

} // namespace subprocess
} // namespace kudu
