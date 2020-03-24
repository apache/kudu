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

SubprocessServer::SubprocessServer(vector<string> subprocess_argv, SubprocessMetrics metrics)
    : call_timeout_(MonoDelta::FromSeconds(FLAGS_subprocess_timeout_secs)),
      next_id_(1),
      closing_(1),
      process_(make_shared<Subprocess>(std::move(subprocess_argv))),
      outbound_call_queue_(FLAGS_subprocess_request_queue_size_bytes),
      inbound_response_queue_(FLAGS_subprocess_response_queue_size_bytes),
      metrics_(std::move(metrics)) {
  process_->ShareParentStdin(false);
  process_->ShareParentStdout(false);
}

SubprocessServer::~SubprocessServer() {
  Shutdown();
}

void SubprocessServer::StartSubprocessThread(const StdStatusCallback& cb) {
  Status s = process_->Start();
  cb(s);
  if (PREDICT_TRUE(s.ok())) {
    // If we successfully started the process, we should stay alive until we
    // shut down.
    closing_.Wait();
  }
}

Status SubprocessServer::Init() {
  VLOG(2) << "Starting the subprocess";
  Synchronizer sync;
  auto cb = sync.AsStdStatusCallback();
  RETURN_NOT_OK(Thread::Create("subprocess", "start",
                               [this, &cb]() { this->StartSubprocessThread(cb); },
                               &read_thread_));
  RETURN_NOT_OK_PREPEND(sync.Wait(), "Failed to start subprocess");

  // Start the message protocol.
  CHECK(!message_protocol_);
  message_protocol_.reset(new SubprocessProtocol(SubprocessProtocol::SerializationMode::PB,
                                                 SubprocessProtocol::CloseMode::CLOSE_ON_DESTROY,
                                                 process_->ReleaseChildStdoutFd(),
                                                 process_->ReleaseChildStdinFd()));
  const int num_threads = FLAGS_subprocess_num_responder_threads;
  responder_threads_.resize(num_threads);
  for (int i = 0; i < num_threads; i++) {
    RETURN_NOT_OK(Thread::Create("subprocess", "responder",
                                 [this]() { this->ResponderThread(); },
                                 &responder_threads_[i]));
  }
  RETURN_NOT_OK(Thread::Create("subprocess", "reader",
                               [this]() { this->ReceiveMessagesThread(); },
                               &read_thread_));
  RETURN_NOT_OK(Thread::Create("subprocess", "writer",
                               [this]() { this->SendMessagesThread(); },
                               &write_thread_));
  return Thread::Create("subprocess", "deadline-checker",
                        [this]() { this->CheckDeadlinesThread(); },
                        &deadline_checker_);
}

Status SubprocessServer::Execute(SubprocessRequestPB* req,
                                 SubprocessResponsePB* resp) {
  DCHECK(!req->has_id());
  req->set_id(next_id_++);
  Synchronizer sync;
  auto cb = sync.AsStdStatusCallback();
  // Before adding to the queue, record the size of the call queue.
  metrics_.server_outbound_queue_size_bytes->Increment(outbound_call_queue_.size());
  CallAndTimer call_and_timer = {
      make_shared<SubprocessCall>(req, resp, &cb, MonoTime::Now() + call_timeout_), {} };
  RETURN_NOT_OK_PREPEND(
      outbound_call_queue_.BlockingPut(call_and_timer, call_and_timer.first->deadline()),
      "couldn't enqueue call");
  return sync.Wait();
}

void SubprocessServer::Shutdown() {
  // Stop further work from happening by killing the subprocess and shutting
 // down the queues.
  if (!closing_.CountDown()) {
    // We may shut down out-of-band in tests; if we've already shut down,
    // there's nothing left to do.
    return;
  }
  // NOTE: ordering isn't too important as long as we shut everything down.
  //
  // Normally the process_ should be started before we reach Shutdown() and the
  // threads below should be running too, except in mock servers because we
  // don't init there. Shutdown() is still called in this case from the
  // destructor though so these checks are necessary.
  if (process_->IsStarted()) {
    WARN_NOT_OK(process_->KillAndWait(SIGTERM), "failed to stop subprocess");
  }
  inbound_response_queue_.Shutdown();
  outbound_call_queue_.Shutdown();

  // We should be able to clean up our threads; they'll see that we're closing,
  // the pipe has been closed, or the queues have been shut down.
  if (write_thread_) {
    write_thread_->Join();
  }
  if (read_thread_) {
    read_thread_->Join();
  }
  if (deadline_checker_) {
    deadline_checker_->Join();
  }
  if (start_thread_) {
    start_thread_->Join();
  }
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
  while (closing_.count() > 0) {
    // Receive a new response from the subprocess.
    SubprocessResponsePB response;
    Status s = message_protocol_->ReceiveMessage(&response);
    if (s.IsEndOfFile()) {
      // The underlying pipe was closed. We're likely shutting down.
      DCHECK_EQ(0, closing_.count());
      LOG(INFO) << "Received an EOF from the subprocess";
      return;
    }
    // TODO(awong): getting an error here indicates that this server and the
    // underlying subprocess are not in sync (e.g. not speaking the same
    // protocol). We should consider either crashing here, or restarting the
    // subprocess.
    DCHECK(s.ok());
    WARN_NOT_OK(s, "failed to receive response from the subprocess");
    // Before adding to the queue, record the size of the response queue.
    metrics_.server_inbound_queue_size_bytes->Increment(inbound_response_queue_.size());
    ResponsePBAndTimer resp_and_timer = { std::move(response), {} };
    if (s.ok() && !inbound_response_queue_.BlockingPut(resp_and_timer).ok()) {
      // The queue has been shut down and we should shut down too.
      DCHECK_EQ(0, closing_.count());
      LOG(INFO) << "failed to put response onto inbound queue";
      return;
    }
  }
}

void SubprocessServer::ResponderThread() {
  Status s;
  do {
    vector<ResponsePBAndTimer> resps;
    // NOTE: since we don't supply a deadline, this will only fail if the queue
    // is shutting down. Also note that even if this fails because we're
    // shutting down, we still populate 'resps' and must run their callbacks.
    s = inbound_response_queue_.BlockingDrainTo(&resps);
    for (auto& resp_and_timer : resps) {
      metrics_.server_inbound_queue_time_ms->Increment(
          resp_and_timer.second.elapsed().ToMilliseconds());
      const auto& resp = resp_and_timer.first;

      if (!resp.has_id()) {
        LOG(FATAL) << Substitute("Received invalid response: $0",
                                 pb_util::SecureDebugString(resp));
      }
      // Regardless of whether this call succeeded or not, parse the returned
      // metrics.
      if (PREDICT_TRUE(resp.has_metrics())) {
        const auto& pb = resp.metrics();
        metrics_.sp_inbound_queue_length->Increment(pb.inbound_queue_length());
        metrics_.sp_outbound_queue_length->Increment(pb.outbound_queue_length());
        metrics_.sp_inbound_queue_time_ms->Increment(pb.inbound_queue_time_ms());
        metrics_.sp_outbound_queue_time_ms->Increment(pb.outbound_queue_time_ms());
        metrics_.sp_execution_time_ms->Increment(pb.execution_time_ms());
      }
    }
    vector<pair<shared_ptr<SubprocessCall>, SubprocessResponsePB>> calls_and_resps;
    calls_and_resps.reserve(resps.size());
    {
      std::lock_guard<simple_spinlock> l(in_flight_lock_);
      for (auto& resp_and_timer : resps) {
        auto& resp = resp_and_timer.first;
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
  DCHECK_EQ(0, closing_.count());
  LOG(INFO) << "get failed, inbound queue shut down: " << s.ToString();
}

void SubprocessServer::CheckDeadlinesThread() {
  while (!closing_.WaitFor(
      MonoDelta::FromMilliseconds(FLAGS_subprocess_deadline_checking_interval_ms))) {
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
  }
}

void SubprocessServer::SendMessagesThread() {
  DCHECK(message_protocol_) << "message protocol is not initialized";
  Status s;
  do {
    vector<CallAndTimer> calls;
    // NOTE: since we don't supply a deadline, this will only fail if the queue
    // is shutting down. Also note that even if this fails because we're
    // shutting down, we still populate 'calls' and should add them to the
    // in-flight map. We'll run their callbacks as a part of shutdown.
    s = outbound_call_queue_.BlockingDrainTo(&calls);
    {
      std::lock_guard<simple_spinlock> l(in_flight_lock_);
      for (const auto& call_and_timer : calls) {
        const auto& call = call_and_timer.first;
        EmplaceOrDie(&call_by_id_, call->id(), call);
      }
    }
    // NOTE: it's possible that before sending the request, the call already
    // timed out and the deadline checker already called its callback. If so,
    // the following call will no-op.
    for (const auto& call_and_timer : calls) {
      const auto& call = call_and_timer.first;
      metrics_.server_outbound_queue_time_ms->Increment(
          call_and_timer.second.elapsed().ToMilliseconds());

      call->SendRequest(message_protocol_.get());
    }
  } while (s.ok());
  DCHECK(s.IsAborted());
  DCHECK_EQ(0, closing_.count());
  LOG(INFO) << "outbound queue shut down: " << s.ToString();
}

} // namespace subprocess
} // namespace kudu
