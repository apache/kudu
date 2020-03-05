// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest_prod.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/subprocess/subprocess.pb.h"
#include "kudu/subprocess/subprocess_protocol.h"
#include "kudu/util/blocking_queue.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/mutex.h"
#include "kudu/util/status.h"
#include "kudu/util/status_callback.h"

namespace kudu {

class Subprocess;
class Thread;

namespace subprocess {

typedef int64_t CallId;

struct SimpleTimer {
  SimpleTimer() {
    start_time = MonoTime::Now();
  }
  MonoTime start_time;
  MonoDelta elapsed() const {
    return MonoTime::Now() - start_time;
  }
};

struct SubprocessMetrics {
  // Metrics returned from the subprocess.
  scoped_refptr<Histogram> sp_execution_time_ms;
  scoped_refptr<Histogram> sp_inbound_queue_length;
  scoped_refptr<Histogram> sp_inbound_queue_time_ms;
  scoped_refptr<Histogram> sp_outbound_queue_length;
  scoped_refptr<Histogram> sp_outbound_queue_time_ms;

  // Metrics recorded by the SubprocessServer.
  scoped_refptr<Histogram> server_inbound_queue_size_bytes;
  scoped_refptr<Histogram> server_inbound_queue_time_ms;
  scoped_refptr<Histogram> server_outbound_queue_size_bytes;
  scoped_refptr<Histogram> server_outbound_queue_time_ms;
};

// Encapsulates the pending state of a request that is in the process of being
// sent to a subprocess. These calls are added to an in-flight map before
// calling SendRequest(). See the method comments for some discussion about
// thread-safety.
class SubprocessCall {
 public:
  SubprocessCall(const SubprocessRequestPB* req,
                 SubprocessResponsePB* resp,
                 StdStatusCallback* cb,
                 MonoTime deadline)
      : id_(req->id()),
        deadline_(deadline),
        req_(req), resp_(resp), cb_(cb) {}

  CallId id() const {
    return id_;
  }

  MonoTime deadline() const {
    return deadline_;
  }

  // Sends the request with the given message protocol, ensuring that the
  // sending of the request doesn't overlap with the calling of the callback
  // (which may, in turn, delete the request state). If the request couldn't be
  // sent, runs the callback with an appropriate error immediately.
  void SendRequest(SubprocessProtocol* message_protocol) {
    std::lock_guard<Mutex> l(lock_);
    // If we've already run the callback, (e.g. we passed the deadline before
    // sending) we shouldn't try sending the message; the request and response
    // may have been destructed.
    if (!cb_) {
      return;
    }
    Status s = message_protocol->SendMessage(*req_);
    // If we failed to send the message, return the error to the caller.
    if (PREDICT_FALSE(!s.ok())) {
      WARN_NOT_OK(s, "failed to send request");
      (*cb_)(s);
      cb_ = nullptr;
    }
  }

  void RespondSuccess(SubprocessResponsePB resp) {
    std::lock_guard<Mutex> l(lock_);
    // If we've already run the callback (e.g. we passed the deadline before
    // responding), there's nothing to do.
    if (!cb_) {
      return;
    }
    *resp_ = std::move(resp);
    (*cb_)(Status::OK());
    cb_ = nullptr;
  }

  void RespondError(const Status& s) {
    DCHECK(!s.ok());
    std::lock_guard<Mutex> l(lock_);
    // If we've already run the callback (e.g. we failed to send the message),
    // there's nothing to do.
    if (!cb_) {
      return;
    }
    (*cb_)(s);
    cb_ = nullptr;
  }

 private:
  friend struct RequestLogicalSize;

  // Lock used to ensure that the sending of the request doesn't overlap with
  // the invocation of the callback. This is important because the callback may
  // destroy the message state (so it is unsafe to update or dereference 'req_'
  // or 'resp_' afterwards). Note that it is safe to update 'cb_' after the
  // callback is invoked.
  Mutex lock_;

  // ID of this call.
  const CallId id_;

  // Deadline for this call.
  const MonoTime deadline_;

  // Request and response associated with this call.
  const SubprocessRequestPB* req_;
  SubprocessResponsePB* resp_;

  // Callback to wake up the caller that enqueued this call. This is called
  // exactly once per SubprocessCall.
  StdStatusCallback* cb_;
};

// Used by BlockingQueue to determine the size of messages.
typedef std::pair<std::shared_ptr<SubprocessCall>, SimpleTimer> CallAndTimer;
struct RequestLogicalSize {
  static size_t logical_size(const CallAndTimer& call_and_timer) {
    return call_and_timer.first->req_->ByteSizeLong();
  }
};
typedef std::pair<SubprocessResponsePB, SimpleTimer> ResponsePBAndTimer;
struct ResponseLogicalSize {
  static size_t logical_size(const ResponsePBAndTimer& resp_and_timer) {
    return resp_and_timer.first.ByteSizeLong();
  }
};

typedef BlockingQueue<CallAndTimer, RequestLogicalSize> SubprocessCallQueue;
typedef BlockingQueue<ResponsePBAndTimer, ResponseLogicalSize> ResponseQueue;

// Wrapper for a subprocess that communicates via protobuf. A server is
// comprised of a few things to facilitate concurrent communication with an
// underlying subprocess:
//
// - An outbound queue of SubprocessCalls to send to the subprocess. When a
//   user enqueues a call, that call is first added to the outbound queue.
//
// - One "writer" thread: this thread pulls work off of the outbound queue and
//   writes it to the subprocess pipe. When a SubprocessCall's request is
//   written to the pipe, the call is tracked, and its callback may be called
//   at any time by the deadline checker or upon receiving a valid response.
//
// - One "reader" thread: this thread reads messages from subprocess pipe and
//   puts it on the inbound response queue.
//
// - An inbound queue of SubprocessResponsePBs that is populated by the reader
//   thread.
//
// - Many "responder" threads: each thread looks for a response on the inbound
//   queue and calls the appropriate callback for it, based on the response ID.
//
// - One "deadline-checker" thread: this thread looks through the oldest calls
//   that have been sent to the subprocess and runs their callbacks with a
//   TimedOut error if they are past their deadline.
//
// Public methods are virtual so a mock server can be used in tests.
class SubprocessServer {
 public:
  SubprocessServer(std::vector<std::string> subprocess_argv, SubprocessMetrics metrics);
  virtual ~SubprocessServer();

  // Initialize the server, starting the subprocess and worker threads.
  virtual Status Init() WARN_UNUSED_RESULT;

  // Synchronously sends a request to the subprocess and populates 'resp' with
  // contents returned from the subprocess, or returns an error if anything
  // failed or timed out along the way.
  virtual Status Execute(SubprocessRequestPB* req, SubprocessResponsePB* resp) WARN_UNUSED_RESULT;

 private:
  FRIEND_TEST(SubprocessServerTest, TestCallsReturnWhenShuttingDown);

  // Stop the subprocess and stop processing messages.
  void Shutdown();

  // Add the call to the outbound queue, returning an error if the call timed
  // out before successfully adding it to the queue, or if the queue is shut
  // down.
  //
  // The call's callback is run asynchronously upon receiving a response from
  // the subprocess, matched by ID, or when the deadline checker thread detects
  // that the call has timed out.
  Status QueueCall(const std::shared_ptr<SubprocessCall>& call) WARN_UNUSED_RESULT;

  // Long running thread that repeatedly looks at the in-flight call with the
  // lowest ID, checks whether its deadline has expired, and runs its callback
  // with a TimedOut error if so.
  void CheckDeadlinesThread();

  // Pulls responses from the inbound response queue and calls the associated
  // callbacks.
  void ResponderThread();

  // Pulls enqueued calls from the outbound request queue and sends their
  // associated requests to the subprocess.
  void SendMessagesThread();

  // Receives messages from the subprocess and puts the responses onto the
  // inbound response queue.
  void ReceiveMessagesThread();

  // Fixed timeout to be used for each call.
  const MonoDelta call_timeout_;

  // Next request ID to be assigned.
  std::atomic<CallId> next_id_;

  // Latch used to indicate that the server is shutting down.
  CountDownLatch closing_;

  // The underlying subprocess.
  std::shared_ptr<Subprocess> process_;

  // Protocol with which to send and receive bytes to and from 'process_'.
  std::shared_ptr<SubprocessProtocol> message_protocol_;

  // Pulls requests off the request queue and serializes them via the
  // message protocol.
  scoped_refptr<Thread> write_thread_;

  // Reads from the message protocol, constructs the response, and puts it on
  // the response queue.
  scoped_refptr<Thread> read_thread_;

  // Looks at the front of the queue for calls that are past their deadlines
  // and triggers their callbacks.
  scoped_refptr<Thread> deadline_checker_;

  // Pull work off the response queue and trigger the associated callbacks if
  // appropriate.
  std::vector<scoped_refptr<Thread>> responder_threads_;

  // Outbound queue of calls to send to the subprocess.
  SubprocessCallQueue outbound_call_queue_;

  // Inbound queue of responses sent by the subprocess.
  ResponseQueue inbound_response_queue_;

  // Metrics for this subprocess.
  SubprocessMetrics metrics_;

  // Calls that are currently in-flight (the requests are being sent over the
  // pipe or waiting for a response), ordered by ID. This ordering allows for
  // lookup by ID, and gives us a rough way to get the calls with earliest
  // start times which is useful for deadline-checking.
  //
  // Only a single thread may remove a given call; that thread must run the
  // call's callback.
  simple_spinlock in_flight_lock_;
  std::map<CallId, std::shared_ptr<SubprocessCall>> call_by_id_;
};

} // namespace subprocess
} // namespace kudu
