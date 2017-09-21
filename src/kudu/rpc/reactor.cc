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

#include "kudu/rpc/reactor.h"

#include <cerrno>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <boost/bind.hpp> // IWYU pragma: keep
#include <boost/intrusive/list.hpp>
#include <boost/optional.hpp>
#include <ev++.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/bind.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/client_negotiation.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/negotiation.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/server_negotiation.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/debug/sanitizer_scopes.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"
#include "kudu/util/thread_restrictions.h"
#include "kudu/util/threadpool.h"
#include "kudu/util/trace.h"

// When compiling on Mac OS X, use 'kqueue' instead of the default, 'select', for the event loop.
// Otherwise we run into problems because 'select' can't handle connections when more than 1024
// file descriptors are open by the process.
#if defined(__APPLE__)
static const int kDefaultLibEvFlags = ev::KQUEUE;
#else
static const int kDefaultLibEvFlags = ev::AUTO;
#endif

using std::string;
using std::shared_ptr;
using std::unique_ptr;
using strings::Substitute;

DEFINE_int64(rpc_negotiation_timeout_ms, 3000,
             "Timeout for negotiating an RPC connection.");
TAG_FLAG(rpc_negotiation_timeout_ms, advanced);
TAG_FLAG(rpc_negotiation_timeout_ms, runtime);

DEFINE_bool(rpc_reopen_outbound_connections, false,
            "Open a new connection to the server for every RPC call. "
            "If not enabled, an already existing connection to a "
            "server is reused upon making another call to the same server. "
            "When this flag is enabled, an already existing _idle_ connection "
            "to the server is closed upon making another RPC call which would "
            "reuse the connection otherwise. "
            "Used by tests only.");
TAG_FLAG(rpc_reopen_outbound_connections, unsafe);
TAG_FLAG(rpc_reopen_outbound_connections, runtime);

METRIC_DEFINE_histogram(server, reactor_load_percent,
                        "Reactor Thread Load Percentage",
                        kudu::MetricUnit::kUnits,
                        "The percentage of time that the reactor is busy "
                        "(not blocked awaiting network activity). If this metric "
                        "shows significant samples nears 100%, increasing the "
                        "number of reactors may be beneficial.", 100, 2);

METRIC_DEFINE_histogram(server, reactor_active_latency_us,
                        "Reactor Thread Active Latency",
                        kudu::MetricUnit::kMicroseconds,
                        "TODO", 1000000, 2);

namespace kudu {
namespace rpc {

namespace {
Status ShutdownError(bool aborted) {
  const char* msg = "reactor is shutting down";
  return aborted ?
      Status::Aborted(msg, "", ESHUTDOWN) :
      Status::ServiceUnavailable(msg, "", ESHUTDOWN);
}

// Callback for libev fatal errors (eg running out of file descriptors).
// Unfortunately libev doesn't plumb these back through to the caller, but
// instead just expects the callback to abort.
//
// This implementation is slightly preferable to the built-in one since
// it uses a FATAL log message instead of printing to stderr, which might
// not end up anywhere useful in a daemonized context.
void LibevSysErr(const char* msg) throw() {
  PLOG(FATAL) << "LibEV fatal error: " << msg;
}

void DoInitLibEv() {
  ev::set_syserr_cb(LibevSysErr);
}

} // anonymous namespace

ReactorThread::ReactorThread(Reactor *reactor, const MessengerBuilder& bld)
  : loop_(kDefaultLibEvFlags),
    cur_time_(MonoTime::Now()),
    last_unused_tcp_scan_(cur_time_),
    reactor_(reactor),
    connection_keepalive_time_(bld.connection_keepalive_time_),
    coarse_timer_granularity_(bld.coarse_timer_granularity_),
    total_client_conns_cnt_(0),
    total_server_conns_cnt_(0) {

  if (bld.metric_entity_) {
    invoke_us_histogram_ =
        METRIC_reactor_active_latency_us.Instantiate(bld.metric_entity_);
    load_percent_histogram_ =
        METRIC_reactor_load_percent.Instantiate(bld.metric_entity_);
  }
}

Status ReactorThread::Init() {
  DCHECK(thread_.get() == nullptr) << "Already started";
  DVLOG(6) << "Called ReactorThread::Init()";
  // Register to get async notifications in our epoll loop.
  async_.set(loop_);
  async_.set<ReactorThread, &ReactorThread::AsyncHandler>(this);
  async_.start();

  // Register the timer watcher.
  // The timer is used for closing old TCP connections and applying
  // backpressure.
  timer_.set(loop_);
  timer_.set<ReactorThread, &ReactorThread::TimerHandler>(this); // NOLINT(*)
  timer_.start(coarse_timer_granularity_.ToSeconds(),
               coarse_timer_granularity_.ToSeconds());

  // Register our callbacks. ev++ doesn't provide handy wrappers for these.
  ev_set_userdata(loop_, this);
  ev_set_loop_release_cb(loop_, &ReactorThread::AboutToPollCb, &ReactorThread::PollCompleteCb);
  ev_set_invoke_pending_cb(loop_, &ReactorThread::InvokePendingCb);

  // Create Reactor thread.
  return kudu::Thread::Create("reactor", "rpc reactor", &ReactorThread::RunThread, this, &thread_);
}

void ReactorThread::InvokePendingCb(struct ev_loop* loop) {
  // Calculate the number of cycles spent calling our callbacks.
  // This is called quite frequently so we use CycleClock rather than MonoTime
  // since it's a bit faster.
  int64_t start = CycleClock::Now();
  ev_invoke_pending(loop);
  int64_t dur_cycles = CycleClock::Now() - start;

  // Contribute this to our histogram.
  ReactorThread* thr = static_cast<ReactorThread*>(ev_userdata(loop));
  if (thr->invoke_us_histogram_) {
    thr->invoke_us_histogram_->Increment(dur_cycles * 1e6 / base::CyclesPerSecond());
  }
}

void ReactorThread::AboutToPollCb(struct ev_loop* loop) noexcept {
  // Store the current time in a member variable to be picked up below
  // in PollCompleteCb.
  ReactorThread* thr = static_cast<ReactorThread*>(ev_userdata(loop));
  thr->cycle_clock_before_poll_ = CycleClock::Now();
}

void ReactorThread::PollCompleteCb(struct ev_loop* loop) noexcept {
  // First things first, capture the time, so that this is as accurate as possible
  int64_t cycle_clock_after_poll = CycleClock::Now();

  // Record it in our accounting.
  ReactorThread* thr = static_cast<ReactorThread*>(ev_userdata(loop));
  DCHECK_NE(thr->cycle_clock_before_poll_, -1)
      << "PollCompleteCb called without corresponding AboutToPollCb";

  int64_t poll_cycles = cycle_clock_after_poll - thr->cycle_clock_before_poll_;
  thr->cycle_clock_before_poll_ = -1;
  thr->total_poll_cycles_ += poll_cycles;
}

void ReactorThread::Shutdown(Messenger::ShutdownMode mode) {
  CHECK(reactor_->closing()) << "Should be called after setting closing_ flag";

  VLOG(1) << name() << ": shutting down Reactor thread.";
  WakeThread();

  if (mode == Messenger::ShutdownMode::SYNC) {
    // Join() will return a bad status if asked to join on the currently
    // running thread.
    CHECK_OK(ThreadJoiner(thread_.get()).Join());
  }
}

void ReactorThread::ShutdownInternal() {
  DCHECK(IsCurrentThread());

  // Tear down any outbound TCP connections.
  Status service_unavailable = ShutdownError(false);
  VLOG(1) << name() << ": tearing down outbound TCP connections...";
  for (const auto& elem : client_conns_) {
    const auto& conn = elem.second;
    VLOG(1) << name() << ": shutting down " << conn->ToString();
    conn->Shutdown(service_unavailable);
  }
  client_conns_.clear();

  // Tear down any inbound TCP connections.
  VLOG(1) << name() << ": tearing down inbound TCP connections...";
  for (const auto& conn : server_conns_) {
    VLOG(1) << name() << ": shutting down " << conn->ToString();
    conn->Shutdown(service_unavailable);
  }
  server_conns_.clear();

  // Abort any scheduled tasks.
  //
  // These won't be found in the ReactorThread's list of pending tasks
  // because they've been "run" (that is, they've been scheduled).
  Status aborted = ShutdownError(true); // aborted
  for (DelayedTask* task : scheduled_tasks_) {
    task->Abort(aborted); // should also free the task.
  }
  scheduled_tasks_.clear();

  // Remove the OpenSSL thread state.
  ERR_remove_thread_state(nullptr);
}

ReactorTask::ReactorTask() {
}
ReactorTask::~ReactorTask() {
}

Status ReactorThread::GetMetrics(ReactorMetrics* metrics) {
  DCHECK(IsCurrentThread());
  metrics->num_client_connections_ = client_conns_.size();
  metrics->num_server_connections_ = server_conns_.size();
  metrics->total_client_connections_ = total_client_conns_cnt_;
  metrics->total_server_connections_ = total_server_conns_cnt_;
  return Status::OK();
}

Status ReactorThread::DumpRunningRpcs(const DumpRunningRpcsRequestPB& req,
                                      DumpRunningRpcsResponsePB* resp) {
  DCHECK(IsCurrentThread());
  for (const scoped_refptr<Connection>& conn : server_conns_) {
    RETURN_NOT_OK(conn->DumpPB(req, resp->add_inbound_connections()));
  }
  for (const conn_multimap_t::value_type& entry : client_conns_) {
    Connection* conn = entry.second.get();
    RETURN_NOT_OK(conn->DumpPB(req, resp->add_outbound_connections()));
  }
  return Status::OK();
}

void ReactorThread::WakeThread() {
  // libev uses some lock-free synchronization, but doesn't have TSAN annotations.
  // See http://lists.schmorp.de/pipermail/libev/2013q2/002178.html or KUDU-366
  // for examples.
  debug::ScopedTSANIgnoreReadsAndWrites ignore_tsan;
  async_.send();
}

// Handle async events.  These events are sent to the reactor by other
// threads that want to bring something to our attention, like the fact that
// we're shutting down, or the fact that there is a new outbound Transfer
// ready to send.
void ReactorThread::AsyncHandler(ev::async& /*watcher*/, int /*revents*/) {
  DCHECK(IsCurrentThread());

  if (PREDICT_FALSE(reactor_->closing())) {
    ShutdownInternal();
    loop_.break_loop(); // break the epoll loop and terminate the thread
    return;
  }

  boost::intrusive::list<ReactorTask> tasks;
  reactor_->DrainTaskQueue(&tasks);

  while (!tasks.empty()) {
    ReactorTask& task = tasks.front();
    tasks.pop_front();
    task.Run(this);
  }
}

void ReactorThread::RegisterConnection(scoped_refptr<Connection> conn) {
  DCHECK(IsCurrentThread());

  Status s = StartConnectionNegotiation(conn);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(ERROR) << "Server connection negotiation failed: " << s.ToString();
    DestroyConnection(conn.get(), s);
    return;
  }
  ++total_server_conns_cnt_;
  server_conns_.emplace_back(std::move(conn));
}

void ReactorThread::AssignOutboundCall(const shared_ptr<OutboundCall>& call) {
  DCHECK(IsCurrentThread());

  // Skip if the outbound has been cancelled already.
  if (PREDICT_FALSE(call->IsCancelled())) {
    return;
  }

  scoped_refptr<Connection> conn;
  Status s = FindOrStartConnection(call->conn_id(),
                                   call->controller()->credentials_policy(),
                                   &conn);
  if (PREDICT_FALSE(!s.ok())) {
    call->SetFailed(s, OutboundCall::Phase::CONNECTION_NEGOTIATION);
    return;
  }

  conn->QueueOutboundCall(call);
}

void ReactorThread::CancelOutboundCall(const shared_ptr<OutboundCall>& call) {
  DCHECK(IsCurrentThread());

  // If the callback has been invoked already, the cancellation is a no-op.
  // The controller may be gone already if the callback has been invoked.
  if (call->IsFinished()) {
    return;
  }

  scoped_refptr<Connection> conn;
  if (FindConnection(call->conn_id(),
                     call->controller()->credentials_policy(),
                     &conn)) {
    conn->CancelOutboundCall(call);
  }
  call->Cancel();
}

//
// Handles timer events.  The periodic timer:
//
// 1. updates Reactor::cur_time_
// 2. every tcp_conn_timeo_ seconds, close down connections older than
//    tcp_conn_timeo_ seconds.
//
void ReactorThread::TimerHandler(ev::timer& /*watcher*/, int revents) {
  DCHECK(IsCurrentThread());
  if (EV_ERROR & revents) {
    LOG(WARNING) << "Reactor " << name() << " got an error in "
      "the timer handler.";
    return;
  }
  cur_time_ = MonoTime::Now();

  // Compute load percentage.
  int64_t now_cycles = CycleClock::Now();
  if (last_load_measurement_.time_cycles != -1) {
    int64_t cycles_delta = (now_cycles - last_load_measurement_.time_cycles);
    int64_t poll_cycles_delta = total_poll_cycles_ - last_load_measurement_.poll_cycles;
    double poll_fraction = static_cast<double>(poll_cycles_delta) / cycles_delta;
    double active_fraction = 1 - poll_fraction;
    if (load_percent_histogram_) {
      load_percent_histogram_->Increment(static_cast<int>(active_fraction * 100));
    }
  }
  last_load_measurement_.time_cycles = now_cycles;
  last_load_measurement_.poll_cycles = total_poll_cycles_;

  ScanIdleConnections();
}

void ReactorThread::RegisterTimeout(ev::timer *watcher) {
  watcher->set(loop_);
}

void ReactorThread::ScanIdleConnections() {
  DCHECK(IsCurrentThread());
  // Enforce TCP connection timeouts: server-side connections.
  const auto server_conns_end = server_conns_.end();
  uint64_t timed_out = 0;
  for (auto it = server_conns_.begin(); it != server_conns_end; ) {
    Connection* conn = it->get();
    if (!conn->Idle()) {
      VLOG(10) << "Connection " << conn->ToString() << " not idle";
      ++it;
      continue;
    }

    const MonoDelta connection_delta(cur_time_ - conn->last_activity_time());
    if (connection_delta <= connection_keepalive_time_) {
      ++it;
      continue;
    }

    conn->Shutdown(Status::NetworkError(
        Substitute("connection timed out after $0", connection_keepalive_time_.ToString())));
    VLOG(1) << "Timing out connection " << conn->ToString() << " - it has been idle for "
            << connection_delta.ToString();
    ++timed_out;
    it = server_conns_.erase(it);
  }

  // Take care of idle client-side connections marked for shutdown.
  uint64_t shutdown = 0;
  for (auto it = client_conns_.begin(); it != client_conns_.end();) {
    Connection* conn = it->second.get();
    if (conn->scheduled_for_shutdown() && conn->Idle()) {
      conn->Shutdown(Status::NetworkError(
          "connection has been marked for shutdown"));
      it = client_conns_.erase(it);
      ++shutdown;
    } else {
      ++it;
    }
  }
  // TODO(aserbin): clients may want to set their keepalive timeout for idle
  //                but not scheduled for shutdown connections.

  VLOG_IF(1, timed_out > 0) << name() << ": timed out " << timed_out << " TCP connections.";
  VLOG_IF(1, shutdown > 0) << name() << ": shutdown " << shutdown << " TCP connections.";
}

const std::string& ReactorThread::name() const {
  return reactor_->name();
}

MonoTime ReactorThread::cur_time() const {
  return cur_time_;
}

Reactor *ReactorThread::reactor() {
  return reactor_;
}

bool ReactorThread::IsCurrentThread() const {
  return thread_.get() == kudu::Thread::current_thread();
}

void ReactorThread::RunThread() {
  ThreadRestrictions::SetWaitAllowed(false);
  ThreadRestrictions::SetIOAllowed(false);
  DVLOG(6) << "Calling ReactorThread::RunThread()...";
  loop_.run(0);
  VLOG(1) << name() << " thread exiting.";

  // No longer need the messenger. This causes the messenger to
  // get deleted when all the reactors exit.
  reactor_->messenger_.reset();
}

bool ReactorThread::FindConnection(const ConnectionId& conn_id,
                                   CredentialsPolicy cred_policy,
                                   scoped_refptr<Connection>* conn) {
  DCHECK(IsCurrentThread());
  const auto range = client_conns_.equal_range(conn_id);
  scoped_refptr<Connection> found_conn;
  for (auto it = range.first; it != range.second;) {
    const auto& c = it->second.get();
    // * Do not use connections scheduled for shutdown to place new calls.
    //
    // * Do not use a connection with a non-compliant credentials policy.
    //   Instead, open a new one, while marking the former as scheduled for
    //   shutdown. This process converges: any connection that satisfies the
    //   PRIMARY_CREDENTIALS policy automatically satisfies the ANY_CREDENTIALS
    //   policy as well. The idea is to keep only one usable connection
    //   identified by the specified 'conn_id'.
    //
    // * If the test-only 'one-connection-per-RPC' mode is enabled, connections
    //   are re-established at every RPC call.
    if (c->scheduled_for_shutdown() ||
        !c->SatisfiesCredentialsPolicy(cred_policy) ||
        PREDICT_FALSE(FLAGS_rpc_reopen_outbound_connections)) {
      if (c->Idle()) {
        // Shutdown idle connections to the target destination. Non-idle ones
        // will be taken care of later by the idle connection scanner.
        DCHECK_EQ(Connection::CLIENT, c->direction());
        c->Shutdown(Status::NetworkError("connection is closed due to non-reuse policy"));
        it = client_conns_.erase(it);
        continue;
      }
      c->set_scheduled_for_shutdown();
    } else {
      DCHECK(!found_conn);
      found_conn = c;
      // Appropriate connection is found; continue further to take care of the
      // rest of connections to mark them for shutdown if they are not
      // satisfying the policy.
    }
    ++it;
  }
  if (found_conn) {
    // Found matching not-to-be-shutdown connection: return it as the result.
    conn->swap(found_conn);
    return true;
  }
  return false;
}

Status ReactorThread::FindOrStartConnection(const ConnectionId& conn_id,
                                            CredentialsPolicy cred_policy,
                                            scoped_refptr<Connection>* conn) {
  DCHECK(IsCurrentThread());
  if (FindConnection(conn_id, cred_policy, conn)) {
    return Status::OK();
  }

  // No connection to this remote. Need to create one.
  VLOG(2) << name() << " FindOrStartConnection: creating "
          << "new connection for " << conn_id.remote().ToString();

  // Create a new socket and start connecting to the remote.
  Socket sock;
  RETURN_NOT_OK(CreateClientSocket(&sock));
  RETURN_NOT_OK(StartConnect(&sock, conn_id.remote()));

  unique_ptr<Socket> new_socket(new Socket(sock.Release()));

  // Register the new connection in our map.
  *conn = new Connection(
      this, conn_id.remote(), std::move(new_socket), Connection::CLIENT, cred_policy);
  (*conn)->set_outbound_connection_id(conn_id);

  // Kick off blocking client connection negotiation.
  Status s = StartConnectionNegotiation(*conn);
  if (s.IsIllegalState()) {
    // Return a nicer error message to the user indicating -- if we just
    // forward the status we'd get something generic like "ThreadPool is closing".
    return Status::ServiceUnavailable("Client RPC Messenger shutting down");
  }
  // Propagate any other errors as-is.
  RETURN_NOT_OK_PREPEND(s, "Unable to start connection negotiation thread");

  // Insert into the client connection map to avoid duplicate connection requests.
  client_conns_.emplace(conn_id, *conn);
  ++total_client_conns_cnt_;

  return Status::OK();
}

Status ReactorThread::StartConnectionNegotiation(const scoped_refptr<Connection>& conn) {
  DCHECK(IsCurrentThread());

  // Set a limit on how long the server will negotiate with a new client.
  MonoTime deadline = MonoTime::Now() +
      MonoDelta::FromMilliseconds(FLAGS_rpc_negotiation_timeout_ms);

  scoped_refptr<Trace> trace(new Trace());
  ADOPT_TRACE(trace.get());
  TRACE("Submitting negotiation task for $0", conn->ToString());
  auto authentication = reactor()->messenger()->authentication();
  auto encryption = reactor()->messenger()->encryption();
  ThreadPool* negotiation_pool =
      reactor()->messenger()->negotiation_pool(conn->direction());
  RETURN_NOT_OK(negotiation_pool->SubmitClosure(
        Bind(&Negotiation::RunNegotiation, conn, authentication, encryption, deadline)));
  return Status::OK();
}

void ReactorThread::CompleteConnectionNegotiation(
    const scoped_refptr<Connection>& conn,
    const Status& status,
    unique_ptr<ErrorStatusPB> rpc_error) {
  DCHECK(IsCurrentThread());
  if (PREDICT_FALSE(!status.ok())) {
    DestroyConnection(conn.get(), status, std::move(rpc_error));
    return;
  }

  // Switch the socket back to non-blocking mode after negotiation.
  Status s = conn->SetNonBlocking(true);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(DFATAL) << "Unable to set connection to non-blocking mode: " << s.ToString();
    DestroyConnection(conn.get(), s, std::move(rpc_error));
    return;
  }

  conn->MarkNegotiationComplete();
  conn->EpollRegister(loop_);
}

Status ReactorThread::CreateClientSocket(Socket *sock) {
  Status ret = sock->Init(Socket::FLAG_NONBLOCKING);
  if (ret.ok()) {
    ret = sock->SetNoDelay(true);
  }
  LOG_IF(WARNING, !ret.ok())
      << "failed to create an outbound connection because a new socket could not be created: "
      << ret.ToString();
  return ret;
}

Status ReactorThread::StartConnect(Socket *sock, const Sockaddr& remote) {
  const Status ret = sock->Connect(remote);
  if (ret.ok()) {
    VLOG(3) << "StartConnect: connect finished immediately for " << remote.ToString();
    return Status::OK();
  }

  int posix_code = ret.posix_code();
  if (Socket::IsTemporarySocketError(posix_code) || posix_code == EINPROGRESS) {
    VLOG(3) << "StartConnect: connect in progress for " << remote.ToString();
    return Status::OK();
  }

  LOG(WARNING) << "Failed to create an outbound connection to " << remote.ToString()
               << " because connect() failed: " << ret.ToString();
  return ret;
}

void ReactorThread::DestroyConnection(Connection *conn,
                                      const Status& conn_status,
                                      unique_ptr<ErrorStatusPB> rpc_error) {
  DCHECK(IsCurrentThread());

  conn->Shutdown(conn_status, std::move(rpc_error));

  // Unlink connection from lists.
  if (conn->direction() == Connection::CLIENT) {
    const auto range = client_conns_.equal_range(conn->outbound_connection_id());
    CHECK(range.first != range.second) << "Couldn't find connection " << conn->ToString();
    // The client_conns_ container is a multi-map.
    for (auto it = range.first; it != range.second;) {
      if (it->second.get() == conn) {
        it = client_conns_.erase(it);
        break;
      }
      ++it;
    }
  } else if (conn->direction() == Connection::SERVER) {
    auto it = server_conns_.begin();
    while (it != server_conns_.end()) {
      if ((*it).get() == conn) {
        server_conns_.erase(it);
        break;
      }
      ++it;
    }
  }
}

DelayedTask::DelayedTask(boost::function<void(const Status&)> func,
                         MonoDelta when)
    : func_(std::move(func)),
      when_(when),
      thread_(nullptr) {
}

void DelayedTask::Run(ReactorThread* thread) {
  DCHECK(thread_ == nullptr) << "Task has already been scheduled";
  DCHECK(thread->IsCurrentThread());

  // Schedule the task to run later.
  thread_ = thread;
  timer_.set(thread->loop_);
  timer_.set<DelayedTask, &DelayedTask::TimerHandler>(this);
  timer_.start(when_.ToSeconds(), // after
               0);                // repeat
  thread_->scheduled_tasks_.insert(this);
}

void DelayedTask::Abort(const Status& abort_status) {
  func_(abort_status);
  delete this;
}

void DelayedTask::TimerHandler(ev::timer& watcher, int revents) {
  // We will free this task's memory.
  thread_->scheduled_tasks_.erase(this);

  if (EV_ERROR & revents) {
    string msg = "Delayed task got an error in its timer handler";
    LOG(WARNING) << msg;
    Abort(Status::Aborted(msg)); // Will delete 'this'.
  } else {
    func_(Status::OK());
    delete this;
  }
}

Reactor::Reactor(shared_ptr<Messenger> messenger,
                 int index, const MessengerBuilder& bld)
    : messenger_(std::move(messenger)),
      name_(StringPrintf("%s_R%03d", messenger_->name().c_str(), index)),
      closing_(false),
      thread_(this, bld) {
  static std::once_flag libev_once;
  std::call_once(libev_once, DoInitLibEv);
}

Status Reactor::Init() {
  DVLOG(6) << "Called Reactor::Init()";
  return thread_.Init();
}

void Reactor::Shutdown(Messenger::ShutdownMode mode) {
  {
    std::lock_guard<LockType> l(lock_);
    if (closing_) {
      return;
    }
    closing_ = true;
  }

  thread_.Shutdown(mode);

  // Abort all pending tasks. No new tasks can get scheduled after this
  // because ScheduleReactorTask() tests the closing_ flag set above.
  Status aborted = ShutdownError(true);
  while (!pending_tasks_.empty()) {
    ReactorTask& task = pending_tasks_.front();
    pending_tasks_.pop_front();
    task.Abort(aborted);
  }
}

Reactor::~Reactor() {
  Shutdown(Messenger::ShutdownMode::ASYNC);
}

const std::string& Reactor::name() const {
  return name_;
}

bool Reactor::closing() const {
  std::lock_guard<LockType> l(lock_);
  return closing_;
}

// Task to call an arbitrary function within the reactor thread.
class RunFunctionTask : public ReactorTask {
 public:
  explicit RunFunctionTask(boost::function<Status()> f)
      : function_(std::move(f)), latch_(1) {}

  void Run(ReactorThread* /*reactor*/) override {
    status_ = function_();
    latch_.CountDown();
  }
  void Abort(const Status& status) override {
    status_ = status;
    latch_.CountDown();
  }

  // Wait until the function has completed, and return the Status
  // returned by the function.
  Status Wait() {
    latch_.Wait();
    return status_;
  }

 private:
  boost::function<Status()> function_;
  Status status_;
  CountDownLatch latch_;
};

Status Reactor::GetMetrics(ReactorMetrics *metrics) {
  return RunOnReactorThread(boost::bind(&ReactorThread::GetMetrics, &thread_, metrics));
}

Status Reactor::RunOnReactorThread(const boost::function<Status()>& f) {
  RunFunctionTask task(f);
  ScheduleReactorTask(&task);
  return task.Wait();
}

Status Reactor::DumpRunningRpcs(const DumpRunningRpcsRequestPB& req,
                                DumpRunningRpcsResponsePB* resp) {
  return RunOnReactorThread(boost::bind(&ReactorThread::DumpRunningRpcs, &thread_,
                                        boost::ref(req), resp));
}

class RegisterConnectionTask : public ReactorTask {
 public:
  explicit RegisterConnectionTask(scoped_refptr<Connection> conn)
      : conn_(std::move(conn)) {
  }

  void Run(ReactorThread* reactor) override {
    reactor->RegisterConnection(std::move(conn_));
    delete this;
  }

  void Abort(const Status& /*status*/) override {
    // We don't need to Shutdown the connection since it was never registered.
    // This is only used for inbound connections, and inbound connections will
    // never have any calls added to them until they've been registered.
    delete this;
  }

 private:
  scoped_refptr<Connection> conn_;
};

void Reactor::RegisterInboundSocket(Socket *socket, const Sockaddr& remote) {
  VLOG(3) << name_ << ": new inbound connection to " << remote.ToString();
  unique_ptr<Socket> new_socket(new Socket(socket->Release()));
  auto task = new RegisterConnectionTask(
      new Connection(&thread_, remote, std::move(new_socket), Connection::SERVER));
  ScheduleReactorTask(task);
}

// Task which runs in the reactor thread to assign an outbound call
// to a connection.
class AssignOutboundCallTask : public ReactorTask {
 public:
  explicit AssignOutboundCallTask(shared_ptr<OutboundCall> call)
      : call_(std::move(call)) {}

  void Run(ReactorThread* reactor) override {
    reactor->AssignOutboundCall(call_);
    delete this;
  }

  void Abort(const Status& status) override {
    // It doesn't matter what is the actual phase of the OutboundCall: just set
    // it to Phase::REMOTE_CALL to finalize the state of the call.
    call_->SetFailed(status, OutboundCall::Phase::REMOTE_CALL);
    delete this;
  }

 private:
  shared_ptr<OutboundCall> call_;
};

void Reactor::QueueOutboundCall(const shared_ptr<OutboundCall>& call) {
  DVLOG(3) << name_ << ": queueing outbound call "
           << call->ToString() << " to remote " << call->conn_id().remote().ToString();
  // Test cancellation when 'call_' is in 'READY' state.
  if (PREDICT_FALSE(call->ShouldInjectCancellation())) {
    QueueCancellation(call);
  }
  ScheduleReactorTask(new AssignOutboundCallTask(call));
}

class CancellationTask : public ReactorTask {
 public:
  explicit CancellationTask(shared_ptr<OutboundCall> call)
      : call_(std::move(call)) {}

  void Run(ReactorThread* reactor) override {
    reactor->CancelOutboundCall(call_);
    delete this;
  }

  void Abort(const Status& /*status*/) override {
    delete this;
  }

 private:
  shared_ptr<OutboundCall> call_;
};

void Reactor::QueueCancellation(const shared_ptr<OutboundCall>& call) {
  ScheduleReactorTask(new CancellationTask(call));
}

void Reactor::ScheduleReactorTask(ReactorTask *task) {
  {
    std::unique_lock<LockType> l(lock_);
    if (closing_) {
      // We guarantee the reactor lock is not taken when calling Abort().
      l.unlock();
      task->Abort(ShutdownError(false));
      return;
    }
    pending_tasks_.push_back(*task);
  }
  thread_.WakeThread();
}

bool Reactor::DrainTaskQueue(boost::intrusive::list<ReactorTask> *tasks) { // NOLINT(*)
  std::lock_guard<LockType> l(lock_);
  if (closing_) {
    return false;
  }
  tasks->swap(pending_tasks_);
  return true;
}

} // namespace rpc
} // namespace kudu
