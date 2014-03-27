// Copyright (c) 2013, Cloudera, inc.

#include "client/batcher.h"

#include <boost/bind.hpp>
#include <boost/thread/locks.hpp>
#include <glog/logging.h>
#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <utility>
#include <vector>

#include "client/client.h"
#include "client/error_collector.h"
#include "client/meta_cache.h"
#include "common/wire_protocol.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "tserver/tserver_service.proxy.h"
#include "util/debug-util.h"

using kudu::rpc::RpcController;
using kudu::tserver::WriteRequestPB;
using kudu::tserver::WriteResponsePB;
using kudu::tserver::WriteResponsePB_PerRowErrorPB;

using std::pair;
using std::tr1::unordered_map;

namespace kudu {
namespace client {
namespace internal {

// About lock ordering in this file:
// ------------------------------
// The locks must be acquired in the following order:
//   - Batcher::lock_
//   - InFlightOp::lock_
//
// The PerTSBuffer::lock_ instance is below the other locks, and is safe
// to take at any point, because we never take further locks while holding it.
//
// It's generally important to release all the locks before either calling
// a user callback, or chaining to another async function, since that function
// may also chain directly to the callback. Without releasing locks first,
// the lock ordering may be violated, or a lock may deadlock on itself (these
// locks are non-reentrant).
// ------------------------------------------------------------

// An operation which has been submitted to the batcher and not yet completed.
// The operation goes through a state machine as it progress through the
// various stages of a request. See the State enum for details.
//
// Note that in-flight ops *conceptually* hold a reference to the Batcher object.
// However, since there might be millions of these objects floating around,
// we can save a pointer per object by manually incrementing the Batcher ref-count
// when we create the object, and decrementing when we delete it.
struct InFlightOp {
  InFlightOp() : state(kNew) {
  }

  // Lock protecting the internal state of the op.
  // This is necessary since callbacks may fire from IO threads
  // concurrent with the user trying to abort/delete the batch.
  // See comment above about lock ordering.
  simple_spinlock lock_;

  enum State {
    // Newly created op.
    //
    // OWNERSHIP: The op is only in this state when in local function scope (Batcher::Add)
    kNew = 0,

    // Waiting for the MetaCache to determine which tablet ID hosts the row associated
    // with this operation. In the case that the relevant tablet's key range was
    // already cached, this state will be passed through immediately. Otherwise,
    // the op may sit in this state for some amount of time while waiting on the
    // MetaCache to perform an RPC to the master and find the correct tablet.
    //
    // OWNERSHIP: the op is present in the 'ops_' set, and also referenced by the
    // in-flight callback provided to MetaCache.
    kLookingUpTablet,

    // Once the tablet ID has been determined, we may need to refresh its locations
    // (e.g if the tablet replicas have been moved by the master due to a failure
    // or normal load balancing activity).
    //
    // If the replica locations are already "fresh", this state will be passed through
    // with no waiting. Otherwise we wait on a potential master RPC.
    //
    // TODO: we could piggy-back the tablet locations with the "LookupTablet" stage above
    // and avoid going through this state in the normal case. Instead, we'd only
    // go to this state after receiving some error that the tablet has moved.
    //
    // OWNERSHIP: the op is present in 'ops_' and also referenced by the callback provided
    // to RemoteTablet::Refresh
    kRefreshingTabletLocations,

    // Once the correct tablet has been determined, and the tablet locations have been
    // refreshed, we are ready to send the operation to the server.
    //
    // In MANUAL_FLUSH mode, the operations wait in this state until Flush has been called.
    //
    // In AUTO_FLUSH_BACKGROUND mode, the operations may wait in this state for one of
    // two reasons:
    //
    //   1) There are already too many outstanding RPCs to the given tablet server.
    //
    //      We restrict the number of concurrent RPCs from one client to a given TS
    //      to achieve better batching and throughput.
    //      TODO: not implemented yet
    //
    //   2) Batching delay.
    //
    //      In order to achieve better batching, we do not immediately send a request
    //      to a TS as soon as we have one pending. Instead, we can wait for a configurable
    //      number of milliseconds for more requests to enter the queue for the same TS.
    //      This makes it likely that if a caller simply issues a small number of requests
    //      to the same tablet in AUTO_FLUSH_BACKGROUND mode that we'll batch all of the
    //      requests together in a single RPC.
    //      TODO: not implemented yet
    //
    // OWNERSHIP: When the operation is in this state, it is present in the 'ops_' set
    // and also in a PerTSBuffer's 'ops_' vector.
    kBufferedToTabletServer,

    // Once the operation has been flushed (either due to explicit Flush() or background flush)
    // it will enter this state.
    //
    // OWNERSHIP: when entering this state, the op is removed from PerTSBuffer's 'ops_'
    // vector and ownership is transfered to an InFlightRPC's 'ops_' vector. The op still
    // remains in the Batcher::ops_ set.
    kRequestSent
  };
  State state;

  // The actual operation.
  // TODO: once other operation batching is supported, this will be a superclass
  // reference.
  gscoped_ptr<Insert> insert;

  // The tablet the operation is destined for.
  // This is only filled in after passing through the kLookingUpTablet state.
  // TODO: we could save 8 bytes per op by making this scoped_refptr instead of a shared_ptr.
  std::tr1::shared_ptr<RemoteTablet> tablet;

  string ToString() const {
    return strings::Substitute("op[state=$0, insert=$1]",
                               state, insert->ToString());
  }
};

// An RPC which is in-flight to the remote server.
struct InFlightRpc {
  explicit InFlightRpc(RemoteTabletServer* dst_ts) :
    ts(dst_ts) {
  }

  ~InFlightRpc() {
    STLDeleteElements(&ops);
  }

  RemoteTabletServer* ts;
  RpcController controller;
  WriteResponsePB response;

  // Operations which were batched into this RPC.
  // These operations are in kRequestSent state.
  vector<InFlightOp*> ops;
};

// Buffer which corresponds to a particular tablet server.
// Contains ops which are in kBufferedToTabletServer or kRequestSent
// states.
//
// This class is thread-safe.
class PerTSBuffer {
 public:
  PerTSBuffer() :
    proxy_ready_(false) {
  }

  void AddOp(InFlightOp* op) {
    boost::lock_guard<simple_spinlock> l(lock_);
    DCHECK_EQ(op->state, InFlightOp::kBufferedToTabletServer);
    ops_.push_back(op);
  }

  void SetProxyReady() {
    boost::lock_guard<simple_spinlock> l(lock_);
    proxy_ready_ = true;
  }

  bool ProxyReady() const {
    boost::lock_guard<simple_spinlock> l(lock_);
    return proxy_ready_;
  }

  void TakeOps(vector<InFlightOp*>* ops) {
    boost::lock_guard<simple_spinlock> l(lock_);
    ops->clear();
    ops->swap(ops_);
  }

 private:
  // This lock is always safe to take - we never take any other locks while
  // we hold it.
  mutable simple_spinlock lock_;
  vector<InFlightOp*> ops_;
  bool proxy_ready_;

  DISALLOW_COPY_AND_ASSIGN(PerTSBuffer);
};

namespace {

// Simple class which releases a reference count on the given object upon
// its destructor.
template<class T>
class ScopedRefReleaser {
 public:
  explicit ScopedRefReleaser(T* obj) : obj_(obj) {}
  ~ScopedRefReleaser() {
    if (obj_) {
      obj_->Release();
    }
  }

  void cancel() {
    obj_ = NULL;
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(ScopedRefReleaser);
  T* obj_;
};
} // anonymous namespace

Batcher::Batcher(KuduClient* client,
                 ErrorCollector* error_collector,
                 const shared_ptr<KuduSession>& session)
  : state_(kGatheringOps),
    client_(client),
    weak_session_(session),
    error_collector_(error_collector),
    had_errors_(false),
    timeout_ms_(0) {
}

void Batcher::Abort() {
  boost::unique_lock<simple_spinlock> l(lock_);
  state_ = kAborted;

  vector<InFlightOp*> to_abort;
  BOOST_FOREACH(InFlightOp* op, ops_) {
    boost::lock_guard<simple_spinlock> l(op->lock_);
    if (op->state == InFlightOp::kBufferedToTabletServer) {
      to_abort.push_back(op);
    }
  }

  BOOST_FOREACH(InFlightOp* op, to_abort) {
    VLOG(1) << "Aborting op: " << op->ToString();
    MarkInFlightOpFailedUnlocked(op, Status::Aborted("Batch aborted"));
  }

  if (flush_callback_) {
    l.unlock();

    flush_callback_(Status::Aborted(""));
  }
}

Batcher::~Batcher() {
  CHECK(state_ == kFlushed || state_ == kAborted) << "Bad state: " << state_;

  if (PREDICT_FALSE(!ops_.empty())) {
    BOOST_FOREACH(InFlightOp* op, ops_) {
      LOG(ERROR) << "Orphaned op: " << op->ToString();
    }
    LOG(FATAL) << "ops_ not empty";
  }
  CHECK(ops_.empty());

  // Remove the per-TS buffers
  STLDeleteValues(&per_ts_buffers_);
}

void Batcher::SetTimeoutMillis(int millis) {
  CHECK_GE(millis, 0);
  boost::lock_guard<simple_spinlock> l(lock_);
  timeout_ms_ = millis;
}


bool Batcher::HasPendingOperations() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return !ops_.empty();
}

int Batcher::CountBufferedOperations() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  if (state_ == kGatheringOps) {
    return ops_.size();
  } else {
    // If we've already started to flush, then the ops aren't
    // considered "buffered".
    return 0;
  }
}

void Batcher::CheckForFinishedFlush() {
  shared_ptr<KuduSession> session;
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    if (state_ != kFlushing || !ops_.empty()) {
      return;
    }

    session = weak_session_.lock();
    state_ = kFlushed;
  }

  if (session) {
    // Important to do this outside of the lock so that we don't have
    // a lock inversion deadlock -- the session lock should always
    // come before the batcher lock.
    session->FlushFinished(this);
  }

  Status s;
  if (had_errors_) {
    // User is responsible for fetching errors from the error collector.
    s = Status::IOError("Some errors occurred");
  }

  flush_callback_(s);
}

void Batcher::FlushAsync(const StatusCallback& cb) {
  unordered_map<RemoteTabletServer*, PerTSBuffer*> bufs_copy;
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    CHECK_EQ(state_, kGatheringOps);
    state_ = kFlushing;
    flush_callback_ = cb;
    // Copy the list of buffers while we're under the lock.
    // We have to drop the lock since FlushBufferIfReady tries to acquire
    // it again, and it's not a recursive lock.
    bufs_copy = per_ts_buffers_;
  }

  // In the case that we have nothing buffered, just call the callback
  // immediately. Otherwise, the callback will be called by the last callback
  // when it sees that the ops_ list has drained.
  CheckForFinishedFlush();

  // Trigger flushing of all of the buffers. Some of these may already have
  // been flushed through an async path, but it's idempotent - a second call
  // to flush would just be a no-op.
  //
  // If some of the operations are still in-flight, then they'll get sent
  // when they hit the PerTSBuffer, since our state is now kFlushing.
  typedef std::pair<RemoteTabletServer*, PerTSBuffer*> entry_type;
  BOOST_FOREACH(const entry_type& e, bufs_copy) {
    FlushBufferIfReady(e.first, e.second);
  }
}

Status Batcher::Add(gscoped_ptr<Insert> insert) {
  // As soon as we get the op, start looking up where it belongs,
  // so that when the user calls Flush, we are ready to go.
  InFlightOp* op = new InFlightOp;
  op->insert.reset(insert.release());
  op->state = InFlightOp::kLookingUpTablet;

  AddInFlightOp(op);
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Looking up tablet for " << op->insert->ToString();
  }

  // Increment our reference count for the outstanding callback.
  AddRef();
  client_->meta_cache_->LookupTabletByRow(op->insert->table(), op->insert->row(), &op->tablet,
                                 boost::bind(&Batcher::TabletLookupFinished, this, op, _1));
  return Status::OK();
}

void Batcher::AddInFlightOp(InFlightOp* op) {
  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(state_, kGatheringOps);
  InsertOrDie(&ops_, op);
}

bool Batcher::IsAborted() const {
  boost::lock_guard<simple_spinlock> l(lock_);
  return state_ == kAborted;
}

void Batcher::MarkHadErrors() {
  boost::lock_guard<simple_spinlock> l(lock_);
  had_errors_ = true;
}

void Batcher::MarkInFlightOpFailed(InFlightOp* op, const Status& s) {
  boost::lock_guard<simple_spinlock> l(lock_);
  MarkInFlightOpFailedUnlocked(op, s);
}

void Batcher::MarkInFlightOpFailedUnlocked(InFlightOp* op, const Status& s) {
  CHECK_EQ(1, ops_.erase(op))
    << "Could not remove op " << op->ToString() << " from in-flight list";
  gscoped_ptr<Error> error(new Error(op->insert.Pass(), s));
  error_collector_->AddError(error.Pass());
  had_errors_ = true;
  delete op;
}

void Batcher::TabletLookupFinished(InFlightOp* op, const Status& s) {
  ScopedRefReleaser<Batcher> releaser(this);
  if (IsAborted()) {
    VLOG(1) << "Aborted batch: TabletLookupFinished for " << op->insert->ToString();
    MarkInFlightOpFailed(op, Status::Aborted("Batch aborted"));
    // 'op' is deleted by above function.
    return;
  }

  {
    boost::lock_guard<simple_spinlock> l(op->lock_);
    if (VLOG_IS_ON(2)) {
      VLOG(2) << "TabletLookupFinished for " << op->insert->ToString()
              << ": " << s.ToString();
      if (s.ok()) {
        VLOG(2) << "Result: tablet_id = " << op->tablet->tablet_id();
      }
    }

    // We've figured out which tablet the row falls under.
    // Next, we need to find the current locations of that tablet.
    CHECK(s.ok()) << "TODO: handle failed lookup of row";
    CHECK_EQ(op->state, InFlightOp::kLookingUpTablet);
    CHECK(op->tablet != NULL);
    op->state = InFlightOp::kRefreshingTabletLocations;
  }

  bool force = false;
  AddRef();
  op->tablet->Refresh(client_,
                      boost::bind(&Batcher::TabletRefreshFinished, this, op, _1),
                      force);
}

void Batcher::TabletRefreshFinished(InFlightOp* op, const Status& s) {
  ScopedRefReleaser<Batcher> releaser(this);
  if (IsAborted()) {
    VLOG(1) << "Aborted batch: TabletRefreshFinished for " << op->insert->ToString();
    MarkInFlightOpFailed(op, Status::Aborted("Batch aborted"));
    // 'op' is deleted by above function.
    return;
  }

  boost::unique_lock<simple_spinlock> l(lock_);
  boost::unique_lock<simple_spinlock> l2(op->lock_);
  CHECK_EQ(op->state, InFlightOp::kRefreshingTabletLocations);

  if (VLOG_IS_ON(2)) {
    VLOG(2) << "TabletRefreshFinished for " << op->insert->ToString()
            << ": " << s.ToString();
  }

  if (!s.ok()) {
    l2.unlock();
    l.unlock();
    MarkInFlightOpFailed(op, s);
    CheckForFinishedFlush();
    return;
  }

  // We've found the locations of the tablet.
  // Next we want to make sure we have DNS-resolved the TS.
  CHECK(s.ok()) << "TODO: handle failed lookup of tablet";

  RemoteTabletServer* ts = op->tablet->replica_tserver(0);
  CHECK(ts != NULL) << "TODO: handle no tablet locations";

  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Result: loc = " << ts->ToString();
  }

  bool needs_tsproxy_refresh = false;
  PerTSBuffer* buf = FindPtrOrNull(per_ts_buffers_, ts);
  if (buf == NULL) {
    VLOG(2) << "Creating new PerTSBuffer for " << ts->ToString();
    buf = new PerTSBuffer();
    InsertOrDie(&per_ts_buffers_, ts, buf);
    needs_tsproxy_refresh = true;
  }

  op->state = InFlightOp::kBufferedToTabletServer;
  buf->AddOp(op);

  l2.unlock();
  l.unlock();

  if (needs_tsproxy_refresh) {
    // Even if we're not ready to flush, we should get the proxy ready
    // to go (eg DNS resolving the tablet server if it's not resolved).
    AddRef();
    ts->RefreshProxy(client_,
                     boost::bind(&Batcher::RefreshTSProxyFinished, this, ts, buf, _1),
                     false);
  }

  FlushBufferIfReady(ts, buf);
}

void Batcher::RefreshTSProxyFinished(RemoteTabletServer* ts, PerTSBuffer* buf,
                                     const Status& status) {
  CHECK(status.ok()) << "Failed to refresh TS proxy. TODO: handle this";
  buf->SetProxyReady();
  FlushBufferIfReady(ts, buf);
  Release();
}


void Batcher::FlushBufferIfReady(RemoteTabletServer* ts, PerTSBuffer* buf) {
  // If we're already in "flushing" state, then we should send the op
  // immediately.
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    if (state_ != kFlushing) {
      return;
    }
  }

  if (!buf->ProxyReady()) {
    VLOG(2) << "FlushBufferIfReady: proxy not yet ready";
    return;
  }

  VLOG(2) << "FlushBufferIfReady: already in flushing state, immediately flushing to "
          << ts->ToString();
  FlushBuffer(ts, buf);
}

void Batcher::FlushBuffer(RemoteTabletServer* ts, PerTSBuffer* buf) {
  // Transfer all the ops out of the buffer. We now own the ops.
  vector<InFlightOp*> ops; // owns ops
  buf->TakeOps(&ops);

  // For each tablet, create a new InFlightRpc and gather all of the
  // ops into it. The InFlightRpcs take ownership of the ops.
  // The InFlightRpc objects themselves are freed when the RPC callback completes.
  unordered_map<RemoteTablet*, InFlightRpc*> rpcs;
  BOOST_FOREACH(InFlightOp* op, ops) {
    InFlightRpc* rpc = FindPtrOrNull(rpcs, op->tablet.get());
    if (!rpc) {
      rpc = new InFlightRpc(ts);
      InsertOrDie(&rpcs, op->tablet.get(), rpc);
    }
    rpc->ops.push_back(op);
    DCHECK_EQ(op->insert->table(), rpc->ops[0]->insert->table());
  }

  // For each tablet, create and send an RPC.
  WriteRequestPB req;
  typedef pair<RemoteTablet*, InFlightRpc*> entry_type;
  BOOST_FOREACH(const entry_type& entry, rpcs) {
    InFlightRpc* rpc = entry.second;
    // All of the ops for a given tablet obviously correspond to the same table,
    // so we'll just grab the table from the first.
    const KuduTable* table = rpc->ops[0]->insert->table();
    const Schema& schema = table->schema();

    req.Clear();
    req.set_tablet_id(entry.first->tablet_id());
    // Set up schema

    CHECK_OK(SchemaToPB(schema, req.mutable_schema()));

    RowOperationsPB* to_insert = req.mutable_to_insert_rows();

    // Add the rows
    BOOST_FOREACH(InFlightOp* op, rpc->ops) {
      op->insert->row().AppendToPB(RowOperationsPB::INSERT, to_insert);

      // Set the state now, even though we haven't yet sent it -- at this point
      // there is no return, and we're definitely going to send it. If we waited
      // until after we sent it, the RPC callback could fire before we got a chance
      // to change its state to 'sent'.
      op->state = InFlightOp::kRequestSent;
    }

    if (VLOG_IS_ON(2)) {
      VLOG(2) << "Created batch for " << entry.first->tablet_id() << ":\n"
              << req.ShortDebugString();
    }

    // TODO: the timeout should actually be a bit smarter -- i.e be the full span from
    // when the flush was called to when the response has to come.
    rpc->controller.set_timeout(MonoDelta::FromMilliseconds(timeout_ms_));
    // Actually send the RPC.
    // The 'rpc' object will be released by the callback.
    AddRef();
    ts->proxy()->WriteAsync(req, &rpc->response, &rpc->controller,
                            boost::bind(&Batcher::WriteRpcFinished, this, rpc));
  }
}

void Batcher::WriteRpcFinished(InFlightRpc* rpc) {
  ScopedRefReleaser<Batcher> releaser(this);

  // TODO: there is a potential race here -- if the Batcher gets destructed while
  // RPCs are in-flight, then accessing state_ will crash. We probably need to keep
  // track of the in-flight RPCs, and in the destructor, change each of them to an
  // "aborted" state.
  CHECK_EQ(state_, kFlushing);

  // Remove all the ops from the "in-flight" list
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    BOOST_FOREACH(InFlightOp* op, rpc->ops) {
      CHECK_EQ(1, ops_.erase(op))
        << "Could not remove op " << op->ToString() << " from in-flight list";
    }
  }

  // Make sure after this method that the ops and the RPC are deleted.
  gscoped_ptr<InFlightRpc> scoped_rpc(rpc);
  Status s = rpc->controller.status();
  if (s.ok() && rpc->response.has_error()) {
    s = StatusFromPB(rpc->response.error().status());
  }

  if (s.ok()) {
    VLOG(2) << "Write RPC success: " << rpc->response.DebugString();
  } else {
    VLOG(2) << "Write RPC failed: " << s.ToString();

    // TODO: need to handle various retry cases here -- eg re-fetching
    // the locations, re-resolving the TS, etc.

    // Mark each of the rows in the insert as failed, since the whole RPC
    // failed.
    BOOST_FOREACH(InFlightOp* op, rpc->ops) {
      gscoped_ptr<Error> error(new Error(op->insert.Pass(), s));
      error_collector_->AddError(error.Pass());
    }

    MarkHadErrors();
  }

  BOOST_FOREACH(const WriteResponsePB_PerRowErrorPB& err_pb, rpc->response.per_row_errors()) {
    // TODO: handle case where we get one of the more specific TS errors
    // like the tablet not being hosted, or too busy?

    if (err_pb.row_index() >= rpc->ops.size()) {
      LOG(ERROR) << "Received a per_row_error for an out-of-bound op index " << err_pb.row_index()
                 << " (sent only " << rpc->ops.size() << " ops)";
      LOG(ERROR) << "Response from TS " << rpc->ts->ToString() << ":\n" <<
        rpc->response.DebugString();
      continue;
    }
    gscoped_ptr<Insert> op = rpc->ops[err_pb.row_index()]->insert.Pass();
    VLOG(1) << "Error on op " << op->ToString() << ": " << err_pb.error().ShortDebugString();
    Status op_status = StatusFromPB(err_pb.error());
    gscoped_ptr<Error> error(new Error(op.Pass(), op_status));
    error_collector_->AddError(error.Pass());
    MarkHadErrors();
  }

  CheckForFinishedFlush();
}

} // namespace internal
} // namespace client
} // namespace kudu
