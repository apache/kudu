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
#include "client/session-internal.h"
#include "client/write_op.h"
#include "common/encoded_key.h"
#include "common/row_operations.h"
#include "common/wire_protocol.h"
#include "gutil/map-util.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "rpc/messenger.h"
#include "tserver/tserver_service.proxy.h"
#include "util/debug-util.h"

using kudu::rpc::ErrorStatusPB;
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
  gscoped_ptr<KuduWriteOperation> write_op;

  gscoped_ptr<EncodedKey> key;

  // The tablet the operation is destined for.
  // This is only filled in after passing through the kLookingUpTablet state.
  scoped_refptr<RemoteTablet> tablet;

  string ToString() const {
    return strings::Substitute("op[state=$0, write_op=$1]",
                               state, write_op->ToString());
  }
};

// An RPC which is in-flight to the remote server.
//
// Keeps a reference on the owning batcher while alive.
struct InFlightRpc {
  explicit InFlightRpc(Batcher* batcher, RemoteTabletServer* dst_ts)
    : batcher(batcher),
      ts(dst_ts),
      attempt(0) {
  }

  ~InFlightRpc() {
    STLDeleteElements(&ops);
  }

  scoped_refptr<Batcher> batcher;
  RemoteTabletServer* ts;
  int attempt;
  RpcController controller;
  WriteRequestPB request;
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
    outstanding_lookups_(0) {
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

  if (!flush_callback_.is_null()) {
    l.unlock();

    flush_callback_.Run(Status::Aborted(""));
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
  timeout_ = MonoDelta::FromMilliseconds(millis);
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
    session->data_->FlushFinished(this);
  }

  Status s;
  if (had_errors_) {
    // User is responsible for fetching errors from the error collector.
    s = Status::IOError("Some errors occurred");
  }

  flush_callback_.Run(s);
}

void Batcher::FlushAsync(const StatusCallback& cb) {
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    CHECK_EQ(state_, kGatheringOps);
    state_ = kFlushing;
    flush_callback_ = cb;
    if (timeout_.Initialized()) {
      deadline_ = MonoTime::Now(MonoTime::FINE);
      deadline_.AddDelta(timeout_);
    }
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
  FlushBuffersIfReady();
}

Status Batcher::Add(gscoped_ptr<KuduWriteOperation> write_op) {
  // As soon as we get the op, start looking up where it belongs,
  // so that when the user calls Flush, we are ready to go.
  InFlightOp* op = new InFlightOp;
  op->write_op.reset(write_op.release());
  op->state = InFlightOp::kLookingUpTablet;
  op->key = op->write_op->CreateKey();

  AddInFlightOp(op);
  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Looking up tablet for " << op->write_op->ToString();
  }

  // Increment our reference count for the outstanding callback.
  base::RefCountInc(&outstanding_lookups_);
  client_->meta_cache_->LookupTabletByKey(op->write_op->table(),
                                          op->key->encoded_key(),
                                          &op->tablet,
                                          base::Bind(&Batcher::TabletLookupFinished, this, op));
  return Status::OK();
}

void Batcher::AddInFlightOp(InFlightOp* op) {
  DCHECK_EQ(op->state, InFlightOp::kLookingUpTablet);

  boost::lock_guard<simple_spinlock> l(lock_);
  CHECK_EQ(state_, kGatheringOps);
  InsertOrDie(&ops_, op);
}

bool Batcher::IsAbortedUnlocked() const {
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
  gscoped_ptr<KuduError> error(new KuduError(op->write_op.Pass(), s));
  error_collector_->AddError(error.Pass());
  had_errors_ = true;
  delete op;
}

void Batcher::TabletLookupFinished(InFlightOp* op, const Status& s) {
  base::RefCountDec(&outstanding_lookups_);

  // Acquire the batcher lock early to atomically:
  // 1. Test if the batcher was aborted, and
  // 2. Change the op state.
  boost::unique_lock<simple_spinlock> l(lock_);

  if (IsAbortedUnlocked()) {
    VLOG(1) << "Aborted batch: TabletLookupFinished for " << op->write_op->ToString();
    MarkInFlightOpFailedUnlocked(op, Status::Aborted("Batch aborted"));
    // 'op' is deleted by above function.
    return;
  }

  if (VLOG_IS_ON(2)) {
    VLOG(2) << "TabletLookupFinished for " << op->write_op->ToString()
            << ": " << s.ToString();
    if (s.ok()) {
      VLOG(2) << "Result: tablet_id = " << op->tablet->tablet_id();
    }
  }

  if (!s.ok()) {
    MarkInFlightOpFailedUnlocked(op, s);
    l.unlock();
    CheckForFinishedFlush();
    return;
  }

  boost::unique_lock<simple_spinlock> l2(op->lock_);

  RemoteTabletServer* ts = op->tablet->LeaderTServer();
  if (PREDICT_FALSE(ts == NULL)) {
    VLOG(1) << "No LEADER found for tablet " << op->tablet->tablet_id()
            << ": failing. TODO: retry instead";
    Status err = Status::ServiceUnavailable(
      "No LEADER for tablet", op->tablet->tablet_id());
    l2.unlock();
    l.unlock();
    MarkInFlightOpFailed(op, err);
    CheckForFinishedFlush();
    return;
  }

  // We've figured out which tablet the row falls under.
  // Next, we need to find the current locations of that tablet.
  CHECK_EQ(op->state, InFlightOp::kLookingUpTablet);
  CHECK(op->tablet != NULL);

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
    ts->RefreshProxy(client_,
                     base::Bind(&Batcher::RefreshTSProxyFinished, this, ts, buf),
                     false);
  } else {
    FlushBuffersIfReady();
  }
}

void Batcher::RefreshTSProxyFinished(RemoteTabletServer* ts, PerTSBuffer* buf,
                                     const Status& status) {
  CHECK(status.ok()) << "Failed to refresh TS proxy. TODO: handle this";
  buf->SetProxyReady();
  FlushBuffersIfReady();
}


void Batcher::FlushBuffersIfReady() {
  unordered_map<RemoteTabletServer*, PerTSBuffer*> bufs_copy;

  // We're only ready to flush if:
  // 1. The batcher is in the flushing state (i.e. FlushAsync was called).
  // 2. All outstanding ops have finished lookup. Why? To avoid a situation
  //    where ops are flushed one by one as they finish lookup.
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    if (state_ != kFlushing) {
      VLOG(2) << "FlushBuffersIfReady: batcher not yet in flushing state";
      return;
    }
    if (!base::RefCountIsZero(&outstanding_lookups_)) {
      VLOG(2) << "FlushBuffersIfReady: "
              << base::subtle::NoBarrier_Load(&outstanding_lookups_)
              << " ops still in lookup";
      return;
    }
    // Copy the list of buffers while we're under the lock.
    bufs_copy = per_ts_buffers_;
  }

  // Now check each PerTSBuffer.
  typedef std::pair<RemoteTabletServer*, PerTSBuffer*> entry_type;
  BOOST_FOREACH(const entry_type& e, bufs_copy) {
    RemoteTabletServer* ts = e.first;
    PerTSBuffer* buf = e.second;

    if (!buf->ProxyReady()) {
      VLOG(2) << "FlushBuffersIfReady: proxy for " << ts->ToString()
              << " not yet ready";
      return;
    }

    VLOG(2) << "FlushBuffersIfReady: already in flushing state, immediately flushing to "
            << ts->ToString();
    FlushBuffer(ts, buf);
  }
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
      rpc = new InFlightRpc(this, ts);
      InsertOrDie(&rpcs, op->tablet.get(), rpc);
    }
    rpc->ops.push_back(op);
    DCHECK_EQ(op->write_op->table(), rpc->ops[0]->write_op->table());
  }

  // For each tablet, create and send an RPC.
  typedef pair<RemoteTablet*, InFlightRpc*> entry_type;
  BOOST_FOREACH(const entry_type& entry, rpcs) {
    InFlightRpc* rpc = entry.second;
    // All of the ops for a given tablet obviously correspond to the same table,
    // so we'll just grab the table from the first.
    const KuduTable* table = rpc->ops[0]->write_op->table();
    const Schema* schema = table->schema().schema_.get();
    const RemoteTablet* tablet = entry.first;

    rpc->request.Clear();
    rpc->request.set_tablet_id(tablet->tablet_id());
    // Set up schema

    CHECK_OK(SchemaToPB(*schema, rpc->request.mutable_schema()));

    RowOperationsPB* requested = rpc->request.mutable_row_operations();

    // Add the rows
    int ctr = 0;
    RowOperationsPBEncoder enc(requested);
    BOOST_FOREACH(InFlightOp* op, rpc->ops) {
      DCHECK(op->key->InRange(op->tablet->start_key(), op->tablet->end_key()))
          << "Row " << schema->DebugEncodedRowKey(op->key->encoded_key().ToString())
          << " not in range (" << schema->DebugEncodedRowKey(tablet->start_key().ToString())
          << ", " << schema->DebugEncodedRowKey(tablet->end_key().ToString())
          << ")";

      enc.Add(op->write_op->RowOperationType(), op->write_op->row());

      // Set the state now, even though we haven't yet sent it -- at this point
      // there is no return, and we're definitely going to send it. If we waited
      // until after we sent it, the RPC callback could fire before we got a chance
      // to change its state to 'sent'.
      op->state = InFlightOp::kRequestSent;
      VLOG(3) << ++ctr << ". Encoded row " << op->write_op->ToString();
    }

    if (VLOG_IS_ON(2)) {
      VLOG(2) << "Created batch for " << entry.first->tablet_id() << ":\n"
              << rpc->request.ShortDebugString();
    }

    SendRpc(rpc, Status::OK());
  }
}

void Batcher::SendRpc(InFlightRpc* rpc, const Status& s) {
  Status new_status = s;

  // Has this RPC timed out?
  if (deadline_.Initialized()) {
    MonoTime now = MonoTime::Now(MonoTime::FINE);
    if (deadline_.ComesBefore(now)) {
      new_status = Status::TimedOut("Write timed out");
    }
  }

  if (!new_status.ok()) {
    FinishRpc(rpc, new_status);
    return;
  }

  rpc->controller.Reset();

  // Compute the new RPC deadline, then send it.
  //
  // The 'rpc' object will be released by the callback.
  if (deadline_.Initialized()) {
    rpc->controller.set_deadline(deadline_);
  }
  rpc->ts->proxy()->WriteAsync(rpc->request, &rpc->response, &rpc->controller,
                               boost::bind(&Batcher::WriteRpcFinished, this, rpc));
}

void Batcher::WriteRpcFinished(InFlightRpc* rpc) {
  // TODO: there is a potential race here -- if the Batcher gets destructed while
  // RPCs are in-flight, then accessing state_ will crash. We probably need to keep
  // track of the in-flight RPCs, and in the destructor, change each of them to an
  // "aborted" state.
  CHECK_EQ(state_, kFlushing);

  // Must extract the actual "too busy" error from the ErrorStatusPB.
  Status s = rpc->controller.status();
  bool retry = false;
  if (s.IsRemoteError()) {
    const ErrorStatusPB* err = rpc->controller.error_response();
    if (err &&
        err->has_code() &&
        err->code() == ErrorStatusPB::ERROR_SERVER_TOO_BUSY) {
      retry = true;
    }
  }
  if (s.ok() && rpc->response.has_error()) {
    s = StatusFromPB(rpc->response.error().status());
  }
  if (s.ok()) {
    VLOG(2) << "Write RPC success: " << rpc->response.DebugString();
  } else {
    VLOG(2) << "Write RPC failed: " << s.ToString();

    if (retry) {
      // Retry provided we haven't exceeded the deadline.
      if (!deadline_.Initialized() || // no deadline --> retry forever
          MonoTime::Now(MonoTime::FINE).ComesBefore(deadline_)) {
        // Add some jitter to the retry delay.
        //
        // If the delay causes us to miss our deadline, SendRpc will fail
        // the RPC on our behalf.
        int num_ms = ++rpc->attempt + ((rand() % 5));
        client_->messenger_->ScheduleOnReactor(
            boost::bind(&Batcher::SendRpc, this, rpc, _1),
            MonoDelta::FromMilliseconds(num_ms));
        return;
      } else {
        VLOG(2) <<
            strings::Substitute("Failing RPC to $0, deadline exceeded: $1",
                                rpc->ts->ToString(), deadline_.ToString());
      }
    }
  }

  FinishRpc(rpc, s);
}

void Batcher::FinishRpc(InFlightRpc* rpc, const Status& s) {
  // Make sure after this method that the ops and the RPC are deleted.
  gscoped_ptr<InFlightRpc> scoped_rpc(rpc);

  if (!s.ok()) {
    // Mark each of the rows in the write op as failed, since the whole RPC failed.
    BOOST_FOREACH(InFlightOp* op, rpc->ops) {
      gscoped_ptr<KuduError> error(new KuduError(op->write_op.Pass(), s));
      error_collector_->AddError(error.Pass());
    }

    MarkHadErrors();
  }


  // Remove all the ops from the "in-flight" list.
  {
    boost::lock_guard<simple_spinlock> l(lock_);
    BOOST_FOREACH(InFlightOp* op, rpc->ops) {
      CHECK_EQ(1, ops_.erase(op))
            << "Could not remove op " << op->ToString()
            << " from in-flight list";
    }
  }

  // Check individual row errors.
  BOOST_FOREACH(const WriteResponsePB_PerRowErrorPB& err_pb, rpc->response.per_row_errors()) {
    // TODO: handle case where we get one of the more specific TS errors
    // like the tablet not being hosted, or too busy?

    if (err_pb.row_index() >= rpc->ops.size()) {
      LOG(ERROR) << "Received a per_row_error for an out-of-bound op index "
                 << err_pb.row_index() << " (sent only "
                 << rpc->ops.size() << " ops)";
      LOG(ERROR) << "Response from TS " << rpc->ts->ToString() << ":\n"
                 << rpc->response.DebugString();
      continue;
    }
    gscoped_ptr<KuduWriteOperation> op = rpc->ops[err_pb.row_index()]->write_op.Pass();
    VLOG(1) << "Error on op " << op->ToString() << ": "
            << err_pb.error().ShortDebugString();
    Status op_status = StatusFromPB(err_pb.error());
    gscoped_ptr<KuduError> error(new KuduError(op.Pass(), op_status));
    error_collector_->AddError(error.Pass());
    MarkHadErrors();
  }

  CheckForFinishedFlush();
}

} // namespace internal
} // namespace client
} // namespace kudu
