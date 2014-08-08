// Copyright (c) 2013, Cloudera, inc.

#include "kudu/client/batcher.h"

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <utility>
#include <vector>

#include "kudu/client/client.h"
#include "kudu/client/client-internal.h"
#include "kudu/client/error_collector.h"
#include "kudu/client/meta_cache.h"
#include "kudu/client/rpc.h"
#include "kudu/client/session-internal.h"
#include "kudu/client/write_op.h"
#include "kudu/client/write_op-internal.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/row_operations.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/tserver/tserver_service.proxy.h"
#include "kudu/util/debug-util.h"

using std::pair;
using std::tr1::unordered_map;
using strings::Substitute;

namespace kudu {

using rpc::ErrorStatusPB;
using rpc::Messenger;
using rpc::RpcController;
using tserver::WriteRequestPB;
using tserver::WriteResponsePB;
using tserver::WriteResponsePB_PerRowErrorPB;

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
    // vector and ownership is transfered to a WriteRPC's 'ops_' vector. The op still
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

// A Write RPC which is in-flight to a tablet server.
//
// Keeps a reference on the owning batcher while alive.
class WriteRpc : public Rpc {
 public:
  WriteRpc(const scoped_refptr<Batcher>& batcher,
           const RemoteTabletServer* ts,
           const vector<InFlightOp*>& ops,
           const MonoTime& deadline,
           const shared_ptr<Messenger>& messenger);
  virtual ~WriteRpc();
  virtual void SendRpc() OVERRIDE;
  virtual string ToString() const OVERRIDE;

  const RemoteTabletServer* ts() const { return ts_; }
  const vector<InFlightOp*>& ops() const { return ops_; }
  const WriteResponsePB& resp() const { return resp_; }

 private:
  virtual void SendRpcCb(const Status& status) OVERRIDE;

  // Pointer back to the batcher. Processes the write response when it
  // completes, regardless of success or failure.
  scoped_refptr<Batcher> batcher_;

  // Proxy to the tablet server servicing this write.
  const RemoteTabletServer* ts_;

  // Request body.
  WriteRequestPB req_;

  // Response body.
  WriteResponsePB resp_;

  // Operations which were batched into this RPC.
  // These operations are in kRequestSent state.
  vector<InFlightOp*> ops_;
};

WriteRpc::WriteRpc(const scoped_refptr<Batcher>& batcher,
                   const RemoteTabletServer* ts,
                   const vector<InFlightOp*>& ops,
                   const MonoTime& deadline,
                   const shared_ptr<Messenger>& messenger)
  : Rpc(deadline, messenger),
    batcher_(batcher),
    ts_(ts),
    ops_(ops) {

  // All of the ops for a given tablet obviously correspond to the same table,
  // so we'll just grab the table from the first.
  const KuduTable* table = ops_[0]->write_op->table();
  const Schema* schema = table->schema().schema_.get();
  const RemoteTablet* tablet = ops_[0]->tablet.get();

  req_.set_tablet_id(tablet->tablet_id());
  // Set up schema

  CHECK_OK(SchemaToPB(*schema, req_.mutable_schema()));

  RowOperationsPB* requested = req_.mutable_row_operations();

  // Add the rows
  int ctr = 0;
  RowOperationsPBEncoder enc(requested);
  BOOST_FOREACH(InFlightOp* op, ops_) {
    DCHECK(op->key->InRange(op->tablet->start_key(), op->tablet->end_key()))
                << "Row " << schema->DebugEncodedRowKey(op->key->encoded_key().ToString())
                << " not in range (" << schema->DebugEncodedRowKey(tablet->start_key().ToString())
                << ", " << schema->DebugEncodedRowKey(tablet->end_key().ToString())
                << ")";

    enc.Add(ToInternalWriteType(op->write_op->type()), op->write_op->row());

    // Set the state now, even though we haven't yet sent it -- at this point
    // there is no return, and we're definitely going to send it. If we waited
    // until after we sent it, the RPC callback could fire before we got a chance
    // to change its state to 'sent'.
    op->state = InFlightOp::kRequestSent;
    VLOG(3) << ++ctr << ". Encoded row " << op->write_op->ToString();
  }

  if (VLOG_IS_ON(2)) {
    VLOG(2) << "Created batch for " << tablet->tablet_id() << ":\n"
        << req_.ShortDebugString();
  }
}

WriteRpc::~WriteRpc() {
  STLDeleteElements(&ops_);
}

void WriteRpc::SendRpc() {
  ts_->proxy()->WriteAsync(req_, &resp_, &retrier().controller(),
                           boost::bind(&WriteRpc::SendRpcCb, this, Status::OK()));
}

string WriteRpc::ToString() const {
  return Substitute("Write($0, $1 ops)", ts_->ToString(), ops_.size());
}

void WriteRpc::SendRpcCb(const Status& status) {
  gscoped_ptr<WriteRpc> delete_me(this); // delete on scope exit

  // Prefer early failures over controller failures.
  Status new_status = status;
  if (new_status.ok() && retrier().HandleResponse(this, &new_status)) {
    ignore_result(delete_me.release());
    return;
  }

  // Prefer controller failures over response failures.
  if (new_status.ok() && resp_.has_error()) {
    new_status = StatusFromPB(resp_.error().status());
  }

  if (!new_status.ok()) {
    LOG(WARNING) << ToString() << " failed: " << new_status.ToString();
  }
  batcher_->ProcessWriteResponse(*this, new_status);
}

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
    lock_guard<simple_spinlock> l(&lock_);
    DCHECK_EQ(op->state, InFlightOp::kBufferedToTabletServer);
    ops_.push_back(op);
  }

  void SetProxyReady() {
    lock_guard<simple_spinlock> l(&lock_);
    proxy_ready_ = true;
  }

  bool ProxyReady() const {
    lock_guard<simple_spinlock> l(&lock_);
    return proxy_ready_;
  }

  void TakeOps(vector<InFlightOp*>* ops) {
    lock_guard<simple_spinlock> l(&lock_);
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
  unique_lock<simple_spinlock> l(&lock_);
  state_ = kAborted;

  vector<InFlightOp*> to_abort;
  BOOST_FOREACH(InFlightOp* op, ops_) {
    lock_guard<simple_spinlock> l(&op->lock_);
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
  lock_guard<simple_spinlock> l(&lock_);
  timeout_ = MonoDelta::FromMilliseconds(millis);
}


bool Batcher::HasPendingOperations() const {
  lock_guard<simple_spinlock> l(&lock_);
  return !ops_.empty();
}

int Batcher::CountBufferedOperations() const {
  lock_guard<simple_spinlock> l(&lock_);
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
    lock_guard<simple_spinlock> l(&lock_);
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
    lock_guard<simple_spinlock> l(&lock_);
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
  client_->data_->meta_cache_->LookupTabletByKey(op->write_op->table(),
                                                 op->key->encoded_key(),
                                                 &op->tablet,
                                                 Bind(&Batcher::TabletLookupFinished,
                                                      this, op));
  return Status::OK();
}

void Batcher::AddInFlightOp(InFlightOp* op) {
  DCHECK_EQ(op->state, InFlightOp::kLookingUpTablet);

  lock_guard<simple_spinlock> l(&lock_);
  CHECK_EQ(state_, kGatheringOps);
  InsertOrDie(&ops_, op);
}

bool Batcher::IsAbortedUnlocked() const {
  return state_ == kAborted;
}

void Batcher::MarkHadErrors() {
  lock_guard<simple_spinlock> l(&lock_);
  had_errors_ = true;
}

void Batcher::MarkInFlightOpFailed(InFlightOp* op, const Status& s) {
  lock_guard<simple_spinlock> l(&lock_);
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
  unique_lock<simple_spinlock> l(&lock_);

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

  unique_lock<simple_spinlock> l2(&op->lock_);

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
                     Bind(&Batcher::RefreshTSProxyFinished, this, ts, buf),
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
    lock_guard<simple_spinlock> l(&lock_);
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

  // Partition the ops into per-tablet groups.
  unordered_map<RemoteTablet*, vector<InFlightOp*> > tablet_to_ops;
  BOOST_FOREACH(InFlightOp* op, ops) {
    vector<InFlightOp*>& tablet_ops = LookupOrInsert(&tablet_to_ops,
                                                     op->tablet.get(),
                                                     vector<InFlightOp*>());
    tablet_ops.push_back(op);
  }

  // Create and send an RPC that aggregates each tablet's ops. Each RPC is
  // freed when its callback completes.
  //
  // The RPC object takes ownership of its ops.
  typedef pair<RemoteTablet*, vector<InFlightOp*> > entry_type;
  BOOST_FOREACH(const entry_type& entry, tablet_to_ops) {
    const vector<InFlightOp*>& tablet_ops = entry.second;

    WriteRpc* rpc = new WriteRpc(this,
                                 ts,
                                 tablet_ops,
                                 deadline_,
                                 client_->data_->messenger_);
    rpc->SendRpc();
  }
}

void Batcher::ProcessWriteResponse(const WriteRpc& rpc,
                                   const Status& s) {
  // TODO: there is a potential race here -- if the Batcher gets destructed while
  // RPCs are in-flight, then accessing state_ will crash. We probably need to keep
  // track of the in-flight RPCs, and in the destructor, change each of them to an
  // "aborted" state.
  CHECK_EQ(state_, kFlushing);

  if (!s.ok()) {
    // Mark each of the rows in the write op as failed, since the whole RPC failed.
    BOOST_FOREACH(InFlightOp* op, rpc.ops()) {
      gscoped_ptr<KuduError> error(new KuduError(op->write_op.Pass(), s));
      error_collector_->AddError(error.Pass());
    }

    MarkHadErrors();
  }


  // Remove all the ops from the "in-flight" list.
  {
    lock_guard<simple_spinlock> l(&lock_);
    BOOST_FOREACH(InFlightOp* op, rpc.ops()) {
      CHECK_EQ(1, ops_.erase(op))
            << "Could not remove op " << op->ToString()
            << " from in-flight list";
    }
  }

  // Check individual row errors.
  BOOST_FOREACH(const WriteResponsePB_PerRowErrorPB& err_pb, rpc.resp().per_row_errors()) {
    // TODO: handle case where we get one of the more specific TS errors
    // like the tablet not being hosted?

    if (err_pb.row_index() >= rpc.ops().size()) {
      LOG(ERROR) << "Received a per_row_error for an out-of-bound op index "
                 << err_pb.row_index() << " (sent only "
                 << rpc.ops().size() << " ops)";
      LOG(ERROR) << "Response from TS " << rpc.ts()->ToString() << ":\n"
                 << rpc.resp().DebugString();
      continue;
    }
    gscoped_ptr<KuduWriteOperation> op = rpc.ops()[err_pb.row_index()]->write_op.Pass();
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
