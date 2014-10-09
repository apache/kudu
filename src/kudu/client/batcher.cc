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
    // and also in the 'per_tablet_ops' map.
    kBufferedToTabletServer,

    // Once the operation has been flushed (either due to explicit Flush() or background flush)
    // it will enter this state.
    //
    // OWNERSHIP: when entering this state, the op is removed from 'per_tablet_ops' map
    // and ownership is transfered to a WriteRPC's 'ops_' vector. The op still
    // remains in the 'ops_' set.
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

// A Write RPC which is in-flight to a tablet. Initially, the RPC is sent
// to the leader replica, but it may be retried with another replica if the
// leader fails.
//
// Keeps a reference on the owning batcher while alive.
class WriteRpc : public Rpc {
 public:
  WriteRpc(const scoped_refptr<Batcher>& batcher,
           RemoteTablet* const tablet,
           const vector<InFlightOp*>& ops,
           const MonoTime& deadline,
           const shared_ptr<Messenger>& messenger);
  virtual ~WriteRpc();
  virtual void SendRpc() OVERRIDE;
  virtual string ToString() const OVERRIDE;

  const RemoteTablet* tablet() const { return tablet_; }
  const vector<InFlightOp*>& ops() const { return ops_; }
  const WriteResponsePB& resp() const { return resp_; }

 private:
  // Called when we refresh a TS proxy. Sends the RPC, provided there was
  // no error.
  void RefreshTSProxyFinished(const Status& status);

  // Marks all replicas on current_ts_ as failed and retries the write on a
  // new replica.
  void FailToNewReplica(const Status& reason);

  virtual void SendRpcCb(const Status& status) OVERRIDE;

  // Pointer back to the batcher. Processes the write response when it
  // completes, regardless of success or failure.
  scoped_refptr<Batcher> batcher_;

  // The tablet that should receive this write.
  RemoteTablet* const tablet_;

  // The TS receiving the write. May change if the write is retried.
  RemoteTabletServer* current_ts_;

  // The last write failure seen, if any.
  Status last_failure_;

  // Request body.
  WriteRequestPB req_;

  // Response body.
  WriteResponsePB resp_;

  // Operations which were batched into this RPC.
  // These operations are in kRequestSent state.
  vector<InFlightOp*> ops_;
};

WriteRpc::WriteRpc(const scoped_refptr<Batcher>& batcher,
                   RemoteTablet* const tablet,
                   const vector<InFlightOp*>& ops,
                   const MonoTime& deadline,
                   const shared_ptr<Messenger>& messenger)
  : Rpc(deadline, messenger),
    batcher_(batcher),
    tablet_(tablet),
    current_ts_(NULL),
    ops_(ops) {

  // All of the ops for a given tablet obviously correspond to the same table,
  // so we'll just grab the table from the first.
  const KuduTable* table = ops_[0]->write_op->table();
  const Schema* schema = table->schema().schema_.get();

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
  // Choose a destination TS. The leader is (obviously) preferred, but
  // we'll try a random replica if the leader failed; it's possible that
  // it is now the new leader.
  //
  // TODO: Guessing which replica is the new leader is wasteful; we should
  // ask the master who it is (repeatedly, until the master knows or our
  // deadline expires).
  current_ts_ = tablet_->LeaderTServer();
  if (!current_ts_) {
    vector<RemoteTabletServer*> candidates;
    tablet_->GetRemoteTabletServers(&candidates);
    if (!candidates.empty()) {
      current_ts_ = candidates[rand() % candidates.size()];
    }
  }
  if (!current_ts_) {
    gscoped_ptr<WriteRpc> delete_me(this);
    VLOG(1) << "No LEADER found for tablet " << tablet_->tablet_id()
            << ": failing. TODO: ask Master for new TSes";
    Status s = !last_failure_.ok() ? last_failure_ :
        Status::ServiceUnavailable("No LEADER for tablet", tablet_->tablet_id());

    // Skip the retry logic in SendRpcCb(); if last_failure_ exists it'll
    // lead to an infinite retry loop.
    batcher_->ProcessWriteResponse(*this, s);
    return;
  }

  // Make sure we have a working proxy before sending out the RPC.
  current_ts_->RefreshProxy(batcher_->client_,
                            Bind(&WriteRpc::RefreshTSProxyFinished, Unretained(this)),
                            false);
}

string WriteRpc::ToString() const {
  return Substitute("Write($0, $1 ops)", tablet_->tablet_id(), ops_.size());
}

void WriteRpc::RefreshTSProxyFinished(const Status& status) {
  // Fail to a replica in the event of a DNS resolution failure.
  if (!status.ok()) {
    FailToNewReplica(status);
    return;
  }

  current_ts_->proxy()->WriteAsync(req_, &resp_, &retrier().controller(),
                                   boost::bind(&WriteRpc::SendRpcCb, this, Status::OK()));
}

void WriteRpc::FailToNewReplica(const Status& reason) {
  VLOG(1) << "Failing " << ToString() << " to a new replica: "
          << reason.ToString();
  tablet_->MarkReplicaFailed(current_ts_, reason);
  last_failure_ = reason;
  retrier().DelayedRetry(this);
}

void WriteRpc::SendRpcCb(const Status& status) {
  // Prefer early failures over controller failures.
  Status new_status = status;
  if (new_status.ok() && retrier().HandleResponse(this, &new_status)) {
    return;
  }

  // Failover to a replica in the event of any network failure.
  //
  // TODO: This is probably too harsh; some network failures should be
  // retried on the current replica.
  if (new_status.IsNetworkError()) {
    FailToNewReplica(new_status);
    return;
  }

  // Prefer controller failures over response failures.
  if (new_status.ok() && resp_.has_error()) {
    new_status = StatusFromPB(resp_.error().status());
  }

  // Oops, we failed over to a replica that wasn't a LEADER. Try again.
  //
  // TODO: IllegalState is obviously way too broad an error category for
  // this case.
  if (new_status.IsIllegalState()) {
    retrier().DelayedRetry(this);
    return;
  }

  if (!new_status.ok()) {
    LOG(WARNING) << ToString() << " failed: " << new_status.ToString();
  }
  batcher_->ProcessWriteResponse(*this, new_status);
  delete this;
}

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
  // when they hit 'per_tablet_ops', since our state is now kFlushing.
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
  //
  // deadline_ is set in FlushAsync(), after all Add() calls are done, so
  // here we're forced to create a new deadline.
  MonoTime deadline = MonoTime::Now(MonoTime::FINE);
  deadline.AddDelta(timeout_);
  base::RefCountInc(&outstanding_lookups_);
  client_->data_->meta_cache_->LookupTabletByKey(op->write_op->table(),
                                                 op->key->encoded_key(),
                                                 deadline,
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

  {
    lock_guard<simple_spinlock> l2(&op->lock_);
    CHECK_EQ(op->state, InFlightOp::kLookingUpTablet);
    CHECK(op->tablet != NULL);

    op->state = InFlightOp::kBufferedToTabletServer;
    per_tablet_ops_[op->tablet.get()].push_back(op);
  }

  l.unlock();

  FlushBuffersIfReady();
}

void Batcher::FlushBuffersIfReady() {
  unordered_map<RemoteTablet*, vector<InFlightOp*> > ops_copy;

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
    // Take ownership of the ops while we're under the lock.
    ops_copy.swap(per_tablet_ops_);
  }

  // Now flush the ops for each tablet.
  BOOST_FOREACH(const OpsMap::value_type& e, ops_copy) {
    RemoteTablet* tablet = e.first;
    const vector<InFlightOp*>& ops = e.second;

    VLOG(2) << "FlushBuffersIfReady: already in flushing state, immediately flushing to "
            << tablet->tablet_id();
    FlushBuffer(tablet, ops);
  }
}

void Batcher::FlushBuffer(RemoteTablet* tablet, const vector<InFlightOp*>& ops) {
  CHECK(!ops.empty());

  // Create and send an RPC that aggregates the ops. The RPC is freed when
  // its callback completes.
  //
  // The RPC object takes ownership of the ops.
  WriteRpc* rpc = new WriteRpc(this,
                               tablet,
                               ops,
                               deadline_,
                               client_->data_->messenger_);
  rpc->SendRpc();
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
      LOG(ERROR) << "Response from tablet " << rpc.tablet()->tablet_id() << ":\n"
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
