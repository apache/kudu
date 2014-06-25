// Copyright (c) 2013, Cloudera, inc.
#ifndef KUDU_CLIENT_BATCHER_H
#define KUDU_CLIENT_BATCHER_H

#include "gutil/gscoped_ptr.h"
#include "gutil/macros.h"
#include "gutil/ref_counted.h"
#include "util/async_util.h"
#include "util/debug-util.h"
#include "util/locks.h"
#include "util/status.h"

#include <tr1/memory>
#include <tr1/unordered_set>
#include <tr1/unordered_map>

namespace kudu {
namespace client {

class KuduClient;
class KuduSession;
class WriteOperation;
class RemoteTabletServer;

namespace internal {

struct InFlightOp;
struct InFlightRpc;
class PerTSBuffer;

class ErrorCollector;

// A Batcher is the class responsible for collecting row operations, routing them to the
// correct tablet server, and possibly batching them together for better efficiency.
//
// It is a reference-counted class: the client session creating the batch holds one
// reference, and all of the in-flight operations hold others. This allows the client
// session to be destructed while ops are still in-flight, without the async callbacks
// attempting to access a destructed Batcher.
class Batcher : public base::RefCountedThreadSafe<Batcher> {
 public:
  // Create a new batcher associated with the given session.
  //
  // Any errors which come back from operations performed by this batcher are posted to
  // the provided ErrorCollector.
  //
  // Takes a reference on error_collector. Creates a weak_ptr to 'session'.
  Batcher(KuduClient* client,
          ErrorCollector* error_collector,
          const std::tr1::shared_ptr<KuduSession>& session);

  // Abort the current batch. Any writes that were buffered and not yet sent are
  // discarded. Those that were sent may still be delivered.  If there is a pending Flush
  // callback, it will be called immediately with an error status.
  void Abort();

  // Set the timeout for this batcher.
  //
  // The timeout is currently set on all of the RPCs, but in the future will be relative
  // to when the Flush call is made (eg even if the lookup of the TS takes a long time, it
  // may time out before even sending an op). TODO: implement that
  void SetTimeoutMillis(int millis);

  // Add a new operation to the batch. Requires that the batch has not yet been flushed.
  // TODO: in other flush modes, this may not be the case -- need to
  // update this when they're implemented.
  Status Add(gscoped_ptr<WriteOperation> write_op);

  // Return true if any operations are still pending. An operation is no longer considered
  // pending once it has either errored or succeeded.  Operations are considering pending
  // as soon as they are added, even if Flush has not been called.
  bool HasPendingOperations() const;

  // Return the number of buffered operations. These are only those operations which are
  // "corked" (i.e not yet flushed). Once Flush has been called, this returns 0.
  int CountBufferedOperations() const;

  // Flush any buffered operations. The callback will be called once there are no
  // more pending operations from this Batcher. If all of the operations succeeded,
  // then the callback will receive Status::OK. Otherwise, it will receive IOError,
  // and the caller must inspect the ErrorCollector to retrieve more detailed
  // information on which operations failed.
  void FlushAsync(const StatusCallback& cb);

 private:
  friend class base::RefCountedThreadSafe<Batcher>;
  ~Batcher();

  // Add an op to the in-flight set and increment the ref-count.
  void AddInFlightOp(InFlightOp* op);

  void RemoveInFlightOp(InFlightOp* op);

  // Return true if the batch has been aborted, and any in-flight ops should stop
  // processing wherever they are.
  bool IsAbortedUnlocked() const;

  // Mark the fact that errors have occurred with this batch. This ensures that
  // the flush callback will get a bad Status.
  void MarkHadErrors();

  // Remove an op from the in-flight op list, and delete the op itself.
  // The operation is reported to the ErrorReporter as having failed with the
  // given status.
  void MarkInFlightOpFailed(InFlightOp* op, const Status& s);
  void MarkInFlightOpFailedUnlocked(InFlightOp* op, const Status& s);

  void CheckForFinishedFlush();
  void FlushBufferIfReady(RemoteTabletServer* ts, PerTSBuffer* buf);
  void FlushBuffer(RemoteTabletServer* ts, PerTSBuffer* buf);

  // Sends a write RPC.
  //
  // If invoked for an RPC retry, 's' may be non-OK if the lookup was aborted
  // before it fired (e.g. the reactor thread shut down).
  void SendRpc(InFlightRpc* rpc, const Status& s);

  // Cleans up an RPC, scooping out any errors and passing them up to the
  // batcher.
  void FinishRpc(InFlightRpc* rpc, const Status& s);

  // Async Callbacks.
  void TabletLookupFinished(InFlightOp* op, const Status& s);
  void RefreshTSProxyFinished(RemoteTabletServer* ts, PerTSBuffer* buf,
                              const Status& status);
  void WriteRpcFinished(InFlightRpc* rpc);

  // See note about lock ordering in batcher.cc
  mutable simple_spinlock lock_;

  enum State {
    kGatheringOps,
    kFlushing,
    kFlushed,
    kAborted
  };
  State state_;

  KuduClient* const client_;
  std::tr1::weak_ptr<KuduSession> weak_session_;

  // Errors are reported into this error collector.
  scoped_refptr<ErrorCollector> const error_collector_;

  // Set to true if there was at least one error from this Batcher.
  // Protected by lock_
  bool had_errors_;

  // If state is kFlushing, this member will be set to the user-provided
  // callback. Once there are no more in-flight operations, the callback
  // will be called exactly once (and the state changed to kFlushed).
  StatusCallback flush_callback_;

  // All buffered or in-flight ops.
  std::tr1::unordered_set<InFlightOp*> ops_;
  // Buffers for each tablet of ops that haven't yet been sent.
  std::tr1::unordered_map<RemoteTabletServer*, PerTSBuffer*> per_ts_buffers_;

  // Amount of time to wait for a given op, from start to finish.
  //
  // Set by SetTimeoutMillis.
  MonoDelta timeout_;

  // After flushing, the absolute deadline for all in-flight ops.
  MonoTime deadline_;

  DISALLOW_COPY_AND_ASSIGN(Batcher);
};

} // namespace internal
} // namespace client
} // namespace kudu
#endif /* KUDU_CLIENT_BATCHER_H */
