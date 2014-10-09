// Copyright (c) 2014, Cloudera, inc.
#ifndef KUDU_CLIENT_RPC_H
#define KUDU_CLIENT_RPC_H

#include <string>

#include "kudu/gutil/callback.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status_callback.h"

namespace kudu {

namespace master {
class MasterServiceProxy;
} // namespace master

namespace rpc {
class Messenger;
} // namespace rpc

namespace client {

namespace internal {

class MetaCache;
class RemoteTablet;
class Rpc;

// Provides utilities for retrying failed RPCs.
//
// All RPCs should use HandleResponse() to retry certain generic errors.
//
// An RPC may wish to Retry() directly if it encounters non-fatal errors
// that are specific to it.
class RpcRetrier {
 public:
  RpcRetrier(const MonoTime& deadline,
             const std::tr1::shared_ptr<rpc::Messenger>& messenger)
    : attempt_num_(1),
      deadline_(deadline),
      messenger_(messenger) {
    if (deadline_.Initialized()) {
      controller_.set_deadline(deadline_);
    }
    controller_.Reset();
  }

  // Tries to handle a failed RPC.
  //
  // If it was handled (e.g. scheduled for retry in the future), returns
  // true. In this case, callers should ensure that 'rpc' remains alive.
  //
  // Otherwise, returns false and writes the controller status to
  // 'out_status'.
  bool HandleResponse(Rpc* rpc, Status* out_status);

  // Retries an RPC at some point in the near future.
  //
  // If the RPC's deadline expires, the callback will fire with a timeout
  // error when the RPC comes up for retrying. This is true even if the
  // deadline has already expired at the time that Retry() was called.
  //
  // Callers should ensure that 'rpc' remains alive.
  void DelayedRetry(Rpc* rpc);

  rpc::RpcController& controller() { return controller_; }

 private:
  // Called when an RPC comes up for retrying. Actually sends the RPC.
  void DelayedRetryCb(Rpc* rpc, const Status& status);

  // The next sent rpc will be the nth attempt (indexed from 1).
  int attempt_num_;

  // If the remote end is busy, the RPC will be retried (with a small
  // delay) until this deadline is reached.
  //
  // May be uninitialized.
  MonoTime deadline_;

  // Messenger to use when sending the RPC.
  std::tr1::shared_ptr<rpc::Messenger> messenger_;

  // RPC controller to use when sending the RPC.
  rpc::RpcController controller_;

  DISALLOW_COPY_AND_ASSIGN(RpcRetrier);
};

// An in-flight remote procedure call to some server.
class Rpc {
 public:
  Rpc(const MonoTime& deadline,
      const std::tr1::shared_ptr<rpc::Messenger>& messenger)
  : retrier_(deadline, messenger) {
  }

  virtual ~Rpc() {}

  // Asynchronously sends the RPC to the remote end.
  //
  // Subclasses should use SendRpcCb() below as the callback function.
  virtual void SendRpc() = 0;

  // Returns a string representation of the RPC.
  virtual std::string ToString() const = 0;

 protected:
  RpcRetrier& retrier() { return retrier_; }

 private:
  friend class RpcRetrier;

  // Callback for SendRpc(). If 'status' is not OK, something failed
  // before the RPC was sent.
  virtual void SendRpcCb(const Status& status) = 0;

  // Used to retry some failed RPCs.
  RpcRetrier retrier_;

  DISALLOW_COPY_AND_ASSIGN(Rpc);
};

} // namespace internal
} // namespace client
} // namespace kudu

#endif
