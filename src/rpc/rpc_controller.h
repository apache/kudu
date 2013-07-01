// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.
#ifndef KUDU_RPC_RPC_CONTROLLER_H
#define KUDU_RPC_RPC_CONTROLLER_H

#include <glog/logging.h>
#include <tr1/memory>

#include "gutil/macros.h"
#include "util/locks.h"
#include "util/monotime.h"
#include "util/status.h"

namespace kudu { namespace rpc {

class OutboundCall;

// Controller for managing properties of a single RPC call, on the client side.
//
// An RpcController maps to exactly one call and is not thread-safe. The client
// may use this class prior to sending an RPC in order to set properties such
// as the call's timeout.
//
// After the call has been sent (e.g using Proxy::AsyncRequest()) the user
// may invoke methods on the RpcController object in order to probe the status
// of the call.
class RpcController {
 public:
  RpcController();

  // Return true if the call has finished.
  // A call is finished if the server has responded, or if the call
  // has timed out.
  bool finished() const;

  // Return the current status of a call.
  //
  // A call is "OK" status until it finishes, at which point it may
  // either remain in "OK" status (if the call was successful), or
  // change to an error status. Error status indicates that there was
  // some RPC-layer issue with making the call, for example, one of:
  //
  // * failed to establish a connection to the server
  // * the server was too busy to handle the request
  // * the server was unable to interpret the request (eg due to a version
  //   mismatch)
  // * a network error occurred which caused the connection to be torn
  //   down
  // * the call timed out
  Status status() const;

  // Set the timeout for the call to be made with this RPC controller.
  //
  // The configured timeout applies to the entire time period between
  // the AsyncRequest() method call and getting a response. For example,
  // if it takes too long to establish a connection to the remote host,
  // or to DNS-resolve the remote host, those will be accounted as part
  // of the timeout period.
  //
  // Timeouts must be set prior to making the request -- the timeout may
  // not currently be adjusted for an already-sent call.
  //
  // Setting the timeout to 0 will result in a call which never times out
  // (not recommended!)
  void set_timeout(const MonoDelta &timeout);

  // Return the configured timeout.
  const MonoDelta &timeout() const { return timeout_; }

  //------------------------------------------------------------
  // Methods called by the RPC framework
  //------------------------------------------------------------

  // Called by the RPC framework to indicate that the call has failed.
  void SetFailed(const Status &status);

  // Called by the RPC framework to indicate that the call has timed out.
  void SetTimedOut();

 private:
  friend class OutboundCall;
  friend class Proxy;

  // Various states the call propagates through.
  // NB: if adding another state, be sure to update RpcController::finished()
  // as well.
  enum State {
    READY,
    ON_OUTBOUND_QUEUE,
    SENT,
    TIMED_OUT,
    FINISHED_ERROR,
    FINISHED_SUCCESS
  };

  void set_state(State new_state);

  // Same as set_state, but requires that the caller already holds
  // lock_
  void set_state_unlocked(State new_state);

  // Lock for state_ and call_status_ fields, since they
  // may be mutated by the reactor thread while the client thread
  // reads them.
  mutable simple_spinlock lock_;
  State state_;
  Status call_status_;

  MonoDelta timeout_;

  // Once the call is sent, it is tracked here.
  // Currently, this is only used so as to tie the deallocation of the
  // call to the deallocation of the controller object. This makes it
  // more likely that the call will be deallocated on the user's
  // caller thread instead of the IO thread, improving tcmalloc
  // caching behavior.
  std::tr1::shared_ptr<OutboundCall> call_;

  DISALLOW_COPY_AND_ASSIGN(RpcController);
};

} // namespace rpc
} // namespace kudu
#endif
