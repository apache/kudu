// Copyright (c) 2014, Cloudera, inc.

#include "kudu/client/rpc.h"

#include <boost/bind.hpp>
#include <string>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/rpc_header.pb.h"

using std::tr1::shared_ptr;
using strings::Substitute;

namespace kudu {

using rpc::ErrorStatusPB;
using rpc::Messenger;

namespace client {

namespace internal {

bool RpcRetrier::HandleResponse(Rpc* rpc, Status* out_status) {
  DCHECK_NOTNULL(rpc);
  DCHECK_NOTNULL(out_status);

  // Did we get a retryable error?
  Status controller_status = controller_.status();
  if (controller_status.IsRemoteError()) {
    const ErrorStatusPB* err = controller_.error_response();
    if (err &&
        err->has_code() &&
        err->code() == ErrorStatusPB::ERROR_SERVER_TOO_BUSY) {
      // Retry provided we haven't exceeded the deadline.
      if (!deadline_.Initialized() || // no deadline --> retry forever
          MonoTime::Now(MonoTime::FINE).ComesBefore(deadline_)) {
        // Add some jitter to the retry delay.
        //
        // If the delay causes us to miss our deadline, SendRpcRescheduled
        // will fail the RPC on our behalf.
        int num_ms = ++attempt_num_ + ((rand() % 5));
        messenger_->ScheduleOnReactor(
            boost::bind(&RpcRetrier::SendRpcRescheduled, this, rpc, _1),
            MonoDelta::FromMilliseconds(num_ms));
        return true;
      } else {
        VLOG(2) << Substitute("Failed $0, deadline exceeded: $1",
                              rpc->ToString(), deadline_.ToString());
      }
    }
  }

  *out_status = controller_status;
  return false;
}

void RpcRetrier::SendRpcRescheduled(Rpc* rpc, const Status& status) {
  Status new_status = status;
  if (new_status.ok()) {
    // Has this RPC timed out?
    if (deadline_.Initialized()) {
      MonoTime now = MonoTime::Now(MonoTime::FINE);
      if (deadline_.ComesBefore(now)) {
        new_status = Status::TimedOut(
            Substitute("$0 timed out", rpc->ToString()));
      }
    }
  }
  if (new_status.ok()) {
    controller_.Reset();
    rpc->SendRpc();
  } else {
    rpc->SendRpcCb(new_status);
  }
}

} // namespace internal
} // namespace client
} // namespace kudu
