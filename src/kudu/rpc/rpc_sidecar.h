// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_RPC_RPC_SIDECAR_H
#define KUDU_RPC_RPC_SIDECAR_H

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"

namespace kudu {
namespace rpc {

// An RpcSidecar is a mechanism which allows replies to RPCs
// to reference blocks of data without extra copies. In other words,
// whenever a protobuf would have a large field where additional copies
// become expensive, one may opt instead to use an RpcSidecar.
//
// The RpcSidecar saves on an additional copy to/from the protobuf on both the
// server and client side. The InboundCall class accepts RpcSidecars, ignorant
// of the form that the sidecar's data is kept in, requiring only that it can
// be represented as a Slice. Data is then immediately copied from the
// Slice returned from AsSlice() to the socket that is responding to the original
// RPC.
//
// In order to distinguish between separate sidecars, whenever a sidecar is
// added to the RPC response on the server side, an index for that sidecar is
// returned. This index must then in some way (i.e., via protobuf) be
// communicated to the client side.
//
// After receiving the RPC response on the client side, OutboundCall decodes
// the original message along with the separate sidecars by using a list
// of sidecar byte offsets that was sent in the message header.
//
// After reconstructing the array of sidecars, the OutboundCall (through
// RpcController's interface) is able to offer retrieval of the sidecar data
// through the same indices that were returned by InboundCall (or indirectly
// through the RpcContext wrapper) on the client side.
class RpcSidecar {
 public:
  // Generates a sidecar with the parameter faststring as its data.
  explicit RpcSidecar(gscoped_ptr<faststring> data) : data_(data.Pass()) {}

  // Returns a Slice representation of the sidecar's data.
  Slice AsSlice() const { return *data_; }

 private:
  const gscoped_ptr<faststring> data_;

  DISALLOW_COPY_AND_ASSIGN(RpcSidecar);
};

} // namespace rpc
} // namespace kudu


#endif /* KUDU_RPC_RPC_SIDECAR_H */
