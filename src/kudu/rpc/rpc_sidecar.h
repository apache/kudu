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
#pragma once

#include <memory>

#include <google/protobuf/repeated_field.h> // IWYU pragma: keep
#include <google/protobuf/stubs/port.h>

#include "kudu/util/slice.h"

namespace kudu {

class Status;
class faststring;

namespace rpc {

// An RpcSidecar is a mechanism which allows replies to RPCs to reference blocks of data
// without extra copies. In other words, whenever a protobuf would have a large field
// where additional copies become expensive, one may opt instead to use an RpcSidecar.
//
// The RpcSidecar saves on an additional copy to/from the protobuf on both the server and
// client side. Both Inbound- and OutboundCall classes accept sidecars to be sent to the
// client and server respectively. They are ignorant of the sidecar's format, requiring
// only that it can be represented as a Slice. Data is copied from the Slice returned from
// AsSlice() to the socket that is responding to the original RPC. The slice should remain
// valid for as long as the call it is attached to takes to complete.
//
// In order to distinguish between separate sidecars, whenever a sidecar is added to the
// RPC response on the server side, an index for that sidecar is returned. This index must
// then in some way (i.e., via protobuf) be communicated to the recipient.
//
// After reconstructing the array of sidecars, servers and clients may retrieve the
// sidecar data through the RpcContext or RpcController interfaces respectively.
class RpcSidecar {
 public:
  static std::unique_ptr<RpcSidecar> FromFaststring(std::unique_ptr<faststring> data);
  static std::unique_ptr<RpcSidecar> FromSlice(Slice slice);

  // Utility method to parse a series of sidecar slices into 'sidecars' from 'buffer' and
  // a set of offsets. 'sidecars' must have length >= TransferLimits::kMaxSidecars, and
  // will be filled from index 0.
  // TODO(henryr): Consider a vector instead here if there's no perf. impact.
  static Status ParseSidecars(
      const ::google::protobuf::RepeatedField<::google::protobuf::uint32>& offsets,
      Slice buffer, Slice* sidecars);

  // Returns a Slice representation of the sidecar's data.
  virtual Slice AsSlice() const = 0;
  virtual ~RpcSidecar() { }
};

} // namespace rpc
} // namespace kudu
