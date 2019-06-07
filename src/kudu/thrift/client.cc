// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/thrift/client.h"

#include <memory>
#include <mutex>
#include <ostream>

#include <glog/logging.h>
#include <thrift/TOutput.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "kudu/thrift/sasl_client_transport.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/status.h"

namespace apache {
namespace thrift {
namespace transport {
class TTransport;
}  // namespace transport
}  // namespace thrift
}  // namespace apache

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TProtocol;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using std::make_shared;
using std::shared_ptr;

namespace kudu {
namespace thrift {

namespace {
// A logging callback for Thrift.
void ThriftOutputFunction(const char* output) {
  LOG(INFO) << output;
}
} // anonymous namespace

shared_ptr<TProtocol> CreateClientProtocol(const HostPort& address, const ClientOptions& options) {
  // Initialize the global Thrift logging callback.
  static std::once_flag set_thrift_logging_callback;
  std::call_once(set_thrift_logging_callback, [] {
      apache::thrift::GlobalOutput.setOutputFunction(ThriftOutputFunction);
  });

  auto socket = make_shared<TSocket>(address.host(), address.port());
  socket->setSendTimeout(options.send_timeout.ToMilliseconds());
  socket->setRecvTimeout(options.recv_timeout.ToMilliseconds());
  socket->setConnTimeout(options.conn_timeout.ToMilliseconds());
  shared_ptr<TTransport> transport;

  if (options.enable_kerberos) {
    DCHECK(!options.service_principal.empty());
    transport = make_shared<SaslClientTransport>(options.service_principal,
                                                 address.host(),
                                                 std::move(socket),
                                                 options.max_buf_size);
  } else {
    transport = make_shared<TBufferedTransport>(std::move(socket));
  }

  return make_shared<TBinaryProtocol>(std::move(transport));
}

bool IsFatalError(const Status& error) {
  // Whitelist of errors which are not fatal. This errs on the side of
  // considering an error fatal since the consequences are low; just an
  // unnecessary reconnect. If a fatal error is not recognized it could cause
  // another RPC to fail, since there is no way to check the status of the
  // connection before sending an RPC.
  return !(error.IsAlreadyPresent()
        || error.IsNotFound()
        || error.IsInvalidArgument()
        || error.IsIllegalState()
        || error.IsNotSupported()
        || error.IsRemoteError());
}
} // namespace thrift
} // namespace kudu
