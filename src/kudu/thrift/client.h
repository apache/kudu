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

// Utilities for working with Thrift clients.

#pragma once

#include <cstdint>
#include <memory>

#include "kudu/util/monotime.h"

namespace apache {
namespace thrift {
namespace protocol {
class TProtocol;
} // namespace protocol
} // namespace thrift
} // namespace apache

namespace kudu {

class HostPort;

namespace thrift {

// Options for a Thrift client connection.
struct ClientOptions {

  // Thrift socket send timeout
  MonoDelta send_timeout = MonoDelta::FromSeconds(60);

  // Thrift socket receive timeout.
  MonoDelta recv_timeout = MonoDelta::FromSeconds(60);

  // Thrift socket connect timeout.
  MonoDelta conn_timeout = MonoDelta::FromSeconds(60);

  // Whether to use SASL Kerberos authentication.
  bool enable_kerberos = false;

  // Maximum size of objects which can be received on the Thrift connection.
  // Defaults to 100MiB to match Thrift TSaslTransport.receiveSaslMessage.
  int32_t max_buf_size = 100 * 1024 * 1024;
};

std::shared_ptr<apache::thrift::protocol::TProtocol> CreateClientProtocol(
    const HostPort& address, const ClientOptions& options);

} // namespace thrift
} // namespace kudu
