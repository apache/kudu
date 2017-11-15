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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <sasl/sasl.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/transport/TVirtualTransport.h>

#include "kudu/rpc/sasl_helper.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace apache {
namespace thrift {
namespace transport {
class TTransport;
} // namespace transport
} // namespace thrift
} // namespace apache

namespace kudu {
namespace rpc {
struct SaslDeleter;
} // namespace rpc
namespace hms {

// An exception representing a SASL or Kerberos failure.
class SaslException : public apache::thrift::transport::TTransportException {
 public:
  explicit SaslException(Status status)
    : TTransportException(status.ToString()),
      status_(std::move(status)) {
  }

  const Status& status() const {
    return status_;
  }

 private:
  Status status_;
};

// An enum describing the possible states of the SASL negotiation protocol.
enum NegotiationStatus {
  TSASL_INVALID = -1,
  TSASL_START = 1,
  TSASL_OK = 2,
  TSASL_BAD = 3,
  TSASL_ERROR = 4,
  TSASL_COMPLETE = 5
};

// A Thrift transport which uses SASL GSSAPI to authenticate as a client to a
// remote server.
//
// SaslClientTransport internally holds buffers, so it does not need the
// underlying transport to be buffered.
class SaslClientTransport
    : public apache::thrift::transport::TVirtualTransport<SaslClientTransport> {
 public:
  SaslClientTransport(const std::string& server_fqdn,
                      std::shared_ptr<TTransport> transport,
                      size_t max_recv_buf_size);

  ~SaslClientTransport() override = default;

  bool isOpen() override;

  bool peek() override;

  void open() override;

  void close() override;

  uint32_t read(uint8_t* buf, uint32_t len);

  void write(const uint8_t* buf, uint32_t len);

  void flush() override;

  int GetOptionCb(const char* plugin_name, const char* option,
                  const char** result, unsigned* len);

 private:

  // Runs SASL negotiation with the remote server.
  void Negotiate();

  // Sends a SASL negotiation message to the underlying transport.
  //
  // Send a SASL negotiation message using the Thrift framing protocol:
  //
  // - 1 byte of status
  // - 4 bytes of remaining length
  // - var-len payload
  void SendSaslMessage(NegotiationStatus status, Slice payload);

  // Receives a SASL negotiation message from the underlying transport.
  //
  // The returned negotiation status will be of type OK or COMPLETE, all
  // other statuses result in an exception.
  NegotiationStatus ReceiveSaslMessage(faststring* payload);

  // Initializes SASL state.
  void SetupSaslContext();

  // Sends the initial SASL connection message.
  void SendSaslStart();

  // Reads a frame from the underlying transport, storing the payload into
  // read_slice_. If the connection is using SASL auth-conf or auth-int
  // protection the data is automatically decoded.
  void ReadFrame();

  // Resets the read buffer to empty, and deallocates its internal buffer.
  void ResetReadBuf();

  // Resets the write buffer to the size of a frame header, and deallocates its
  // internal buffer.
  void ResetWriteBuf();

  // The underlying transport. Typically a TCP socket.
  std::shared_ptr<TTransport> transport_;

  // SASL state.
  rpc::SaslHelper sasl_helper_;
  std::unique_ptr<sasl_conn_t, rpc::SaslDeleter> sasl_conn_;
  std::vector<sasl_callback_t> sasl_callbacks_;

  // Whether the connection is using auth-int or auth-conf protection.
  bool needs_wrap_;

  // The negotiated SASL maximum buffer sizes. These correspond to the maximum
  // sized frames that can be received or sent.
  //
  // Note: the Java implementation of the Thrift SASL transport does not respect
  // the negotiated maximum buffer size (THRIFT-4483) and never splits a message
  // into multiple frames, so we end up having to set the recv buf size to match
  // the largest serialized Thrift message we want to be able to receive.
  size_t max_recv_buf_size_;
  size_t max_send_buf_size_;

  // The read buffer and slice. The slice points to the remaining frame data
  // which hasn't been read yet.
  faststring read_buf_;
  Slice read_slice_;

  // The write buffer.
  faststring write_buf_;
};

} // namespace hms
} // namespace kudu
