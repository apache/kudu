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

#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class faststring; // NOLINT

namespace subprocess {
// Facilitates sending and receiving messages with a subprocess via protobuf-based
// protocol.
//
// May be used by a subprocess communicating with the parent process via pipe, or
// by the parent process itself to read/write messages via stdin/stdout respectively.
class SubprocessProtocol {
 public:
  enum class SerializationMode {
    // Each message is serialized as a four byte big-endian size followed by
    // the protobuf-encoded message itself.
    PB,

    // Each message is serialized into a protobuf-like JSON representation
    // terminated with a newline character.
    JSON,
  };

  // Whether the provided fds are closed at class destruction time.
  enum class CloseMode {
    CLOSE_ON_DESTROY,
    NO_CLOSE_ON_DESTROY,
  };

  // Constructs a new protocol instance.
  //
  // If 'close_mode' is CLOSE_ON_DESTROY, the instance has effectively taken
  // control of 'read_fd' and 'write_fd' and the caller shouldn't use them.
  // 'max_msg_bytes' represents the maximum number of bytes per message.
  SubprocessProtocol(SerializationMode serialization_mode,
                     CloseMode close_mode,
                     int read_fd,
                     int write_fd,
                     int max_msg_bytes = kMaxMessageBytes);

  ~SubprocessProtocol();

  // Receives a protobuf message, blocking if the pipe is empty.
  //
  // Returns EndOfFile if the writer on the other end of the pipe was closed.
  //
  // Returns an error if serialization_mode_ is PB and the received message
  // sizes exceeds kMaxMessageBytes.
  template <class M>
  Status ReceiveMessage(M* message);

  // Sends a protobuf message, blocking if the pipe is full.
  //
  // Returns EndOfFile if the reader on the other end of the pipe was closed.
  template <class M>
  Status SendMessage(const M& message);

 private:
  // Private helpers to drive actual pipe reading and writing.
  Status DoRead(faststring* buf);
  Status DoWrite(const faststring& buf);

  static const int kMaxMessageBytes;

  const SerializationMode serialization_mode_;
  const CloseMode close_mode_;
  const int read_fd_;
  const int write_fd_;
  const int max_msg_bytes_;

  DISALLOW_COPY_AND_ASSIGN(SubprocessProtocol);
};

} // namespace subprocess
} // namespace kudu
