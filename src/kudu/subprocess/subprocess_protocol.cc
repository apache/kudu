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

#include "kudu/subprocess/subprocess_protocol.h"

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <unistd.h>

#include <ostream>
#include <string>

#include <glog/logging.h>
#include <google/protobuf/util/json_util.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/subprocess/subprocess.pb.h" // IWYU pragma: keep
#include "kudu/tools/tool.pb.h"  // IWYU pragma: keep
#include "kudu/util/faststring.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

using kudu::pb_util::SecureDebugString;
using strings::Substitute;
using std::string;

namespace kudu {
namespace subprocess {

const int SubprocessProtocol::kMaxMessageBytes = 1024 * 1024;

SubprocessProtocol::SubprocessProtocol(SerializationMode serialization_mode,
                                       CloseMode close_mode,
                                       int read_fd,
                                       int write_fd,
                                       int max_msg_bytes)
    : serialization_mode_(serialization_mode),
      close_mode_(close_mode),
      read_fd_(read_fd),
      write_fd_(write_fd),
      max_msg_bytes_(max_msg_bytes) {
}

SubprocessProtocol::~SubprocessProtocol() {
  if (close_mode_ == CloseMode::CLOSE_ON_DESTROY) {
    int ret;
    RETRY_ON_EINTR(ret, close(read_fd_));
    RETRY_ON_EINTR(ret, close(write_fd_));
  }
}

template <class M>
Status SubprocessProtocol::ReceiveMessage(M* message) {
  switch (serialization_mode_) {
    case SerializationMode::JSON:
    {
      // Read and accumulate one byte at a time, looking for the newline.
      //
      // TODO(adar): it would be more efficient to read a chunk of data, look
      // for a newline, and if found, store the remainder for the next message.
      faststring buf;
      faststring one_byte;
      one_byte.resize(1);
      while (true) {
        RETURN_NOT_OK_PREPEND(DoRead(&one_byte), "unable to receive message byte");
        if (one_byte[0] == '\n') {
          break;
        }
        buf.push_back(one_byte[0]);
      }

      // Parse the JSON-encoded message.
      const auto& google_status =
          google::protobuf::util::JsonStringToMessage(buf.ToString(), message);
      if (!google_status.ok()) {
        return Status::InvalidArgument(
            Substitute("unable to parse JSON: $0", buf.ToString()),
            google_status.error_message().ToString());
      }
      break;
    }
    case SerializationMode::PB:
    {
      // Read four bytes of size (big-endian).
      faststring size_buf;
      size_buf.resize(sizeof(uint32_t));
      RETURN_NOT_OK_PREPEND(DoRead(&size_buf), "unable to receive message size");
      uint32_t body_size = NetworkByteOrder::Load32(size_buf.data());

      if (body_size > max_msg_bytes_) {
        return Status::IOError(
            Substitute("message size ($0) exceeds maximum message size ($1)",
                       body_size, max_msg_bytes_));
      }

      // Read the variable size body.
      faststring body_buf;
      body_buf.resize(body_size);
      RETURN_NOT_OK_PREPEND(DoRead(&body_buf), "unable to receive message body");

      // Parse the body into a PB request.
      RETURN_NOT_OK_PREPEND(pb_util::ParseFromArray(
          message, body_buf.data(), body_buf.length()),
              Substitute("unable to parse PB: $0", body_buf.ToString()));
      break;
    }
    default: LOG(FATAL) << "Unknown mode";
  }

  VLOG(1) << "Received message: " << pb_util::SecureDebugString(*message);
  return Status::OK();
}

template <class M>
Status SubprocessProtocol::SendMessage(const M& message) {
  VLOG(1) << "Sending message: " << pb_util::SecureDebugString(message);

  faststring buf;
  switch (serialization_mode_) {
    case SerializationMode::JSON:
    {
      string serialized;
      const auto& google_status =
          google::protobuf::util::MessageToJsonString(message, &serialized);
      if (!google_status.ok()) {
        return Status::InvalidArgument(Substitute(
            "unable to serialize JSON: $0", pb_util::SecureDebugString(message)),
                                       google_status.error_message().ToString());
      }

      buf.append(serialized);
      buf.append("\n");
      break;
    }
    case SerializationMode::PB:
    {
      size_t msg_size = message.ByteSizeLong();
      buf.resize(sizeof(uint32_t) + msg_size);
      NetworkByteOrder::Store32(buf.data(), msg_size);
      if (!message.SerializeWithCachedSizesToArray(buf.data() + sizeof(uint32_t))) {
        return Status::Corruption("failed to serialize PB to array");
      }
      break;
    }
    default:
      break;
  }
  RETURN_NOT_OK_PREPEND(DoWrite(buf), "unable to send message");
  return Status::OK();
}

Status SubprocessProtocol::DoRead(faststring* buf) {
  uint8_t* pos = buf->data();
  size_t rem = buf->length();
  while (rem > 0) {
    ssize_t r;
    RETRY_ON_EINTR(r, read(read_fd_, pos, rem));
    if (r == -1) {
      return Status::IOError("Error reading from pipe", "", errno);
    }
    if (r == 0) {
      return Status::EndOfFile("Other end of pipe was closed");
    }
    DCHECK_GE(rem, r);
    rem -= r;
    pos += r;
  }
  return Status::OK();
}

Status SubprocessProtocol::DoWrite(const faststring& buf) {
  const uint8_t* pos = buf.data();
  size_t rem = buf.length();
  while (rem > 0) {
    ssize_t r;
    RETRY_ON_EINTR(r, write(write_fd_, pos, rem));
    if (r == -1) {
      if (errno == EPIPE) {
        return Status::EndOfFile("Other end of pipe was closed");
      }
      return Status::IOError("Error writing to pipe", "", errno);
    }
    DCHECK_GE(rem, r);
    rem -= r;
    pos += r;
  }
  return Status::OK();
}


// Explicit specialization for callers outside this compilation unit.
template
Status SubprocessProtocol::ReceiveMessage(tools::ControlShellRequestPB* message);
template
Status SubprocessProtocol::ReceiveMessage(tools::ControlShellResponsePB* message);
template
Status SubprocessProtocol::SendMessage(const tools::ControlShellRequestPB& message);
template
Status SubprocessProtocol::SendMessage(const tools::ControlShellResponsePB& message);

template
Status SubprocessProtocol::ReceiveMessage(SubprocessRequestPB* message);
template
Status SubprocessProtocol::ReceiveMessage(SubprocessResponsePB* message);
template
Status SubprocessProtocol::SendMessage(const SubprocessRequestPB& message);
template
Status SubprocessProtocol::SendMessage(const SubprocessResponsePB& message);

} // namespace subprocess
} // namespace kudu
