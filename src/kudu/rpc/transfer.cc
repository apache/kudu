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

#include "kudu/rpc/transfer.h"

#include <stdint.h>

#include <iostream>
#include <sstream>

#include <glog/logging.h>

#include "kudu/gutil/endian.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/constants.h"
#include "kudu/rpc/messenger.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/memory/memory.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"

DEFINE_int32(rpc_max_message_size, (50 * 1024 * 1024),
             "The maximum size of a message that any RPC that the server will accept. "
             "Must be at least 1MB.");
TAG_FLAG(rpc_max_message_size, advanced);
TAG_FLAG(rpc_max_message_size, runtime);

static bool ValidateMaxMessageSize(const char* flagname, int32_t value) {
  if (value < 1 * 1024 * 1024) {
    LOG(ERROR) << flagname << " must be at least 1MB.";
    return false;
  }
  return true;
}
static bool dummy = google::RegisterFlagValidator(
    &FLAGS_rpc_max_message_size, &ValidateMaxMessageSize);

namespace kudu {
namespace rpc {

using std::ostringstream;
using std::shared_ptr;
using std::string;
using strings::Substitute;

// The outbound transfer for an outbound call can be aborted after it has started already.
// The system has to continue sending up to number of bytes specified in the header or the
// receiving end will never make progress, leading to resource leak. In order to release
// the sidecars early, we call MarkPayloadCancelled() on the outbound call to relocate the
// sidecars to point to 'g_dummy_buffer'. SendBuffer() below recognizes slices pointing to
// this dummy buffer and handles them differently.
#define DUMMY_BUFFER_SIZE (1 << 20)
uint8_t g_dummy_buffer[DUMMY_BUFFER_SIZE];

#define RETURN_ON_ERROR_OR_SOCKET_NOT_READY(status) \
  if (PREDICT_FALSE(!status.ok())) {                            \
    if (Socket::IsTemporarySocketError(status.posix_code())) {  \
      return Status::OK(); /* EAGAIN, etc. */                   \
    }                                                           \
    return status;                                              \
  }

// Initialize the dummy buffer with a known pattern for easier debugging.
__attribute__((constructor))
static void InitializeDummyBuffer() {
  OverwriteWithPattern(reinterpret_cast<char*>(g_dummy_buffer),
                       DUMMY_BUFFER_SIZE, "ABORTED-TRANSFER");
}

TransferCallbacks::~TransferCallbacks()
{}

InboundTransfer::InboundTransfer()
  : total_length_(kMsgLengthPrefixLength),
    cur_offset_(0) {
  buf_.resize(kMsgLengthPrefixLength);
}

Status InboundTransfer::ReceiveBuffer(Socket &socket) {
  if (cur_offset_ < kMsgLengthPrefixLength) {
    // receive int32 length prefix
    int32_t rem = kMsgLengthPrefixLength - cur_offset_;
    int32_t nread;
    Status status = socket.Recv(&buf_[cur_offset_], rem, &nread);
    RETURN_ON_ERROR_OR_SOCKET_NOT_READY(status);
    if (nread == 0) {
      return Status::OK();
    }
    DCHECK_GE(nread, 0);
    cur_offset_ += nread;
    if (cur_offset_ < kMsgLengthPrefixLength) {
      // If we still don't have the full length prefix, we can't continue
      // reading yet.
      return Status::OK();
    }
    // Since we only read 'rem' bytes above, we should now have exactly
    // the length prefix in our buffer and no more.
    DCHECK_EQ(cur_offset_, kMsgLengthPrefixLength);

    // The length prefix doesn't include its own 4 bytes, so we have to
    // add that back in.
    total_length_ = NetworkByteOrder::Load32(&buf_[0]) + kMsgLengthPrefixLength;
    if (total_length_ > FLAGS_rpc_max_message_size) {
      return Status::NetworkError(Substitute(
          "RPC frame had a length of $0, but we only support messages up to $1 bytes "
          "long.", total_length_, FLAGS_rpc_max_message_size));
    }
    if (total_length_ <= kMsgLengthPrefixLength) {
      return Status::NetworkError(Substitute("RPC frame had invalid length of $0",
                                             total_length_));
    }
    buf_.resize(total_length_);

    // Fall through to receive the message body, which is likely to be already
    // available on the socket.
  }

  // receive message body
  int32_t nread;
  int32_t rem = total_length_ - cur_offset_;
  Status status = socket.Recv(&buf_[cur_offset_], rem, &nread);
  RETURN_ON_ERROR_OR_SOCKET_NOT_READY(status);
  cur_offset_ += nread;

  return Status::OK();
}

bool InboundTransfer::TransferStarted() const {
  return cur_offset_ != 0;
}

bool InboundTransfer::TransferFinished() const {
  return cur_offset_ == total_length_;
}

string InboundTransfer::StatusAsString() const {
  return Substitute("$0/$1 bytes received", cur_offset_, total_length_);
}

OutboundTransfer* OutboundTransfer::CreateForCallRequest(int32_t call_id,
                                                         const TransferPayload &payload,
                                                         size_t n_payload_slices,
                                                         TransferCallbacks *callbacks) {
  return new OutboundTransfer(call_id, payload, n_payload_slices, callbacks);
}

OutboundTransfer* OutboundTransfer::CreateForCallResponse(const TransferPayload &payload,
                                                          size_t n_payload_slices,
                                                          TransferCallbacks *callbacks) {
  return new OutboundTransfer(kInvalidCallId, payload, n_payload_slices, callbacks);
}

OutboundTransfer::OutboundTransfer(int32_t call_id,
                                   const TransferPayload &payload,
                                   size_t n_payload_slices,
                                   TransferCallbacks *callbacks)
  : n_payload_slices_(n_payload_slices),
    cur_slice_idx_(0),
    cur_offset_in_slice_(0),
    callbacks_(callbacks),
    call_id_(call_id),
    aborted_(false) {
  // We should leave the last entry for the footer.
  CHECK_LE(n_payload_slices_, payload_slices_.size() - 1);
  for (int i = 0; i < n_payload_slices; i++) {
    payload_slices_[i] = payload[i];
  }
}

void OutboundTransfer::AppendFooter(const shared_ptr<OutboundCall> &call) {
  DCHECK(!TransferStarted());
  DCHECK(is_for_outbound_call());
  DCHECK_LE(n_payload_slices_, payload_slices_.size() - 1);
  call->AppendFooter(&payload_slices_[n_payload_slices_++]);
}

OutboundTransfer::~OutboundTransfer() {
  if (!TransferFinished() && !aborted_) {
    callbacks_->NotifyTransferAborted(
        Status::RuntimeError("RPC transfer destroyed before it finished sending"));
  }
}

void OutboundTransfer::Abort(const Status &status) {
  CHECK(!aborted_) << "Already aborted";
  CHECK(!TransferFinished()) << "Cannot abort a finished transfer";
  callbacks_->NotifyTransferAborted(status);
  aborted_ = true;
}

void OutboundTransfer::Cancel(const shared_ptr<OutboundCall> &call,
                              const Status &status) {
  // Only transfer for outbound calls support cancellation.
  DCHECK(is_for_outbound_call());
  // If transfer has finished already, it's a no-op.
  if (TransferFinished()) {
    return;
  }
  // If transfer hasn't started yet, simply abort it.
  if (!TransferStarted()) {
    Abort(status);
    return;
  }
  // Transfer has started already. Update the payload to give up the sidecars
  // if they haven't all been sent yet. We only reach this point if remote also
  // supports cancellation so the last slice is always the footer. Don't cancel
  // the transfer if only the footer is left.
  if (cur_slice_idx_ < n_payload_slices_ - 1) {
    call->MarkPayloadCancelled(&payload_slices_, g_dummy_buffer);
    status_ = status;
  }
}

#define IO_VEC_SIZE (16)

Status OutboundTransfer::SendBuffer(Socket &socket) {
  DCHECK_LT(cur_slice_idx_, n_payload_slices_);

  int idx = cur_slice_idx_;
  int offset_in_slice = cur_offset_in_slice_;
  struct iovec iovec[IO_VEC_SIZE];
  int n_iovecs;
  for (n_iovecs = 0; n_iovecs < IO_VEC_SIZE && idx < n_payload_slices_; ++n_iovecs) {
    Slice &slice = payload_slices_[idx];
    uint8_t* ptr = slice.mutable_data();
    if (PREDICT_TRUE(ptr != g_dummy_buffer)) {
      iovec[n_iovecs].iov_base = ptr + offset_in_slice;
      iovec[n_iovecs].iov_len = slice.size() - offset_in_slice;
    } else {
      iovec[n_iovecs].iov_base = g_dummy_buffer;
      iovec[n_iovecs].iov_len = slice.size() - offset_in_slice;
      // The dummy buffer is of limited size. Split a slice into multiple slices
      // if it's larger than DUMMY_BUFFER_SIZE.
      if (iovec[n_iovecs].iov_len > DUMMY_BUFFER_SIZE) {
        iovec[n_iovecs].iov_len = DUMMY_BUFFER_SIZE;
        offset_in_slice += DUMMY_BUFFER_SIZE;
        continue;
      }
    }
    offset_in_slice = 0;
    ++idx;
  }

  int32_t written;
  Status status = socket.Writev(iovec, n_iovecs, &written);
  RETURN_ON_ERROR_OR_SOCKET_NOT_READY(status);

  // Adjust our accounting of current writer position.
  for (int i = cur_slice_idx_; i < n_payload_slices_; i++) {
    Slice &slice = payload_slices_[i];
    int rem_in_slice = slice.size() - cur_offset_in_slice_;
    DCHECK_GE(rem_in_slice, 0);

    if (written >= rem_in_slice) {
      // Used up this entire slice, advance to the next slice.
      cur_slice_idx_++;
      cur_offset_in_slice_ = 0;
      written -= rem_in_slice;
    } else {
      // Partially used up this slice, just advance the offset within it.
      cur_offset_in_slice_ += written;
      break;
    }
  }

  if (cur_slice_idx_ == n_payload_slices_) {
    if (PREDICT_FALSE(!status_.ok())) {
      callbacks_->NotifyTransferAborted(status_);
    } else {
      callbacks_->NotifyTransferFinished();
    }
    DCHECK_EQ(0, cur_offset_in_slice_);
  } else {
    DCHECK_LT(cur_slice_idx_, n_payload_slices_);
    DCHECK_LT(cur_offset_in_slice_, payload_slices_[cur_slice_idx_].size());
  }

  return Status::OK();
}

bool OutboundTransfer::TransferStarted() const {
  return cur_offset_in_slice_ != 0 || cur_slice_idx_ != 0;
}

bool OutboundTransfer::TransferFinished() const {
  if (cur_slice_idx_ == n_payload_slices_) {
    DCHECK_EQ(0, cur_offset_in_slice_); // sanity check
    return true;
  }
  return false;
}

bool OutboundTransfer::TransferAborted() const {
  return aborted_;
}

string OutboundTransfer::HexDump() const {
  if (KUDU_SHOULD_REDACT()) {
    return kRedactionMessage;
  }

  string ret;
  for (int i = 0; i < n_payload_slices_; i++) {
    ret.append(payload_slices_[i].ToDebugString());
  }
  return ret;
}

int32_t OutboundTransfer::TotalLength() const {
  int32_t ret = 0;
  for (int i = 0; i < n_payload_slices_; i++) {
    ret += payload_slices_[i].size();
  }
  return ret;
}

} // namespace rpc
} // namespace kudu
