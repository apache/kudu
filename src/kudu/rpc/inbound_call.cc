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

#include "kudu/rpc/inbound_call.h"

#include <cstdint>
#include <memory>
#include <ostream>

#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/message_lite.h>

#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/connection.h"
#include "kudu/rpc/rpc_introspection.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/rpc/rpcz_store.h"
#include "kudu/rpc/serialization.h"
#include "kudu/rpc/service_if.h"
#include "kudu/rpc/transfer.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/metrics.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/trace.h"

namespace google {
namespace protobuf {
class FieldDescriptor;
}
}

using google::protobuf::FieldDescriptor;
using google::protobuf::Message;
using google::protobuf::MessageLite;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace rpc {

InboundCall::InboundCall(Connection* conn)
  : conn_(conn),
    trace_(new Trace),
    method_info_(nullptr) {
  RecordCallReceived();
}

InboundCall::~InboundCall() {}

Status InboundCall::ParseFrom(gscoped_ptr<InboundTransfer> transfer) {
  TRACE_EVENT_FLOW_BEGIN0("rpc", "InboundCall", this);
  TRACE_EVENT0("rpc", "InboundCall::ParseFrom");
  RETURN_NOT_OK(serialization::ParseMessage(transfer->data(), &header_, &serialized_request_));

  // Adopt the service/method info from the header as soon as it's available.
  if (PREDICT_FALSE(!header_.has_remote_method())) {
    return Status::Corruption("Non-connection context request header must specify remote_method");
  }
  if (PREDICT_FALSE(!header_.remote_method().IsInitialized())) {
    return Status::Corruption("remote_method in request header is not initialized",
                              header_.remote_method().InitializationErrorString());
  }
  remote_method_.FromPB(header_.remote_method());

  if (header_.sidecar_offsets_size() > TransferLimits::kMaxSidecars) {
    return Status::Corruption(strings::Substitute(
            "Received $0 additional payload slices, expected at most %d",
            header_.sidecar_offsets_size(), TransferLimits::kMaxSidecars));
  }

  RETURN_NOT_OK(RpcSidecar::ParseSidecars(
          header_.sidecar_offsets(), serialized_request_, inbound_sidecar_slices_));
  if (header_.sidecar_offsets_size() > 0) {
    // Trim the request to just the message
    serialized_request_ = Slice(serialized_request_.data(), header_.sidecar_offsets(0));
  }

  // Retain the buffer that we have a view into.
  transfer_.swap(transfer);
  return Status::OK();
}

void InboundCall::RespondSuccess(const MessageLite& response) {
  TRACE_EVENT0("rpc", "InboundCall::RespondSuccess");
  Respond(response, true);
}

void InboundCall::RespondUnsupportedFeature(const vector<uint32_t>& unsupported_features) {
  TRACE_EVENT0("rpc", "InboundCall::RespondUnsupportedFeature");
  ErrorStatusPB err;
  err.set_message("unsupported feature flags");
  err.set_code(ErrorStatusPB::ERROR_INVALID_REQUEST);
  for (uint32_t feature : unsupported_features) {
    err.add_unsupported_feature_flags(feature);
  }

  Respond(err, false);
}

void InboundCall::RespondFailure(ErrorStatusPB::RpcErrorCodePB error_code,
                                 const Status& status) {
  TRACE_EVENT0("rpc", "InboundCall::RespondFailure");
  ErrorStatusPB err;
  err.set_message(status.ToString());
  err.set_code(error_code);

  Respond(err, false);
}

void InboundCall::RespondApplicationError(int error_ext_id, const std::string& message,
                                          const MessageLite& app_error_pb) {
  ErrorStatusPB err;
  ApplicationErrorToPB(error_ext_id, message, app_error_pb, &err);
  Respond(err, false);
}

void InboundCall::ApplicationErrorToPB(int error_ext_id, const std::string& message,
                                       const google::protobuf::MessageLite& app_error_pb,
                                       ErrorStatusPB* err) {
  err->set_message(message);
  const FieldDescriptor* app_error_field =
    err->GetReflection()->FindKnownExtensionByNumber(error_ext_id);
  if (app_error_field != nullptr) {
    err->GetReflection()->MutableMessage(err, app_error_field)->CheckTypeAndMergeFrom(app_error_pb);
  } else {
    LOG(DFATAL) << "Unable to find application error extension ID " << error_ext_id
                << " (message=" << message << ")";
  }
}

void InboundCall::Respond(const MessageLite& response,
                          bool is_success) {
  TRACE_EVENT_FLOW_END0("rpc", "InboundCall", this);
  SerializeResponseBuffer(response, is_success);

  TRACE_EVENT_ASYNC_END1("rpc", "InboundCall", this,
                         "method", remote_method_.method_name());
  TRACE_TO(trace_, "Queueing $0 response", is_success ? "success" : "failure");
  RecordHandlingCompleted();
  conn_->rpcz_store()->AddCall(this);
  conn_->QueueResponseForCall(gscoped_ptr<InboundCall>(this));
}

void InboundCall::SerializeResponseBuffer(const MessageLite& response,
                                          bool is_success) {
  if (PREDICT_FALSE(!response.IsInitialized())) {
    LOG(ERROR) << "Invalid RPC response for " << ToString()
               << ": protobuf missing required fields: "
               << response.InitializationErrorString();
    // Send it along anyway -- the client will also notice the missing fields
    // and produce an error on the other side, but this will at least
    // make it clear on both sides of the RPC connection what kind of error
    // happened.
  }

  uint32_t protobuf_msg_size = response.ByteSize();

  ResponseHeader resp_hdr;
  resp_hdr.set_call_id(header_.call_id());
  resp_hdr.set_is_error(!is_success);
  int32_t sidecar_byte_size = 0;
  for (const unique_ptr<RpcSidecar>& car : outbound_sidecars_) {
    resp_hdr.add_sidecar_offsets(sidecar_byte_size + protobuf_msg_size);
    int32_t sidecar_bytes = car->AsSlice().size();
    DCHECK_LE(sidecar_byte_size, TransferLimits::kMaxTotalSidecarBytes - sidecar_bytes);
    sidecar_byte_size += sidecar_bytes;
  }

  serialization::SerializeMessage(response, &response_msg_buf_,
                                  sidecar_byte_size, true);
  int64_t main_msg_size = sidecar_byte_size + response_msg_buf_.size();
  serialization::SerializeHeader(resp_hdr, main_msg_size,
                                 &response_hdr_buf_);
}

size_t InboundCall::SerializeResponseTo(TransferPayload* slices) const {
  TRACE_EVENT0("rpc", "InboundCall::SerializeResponseTo");
  DCHECK_GT(response_hdr_buf_.size(), 0);
  DCHECK_GT(response_msg_buf_.size(), 0);
  size_t n_slices = 2 + outbound_sidecars_.size();
  DCHECK_LE(n_slices, slices->size());
  auto slice_iter = slices->begin();
  *slice_iter++ = Slice(response_hdr_buf_);
  *slice_iter++ = Slice(response_msg_buf_);
  for (auto& sidecar : outbound_sidecars_) {
    *slice_iter++ = sidecar->AsSlice();
  }
  DCHECK_EQ(slice_iter - slices->begin(), n_slices);
  return n_slices;
}

Status InboundCall::AddOutboundSidecar(unique_ptr<RpcSidecar> car, int* idx) {
  // Check that the number of sidecars does not exceed the number of payload
  // slices that are free (two are used up by the header and main message
  // protobufs).
  if (outbound_sidecars_.size() > TransferLimits::kMaxSidecars) {
    return Status::ServiceUnavailable("All available sidecars already used");
  }
  int64_t sidecar_bytes = car->AsSlice().size();
  if (outbound_sidecars_total_bytes_ >
      TransferLimits::kMaxTotalSidecarBytes - sidecar_bytes) {
    return Status::RuntimeError(Substitute("Total size of sidecars $0 would exceed limit $1",
        static_cast<int64_t>(outbound_sidecars_total_bytes_) + sidecar_bytes,
        TransferLimits::kMaxTotalSidecarBytes));
  }

  outbound_sidecars_.emplace_back(std::move(car));
  outbound_sidecars_total_bytes_ += sidecar_bytes;
  DCHECK_GE(outbound_sidecars_total_bytes_, 0);
  *idx = outbound_sidecars_.size() - 1;
  return Status::OK();
}

string InboundCall::ToString() const {
  if (header_.has_request_id()) {
    return Substitute("Call $0 from $1 (ReqId={client: $2, seq_no=$3, attempt_no=$4})",
                      remote_method_.ToString(),
                      conn_->remote().ToString(),
                      header_.request_id().client_id(),
                      header_.request_id().seq_no(),
                      header_.request_id().attempt_no());
  }
  return Substitute("Call $0 from $1 (request call id $2)",
                      remote_method_.ToString(),
                      conn_->remote().ToString(),
                      header_.call_id());
}

void InboundCall::DumpPB(const DumpRunningRpcsRequestPB& req,
                         RpcCallInProgressPB* resp) {
  resp->mutable_header()->CopyFrom(header_);
  if (req.include_traces() && trace_) {
    resp->set_trace_buffer(trace_->DumpToString());
  }
  resp->set_micros_elapsed((MonoTime::Now() - timing_.time_received)
                           .ToMicroseconds());
}

const RemoteUser& InboundCall::remote_user() const {
  return conn_->remote_user();
}

const Sockaddr& InboundCall::remote_address() const {
  return conn_->remote();
}

const scoped_refptr<Connection>& InboundCall::connection() const {
  return conn_;
}

Trace* InboundCall::trace() {
  return trace_.get();
}

void InboundCall::RecordCallReceived() {
  TRACE_EVENT_ASYNC_BEGIN0("rpc", "InboundCall", this);
  DCHECK(!timing_.time_received.Initialized());  // Protect against multiple calls.
  timing_.time_received = MonoTime::Now();
}

void InboundCall::RecordHandlingStarted(Histogram* incoming_queue_time) {
  DCHECK(incoming_queue_time != nullptr);
  DCHECK(!timing_.time_handled.Initialized());  // Protect against multiple calls.
  timing_.time_handled = MonoTime::Now();
  incoming_queue_time->Increment(
      (timing_.time_handled - timing_.time_received).ToMicroseconds());
}

void InboundCall::RecordHandlingCompleted() {
  DCHECK(!timing_.time_completed.Initialized());  // Protect against multiple calls.
  timing_.time_completed = MonoTime::Now();

  if (!timing_.time_handled.Initialized()) {
    // Sometimes we respond to a call before we begin handling it (e.g. due to queue
    // overflow, etc). These cases should not be counted against the histogram.
    return;
  }

  if (method_info_) {
    method_info_->handler_latency_histogram->Increment(
        (timing_.time_completed - timing_.time_handled).ToMicroseconds());
  }
}

bool InboundCall::ClientTimedOut() const {
  if (!header_.has_timeout_millis() || header_.timeout_millis() == 0) {
    return false;
  }

  MonoTime now = MonoTime::Now();
  int total_time = (now - timing_.time_received).ToMilliseconds();
  return total_time > header_.timeout_millis();
}

MonoTime InboundCall::GetClientDeadline() const {
  if (!header_.has_timeout_millis() || header_.timeout_millis() == 0) {
    return MonoTime::Max();
  }
  return timing_.time_received + MonoDelta::FromMilliseconds(header_.timeout_millis());
}

MonoTime InboundCall::GetTimeReceived() const {
  return timing_.time_received;
}

vector<uint32_t> InboundCall::GetRequiredFeatures() const {
  vector<uint32_t> features;
  for (uint32_t feature : header_.required_feature_flags()) {
    features.push_back(feature);
  }
  return features;
}

Status InboundCall::GetInboundSidecar(int idx, Slice* sidecar) const {
  DCHECK(transfer_) << "Sidecars have been discarded";
  if (idx < 0 || idx >= header_.sidecar_offsets_size()) {
    return Status::InvalidArgument(strings::Substitute(
            "Index $0 does not reference a valid sidecar", idx));
  }
  *sidecar = inbound_sidecar_slices_[idx];
  return Status::OK();
}

void InboundCall::DiscardTransfer() {
  transfer_.reset();
}

size_t InboundCall::GetTransferSize() {
  if (!transfer_) return 0;
  return transfer_->data().size();
}

} // namespace rpc
} // namespace kudu
